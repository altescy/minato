from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Optional, Union

from minato.cache import Cache, CachedFile
from minato.config import Config
from minato.util import extract_path, is_local, open_file


class Minato:
    def __init__(
        self,
        cache_directory: Optional[Path] = None,
        sqlite_database: Optional[Path] = None,
    ) -> None:
        config = Config()
        cache_directory = cache_directory or config.cache_directory
        sqlite_database = sqlite_database or config.sqlite_database

        self._cache = Cache(cache_directory, sqlite_database)

    @property
    def cache(self) -> Cache:
        return self._cache

    @contextmanager
    def open(
        self,
        url_or_filename: Union[str, Path],
        mode: str = "r",
        use_cache: bool = False,
        update: bool = False,
    ) -> Iterator[IO[Any]]:
        if use_cache:
            url_or_filename = self.cached_path(
                url_or_filename,
                update=update,
            )

        with open_file(url_or_filename, mode) as fp:
            yield fp

    def cached_path(
        self,
        url_or_filename: Union[str, Path],
        update: bool = False,
    ) -> Path:
        if is_local(url_or_filename):
            filename = extract_path(url_or_filename)
            return filename

        url = str(url_or_filename)
        cached_file = self.download(url)
        local_path = cached_file.local_path

        return local_path

    def download(self, url: str, update: bool = False) -> CachedFile:
        if url in self._cache:
            cached_file = self._cache.by_url(url)
        else:
            with self._cache.tx() as tx:
                cached_file = tx.add(url)

        with open(cached_file.local_path, "wb") as local_file:
            with open_file(url, "rb") as remote_file:
                content = remote_file.read()
                local_file.write(content)

        if update:
            with self._cache.tx() as tx:
                tx.update(cached_file)

        return cached_file

    def remove(self, id_or_url: Union[int, str]) -> None:
        if isinstance(id_or_url, int):
            cache_id = id_or_url
            cached_file = self._cache.by_id(cache_id)
        else:
            url = id_or_url
            cached_file = self._cache.by_url(url)

        self._cache.delete(cached_file)
