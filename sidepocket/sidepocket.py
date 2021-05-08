from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Optional, Union

from sidepocket.cache import Cache, CachedFile
from sidepocket.config import Config
from sidepocket.util import extract_path, is_local, open_file


class SidePocket:
    def __init__(
        self,
        cache_directory: Optional[Path] = None,
        sqlite_database: Optional[Path] = None,
        expire_days: Optional[int] = None,
    ) -> None:
        config = Config()
        cache_directory = cache_directory or config.cache_directory
        sqlite_database = sqlite_database or config.sqlite_database
        expire_days = expire_days or config.expire_days

        self._cache = Cache(cache_directory, sqlite_database, expire_days)

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
