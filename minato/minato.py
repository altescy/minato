import os
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Optional, Union

from minato.cache import Cache
from minato.config import Config
from minato.util import extract_path, is_local, open_file


class Minato:
    def __init__(
        self,
        root: Optional[Path] = None,
    ) -> None:
        if root is not None and not root.exists():
            os.makedirs(root, exist_ok=True)

        config = Config(minato_root=root)
        self._cache = Cache(config.cache_directory, config.sqlite_database)

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
        if url in self._cache:
            cached_file = self._cache.by_url(url)
            if not update:
                return cached_file.local_path
        else:
            with self._cache.tx() as tx:
                cached_file = tx.add(url)

        self.download(cached_file.url, cached_file.local_path)

        if update:
            with self._cache.tx() as tx:
                tx.update(cached_file)

        return cached_file.local_path

    def download(self, url: str, filename: Path) -> None:
        with open(filename, "wb") as local_file:
            with open_file(url, "rb") as remote_file:
                content = remote_file.read()
                local_file.write(content)

    def upload(self, filename: Path, url: str) -> None:
        with open(filename, "rb") as local_file:
            with open_file(url, "wb") as remote_file:
                content = local_file.read()
                remote_file.write(content)

    def remove(self, id_or_url: Union[int, str]) -> None:
        if isinstance(id_or_url, int):
            cache_id = id_or_url
            cached_file = self._cache.by_id(cache_id)
        else:
            url = id_or_url
            cached_file = self._cache.by_url(url)

        self._cache.delete(cached_file)
