import os
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Optional, Union

from minato.cache import Cache
from minato.config import Config
from minato.filesystems import open_file
from minato.util import extract_archive_file, extract_path, is_archive_file, is_local


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
        extract: bool = False,
        use_cache: bool = True,
        update: bool = False,
    ) -> Iterator[IO[Any]]:
        if mode.startswith("r") and use_cache:
            url_or_filename = self.cached_path(
                url_or_filename,
                extract=extract,
                update=update,
            )

        with open_file(url_or_filename, mode) as fp:
            yield fp

    def cached_path(
        self,
        url_or_filename: Union[str, Path],
        update: bool = False,
        extract: bool = False,
    ) -> Path:
        url_or_filename = str(url_or_filename)

        if "!" in url_or_filename:
            remote_archive_path, file_path = url_or_filename.rsplit("!", 1)
            archive_path = self.cached_path(remote_archive_path, extract=True)
            if not archive_path.is_dir():
                raise ValueError(
                    f"{url_or_filename} uses the ! syntax, but this is not an archive file."
                )

            file_path = extract_path(file_path)
            filename = archive_path / file_path
            return filename

        if is_local(url_or_filename):
            url_or_filename = extract_path(url_or_filename)
            if not extract and not is_archive_file(url_or_filename):
                return Path(url_or_filename)

        url = str(url_or_filename)
        if url in self._cache:
            cached_file = self._cache.by_url(url)
        else:
            with self._cache.tx() as tx:
                cached_file = tx.add(url)

        if not cached_file.local_path.exists() or update:
            self.download(cached_file.url, cached_file.local_path)
            update = True

        if (
            extract
            and cached_file.extraction_path is None
            and is_archive_file(cached_file.local_path)
        ):
            cached_file.extraction_path = Path(
                str(cached_file.local_path) + "-extracted"
            )
            extract_archive_file(cached_file.local_path, cached_file.extraction_path)
            update = True

        if update:
            with self._cache.tx() as tx:
                tx.update(cached_file)

        if extract and cached_file.extraction_path:
            return cached_file.extraction_path

        return cached_file.local_path

    @staticmethod
    def download(url: str, filename: Path) -> None:
        with open(filename, "wb") as local_file:
            with open_file(url, "rb") as remote_file:
                content = remote_file.read()
                local_file.write(content)

    @staticmethod
    def upload(filename: Path, url: str) -> None:
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
