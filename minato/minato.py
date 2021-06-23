from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Optional, Union

from minato.cache import Cache, CacheStatus
from minato.config import Config
from minato.exceptions import InvalidCacheStatus
from minato.filesystems import download, open_file
from minato.util import (
    extract_archive_file,
    extract_path,
    is_archive_file,
    is_local,
    remove_file_or_directory,
)


class Minato:
    def __init__(self, config: Optional[Config] = None) -> None:
        self._config = config or Config.load()
        self._cache = Cache(
            root=self._config.cache_root,
            expire_days=self._config.expire_days,
        )

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
        force_download: bool = False,
        force_extract: bool = False,
    ) -> Iterator[IO[Any]]:
        if (
            not ("a" in mode and "w" in mode and "x" in mode and "+" in mode)
            and use_cache
        ):
            url_or_filename = self.cached_path(
                url_or_filename,
                extract=extract,
                force_download=force_download,
                force_extract=force_extract,
            )

        with open_file(url_or_filename, mode) as fp:
            yield fp

    def cached_path(
        self,
        url_or_filename: Union[str, Path],
        extract: bool = False,
        force_download: bool = False,
        force_extract: bool = False,
        retry: bool = True,
    ) -> Path:
        url_or_filename = str(url_or_filename)

        if "!" in url_or_filename:
            remote_archive_path, file_path = url_or_filename.rsplit("!", 1)
            archive_path = self.cached_path(
                remote_archive_path,
                extract=True,
                force_extract=force_extract,
                force_download=force_download,
            )
            if not archive_path.is_dir():
                raise ValueError(
                    f"{url_or_filename} uses the ! syntax, but this is not an archive file."
                )

            file_path = extract_path(file_path)
            local_path = archive_path / file_path
            if not local_path.exists():
                raise FileNotFoundError(local_path)
            return local_path

        if is_local(url_or_filename):
            url_or_filename = extract_path(url_or_filename)
            if not extract and not is_archive_file(url_or_filename):
                local_path = Path(url_or_filename)
                if not local_path.exists():
                    raise FileNotFoundError(local_path)
                return local_path

        url = str(url_or_filename)
        if url in self._cache:
            cached_file = self._cache.by_url(url)
        else:
            cached_file = self._cache.new(url)

        with self._cache.lock(cached_file):
            if not self._cache.exists(cached_file):
                self._cache.add(cached_file)

            cached_file = self._cache.by_url(url)

            if retry and cached_file.status != CacheStatus.COMPLETED:
                force_download = True

            try:
                downloaded = False
                if (
                    not cached_file.local_path.exists()
                    or self._cache.is_expired(cached_file)
                    or force_download
                ):
                    remove_file_or_directory(cached_file.local_path)

                    cached_file.status = CacheStatus.DOWNLOADING
                    self._cache.update(cached_file)

                    self.download(cached_file.url, cached_file.local_path)
                    downloaded = True

                extracted = False
                if (
                    (extract and cached_file.extraction_path is None)
                    or (downloaded and cached_file.extraction_path is not None)
                    or force_extract
                ) and is_archive_file(cached_file.local_path):
                    cached_file.extraction_path = Path(
                        str(cached_file.local_path) + "-extracted"
                    )
                    remove_file_or_directory(cached_file.extraction_path)

                    cached_file.status = CacheStatus.EXTRACTING
                    self._cache.update(cached_file)

                    extract_archive_file(
                        cached_file.local_path, cached_file.extraction_path
                    )
                    extracted = True

                if downloaded or extracted:
                    cached_file.status = CacheStatus.COMPLETED
                    self._cache.update(cached_file)
            except FileNotFoundError:
                self._cache.delete(cached_file)
                raise
            except (Exception, SystemExit, KeyboardInterrupt):
                cached_file.status = CacheStatus.FAILED
                self._cache.update(cached_file)
                raise

        if (extract or force_extract) and cached_file.extraction_path:
            if not cached_file.extraction_path.exists():
                raise FileNotFoundError(cached_file.extraction_path)
            if cached_file.status != CacheStatus.COMPLETED:
                raise InvalidCacheStatus(
                    f"Cached path status is not completed: status={cached_file.status}"
                )
            return cached_file.extraction_path

        if not cached_file.local_path.exists():
            raise FileNotFoundError(cached_file.local_path)
        if cached_file.status != CacheStatus.COMPLETED:
            raise InvalidCacheStatus(
                f"Cached path status is not completed: status={cached_file.status}"
            )
        return cached_file.local_path

    @staticmethod
    def download(url: str, filename: Path) -> None:
        download(url, filename)

    @staticmethod
    def upload(filename: Path, url: str) -> None:
        with open(filename, "rb") as local_file:
            with open_file(url, "wb") as remote_file:
                content = local_file.read()
                remote_file.write(content)

    def remove(self, url: str) -> None:
        cached_file = self._cache.by_url(url)

        remove_file_or_directory(cached_file.local_path)
        if cached_file.extraction_path is not None:
            remove_file_or_directory(cached_file.extraction_path)
        self._cache.delete(cached_file)
