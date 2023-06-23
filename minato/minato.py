from __future__ import annotations

import logging
from os import PathLike
from pathlib import Path
from typing import IO, Any, BinaryIO, ContextManager, TextIO, overload

from minato.cache import Cache, CacheStatus
from minato.config import Config
from minato.exceptions import CacheNotFoundError, InvalidCacheStatus
from minato.filesystems import delete, download, exists, get_version, open_file, upload
from minato.util import (
    DecompressOption,
    OpenBinaryMode,
    OpenTextMode,
    extract_archive_file,
    extract_path,
    is_archive_file,
    is_local,
    remove_file_or_directory,
)

logger = logging.getLogger(__name__)


class Minato:
    def __init__(self, config: Config | None = None) -> None:
        self._config = config or Config.load()
        self._cache = Cache(
            root=self._config.cache_root,
            default_expire_days=self._config.expire_days,
            default_auto_update=self._config.auto_update,
        )

    @property
    def cache(self) -> Cache:
        return self._cache

    @overload
    def open(
        self,
        url_or_filename: str | PathLike,
        mode: OpenTextMode = ...,
        buffering: int = ...,
        encoding: str | None = ...,
        errors: str | None = ...,
        newline: str | None = ...,
        *,
        extract: bool = ...,
        decompress: DecompressOption = ...,
        auto_update: bool | None = ...,
        expire_days: int | None = ...,
        use_cache: bool = ...,
        force_download: bool = ...,
        force_extract: bool = ...,
        retry: bool = ...,
    ) -> ContextManager[TextIO]:
        ...

    @overload
    def open(
        self,
        url_or_filename: str | PathLike,
        mode: OpenBinaryMode,
        buffering: int = ...,
        encoding: str | None = ...,
        errors: str | None = ...,
        newline: str | None = ...,
        *,
        extract: bool = ...,
        decompress: DecompressOption = ...,
        auto_update: bool | None = ...,
        expire_days: int | None = ...,
        use_cache: bool = ...,
        force_download: bool = ...,
        force_extract: bool = ...,
        retry: bool = ...,
    ) -> ContextManager[BinaryIO]:
        ...

    @overload
    def open(
        self,
        url_or_filename: str | PathLike,
        mode: str,
        buffering: int = ...,
        encoding: str | None = ...,
        errors: str | None = ...,
        newline: str | None = ...,
        *,
        extract: bool = ...,
        decompress: DecompressOption = ...,
        auto_update: bool | None = ...,
        expire_days: int | None = ...,
        use_cache: bool = ...,
        force_download: bool = ...,
        force_extract: bool = ...,
        retry: bool = ...,
    ) -> ContextManager[IO[Any]]:
        ...

    def open(
        self,
        url_or_filename: str | PathLike,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        *,
        extract: bool = False,
        decompress: DecompressOption = "none",
        auto_update: bool | None = None,
        expire_days: int | None = None,
        use_cache: bool = True,
        force_download: bool = False,
        force_extract: bool = False,
        retry: bool = True,
    ) -> ContextManager[IO[Any]]:
        if not ("a" in mode or "w" in mode or "x" in mode or "+" in mode) and use_cache:
            logger.debug("Open cached file of %s.", url_or_filename)
            url_or_filename = self.cached_path(
                url_or_filename,
                extract=extract,
                auto_update=auto_update,
                expire_days=expire_days,
                force_download=force_download,
                force_extract=force_extract,
                retry=retry,
            )

        return open_file(url_or_filename, mode, buffering, encoding, errors, newline, decompress=decompress)

    def cached_path(
        self,
        url_or_filename: str | PathLike,
        extract: bool = False,
        auto_update: bool | None = None,
        expire_days: int | None = None,
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
                raise ValueError(f"{url_or_filename} uses the ! syntax, but this is not an archive file.")

            file_path = extract_path(file_path)
            local_path = archive_path / file_path
            if not local_path.exists():
                raise FileNotFoundError(local_path)
            return local_path

        if is_local(url_or_filename):
            url_or_filename = extract_path(url_or_filename)
            if not (extract and is_archive_file(url_or_filename)):
                local_path = Path(url_or_filename)
                if not local_path.exists():
                    raise FileNotFoundError(local_path)
                return local_path

        url = url_or_filename
        try:
            cached_file = self._cache.by_url(url)
        except CacheNotFoundError:
            cached_file = self._cache.new(url)

        with self._cache.lock(cached_file):
            if not self._cache.exists(cached_file):
                self._cache.add(cached_file)

            cached_file = self._cache.by_uid(cached_file.uid)

            if expire_days is not None:
                cached_file.expire_days = expire_days
                self._cache.save(cached_file)

            if auto_update is not None:
                cached_file.auto_update = auto_update
                self._cache.save(cached_file)

            if cached_file.auto_update and cached_file.version is not None:
                current_version = get_version(cached_file.url)
                if current_version != cached_file.version:
                    logger.debug("New version of data is available. It will be automatically updated.")
                    force_download = True

            if retry and cached_file.status != CacheStatus.COMPLETED:
                force_download = True

            try:
                downloaded = False
                if not cached_file.local_path.exists() or self._cache.is_expired(cached_file) or force_download:
                    remove_file_or_directory(cached_file.local_path)

                    cached_file.status = CacheStatus.DOWNLOADING
                    self._cache.update(cached_file)

                    logger.debug(
                        "Start downloading file(s) from %s to %s.",
                        cached_file.url,
                        cached_file.local_path,
                    )
                    self.download(cached_file.url, cached_file.local_path)
                    cached_file.version = get_version(cached_file.url)
                    downloaded = True

                extracted = False
                if (
                    (extract and cached_file.extraction_path is None)
                    or (downloaded and cached_file.extraction_path is not None)
                    or force_extract
                ) and is_archive_file(cached_file.local_path):
                    cached_file.extraction_path = Path(str(cached_file.local_path) + "-extracted")
                    remove_file_or_directory(cached_file.extraction_path)

                    cached_file.status = CacheStatus.EXTRACTING
                    self._cache.update(cached_file)

                    logging.debug(
                        "Extracting archive file from %s to %s.",
                        cached_file.local_path,
                        cached_file.extraction_path,
                    )
                    extract_archive_file(cached_file.local_path, cached_file.extraction_path)
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
                raise InvalidCacheStatus(f"Cached path status is not completed: status={cached_file.status}")
            return cached_file.extraction_path

        if not cached_file.local_path.exists():
            raise FileNotFoundError(cached_file.local_path)
        if cached_file.status != CacheStatus.COMPLETED:
            raise InvalidCacheStatus(f"Cached path status is not completed: status={cached_file.status}")
        return cached_file.local_path

    def available_update(self, url_or_filename: str | PathLike) -> bool:
        if is_local(url_or_filename):
            return False

        url = str(url_or_filename)
        cached_file = self._cache.by_url(url)
        current_version = get_version(url)
        return cached_file.version != current_version

    @staticmethod
    def download(url: str, filename: str | PathLike) -> None:
        download(url, filename)

    @staticmethod
    def upload(filename: str | PathLike, url: str) -> None:
        upload(filename, url)

    @staticmethod
    def delete(url_or_filename: str | PathLike) -> None:
        delete(url_or_filename)

    @staticmethod
    def exists(url_or_filename: str | PathLike) -> bool:
        return exists(url_or_filename)
