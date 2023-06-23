from __future__ import annotations

import logging
import os
import re
import tempfile
from contextlib import contextmanager
from os import PathLike
from pathlib import Path
from typing import IO, Any, BinaryIO, ContextManager, Iterator, TextIO, overload

from minato.common import Progress
from minato.filesystems.filesystem import FileSystem
from minato.util import DecompressOption, OpenBinaryMode, OpenTextMode, sizeof_fmt, xopen

try:
    import google.cloud.storage as gcs
    from google.cloud.storage import Blob, Client
except ImportError:
    gcs = None
    Blob, Client = None, None


logger = logging.getLogger(__name__)


@FileSystem.register(["gs", "gcs"])
class GCSFileSystem(FileSystem):
    def __init__(self, url_or_filename: str | PathLike) -> None:
        if gcs is None:
            raise ModuleNotFoundError(
                "GCSFileSystem is not available. Please make sure that "
                "google-cloud-storage is successfully installed."
            )

        super().__init__(url_or_filename)
        self._bucket_name = self._url.netloc or ""
        self._key = re.sub(r"^/", "", self._url.path)

        self._project = self._url.get_query("project")
        self._api_endpoint = self._url.get_query("api_endpoint")

        self._client = Client()
        if self._project:
            self._client.project = self._project
        if self._api_endpoint:
            self._client.client_options = {"api_endpoint": self._api_endpoint}

    def _get_blob(self, blob_name: str) -> Blob:
        bucket = self._client.bucket(self._bucket_name)
        blob = bucket.blob(blob_name)
        return blob

    def _get_url_from_blob(self, blob: Blob) -> str:
        return "gs://{self._bucket_name}/{blob.name}"

    def _download_fileobj(self, key: str, fp: IO[Any]) -> None:
        blob = self._get_blob(key)
        blob.download_to_file(fp)

    def _upload_fileobj(self, fp: IO[Any], key: str) -> None:
        blob = self._get_blob(key)
        blob.upload_from_file(fp)

    def exists(self) -> bool:
        client = self._client
        bucket = client.bucket(self._bucket_name)
        blobs = list(bucket.list_blobs(prefix=self._key))
        return len(blobs) > 0

    def download(self, path: str | PathLike) -> None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        if not isinstance(path, Path):
            path = Path(path)

        client = self._client
        bucket = client.bucket(self._bucket_name)
        blobs = [blob for blob in bucket.list_blobs(prefix=self._key) if not blob.name.endswith("/")]
        total = sum(blob.size for blob in blobs if blob.size)

        logger.debug("%s file(s) (%sB) will be downloaded to %s.", len(blobs), total, path)
        with Progress[int](
            total,
            unit="iB",
            desc="downloading from gcs",
            sizeof_formatter=sizeof_fmt,
        ) as progress:
            for blob in blobs:
                relpath = os.path.relpath(blob.name, self._key)
                file_path = path / relpath
                os.makedirs(file_path.parent, exist_ok=True)
                blob.download_to_filename(str(file_path))
                progress.update(blob.size or 0)

    def upload(self, path: str | PathLike) -> None:
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(path)

        prefix = self._key
        if prefix.endswith("/"):
            prefix = os.path.join(prefix, path.name)

        filenames = [subpath for subpath in path.glob("**/*") if subpath.is_file()] if path.is_dir() else [path]

        total = sum(filename.stat().st_size for filename in filenames)

        logger.debug("%s file(s) (%sB) will be uploaded to %s", len(filenames), total, self._url)

        client = self._client
        bucket = client.bucket(self._bucket_name)

        with Progress[int](
            total,
            unit="iB",
            desc="uploading from gcs",
            sizeof_formatter=sizeof_fmt,
        ) as progress:
            for filename in filenames:
                if filename != path:
                    relpath = os.path.relpath(filename, path)
                    key = os.path.join(prefix, relpath)
                else:
                    key = prefix

                blob = bucket.blob(key)
                blob.upload_from_filename(str(filename))
                progress.update(filename.stat().st_size)

    def delete(self) -> None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        client = self._client
        bucket = client.bucket(self._bucket_name)
        blobs = list(bucket.list_blobs(prefix=self._key))
        for blob in blobs:
            blob.delete()

    def get_version(self) -> str | None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        client = self._client
        bucket = client.bucket(self._bucket_name)
        blobs = list(bucket.list_blobs(prefix=self._key))
        hashes = [str(blob.md5_hash) for blob in blobs if blob.md5_hash and not blob.name.endswith("/")]
        return ".".join(sorted(hashes)) if hashes else None

    @overload
    def open_file(
        self,
        mode: OpenTextMode = ...,
        buffering: int = ...,
        encoding: str | None = ...,
        errors: str | None = ...,
        newline: str | None = ...,
        *,
        decompress: DecompressOption = ...,
    ) -> ContextManager[TextIO]:
        ...

    @overload
    def open_file(
        self,
        mode: OpenBinaryMode,
        buffering: int = ...,
        encoding: str | None = ...,
        errors: str | None = ...,
        newline: str | None = ...,
        *,
        decompress: DecompressOption = ...,
    ) -> ContextManager[BinaryIO]:
        ...

    @overload
    def open_file(
        self,
        mode: str,
        buffering: int = ...,
        encoding: str | None = ...,
        errors: str | None = ...,
        newline: str | None = ...,
        *,
        decompress: DecompressOption = ...,
    ) -> ContextManager[IO[Any]]:
        ...

    def open_file(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        *,
        decompress: DecompressOption = "none",
    ) -> ContextManager[IO[Any]]:
        @contextmanager
        def _open(
            mode: str,
            buffering: int,
            encoding: str | None,
            errors: str | None,
            newline: str | None,
        ) -> Iterator[IO[Any]]:
            if "x" in mode and self.exists():
                raise FileExistsError(self._url.raw)

            suffix = "-" + (self._url.path.split("/")[-1] if self._url.path else "")
            local_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)

            try:
                if "r" in mode or "a" in mode or "+" in mode:
                    if not self.exists():
                        raise FileNotFoundError(self._url.raw)
                    self._download_fileobj(self._key, local_file)

                local_file.close()
                with xopen(
                    local_file.name,
                    mode=mode,
                    buffering=buffering,
                    encoding=encoding,
                    errors=errors,
                    newline=newline,
                    decompress=decompress,
                ) as fp:
                    yield fp

                if "w" in mode or "a" in mode or "+" in mode or "x" in mode:
                    with open(local_file.name, "rb") as fp:
                        self._upload_fileobj(fp, self._key)

            finally:
                local_file.close()
                os.remove(local_file.name)

        return _open(mode, buffering, encoding, errors, newline)
