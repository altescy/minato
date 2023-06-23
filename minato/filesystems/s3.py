from __future__ import annotations

import logging
import os
import re
import tempfile
import threading
from contextlib import contextmanager
from os import PathLike
from pathlib import Path
from typing import IO, Any, BinaryIO, ContextManager, Iterator, TextIO, overload

from minato.common import Progress
from minato.filesystems.filesystem import FileSystem
from minato.util import DecompressOption, OpenBinaryMode, OpenTextMode, sizeof_fmt, xopen

try:
    import boto3
except ModuleNotFoundError:
    boto3 = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)


@FileSystem.register(["s3"])
class S3FileSystem(FileSystem):
    def __init__(self, url_or_filename: str | PathLike) -> None:
        if boto3 is None:
            raise ModuleNotFoundError(
                "S3FileSystem is not available. Please make sure that " "boto3 is successfully installed."
            )

        super().__init__(url_or_filename)
        self._tlocal = threading.local()

        self._bucket_name = self._url.netloc or ""
        self._key = re.sub(r"^/", "", self._url.path)

        self._aws_access_key_id = self._url.username or os.environ.get("AWS_ACCESS_KEY_ID")
        self._aws_secret_access_key = self._url.password or os.environ.get("AWS_SECRET_ACCESS_KEY")
        self._endpoint_url = self._url.get_query("endpoint_url")
        self._region_name = self._url.get_query("region")

    def _get_resource(self):  # type: ignore
        if not hasattr(self._tlocal, "resource"):
            self._tlocal.resource = boto3.resource(
                "s3",
                region_name=self._region_name,
                aws_access_key_id=self._aws_access_key_id,
                aws_secret_access_key=self._aws_secret_access_key,
                endpoint_url=self._endpoint_url,
            )
        return self._tlocal.resource

    def _get_client(self):  # type: ignore
        if not hasattr(self._tlocal, "client"):
            self._tlocal.client = boto3.client(
                "s3",
                region_name=self._region_name,
                aws_access_key_id=self._aws_access_key_id,
                aws_secret_access_key=self._aws_secret_access_key,
                endpoint_url=self._endpoint_url,
            )
        return self._tlocal.client

    def _download_fileobj(self, key: str, fp: IO[Any]) -> None:
        client = self._get_client()  # type: ignore
        client.download_fileobj(self._bucket_name, key, fp)

    def _upload_fileobj(self, fp: IO[Any], key: str) -> None:
        client = self._get_client()  # type: ignore
        client.upload_fileobj(fp, self._bucket_name, key)

    def exists(self) -> bool:
        resource = self._get_resource()  # type: ignore
        bucket = resource.Bucket(self._bucket_name)
        objects = list(bucket.objects.filter(Prefix=self._key))
        return len(objects) > 0

    def download(self, path: str | PathLike) -> None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        path = Path(path)

        resource = self._get_resource()  # type: ignore
        bucket = resource.Bucket(self._bucket_name)
        objects = [obj for obj in bucket.objects.filter(Prefix=self._key) if not obj.key.endswith("/")]
        total = sum(obj.size for obj in objects)

        logger.debug("%s file(s) (%sB) will be downloaded to %s.", len(objects), total, path)

        path = path / os.path.basename(self._key) if path.is_dir() else path

        with Progress[int](
            total,
            unit="iB",
            desc="downloading from s3",
            sizeof_formatter=sizeof_fmt,
        ) as progress:
            for obj in objects:
                relprefix = os.path.relpath(obj.key, self._key)
                file_path = path / relprefix
                os.makedirs(file_path.parent, exist_ok=True)
                bucket.download_file(obj.key, str(file_path), Callback=progress.update)

    def upload(self, path: str | PathLike) -> None:
        path = Path(str(path)).absolute()

        prefix = self._key
        if prefix.endswith("/"):
            prefix = os.path.join(prefix, path.name)

        filenames = [subpath for subpath in path.glob("**/*") if subpath.is_file()] if path.is_dir() else [path]

        total = sum(filename.stat().st_size for filename in filenames)

        logger.info("%s file(s) (%sB) will be uploaded to %s", len(filenames), total, self._url)

        resource = self._get_resource()  # type: ignore
        bucket = resource.Bucket(self._bucket_name)

        with Progress[int](
            total,
            unit="iB",
            desc="uploading to s3",
            sizeof_formatter=sizeof_fmt,
        ) as progress:
            for filename in filenames:
                if filename != path:
                    relpath = os.path.relpath(filename, path)
                    key = os.path.join(prefix, relpath)
                else:
                    key = prefix
                bucket.upload_file(str(filename), key, Callback=progress.update)

    def delete(self) -> None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        resource = self._get_resource()  # type: ignore
        bucket = resource.Bucket(self._bucket_name)
        bucket.objects.filter(Prefix=self._key).delete()

    def get_version(self) -> str | None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        resource = self._get_resource()  # type: ignore
        bucket = resource.Bucket(self._bucket_name)
        objects = list(bucket.objects.filter(Prefix=self._key))
        etags = [str(obj.e_tag).strip('"') for obj in objects if obj.e_tag]
        return ".".join(sorted(etags)) if etags else None

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
