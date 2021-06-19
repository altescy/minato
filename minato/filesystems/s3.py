import os
import re
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Union

import boto3

from minato.filesystems.filesystem import FileSystem


@FileSystem.register(["s3"])
class S3FileSystem(FileSystem):
    def __init__(self, url_or_filename: Union[str, Path]) -> None:
        super().__init__(url_or_filename)
        self._tlocal = threading.local()

        self._bucket_name = self._url.hostname or ""
        self._key = re.sub(r"^/", "", self._url.path)

        self._aws_access_key_id = self._url.username or os.environ.get(
            "AWS_ACCESS_KEY_ID"
        )
        self._aws_secret_access_key = self._url.password or os.environ.get(
            "AWS_SECRET_ACCESS_KEY"
        )
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

    def download(self, path: Union[str, Path]) -> None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        if isinstance(path, str):
            path = Path(path)

        resource = self._get_resource()  # type: ignore
        bucket = resource.Bucket(self._bucket_name)
        objects = list(bucket.objects.filter(Prefix=self._key))

        if len(objects) == 1:  # if the given path is a file
            obj = objects[0]
            if path.is_dir():
                file_path = path / os.path.basename(obj.key)
            else:
                file_path = path
            os.makedirs(file_path.parent, exist_ok=True)
            bucket.download_file(obj.key, str(file_path))
        else:  # if the given path is a directory
            for obj in objects:
                relpath = os.path.relpath(obj.key, self._key)
                parent_dir = path / os.path.dirname(relpath)
                os.makedirs(parent_dir, exist_ok=True)

                file_path = path / relpath
                bucket.download_file(obj.key, str(file_path))

    @contextmanager
    def open_file(
        self,
        mode: str = "r",
    ) -> Iterator[IO[Any]]:
        if "x" in mode and self.exists():
            raise FileExistsError(self._url.raw)

        local_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            if "r" in mode or "a" in mode or "+" in mode:
                if not self.exists():
                    raise FileNotFoundError(self._url.raw)
                self._download_fileobj(self._key, local_file)

            local_file.close()
            with open(local_file.name, mode) as fp:
                yield fp

            if "w" in mode or "a" in mode or "+" in mode or "x" in mode:
                with open(local_file.name, "rb") as fp:
                    self._upload_fileobj(fp, self._key)

        finally:
            local_file.close()
            os.remove(local_file.name)
