import logging
import os
import re
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Optional, Union

from tqdm import tqdm

from minato.filesystems.filesystem import FileSystem

try:
    import boto3
except ModuleNotFoundError:
    boto3 = None


logger = logging.getLogger(__name__)


@FileSystem.register(["s3"])
class S3FileSystem(FileSystem):
    def __init__(self, url_or_filename: Union[str, Path]) -> None:
        if boto3 is None:
            raise ModuleNotFoundError(
                "S3FileSystem is not available. Please make sure that "
                "boto3 is successfully installed."
            )

        super().__init__(url_or_filename)
        self._tlocal = threading.local()

        self._bucket_name = self._url.netloc or ""
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
        objects = [
            obj
            for obj in bucket.objects.filter(Prefix=self._key)
            if not obj.key.endswith("/")
        ]
        total = sum(obj.size for obj in objects)

        logger.info(
            "%s file(s) (%sB) will be downloaded to %s.", len(objects), total, path
        )
        progress = tqdm(unit="B", total=total, desc="downloading")
        if len(objects) == 1:  # if the given path is a file
            obj = objects[0]
            if path.is_dir():
                file_path = path / os.path.basename(obj.key)
            else:
                file_path = path
            os.makedirs(file_path.parent, exist_ok=True)
            bucket.download_file(obj.key, str(file_path))
            progress.update(obj.size)
        else:  # if the given path is a directory
            for obj in objects:
                relpath = os.path.relpath(obj.key, self._key)
                parent_dir = path / os.path.dirname(relpath)
                os.makedirs(parent_dir, exist_ok=True)

                file_path = path / relpath
                bucket.download_file(obj.key, str(file_path))
                progress.update(obj.size)
        progress.close()

    def delete(self) -> None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        resource = self._get_resource()  # type: ignore
        bucket = resource.Bucket(self._bucket_name)
        bucket.objects.filter(Prefix=self._key).delete()

    def get_version(self) -> Optional[str]:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        resource = self._get_resource()  # type: ignore
        bucket = resource.Bucket(self._bucket_name)
        objects = list(bucket.objects.filter(Prefix=self._key))
        etags = [str(obj.e_tag).strip('"') for obj in objects if obj.e_tag]
        return ".".join(sorted(etags)) if etags else None

    @contextmanager
    def open_file(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
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
            with open(
                local_file.name,
                mode=mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
            ) as fp:
                yield fp

            if "w" in mode or "a" in mode or "+" in mode or "x" in mode:
                with open(local_file.name, "rb") as fp:
                    self._upload_fileobj(fp, self._key)

        finally:
            local_file.close()
            os.remove(local_file.name)
