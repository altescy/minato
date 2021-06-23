import os
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Union

from google.cloud.storage import Blob, Client

from minato.filesystems.filesystem import FileSystem


@FileSystem.register(["gs", "gcs"])
class GCSFileSystem(FileSystem):
    def __init__(self, url_or_filename: Union[str, Path]) -> None:
        super().__init__(url_or_filename)
        self._bucket_name = self._url.hostname or ""
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

    def download(self, path: Union[str, Path]) -> None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        if isinstance(path, str):
            path = Path(path)

        client = self._client
        bucket = client.bucket(self._bucket_name)
        blobs = list(bucket.list_blobs(prefix=self._key))

        for blob in blobs:
            relpath = os.path.relpath(blob.name, self._key)
            file_path = path / relpath
            os.makedirs(file_path.parent, exist_ok=True)
            if not blob.name.endswith("/"):
                blob.download_to_filename(str(file_path))

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
