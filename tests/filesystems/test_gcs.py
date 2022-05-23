from __future__ import annotations

import os
import tempfile
from collections.abc import Callable
from io import StringIO
from pathlib import Path
from typing import Any, Literal, Optional

import pytest
from google.cloud.storage import Blob, Client

from minato.filesystems.gcs import GCSFileSystem


class TempGCS:
    def __init__(self, bucket_name: Optional[str] = None, prefix: str = "tmp/") -> None:
        self._bucket_name = bucket_name or os.environ["MINATO_GCS_BUCKET_TEST"]
        self._prefix = prefix
        self._client: Optional[Client] = None

    def __enter__(self) -> TempGCS:
        if self._client is None:
            self._client = Client()
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> Literal[False]:
        if self._client is not None:
            bucket = self._client.bucket(self._bucket_name)
            for blob in bucket.list_blobs(prefix=self._prefix):
                blob.delete()
        return False

    def get_blob(self, key: str) -> Blob:
        assert self._client is not None
        key = self.get_key(key)
        bucket = self._client.bucket(self._bucket_name)
        return bucket.blob(key)

    def get_key(self, key: str) -> str:
        return os.path.join(self._prefix, key)

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def get_path(self, key: str) -> str:
        return f"gs://{self.bucket_name}/{self.get_key(key)}"


def check_google_application_credentials(
    func: Callable[[], None]
) -> Callable[[], None]:
    def decorator() -> None:
        google_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not google_credentials:
            pytest.skip(
                f"GOOGLE_APPLICATION_CREDENTIALS was not given, so {func.__name__} was skipped."
            )

        test_gcs_bucket = os.environ.get("MINATO_GCS_BUCKET_TEST")
        if not test_gcs_bucket:
            pytest.skip(
                f"MINATO_GCS_BUCKET_TEST was not given, so {func.__name__} was skipped."
            )

        func()

    return decorator


@check_google_application_credentials
def test_open_file() -> None:
    with TempGCS() as gcs:
        path = gcs.get_path("hello.txt")

        fs = GCSFileSystem(path)
        with fs.open_file("w") as fp:
            fp.write("Hello, world!")

        with fs.open_file("r") as fp:
            text = fp.read()

        assert text == "Hello, world!"


@check_google_application_credentials
def test_download() -> None:
    with TempGCS() as gcs:
        blob = gcs.get_blob("foo/foo.txt")
        with StringIO("foo") as fp:
            blob.upload_from_file(fp)

        blob = gcs.get_blob("foo/bar/bar.txt")
        with StringIO("bar") as fp:
            blob.upload_from_file(fp)

        with tempfile.TemporaryDirectory() as _tempdir:
            tempdir = Path(_tempdir)

            path = gcs.get_path("foo")
            fs = GCSFileSystem(path)
            fs.download(tempdir)

            assert (tempdir / "foo.txt").is_file()
            assert (tempdir / "bar").is_dir()
            assert (tempdir / "bar" / "bar.txt").is_file()

            with open(tempdir / "bar" / "bar.txt") as f:
                text = f.read()
                assert text == "bar"


@check_google_application_credentials
def test_exists() -> None:
    with TempGCS() as gcs:
        blob = gcs.get_blob("foo/foo.txt")
        with StringIO("foo") as fp:
            blob.upload_from_file(fp)

        assert GCSFileSystem(gcs.get_path("foo")).exists()
        assert GCSFileSystem(gcs.get_path("foo/foo.txt")).exists()
        assert not GCSFileSystem(gcs.get_path("foo/bar.txt")).exists()


@check_google_application_credentials
def test_delete() -> None:
    with TempGCS() as gcs:
        blob = gcs.get_blob("foo/foo.txt")
        with StringIO("foo") as fp:
            blob.upload_from_file(fp)

        fs = GCSFileSystem(gcs.get_path("foo"))
        assert fs.exists()

        fs.delete()

        assert not fs.exists()


@check_google_application_credentials
def test_get_version() -> None:
    with TempGCS() as gcs:
        blob = gcs.get_blob("dir/foo.txt")
        with StringIO("foo") as fp:
            blob.upload_from_file(fp)

        blob = gcs.get_blob("dir/bar.txt")
        with StringIO("bar") as fp:
            blob.upload_from_file(fp)

        fs = GCSFileSystem(gcs.get_path("dir"))
        version = fs.get_version()

        assert version is not None
        assert len(version.split(".")) == 2


@check_google_application_credentials
def test_update_version() -> None:
    with TempGCS() as gcs:
        fs = GCSFileSystem(gcs.get_path("foo"))

        with fs.open_file("w") as fp:
            fp.write("hello")

        old_version = fs.get_version()
        assert old_version is not None

        current_version = fs.get_version()
        assert current_version is not None
        assert current_version == old_version

        with fs.open_file("w") as fp:
            fp.write("world")

        new_version = fs.get_version()
        assert new_version is not None
        assert old_version != new_version
