import tempfile
from pathlib import Path

import boto3
from moto import mock_s3

import minato


def test_version() -> None:
    assert minato.__version__ == "0.3.0"


def test_open() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        cache_root = Path(tempdir)

        with minato.open(
            "https://raw.githubusercontent.com/altescy/minato/main/tests/fixtures/hello.txt",
            cache_root=cache_root,
        ) as fp:
            text = fp.readline().strip()

        assert text == "Hello, world!"


def test_cached_path() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        cache_root = Path(tempdir)

        path = minato.cached_path(
            "https://raw.githubusercontent.com/altescy/minato/main/tests/fixtures/hello.txt",
            cache_root=cache_root,
        )

        assert path.exists()
        assert path.parent == cache_root


def test_cached_path_with_zip_file() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        minato_root = Path(tempdir)
        path = minato.cached_path(
            "https://github.com/altescy/minato/raw/main"
            "/tests/fixtures/archive.zip!archive/foo.txt",
            cache_root=minato_root,
        )

        assert path.exists()
        assert path.is_file()

        with path.open("r") as fp:
            content = fp.read()
        assert content == "this file is foo.txt\n"


def test_cached_path_with_local_tar_file() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        cache_root = Path(tempdir)
        path = minato.cached_path(
            "tests/fixtures/archive.tar.gz!foo.txt",
            cache_root=cache_root,
        )

        assert path.exists()
        assert path.is_file()

        with path.open("r") as fp:
            content = fp.read()
        assert content == "this file is foo.txt\n"


@mock_s3
def test_auto_update() -> None:
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="my_bucket")

    with tempfile.TemporaryDirectory() as tempdir:
        cache_root = Path(tempdir)

        url = "s3://my_bucket/foo"

        with minato.open(url, "w") as fp:
            fp.write("hello")

        minato.cached_path(url, expire_days=10, cache_root=cache_root)

        with minato.open(
            url,
            auto_update=False,
            use_cache=True,
            cache_root=cache_root,
        ) as fp:
            text = fp.read().strip()
            assert text == "hello"

        with minato.open(
            url,
            auto_update=True,
            use_cache=True,
            cache_root=cache_root,
        ) as fp:
            text = fp.read().strip()
            assert text == "hello"

        with minato.open(url, "w") as fp:
            fp.write("world")

        with minato.open(
            url,
            auto_update=False,
            use_cache=True,
            cache_root=cache_root,
        ) as fp:
            text = fp.read().strip()
            assert text == "hello"

        with minato.open(
            url,
            auto_update=True,
            use_cache=True,
            cache_root=cache_root,
        ) as fp:
            text = fp.read().strip()
            assert text == "world"
