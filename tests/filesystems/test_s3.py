import tempfile
from pathlib import Path

import boto3
from moto import mock_s3

from minato.filesystems import S3FileSystem


@mock_s3
def test_open_file() -> None:
    url = "s3://my_bucket/path/to/file"

    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="my_bucket")

    fs = S3FileSystem(url)
    with fs.open_file("w") as fp:
        fp.write("Hello, world!")

    with fs.open_file("r") as fp:
        text = fp.read()
        assert text == "Hello, world!"


@mock_s3
def test_download_file() -> None:
    url = "s3://my_bucket/path/to/file"

    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="my_bucket")

    with S3FileSystem("s3://my_bucket/path/to/file").open_file("w") as fp:
        fp.write("file")

    with tempfile.TemporaryDirectory() as _tempdir:
        tempdir = Path(_tempdir)

        fs = S3FileSystem(url)
        fs.download(tempdir)

        assert (tempdir / "file").is_file()


@mock_s3
def test_download_dir_with_trailing_slash() -> None:
    url = "s3://my_bucket/path/to/dir/"

    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="my_bucket")

    with S3FileSystem("s3://my_bucket/path/to/dir/foo.txt").open_file("w") as fp:
        fp.write("foo")

    with S3FileSystem("s3://my_bucket/path/to/dir/bar/bar.txt").open_file("w") as fp:
        fp.write("bar")

    with tempfile.TemporaryDirectory() as _tempdir:
        tempdir = Path(_tempdir)

        fs = S3FileSystem(url)
        fs.download(tempdir)

        assert (tempdir / "foo.txt").is_file()
        assert (tempdir / "bar").is_dir()
        assert (tempdir / "bar" / "bar.txt").is_file()


@mock_s3
def test_download_dir_without_trailing_slash() -> None:
    url = "s3://my_bucket/path/to/dir"

    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="my_bucket")

    with S3FileSystem("s3://my_bucket/path/to/dir/foo.txt").open_file("w") as fp:
        fp.write("foo")

    with S3FileSystem("s3://my_bucket/path/to/dir/bar/bar.txt").open_file("w") as fp:
        fp.write("bar")

    with tempfile.TemporaryDirectory() as _tempdir:
        tempdir = Path(_tempdir)

        fs = S3FileSystem(url)
        fs.download(tempdir)

        assert (tempdir / "dir" / "foo.txt").is_file()
        assert (tempdir / "dir" / "bar").is_dir()
        assert (tempdir / "dir" / "bar" / "bar.txt").is_file()


@mock_s3
def test_exists() -> None:
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="my_bucket")

    with S3FileSystem("s3://my_bucket/path/to/dir/foo.txt").open_file("w") as fp:
        fp.write("foo")

    assert S3FileSystem("s3://my_bucket/path/to/dir").exists()
    assert S3FileSystem("s3://my_bucket/path/to/dir/foo.txt").exists()
    assert not S3FileSystem("s3://my_bucket/path/to/dir/bar.txt").exists()


@mock_s3
def test_delete() -> None:
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="my_bucket")

    with S3FileSystem("s3://my_bucket/path/to/dir/foo.txt").open_file("w") as fp:
        fp.write("foo")

    fs = S3FileSystem("s3://my_bucket/path/to/dir")
    assert fs.exists()

    fs.delete()

    assert not fs.exists()


@mock_s3
def test_get_version() -> None:
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="my_bucket")

    with S3FileSystem("s3://my_bucket//dir/foo.txt").open_file("w") as fp:
        fp.write("foo")
    with S3FileSystem("s3://my_bucket//dir/bar.txt").open_file("w") as fp:
        fp.write("bar")

    fs = S3FileSystem("s3://my_bucket//dir")
    version = fs.get_version()

    assert version is not None
    assert len(version.split(".")) == 2


@mock_s3
def test_update_version() -> None:
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="my_bucket")

    fs = S3FileSystem("s3://my_bucket//dir/foo.txt")

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


@mock_s3
def test_upload_file(tmpdir: Path) -> None:
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="my_bucket")

    filename = tmpdir / "foo.txt"
    with filename.open("w") as localfile:
        localfile.write("this is foo!")

    fs = S3FileSystem("s3://my_bucket/dir/foo.txt")
    assert not fs.exists()

    fs.upload(filename)
    assert fs.exists()

    with fs.open_file("r") as remotefile:
        content = remotefile.read()
    assert content == "this is foo!"


@mock_s3
def test_upload_file_to_dir(tmpdir: Path) -> None:
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="my_bucket")

    filename = tmpdir / "foo.txt"
    with filename.open("w") as localfile:
        localfile.write("this is foo!")

    dirfs = S3FileSystem("s3://my_bucket/dir/")
    assert not dirfs.exists()

    dirfs.upload(filename)

    filefs = S3FileSystem("s3://my_bucket/dir/foo.txt")
    assert filefs.exists()

    with filefs.open_file("r") as remotefile:
        content = remotefile.read()
    assert content == "this is foo!"


@mock_s3
def test_upload_dir(tmpdir: Path) -> None:
    workdir = tmpdir / "work"
    workdir.mkdir()

    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="my_bucket")

    with open(workdir / "foo.txt", "w") as localfile:
        localfile.write("this is foo!")
    with open(workdir / "bar.txt", "w") as localfile:
        localfile.write("this is bar!")

    dirfs = S3FileSystem("s3://my_bucket/dir")
    assert not dirfs.exists()

    dirfs.upload(workdir)

    foofs = S3FileSystem("s3://my_bucket/dir/foo.txt")
    barfs = S3FileSystem("s3://my_bucket/dir/bar.txt")
    assert foofs.exists()
    assert barfs.exists()

    with foofs.open_file("r") as remotefile:
        content = remotefile.read()
    assert content == "this is foo!"

    with barfs.open_file("r") as remotefile:
        content = remotefile.read()
    assert content == "this is bar!"


@mock_s3
def test_upload_dir_to_dir(tmpdir: Path) -> None:
    workdir = tmpdir / "work"
    workdir.mkdir()

    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="my_bucket")

    with open(workdir / "foo.txt", "w") as localfile:
        localfile.write("this is foo!")
    with open(workdir / "bar.txt", "w") as localfile:
        localfile.write("this is bar!")

    dirfs = S3FileSystem("s3://my_bucket/dir/")
    assert not dirfs.exists()

    dirfs.upload(workdir)

    foofs = S3FileSystem("s3://my_bucket/dir/work/foo.txt")
    barfs = S3FileSystem("s3://my_bucket/dir/work/bar.txt")
    assert foofs.exists()
    assert barfs.exists()

    with foofs.open_file("r") as remotefile:
        content = remotefile.read()
    assert content == "this is foo!"

    with barfs.open_file("r") as remotefile:
        content = remotefile.read()
    assert content == "this is bar!"
