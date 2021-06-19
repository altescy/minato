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
