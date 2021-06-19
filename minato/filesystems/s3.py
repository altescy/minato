import os
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Union

from fs.opener.parse import parse_fs_url
from fs_s3fs import S3FS

from minato.filesystems.filesystem import FileSystem
from minato.util import get_parent_path_and_filename


@FileSystem.register(["s3"])
class S3FileSystem(FileSystem):
    def __init__(self, url_or_filename: Union[str, Path]) -> None:
        super().__init__(url_or_filename)
        self._url = str(url_or_filename)

    @contextmanager
    def open_file(
        self,
        mode: str = "r",
    ) -> Iterator[IO[Any]]:
        parsed_url = parse_fs_url(self._url)
        bucket_name, _, path = parsed_url.resource.partition("/")
        dir_path, s3_filename = get_parent_path_and_filename(path)

        aws_access_key_id = parsed_url.username or os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = parsed_url.password or os.environ.get(
            "AWS_SECRET_ACCESS_KEY"
        )
        endpoint_url = parsed_url.params.get("endpoint_url")

        with S3FS(
            bucket_name,
            dir_path=dir_path or "/",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url,
        ) as s3fs:
            with s3fs.open(s3_filename, mode) as fp:
                yield fp
