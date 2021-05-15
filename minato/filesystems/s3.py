from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Union
from urllib.parse import urlparse

from fs_s3fs import S3FS

from minato.filesystems.filesystem import FileSystem


@FileSystem.register(["s3"])
class S3FileSystem(FileSystem):
    @contextmanager
    def open(
        self,
        filename: Union[str, Path],
        mode: str = "r",
    ) -> Iterator[IO[Any]]:
        url = str(filename)

        parsed_url = urlparse(url)
        path = parsed_url.path  # assume '<bucket>/<path>/<to>/<filename>'

        splitted_path = path.split("/")

        if len(splitted_path) < 2:
            raise ValueError(f"Invalid URL: {url}")

        bucket_name = splitted_path.pop(0)
        s3_filename = splitted_path.pop(-1)
        dir_path = "/".join(splitted_path) if splitted_path else "/"

        with S3FS(bucket_name, dir_path=dir_path) as s3fs:
            with s3fs.open(s3_filename) as fp:
                yield fp
