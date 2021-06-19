from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Union
from urllib.parse import urlparse

from minato.filesystems.filesystem import FileSystem


@FileSystem.register(["file", "osfs", ""])
class OSFileSystem(FileSystem):
    def __init__(self, url_or_filename: Union[str, Path]) -> None:
        super().__init__(url_or_filename)

        parse_result = urlparse(str(self._url_or_filename))
        self._path = Path(parse_result.path)

    @contextmanager
    def open_file(
        self,
        mode: str = "r",
    ) -> Iterator[IO[Any]]:
        with self._path.open(mode) as fp:
            yield fp
