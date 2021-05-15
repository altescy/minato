from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Union

from minato.filesystems.filesystem import FileSystem


@FileSystem.register(["file", "osfs", ""])
class OSFileSystem(FileSystem):
    @contextmanager
    def open(
        self,
        filename: Union[str, Path],
        mode: str = "r",
    ) -> Iterator[IO[Any]]:
        with open(filename, mode) as fp:
            yield fp
