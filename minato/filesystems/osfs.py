import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Union

from minato.filesystems.filesystem import FileSystem


@FileSystem.register(["file", "osfs", ""])
class OSFileSystem(FileSystem):
    def __init__(self, url_or_filename: Union[str, Path]) -> None:
        super().__init__(url_or_filename)
        self._path = Path(self._url.path)

    def exists(self) -> bool:
        return self._path.exists()

    def download(self, path: Union[str, Path]) -> None:
        shutil.copy(self._path, path)

    @contextmanager
    def open_file(
        self,
        mode: str = "r",
    ) -> Iterator[IO[Any]]:
        with self._path.open(mode) as fp:
            yield fp
