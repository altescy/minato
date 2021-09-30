import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Optional, Union

from minato.filesystems.filesystem import FileSystem
from minato.util import remove_file_or_directory


@FileSystem.register(["file", "osfs", ""])
class OSFileSystem(FileSystem):
    def __init__(self, url_or_filename: Union[str, Path]) -> None:
        super().__init__(url_or_filename)
        self._path = Path(self._url.path)

    def exists(self) -> bool:
        return self._path.exists()

    def download(self, path: Union[str, Path]) -> None:
        shutil.copy(self._path, path)

    def delete(self) -> None:
        if not self._path.exists():
            raise FileNotFoundError(self._path)
        remove_file_or_directory(self._path)

    def get_version(self) -> Optional[str]:
        return None

    @contextmanager
    def open_file(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> Iterator[IO[Any]]:
        with self._path.open(
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        ) as fp:
            yield fp
