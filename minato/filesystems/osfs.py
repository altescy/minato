import shutil
from contextlib import contextmanager
from os import PathLike
from pathlib import Path
from typing import (
    IO,
    Any,
    BinaryIO,
    ContextManager,
    Iterator,
    Optional,
    TextIO,
    Union,
    overload,
)

from minato.filesystems.filesystem import FileSystem
from minato.util import OpenBinaryMode, OpenTextMode, remove_file_or_directory


@FileSystem.register(["file", "osfs", ""])
class OSFileSystem(FileSystem):
    def __init__(self, url_or_filename: Union[str, PathLike]) -> None:
        super().__init__(url_or_filename)
        self._path = Path(self._url.path)

    def exists(self) -> bool:
        return self._path.exists()

    def download(self, path: Union[str, PathLike]) -> None:
        shutil.copy(self._path, path)

    def delete(self) -> None:
        if not self._path.exists():
            raise FileNotFoundError(self._path)
        remove_file_or_directory(self._path)

    def get_version(self) -> Optional[str]:
        return None

    @overload
    def open_file(
        self,
        mode: OpenTextMode = ...,
        buffering: int = ...,
        encoding: Optional[str] = ...,
        errors: Optional[str] = ...,
        newline: Optional[str] = ...,
    ) -> ContextManager[TextIO]:
        ...

    @overload
    def open_file(
        self,
        mode: OpenBinaryMode,
        buffering: int = ...,
        encoding: Optional[str] = ...,
        errors: Optional[str] = ...,
        newline: Optional[str] = ...,
    ) -> ContextManager[BinaryIO]:
        ...

    @overload
    def open_file(
        self,
        mode: str,
        buffering: int = ...,
        encoding: Optional[str] = ...,
        errors: Optional[str] = ...,
        newline: Optional[str] = ...,
    ) -> ContextManager[IO[Any]]:
        ...

    def open_file(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> ContextManager[IO[Any]]:
        @contextmanager
        def _open(
            mode: str,
            buffering: int,
            encoding: Optional[str],
            errors: Optional[str],
            newline: Optional[str],
        ) -> Iterator[IO[Any]]:
            with self._path.open(
                mode=mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
            ) as fp:
                yield fp

        return _open(mode, buffering, encoding, errors, newline)
