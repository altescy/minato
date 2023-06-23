from __future__ import annotations

import shutil
from contextlib import contextmanager
from os import PathLike
from pathlib import Path
from typing import IO, Any, BinaryIO, ContextManager, Iterator, TextIO, overload

from minato.filesystems.filesystem import FileSystem
from minato.util import DecompressOption, OpenBinaryMode, OpenTextMode, remove_file_or_directory, xopen


@FileSystem.register(["file", "osfs", ""])
class OSFileSystem(FileSystem):
    def __init__(self, url_or_filename: str | PathLike) -> None:
        super().__init__(url_or_filename)
        self._path = Path(self._url.path)

    def exists(self) -> bool:
        return self._path.exists()

    def download(self, path: str | PathLike) -> None:
        shutil.copy(self._path, path)

    def delete(self) -> None:
        if not self._path.exists():
            raise FileNotFoundError(self._path)
        remove_file_or_directory(self._path)

    def get_version(self) -> str | None:
        return str(self._path.stat().st_mtime_ns)

    @overload
    def open_file(
        self,
        mode: OpenTextMode = ...,
        buffering: int = ...,
        encoding: str | None = ...,
        errors: str | None = ...,
        newline: str | None = ...,
        *,
        decompress: DecompressOption = ...,
    ) -> ContextManager[TextIO]:
        ...

    @overload
    def open_file(
        self,
        mode: OpenBinaryMode,
        buffering: int = ...,
        encoding: str | None = ...,
        errors: str | None = ...,
        newline: str | None = ...,
        *,
        decompress: DecompressOption = ...,
    ) -> ContextManager[BinaryIO]:
        ...

    @overload
    def open_file(
        self,
        mode: str,
        buffering: int = ...,
        encoding: str | None = ...,
        errors: str | None = ...,
        newline: str | None = ...,
        *,
        decompress: DecompressOption = ...,
    ) -> ContextManager[IO[Any]]:
        ...

    def open_file(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        *,
        decompress: DecompressOption = "none",
    ) -> ContextManager[IO[Any]]:
        @contextmanager
        def _open(
            mode: str,
            buffering: int,
            encoding: str | None,
            errors: str | None,
            newline: str | None,
            decompress: DecompressOption,
        ) -> Iterator[IO[Any]]:
            with xopen(
                self._path,
                mode=mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
                decompress=decompress,
            ) as fp:
                yield fp

        return _open(mode, buffering, encoding, errors, newline, decompress)
