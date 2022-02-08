import os
import tempfile
from contextlib import contextmanager
from os import PathLike
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

import requests

from minato.filesystems.filesystem import FileSystem
from minato.util import OpenBinaryMode, OpenTextMode, _session_with_backoff, http_get


@FileSystem.register(["http", "https"])
class HttpFileSystem(FileSystem):
    def exists(self) -> bool:
        response = requests.head(self._url.raw, allow_redirects=True)
        status_code = response.status_code
        return status_code == 200

    def download(self, path: Union[str, PathLike]) -> None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        with open(path, "w+b") as fp:
            http_get(self._url.raw, fp)

    def delete(self) -> None:
        raise OSError("HttpFileSystem cannot delete files or directories.")

    def get_version(self) -> Optional[str]:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        with _session_with_backoff() as session:
            response = session.head(self._url.raw, allow_redirects=True)
        if response.status_code != 200:
            raise OSError(
                "HEAD request failed for url {} with status code {}".format(
                    self._url.raw, response.status_code
                )
            )
        return response.headers.get("ETag")

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
            if not self.exists():
                raise FileNotFoundError(self._url.raw)

            if "a" in mode or "w" in mode or "+" in mode or "x" in mode:
                raise ValueError("HttpFileSystem is not writable.")

            temp_file = tempfile.NamedTemporaryFile(delete=False)
            try:
                http_get(self._url.raw, temp_file)
                temp_file.close()
                with open(
                    temp_file.name,
                    mode=mode,
                    encoding=encoding,
                    buffering=buffering,
                    errors=errors,
                    newline=newline,
                ) as fp:
                    yield fp
            finally:
                os.remove(temp_file.name)

        return _open(mode, buffering, encoding, errors, newline)
