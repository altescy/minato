from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from os import PathLike
from typing import IO, Any, BinaryIO, ContextManager, Iterator, TextIO, overload

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from minato.common import Progress
from minato.filesystems.filesystem import FileSystem
from minato.util import OpenBinaryMode, OpenTextMode, sizeof_fmt


@FileSystem.register(["http", "https"])
class HttpFileSystem(FileSystem):
    def exists(self) -> bool:
        response = requests.head(self._url.raw, allow_redirects=True)
        status_code = response.status_code
        return status_code == 200

    def download(self, path: str | PathLike) -> None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        with open(path, "w+b") as fp:
            HttpFileSystem.http_get(self._url.raw, fp)

    def delete(self) -> None:
        raise OSError("HttpFileSystem cannot delete files or directories.")

    def get_version(self) -> str | None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        with HttpFileSystem._session_with_backoff() as session:
            response = session.head(self._url.raw, allow_redirects=True)
        if response.status_code != 200:
            raise OSError(
                "HEAD request failed for url {} with status code {}".format(self._url.raw, response.status_code)
            )
        return response.headers.get("ETag")

    @overload
    def open_file(
        self,
        mode: OpenTextMode = ...,
        buffering: int = ...,
        encoding: str | None = ...,
        errors: str | None = ...,
        newline: str | None = ...,
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
    ) -> ContextManager[IO[Any]]:
        ...

    def open_file(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> ContextManager[IO[Any]]:
        @contextmanager
        def _open(
            mode: str,
            buffering: int,
            encoding: str | None,
            errors: str | None,
            newline: str | None,
        ) -> Iterator[IO[Any]]:
            if not self.exists():
                raise FileNotFoundError(self._url.raw)

            if "a" in mode or "w" in mode or "+" in mode or "x" in mode:
                raise ValueError("HttpFileSystem is not writable.")

            temp_file = tempfile.NamedTemporaryFile(delete=False)
            try:
                HttpFileSystem.http_get(self._url.raw, temp_file)
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

    @staticmethod
    def http_get(url: str, temp_file: IO[Any]) -> None:
        with HttpFileSystem._session_with_backoff() as session:
            req = session.get(url, stream=True)
            req.raise_for_status()
            content_length = req.headers.get("Content-Length")
            total = int(content_length) if content_length is not None else None
            with Progress[int](
                total,
                unit="iB",
                desc="downloading",
                sizeof_formatter=sizeof_fmt,
            ) as progress:
                for chunk in req.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        progress.update(len(chunk))
                        temp_file.write(chunk)

    @staticmethod
    def _session_with_backoff() -> requests.Session:
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        session.mount("http://", HTTPAdapter(max_retries=retries))
        session.mount("https://", HTTPAdapter(max_retries=retries))

        return session
