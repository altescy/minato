from __future__ import annotations

import http.client
import os
import tempfile
import time
import urllib.parse
from contextlib import contextmanager
from os import PathLike
from typing import IO, Any, BinaryIO, Callable, ContextManager, Iterator, TextIO, overload

from minato.common import Progress
from minato.filesystems.filesystem import FileSystem
from minato.util import DecompressOption, OpenBinaryMode, OpenTextMode, sizeof_fmt, xopen


@FileSystem.register(["http", "https"])
class HttpFileSystem(FileSystem):
    def exists(self) -> bool:
        with self.http_head(self._url.raw, allow_redirects=True) as response:
            return response.status == 200

    def download(self, path: str | PathLike) -> None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        with open(path, "w+b") as fp:
            HttpFileSystem.http_get(self._url.raw, fp, allow_redirects=True)

    def delete(self) -> None:
        raise OSError("HttpFileSystem cannot delete files or directories.")

    def get_version(self) -> str | None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        with HttpFileSystem.http_head(self._url.raw, allow_redirects=True) as response:
            if response.status != 200:
                raise OSError(
                    "HEAD request failed for url {} with status code {}".format(self._url.raw, response.status)
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
        ) -> Iterator[IO[Any]]:
            if not self.exists():
                raise FileNotFoundError(self._url.raw)

            if "a" in mode or "w" in mode or "+" in mode or "x" in mode:
                raise ValueError("HttpFileSystem is not writable.")

            suffix = "-" + (self._url.path.split("/")[-1] if self._url.path else "")
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)

            try:
                HttpFileSystem.http_get(self._url.raw, temp_file, allow_redirects=True)
                temp_file.close()
                with xopen(
                    temp_file.name,
                    mode=mode,
                    encoding=encoding,
                    buffering=buffering,
                    errors=errors,
                    newline=newline,
                    decompress=decompress,
                ) as fp:
                    yield fp
            finally:
                os.remove(temp_file.name)

        return _open(mode, buffering, encoding, errors, newline)

    @staticmethod
    def http_get(
        url: str,
        temp_file: IO[Any],
        *,
        retries: int = 5,
        allow_redirects: bool = False,
    ) -> None:
        def get_response(connection: http.client.HTTPConnection, url: str) -> http.client.HTTPResponse:
            connection.request("GET", url)
            return connection.getresponse()

        url_parsed = urllib.parse.urlparse(url)
        with HttpFileSystem.http_request_with_retry(
            url_parsed,
            get_response,
            retries=retries,
            allow_redirects=allow_redirects,
        ) as response:
            content_length = response.headers.get("Content-Length")
            total = int(content_length) if content_length is not None else None

            with Progress[int](
                total,
                unit="iB",
                desc="downloading",
                sizeof_formatter=sizeof_fmt,
            ) as progress:
                while True:
                    chunk = response.read(1024)
                    if not chunk:
                        break
                    progress.update(len(chunk))
                    temp_file.write(chunk)

    @staticmethod
    def http_head(
        url: str,
        *,
        retries: int = 5,
        allow_redirects: bool = False,
    ) -> http.client.HTTPResponse:
        def get_response(connection: http.client.HTTPConnection, url: str) -> http.client.HTTPResponse:
            connection.request("HEAD", url)
            return connection.getresponse()

        url_parsed = urllib.parse.urlparse(url)
        response = HttpFileSystem.http_request_with_retry(
            url_parsed,
            get_response,
            retries=retries,
            allow_redirects=allow_redirects,
        )

        return response

    @staticmethod
    def http_request_with_retry(
        url_parsed: urllib.parse.ParseResult,
        get_response: Callable[[http.client.HTTPConnection, str], http.client.HTTPResponse],
        retries: int = 5,
        backoff_factor: int = 1,
        allow_redirects: bool = False,
    ) -> http.client.HTTPResponse:
        status_codes_to_retry = {502, 503, 504}
        status_codes_to_redirect = {300, 301, 302, 303, 307, 308}

        for retry in range(retries):
            try:
                connection = (
                    http.client.HTTPConnection(url_parsed.netloc)
                    if url_parsed.scheme == "http"
                    else http.client.HTTPSConnection(url_parsed.netloc)
                )
                path_with_query = url_parsed.path + ("?" + url_parsed.query if url_parsed.query else "")
                response = get_response(connection, path_with_query)

                if allow_redirects and response.status in status_codes_to_redirect:
                    redirect_url = response.headers.get("Location")
                    if redirect_url:
                        url_parsed = urllib.parse.urlparse(redirect_url)
                        continue
                elif response.status != 200:
                    if response.status in status_codes_to_retry and retry < retries - 1:
                        time.sleep(backoff_factor * (2**retry))
                        continue
                    else:
                        raise http.client.HTTPException(f"Request failed with status {response.status}")

                break
            except Exception as e:
                if retry < retries - 1:
                    time.sleep(backoff_factor * (2**retry))
                else:
                    raise e

        return response
