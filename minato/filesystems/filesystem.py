from __future__ import annotations

import logging
from collections.abc import Callable
from os import PathLike
from typing import IO, Any, BinaryIO, ContextManager, TextIO, Type, overload
from urllib.parse import urlparse

from minato.common import URL
from minato.util import DecompressOption, OpenBinaryMode, OpenTextMode

logger = logging.getLogger(__name__)


@overload
def open_file(
    url_or_filename: str | PathLike,
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
    url_or_filename: str | PathLike,
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
    url_or_filename: str | PathLike,
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
    url_or_filename: str | PathLike,
    mode: str = "r",
    buffering: int = -1,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    *,
    decompress: DecompressOption = "none",
) -> ContextManager[IO[Any]]:
    url = str(url_or_filename)
    filesystem = FileSystem.by_url(url)
    return filesystem.open_file(
        mode=mode,
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
        decompress=decompress,
    )


def exists(url_or_filename: str | PathLike) -> bool:
    url = str(url_or_filename)
    filesystem = FileSystem.by_url(url)
    return filesystem.exists()


def download(
    url_or_filename: str | PathLike,
    download_path: str | PathLike,
) -> None:
    url = str(url_or_filename)
    filesystem = FileSystem.by_url(url)
    filesystem.download(download_path)


def upload(source: str | PathLike, target: str | PathLike) -> None:
    target = str(target)
    filesystem = FileSystem.by_url(target)
    filesystem.upload(source)


def delete(url_or_filename: str | PathLike) -> None:
    url = str(url_or_filename)
    filesystem = FileSystem.by_url(url)
    filesystem.delete()


def get_version(
    url_or_filename: str | PathLike,
) -> str | None:
    url = str(url_or_filename)
    filesystem = FileSystem.by_url(url)
    return filesystem.get_version()


class FileSystem:
    registry: dict[str, Type["FileSystem"]] = {}

    @classmethod
    def register(cls, schemes: list[str]) -> Callable[[Type[FileSystem]], Type[FileSystem]]:
        def decorator(subclass: Type[FileSystem]) -> Type[FileSystem]:
            for scheme in schemes:
                FileSystem.registry[scheme] = subclass
            return subclass

        return decorator

    @classmethod
    def by_url(cls, url: str) -> FileSystem:
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme
        if scheme not in FileSystem.registry:
            schemes = ", ".join(FileSystem.registry.keys())
            raise KeyError(f"Invalid scheme: {scheme} (not in {schemes})")
        subclass = FileSystem.registry[scheme]
        logger.debug("Infer file system of %s from url: %s", subclass.__name__, url)
        return subclass(url)

    def __init__(self, url_or_filename: str | PathLike) -> None:
        self._url = URL(str(url_or_filename))

    def exists(self) -> bool:
        raise NotImplementedError

    def download(self, path: str | PathLike) -> None:
        raise NotImplementedError

    def upload(self, path: str | PathLike) -> None:
        raise NotImplementedError

    def delete(self) -> None:
        raise NotImplementedError

    def get_version(self) -> str | None:
        raise NotImplementedError

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
        raise NotImplementedError
