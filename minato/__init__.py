from __future__ import annotations

from importlib.metadata import version
from os import PathLike
from pathlib import Path
from typing import IO, Any, BinaryIO, ContextManager, TextIO, overload

from minato.cache import Cache
from minato.config import Config
from minato.filesystems import FileSystem
from minato.minato import Minato
from minato.util import DecompressOption, OpenBinaryMode, OpenTextMode

__version__ = version("minato")
__all__ = [
    "Cache",
    "Config",
    "FileSystem",
    "Minato",
    "cached_path",
    "download",
    "exists",
    "open",
    "upload",
]


@overload
def open(
    url_or_filename: str | PathLike,
    mode: OpenTextMode = ...,
    buffering: int = ...,
    encoding: str | None = ...,
    errors: str | None = ...,
    newline: str | None = ...,
    *,
    extract: bool = ...,
    decompress: DecompressOption = ...,
    auto_update: bool | None = ...,
    use_cache: bool = ...,
    force_download: bool = ...,
    force_extract: bool = ...,
    cache_root: str | PathLike | None = ...,
    expire_days: int | None = ...,
    retry: bool = ...,
) -> ContextManager[TextIO]:
    ...


@overload
def open(
    url_or_filename: str | PathLike,
    mode: OpenBinaryMode,
    buffering: int = ...,
    encoding: str | None = ...,
    errors: str | None = ...,
    newline: str | None = ...,
    *,
    extract: bool = ...,
    decompress: DecompressOption = ...,
    auto_update: bool | None = ...,
    use_cache: bool = ...,
    force_download: bool = ...,
    force_extract: bool = ...,
    cache_root: str | PathLike | None = ...,
    expire_days: int | None = ...,
    retry: bool = ...,
) -> ContextManager[BinaryIO]:
    ...


@overload
def open(
    url_or_filename: str | PathLike,
    mode: str,
    buffering: int = ...,
    encoding: str | None = ...,
    errors: str | None = ...,
    newline: str | None = ...,
    *,
    extract: bool = ...,
    decompress: DecompressOption = ...,
    auto_update: bool | None = ...,
    use_cache: bool = ...,
    force_download: bool = ...,
    force_extract: bool = ...,
    cache_root: str | PathLike | None = ...,
    expire_days: int | None = ...,
    retry: bool = ...,
) -> ContextManager[IO[Any]]:
    ...


def open(
    url_or_filename: str | PathLike,
    mode: str = "r",
    buffering: int = -1,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    *,
    extract: bool = False,
    decompress: DecompressOption = "none",
    auto_update: bool | None = None,
    use_cache: bool = True,
    force_download: bool = False,
    force_extract: bool = False,
    cache_root: str | PathLike | None = None,
    expire_days: int | None = None,
    retry: bool = True,
) -> ContextManager[IO[Any]]:
    config = Config.load(
        cache_root=cache_root,
    )

    return Minato(config).open(
        url_or_filename,
        mode=mode,
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
        extract=extract,
        decompress=decompress,
        auto_update=auto_update,
        use_cache=use_cache,
        force_download=force_download,
        force_extract=force_extract,
        expire_days=expire_days,
        retry=retry,
    )


def cached_path(
    url_or_filename: str | PathLike,
    extract: bool = False,
    auto_update: bool | None = None,
    force_download: bool = False,
    force_extract: bool = False,
    retry: bool = True,
    cache_root: str | PathLike | None = None,
    expire_days: int | None = None,
) -> Path:
    config = Config.load(
        cache_root=cache_root,
    )

    return Minato(config).cached_path(
        url_or_filename,
        extract=extract,
        auto_update=auto_update,
        force_download=force_download,
        force_extract=force_extract,
        expire_days=expire_days,
        retry=retry,
    )


def download(url: str, filename: str | PathLike) -> None:
    filename = Path(filename)
    Minato.download(url, filename)


def upload(filename: str | PathLike, url: str) -> None:
    filename = Path(filename)
    Minato.upload(filename, url)


def delete(url_or_filename: str | PathLike) -> None:
    Minato.delete(url_or_filename)


def exists(url_or_filename: str | PathLike) -> bool:
    return Minato.exists(url_or_filename)
