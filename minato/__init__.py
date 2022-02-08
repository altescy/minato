from importlib.metadata import version
from os import PathLike
from pathlib import Path
from typing import IO, Any, BinaryIO, ContextManager, Optional, TextIO, Union, overload

from minato.cache import Cache
from minato.config import Config
from minato.filesystems import FileSystem
from minato.minato import Minato
from minato.util import OpenBinaryMode, OpenTextMode

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
    url_or_filename: Union[str, PathLike],
    mode: OpenTextMode = ...,
    buffering: int = ...,
    encoding: Optional[str] = ...,
    errors: Optional[str] = ...,
    newline: Optional[str] = ...,
    *,
    extract: bool = ...,
    auto_update: Optional[bool] = ...,
    use_cache: bool = ...,
    force_download: bool = ...,
    force_extract: bool = ...,
    cache_root: Optional[Union[str, PathLike]] = ...,
    expire_days: Optional[int] = ...,
    retry: bool = ...,
) -> ContextManager[TextIO]:
    ...


@overload
def open(
    url_or_filename: Union[str, PathLike],
    mode: OpenBinaryMode,
    buffering: int = ...,
    encoding: Optional[str] = ...,
    errors: Optional[str] = ...,
    newline: Optional[str] = ...,
    *,
    extract: bool = ...,
    auto_update: Optional[bool] = ...,
    use_cache: bool = ...,
    force_download: bool = ...,
    force_extract: bool = ...,
    cache_root: Optional[Union[str, PathLike]] = ...,
    expire_days: Optional[int] = ...,
    retry: bool = ...,
) -> ContextManager[BinaryIO]:
    ...


@overload
def open(
    url_or_filename: Union[str, PathLike],
    mode: str,
    buffering: int = ...,
    encoding: Optional[str] = ...,
    errors: Optional[str] = ...,
    newline: Optional[str] = ...,
    *,
    extract: bool = ...,
    auto_update: Optional[bool] = ...,
    use_cache: bool = ...,
    force_download: bool = ...,
    force_extract: bool = ...,
    cache_root: Optional[Union[str, PathLike]] = ...,
    expire_days: Optional[int] = ...,
    retry: bool = ...,
) -> ContextManager[IO[Any]]:
    ...


def open(
    url_or_filename: Union[str, PathLike],
    mode: str = "r",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    *,
    extract: bool = False,
    auto_update: Optional[bool] = None,
    use_cache: bool = True,
    force_download: bool = False,
    force_extract: bool = False,
    cache_root: Optional[Union[str, PathLike]] = None,
    expire_days: Optional[int] = None,
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
        auto_update=auto_update,
        use_cache=use_cache,
        force_download=force_download,
        force_extract=force_extract,
        expire_days=expire_days,
        retry=retry,
    )


def cached_path(
    url_or_filename: Union[str, PathLike],
    extract: bool = False,
    auto_update: Optional[bool] = None,
    force_download: bool = False,
    force_extract: bool = False,
    retry: bool = True,
    cache_root: Optional[Union[str, PathLike]] = None,
    expire_days: Optional[int] = None,
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


def download(url: str, filename: Union[str, PathLike]) -> None:
    filename = Path(filename)
    Minato.download(url, filename)


def upload(filename: Union[str, PathLike], url: str) -> None:
    filename = Path(filename)
    Minato.upload(filename, url)


def delete(url_or_filename: Union[str, PathLike]) -> None:
    Minato.delete(url_or_filename)


def exists(url_or_filename: Union[str, PathLike]) -> bool:
    return Minato.exists(url_or_filename)
