from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Optional, Union

from minato.cache import Cache
from minato.config import Config
from minato.filesystems import FileSystem
from minato.minato import Minato

__version__ = "0.3.0"
__all__ = [
    "Cache",
    "Config",
    "FileSystem",
    "Minato",
    "cached_path",
    "download",
    "open",
    "upload",
]


@contextmanager
def open(
    url_or_filename: Union[str, Path],
    mode: str = "r",
    extract: bool = False,
    use_cache: bool = True,
    force_download: bool = False,
    force_extract: bool = False,
    cache_root: Optional[Union[str, Path]] = None,
    expire_days: Optional[int] = None,
) -> Iterator[IO[Any]]:
    config = Config.load(
        cache_root=cache_root,
        expire_days=expire_days,
    )

    with Minato(config).open(
        url_or_filename,
        mode=mode,
        extract=extract,
        use_cache=use_cache,
        force_download=force_download,
        force_extract=force_extract,
    ) as fp:
        yield fp


def cached_path(
    url_or_filename: Union[str, Path],
    extract: bool = False,
    force_download: bool = False,
    force_extract: bool = False,
    retry: bool = True,
    cache_root: Optional[Union[str, Path]] = None,
    expire_days: Optional[int] = None,
) -> Path:
    config = Config.load(
        cache_root=cache_root,
        expire_days=expire_days,
    )

    return Minato(config).cached_path(
        url_or_filename,
        extract=extract,
        force_download=force_download,
        force_extract=force_extract,
        retry=retry,
    )


def download(url: str, filename: Union[str, Path]) -> None:
    filename = Path(filename)
    Minato.download(url, filename)


def upload(filename: Union[str, Path], url: str) -> None:
    filename = Path(filename)
    Minato.upload(filename, url)
