from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Optional, Union

from minato.minato import Minato

__version__ = "0.2.0"
__all__ = ["Minato", "cached_path", "download", "open", "upload"]


@contextmanager
def open(
    url_or_filename: Union[str, Path],
    mode: str = "r",
    extract: bool = False,
    use_cache: bool = True,
    update: bool = False,
    minato_root: Optional[Path] = None,
) -> Iterator[IO[Any]]:
    if minato_root is not None:
        minato_root = Path(minato_root)

    with Minato(minato_root).open(
        url_or_filename,
        mode=mode,
        extract=extract,
        use_cache=use_cache,
        update=update,
    ) as fp:
        yield fp


def cached_path(
    url_or_filename: Union[str, Path],
    extract: bool = False,
    update: bool = False,
    minato_root: Optional[Path] = None,
) -> Path:
    if minato_root is not None:
        minato_root = Path(minato_root)

    return Minato(minato_root).cached_path(
        url_or_filename,
        extract=extract,
        update=update,
    )


def download(url: str, filename: Union[str, Path]) -> None:
    filename = Path(filename)
    Minato.download(url, filename)


def upload(filename: Union[str, Path], url: str) -> None:
    filename = Path(filename)
    Minato.upload(filename, url)
