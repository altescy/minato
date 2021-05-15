from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Union

from minato.minato import Minato

__version__ = "0.1.0"
__all__ = ["Minato", "cached_path", "download", "open"]


@contextmanager
def open(
    url_or_filename: Union[str, Path],
    mode: str = "r",
) -> Iterator[IO[Any]]:
    with Minato().open(url_or_filename, mode) as fp:
        yield fp


def cached_path(
    url_or_filename: Union[str, Path],
) -> Path:
    return Minato().cached_path(url_or_filename)


def download(url: str, filename: Union[str, Path]) -> None:
    filename = Path(filename)
    Minato().download(url, filename)
