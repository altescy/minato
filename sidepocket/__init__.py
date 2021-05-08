from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Union

from sidepocket.sidepocket import SidePocket

__version__ = "0.1.0"


@contextmanager
def open(
    url_or_filename: Union[str, Path],
    mode: str = "r",
) -> Iterator[IO[Any]]:
    with SidePocket().open(url_or_filename, mode) as fp:
        yield fp


def cached_path(
    url_or_filename: Union[str, Path],
) -> Path:
    return SidePocket().cached_path(url_or_filename)
