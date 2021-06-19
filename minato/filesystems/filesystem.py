from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Callable, Dict, Iterator, List, Type, Union
from urllib.parse import urlparse


@contextmanager
def open_file(
    url_or_filename: Union[str, Path],
    mode: str = "r",
) -> Iterator[IO[Any]]:

    url = str(url_or_filename)
    filesystem = FileSystem.by_url(url)
    with filesystem.open_file(mode) as fp:
        yield fp


class FileSystem:
    registry: Dict[str, Type["FileSystem"]] = {}

    @classmethod
    def register(
        cls, schemes: List[str]
    ) -> Callable[[Type[FileSystem]], Type[FileSystem]]:
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
        return subclass(url)

    def __init__(self, url_or_filename: Union[str, Path]) -> None:
        self._url_or_filename = url_or_filename

    @contextmanager
    def open_file(self, mode: str = "r") -> Iterator[IO[Any]]:
        raise NotImplementedError
