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
    filesystem = FileSystem.by_url(url)()
    with filesystem.open(url_or_filename, mode) as fp:
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
    def by_scheme(cls, scheme: str) -> Type[FileSystem]:
        if scheme not in FileSystem.registry:
            schemes = ", ".join(FileSystem.registry.keys())
            raise KeyError(f"Invalid scheme: {scheme} (not in {schemes})")
        return FileSystem.registry[scheme]

    @classmethod
    def by_url(cls, url: str) -> Type[FileSystem]:
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme
        return FileSystem.by_scheme(scheme)

    def __init__(self) -> None:
        pass

    @contextmanager
    def open(
        self,
        filename: Union[str, Path],
        mode: str = "r",
    ) -> Iterator[IO[Any]]:
        raise NotImplementedError
