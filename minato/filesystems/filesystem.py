from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Callable, Dict, Iterator, List, Optional, Type, Union
from urllib.parse import urlparse

from minato.url import URL

logger = logging.getLogger(__name__)


@contextmanager
def open_file(
    url_or_filename: Union[str, Path],
    mode: str = "r",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
) -> Iterator[IO[Any]]:

    url = str(url_or_filename)
    filesystem = FileSystem.by_url(url)
    with filesystem.open_file(
        mode=mode,
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
    ) as fp:
        yield fp


def download(
    url_or_filename: Union[str, Path],
    download_path: Union[str, Path],
) -> None:
    url = str(url_or_filename)
    filesystem = FileSystem.by_url(url)
    filesystem.download(download_path)


def delete(url_or_filename: Union[str, Path]) -> None:
    url = str(url_or_filename)
    filesystem = FileSystem.by_url(url)
    filesystem.delete()


def get_version(
    url_or_filename: Union[str, Path],
) -> Optional[str]:
    url = str(url_or_filename)
    filesystem = FileSystem.by_url(url)
    return filesystem.get_version()


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
        logger.info("Infer file system of %s from url: %s", subclass.__name__, url)
        return subclass(url)

    def __init__(self, url_or_filename: Union[str, Path]) -> None:
        self._url = URL(str(url_or_filename))

    def exists(self) -> bool:
        raise NotImplementedError

    def download(self, path: Union[str, Path]) -> None:
        raise NotImplementedError

    def delete(self) -> None:
        raise NotImplementedError

    def get_version(self) -> Optional[str]:
        raise NotImplementedError

    @contextmanager
    def open_file(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> Iterator[IO[Any]]:
        raise NotImplementedError
