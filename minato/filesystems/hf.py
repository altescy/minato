from __future__ import annotations

import functools
import os
from contextlib import contextmanager
from logging import getLogger
from os import PathLike
from pathlib import Path
from typing import IO, Any, BinaryIO, ContextManager, Iterator, TextIO, overload

from minato.common import Progress
from minato.filesystems.filesystem import FileSystem
from minato.util import DecompressOption, OpenBinaryMode, OpenTextMode, sizeof_fmt

try:
    from huggingface_hub import HfFileSystem
except ModuleNotFoundError:
    HfFileSystem = None


logger = getLogger(__name__)


@FileSystem.register(["hf"])
class HuggingfaceHubFileSystem(FileSystem):
    @functools.cached_property
    def _hf(self) -> "HfFileSystem":
        if HfFileSystem is None:
            raise ModuleNotFoundError(
                "HuggingfaceHubFileSystem is not available. Please make sure that "
                "huggingface-hub is successfully installed."
            )
        return HfFileSystem()

    def exists(self) -> bool:
        return bool(self._hf.exists(self._url.raw))

    def download(self, path: str | PathLike) -> None:
        if not self.exists():
            raise FileNotFoundError(self._url.raw)

        path = Path(path)

        root = f"{self._url.netloc}/{self._url.path}"
        if self._hf.isdir(self._url.raw):
            paths = self._hf.expand_path(f"{self._url.raw}/**")
        else:
            paths = self._hf.expand_path(self._url.raw)

        paths = [path for path in paths if not self._hf.isdir(path)]
        total = sum(self._hf.size(path) for path in paths)

        logger.debug("%s file(s) (%sB) will be downloaded to %s.", len(paths), total, path)

        path = path / os.path.basename(root) if path.is_dir() else path

        with Progress[int](
            total,
            unit="iB",
            desc="downloading from s3",
            sizeof_formatter=sizeof_fmt,
        ) as progress:
            for source_path in paths:
                relprefix = os.path.relpath(source_path, root)
                target_path = path / relprefix
                target_path.parent.mkdir(parents=True, exist_ok=True)
                with self._hf.open(source_path, "rb") as src, target_path.open("wb") as dst:
                    while True:
                        chunk = src.read(1024)
                        if not chunk:
                            break
                        dst.write(chunk)
                        progress.update(len(chunk))

    def upload(self, path: str | PathLike) -> None:
        path = Path(str(path)).absolute()

        root = f"{self._url.netloc}/{self._url.path}"
        if root.endswith("/"):
            root = os.path.join(root, os.path.basename(path))

        filenames = [subpath for subpath in path.glob("**/*") if subpath.is_file()] if path.is_dir() else [path]

        total = sum(filename.stat().st_size for filename in filenames)
        logger.info("%s file(s) (%sB) will be uploaded to %s", len(filenames), total, self._url)

        with Progress[int](
            total,
            unit="iB",
            desc="uploading to s3",
            sizeof_formatter=sizeof_fmt,
        ) as progress:
            for source_path in filenames:
                if source_path != path:
                    relpath = os.path.relpath(source_path, path)
                    target_path = os.path.join(root, relpath)
                else:
                    target_path = root

                with source_path.open("rb") as src, self._hf.open(target_path, "wb") as dst:
                    while True:
                        chunk = src.read(1024)
                        if not chunk:
                            break
                        dst.write(chunk)
                        progress.update(len(chunk))

    def delete(self) -> None:
        self._hf.rm(self._url.raw, recursive=True)

    def get_version(self) -> str | None:
        return str(self._hf.checksum(self._url.raw))

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
        @contextmanager
        def _open(
            mode: str,
            buffering: int,
            encoding: str | None,
            errors: str | None,
            newline: str | None,
        ) -> Iterator[IO[Any]]:
            if not self.exists():
                raise FileNotFoundError(self._url.raw)

            with self._hf.open(
                self._url.raw,
                mode,
                encoding=encoding,
                errors=errors,
                newline=newline,
                compression=None if decompress == "none" else "infer",
            ) as f:
                yield f

        return _open(mode, buffering, encoding, errors, newline)
