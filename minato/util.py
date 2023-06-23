from __future__ import annotations

import bz2
import gzip
import logging
import lzma
import os
import shutil
import tarfile
import tempfile
from contextlib import suppress
from os import PathLike
from pathlib import Path
from typing import IO, Any, Literal, Union, cast
from urllib.parse import urlparse
from zipfile import ZipFile, is_zipfile

logger = logging.getLogger(__name__)


OpenTextModeUpdating = Literal[
    "r+",
    "+r",
    "rt+",
    "r+t",
    "+rt",
    "tr+",
    "t+r",
    "+tr",
    "w+",
    "+w",
    "wt+",
    "w+t",
    "+wt",
    "tw+",
    "t+w",
    "+tw",
    "a+",
    "+a",
    "at+",
    "a+t",
    "+at",
    "ta+",
    "t+a",
    "+ta",
    "x+",
    "+x",
    "xt+",
    "x+t",
    "+xt",
    "tx+",
    "t+x",
    "+tx",
]
OpenTextModeWriting = Literal[
    "w",
    "wt",
    "tw",
    "a",
    "at",
    "ta",
    "x",
    "xt",
    "tx",
]
OpenTextModeReading = Literal[
    "r",
    "rt",
    "tr",
    "U",
    "rU",
    "Ur",
    "rtU",
    "rUt",
    "Urt",
    "trU",
    "tUr",
    "Utr",
]
OpenTextMode = Union[OpenTextModeUpdating, OpenTextModeWriting, OpenTextModeReading]
OpenBinaryModeUpdating = Literal[
    "rb+",
    "r+b",
    "+rb",
    "br+",
    "b+r",
    "+br",
    "wb+",
    "w+b",
    "+wb",
    "bw+",
    "b+w",
    "+bw",
    "ab+",
    "a+b",
    "+ab",
    "ba+",
    "b+a",
    "+ba",
    "xb+",
    "x+b",
    "+xb",
    "bx+",
    "b+x",
    "+bx",
]
OpenBinaryModeWriting = Literal[
    "wb",
    "bw",
    "ab",
    "ba",
    "xb",
    "bx",
]
OpenBinaryModeReading = Literal[
    "rb",
    "br",
    "rbU",
    "rUb",
    "Urb",
    "brU",
    "bUr",
    "Ubr",
]
OpenBinaryMode = Union[OpenBinaryModeUpdating, OpenBinaryModeReading, OpenBinaryModeWriting]


def remove_file_or_directory(path: str | PathLike) -> None:
    with suppress(FileNotFoundError):
        path = Path(path)
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def is_archive_file(filename: str | PathLike) -> bool:
    if not Path(filename).is_file():
        return False
    return is_zipfile(filename) or tarfile.is_tarfile(filename)


def extract_archive_file(
    source_path: str | PathLike,
    target_path: str | PathLike,
) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        if is_zipfile(source_path):
            with ZipFile(source_path, "r") as zip_file:
                zip_file.extractall(temp_dir)
        elif tarfile.is_tarfile(source_path):
            with tarfile.open(source_path) as tar_file:
                tar_file.extractall(temp_dir)
        os.replace(temp_dir, target_path)


def extract_path(filename: str | PathLike) -> str:
    parsed = urlparse(str(filename))
    return parsed.path


def is_local(url_or_filename: str | PathLike) -> bool:
    if isinstance(url_or_filename, Path):
        return True

    parsed = urlparse(str(url_or_filename))
    if parsed.scheme in ("", "file", "osfs"):
        return True

    return False


def get_parent_path_and_filename(path: str | PathLike) -> tuple[str, str]:
    if isinstance(path, Path):
        parent = str(path.parent)
        name = str(path.name)
        return parent, name

    path = str(path)
    parsed_url = urlparse(path)
    scheme = parsed_url.scheme
    netloc = parsed_url.netloc
    path = parsed_url.path

    splitted = path.rsplit("/", 1)
    if len(splitted) == 2:
        parent, name = splitted
    else:
        parent = ""
        name = path

    if scheme and netloc:
        parent = f"{scheme}://{netloc}/{parent}"

    return parent, name


def sizeof_fmt(num: int | float, suffix: str = "", dividor: int | float = 1024) -> str:
    for unit in ("", "K", "M", "G", "T", "P", "E", "Z"):
        if abs(num) < dividor:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= dividor
    return "%.1f%s%s" % (num, "Yi", suffix)


DecompressOption = Literal["none", "force", "auto"]


def xopen(
    file: str | PathLike,
    mode: str = "r",
    buffering: int = -1,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    *,
    decompress: DecompressOption = "auto",
) -> IO[Any]:
    if decompress == "none":
        return open(file, mode, buffering, encoding, errors, newline=newline)

    if Path(file).exists():
        with suppress(gzip.BadGzipFile):
            with gzip.open(file, "r", encoding=encoding, errors=errors, newline=newline) as gzipfile:
                gzipfile.read(1)
                gzipfile.seek(0)
            return cast(IO[Any], gzip.open(file, mode, encoding=encoding, errors=errors, newline=newline))

        with suppress(lzma.LZMAError):
            with lzma.open(file, "r", encoding=encoding, errors=errors, newline=newline) as lzmafile:
                lzmafile.read(1)
                lzmafile.seek(0)
            return cast(IO[Any], lzma.open(file, mode, encoding=encoding, errors=errors, newline=newline))

        with suppress(IOError):
            with bz2.open(file, "r", encoding=encoding, errors=errors, newline=newline) as bz2file:
                bz2file.read(1)
                bz2file.seek(0)
            return cast(IO[Any], bz2.open(file, mode, encoding=encoding, errors=errors, newline=newline))

        raise ValueError(f"Failed to open with decompress: {file}")

    if str(file).endswith(".gz"):
        return cast(IO[Any], gzip.open(file, mode, encoding=encoding, errors=errors, newline=newline))

    if str(file).endswith((".xz", ".lzma")):
        return cast(IO[Any], lzma.open(file, mode, encoding=encoding, errors=errors, newline=newline))

    if str(file).endswith(".bz2"):
        return cast(IO[Any], bz2.open(file, mode, encoding=encoding, errors=errors, newline=newline))

    if decompress == "auto":
        return open(file, mode, buffering, encoding, errors, newline=newline)

    raise ValueError(f"Unknown compression type for file: {file}")
