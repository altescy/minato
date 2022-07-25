from __future__ import annotations

import logging
import os
import shutil
import tarfile
import tempfile
from os import PathLike
from pathlib import Path
from typing import Literal, Union
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
    try:
        path = Path(path)
        if path.is_dir():
            shutil.rmtree(path)
        else:
            os.remove(path)
    except FileNotFoundError:
        pass


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

    splitted = str(path).rsplit("/", 1)
    if len(splitted) == 2:
        parent, name = splitted
    else:
        parent = ""
        name = str(path)

    if scheme and netloc:
        parent = f"{scheme}://{netloc}/{parent}"

    return parent, name


def sizeof_fmt(num: int | float, suffix: str = "", dividor: int | float = 1024) -> str:
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < dividor:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= dividor
    return "%.1f%s%s" % (num, "Yi", suffix)
