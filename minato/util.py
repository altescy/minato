import logging
import os
import shutil
import tarfile
import tempfile
from os import PathLike
from pathlib import Path
from typing import IO, Any, Literal, Tuple, Union
from urllib.parse import urlparse
from zipfile import ZipFile, is_zipfile

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

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
OpenBinaryMode = Union[
    OpenBinaryModeUpdating, OpenBinaryModeReading, OpenBinaryModeWriting
]


def remove_file_or_directory(path: Union[str, PathLike]) -> None:
    try:
        path = Path(path)
        if path.is_dir():
            shutil.rmtree(path)
        else:
            os.remove(path)
    except FileNotFoundError:
        pass


def is_archive_file(filename: Union[str, PathLike]) -> bool:
    if not Path(filename).is_file():
        return False
    return is_zipfile(filename) or tarfile.is_tarfile(filename)


def extract_archive_file(
    source_path: Union[str, PathLike],
    target_path: Union[str, PathLike],
) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        if is_zipfile(source_path):
            with ZipFile(source_path, "r") as zip_file:
                zip_file.extractall(temp_dir)
        elif tarfile.is_tarfile(source_path):
            with tarfile.open(source_path) as tar_file:
                tar_file.extractall(temp_dir)
        os.replace(temp_dir, target_path)


def extract_path(filename: Union[str, PathLike]) -> str:
    parsed = urlparse(str(filename))
    return parsed.path


def is_local(url_or_filename: Union[str, PathLike]) -> bool:
    if isinstance(url_or_filename, Path):
        return True

    parsed = urlparse(str(url_or_filename))
    if parsed.scheme in ("", "file", "osfs"):
        return True

    return False


def get_parent_path_and_filename(path: Union[str, PathLike]) -> Tuple[str, str]:
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


def sizeof_fmt(num: Union[int, float], suffix: str = "B") -> str:
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, "Yi", suffix)


def http_get(url: str, temp_file: IO[Any]) -> None:
    with _session_with_backoff() as session:
        req = session.get(url, stream=True)
        req.raise_for_status()
        content_length = req.headers.get("Content-Length")
        total = int(content_length) if content_length is not None else None
        with tqdm(
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            total=total,
            desc="downloading",
        ) as progress:
            for chunk in req.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    progress.update(len(chunk))
                    temp_file.write(chunk)


def _session_with_backoff() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))

    return session
