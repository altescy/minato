import hashlib
import logging
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Tuple, Union
from urllib.parse import urlparse

import requests
from fs import open_fs
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def extract_path(filename: Union[str, Path]) -> Path:
    parsed = urlparse(str(filename))
    return Path(parsed.path)


def is_local(url_or_filename: Union[str, Path]) -> bool:
    if isinstance(url_or_filename, Path):
        return True

    parsed = urlparse(str(url_or_filename))
    if parsed.scheme in ("", "file", "osfs"):
        return True

    return False


def get_parent_path_and_filename(path: Union[str, Path]) -> Tuple[str, str]:
    if isinstance(path, Path):
        parent = str(path.parent)
        name = str(path.name)
        return parent, name

    splitted = str(path).rsplit("/", 1)
    if len(splitted) == 2:
        parent, name = splitted
    else:
        parent = "./"
        name = str(path)
    return parent, name


@contextmanager
def open_file(
    url_or_filename: Union[str, Path], mode: str = "r", **kwargs: Any
) -> Iterator[IO[Any]]:
    parsed = urlparse(str(url_or_filename))

    if parsed.scheme in ("http", "https"):
        url = str(url_or_filename)
        with open_file_with_http(url, mode=mode) as fp:
            yield fp

    else:
        with open_file_with_fs(url_or_filename, mode=mode, **kwargs) as fp:
            yield fp


@contextmanager
def open_file_with_http(url: str, mode: str = "r", **kwargs: Any) -> Iterator[IO[Any]]:
    if not mode.startswith("r"):
        raise ValueError(f"invalid mode for http(s): {mode}")

    temp_file = tempfile.NamedTemporaryFile(delete=False)
    try:
        _http_get(url, temp_file)
        temp_file.close()
        with open(temp_file.name, mode, **kwargs) as fp:
            yield fp
    finally:
        os.remove(temp_file.name)


@contextmanager
def open_file_with_fs(
    filename: Union[str, Path], *args: Any, **kwargs: Any
) -> Iterator[IO[Any]]:
    filename = str(filename)
    parent, name = get_parent_path_and_filename(filename)

    with open_fs(parent) as fs:
        with fs.open(name, *args, **kwargs) as fp:
            yield fp


def _session_with_backoff() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))

    return session


def _http_get(url: str, temp_file: IO[Any]) -> None:
    with _session_with_backoff() as session:
        req = session.get(url, stream=True)
        req.raise_for_status()
        content_length = req.headers.get("Content-Length")
        total = int(content_length) if content_length is not None else None
        progress = tqdm(unit="B", total=total, desc="downloading")
        for chunk in req.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                progress.update(len(chunk))
                temp_file.write(chunk)
        progress.close()


def _get_cached_filename(path: Union[str, Path]) -> str:
    encoded_path = str(path).encode()
    name = hashlib.md5(encoded_path).hexdigest()
    return name
