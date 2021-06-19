import tempfile
from pathlib import Path

import minato
from minato.config import Config


def test_version() -> None:
    assert minato.__version__ == "0.2.0"


def test_open() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        cache_root = Path(tempdir)

        with minato.open(
            "https://raw.githubusercontent.com/altescy/xsklearn/main/README.md",
            cache_root=cache_root,
        ) as fp:
            text = fp.readline().strip()

        assert text == "xsklearn"


def test_cached_path() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        cache_root = Path(tempdir)

        path = minato.cached_path(
            "https://raw.githubusercontent.com/altescy/xsklearn/main/README.md",
            cache_root=cache_root,
        )

        assert path.exists()
        assert path.parent == Config(cache_root=cache_root).cache_artifact_dir


def test_cached_path_with_zip_file() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        minato_root = Path(tempdir)
        path = minato.cached_path(
            "https://github.com/altescy/minato/raw/main"
            "/tests/fixtures/archive.zip!archive/foo.txt",
            cache_root=minato_root,
        )

        assert path.exists()
        assert path.is_file()

        with path.open("r") as fp:
            content = fp.read()
        assert content == "this file is foo.txt\n"


def test_cached_path_with_local_tar_file() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        cache_root = Path(tempdir)
        path = minato.cached_path(
            "tests/fixtures/archive.tar.gz!foo.txt",
            cache_root=cache_root,
        )

        assert path.exists()
        assert path.is_file()

        with path.open("r") as fp:
            content = fp.read()
        assert content == "this file is foo.txt\n"
