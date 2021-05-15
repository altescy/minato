import tempfile
from pathlib import Path

import minato
from minato.config import Config


def test_version() -> None:
    assert minato.__version__ == "0.1.0"


def test_open() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        minato_root = Path(tempdir)

        with minato.open(
            "https://raw.githubusercontent.com/altescy/xsklearn/main/README.md",
            minato_root=minato_root,
        ) as fp:
            text = fp.readline().strip()

        assert text == "xsklearn"


def test_cached_path() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        minato_root = Path(tempdir)

        path = minato.cached_path(
            "https://raw.githubusercontent.com/altescy/xsklearn/main/README.md",
            minato_root=minato_root,
        )

        assert path.exists()
        assert path.parent == Config(minato_root=minato_root).cache_directory
