import tempfile
from pathlib import Path

from minato.filesystems import OSFileSystem


def test_open_file() -> None:
    fixture_root = Path("tests/fixtures")
    fs = OSFileSystem(fixture_root / "hello.txt")
    with fs.open_file() as fp:
        text = fp.read().strip()
    assert text == "Hello, world!"


def test_download() -> None:
    fixture_root = Path("tests/fixtures")
    fs = OSFileSystem(fixture_root / "hello.txt")
    with tempfile.TemporaryDirectory() as _tempdir:
        tempdir = Path(_tempdir)
        fs.download(tempdir)

        assert (tempdir / "hello.txt").is_file()


def test_exists() -> None:
    fixture_root = Path("tests/fixtures")
    assert OSFileSystem(fixture_root / "hello.txt").exists()
    assert not OSFileSystem(fixture_root / "foo.txt").exists()
