from pathlib import Path

from minato.filesystems import OSFileSystem


def test_open_file() -> None:
    fixture_root = Path("tests/fixtures")
    fs = OSFileSystem(fixture_root / "hello.txt")
    with fs.open_file() as fp:
        text = fp.read().strip()
    assert text == "Hello, world!"
