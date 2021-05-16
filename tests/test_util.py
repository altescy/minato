import tempfile
from pathlib import Path

from minato import util


def test_is_archive_file() -> None:
    archive_path = "tests/fixtures/archive.zip"
    normal_path = "tests/fixtures/hello.txt"

    assert util.is_archive_file(archive_path)
    assert not util.is_archive_file(normal_path)


def test_extract_archive_file() -> None:
    source_path = "tests/fixtures/archive.zip"

    with tempfile.TemporaryDirectory() as temp_dir:
        target_path = Path(temp_dir) / "target"
        util.extract_archive_file(source_path, target_path)

        assert (target_path / "archive" / "foo.txt").exists()
        assert (target_path / "archive" / "bar.txt").exists()
