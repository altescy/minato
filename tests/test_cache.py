import tempfile
from pathlib import Path

from minato.cache import Cache


def test_cache_add_list_and_delete() -> None:
    with tempfile.TemporaryDirectory() as _tempdir:
        tempdir = Path(_tempdir)
        cache = Cache(tempdir / "artifacts", tempdir / "sqlite.db")

        with cache.tx() as tx:
            cached_file = tx.add("https://example.com/path/to/file_1")
            _ = tx.add("https://example.com/path/to/file_2")
            _ = tx.add("https://example.com/path/to/file_3")

        with cached_file.local_path.open("w") as fp:
            fp.write("Hello, world!")
        assert cached_file.local_path.exists()

        files = tx.list()
        assert len(files) == 3

        with cache.tx() as tx:
            tx.delete(cached_file)
        assert not cached_file.local_path.exists()


def test_cache_contains() -> None:
    with tempfile.TemporaryDirectory() as _tempdir:
        tempdir = Path(_tempdir)
        cache = Cache(tempdir / "artifacts", tempdir / "sqlite.db")

        url = "https://example.com/path/to/file"
        with cache.tx() as tx:
            _ = tx.add(url)

        assert url in cache
