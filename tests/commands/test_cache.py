import tempfile
from pathlib import Path

from minato.commands import create_parser
from minato.commands.cache import CacheCommand  # noqa: F401
from minato.config import Config


def test_cache_command() -> None:
    url = (
        "https://raw.githubusercontent.com/altescy/minato/main/tests/fixtures/hello.txt"
    )

    with tempfile.TemporaryDirectory() as tempdir:
        config = Config(cache_root=Path(tempdir))

        parser = create_parser()
        args = parser.parse_args(["cache", url, "--root", str(config.cache_root)])

        args.func(args)

        cached_files = [
            x
            for x in config.cache_artifact_dir.glob("*")
            if not x.name.endswith(".lock")
        ]
        assert len(cached_files) == 1

        filepath = cached_files[0]
        with filepath.open("r") as fp:
            text = fp.read()
        assert text == "Hello, world!\n"
