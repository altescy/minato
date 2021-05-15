import tempfile
from pathlib import Path

from minato.commands import create_parser
from minato.commands.add import AddCommand  # noqa: F401


def test_add_command() -> None:
    url = (
        "https://raw.githubusercontent.com/altescy/minato/main/tests/fixtures/hello.txt"
    )

    with tempfile.TemporaryDirectory() as tempdir:
        minato_root = Path(tempdir)

        parser = create_parser()
        args = parser.parse_args(["add", url, "--root", str(minato_root)])

        args.func(args)

        cached_files = list(minato_root.glob("cache/*"))
        assert len(cached_files) == 1

        filepath = cached_files[0]
        with filepath.open("r") as fp:
            text = fp.read()
        assert text == "Hello, world!\n"
