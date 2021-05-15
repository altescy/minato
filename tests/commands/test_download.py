import tempfile
from pathlib import Path

from minato.commands import create_parser
from minato.commands.add import AddCommand  # noqa: F401


def test_download_command() -> None:
    url = (
        "https://raw.githubusercontent.com/altescy/minato/main/tests/fixtures/hello.txt"
    )

    with tempfile.TemporaryDirectory() as tempdir:
        minato_root = Path(tempdir)

        parser = create_parser()
        args = parser.parse_args(["download", url, str(minato_root)])

        args.func(args)

        filepath = minato_root / "hello.txt"
        assert filepath.exists()

        with filepath.open("r") as fp:
            text = fp.read()
        assert text == "Hello, world!\n"


def test_download_command_with_filename() -> None:
    url = (
        "https://raw.githubusercontent.com/altescy/minato/main/tests/fixtures/hello.txt"
    )

    with tempfile.TemporaryDirectory() as tempdir:
        minato_root = Path(tempdir)

        parser = create_parser()
        args = parser.parse_args(["download", url, str(minato_root / "foo.txt")])

        args.func(args)

        filepath = minato_root / "foo.txt"
        assert filepath.exists()

        with filepath.open("r") as fp:
            text = fp.read()
        assert text == "Hello, world!\n"
