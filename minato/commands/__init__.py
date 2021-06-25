import argparse
from typing import Optional

from minato import __version__
from minato.commands.cache import CacheCommand  # noqa: F401
from minato.commands.list import ListCommand  # noqa: F401
from minato.commands.remove import RemoveCommand  # noqa: F401
from minato.commands.subcommand import Subcommand
from minato.commands.update import UpdateCommand  # noqa: F401


def create_parser(prog: Optional[str] = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(usage="%(prog)s", prog=prog)
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s " + __version__,
    )

    subparsers = parser.add_subparsers()

    for subcommand in Subcommand.subcommands:
        subcommand.setup(subparsers)

    return parser


def main(prog: Optional[str] = None) -> None:
    parser = create_parser(prog)
    args = parser.parse_args()

    func = getattr(args, "func", None)
    if func is None:
        parser.parse_args(["--help"])

    func(args)
