import argparse
from typing import Optional

from minato import __version__
from minato.commands.cache import CacheCommand  # noqa: F401
from minato.commands.list import ListCommand  # noqa: F401
from minato.commands.remove import RemoveCommand  # noqa: F401
from minato.commands.subcommand import Subcommand
from minato.commands.update import UpdateCommand  # noqa: F401


def main(prog: Optional[str] = None) -> None:
    parser = argparse.ArgumentParser(usage="%(prog)s", prog=prog)
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s " + __version__,
    )

    app = Subcommand.build(parser)
    app()
