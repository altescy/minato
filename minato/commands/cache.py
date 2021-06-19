import argparse
from pathlib import Path

from minato.commands.subcommand import Subcommand
from minato.config import Config
from minato.minato import Minato


@Subcommand.register(
    "cache",
    description="cache remote file and return cached local file path",
    help="cache remote file and return cached local file path",
)
class CacheCommand(Subcommand):
    def set_arguments(self) -> None:
        self.parser.add_argument("url", type=str)
        self.parser.add_argument("--root", type=Path, default=None)

    def run(self, args: argparse.Namespace) -> None:
        config = Config.load(cache_root=args.root)

        minato = Minato(config)
        local_path = minato.cached_path(args.url)
        print(local_path)
