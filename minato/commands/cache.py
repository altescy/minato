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
        self.parser.add_argument("--extract", action="store_true")
        self.parser.add_argument("--force-extract", action="store_true")
        self.parser.add_argument("--force-download", action="store_true")
        self.parser.add_argument("--not-retry", action="store_false")
        self.parser.add_argument("--expire-days", type=int, default=None)
        self.parser.add_argument("--root", type=Path, default=None)

    def run(self, args: argparse.Namespace) -> None:
        config = Config.load(
            cache_root=args.root,
            expire_days=args.expire_days,
        )
        minato = Minato(config)

        cached_path = minato.cached_path(
            args.url,
            extract=args.extract,
            force_extract=args.force_extract,
            force_download=args.force_download,
            retry=args.not_retry,
        )
        print(cached_path)
