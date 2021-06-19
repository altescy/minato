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
        self.parser.add_argument("url_or_id", type=str)
        self.parser.add_argument("--extract", action="store_true")
        self.parser.add_argument("--force-extract", action="store_true")
        self.parser.add_argument("--root", type=Path, default=None)

    def run(self, args: argparse.Namespace) -> None:
        config = Config.load(cache_root=args.root)
        minato = Minato(config)

        try:
            cache_id = int(args.url_or_id)
            cached_file = minato.cache.by_id(cache_id)
            local_path = cached_file.local_path
        except ValueError:
            url = args.url_or_id
            local_path = minato.cached_path(
                url,
                extract=args.extract,
                force_extract=args.force_extract,
            )

        print(local_path)
