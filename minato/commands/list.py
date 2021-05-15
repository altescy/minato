import argparse
from typing import Dict

from minato.cache import Cache, CachedFile
from minato.commands.subcommand import Subcommand
from minato.config import Config
from minato.table import Table
from minato.util import sizeof_fmt


@Subcommand.register(
    "list",
    description="show list of cached files",
    help="show list of cached files",
)
class ListCommand(Subcommand):
    def set_arguments(self) -> None:
        self.parser.add_argument("--sort", type=str, default="id")
        self.parser.add_argument("--desc", action="store_true")
        self.parser.add_argument("--details", action="store_true")
        self.parser.add_argument("--column-width", type=int, default=None)

    def run(self, args: argparse.Namespace) -> None:
        config = Config()
        cache = Cache(config.cache_directory, config.sqlite_database)

        columns = ["id", "url", "size"]
        if args.details:
            columns.append("local_path")
            columns.append("created_at")
            columns.append("updated_at")

        table = Table(
            columns=columns,
            max_column_width=args.column_width,
        )
        for cached_file in cache.list():
            info = cached_file.to_dict()

            if cached_file.local_path.exists():
                info["size"] = sizeof_fmt(cached_file.local_path.stat().st_size)
            else:
                info["size"] = "-"

            table.add(info)

        table.sort(args.sort, args.desc)

        table.print()