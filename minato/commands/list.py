import argparse
from pathlib import Path

from minato.commands.subcommand import Subcommand
from minato.minato import Minato
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
        self.parser.add_argument("--root", type=Path, default=None)

    def run(self, args: argparse.Namespace) -> None:
        minato = Minato(args.root)
        cache = minato.cache

        columns = ["id", "url", "size"]
        if args.details:
            columns.append("local_path")
            columns.append("created_at")
            columns.append("updated_at")
            columns.append("extraction_path")

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
