import argparse
from pathlib import Path

from minato.commands.subcommand import Subcommand
from minato.config import Config
from minato.minato import Minato
from minato.table import Table
from minato.util import is_archive_file, sizeof_fmt


@Subcommand.register(
    "list",
    description="show list of cached files",
    help="show list of cached files",
)
class ListCommand(Subcommand):
    def set_arguments(self) -> None:
        self.parser.add_argument("uid_or_url", nargs="*", default=[])
        self.parser.add_argument("--sort", type=str, default=None)
        self.parser.add_argument("--desc", action="store_true")
        self.parser.add_argument("--details", action="store_true")
        self.parser.add_argument("--column-width", type=int, default=None)
        self.parser.add_argument("--root", type=Path, default=None)
        self.parser.add_argument("--expired", action="store_true", default=None)
        self.parser.add_argument("--failed", action="store_true", default=None)
        self.parser.add_argument("--completed", action="store_true", default=None)
        self.parser.add_argument("--expire-days", type=int, default=None)

    def run(self, args: argparse.Namespace) -> None:
        config = Config.load(
            cache_root=args.root,
            expire_days=args.expire_days,
        )
        minato = Minato(config)
        cache = minato.cache

        cached_files = cache.filter(
            queries=args.uid_or_url,
            expired=args.expired or args.expire_days is not None,
            failed=args.failed,
            completed=args.completed,
        )

        columns = ["uid", "url", "size", "type", "status", "expired"]
        if args.details:
            columns.append("local_path")
            columns.append("created_at")
            columns.append("updated_at")
            columns.append("extraction_path")

        table = Table(
            columns=columns,
            max_column_width=args.column_width,
        )

        for cached_file in cached_files:
            info = cached_file.to_dict()

            info["uid"] = info["uid"][:8]

            if cached_file.local_path.exists():
                info["size"] = sizeof_fmt(cached_file.local_path.stat().st_size)
            else:
                info["size"] = "-"

            info["type"] = get_cache_type(cached_file.local_path)
            info["expired"] = cache.is_expired(cached_file)

            table.add(info)

        if args.sort:
            table.sort(args.sort, args.desc)

        table.print()


def get_cache_type(path: Path) -> str:
    if path.is_dir():
        return "dir"
    if is_archive_file(path):
        return "archive"
    return "file"
