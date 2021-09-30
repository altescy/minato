import argparse
import datetime
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
        self.parser.add_argument(
            "query",
            nargs="*",
            default=[],
            help="query to filter cached files",
        )
        self.parser.add_argument(
            "--sort",
            type=str,
            default=None,
            help="sort by the specified key",
        )
        self.parser.add_argument(
            "--desc",
            action="store_true",
            help="sort the list in descending order",
        )
        self.parser.add_argument(
            "--details",
            action="store_true",
            help="show more detailed information",
        )
        self.parser.add_argument(
            "--column-width",
            type=int,
            default=None,
            help="specify the max column width",
        )
        self.parser.add_argument(
            "--root",
            type=Path,
            default=None,
            help="specify the root directory path of cached data",
        )
        self.parser.add_argument(
            "--expired",
            action="store_true",
            default=None,
            help="show cached files that were expired",
        )
        self.parser.add_argument(
            "--failed",
            action="store_true",
            default=None,
            help="show cached files that were failed to download",
        )
        self.parser.add_argument(
            "--completed",
            action="store_true",
            default=None,
            help="show cached files that were successfully downloaded",
        )

    def run(self, args: argparse.Namespace) -> None:
        config = Config.load(
            cache_root=args.root,
        )
        minato = Minato(config)
        cache = minato.cache

        cached_files = cache.filter(
            queries=args.query,
            expired=args.expired,
            failed=args.failed,
            completed=args.completed,
        )

        columns = ["uid", "url", "size", "type", "status", "expire_days"]
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

            if cache.is_expired(cached_file):
                info["expire_days"] = f"EXPIRED({cached_file.expire_days})"
            elif cached_file.expire_days < 0:
                info["expire_days"] = "NONE"
            else:
                now = datetime.datetime.now()
                delta = now - cached_file.updated_at
                info["expire_days"] = f"{delta.days}/{cached_file.expire_days}"

            info["type"] = get_cache_type(cached_file.local_path)

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
