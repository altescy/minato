import argparse
from pathlib import Path

from minato.commands.subcommand import Subcommand
from minato.config import Config
from minato.minato import Minato


@Subcommand.register(
    "update",
    description="update cached files",
    help="update cached files",
)
class UpdateCommand(Subcommand):
    def set_arguments(self) -> None:
        self.parser.add_argument(
            "query",
            nargs="*",
            default=[],
            help="query to filter cached files",
        )
        self.parser.add_argument(
            "--auto",
            action="store_true",
            help="check update and download files if updates are available",
        )
        self.parser.add_argument(
            "--force",
            action="store_true",
            help="update cached files without confirmation",
        )
        self.parser.add_argument(
            "--force-download",
            action="store_true",
            help="force to download files whether updates exist or not",
        )
        self.parser.add_argument(
            "--force-extract",
            action="store_true",
            help="force to extract archive files",
        )
        self.parser.add_argument(
            "--expired",
            action="store_true",
            help="update cached files that were expired",
        )
        self.parser.add_argument(
            "--failed",
            action="store_true",
            help="update cached files that were failed to download",
        )
        self.parser.add_argument(
            "--expire-days",
            type=int,
            default=None,
            help="specify expire days of the cache",
        )
        self.parser.add_argument(
            "--root",
            type=Path,
            default=None,
            help="specify the root directory path of cached data",
        )

    def run(self, args: argparse.Namespace) -> None:
        config = Config.load(
            cache_root=args.root,
        )
        minato = Minato(config)
        cache = minato.cache

        if args.query or args.expired or args.failed:
            cached_files = cache.filter(
                queries=args.query,
                expired=args.expired,
                failed=args.failed,
            )
        elif args.auto:
            cached_files = cache.all()
        else:
            cached_files = []

        cached_files = [x for x in cached_files if minato.available_update(x.url)]

        num_caches = len(cached_files)
        if num_caches == 0:
            print("No caches to update.")
            return

        print(f"{num_caches} files will be updated:")
        for cached_file in cached_files:
            print(f"  [{cached_file.uid[:8]}] {cached_file.url}")

        if not args.force:
            yes_or_no = input("Are you sure to update these caches? y/[n]:")
            if yes_or_no not in ("y", "Y"):
                print("canceled")
                return

        for cached_file in cached_files:
            minato.cached_path(
                cached_file.url,
                expire_days=args.expire_days,
                auto_update=args.auto,
                force_download=args.force_download,
                force_extract=args.force_extract,
            )

        print("Cache files were successfully updated.")
