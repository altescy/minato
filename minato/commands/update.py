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
        self.parser.add_argument("uid_or_url", nargs="*", default=[])
        self.parser.add_argument("--force", action="store_true")
        self.parser.add_argument("--expired", action="store_true")
        self.parser.add_argument("--failed", action="store_true")
        self.parser.add_argument("--expire-days", type=int, default=None)
        self.parser.add_argument("--root", type=Path, default=None)

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
        )

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
            minato.cached_path(cached_file.url)

        print("Cache files were successfully updated.")
