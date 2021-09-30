import argparse
from pathlib import Path

from minato.commands.subcommand import Subcommand
from minato.config import Config
from minato.minato import Minato


@Subcommand.register(
    "remove",
    description="remove cached files",
    help="remove cached files",
)
class RemoveCommand(Subcommand):
    def set_arguments(self) -> None:
        self.parser.add_argument(
            "query",
            nargs="*",
            type=str,
            default=[],
            help="query to filter cached files",
        )
        self.parser.add_argument(
            "--force",
            action="store_true",
            help="remove files without confirmation",
        )
        self.parser.add_argument(
            "--expired",
            action="store_true",
            default=None,
            help="remove files that were expired",
        )
        self.parser.add_argument(
            "--failed",
            action="store_true",
            default=None,
            help="remove files that were failed to download",
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

        if args.query or args.expired is not None or args.failed is not None:
            cached_files = cache.filter(
                queries=args.query,
                expired=args.expired,
                failed=args.failed,
            )
        else:
            cached_files = []

        num_caches = len(cached_files)

        if not cached_files:
            print("No files to delete")
            return

        print(f"{num_caches} files will be deleted:")
        for cached_file in cached_files:
            print(f"  [{cached_file.uid[:8]}] {cached_file.url}")

        if not args.force:
            yes_or_not = input("Delete these caches? y/[n]: ")
            if yes_or_not not in ("y", "Y"):
                print("canceled")
                return

        for cached_file in cached_files:
            with cache.lock(cached_file):
                cache.delete(cached_file)

        print("Cache files were successfully deleted.")
