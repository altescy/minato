import argparse
from pathlib import Path

from minato.commands.subcommand import Subcommand
from minato.minato import Minato


@Subcommand.register(
    "remove",
    description="remove cached files",
    help="remove cached files",
)
class RemoveCommand(Subcommand):
    def set_arguments(self) -> None:
        # TODO: id or url
        self.parser.add_argument("id", action="append", type=int, default=[])
        self.parser.add_argument("--force", action="store_true")
        self.parser.add_argument("--root", type=Path, default=None)

    def run(self, args: argparse.Namespace) -> None:
        minato = Minato(args.root)
        cache = minato.cache

        cached_files = [cache.by_id(cache_id) for cache_id in args.id]
        num_caches = len(cached_files)

        if not cached_files:
            print("No files to delete")
            return

        if not args.force:
            print(f"{num_caches} files:")
            for cached_file in cached_files:
                print(f"  [{cached_file.id}] {cached_file.url}")

            yes_or_not = input("Delete these caches? y/[n]: ")
            if yes_or_not not in ("y", "Y"):
                print("canceled")
                return

        with cache.tx() as tx:
            for cached_file in cached_files:
                tx.delete(cached_file)

        print("Cache files were successfully deleted.")
