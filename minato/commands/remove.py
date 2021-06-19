import argparse
from pathlib import Path

from minato.cache import CachedFile
from minato.commands.subcommand import Subcommand
from minato.minato import Minato


@Subcommand.register(
    "remove",
    description="remove cached files",
    help="remove cached files",
)
class RemoveCommand(Subcommand):
    def set_arguments(self) -> None:
        self.parser.add_argument("url_or_id", nargs="+", type=str)
        self.parser.add_argument("--force", action="store_true")
        self.parser.add_argument("--root", type=Path, default=None)

    def run(self, args: argparse.Namespace) -> None:
        minato = Minato(args.root)
        cache = minato.cache

        def get_cached_file(url_or_id: str) -> CachedFile:
            try:
                cache_id = int(url_or_id)
                cached_file = cache.by_id(cache_id)
            except ValueError:
                url = url_or_id
                cached_file = cache.by_url(url)
            return cached_file

        cached_files = [get_cached_file(url_or_id) for url_or_id in args.url_or_id]
        num_caches = len(cached_files)

        if not cached_files:
            print("No files to delete")
            return

        print(f"{num_caches} files will be deleted:")
        for cached_file in cached_files:
            print(f"  [{cached_file.id}] {cached_file.url}")

        if not args.force:
            yes_or_not = input("Delete these caches? y/[n]: ")
            if yes_or_not not in ("y", "Y"):
                print("canceled")
                return

        with cache.tx() as tx:
            for cached_file in cached_files:
                tx.delete(cached_file)

        print("Cache files were successfully deleted.")
