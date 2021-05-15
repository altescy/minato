import argparse
from pathlib import Path

from minato.commands.subcommand import Subcommand
from minato.minato import Minato


@Subcommand.register(
    "update",
    description="update cached files",
    help="update cached files",
)
class UpdateCommand(Subcommand):
    def set_arguments(self) -> None:
        # TODO: id or url
        self.parser.add_argument("id", action="append", default=[])
        self.parser.add_argument("--force", action="store_true")
        self.parser.add_argument("--root", type=Path, default=None)

    def run(self, args: argparse.Namespace) -> None:
        minato = Minato(args.root)
        cache = minato.cache

        cached_files = [cache.by_id(cache_id) for cache_id in args.id]
        num_caches = len(cached_files)

        if not args.force:
            print(f"{num_caches} files will be updated:")
            for cached_file in cached_files:
                print(f"  [{cached_file.id}] {cached_file.url}")
            yes_or_no = input("Are you sure to update these caches? y/[n]:")
            if yes_or_no not in ("y", "Y"):
                print("canceled")
                return

        with cache.tx() as tx:
            for cached_file in cached_files:
                minato.download(cached_file.url, cached_file.local_path)
                tx.update(cached_file)

        print("Cache files were successfully updated.")
