import argparse
from pathlib import Path

from minato.cache import CachedFile
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

        print(f"{num_caches} files will be updated:")
        for cached_file in cached_files:
            print(f"  [{cached_file.id}] {cached_file.url}")

        if not args.force:
            yes_or_no = input("Are you sure to update these caches? y/[n]:")
            if yes_or_no not in ("y", "Y"):
                print("canceled")
                return

        with cache.tx() as tx:
            for cached_file in cached_files:
                minato.download(cached_file.url, cached_file.local_path)
                tx.update(cached_file)

        print("Cache files were successfully updated.")
