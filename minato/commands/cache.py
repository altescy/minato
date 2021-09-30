import argparse
from pathlib import Path

from minato.commands.subcommand import Subcommand
from minato.config import Config
from minato.minato import Minato


@Subcommand.register(
    "cache",
    description="cache remote file and return cached local file path",
    help="cache remote file and return cached local file path",
)
class CacheCommand(Subcommand):
    def set_arguments(self) -> None:
        self.parser.add_argument(
            "url",
            type=str,
            help="specify file path or url",
        )
        self.parser.add_argument(
            "--extract",
            action="store_true",
            help="extract archive file",
        )
        self.parser.add_argument(
            "--auto-update",
            action="store_true",
            help="download new version if available",
        )
        self.parser.add_argument(
            "--force-extract",
            action="store_true",
            help="force to extract the specified archive file",
        )
        self.parser.add_argument(
            "--force-download",
            action="store_true",
            help="force to download the specified file whether the cache exists or not",
        )
        self.parser.add_argument(
            "--not-retry",
            action="store_false",
            help="do not retry to download the file even if "
            "the previous download operation is failed",
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

        cached_path = minato.cached_path(
            args.url,
            extract=args.extract,
            auto_update=args.auto_update,
            expire_days=args.expire_days,
            force_extract=args.force_extract,
            force_download=args.force_download,
            retry=args.not_retry,
        )
        print(cached_path)
