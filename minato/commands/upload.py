import argparse
from pathlib import Path

from minato.commands.subcommand import Subcommand
from minato.minato import Minato


@Subcommand.register(
    "upload",
    description="upload local file to remote",
    help="upload local file to remote",
)
class UploadCommand(Subcommand):
    def set_arguments(self) -> None:
        # TODO: id or url
        self.parser.add_argument("local", type=Path)
        self.parser.add_argument("remote", type=str)

    def run(self, args: argparse.Namespace) -> None:
        print(f"upload {args.local} to {args.remote}")
        Minato.upload(args.local, args.remote)
