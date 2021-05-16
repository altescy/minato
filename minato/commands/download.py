import argparse
from pathlib import Path

from minato.commands.subcommand import Subcommand
from minato.minato import Minato
from minato.util import get_parent_path_and_filename


@Subcommand.register(
    "download",
    description="download file to local",
    help="download file to local",
)
class DownloadCommand(Subcommand):
    def set_arguments(self) -> None:
        self.parser.add_argument("url", type=str)
        self.parser.add_argument("path", type=Path)
        self.parser.add_argument("--overwrite", action="store_true")

    def run(self, args: argparse.Namespace) -> None:
        url = args.url
        file_path = args.path
        if file_path.is_dir():
            _, filename = get_parent_path_and_filename(args.url)
            file_path = file_path / filename

        if file_path.exists() and not args.overwrite:
            raise FileExistsError(f"File {str(file_path)} already exists.")

        Minato.download(url, file_path)
