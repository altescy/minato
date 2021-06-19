import os
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Union

from fs.ftpfs import FTPFS
from fs.opener.parse import parse_fs_url

from minato.filesystems.filesystem import FileSystem
from minato.util import get_parent_path_and_filename


@FileSystem.register(["ftp"])
class FTPFileSystem(FileSystem):
    def __init__(self, url_or_filename: Union[str, Path]) -> None:
        super().__init__(url_or_filename)
        self._url = str(url_or_filename)

    @contextmanager
    def open_file(
        self,
        mode: str = "r",
    ) -> Iterator[IO[Any]]:
        parsed_url = parse_fs_url(self._url)
        ftp_host, _, ftp_path = parsed_url.resource.partition("/")
        ftp_host, _, ftp_port = ftp_host.partition(":")
        dir_path, ftp_filename = get_parent_path_and_filename(ftp_path)

        ftp_username = parsed_url.username or os.environ.get("FTP_USERNAME")
        ftp_password = parsed_url.password or os.environ.get("FTP_PASSWORD")
        ftp_proxy = parsed_url.params.get("proxy")

        ftp_timeout = parsed_url.params.get("timeout", "10")
        ftp_tls = parsed_url.protocol == "ftps"

        ftp_filename = os.path.join(dir_path, ftp_filename)

        with FTPFS(
            ftp_host,
            port=int(ftp_port),
            user=ftp_username or "anonymous",
            passwd=ftp_password or "",
            proxy=ftp_proxy,
            timeout=int(ftp_timeout),
            tls=bool(ftp_tls),
        ) as ftpfs:
            if dir_path:
                ftpfs.makedirs(dir_path, recreate=True)
            with ftpfs.open(ftp_filename, mode) as fp:
                yield fp
