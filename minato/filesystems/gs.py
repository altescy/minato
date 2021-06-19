from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Union

from fs.opener.parse import parse_fs_url
from fs_gcsfs import GCSFS
from google.cloud.storage import Client

from minato.filesystems.filesystem import FileSystem
from minato.util import get_parent_path_and_filename


@FileSystem.register(["gs"])
class GoogleCloudStorageFileSystem(FileSystem):
    def __init__(self, url_or_filename: Union[str, Path]) -> None:
        super().__init__(url_or_filename)
        self._url = str(url_or_filename)

    @contextmanager
    def open(
        self,
        mode: str = "r",
    ) -> Iterator[IO[Any]]:
        parsed_url = parse_fs_url(self._url)
        bucket_name, _, path = parsed_url.resource.partition("/")
        dir_path, gs_filename = get_parent_path_and_filename(path)

        if parsed_url.params.get("strict") == "false":
            strict = False
        else:
            strict = True

        client = Client()
        project = parsed_url.params.get("project")
        if project:
            client.project = project

        api_endpoint = parsed_url.params.get("api_endpoint")
        if api_endpoint:
            client.client_options = {"api_endpoint": api_endpoint}

        with GCSFS(
            bucket_name,
            root_path=dir_path,
            client=client,
            strict=strict,
            create=True,
        ) as gcsfs:
            with gcsfs.open(gs_filename, mode) as fp:
                yield fp
