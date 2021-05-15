import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Iterator, Union

from minato.filesystems.filesystem import FileSystem
from minato.util import http_get


@FileSystem.register(["http", "https"])
class HttpFileSystem(FileSystem):
    @contextmanager
    def open(
        self,
        filename: Union[str, Path],
        mode: str = "r",
    ) -> Iterator[IO[Any]]:
        if not mode.startswith("r"):
            raise ValueError(f"Invalid mode: {mode}")

        url = str(filename)
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            http_get(url, temp_file)
            temp_file.close()
            with open(temp_file.name, mode) as fp:
                yield fp
        finally:
            os.remove(temp_file.name)
