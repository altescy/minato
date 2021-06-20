from __future__ import annotations

import fcntl
import os
from pathlib import Path
from typing import IO, Any, Optional, Union


class FileLock:
    def __init__(self, lockfile: Union[str, Path]) -> None:
        self._file_path = lockfile
        self._lockfile: Optional[IO[Any]] = None

    def acquire(self) -> None:
        if self._lockfile is None:
            self._lockfile = open(self._file_path, "w")
            fcntl.flock(self._lockfile, fcntl.LOCK_EX)

    def release(self) -> None:
        if self._lockfile is not None:
            fcntl.flock(self._lockfile, fcntl.LOCK_UN)
            self._lockfile.close()
            self._lockfile = None

    def __enter__(self) -> FileLock:
        self.acquire()
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self.release()
