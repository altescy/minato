from __future__ import annotations

import fcntl
from pathlib import Path
from types import TracebackType
from typing import IO, Any, Type


class FileLock:
    def __init__(self, lockfile: str | Path) -> None:
        self._file_path = lockfile
        self._lockfile: IO[Any] | None = None

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

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        self.release()
        return exc_type is None and exc_value is None and traceback is None
