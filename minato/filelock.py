from __future__ import annotations

import fcntl
from pathlib import Path
from types import TracebackType
from typing import IO, Any, Optional, Type, Union


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

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        self.release()
        return exc_type is None and exc_value is None and traceback is None


class LockedFile:
    def __init__(self, path: Path) -> None:
        self._path = path

        self._lockfile = path.parent / (path.name + ".lock")
        self._lock = FileLock(self._lockfile)

    @property
    def path(self) -> Path:
        return self._path

    def __repr__(self) -> str:
        return str(self._path)

    def acquire(self) -> None:
        self._lock.acquire()

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> LockedFile:
        self._lock.acquire()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        self.release()
        return exc_type is None and exc_value is None and traceback is None
