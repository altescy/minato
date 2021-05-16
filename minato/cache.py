from __future__ import annotations

import dataclasses
import datetime
import os
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from minato.exceptions import CacheNotFoundError, ConfigurationError


@dataclasses.dataclass
class CachedFile:
    id: int
    url: str
    local_path: Path
    created_at: datetime.datetime
    updated_at: datetime.datetime
    extraction_path: Optional[Path]

    def __init__(
        self,
        id: int,
        url: str,
        local_path: Union[str, Path],
        created_at: Union[str, datetime.datetime],
        updated_at: Union[str, datetime.datetime],
        extraction_path: Optional[Union[str, Path]] = None,
    ) -> None:
        if isinstance(local_path, str):
            local_path = Path(local_path)
        if isinstance(created_at, str):
            created_at = datetime.datetime.fromisoformat(created_at)
        if isinstance(updated_at, str):
            updated_at = datetime.datetime.fromisoformat(updated_at)
        if isinstance(extraction_path, str):
            extraction_path = Path(extraction_path)
        if extraction_path is not None:
            extraction_path = extraction_path.absolute()

        self.id = id
        self.url = url
        self.local_path = local_path.absolute()
        self.created_at = created_at
        self.updated_at = updated_at
        self.extraction_path = extraction_path

    def to_tuple(self) -> Tuple[int, str, str, str, str, Optional[str]]:
        return (
            self.id,
            self.url,
            str(self.local_path),
            self.created_at.isoformat(),
            self.updated_at.isoformat(),
            str(self.extraction_path) if self.extraction_path else None,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "url": str(self.url),
            "local_path": str(self.local_path),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "extraction_path": str(self.extraction_path)
            if self.extraction_path
            else None,
        }


class Cache:
    def __init__(
        self,
        cache_directory: Path,
        sqlite_path: Path,
    ) -> None:
        if not cache_directory.exists():
            os.makedirs(cache_directory, exist_ok=True)

        if not cache_directory.is_dir():
            raise ConfigurationError(
                f"Given cache_directory path is not a directory: {cache_directory}"
            )

        self._cache_directory = cache_directory
        self._sqlite_path = sqlite_path

        self._connection: Optional[sqlite3.Connection] = None
        self._cursor: Optional[sqlite3.Cursor] = None

        self.migrate()

    @contextmanager
    def tx(self) -> Iterator[Cache]:
        try:
            self.__enter__()
            yield self
        finally:
            self.__exit__()

    def __enter__(self) -> Cache:
        self._connection = sqlite3.connect(self._sqlite_path)
        self._cursor = self._connection.cursor()
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> bool:
        assert self._connection is not None
        assert self._cursor is not None

        self._connection.commit()

        self._cursor.close()
        self._connection.close()

        self._connection = None
        self._cursor = None
        return True

    def __contains__(self, url: str) -> bool:
        try:
            self.by_url(url)
            return True
        except CacheNotFoundError:
            return False

    def _check_connection(self) -> None:
        if self._connection is None or self._cursor is None:
            raise RuntimeError("SQLite connection is not established.")

    def _get_connection(self) -> sqlite3.Connection:
        if self._connection is not None:
            return self._connection

        return sqlite3.connect(self._sqlite_path)

    def _generate_unique_filename(self) -> Path:
        name = uuid.uuid4().hex
        return self._cache_directory / name

    def add(self, url: str) -> CachedFile:
        self._check_connection()
        assert self._connection is not None
        assert self._cursor is not None

        local_path = self._generate_unique_filename()
        self._cursor.execute(
            "INSERT INTO cached_files (url, local_path) VALUES (?, ?)",
            (url, str(local_path)),
        )

        cached_file = self.by_id(self._cursor.lastrowid)
        return cached_file

    def update(self, item: CachedFile) -> None:
        self._check_connection()
        assert self._connection is not None
        assert self._cursor is not None

        self._cursor.execute(
            """
            UPDATE
                cached_files
            SET
                url = ?
                , local_path = ?
                , updated_at = (datetime('now', 'localtime'))
                , extraction_path = ?
            WHERE
                id = ?
            """,
            (item.url, str(item.local_path), str(item.extraction_path), item.id),
        )

    def by_id(self, id_: int) -> CachedFile:
        connection = self._get_connection()

        rows = list(
            connection.execute("SELECT * FROM cached_files WHERE id = ?", (id_,))
        )
        if not rows:
            raise CacheNotFoundError(f"Cache not found with id={id}")

        row = rows[0]
        return CachedFile(*row)

    def by_url(self, url: str) -> CachedFile:
        connection = self._get_connection()

        rows = list(
            connection.execute("SELECT * FROM cached_files WHERE url = ?", (url,))
        )
        if not rows:
            raise CacheNotFoundError(f"Cache not found with id={id}")

        row = rows[0]
        return CachedFile(*row)

    def delete(self, item: CachedFile) -> None:
        self._check_connection()
        assert self._connection is not None
        assert self._cursor is not None

        self._cursor.execute("DELETE FROM cached_files WHERE id = ?", (item.id,))
        os.remove(item.local_path)

    def list(self) -> List[CachedFile]:
        connection = self._get_connection()
        rows = connection.execute("SELECT * FROM cached_files")
        cached_files = [CachedFile(*row) for row in rows]
        return cached_files

    def migrate(self) -> None:
        connection = sqlite3.connect(self._sqlite_path)
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS cached_files (
                    id INTEGER PRIMARY KEY
                    , url TEXT NOT NULL UNIQUE
                    , local_path TEXT NOT NULL
                    , created_at TEXT DEFAULT (datetime('now', 'localtime'))
                    , updated_at TEXT DEFAULT (datetime('now', 'localtime'))
                    , extraction_path TEXT
                )
                """
            )
        finally:
            cursor.close()
            connection.close()
