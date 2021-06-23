from __future__ import annotations

import dataclasses
import datetime
import json
import os
import uuid
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from minato.exceptions import CacheAlreadyExists, CacheNotFoundError, ConfigurationError
from minato.filelock import FileLock
from minato.util import remove_file_or_directory


class CacheStatus(Enum):
    PENDING = "PENDING"
    DOWNLOADING = "DOWNLOADING"
    EXTRACTING = "EXTRACTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DELETED = "DELETED"


@dataclasses.dataclass
class CachedFile:
    uid: str
    url: str
    local_path: Path
    created_at: datetime.datetime
    updated_at: datetime.datetime
    extraction_path: Optional[Path]
    status: CacheStatus

    def __init__(
        self,
        uid: str,
        url: str,
        local_path: Union[str, Path],
        created_at: Union[str, datetime.datetime],
        updated_at: Union[str, datetime.datetime],
        extraction_path: Optional[Union[str, Path]] = None,
        status: Union[str, CacheStatus] = CacheStatus.PENDING,
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
        if isinstance(status, str):
            status = CacheStatus(status)

        self.uid = uid
        self.url = url
        self.local_path = local_path.absolute()
        self.created_at = created_at
        self.updated_at = updated_at
        self.extraction_path = extraction_path
        self.status = status

    def to_tuple(self) -> Tuple[str, str, str, str, str, Optional[str], str]:
        return (
            self.uid,
            self.url,
            str(self.local_path),
            self.created_at.isoformat(),
            self.updated_at.isoformat(),
            str(self.extraction_path) if self.extraction_path else None,
            self.status.value,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uid": str(self.uid),
            "url": str(self.url),
            "local_path": str(self.local_path),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "extraction_path": str(self.extraction_path)
            if self.extraction_path
            else None,
            "status": self.status.value,
        }


class Cache:
    def __init__(
        self,
        root: Path,
        expire_days: int = -1,
    ) -> None:
        if not root.exists():
            os.makedirs(root, exist_ok=True)

        if not root.is_dir():
            raise ConfigurationError(
                f"Given cache_directory path is not a directory: {root}"
            )

        self._root = root
        self._expire_days = expire_days

    def __contains__(self, url: str) -> bool:
        try:
            self.by_url(url)
            return True
        except CacheNotFoundError:
            return False

    @contextmanager
    def lock(self, item: CachedFile) -> Iterator[None]:
        lock = FileLock(self.get_lockfile_path(item.uid))
        try:
            lock.acquire()
            yield
        finally:
            lock.release()

    @staticmethod
    def _generate_uid() -> str:
        return uuid.uuid4().hex

    def get_metadata_path(self, uid: str) -> Path:
        return self._root / (uid + ".json")

    def get_lockfile_path(self, uid: str) -> Path:
        return self._root / (uid + ".lock")

    def load_cached_file(self, metadata_path: Path) -> CachedFile:
        if not metadata_path.exists():
            raise CacheNotFoundError(f"Cache not found: {metadata_path}")
        with open(metadata_path, "r") as fp:
            params = json.load(fp)
        return CachedFile(**params)

    def new(self, url: str) -> CachedFile:
        uid = self._generate_uid()
        cached_file = CachedFile(
            uid=uid,
            url=url,
            local_path=self._root / uid,
            created_at=datetime.datetime.now(),
            updated_at=datetime.datetime.now(),
        )
        return cached_file

    def exists(self, item: CachedFile) -> bool:
        metadata_path = self.get_metadata_path(item.uid)
        return metadata_path.exists()

    def save(self, item: CachedFile) -> None:
        metadata_path = self.get_metadata_path(item.uid)
        with open(metadata_path, "w") as fp:
            json.dump(item.to_dict(), fp)

    def add(self, item: CachedFile) -> CachedFile:
        if self.exists(item):
            raise CacheAlreadyExists(item.url)
        self.save(item)
        return item

    def update(self, item: CachedFile) -> None:
        metadata_path = self.get_metadata_path(item.uid)
        if not metadata_path.exists():
            raise CacheNotFoundError(f"Cache not found with uid={item.uid}")
        item.updated_at = datetime.datetime.now()
        self.save(item)

    def by_uid(self, uid: str) -> CachedFile:
        metadata_path = self.get_metadata_path(uid)
        if not metadata_path.exists():
            raise CacheNotFoundError(f"Cache not found with uid={uid}")
        with open(metadata_path, "r") as fp:
            params = json.load(fp)
        return CachedFile(**params)

    def by_url(self, url: str) -> CachedFile:
        for cached_file in self.all():
            if cached_file.url == url:
                return cached_file
        raise CacheNotFoundError(f"Cache not found with url={url}")

    def delete(self, item: CachedFile) -> None:
        metadata_path = self.get_metadata_path(item.uid)
        lockfile_path = self.get_lockfile_path(item.uid)

        remove_file_or_directory(item.local_path)
        if item.extraction_path is not None:
            remove_file_or_directory(item.extraction_path)

        remove_file_or_directory(metadata_path)
        remove_file_or_directory(lockfile_path)

    def is_expired(self, item: CachedFile) -> bool:
        if self._expire_days < 0:
            return False
        now = datetime.datetime.now()
        delta = now - item.updated_at
        return delta.days >= self._expire_days

    def all(self) -> List[CachedFile]:
        cached_files: List[CachedFile] = []
        for metafile_path in self._root.glob("*.json"):
            with open(metafile_path, "r") as fp:
                params = json.load(fp)
                cached_file = CachedFile(**params)
            cached_files.append(cached_file)
        cached_files = sorted(cached_files, key=lambda x: x.created_at)
        return cached_files

    def filter(
        self,
        queries: List[str],
        expired: Optional[bool] = None,
        failed: Optional[bool] = None,
        completed: Optional[bool] = None,
    ) -> List[CachedFile]:
        cached_files = self.all()
        for query in queries:
            cached_files = [
                x for x in cached_files if query in x.url or x.uid.startswith(query)
            ]
        if expired is not None:
            cached_files = [x for x in cached_files if self.is_expired(x) == expired]
        if failed is not None:
            cached_files = [
                x for x in cached_files if (x.status == CacheStatus.FAILED) == failed
            ]
        if completed is not None:
            cached_files = [
                x
                for x in cached_files
                if (x.status == CacheStatus.COMPLETED) == completed
            ]

        unique_caches = {x.uid: x for x in cached_files}
        cached_files = sorted(unique_caches.values(), key=lambda x: x.created_at)
        return cached_files
