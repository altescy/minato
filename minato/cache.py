from __future__ import annotations

import dataclasses
import datetime
import hashlib
import json
import logging
import os
import uuid
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

from minato.exceptions import CacheAlreadyExists, CacheNotFoundError, ConfigurationError
from minato.filelock import FileLock
from minato.util import remove_file_or_directory

logger = logging.getLogger(__name__)


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
    expire_days: int
    extraction_path: Optional[Path]
    status: CacheStatus
    version: Optional[str]
    auto_update: bool

    def __init__(
        self,
        uid: str,
        url: str,
        local_path: Union[str, Path],
        created_at: Union[str, datetime.datetime],
        updated_at: Union[str, datetime.datetime],
        expire_days: int = -1,
        extraction_path: Optional[Union[str, Path]] = None,
        status: Union[str, CacheStatus] = CacheStatus.PENDING,
        version: Optional[str] = None,
        auto_update: bool = True,
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
        self.expire_days = expire_days
        self.extraction_path = extraction_path
        self.status = status
        self.version = version
        self.auto_update = auto_update

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uid": str(self.uid),
            "url": str(self.url),
            "local_path": str(self.local_path),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "expire_days": self.expire_days,
            "extraction_path": str(self.extraction_path)
            if self.extraction_path
            else None,
            "status": self.status.value,
            "version": self.version,
            "auto_update": self.auto_update,
        }


class Cache:
    def __init__(
        self,
        root: Path,
        default_expire_days: int = -1,
        default_auto_update: bool = True,
    ) -> None:
        if not root.exists():
            os.makedirs(root, exist_ok=True)

        if not root.is_dir():
            raise ConfigurationError(
                f"Given cache_directory path is not a directory: {root}"
            )

        self._root = root
        self._default_expire_days = default_expire_days
        self._default_auto_update = default_auto_update

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
            logger.info("Trying to acquire file lock of %s.", item.url)
            lock.acquire()
            yield
        finally:
            lock.release()
            logger.info("File lock of %s was released.", item.url)

    @staticmethod
    def _generate_uid(url: str) -> str:
        hashval = hashlib.md5(url.encode()).hexdigest()
        uuidval = uuid.uuid4().hex
        return f"{hashval}-{uuidval}"

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
        uid = self._generate_uid(url)
        cached_file = CachedFile(
            uid=uid,
            url=url,
            local_path=self._root / uid,
            created_at=datetime.datetime.now(),
            updated_at=datetime.datetime.now(),
            expire_days=self._default_expire_days,
            auto_update=self._default_auto_update,
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
        logger.info("New cached file of %s was added.", item.url)
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
        logger.info("Try to find cached file of %s", url)
        hashval = hashlib.md5(url.encode()).hexdigest()
        for metadata_path in self._root.glob(f"{hashval}-*.json"):
            cached_file = self.load_cached_file(metadata_path)
            if cached_file.url == url:
                logger.info("Find cached file of %s: %s", url, cached_file.local_path)
                return cached_file

        logger.info(
            "There is no cached files with hashval of %s, "
            "so try to find a corresponding file from all files in %s",
            url,
            self._root,
        )

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
        logger.info("A cached file of %s was successfully deleted.", item.url)

    def is_expired(self, item: CachedFile) -> bool:
        if item.expire_days < 0:
            return False
        now = datetime.datetime.now()
        delta = now - item.updated_at
        return delta.days >= item.expire_days

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
