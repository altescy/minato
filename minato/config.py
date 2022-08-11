from __future__ import annotations

import dataclasses
from configparser import ConfigParser
from os import PathLike
from pathlib import Path

MINATO_ROOT = Path.home() / ".minato"
DEFAULT_CACHE_ROOT = MINATO_ROOT / "cache"
ROOT_CONFIG_PATH = MINATO_ROOT / "config.ini"
LOCAL_CONFIG_PATH = Path.cwd() / "minato.ini"


@dataclasses.dataclass
class Config:
    cache_root: Path = DEFAULT_CACHE_ROOT
    expire_days: int = -1
    auto_update: bool = True
    selector_command: str | None = None

    @classmethod
    def load(
        cls,
        cache_root: str | PathLike | None = None,
        expire_days: int | None = None,
        auto_update: bool | None = None,
        files: list[str | PathLike] | None = None,
    ) -> Config:
        if files is None:
            files = [ROOT_CONFIG_PATH, LOCAL_CONFIG_PATH]

        config = cls()
        config.read_files(files)
        if cache_root is not None:
            config.cache_root = Path(cache_root)
        if expire_days is not None:
            config.expire_days = expire_days
        if auto_update is not None:
            config.auto_update = auto_update

        return config

    def read_files(self, files: list[str | PathLike]) -> None:
        parser = ConfigParser()
        parser.read([str(path) for path in files])
        self._update_from_configparser(parser)

    def _update_from_configparser(self, parser: ConfigParser) -> None:
        if parser.has_section("cache"):
            section = parser["cache"]
            if "root" in section:
                self.cache_root = Path(parser["cache"]["root"])
            if "expire_days" in section:
                self.expire_days = parser.getint("cache", "expire_days")
            if "auto_update" in section:
                self.auto_update = parser.getboolean("cache", "auto_update")
        if parser.has_section("ui"):
            section = parser["ui"]
            if "selector_command" in section:
                self.selector_command = section["selector_command"]
