import configparser
from pathlib import Path
from typing import Optional

MINATO_ROOT = Path.home() / ".minato"
ROOT_CONFIG_FILENAME = MINATO_ROOT / "config.ini"
LOCAL_CONFIG_FILENAME = Path.cwd() / "minato.ini"


class Config:
    DEFAULT_CONFIG = {
        "DEFAULT": {
            "minato_root": MINATO_ROOT,
            "cache_directory": MINATO_ROOT / "cache",
            "sqlite_database": MINATO_ROOT / "minato.db",
        }
    }

    def __init__(self, filename: Optional[Path] = None) -> None:
        self._config = configparser.ConfigParser()
        # Read default config
        self._config.read_dict(Config.DEFAULT_CONFIG)
        # Read root config file
        if ROOT_CONFIG_FILENAME.exists():
            with ROOT_CONFIG_FILENAME.open("r") as config_file:
                self._config.read_file(config_file)
        # Read local config file
        if LOCAL_CONFIG_FILENAME.exists():
            with LOCAL_CONFIG_FILENAME.open("r") as config_file:
                self._config.read_file(config_file)
        # Read user config file
        if filename is not None and filename.exists():
            with filename.open("r") as config_file:
                self._config.read_file(config_file)

    @property
    def minato_root(self) -> Path:
        return Path(self._config["DEFAULT"]["minato_root"])

    @property
    def cache_directory(self) -> Path:
        return Path(self._config["DEFAULT"]["cache_directory"])

    @property
    def sqlite_database(self) -> Path:
        return Path(self._config["DEFAULT"]["sqlite_database"])
