import configparser
from pathlib import Path
from typing import Optional


class Config:
    ROOT_CONFIG_FILENAME = Path.home() / ".minato" / "config.ini"
    LOCAL_CONFIG_FILENAME = Path.cwd() / "minato.ini"
    DEFAULT_CONFIG = {
        "DEFAULT": {
            "cache_directory": Path.home() / ".minato" / "cache",
            "sqlite_database": Path.home() / ".minato" / "minato.db",
        }
    }

    def __init__(self, filename: Optional[Path] = None) -> None:
        self._config = configparser.ConfigParser()
        # Read default config
        self._config.read_dict(Config.DEFAULT_CONFIG)
        # Read root config file
        if Config.ROOT_CONFIG_FILENAME.exists():
            with Config.ROOT_CONFIG_FILENAME.open("r") as config_file:
                self._config.read_file(config_file)
        # Read local config file
        if Config.LOCAL_CONFIG_FILENAME.exists():
            with Config.LOCAL_CONFIG_FILENAME.open("r") as config_file:
                self._config.read_file(config_file)
        # Read user config file
        if filename is not None and filename.exists():
            with filename.open("r") as config_file:
                self._config.read_file(config_file)

    @property
    def cache_directory(self) -> Path:
        return Path(self._config["DEFAULT"]["cache_directory"])

    @property
    def sqlite_database(self) -> Path:
        return Path(self._config["DEFAULT"]["sqlite_database"])
