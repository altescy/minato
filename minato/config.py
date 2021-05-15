import configparser
from pathlib import Path
from typing import Any, Dict, Optional

DEFAULT_MINATO_ROOT = Path.home() / ".minato"
ROOT_CONFIG_FILENAME = "config.ini"
LOCAL_CONFIG_FILENAME = "minato.ini"


class Config:
    def __init__(
        self,
        filename: Optional[Path] = None,
        minato_root: Optional[Path] = None,
    ) -> None:
        minato_root = minato_root or DEFAULT_MINATO_ROOT

        self._config = configparser.ConfigParser()

        # Read default config
        self._config.read_dict(self._default_config(minato_root))

        # Read root config file
        root_config_path = minato_root / ROOT_CONFIG_FILENAME
        if root_config_path.exists():
            with root_config_path.open("r") as config_file:
                self._config.read_file(config_file)

        # Read local config file
        local_config_path = Path.cwd() / LOCAL_CONFIG_FILENAME
        if local_config_path.exists():
            with local_config_path.open("r") as config_file:
                self._config.read_file(config_file)

        # Read user config file
        if filename is not None and filename.exists():
            with filename.open("r") as config_file:
                self._config.read_file(config_file)

    @staticmethod
    def _default_config(minato_root: Optional[Path]) -> Dict[str, Any]:
        minato_root = minato_root or DEFAULT_MINATO_ROOT
        return {
            "DEFAULT": {
                "minato_root": minato_root,
                "cache_directory": minato_root / "cache",
                "sqlite_database": minato_root / "minato.db",
            }
        }

    @property
    def minato_root(self) -> Path:
        return Path(self._config["DEFAULT"]["minato_root"])

    @property
    def cache_directory(self) -> Path:
        return Path(self._config["DEFAULT"]["cache_directory"])

    @property
    def sqlite_database(self) -> Path:
        return Path(self._config["DEFAULT"]["sqlite_database"])
