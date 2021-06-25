import logging
import os

from minato.commands import main

if os.environ.get("MINATO_DEBUG"):
    LEVEL = logging.DEBUG
else:
    level_name = os.environ.get("MINATO_LOG_LEVEL", "WARNING")
    LEVEL = logging._nameToLevel.get(level_name, logging.WARNING)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", level=LEVEL
)


def run() -> None:
    main(prog="minato")


if __name__ == "__main__":
    run()
