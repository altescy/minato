import minato
from minato.config import Config


def test_version() -> None:
    assert minato.__version__ == "0.1.0"


def test_open() -> None:
    with minato.open(
        "https://raw.githubusercontent.com/altescy/xsklearn/main/README.md"
    ) as fp:
        text = fp.readline().strip()

    assert text == "xsklearn"


def test_cached_path() -> None:
    path = minato.cached_path(
        "https://raw.githubusercontent.com/altescy/xsklearn/main/README.md"
    )

    assert path.exists()
    assert path.parent == Config().cache_directory