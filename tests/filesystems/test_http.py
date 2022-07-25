from minato.filesystems import HttpFileSystem


def test_open_file() -> None:
    url = "https://raw.githubusercontent.com/altescy/minato/main/tests/fixtures/hello.txt"

    fs = HttpFileSystem(url)
    with fs.open_file("r") as fp:
        text = fp.read().strip()
        assert text == "Hello, world!"


def test_get_version() -> None:
    url = "https://raw.githubusercontent.com/altescy/minato/main/tests/fixtures/hello.txt"

    fs = HttpFileSystem(url)
    version = fs.get_version()

    assert isinstance(version, str)
