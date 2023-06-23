from pathlib import Path

from minato.filesystems import HuggingfaceHubFileSystem


def test_download_file(tmp_path: Path) -> None:
    url = "hf://datasets/hf-internal-testing/test-dataset/test.tmp"

    fs = HuggingfaceHubFileSystem(url)
    fs.download(tmp_path)

    assert (tmp_path / "test.tmp").is_file()


def test_download_dir_with_trailing_slash(tmp_path: Path) -> None:
    url = "hf://datasets/hf-internal-testing/test-dataset/"

    fs = HuggingfaceHubFileSystem(url)
    fs.download(tmp_path)

    assert (tmp_path / "test.tmp").is_file()
    assert (tmp_path / ".gitattributes").is_file()


def test_download_dir_without_trailing_slash(tmp_path: Path) -> None:
    url = "hf://datasets/hf-internal-testing/test-dataset"

    fs = HuggingfaceHubFileSystem(url)
    fs.download(tmp_path)

    assert (tmp_path / "test-dataset").is_dir()
    assert (tmp_path / "test-dataset" / "test.tmp").is_file()
    assert (tmp_path / "test-dataset" / ".gitattributes").is_file()


def test_exists() -> None:
    assert HuggingfaceHubFileSystem("hf://datasets/hf-internal-testing/test-dataset/test.tmp").exists()
    assert not HuggingfaceHubFileSystem("hf://datasets/hf-internal-testing/test-dataset/NEVER_EXIST").exists()


def test_get_version() -> None:
    url = "hf://datasets/hf-internal-testing/test-dataset/test.tmp"
    version = HuggingfaceHubFileSystem(url).get_version()
    assert isinstance(version, str)
