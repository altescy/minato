# ⚓ Minato

[![Actions Status](https://github.com/altescy/minato/workflows/CI/badge.svg)](https://github.com/altescy/minato/actions/workflows/ci.yml)
[![Python version](https://img.shields.io/pypi/pyversions/minato)](https://github.com/altescy/minato)
[![License](https://img.shields.io/github/license/altescy/minato)](https://github.com/altescy/minato/blob/master/LICENSE)
[![pypi version](https://img.shields.io/pypi/v/minato)](https://pypi.org/project/minato/)

A Unified File I/O Library for Python


Minato is a Python library that provides a unified and simple interface to work with local and remote files, as well as compressed and archived files.
With Minato, you can seamlessly read and write files from various sources like local filesystem, HTTP(S), Amazon S3, Google Cloud Storage, and Hugging Fase Hub.
It also supports reading and writing compressed files such as gzip, bz2, and lzma, as well as directly accessing files inside archives like zip and tar.

One of Minato's key features is its built-in caching mechanism, which allows you to cache remote files locally, and manage the cache with a provided CLI.
The cache is automatically updated based on ETag headers, ensuring that you always work with the latest version of the files.

## Features

- Unified file I/O for local and remote files (HTTP(S), S3, GCP, Hugging Face Hub)
- Support for reading and writing compressed files (gzip, bz2, lzma)
- Direct access to files inside archives (zip, tar)
- Local caching of remote files with cache management CLI
- Automatic cache updates based on ETag headers

## Installation

Install Minato using pip:

```bash
pip install minato                   # minimal installation for only local/http(s) file I/O
pip install minato[s3]               # for Amazon S3
pip install minato[gcs]              # for Google Cloud Storage
pip install minato[huggingface-hub]  # for Hugging Face Hub
pip install minato[all]              # for all supported file I/O
```

## Usage

### Quick Start

Here's a simple example demonstrating how to read and write files on online storage:

```python
import minato

# Write a file to an S3 bucket
s3_path = "s3://your_bucket/path/to/file"
with minato.open(s3_path, "w") as f:
    f.write("Create a new file on AWS S3!")
```

Access cached online resources in local storage:

```python
# Cache a remote file and get its local path
remote_path = "http://example.com/path/to/archive.zip!inner/path/to/file"
local_filename = minato.cached_path(remote_path)
```

Access files inside archives like zip by connecting the archive path and inner file path with an exclamation mark (`!`) like above.

Automatically decompress files with gzip / lzma / bz2 compression:

```python
with minato.open("data.txt.gz", "rt", decompress=True) as f:
    content = f.read()
```

In the example above, Minato will automatically detect the file format based on the file's content or filename and decompress the file accordingly.

### Cache Management

```bash
❯ poetry run minato --help
usage: minato

positional arguments:
  {cache,list,remove,update}
    cache               cache remote file and return cached local file path
    list                show list of cached files
    remove              remove cached files
    update              update cached files

optional arguments:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
```
