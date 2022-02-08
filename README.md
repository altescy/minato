Minato
======

[![Actions Status](https://github.com/altescy/minato/workflows/CI/badge.svg)](https://github.com/altescy/minato/actions/workflows/ci.yml)
[![Python version](https://img.shields.io/pypi/pyversions/minato)](https://github.com/altescy/minato)
[![License](https://img.shields.io/github/license/altescy/minato)](https://github.com/altescy/minato/blob/master/LICENSE)
[![pypi version](https://img.shields.io/pypi/v/minato)](https://pypi.org/project/minato/)

Cache & file system for online resources in Python


## Features

Minato enables you to:
- Download & cache online recsources
  - minato supports the following protocols: HTTP(S) / AWS S3 / Google Cloud Storage
  - You can manage cached files via command line interface
- Automatically update cached files based on ETag
  - minato downloads new versions if available when you access cached files
- Open online files super easily
  - By using `minato.open`, you can read/write online resources like the built-in `open` method

## Installation

```
pip install minato[all]
```

## Usage

### Python

```python
import minato

# Read / write files on online storage
with minato.open("s3://your_bucket/path/to/file", "w") as f:
    f.write("Create a new file on AWS S3!")

# Cache & manage online resources in local storage
local_filename = minato.cached_path("http://example.com/path/to/archive.zip!inner/path/to/file")
```

### CLI

```
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
