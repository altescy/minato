Minato
======

[![Actions Status](https://github.com/altescy/minato/workflows/CI/badge.svg)](https://github.com/altescy/minato/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/altescy/minato)](https://github.com/altescy/minato/blob/master/LICENSE)

Cache & file system for online resources in Python 


## Installation

```
$ pip install git+https://github.com/altescy/minato.git
```

## Usage

```python
import minato

# Read / write files on online storage by PyFilesystem2
with minato.open("s3://your_bucket/path/to/file", "w") as f:
    f.write("Create a new file on AWS S3!")

# Cache & manage online resources in local storage
local_filename = minato.cached_path("http://example.com/path/to/file")
```
