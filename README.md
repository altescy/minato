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

### Python

```python
import minato

# Read / write files on online storage
with minato.open("s3://your_bucket/path/to/file", "w") as f:
    f.write("Create a new file on AWS S3!")

# Cache & manage online resources in local storage
local_filename = minato.cached_path("http://example.com/path/to/file")
```

### CLI

```
$ minato --help
usage: minato

positional arguments:
  {add,download,list,remove,update,upload}
    add                 add local cache file from url
    download            download file to local
    list                show list of cached files
    remove              remove cached files
    update              update cached files
    upload              upload local file to remote

optional arguments:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
```
