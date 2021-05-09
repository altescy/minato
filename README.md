SidePocket
======

[![Actions Status](https://github.com/altescy/sidepocket/workflows/CI/badge.svg)](https://github.com/altescy/sidepocket/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/altescy/sidepocket)](https://github.com/altescy/sidepocket/blob/master/LICENSE)


```python
import sidepocket

# Read / write files on online storage by PyFilesystem2
with sidepocket.open("s3://your_bucket/path/to/file", "w") as f:
    f.write("Create a new file on AWS S3!")

# Cache & manage online resources in local storage
local_filename = sidepocket.cached_path("http://example.com/path/to/file")
```
