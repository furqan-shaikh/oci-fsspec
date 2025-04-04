# oci-fsspec
`oci-fsspec` is a Pythonic file interface to OCI Object Storage.
`oci-fsspec` uses and is based upon [fsspec](https://github.com/fsspec/filesystem_spec)

# Examples of file system usage
```python
import fsspec
from ocifsspec.core.auth.user_token_authentication import UserTokenAuthentication
from ocifsspec.core.models.constants import PROTOCOL

fs = fsspec.filesystem(PROTOCOL, authentication=UserTokenAuthentication())

# 1. Get directory listing with details
fs.ls("oci://<bucket>@<namespace>/<prefix>/", detail=True)
# 2.Get metadata about a file
fs.info("oci://<bucket>@<namespace>/<prefix>/file.txt")
# 3.Open a file for writing/flushing into file in OCI Object Storage bucket
with fs.open("oci://<bucket>@<namespace>/<prefix>/file.txt", 'w', autocommit=True) as f:
        f.write("Some content")
        f.flush()
# 4.Open a file for reading a file from OCI Object Storage bucket
with fs.open("oci://<bucket>@<namespace>/<prefix>/file.txt") as f:
        print(f.read())
# 5.Find files by glob-matching
fs.glob("oci://<bucket>@<namespace>/<prefix>/*.txt")
```

# Integration
Popular libraries like `pandas`, `dask` accept URLs with the prefix “oci://”, and will use `oci-fsspec` to complete the IO operations.
The IO functions take an argument `storage_options`, which is passed as is to `oci-fsspec`. 

## With pandas
```python
import pandas as pd
from ocifsspec.core.auth.user_token_authentication import UserTokenAuthentication
pd.read_csv("oci://<bucket>@<namespace>/<object.csv>", storage_options={"authentication": UserTokenAuthentication()})
```
## Wth dask
```python
from dask import dataframe as dd
from ocifsspec.core.auth.user_token_authentication import UserTokenAuthentication
ddf = dd.read_csv("oci://<bucket>@<namespace>/<object.csv>", storage_options={"authentication": UserTokenAuthentication()})
ddf.compute().to_csv("oci://<bucket>@<namespace>/<object.csv>",storage_options={"authentication": UserTokenAuthentication()})
```

# Async
`oci-fsspec` offers async functionality for some IO operations.
To use async mode in `oci-fsspec`, set `RUN_MODE` environment variable as:
```bash
export RUN_MODE=async
```
Examples of using async are shown below:
```python
import fsspec
from ocifsspec.core.auth.user_token_authentication import UserTokenAuthentication
async def ls(path):
    fs = fsspec.filesystem("oci", storage_options={"authentication": UserTokenAuthentication()})
    return await fs._ls(path=path)
```

## Getting Started
1. Clone the project
2. Setup virtual environment
3. Install the prerequisites
```bash
pip install .
```

## Software Prerequisites
Python >= 3.9




