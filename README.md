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
# 6.Fetch(potentially multiple paths' contents
fs.cat("oci://<bucket>@<namespace>/<prefix>/file.txt")
# 7.Create empty file or truncate in OCI object storage bucket
fs.touch("oci://<bucket>@<namespace>/<refix>/file.txt", truncate=True)
# 8.Get the size in bytes of a file
fs.size("oci://<bucket>@<namespace>/<prefix>/file.txt")
# 9.Get the Size in bytes of each file in a list of paths
fs.sizes(["oci://<bucket>@<namespace>/<prefix>/file.txt", "oci://<bucket>@<namespace>/<prefix>/file_2.txt"])
# 10. Get the created timestamp of a file as a datetime.datetime
fs.created(path="oci://<bucket>@<namespace>/<prefix>/file.txt")
# 11. Get the modified timestamp of a file as a datetime.datetime
fs.modified(path="oci://<bucket>@<namespace>/<prefix>/file.txt")
# 12. Get the space used by files and optionally directories within a path
fs.du(path="oci://<bucket>@<namespace>/<prefix>/file.txt")
# 13.Get the contents of byte ranges from one or more files
fs.cat_ranges(["oci://<bucket>@<namespace>/<prefix>/file.txt"], starts=[None, None, None],ends=[None, None, None], on_error="return")
# 14. Copy within two locations in the filesystem
fs.cp(path1="oci://<bucket>@<namespace>/<prefix>/file.txt", path2="oci://<bucket>@<namespace>/<prefix>/file.txt", destination_region="<region>")
# 15 .Hash of file properties, to tell if it has changed
fs.ukey("oci://<bucket>@<namespace>/<prefix>/file.txt")
# 16.Is this entry directory-like?
fs.isdir("oci://<bucket>@<namespace>")
# 17.Is this entry file-like?
fs.isfile("oci://<bucket>@<namespace>/<prefix>/file.txt")
# 18.If there is a file at the given path (including broken links)
fs.lexists("oci://<bucket>@<namespace>/<prefix>/file.txt")
# 19.Get directory listing with details
fs.listdir("oci://<bucket>@<namespace>/<prefix>/", detail=True)
# 20.Get the first ``size`` bytes from file
fs.head("oci://<bucket>@<namespace>/<prefix>/file.txt", size=1024)
# 21.Get the last ``size`` bytes from file
fs.tail("oci://<bucket>@<namespace>/<prefix>/file.txt", size=1024)
# 22.Get the contents of the file as a byte
fs.read_bytes("oci://<bucket>@<namespace>/<prefix>/file.txt", start=0, end=100)
# 23.Get the contents of the file as a string
fs.read_text("oci://<bucket>@<namespace>/<prefix>/file.txt", encoding=None, errors=None, newline=None)
# 24.Delete a file from the bucket
fs.rm("oci://<bucket>@<namespace>/<prefix>/file.txt")
# 25.Create bucket if it doesn't exist
fs.mkdir("oci://<bucket>@<namespace>", create_parents=True, compartment_id="<compartment id>")
# 26.Create bucket if it doesn't exist
fs.sign("oci://<bucket>@<namespace>/prefix/file.txt",expires_in=10, name="par_pdf", object_operation="ObjectRead")
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

# Installation
## Install from source
You can download the `oci-fsspec` library from Github and install locally:
```bash
git clone https://github.com/furqan-shaikh/oci-fsspec.git
cd oci-fsspec
pip install .
```

## Software Prerequisites
Python >= 3.9

## Design
For technical details, please refer [here](./docs/README.md)




