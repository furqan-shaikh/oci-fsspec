import os

from fsspec import register_implementation

from ocifsspec.core.impl.oci_object_storage_file_system_async import OCIObjectStorageFileSystemAsync
from ocifsspec.core.impl.oci_object_storage_file_system import OCIObjectStorageFileSystem

# RUN_MODE: sync, async
RUN_MODE = os.getenv("RUN_MODE", "sync")
if RUN_MODE == "sync":
    register_implementation("oci", OCIObjectStorageFileSystem)
else:
    register_implementation("oci", OCIObjectStorageFileSystemAsync)
