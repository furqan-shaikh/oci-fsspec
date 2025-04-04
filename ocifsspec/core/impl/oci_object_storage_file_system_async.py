import asyncio

from fsspec.asyn import AsyncFileSystem


from ocifsspec.core.impl.oci_object_storage_file_system import OCIObjectStorageFileSystem
from ocifsspec.core.models.constants import CURRENT_REGION_KEY, OCI_AUTHENTICATION_TYPE_KEY
from ocifsspec.core.oci_object_storage.object_storage_client import get_object_storage_client

async_methods = [
    "_ls",
    "_info",
    "_cat_file"
]

def async_wrapper(func):
    async def wrapper(*args, **kwargs):
        return await asyncio.to_thread(func, *args, **kwargs)
    return wrapper

class OCIObjectStorageFileSystemAsync(AsyncFileSystem):
    protocol = "oci"
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.region = kwargs.get(CURRENT_REGION_KEY)
        self.object_storage_client = get_object_storage_client(kwargs.get(OCI_AUTHENTICATION_TYPE_KEY))
        self.fs = OCIObjectStorageFileSystem(*args, **kwargs)

        # for method_name in async_methods:
        #     method = getattr(self, method_name)
        #     setattr(self, method_name, async_wrapper(method))

    async def _to_async_wrapper(self, method, *args, **kwargs):
        return await asyncio.to_thread(method, *args, **kwargs)

    async def _ls(self, path: str, detail: bool = True, **kwargs) -> list:
        """
        :param path: of the format oci://bucket@namespace/path/to/file
        :param detail:
        :param kwargs:
                limit: integer value depicting the page size. When not set, fetches default page size
        :return:
        """
        return await self._to_async_wrapper(self.fs.ls, path, detail, **kwargs)

    async def _info(self, path, **kwargs):
        """
        Give details of entry at path. Returns a single dictionary, with exactly the same information as `ls` would with detail=True.
        The default implementation calls ls and could be overridden by a shortcut. kwargs are passed on to `ls().

        Some file systems might not be able to measure the file’s size, in which case, the returned dict will include 'size': None.
        :param path:
        :param kwargs:
        :return:
        """
        return await self._to_async_wrapper(self.fs.info, path, **kwargs)

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        return await self._to_async_wrapper(self.fs.cat_file, path, start, end, **kwargs)








