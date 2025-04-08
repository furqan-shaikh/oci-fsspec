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

    async def _touch(self, path, truncate=True, **kwargs):
        """Create empty file

        Parameters
        ----------
        path: str
            file location
        truncate: bool
             If True, always set file size to 0; if False, raise ValueError as OCI Object Storage doesn't support touching existing files
        """
        return await self._to_async_wrapper(self.fs.touch, path, truncate, **kwargs)

    async def _created(self, path, **kwargs):
        """Return the created timestamp of a file as a datetime.datetime"""
        return await self._to_async_wrapper(self.fs.created, path, **kwargs)

    async def _modified(self, path, **kwargs):
        """Return the modified timestamp of a file as a datetime.datetime"""
        return await self._to_async_wrapper(self.fs.modified, path, **kwargs)

    async def _mkdir(self, path:str, create_parents:bool=True, compartment_id: str = None, **kwargs):
        return await self._to_async_wrapper(self.fs.mkdir, path, create_parents, compartment_id, **kwargs)

    async def _sign(self, path: str, expiration: int=100 ,**kwargs):
        """Create a signed URL representing the given path

                Parameters
                ----------
                path : str
                     The path on the filesystem
                expiration : int
                    Number of seconds to enable the URL for (if supported)
                kwargs:
                    name: str
                        A user-specified name for the pre-authenticated request.
                        Names can be helpful in managing pre-authenticated requests. Avoid entering confidential information.
                    object_operation : str
                        The operation that can be performed on this resource.
                        Allowed values for this property are: “ObjectRead”, “ObjectWrite”, “ObjectReadWrite”, “AnyObjectWrite”, “AnyObjectRead”, “AnyObjectReadWrite”

                Returns
                -------
                Object : Dict
                    {
                          "access_type": "",
                          "access_uri": "",
                          "bucket_listing_action": null,
                          "full_path": "",
                          "id": "",
                          "name": "",
                          "object_name": "",
                          "time_created": "2025-04-08T12:35:29.912000+00:00",
                          "time_expires": "2025-04-08T12:35:38+00:00"
                    }

                """
        return await self._to_async_wrapper(self.fs.sign, path, expiration, **kwargs)








