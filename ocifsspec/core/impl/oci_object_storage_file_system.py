import oci
from fsspec import AbstractFileSystem
from fsspec.utils import tokenize
from oci.exceptions import ClientError, ServiceError

from ocifsspec.core.models.constants import OCI_AUTHENTICATION_TYPE_KEY, PROTOCOL, LIST_OBJECTS_PAGE_SIZE, \
    LIST_OBJECTS_PAGE_SIZE_KEY, DESTINATION_REGION_KEY, CURRENT_REGION_KEY, OPC_WORK_REQUEST_ID_KEY, OPC_REQUEST_ID_KEY, \
    DATE_KEY
from ocifsspec.core.models.copy_response import CopyResponse
from ocifsspec.core.oci_object_storage.object_storage_client import get_object_storage_client, \
    get_create_multipart_upload_details, get_create_bucket_details, get_create_preauthenticated_request_details

from fsspec.spec import AbstractBufferedFile

from ocifsspec.core.models.object_storage_name import ObjectStorageName


class OCIObjectStorageFile(AbstractBufferedFile):
    def __init__(self, fs: "OCIObjectStorageFileSystem", path: str, mode: str ,object_storage_name: ObjectStorageName, **kwargs):
        super().__init__(fs, path, mode, **kwargs)
        self.path = path
        self.mode = mode
        self.object_storage_name = object_storage_name
        self.upload_id = None
        self.parts = []

    def _initiate_upload(self):
        # if buffer size < block size, just return, since its a small file and doesn't require multi-part upload
        if self.buffer.tell() < self.blocksize:
            return

        # 1. Initiate Multipart Upload
        try:
            response = self.fs.object_storage_client.create_multipart_upload(namespace_name=self.object_storage_name.namespace,
                                                                             bucket_name=self.object_storage_name.bucket,
                                                                             create_multipart_upload_details=get_create_multipart_upload_details(
                                                                                object_name=self.object_storage_name.object_name
                                                                             ))
            self.upload_id = response.data.upload_id
            print(f"Upload Id: {self.upload_id}")
        except ClientError as e:
            print(f"Error initiating multipart upload: {e}")
            raise e

    def _upload_chunk(self, final:bool = False):
        if self.autocommit and final and self.buffer.tell() < self.blocksize:
            part_data = False
        else:
            # 1. Do a read on the buffer based on the blocksize. For eg: if blocksize is 1 MB and file size is 17 MB
            # 2. Keep reading till there are no more data left
            # 3     For every block of data read, do a multi-part upload, save each part in parts
            # 4. if autocommit and final, call commit, else return not final
            # 2. Upload Part
            self.buffer.seek(0)
            part_data = self.buffer.read(self.blocksize)
        while part_data:
            try:
                response = self.fs.object_storage_client.upload_part(
                    namespace_name=self.object_storage_name.namespace,
                    bucket_name=self.object_storage_name.bucket,
                    object_name=self.object_storage_name.object_name,
                    upload_id=self.upload_id,
                    upload_part_num=len(self.parts) + 1,
                    upload_part_body=part_data
                )
                # Store the part information for completion
                self.parts.append(oci.object_storage.models.CommitMultipartUploadPartDetails(
                    part_num=len(self.parts) + 1,
                    etag=response.headers['etag']))
                print(f"Uploaded Part: {len(self.parts) + 1}")
                part_data = self.buffer.read(self.blocksize)
            except ClientError as e:
                print(f"Error uploading part: {e}")
                return False
        if self.autocommit and final:
            self.commit()
        return not final


    def commit(self):
        if len(self.parts) == 0:
            # make sure the buffer's position is at the beginning.
            # If you’ve already written data to the buffer, the current position might be at the end.
            # To reset it to the beginning
            self.buffer.seek(0)
            self.fs.object_storage_client.put_object(namespace_name=self.object_storage_name.namespace,
                                          bucket_name=self.object_storage_name.bucket,
                                          object_name=self.object_storage_name.object_name,
                                          put_object_body=self.buffer,
                                          content_length=self.buffer.tell())
        else:
            # requires multi-part upload
            # 3. Complete Multipart Upload
            print(f"commit for multi-part upload: {self.upload_id}")
            try:
                response = self.fs.object_storage_client.commit_multipart_upload(
                                namespace_name=self.object_storage_name.namespace,
                                bucket_name=self.object_storage_name.bucket,
                                object_name=self.object_storage_name.object_name,
                                upload_id=self.upload_id,
                                commit_multipart_upload_details=oci.object_storage.models.CommitMultipartUploadDetails(
                                    parts_to_commit=self.parts)
                            )
                print(f"Multipart upload completed: {response.headers['etag']}")
            except ClientError as e:
                    print(f"Error completing multipart upload: {e}")

    def discard(self):
        if len(self.parts) != 0 or self.upload_id:
            try:
                print(f"Aborting Multi-part upload: {self.upload_id}")
                self.fs.object_storage_client.abort_multipart_upload(namespace_name=self.object_storage_name.namespace,
                                                                                bucket_name=self.object_storage_name.bucket,
                                                                                object_name=self.object_storage_name.object_name,
                                                                                upload_id=self.upload_id)
                print(f"Successfully aborted Multipart upload: {self.upload_id}")
            except ServiceError as e:
                raise e
            finally:
                self._cleanup_multipart_upload()

    def _cleanup_multipart_upload(self):
        self.buffer = None
        self.parts = []
        self.upload_id = None



class OCIObjectStorageFileSystem(AbstractFileSystem):
    protocol = PROTOCOL
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.region = kwargs.get(CURRENT_REGION_KEY)
        self.object_storage_client = get_object_storage_client(kwargs.get(OCI_AUTHENTICATION_TYPE_KEY))

    def ls(self, path: str, detail: bool = True, **kwargs) -> list:
        """
        :param path: of the format oci://bucket@namespace/path/to/file
        :param detail:
        :param kwargs:
                limit: integer value depicting the page size. When not set, fetches default page size
        :return:
        """

        # https://docs.oracle.com/en-us/iaas/tools/python-sdk-examples/2.149.1/objectstorage/list_objects.py.html
        object_storage_name = self._parse_path_2(path)
        limit=kwargs.get(LIST_OBJECTS_PAGE_SIZE_KEY, LIST_OBJECTS_PAGE_SIZE)
        results = []
        has_more_page=True
        while has_more_page:
            response, has_more_page = self._get_page_data(object_storage_name=object_storage_name,
                                                          detail=detail,
                                                         limit=limit)
            results.extend(response)
        return results

    def info(self, path, **kwargs):
        """
        Give details of entry at path. Returns a single dictionary, with exactly the same information as `ls` would with detail=True.
        The default implementation calls ls and could be overridden by a shortcut. kwargs are passed on to `ls().

        Some file systems might not be able to measure the file’s size, in which case, the returned dict will include 'size': None.
        :param path:
        :param kwargs:
        :return:
        """
        object_storage_name = self._parse_path_2(path)
        if object_storage_name.object_name:
            try:
                return self._head_object(path=path, object_storage_name=object_storage_name,**kwargs)
            except Exception as e:
                raise e
        return self._get_directory_object(path=path)

    def cat_file(self, path, start=None, end=None, **kwargs) -> bytes:
        """
        Get the content of a file
        :param path: URL of file on object storage
        :param start:Bytes limits of the read. Negative range is not supported
        :param end:Bytes limits of the read. Negative range is not supported
        :param kwargs:
        :return: raw bytes of the file content
        """
        print(f"cat_file - path: {path}, start: {start}, end: {end}")
        object_storage_name = self._parse_path_2(path)
        kwargs = {
            "namespace_name": object_storage_name.namespace,
            "bucket_name": object_storage_name.bucket,
            "object_name": object_storage_name.object_name
        }
        range_bytes = self.get_bytes_range(start, end)
        if range_bytes:
            kwargs["range"] = range_bytes
        get_object_response = self.object_storage_client.get_object(**kwargs)

        # Get the content from response in bytes
        return get_object_response.data.content

    def cat(self, path, recursive=False, on_error="raise", **kwargs):
        return super().cat(path=path, recursive=recursive,on_error=on_error,**kwargs)

    def touch(self, path, truncate=True, **kwargs):
        """Create empty file

        Parameters
        ----------
        path: str
            file location
        truncate: bool
            If True, always set file size to 0; if False, raise ValueError as OCI Object Storage doesn't support touching existing files
        """
        try:
            object_storage_name = self._parse_path_2(path=path)
            if not object_storage_name.object_name:
                raise ValueError(
                f"The path {path} seems to be a new bucket. We do not support creating "
                "buckets with touch"
            )
            super().touch(path=path, truncate=truncate, **kwargs)
        except NotImplementedError:
            raise ValueError("OCI Object Storage does not support touching existent files")

    def checksum(self, path: str):
        """
        Unique value for current version of file.
        If the checksum is the same from one moment to another, the contents are guaranteed to be the same.
        If the checksum changes, the contents might have changed.
        :param path:
        :return: checksum in integer format
        """

        info_response = self.info(path=path)
        first_file = info_response[0]
        tokenize_request_value = first_file.get("etag", info_response)
        hash_value = tokenize(tokenize_request_value)
        # convert hexadecimal value to integer value
        return int(hash_value, 16)

    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime"""
        return self._get_time(path=path, key="timeCreated")

    def modified(self, path):
        """Return the modified timestamp of a file as a datetime.datetime"""
        return self._get_time(path=path, key="lastModified")

    def _get_time(self, path, key):
        response = self.info(path=path)
        time = None
        if response["type"] == "file":
            time = response[key]
        return time

    def copy(self,path1, path2, recursive=False, maxdepth=None, on_error=None, **kwargs) -> CopyResponse:
        source_object_storage_name = self._parse_path_2(path1)
        destination_object_storage_name = self._parse_path_2(path2)
        destination_region = kwargs.get(DESTINATION_REGION_KEY,self.region)
        if not destination_region:
            raise ValueError(
                "No region specified. Please set the 'destination_region' parameter in the kwargs."
            )

        copy_object_response = self.object_storage_client.copy_object(
            namespace_name=source_object_storage_name.namespace,
            bucket_name=source_object_storage_name.bucket,
            copy_object_details=oci.object_storage.models.CopyObjectDetails(
                source_object_name=source_object_storage_name.object_name,
                destination_region=destination_region,
                destination_namespace=destination_object_storage_name.namespace,
                destination_bucket=destination_object_storage_name.bucket,
                destination_object_name=destination_object_storage_name.object_name))

        return CopyResponse(
            opc_work_request_id=copy_object_response.headers[OPC_WORK_REQUEST_ID_KEY],
            opc_request_id=copy_object_response.headers[OPC_REQUEST_ID_KEY],
            date=copy_object_response.headers[DATE_KEY],
        )

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        return OCIObjectStorageFile(fs=self, path=path, mode=mode, object_storage_name=self._parse_path_2(path))

    def open(self, urlpath, mode='rb', compression=None, encoding='utf8', errors=None, protocol=None, newline=None, expand=None, **kwargs):
        """
        :param urlpath:string or list. Absolute or relative filepath. Prefix with a protocol like oci:// to read from alternative filesystems. Should not include glob character(s).
        :param mode:‘rb’, ‘wb’, etc.
        :param compression:
        :param encoding:
        :param errors:
        :param protocol:
        :param newline:
        :param expand:
        :param kwargs:
        :return:
            OpenFile object.
        Examples
        --------
        >>> openfile = open('oci://bucket/2015-01-01.csv.gz', compression='gzip')
        >>> with openfile as f:
                df = pd.read_csv(f)
        """


        # 1. User calls open passing path and wb mode
        # 2. open method is invoked
        # 3. return a file-like object which can buffer data
        # 4. user writes data using write on the returned file-like object: f.write
        # 5. When close is invoked, read the data from buffer and call OCI Object Storage::put_object method

        return super().open(path=urlpath, mode=mode, compression=compression, encoding=encoding)

    def rm(self, path, recursive=False, maxdepth=None):
        """Delete files.

        Parameters
        ----------
        path: str or list of str
            File(s) to delete.
        recursive: bool
            If file(s) are directories, recursively delete contents and then
            also remove the directory
        maxdepth: int or None
            Depth to pass to walk for finding files to delete, if recursive.
            If None, there will be no limit and infinite recursion may be
            possible.
        """
        object_storage_name = self._parse_path_2(path=path)
        if not object_storage_name.object_name:
            raise ValueError("Cannot delete the bucket")
        super().rm(path, recursive, maxdepth)

    def rm_file(self, path):
        """Delete a file"""
        object_storage_name = self._parse_path_2(path=path)
        if not object_storage_name.object_name:
            raise ValueError("Cannot delete the bucket")
        try:
            delete_object_response = self.object_storage_client.delete_object(
                namespace_name=object_storage_name.namespace,
                bucket_name=object_storage_name.bucket,
                object_name=object_storage_name.object_name)
            return delete_object_response.headers
        except Exception as e:
            print(f"Failed to delete file: {path}")
            raise e

    def mkdir(self, path:str, create_parents:bool=True, compartment_id: str = None, **kwargs):
        """
        Create bucket if it doesn't exist

        Parameters
        ----------
        path: str
            location
        create_parents: bool
            if True, this is equivalent to ``makedirs``
        compartment_id : str
            compartment in which to create the bucket
        kwargs : dict
            additional args for OCI
        """
        object_storage_name = self._parse_path_2(path=path)

        # create a bucket if create_parents is True or path has no prefix provided
        if create_parents or not object_storage_name.object_name:
            # create the bucket
            try:
                create_bucket_details = get_create_bucket_details(object_storage_name, compartment_id, **kwargs)
                self.object_storage_client.create_bucket(namespace_name=object_storage_name.namespace,
                                                         create_bucket_details=create_bucket_details
                                                         )
            except ServiceError as e:
                if e.code == "MissingCompartmentId":
                    raise e
                if e.code == "BucketAlreadyExists":
                    raise FileExistsError
                print(f"Bucket {object_storage_name.bucket} already exists")

    def sign(self, path: str, expiration: int=100 ,**kwargs):
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
        allowed_object_operations=["ObjectRead", "ObjectWrite", "ObjectReadWrite", "AnyObjectWrite", "AnyObjectRead", "AnyObjectReadWrite"]
        object_operation = kwargs.get("object_operation", allowed_object_operations[0])
        name = kwargs.get("name", "")
        if not name:
            raise ValueError(f"Name cannot be empty: {name}")
        object_storage_name = self._parse_path_2(path=path)
        if not object_storage_name.object_name:
            raise ValueError(f"Invalid path: {path}")
        try:
            create_preauthenticated_request_details = get_create_preauthenticated_request_details(name=name,
                                                                                                  object_name=object_storage_name.object_name,
                                                                                                  access_type=object_operation,
                                                                                                  expires_in=expiration)
            create_preauthenticated_request_response = self.object_storage_client.create_preauthenticated_request(
                namespace_name=object_storage_name.namespace,
                bucket_name=object_storage_name.bucket,
                create_preauthenticated_request_details=create_preauthenticated_request_details)

            # Get the data from response
            return create_preauthenticated_request_response.data
        except Exception as e:
            print(f"Failed to create PAR for : {path}, {e}")
            raise

    def _parse_path_2(self, path: str) -> ObjectStorageName:
        # oci://bucket@namespace/path/to/file

        # bucket@namespace/path/to/file
        full_bucket, _, obj_path = super()._strip_protocol(path).partition("/")
        # bucket@namespace, /path/to/file

        bucket, _, namespace = full_bucket.partition("@")
        object_path = obj_path.rstrip("/")
        return ObjectStorageName(namespace=namespace, bucket=bucket,object_name=object_path)

    def _parse_path(self, path: str, validate_path: bool = False) -> ObjectStorageName:
        """
        :param path: of the format oci://bucket@namespace/path/to/file
        :return:
        """
        # extract bucket, namespace and object_name from the path
        # if the path is invalid, throw ValueError error
        base_paths = path.split("://")
        # print(base_paths)
        if len(base_paths) != 2 or base_paths[0] != PROTOCOL:
            raise ValueError(f"Invalid path provided: {path}")

        # parse bucket, namespace and path
        bucket_namespace_path = base_paths[1].split("/")
        if len(bucket_namespace_path) <= 1:
            raise ValueError(f"Invalid path provided: {path}")

        # parse bucket and namespace
        bucket_namespace = bucket_namespace_path[0].split("@")
        if len(bucket_namespace_path) != 2:
            raise ValueError(f"Invalid path provided: {path}")

        bucket = bucket_namespace[0]
        namespace = bucket_namespace[1]

        if validate_path and len(bucket_namespace_path) == 1:
            raise ValueError(f"Invalid path provided: {path}")

        # bucket_namespace_path = ['bucket@namespace', 'path', 'to', 'file']
        # from the list, take all but first, join them into a string separated by /
        path = '/'.join(bucket_namespace_path[1:])  # All but the first element

        return ObjectStorageName(namespace=namespace, bucket=bucket,object_name=path)

    def _get_file_name(self, object_storage_name: ObjectStorageName, name: str) -> str:
        return f"{object_storage_name.bucket}@{object_storage_name.namespace}/{name}"

    def _head_object(self, path:str, object_storage_name: ObjectStorageName, **kwargs):
        try:
            head_object_response = self.object_storage_client.head_object(namespace_name=object_storage_name.namespace,
                                                                          bucket_name=object_storage_name.bucket,
                                                                          object_name=object_storage_name.object_name,
                                                                          **kwargs).headers
            return {
                "name": path,
                "type": "file",
                "size": int(head_object_response["Content-Length"]),
                "etag": head_object_response.get("etag"),
                "timeCreated": head_object_response.get("date"),
                "lastModified": head_object_response.get("last-modified"),
                "contentMd5": head_object_response.get("content-md5"),
                "storageTier": head_object_response.get("storage-tier"),
                "versionId": head_object_response.get("version-id"),
                "contentType": head_object_response.get("Content-Type"),
            }
        except ServiceError as e:
            if e.status == 404:
                try:
                    if self.object_storage_client.list_objects(namespace_name=object_storage_name.namespace,
                                                               bucket_name=object_storage_name.bucket,
                                                               prefix=object_storage_name.object_name.rstrip("/") + "/",
                                                               limit=1,
                                                               ).data.objects:
                        return self._get_directory_object(path=path)
                except Exception as e:
                    raise e

    def _get_page_data(self, object_storage_name: ObjectStorageName, detail: bool, limit: int):
        prefix = object_storage_name.object_name + "/" if object_storage_name.object_name else ""
        list_objects_response = self.object_storage_client.list_objects(
            namespace_name=object_storage_name.namespace,
            bucket_name=object_storage_name.bucket,
            prefix=prefix,
            delimiter="/",
            fields="name,size,etag,md5,timeCreated, timeModified",
            limit=limit)

        if list_objects_response.data.next_start_with is None:
            has_more_page = False
        else:
            has_more_page = True
        response = self._generate_results(list_objects_response=list_objects_response.data,
                               object_storage_name=object_storage_name,
                               detail=detail)

        if list_objects_response.data.prefixes:
            for i in range(len(list_objects_response.data.prefixes)):
                response.append(self._get_directory_object(path=self._get_file_name(object_storage_name,list_objects_response.data.prefixes[i])))

        return response, has_more_page

    def _generate_results(self, list_objects_response,
                          object_storage_name: ObjectStorageName,
                          detail: bool,
                          ):
        if detail is False:
            response = [{'name': self._get_file_name(object_storage_name, item.name)} for item in
                        list_objects_response.objects]
        else:
            response = [{'name': self._get_file_name(object_storage_name, item.name),
                         'size': item.size,
                         'etag': item.etag,
                         'md5': item.md5,
                         'time_created': item.time_created,
                         'time_modified': item.time_modified,
                         'type': 'file'
                         } for item in list_objects_response.objects]
        return response

    def _get_directory_object(self, path: str):
        return {"name": path, "size": 0, "type": "directory"}

    def get_bytes_range(self, start, end):
        # if either start or end is provided, generate the range parameter
        start_range = start if start is not None and start > 0 else ""
        end_range = end if end is not None and end > 0 else ""
        if start_range or end_range:
            # https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Range
            range_bytes = f"bytes={start_range}-{end_range}"
        else:
            range_bytes = ""
        return range_bytes






