import datetime

import oci
from oci.config import from_file
from ocifsspec.core.auth.session_token_authentication import SessionTokenAuthentication
from ocifsspec.core.auth.user_token_authentication import UserTokenAuthentication
from ocifsspec.core.exceptions.oci_authentication_error import OCIAuthenticationError
from ocifsspec.core.models.object_storage_name import ObjectStorageName


def get_object_storage_client(authentication):
    if isinstance(authentication, SessionTokenAuthentication):
        # read profile from oci config file
        config = from_file(file_location=authentication.config_path,
                                      profile_name=authentication.profile_name)
        token_file = config['security_token_file']
        token = None
        with open(token_file, 'r') as f:
            token = f.read()
        private_key = oci.signer.load_private_key_from_file(config['key_file'])
        signer = oci.auth.signers.SecurityTokenSigner(token, private_key)
        return oci.object_storage.ObjectStorageClient(config=config, signer=signer)
    if isinstance(authentication, UserTokenAuthentication):
        config = from_file(file_location=authentication.config_path,
                           profile_name=authentication.profile_name)
        return oci.object_storage.ObjectStorageClient(config)
    else:
        raise OCIAuthenticationError("Invalid authentication type")

def get_work_request(object_storage_client, work_request_id: str):
    get_work_request_response = object_storage_client.get_work_request(
        work_request_id=work_request_id)

    # Get the data from response
    return get_work_request_response.data


def get_create_multipart_upload_details(object_name: str):
    return oci.object_storage.models.CreateMultipartUploadDetails(
        object=object_name
    )

def get_create_bucket_details(object_storage_name: ObjectStorageName, compartment_id: str, **kwargs):
    return oci.object_storage.models.CreateBucketDetails(
        name=object_storage_name.bucket,
        compartment_id=compartment_id,
        **kwargs
    )

def get_create_preauthenticated_request_details(name: str, object_name: str, access_type: str, expires_in: int):
    return oci.object_storage.models.CreatePreauthenticatedRequestDetails(
        name=name,
        access_type=access_type,
        object_name=object_name,
        time_expires=_get_rfc3339_time(expires_in))

def _get_rfc3339_time(expires_in: int):
    """
    Gets the time_expires of this CreatePreauthenticatedRequestDetails.
    The expiration date for the pre-authenticated request as per RFC 3339.
    :param expires_in: time in seconds
    :return: str in RFC 3339 format
    """
    # Get current UTC time.
    # Add expires seconds to it.
    # Format it as an RFC3339 string (YYYY-MM-DDTHH:MM:SSZ).
    expiry_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=expires_in)
    return expiry_time.replace(microsecond=0).isoformat() + "Z"