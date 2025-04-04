import oci

from ocifsspec.core.auth.session_token_authentication import SessionTokenAuthentication
from ocifsspec.core.exceptions.oci_authentication_error import OCIAuthenticationError


def get_object_storage_client(authentication):
    if isinstance(authentication, SessionTokenAuthentication):
        # read profile from oci config file
        config = oci.config.from_file(file_location=authentication.config_path,
                                      profile_name=authentication.profile_name)
        token_file = config['security_token_file']
        token = None
        with open(token_file, 'r') as f:
            token = f.read()
        private_key = oci.signer.load_private_key_from_file(config['key_file'])
        signer = oci.auth.signers.SecurityTokenSigner(token, private_key)
        return oci.object_storage.ObjectStorageClient(config=config, signer=signer)
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