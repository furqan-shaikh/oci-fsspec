from dataclasses import dataclass

from oci.config import DEFAULT_PROFILE
from ocifsspec.core.models.constants import OCI_CONFIG_DEFAULT_PATH


@dataclass
class UserTokenAuthentication:
    profile_name: str = DEFAULT_PROFILE
    config_path: str = OCI_CONFIG_DEFAULT_PATH