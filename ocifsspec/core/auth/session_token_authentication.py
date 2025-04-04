from dataclasses import dataclass

from ocifsspec.core.models.constants import OCI_CONFIG_DEFAULT_PATH


@dataclass
class SessionTokenAuthentication:
    profile_name: str
    config_path: str = OCI_CONFIG_DEFAULT_PATH