from dataclasses import dataclass


@dataclass
class ObjectStorageName:
    namespace: str
    bucket: str
    object_name: str