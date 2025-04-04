from dataclasses import dataclass


@dataclass
class CopyResponse:
    opc_work_request_id: str
    date: str
    opc_request_id: str