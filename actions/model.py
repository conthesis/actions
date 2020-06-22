from typing import Union, List, Dict
from enum import Enum
from pydantic import BaseModel


class PropertyKind(Enum):
    ENTITY = "ENTITY"
    CAS_POINTER = "CAS_POINTER"
    LITERAL = "LITERAL"


class DataFormat(Enum):
    JSON = "JSON"


class ActionProperty(BaseModel):
    name: str
    kind: PropertyKind
    data_format: DataFormat = DataFormat.JSON
    value: Union[str, Dict, List]


class ActionRequest(BaseModel):
    kind: str
    properties: List[ActionProperty]
