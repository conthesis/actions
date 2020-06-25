from enum import Enum
from typing import Any, Dict, List, Union

import orjson
from pydantic import BaseModel


class PropertyKind(Enum):
    ENTITY = "ENTITY"
    CAS_POINTER = "CAS_POINTER"
    META_FIELD = "META_FIELD"
    META_ENTITY = "META_ENTITY"
    LITERAL = "LITERAL"


class ActionSource(Enum):
    ENTITY = "ENTITY"
    LITERAL = "LITERAL"


class DataFormat(Enum):
    JSON = "JSON"


class ActionProperty(BaseModel):
    name: str
    kind: PropertyKind
    data_format: DataFormat = DataFormat.JSON
    value: Union[str, Dict, List]


class Action(BaseModel):
    kind: str
    properties: List[ActionProperty]

    @classmethod
    def from_bytes(cls, data: bytes) -> "Action":
        return cls(**orjson.loads(data))


class ActionTrigger(BaseModel):
    meta: Dict[str, Any] = {}
    action_source: ActionSource
    action: Union[str, Action]
    # TODO: Add validator forcing action and action_source to match

    @classmethod
    def from_bytes(cls, data: bytes) -> "Action":
        return cls(**orjson.loads(data))


class ActionResult(BaseModel):
    success: bool

    def to_bytes(self) -> bytes:
        return orjson.dumps(self.to_dict())
