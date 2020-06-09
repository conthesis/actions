from typing import Any, Dict, Tuple

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from fastapi.responses import ORJSONResponse

from actions.actions import identity

router = APIRouter()

class ActionRequest(BaseModel):
    kind: str
    properties: Dict[str, Any]


@router.post("/compute")
async def compute_internal_action(action_request: ActionRequest) -> Dict[str, Any]:
    # TODO: implement actions 😛
    if action_request.kind == 'identity':
        return identity(action_request.properties)
    return {}
