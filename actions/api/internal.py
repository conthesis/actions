# coding: utf-8

import os

from typing import Any, Dict, Tuple

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from fastapi.responses import ORJSONResponse

from actions.actions import identity
from actions.deps import http_client

router = APIRouter()

class ActionRequest(BaseModel):
    kind: str
    properties: Dict[str, Any]


COMPGRAPH_BASE_URL = os.environ["COMPGRAPH_BASE_URL"]


async def post_action(http_client, url, properties: Dict[str, Any]):
    res = http_client.post(url, json=properties)
    await res.raise_for_status()
    return await res.json()

@router.post("/compute")
async def compute_internal_action(action_request: ActionRequest, http_client = Depends(http_client)) -> Dict[str, Any]:
    # TODO: implement actions 😛
    if action_request.kind == 'identity':
        return identity(action_request.properties)
    elif action_request.kind == "TriggerDag":
        return await post_action(http_client, f"{COMPGRAPH_BASE_URL}/triggerProcess", action_request.properties)
    return {}
