# coding: utf-8

import os
from enum import Enum

from typing import Any, Dict, Tuple, List, Union

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from fastapi.responses import ORJSONResponse

from actions.actions import identity
from actions.entity_fetcher import EntityFetcher
from actions.deps import http_client, entity_fetcher

router = APIRouter()


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


COMPGRAPH_BASE_URL = os.environ["COMPGRAPH_BASE_URL"]


async def post_action(http_client, url, properties: Dict[str, Any]):
    res = await http_client.post(url, json=properties)
    res.raise_for_status()
    return res.json()


async def resolve_value(entity_fetcher: EntityFetcher, prop: ActionProperty) -> Any:
    if prop.kind == PropertyKind.LITERAL:
        return prop.value
    elif prop.kind == PropertyKind.CAS_POINTER:
        raise RuntimeError("Not yet implemented")
    elif prop.kind == PropertyKind.ENTITY:
        if prop.data_format == DataFormat.JSON:
            return await entity_fetcher.fetch_json(prop.value)

async def resolve_properties(entity_fetcher: EntityFetcher, properties: List[ActionProperty]) -> Dict[str, Any]:
    return {p.name: await resolve_value(entity_fetcher, p) for p in properties}



async def perform_action(http_client, kind: str, properties: Dict[str, any]) -> Dict[str, Any]:
    if kind == 'identity':
        return identity(properties)
    elif kind == "TriggerDAG":
        return await post_action(http_client, f"{COMPGRAPH_BASE_URL}triggerProcess", properties)
    return {}


@router.post("/compute")
async def compute_internal_action(action_request: ActionRequest, http_client = Depends(http_client), entity_fetcher = Depends(entity_fetcher)) -> Dict[str, Any]:
    resolved = await resolve_properties(entity_fetcher, action_request.properties)
    res = await perform_action(http_client, action_request.kind, resolved)
    return res
