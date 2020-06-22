from typing import Any, Dict

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from actions.service import Service
from actions.model import ActionRequest
import actions.deps as deps

router = APIRouter()


class ActionByEntityRequest(BaseModel):
    action_id: str


@router.post("/compute-entity")
async def compute_entity(
    action_by_entity: ActionByEntityRequest, svc: Service = Depends(deps.service)
) -> Dict[str, Any]:
    action_id = action_by_entity.action_id
    return await svc.compute_entity(action_id)


@router.post("/compute")
async def compute_internal_action(
    action_request: ActionRequest, svc: Service = Depends(deps.service)
) -> Dict[str, Any]:
    return await svc.compute_literal(action_request)
