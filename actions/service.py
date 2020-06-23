from typing import Any, Dict, List

import orjson
from nats.aio.client import Client as NATS

from actions.entity_fetcher import EntityFetcher
from actions.model import ActionProperty, ActionRequest, DataFormat, PropertyKind


def _service_queue(kind: str) -> str:
    return f"conthesis.action.{kind}"


class Service:
    def __init__(self, nc: NATS, entity_fetcher: EntityFetcher):
        self.nc = nc
        self.entity_fetcher = entity_fetcher

    async def perform_action(
        self, kind: str, properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        props_json = orjson.dumps(properties)
        resp = await self.nc.request(_service_queue(kind), props_json)
        if resp.data is None or len(resp.data) == 0:
            return None
        return orjson.loads(resp.data)

    async def resolve_value(self, prop: ActionProperty) -> Any:
        if prop.kind == PropertyKind.LITERAL:
            return prop.value
        elif prop.kind == PropertyKind.CAS_POINTER:
            raise RuntimeError("Not yet implemented")
        elif prop.kind == PropertyKind.ENTITY:
            if prop.data_format == DataFormat.JSON:
                return await self.entity_fetcher.fetch_json(prop.value)

    async def resolve_properties(
        self, properties: List[ActionProperty]
    ) -> Dict[str, Any]:
        return {p.name: await self.resolve_value(p) for p in properties}

    async def compute_entity(self, entity: str):
        action = ActionRequest(**await self.entity_fetcher.fetch_json(entity))
        resolved = await self.resolve_properties(action.properties)
        return await self.perform_action(action.kind, resolved)

    async def compute_literal(self, req: ActionRequest):
        resolved = await resolve_properties(entity_fetcher, action_request.properties)
        return await self.perform_action(action_request.kind, resolved)
