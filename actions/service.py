from typing import Dict, Any, List
import httpx
import os

from actions.entity_fetcher import EntityFetcher
from actions.model import ActionProperty, ActionRequest, PropertyKind, DataFormat

COMPGRAPH_BASE_URL = os.environ["COMPGRAPH_BASE_URL"]


class Service:
    def __init__(self, http_client: httpx.AsyncClient, entity_fetcher: EntityFetcher):
        self.http_client = http_client
        self.entity_fetcher = entity_fetcher

    async def post_action(self, url: str, properties: Dict[str, Any]):
        res = await self.http_client.post(url, json=properties)
        res.raise_for_status()
        return res.json()

    async def perform_action(
        self, kind: str, properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        if kind == "identity":
            return identity(properties)
        elif kind == "TriggerDAG":
            return await self.post_action(
                f"{COMPGRAPH_BASE_URL}triggerProcess", properties
            )
        return {}

    async def resolve_value(self, prop: ActionProperty) -> Any:
        if prop.kind == PropertyKind.LITERAL:
            return prop.value
        elif prop.kind == PropertyKind.CAS_POINTER:
            raise RuntimeError("Not yet implemented")
        elif prop.kind == PropertyKind.ENTITY:
            if prop.data_format == DataFormat.JSON:
                return await self.entity_fetcher.fetch_json(prop.value)

    async def resolve_properties(self, properties: List[ActionProperty]) -> Dict[str, Any]:
        return {p.name: await self.resolve_value(p) for p in properties}

    async def compute_entity(self, entity: str):
        action = ActionRequest(**await self.entity_fetcher.fetch_json(entity))
        resolved = await self.resolve_properties(action.properties)
        return await self.perform_action(action.kind, resolved)

    async def compute_literal(self, req: ActionRequest):
        resolved = await resolve_properties(entity_fetcher, action_request.properties)
        return await self.perform_action(action_request.kind, resolved)
