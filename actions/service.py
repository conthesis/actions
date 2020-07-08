from typing import Any, Dict, List

import orjson
from nats.aio.client import Client as NATS

from actions.entity_fetcher import EntityFetcher
from actions.model import (
    Action,
    ActionProperty,
    ActionSource,
    ActionTrigger,
    DataFormat,
    PropertyKind,
)


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
        try:
            resp = await self.nc.request(_service_queue(kind), props_json, timeout=5.0)
        except:
            print("Timeout waiting for response on", _service_queue(kind))
            return None
        if resp.data is None or len(resp.data) == 0:
            return None
        return orjson.loads(resp.data)

    async def resolve_value(self, prop: ActionProperty, meta: Dict[str, Any]) -> Any:
        if prop.kind == PropertyKind.LITERAL:
            return prop.value
        elif prop.kind == PropertyKind.CAS_POINTER:
            raise RuntimeError("Not yet implemented")
        elif prop.kind == PropertyKind.ENTITY:
            if prop.data_format == DataFormat.JSON:
                return await self.entity_fetcher.fetch_json(prop.value)
            else:
                return await self.entity_fetcher.fetch(prop.value)
        elif prop.kind == PropertyKind.META_FIELD:
            return meta.get(prop.value)
        elif prop.kind == PropertyKind.META_ENTITY:
            mval = meta.get(prop.value)
            if mval is None:
                return None
            if prop.data_format == DataFormat.JSON:
                return await self.entity_fetcher.fetch_json(mval)
            else:
                return await self.entity_fetcher.fetch(mval)

    async def resolve_properties(
        self, properties: List[ActionProperty], meta: Dict[str, Any]
    ) -> Dict[str, Any]:
        return {p.name: await self.resolve_value(p, meta) for p in properties}

    async def get_action(self, trigger: ActionTrigger) -> Action:
        if trigger.action_source == ActionSource.LITERAL and isinstance(
            trigger.action, Action
        ):
            return trigger.action
        elif trigger.action_source == ActionSource.ENTITY and isinstance(
            trigger.action, str
        ):
            data = await self.entity_fetcher.fetch(trigger.action)
            if data is None:
                raise RuntimeError(f"Action {trigger.action} not found")
            return Action.from_bytes(data)
        else:
            raise RuntimeError("Illegal action trigger")

    async def compute(self, trigger: ActionTrigger):
        action = await self.get_action(trigger)
        resolved = await self.resolve_properties(action.properties, trigger.meta)
        return await self.perform_action(action.kind, resolved)
