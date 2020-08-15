from typing import Any, Dict, List
import asyncio
import orjson
from nats.aio.client import Client as NATS
import logging

log = logging.getLogger("service")

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

def _response_queue(jid: str) -> str:
    return f"conthesis.actions.responses.{jid}"


class Service:
    def __init__(self, nc: NATS, entity_fetcher: EntityFetcher):
        self.nc = nc
        self.entity_fetcher = entity_fetcher

    async def perform_action(
        self, kind: str, properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        props_json = orjson.dumps(properties)
        try:
            queue = _service_queue(kind)
            log.info(f"Attempting task {queue}")
            resp = await self.nc.request(queue, props_json, timeout=5.0)
        except:
            log.error("Timeout waiting for response on", _service_queue(kind))
            return { "error": True }
        if resp.data is None or len(resp.data) == 0:
            return None
        return orjson.loads(resp.data)

    async def perform_action_async(self, jid: str, kind: str, properties: Dict[str, Any]) -> None:
        log.info(f"Attempting to call {_service_queue(kind)}")
        await self.nc.publish_request(_service_queue(kind), _response_queue(jid), orjson.dumps(properties))

    async def resolve_value(self, prop: ActionProperty) -> Any:
        if prop.kind == PropertyKind.LITERAL:
            return prop.value
        elif prop.kind == PropertyKind.PATH:
            if prop.data_format == DataFormat.JSON:
                return await self.entity_fetcher.fetch_path_json(prop.value)
            else:
                return await self.entity_fetcher.fetch_path(prop.value)
        else:
            assert False, f"{prop} was not of a supported property kind"


    async def simplify_property(self, prop: ActionProperty, meta: Dict[str, Any]) -> ActionProperty:
        prop = prop.simplify(meta)
        if prop.kind == PropertyKind.LITERAL:
            return prop

    async def freeze_property(
            self, p: ActionProperty,
    ):
        if p.kind == PropertyKind.PATH:
            if (path := await self.entity_fetcher.readlink(p.value)) != p.value.encode("utf-8"):
                return p.copy_with(value=path)
            else:
                data = None
                if p.data_format == DataFormat.JSON:
                    data = await self.entity.fetch_path_json(p.value)
                else:
                    data = await self.entity.fetch_path_json(p.value)
                return p.copy_with(PropertyKind.LITERAL, data)
        elif p.kind == PropertyKind.LITERAL:
            return p
        else:
            assert False, "Simplified must be path or literal"


    async def freeze_properties(
            self, properties: List[ActionProperty], meta: Dict[str, Any]
    ):
        return await asyncio.gather(*[
            self.freeze_property(p.simplify(meta))
            for p in properties
        ])


    async def resolve_properties(
        self, properties: List[ActionProperty]
    ) -> Dict[str, Any]:
        return {p.name: await self.resolve_value(p) for p in properties}

    async def get_action(self, trigger: ActionTrigger) -> Action:
        if trigger.action_source == ActionSource.LITERAL and isinstance(
            trigger.action, Action
        ):
            return trigger.action
        elif trigger.action_source == ActionSource.PATH and isinstance(
            trigger.action, str
        ):
            data = await self.entity_fetcher.fetch_path(trigger.action)
            if data is None:
                raise RuntimeError(f"Action {trigger.action} not found")
            return Action.from_bytes(data)
        else:
            raise RuntimeError("Illegal action trigger")

    async def compute(self, trigger: ActionTrigger):
        action = await self.get_action(trigger)

        frozen_props = await self.freeze_properties(action.properties, trigger.meta)
        resolved = await self.resolve_properties(frozen_props)
        return await self.perform_action(action.kind, resolved)
