import traceback
import os
import asyncio
import orjson
import traceback
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

from actions.model import ActionRequest
from actions.service import Service

BY_ENTITY_TOPIC = "conthesis.actions.by-entity"
LITERAL_TOPIC = "conthesis.actions.literal"

class Worker:
    svc: Service
    nc: NATS
    def __init__(self, svc: Service):
        self.nc = NATS()
        self.svc = svc

    async def setup(self):
        await self.nc.connect(os.environ["NATS_URL"])
        await self.nc.subscribe(LITERAL_TOPIC, cb=self.handle_literal)
        await self.nc.subscribe(BY_ENTITY_TOPIC, cb=self.handle_entity)


    async def reply(self, msg, data):
        reply = msg.reply
        if reply:
            serialized = orjson.dumps(data)
            await self.nc.publish(reply, serialized)

    async def handle_literal(self, msg):
        try:
            res = await self.svc.compute_literal(ActionRequest.from_bytes(msg))
            await self.reply(msg, res)
        except Exception:
            traceback.print_exc()


    async def handle_entity(self, msg):
        entity = msg.data.decode("utf-8")
        print(entity)
        try:
            res = await self.svc.compute_entity(entity)
            await self.reply(msg, res)
        except Exception:
            traceback.print_exc()



    async def shutdown():
        await self.nc.drain()
