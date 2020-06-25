import asyncio
import os
import traceback

import orjson
from nats.aio.client import Client as NATS

from actions.model import ActionTrigger
from actions.service import Service

TOPIC = "conthesis.actions.TriggerAction"


class Worker:
    svc: Service
    nc: NATS

    def __init__(self, nc: NATS, svc: Service):
        self.nc = nc
        self.svc = svc

    async def setup(self):
        await self.nc.connect(os.environ["NATS_URL"], loop=asyncio.get_event_loop())
        await self.nc.subscribe(TOPIC, cb=self.handle)

    async def reply(self, msg, data):
        reply = msg.reply
        if reply:
            serialized = orjson.dumps(data)
            await self.nc.publish(reply, serialized)

    async def handle(self, msg):
        try:
            trigger = ActionTrigger.from_bytes(msg.data)
            res = await self.svc.compute(trigger)
            await self.reply(msg, res)
        except Exception:
            traceback.print_exc()

    async def shutdown():
        await self.nc.drain()
