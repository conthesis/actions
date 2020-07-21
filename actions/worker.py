import time
import asyncio
import os
import traceback

import orjson
from nats.aio.client import Client as NATS

from actions.model import ActionTrigger
from actions.service import Service
from actions.jobs import JobsManager

ASYNC_TOPIC = "conthesis.action.TriggerAsyncAction"
RESPONSE_TOPICS = "conthesis.actions.responses.*"
TOPIC = "conthesis.action.TriggerAction"


class Worker:
    svc: Service
    jobs: JobsManager
    nc: NATS

    def __init__(self, nc: NATS, svc: Service, jobs: JobsManager):
        self.nc = nc
        self.svc = svc
        self.jobs = jobs

    async def setup(self):
        await self.nc.connect(os.environ["NATS_URL"], loop=asyncio.get_event_loop())
        await self.nc.subscribe(TOPIC, cb=self.handle)
        await self.nc.subscribe(ASYNC_TOPIC, cb=self.handle_async_job)
        await self.nc.subscribe(RESPONSE_TOPICS, cb=self.handle_action_response)

    async def reply(self, msg, data, json=True):
        reply = msg.reply
        if reply:
            if json:
                data = orjson.dumps(data)
            await self.nc.publish(reply, data)

    async def handle_async_job(self, msg):
        start = time.monotonic()
        trigger = None
        try:
            trigger = ActionTrigger.from_bytes(msg.data)
        except:
            traceback.print_exc()
            return
        try:
            await self.jobs.register(trigger)
            await self.reply(msg, b"{}", json=False)
        except Exception:
            traceback.print_exc()
            return

        end = time.monotonic()

        delta = end - start

    async def handle_action_response(self, msg):
        try:
            jid = msg.subject[len(RESPONSE_TOPICS) - 1 : ]
            await self.jobs.resume(jid, orjson.loads(msg.data))
        except:
            traceback.print_exc()


    async def handle(self, msg):
        trigger = None
        try:
            trigger = ActionTrigger.from_bytes(msg.data)
        except Exception:
            print("Invalid format:", msg.data)
            traceback.print_exc()
            return

        try:
            res = await self.svc.compute(trigger)
            await self.reply(msg, res)
        except Exception:
            traceback.print_exc()

    async def shutdown():
        await self.nc.drain()
