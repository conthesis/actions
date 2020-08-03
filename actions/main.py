import asyncio
import logging
import os

from nats.aio.client import Client as NATS

from .entity_fetcher import EntityFetcher
from .service import Service
from .worker import Worker
from .jobs import JobsManager

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


async def main():
    actions = Actions()
    await actions.setup()
    await actions.wait_for_shutdown()


class Actions:
    def __init__(self):
        self.shutdown_f = asyncio.get_running_loop().create_future()
        nats = NATS()
        entity_fetcher = EntityFetcher(nats)
        service = Service(nats, entity_fetcher)
        self.jobs = JobsManager(service)
        self.worker = Worker(nats, service, self.jobs)

    async def setup(self):
        await self.worker.setup()
        await self.jobs.setup()

    async def wait_for_shutdown(self):
        await self.shutdown_f

    async def shutdown(self):
        try:
            await self.jobs.stop()
            await self.worker.shutdown()
        finally:
            self.shutdown_f.set_result(True)
