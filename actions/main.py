import asyncio

from nats.aio.client import Client as NATS

from .entity_fetcher import EntityFetcher
from .service import Service
from .worker import Worker
from .jobs import JobsManager


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
        jobs = JobsManager(service)
        self.worker = Worker(nats, service, jobs)

    async def setup(self):
        await self.worker.setup()

    async def wait_for_shutdown(self):
        await self.shutdown_f

    async def shutdown(self):
        try:
            await self.worker.shutdown()
        finally:
            self.shutdown_f.set_result(True)
