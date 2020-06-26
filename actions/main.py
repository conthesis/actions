import asyncio

import httpx
from nats.aio.client import Client as NATS

from .dcollect import DCollect
from .entity_fetcher import EntityFetcher
from .service import Service
from .worker import Worker


async def main():
    actions = Actions()
    await actions.setup()
    await actions.wait_for_shutdown()


class Actions:
    def __init__(self):
        self.http_client = httpx.AsyncClient()
        self.shutdown_f = asyncio.get_running_loop().create_future()
        nats = NATS()
        dcollect = DCollect(self.http_client)
        entity_fetcher = EntityFetcher(dcollect, nats)
        service = Service(nats, entity_fetcher)
        self.worker = Worker(nats, service)

    async def setup(self):
        await self.worker.setup()

    async def wait_for_shutdown(self):
        await self.shutdown_f

    async def shutdown(self):
        try:
            await self.worker.shutdown()
            await self.http_client.aclose()
        finally:
            self.shutdown_f.set_result(True)
