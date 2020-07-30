from typing import Optional

import orjson
from nats.aio.client import Client as NATS

CFS_GET = "conthesis.cfs.get"


class EntityFetcher:
    nc: NATS

    def __init__(self, nc: NATS):
        self.nc = nc

    async def fetch(self, entity: str) -> Optional[bytes]:
        res = await self.nc.request(CFS_GET, f"/entity/{entity}".encode("utf-8"))

        if res.data is None or len(res.data) == 0:
            print(f"Not found {entity}")
            return None
        else:
            return res.data

    async def fetch_json(self, entity: str) -> Optional[dict]:
        data = await self.fetch(entity)
        if data is None:
            return None
        return orjson.loads(data)
