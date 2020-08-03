from typing import Optional

import orjson
from nats.aio.client import Client as NATS

CFS_GET = "conthesis.cfs.get"
CFS_READLINK = "conthesis.cfs.readlink"

def jsonize(data):
    if data is None:
            return None
    return orjson.loads(data)

class EntityFetcher:
    nc: NATS

    def __init__(self, nc: NATS):
        self.nc = nc

    async def fetch_path(self, path):
        path = path.encode("utf-8") if isinstance(path, str) else path
        res = await self.nc.request(CFS_GET, path)
        if res.data is None or len(res.data) == 0:
            print(f"Not found {path}")
            return None
        else:
            return res.data

    async def readlink(self, path: str) -> str:
        p  = path.encode("utf-8")
        res = await self.nc.request(CFS_READLINK, p)
        data = res.data
        return data

    async def fetch_path_json(self, path):
        return jsonize(await self.fetch_path(path))

    async def fetch(self, entity: str) -> Optional[bytes]:
        return await self.fetch_path(f"/entity/{entity}".encode("utf-8"))

    async def fetch_json(self, entity: str) -> Optional[dict]:
        return jsonize(await self.fetch(entity))
