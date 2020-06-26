from typing import Optional

from nats.aio.client import Client as NATS
import orjson

from .dcollect import DCollect

CAS_GET = "conthesis.cas.get"

class EntityFetcher:
    dc: DCollect
    nc: NATS
    def __init__(self, dc: DCollect, nc: NATS):
        self.dc = dc
        self.nc = nc

    async def fetch(self, entity: str) -> Optional[bytes]:
        ptr = await self.dc.get_pointer(entity)
        if ptr is None:
            return None
        res = await self.nc.request(CAS_GET, ptr, timeout=1)
        if res.data is None or len(res.data) == 0:
            return None
        else:
            return res.data

    async def fetch_json(self, entity: str) -> Optional[dict]:
        data = await self.fetch(entity)
        if data is None:
            return None
        return orjson.loads(data)
