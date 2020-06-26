from typing import Optional

import orjson
from nats.aio.client import Client as NATS

CAS_GET = "conthesis.cas.get"
DCOLLECT_GET = "conthesis.dcollect.get"


class EntityFetcher:
    nc: NATS

    def __init__(self, nc: NATS):
        self.nc = nc

    async def fetch(self, entity: str) -> Optional[bytes]:
        ptr_res = await self.nc.request(DCOLLECT_GET, entity.encode("utf-8"))
        ptr = ptr_res.data
        if ptr is None or len(ptr) == 0:
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
