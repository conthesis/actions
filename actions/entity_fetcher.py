from typing import Optional

import orjson

from .cas import CAS
from .dcollect import DCollect


class EntityFetcher:
    dc: DCollect
    cas: CAS

    def __init__(self, dc: DCollect, cas: CAS):
        self.dc = dc
        self.cas = cas

    async def fetch(self, entity: str) -> Optional[bytes]:
        ptr = await self.dc.get_pointer(entity)
        if ptr is None:
            return None
        return await self.cas.get(ptr)

    async def fetch_json(self, entity: str) -> Optional[dict]:
        data = await self.fetch(entity)
        if data is None:
            return None
        return orjson.loads(data)
