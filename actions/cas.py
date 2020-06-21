import os
from typing import Dict

import httpx


class CAS:
    base_url: str
    http: httpx.AsyncClient

    def __init__(self, http: httpx.AsyncClient):
        self.http = http
        self.base_url = os.environ["CAS_URL"]

    async def get(self, key: bytes) -> bytes:
        resp = await self.http.post(f"{self.base_url}/v1/get", data=key)
        resp.raise_for_status()
        return await resp.aread()
