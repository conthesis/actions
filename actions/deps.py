from typing import Optional

import httpx
from fastapi import Depends


http_client_: Optional[httpx.AsyncClient] = None


def http_client() -> httpx.AsyncClient:
    global http_client_
    if http_client_ is None:
        http_client_ = httpx.AsyncClient()
    return http_client_
