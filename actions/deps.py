from typing import Optional

import httpx
from fastapi import Depends

from .cas import CAS
from .dcollect import DCollect
from .entity_fetcher import EntityFetcher
from .service import Service

http_client_: Optional[httpx.AsyncClient] = None
cas_: Optional[CAS] = None
dcollect_: Optional[DCollect] = None
entity_fetcher_: Optional[EntityFetcher] = None
service_: Optional[Service] = None


def http_client() -> httpx.AsyncClient:
    global http_client_
    if http_client_ is None:
        http_client_ = httpx.AsyncClient()
    return http_client_


def cas(http_client: httpx.AsyncClient = Depends(http_client)) -> CAS:
    global cas_
    if cas_ is None:
        cas_ = CAS(http_client)
    return cas_


def dcollect(http_client: httpx.AsyncClient = Depends(http_client)) -> DCollect:
    global dcollect_
    if dcollect_ is None:
        dcollect_ = DCollect(http_client)
    return dcollect_


def entity_fetcher(
    dc: DCollect = Depends(dcollect), cas: CAS = Depends(cas)
) -> EntityFetcher:
    global entity_fetcher_
    if entity_fetcher_ is None:
        entity_fetcher_ = EntityFetcher(dc, cas)
    return entity_fetcher_


def service(
    http_client: httpx.AsyncClient = Depends(http_client),
    entity_fetcher: EntityFetcher = Depends(entity_fetcher),
) -> Service:
    global service_
    if service_ is None:
        service_ = Service(http_client, entity_fetcher)
    return service_
