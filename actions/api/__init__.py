""""""
from fastapi import APIRouter

from .internal import router as internal

router = APIRouter()
router.include_router(internal, prefix="/internal", tags=["internal"])
