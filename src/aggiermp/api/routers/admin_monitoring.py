"""Authenticated, read-only operational dashboard endpoints."""

from fastapi import APIRouter, Depends, Response

from ...core.admin_auth import require_admin_key
from ...core.admin_monitoring import health_reader, monitor_reader

router = APIRouter(
    prefix="/admin/operations",
    tags=["Admin"],
    dependencies=[Depends(require_admin_key)],
)


@router.get("/monitors")
async def monitors(response: Response) -> dict:
    response.headers["Cache-Control"] = "no-store"
    return await monitor_reader.read()


@router.get("/health")
async def health(response: Response) -> dict:
    response.headers["Cache-Control"] = "no-store"
    return await health_reader.read()
