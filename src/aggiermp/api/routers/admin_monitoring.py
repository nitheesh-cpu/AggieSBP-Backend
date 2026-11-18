"""Authenticated, read-only operational dashboard endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.exc import SQLAlchemyError

from ...core.admin_auth import require_admin_key
from ...core.admin_monitoring import freshness_reader, health_reader, monitor_reader
from ...core.admin_seat_alerts import SeatAlertPeriod, read_seat_alerts
from ...core.gpa_progress import GpaRun, read_gpa_progress

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


@router.get("/freshness")
async def freshness(response: Response) -> dict:
    result = await freshness_reader.read()
    if result is None:
        raise HTTPException(
            status_code=503,
            detail="Could not read dataset timestamps.",
            headers={"Cache-Control": "no-store"},
        )
    response.headers["Cache-Control"] = "no-store"
    return result


@router.get("/gpa")
def gpa_progress(response: Response) -> GpaRun | None:
    try:
        result = read_gpa_progress()
    except (OSError, ValueError) as exc:
        raise HTTPException(
            status_code=503,
            detail="GPA import progress is currently unavailable.",
            headers={"Cache-Control": "no-store"},
        ) from exc
    response.headers["Cache-Control"] = "no-store"
    return result


@router.get("/seat-alerts")
def seat_alerts(response: Response, period: SeatAlertPeriod = "week") -> dict:
    try:
        result = read_seat_alerts(period)
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=503,
            detail="Seat alert history is currently unavailable.",
            headers={"Cache-Control": "no-store"},
        ) from exc
    response.headers["Cache-Control"] = "no-store"
    return result
