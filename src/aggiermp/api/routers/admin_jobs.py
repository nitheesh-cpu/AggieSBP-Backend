"""Admin job API; execution belongs to the dedicated worker."""

from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from ...core.admin_auth import require_admin_key
from ...core import admin_jobs as jobs
from ...database.base import request_session

router = APIRouter(
    prefix="/admin", tags=["Admin"], dependencies=[Depends(require_admin_key)]
)


def database(db=Depends(request_session)):
    if not jobs.ready(db):
        raise HTTPException(
            503, "Run migration 004_admin_jobs.sql to enable summary jobs."
        )
    return db


class Submission(BaseModel):
    professor_ids: list[str] = Field(min_length=1, max_length=500)


@router.get("/summary-rebuilds")
def rebuilds(db=Depends(database)):
    return {"professors": jobs.pending(db)}


@router.get("/summary-jobs")
def history(db=Depends(database)):
    rows = db.execute(
        text("SELECT * FROM admin_summary_jobs ORDER BY created_at DESC LIMIT 100")
    ).mappings()
    return {"jobs": [jobs.serialize(r) for r in rows]}


@router.get("/summary-jobs/{job_id}")
def detail(job_id: UUID, db=Depends(database)):
    job = jobs.get_job(db, job_id)
    if not job:
        raise HTTPException(404, "Job not found.")
    return {"job": job}


def submit(db, ids):
    try:
        return jobs.enqueue(db, ids)
    except ValueError as exc:
        db.rollback()
        raise HTTPException(409, str(exc)) from exc


@router.post("/summary-jobs", status_code=202)
def create(body: Submission, db=Depends(database)):
    return submit(db, body.professor_ids)


@router.post("/summary-jobs/{job_id}/retry", status_code=202)
def retry(job_id: UUID, db=Depends(database)):
    job = detail(job_id, db)["job"]
    if job["status"] not in ("failed", "quota_exhausted"):
        raise HTTPException(409, "Only failed or quota-exhausted jobs can be retried.")
    remaining = [pid for pid in job["professorIds"] if pid not in job["completedIds"]]
    if not remaining:
        raise HTTPException(409, "No unfinished professors remain.")
    return submit(db, remaining)
