"""
Admin endpoints for reviewing instructor <-> RateMyProfessors profile links.

Every route needs the X-Admin-Key header. The monthly report
(pipelines/professors/unmatched_instructors.py --write-proposals) stores
candidates the matcher would only attach on an assumption; these endpoints
list them and record a person's decision, which the course page matcher
then applies (core/instructor_links.py).

  GET  /admin/instructor-links?tab=&q=&limit=&offset=
  POST /admin/instructor-links/link        {"key", "professorIds": [...]}
  POST /admin/instructor-links/no-profile  {"key"}
  POST /admin/instructor-links/reject      {"key", "professorIds": [...]}
  POST /admin/instructor-links/reject-all  {"key"}
  POST /admin/instructor-links/undo        {"key"}

Each POST returns {"instructor": InstructorLinks, "invalidatedCourses": [...]}
after clearing the cached course pages of the instructor's courses.
"""

from datetime import datetime, timezone
from hashlib import sha256
import json
import logging
from pathlib import Path
from typing import Annotated, Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy.orm import Session

from ...core.admin_auth import require_admin_key
from ...core.cache_invalidation import ainvalidate_courses, normalize_course_code
from ...core.config import settings
from ...core.instructor_ai_report import read_saved_ai_report
from ...core.instructor_links import (
    MISSING_TABLE,
    LinkError,
    ai_rejection_candidates,
    change_links,
    links_table_exists,
    list_instructor_links,
    reject_ai_candidates,
)
from ...database.base import request_session

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/admin",
    tags=["Admin"],
    dependencies=[Depends(require_admin_key)],
)


class _Body(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    key: str = Field(..., min_length=1, max_length=500)


class LinkBody(_Body):
    professor_id: Optional[str] = Field(None, alias="professorId", min_length=1, max_length=200)
    professor_ids: Optional[List[Annotated[str, Field(min_length=1, max_length=200)]]] = Field(
        None, alias="professorIds", min_length=1,
    )

    @model_validator(mode="after")
    def require_one_selection(self) -> "LinkBody":
        if (self.professor_id is None) == (self.professor_ids is None):
            raise ValueError("supply either professorId or professorIds")
        return self


class RejectBody(_Body):
    professor_ids: List[str] = Field(..., alias="professorIds", min_length=1, max_length=200)


class AiRejectBody(BaseModel):
    token: str = Field(pattern=r"^[0-9a-f]{64}$")


def _require_table(db: Session) -> None:
    if not links_table_exists(db):
        raise HTTPException(status_code=409, detail=MISSING_TABLE)


@router.get("/instructor-links", summary="List instructor <-> RMP profile links")
async def get_instructor_links(
    tab: Literal["proposed", "confirmed", "rejected", "conflict"] = Query("proposed"),
    q: Optional[str] = Query(None, max_length=200),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(request_session),
) -> Dict[str, Any]:
    _require_table(db)
    reviews = None
    if tab in {"proposed", "conflict"}:
        try:
            reviews = _load_ai_report()["reviews"]
        except HTTPException:
            # An absent or unreadable report must not block manual review.
            pass
    return list_instructor_links(db, tab, q or "", limit, offset, ai_reviews=reviews)


@router.get("/instructor-links/ai-report", summary="Read the latest AI instructor review report")
def get_instructor_ai_report(response: Response) -> Dict[str, Any]:
    response.headers["Cache-Control"] = "no-store"
    return _load_ai_report()


def _load_ai_report() -> Dict[str, Any]:
    path = settings.instructor_ai_report_path
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[4] / path

    try:
        stat = path.stat()
        data = read_saved_ai_report(path, stat.st_mtime_ns)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="No AI instructor report has been saved on the backend yet.")
    except (OSError, UnicodeError):
        logger.exception("Could not read instructor AI report")
        raise HTTPException(status_code=500, detail="Could not read the AI instructor report.")

    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {**data, "updatedAt": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()}


def _ai_reject_preview(db: Session) -> tuple[dict[str, list[str]], dict]:
    _require_table(db)
    try:
        reviews = _load_ai_report()["reviews"]
    except HTTPException:
        # Clear department mismatches can be evaluated without an AI report.
        reviews = []
    grouped = ai_rejection_candidates(db, reviews)
    token = sha256(json.dumps(grouped, sort_keys=True).encode()).hexdigest()
    return grouped, {
        "token": token,
        "candidates": sum(len(ids) for ids in grouped.values()),
        "instructors": len(grouped),
    }


@router.get("/instructor-links/ai-reject", summary="Preview bulk rejection of strong AI mismatches")
def preview_ai_reject(response: Response, db: Session = Depends(request_session)) -> dict:
    response.headers["Cache-Control"] = "no-store"
    return _ai_reject_preview(db)[1]


@router.post("/instructor-links/ai-reject", summary="Reject the previewed strong AI mismatches")
async def ai_reject(body: AiRejectBody, db: Session = Depends(request_session)) -> dict:
    grouped, preview = _ai_reject_preview(db)
    if body.token != preview["token"]:
        raise HTTPException(status_code=409, detail="The AI rejection list changed. Preview it again.")
    rejected, courses = reject_ai_candidates(db, grouped)
    normalized = sorted({normalize_course_code(c) for c in courses} - {""})
    if normalized:
        await ainvalidate_courses(normalized)
    logger.info("Admin bulk AI rejection rejected %d of %d candidates", rejected, preview["candidates"])
    return {"rejected": rejected}


async def _change(
    db: Session,
    action: str,
    key: str,
    professor_id: Optional[str] = None,
    professor_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    _require_table(db)
    try:
        instructor = change_links(db, action, key, professor_id, professor_ids or [])
    except LinkError as error:
        db.rollback()
        raise HTTPException(status_code=error.status_code, detail=error.detail)
    courses = sorted({normalize_course_code(c) for c in instructor["courses"]} - {""})
    # The course pages of this instructor's courses decide which profile to
    # show; they are cached for a week. A rejection can change the outcome
    # too (it may remove a tie), so every decision clears them.
    if courses:
        await ainvalidate_courses(courses)
    logger.info("Admin instructor link %s for %s (%d courses cleared)", action, key, len(courses))
    return {"instructor": instructor, "invalidatedCourses": courses}


@router.post("/instructor-links/link", summary="Confirm an instructor's RMP profiles")
async def link(body: LinkBody, db: Session = Depends(request_session)) -> Dict[str, Any]:
    return await _change(db, "link", body.key, body.professor_id, body.professor_ids)


@router.post("/instructor-links/no-profile", summary="Confirm an instructor has no RMP profile")
async def no_profile(body: _Body, db: Session = Depends(request_session)) -> Dict[str, Any]:
    return await _change(db, "no-profile", body.key)


@router.post("/instructor-links/reject", summary="Turn candidate profiles down")
async def reject(body: RejectBody, db: Session = Depends(request_session)) -> Dict[str, Any]:
    return await _change(db, "reject", body.key, professor_ids=body.professor_ids)


@router.post("/instructor-links/reject-all", summary="Reject all pending candidates for an instructor")
async def reject_all(body: _Body, db: Session = Depends(request_session)) -> Dict[str, Any]:
    return await _change(db, "reject-all", body.key)


@router.post("/instructor-links/undo", summary="Reopen an instructor's decision")
async def undo(body: _Body, db: Session = Depends(request_session)) -> Dict[str, Any]:
    return await _change(db, "undo", body.key)
