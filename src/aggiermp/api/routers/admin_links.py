"""
Admin endpoints for reviewing instructor <-> RateMyProfessors profile links.

Every route needs the X-Admin-Key header. The monthly report
(pipelines/professors/unmatched_instructors.py --write-proposals) stores
candidates the matcher would only attach on an assumption; these endpoints
list them and record a person's decision, which the course page matcher
then applies (core/instructor_links.py).

  GET  /admin/instructor-links?tab=&q=&limit=&offset=
  POST /admin/instructor-links/link        {"key", "professorId"}
  POST /admin/instructor-links/no-profile  {"key"}
  POST /admin/instructor-links/reject      {"key", "professorIds": [...]}
  POST /admin/instructor-links/undo        {"key"}

Each POST returns {"instructor": InstructorLinks, "invalidatedCourses": [...]}
after clearing the cached course pages of the instructor's courses.
"""

import logging
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from ...core.admin_auth import require_admin_key
from ...core.cache_invalidation import ainvalidate_courses, normalize_course_code
from ...core.instructor_links import (
    MISSING_TABLE,
    LinkError,
    change_links,
    links_table_exists,
    list_instructor_links,
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
    professor_id: str = Field(..., alias="professorId", min_length=1, max_length=200)


class RejectBody(_Body):
    professor_ids: List[str] = Field(..., alias="professorIds", min_length=1, max_length=200)


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
    return list_instructor_links(db, tab, q or "", limit, offset)


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


@router.post("/instructor-links/link", summary="Confirm an instructor's RMP profile")
async def link(body: LinkBody, db: Session = Depends(request_session)) -> Dict[str, Any]:
    return await _change(db, "link", body.key, professor_id=body.professor_id)


@router.post("/instructor-links/no-profile", summary="Confirm an instructor has no RMP profile")
async def no_profile(body: _Body, db: Session = Depends(request_session)) -> Dict[str, Any]:
    return await _change(db, "no-profile", body.key)


@router.post("/instructor-links/reject", summary="Turn candidate profiles down")
async def reject(body: RejectBody, db: Session = Depends(request_session)) -> Dict[str, Any]:
    return await _change(db, "reject", body.key, professor_ids=body.professor_ids)


@router.post("/instructor-links/undo", summary="Reopen an instructor's decision")
async def undo(body: _Body, db: Session = Depends(request_session)) -> Dict[str, Any]:
    return await _change(db, "undo", body.key)
