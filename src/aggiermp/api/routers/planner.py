"""Degree planner state for signed-in users.

Two JSON documents per user, stored in user_planner_state
(src/aggiermp/database/migrations/001_user_planner_state.sql):

- record: courses the student has completed, is taking, or plans to take.
- plan: requirement buckets and watchlist (shape owned by the frontend).

The frontend is local-first: it keeps both documents in localStorage, merges
with the server copy, and saves with the revision it last saw. A save against
an older revision gets 409 with the current document so the client can merge
and retry, instead of one device silently overwriting another.
"""

import json
import logging
from typing import Any, Dict, Iterator, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session
from supertokens_python.recipe.session import SessionContainer
from supertokens_python.recipe.session.framework.fastapi import verify_session

from ...database.base import get_session as _get_session_raw

logger = logging.getLogger(__name__)

router: APIRouter = APIRouter(prefix="/users/me/planner", tags=["planner"])

# Module-level so tests can override it via app.dependency_overrides.
require_session = verify_session()

MAX_COURSES = 400
MAX_PLAN_BYTES = 100_000

CourseStatus = Literal["completed", "in_progress", "planned"]
Grade = Literal["A", "B", "C", "D", "F", "S", "U", "P", "Q", "W", "I"]
Classification = Literal["freshman", "sophomore", "junior", "senior", "graduate"]


def get_session() -> Iterator[Session]:
    """FastAPI dependency that always closes the session on exit."""
    db = _get_session_raw()
    try:
        yield db
    finally:
        db.close()


class RecordCourse(BaseModel):
    code: str = Field(pattern=r"^[A-Z]{3,4} \d{3}[A-Z]?$")
    status: CourseStatus
    # Campus-independent term key: year + season digit (1 spring, 2 summer,
    # 3 fall), e.g. "20263" for Fall 2026. None for credit by exam/transfer.
    term: Optional[str] = Field(None, pattern=r"^\d{4}[1-3]$")
    grade: Optional[Grade] = None
    source: Literal["manual", "paste", "credit"] = "manual"
    credit: Optional[Literal["ap", "ib", "dual", "transfer", "exam", "other"]] = None
    # Last edit time (ISO 8601); merges keep the newer entry per course.
    updatedAt: str = Field(max_length=40)
    # Tombstone: removed on some device; kept so merges don't resurrect it.
    deleted: bool = False


class AcademicRecord(BaseModel):
    version: Literal[1] = 1
    courses: List[RecordCourse] = Field(default_factory=list, max_length=MAX_COURSES)
    classification: Optional[Classification] = None
    classificationUpdatedAt: Optional[str] = Field(None, max_length=40)

    @field_validator("courses")
    @classmethod
    def unique_codes(cls, courses: List[RecordCourse]) -> List[RecordCourse]:
        codes = [c.code for c in courses]
        if len(codes) != len(set(codes)):
            raise ValueError("each course code may appear once")
        return courses


class SaveRecordRequest(BaseModel):
    record: AcademicRecord
    base_rev: int = Field(ge=0)


class SavePlanRequest(BaseModel):
    plan: Dict[str, Any]
    base_rev: int = Field(ge=0)

    @field_validator("plan")
    @classmethod
    def size_limit(cls, plan: Dict[str, Any]) -> Dict[str, Any]:
        if len(json.dumps(plan)) > MAX_PLAN_BYTES:
            raise ValueError(f"plan is larger than {MAX_PLAN_BYTES} bytes")
        return plan


class PlannerState(BaseModel):
    record: Optional[Dict[str, Any]] = None
    record_rev: int = 0
    plan: Optional[Dict[str, Any]] = None
    plan_rev: int = 0


class SavedDoc(BaseModel):
    value: Dict[str, Any]
    rev: int


@router.get("", response_model=PlannerState, summary="/users/me/planner")
async def get_planner_state(
    session: SessionContainer = Depends(require_session),
    db: Session = Depends(get_session),
) -> PlannerState:
    try:
        row = db.execute(
            text(
                "SELECT record, record_rev, plan, plan_rev FROM user_planner_state "
                "WHERE user_id = :user_id"
            ),
            {"user_id": session.get_user_id()},
        ).fetchone()
    except Exception as e:
        logger.error("DB error in get_planner_state: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An internal server error occurred.")
    if row is None:
        return PlannerState()
    return PlannerState(
        record=row.record or None,
        record_rev=row.record_rev,
        plan=row.plan or None,
        plan_rev=row.plan_rev,
    )


def _save(db: Session, user_id: str, doc: Literal["record", "plan"], value: Dict[str, Any], base_rev: int) -> SavedDoc:
    """Write one document if its revision is still base_rev; else 409."""
    # `doc` is one of two literals, never user input, so formatting is safe.
    rev_col = f"{doc}_rev"
    try:
        # FOR UPDATE on a missing row locks nothing, so two first saves would
        # both INSERT and one would hit a unique violation (500) instead of a
        # clean 409. Create the row first, then lock it.
        db.execute(
            text("INSERT INTO user_planner_state (user_id) VALUES (:u) ON CONFLICT (user_id) DO NOTHING"),
            {"u": user_id},
        )
        current = db.execute(
            text(f"SELECT {doc}, {rev_col} FROM user_planner_state WHERE user_id = :u FOR UPDATE"),
            {"u": user_id},
        ).fetchone()
        current_rev = current[1] if current else 0
        if current_rev != base_rev:
            db.rollback()
            raise HTTPException(
                status_code=409,
                detail={
                    "message": f"{doc} changed on another device",
                    "rev": current_rev,
                    "value": (current[0] if current else None) or {},
                },
            )
        db.execute(
            text(
                f"UPDATE user_planner_state SET {doc} = CAST(:v AS JSONB), "
                f"{rev_col} = {rev_col} + 1, updated_at = now() WHERE user_id = :u"
            ),
            {"u": user_id, "v": json.dumps(value)},
        )
        db.commit()
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error("DB error saving planner %s: %s", doc, e, exc_info=True)
        raise HTTPException(status_code=500, detail="An internal server error occurred.")
    return SavedDoc(value=value, rev=base_rev + 1)


@router.put(
    "/record",
    response_model=SavedDoc,
    summary="/users/me/planner/record",
    responses={409: {"description": "Saved from another device since base_rev; body has the current record"}},
)
async def save_record(
    payload: SaveRecordRequest,
    session: SessionContainer = Depends(require_session),
    db: Session = Depends(get_session),
) -> SavedDoc:
    return _save(db, session.get_user_id(), "record", payload.record.model_dump(), payload.base_rev)


@router.put(
    "/plan",
    response_model=SavedDoc,
    summary="/users/me/planner/plan",
    responses={409: {"description": "Saved from another device since base_rev; body has the current plan"}},
)
async def save_plan(
    payload: SavePlanRequest,
    session: SessionContainer = Depends(require_session),
    db: Session = Depends(get_session),
) -> SavedDoc:
    return _save(db, session.get_user_id(), "plan", payload.plan, payload.base_rev)
