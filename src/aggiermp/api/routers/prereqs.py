"""Bulk prerequisite trees for the degree planner and Fit My Schedule.

Trees come from courses.prereq_tree / coreq_tree, filled by
pipelines/courses/parse_prereqs.py. Node shapes are documented in
pipelines/courses/prereq_parser.py.
"""

import logging
import re
from typing import Any, Dict, Iterator, List, Optional, Set

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from ...core.cache import TTL_LONG, cached
from ...database.base import get_session as _get_session_raw

logger = logging.getLogger(__name__)

router: APIRouter = APIRouter(prefix="/courses", tags=["courses"])


def get_session() -> Iterator[Session]:
    """FastAPI dependency that always closes the session on exit."""
    db = _get_session_raw()
    try:
        yield db
    finally:
        db.close()

MAX_REQUESTED = 100
MAX_DEPTH = 4
MAX_TOTAL = 500

_CODE_RE = re.compile(r"^\s*([A-Za-z]{3,4})[\s\-_]*(\d{3}[A-Za-z]?)\s*$")


def normalize_code(raw: str) -> Optional[str]:
    m = _CODE_RE.match(raw)
    return f"{m.group(1).upper()} {m.group(2).upper()}" if m else None


def tree_codes(tree: Any) -> Set[str]:
    out: Set[str] = set()
    stack = [tree]
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        if node.get("type") == "course" and isinstance(node.get("code"), str):
            out.add(node["code"])
        stack.extend(node.get("children") or [])
    return out


class CoursePrereqs(BaseModel):
    code: str
    name: Optional[str] = None
    credits: Optional[int] = None
    prereqText: Optional[str] = None
    coreqText: Optional[str] = None
    prereqTree: Optional[Dict[str, Any]] = None
    coreqTree: Optional[Dict[str, Any]] = None
    # high | low | None (not parsed yet). Low means "verify in the catalog".
    confidence: Optional[str] = None
    crossListings: List[str] = []


class PrereqGraphResponse(BaseModel):
    courses: Dict[str, CoursePrereqs]
    # Codes requested or referenced by a tree that aren't in the catalog.
    missing: List[str]
    # True when MAX_TOTAL stopped the walk before `depth` was reached.
    truncated: bool


def _fetch(db: Session, codes: List[str]) -> Dict[str, CoursePrereqs]:
    rows = db.execute(
        text(
            """
            SELECT DISTINCT ON (code)
                code, name, credits, prerequisites, corequisites,
                prereq_tree, coreq_tree, prereq_parse, cross_listings
            FROM courses
            WHERE code = ANY(:codes)
            ORDER BY code, updated_at DESC
            """
        ),
        {"codes": codes},
    ).fetchall()
    return {
        r.code: CoursePrereqs(
            code=r.code,
            name=r.name,
            credits=r.credits,
            prereqText=r.prerequisites,
            coreqText=r.corequisites,
            prereqTree=r.prereq_tree,
            coreqTree=r.coreq_tree,
            confidence=(r.prereq_parse or {}).get("confidence"),
            crossListings=list(r.cross_listings or []),
        )
        for r in rows
    }


@router.get(
    "/prereqs",
    response_model=PrereqGraphResponse,
    summary="/courses/prereqs",
    description=(
        "Prerequisite and corequisite trees for up to 100 courses. With depth > 0, "
        "also returns the courses those trees reference, recursively (for the "
        "degree planner flowchart)."
    ),
)
@cached(ttl=TTL_LONG)
async def get_course_prereqs(
    request: Request,
    codes: str = Query(..., description="Comma-separated codes: CSCE121,CSCE 221"),
    depth: int = Query(0, ge=0, le=MAX_DEPTH),
    db: Session = Depends(get_session),
) -> PrereqGraphResponse:
    requested: List[str] = []
    for raw in codes.split(","):
        if not raw.strip():
            continue
        code = normalize_code(raw)
        if code is None:
            raise HTTPException(status_code=400, detail=f"Invalid course code: {raw.strip()[:20]}")
        if code not in requested:
            requested.append(code)
    if not requested:
        raise HTTPException(status_code=400, detail="No course codes given")
    if len(requested) > MAX_REQUESTED:
        raise HTTPException(status_code=400, detail=f"At most {MAX_REQUESTED} codes per request")

    found: Dict[str, CoursePrereqs] = {}
    missing: Set[str] = set()
    frontier = requested
    truncated = False
    try:
        for level in range(depth + 1):
            room = MAX_TOTAL - len(found)
            if len(frontier) > room:
                frontier, truncated = frontier[:room], True
            if not frontier:
                break
            batch = _fetch(db, frontier)
            found.update(batch)
            missing.update(c for c in frontier if c not in batch)
            if level == depth:
                break
            referenced: Set[str] = set()
            for course in batch.values():
                referenced |= tree_codes(course.prereqTree) | tree_codes(course.coreqTree)
            frontier = sorted(referenced - found.keys() - missing)
    except Exception as e:
        logger.error("DB error in get_course_prereqs: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An internal server error occurred.")

    return PrereqGraphResponse(courses=found, missing=sorted(missing), truncated=truncated)
