"""
professors.py
=============
Router for professor-specific endpoints.

Currently provides:
  GET /professors/compare?ids=id1,id2[,id3,id4]
    Side-by-side comparison of 2–4 professors including ratings, GPA, tags,
    and AI-generated summaries.
"""
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from ...database.base import get_session
from ...core.cache import cached, TTL_LONG

logger = logging.getLogger(__name__)

router: APIRouter = APIRouter(prefix="/professors", tags=["professors"])


def _db_error(e: Exception, context: str = "") -> HTTPException:
    msg = f"DB error in {context}: {e}" if context else f"DB error: {e}"
    logger.error(msg, exc_info=True)
    return HTTPException(status_code=500, detail="An internal server error occurred.")


# ---------------------------------------------------------------------------
# Response models (plain dicts — Pydantic validation at the route level)
# ---------------------------------------------------------------------------

class CourseSummary:
    """Not a Pydantic model — just a docstring target."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_ids(ids_param: str) -> List[str]:
    """Split comma-separated professor IDs and validate count."""
    ids = [i.strip() for i in ids_param.split(",") if i.strip()]
    if len(ids) < 2:
        raise HTTPException(
            status_code=422, detail="At least 2 professor IDs are required."
        )
    if len(ids) > 4:
        raise HTTPException(
            status_code=422, detail="At most 4 professors can be compared at once."
        )
    return ids


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/compare",
    summary="Compare 2–4 professors side by side",
    description=(
        "Returns a structured comparison payload for 2–4 professors. "
        "Pass professor IDs as a comma-separated `ids` query parameter. "
        "Each entry includes ratings, GPA stats, top tags, and AI summaries."
    ),
)
@cached(TTL_LONG)
async def compare_professors(
    request: Request,
    ids: str = Query(..., description="Comma-separated professor IDs (2–4)"),
    db: Session = Depends(get_session),
) -> List[Dict[str, Any]]:
    professor_ids = _parse_ids(ids)

    try:
        # ── 1. Core professor data + overall summary ──────────────────────
        prof_rows = db.execute(
            text("""
                SELECT
                    p.id,
                    p.first_name,
                    p.last_name,
                    p.department,
                    COALESCE(psn.avg_rating, p.avg_rating, 0)      AS avg_rating,
                    COALESCE(psn.avg_difficulty, p.avg_difficulty, 0) AS avg_difficulty,
                    COALESCE(psn.total_reviews, p.num_ratings, 0)   AS total_reviews,
                    p.would_take_again_percent,
                    psn.overall_sentiment,
                    psn.strengths,
                    psn.complaints,
                    psn.common_tags,
                    psn.tag_frequencies
                FROM professors p
                LEFT JOIN professor_summaries_new psn
                    ON psn.professor_id = p.id AND psn.course_code IS NULL
                WHERE p.id = ANY(:ids)
            """),
            {"ids": professor_ids},
        ).fetchall()

        if not prof_rows:
            raise HTTPException(status_code=404, detail="No professors found for the given IDs.")

        prof_by_id: Dict[str, Any] = {r.id: r for r in prof_rows}

        # ── 2. GPA aggregates per professor ──────────────────────────────
        gpa_rows = db.execute(
            text("""
                SELECT
                    p.id                                                       AS professor_id,
                    AVG(g.gpa)                                                 AS avg_gpa,
                    SUM(g.grade_a + g.grade_b)::float
                        / NULLIF(SUM(g.total_students), 0) * 100               AS percent_ab,
                    SUM(g.total_students)                                      AS total_students
                FROM professors p
                LEFT JOIN gpa_data g ON (
                    p.department IS NOT NULL
                    AND g.dept = p.department
                    AND g.professor ILIKE p.last_name || '%%'
                )
                WHERE p.id = ANY(:ids)
                GROUP BY p.id
            """),
            {"ids": professor_ids},
        ).fetchall()
        gpa_by_id: Dict[str, Any] = {r.professor_id: r for r in gpa_rows}

        # ── 3. Course-specific summaries (all courses for these profs) ────
        course_summary_rows = db.execute(
            text("""
                SELECT
                    professor_id,
                    course_code,
                    teaching,
                    exams,
                    grading,
                    workload,
                    avg_rating,
                    avg_difficulty,
                    total_reviews,
                    confidence
                FROM professor_summaries_new
                WHERE professor_id = ANY(:ids)
                  AND course_code IS NOT NULL
                ORDER BY professor_id, total_reviews DESC
            """),
            {"ids": professor_ids},
        ).fetchall()

        courses_by_prof: Dict[str, List[Dict[str, Any]]] = {}
        for r in course_summary_rows:
            courses_by_prof.setdefault(r.professor_id, []).append(
                {
                    "courseCode": r.course_code,
                    "teaching": r.teaching,
                    "exams": r.exams,
                    "grading": r.grading,
                    "workload": r.workload,
                    "avgRating": round(float(r.avg_rating), 2) if r.avg_rating else None,
                    "avgDifficulty": round(float(r.avg_difficulty), 2) if r.avg_difficulty else None,
                    "totalReviews": r.total_reviews or 0,
                    "confidence": round(float(r.confidence), 2) if r.confidence else 0.0,
                }
            )

        # ── 4. Assemble response in the requested order ───────────────────
        result = []
        for pid in professor_ids:
            p = prof_by_id.get(pid)
            if p is None:
                # Return a placeholder so the front-end still gets a slot
                result.append({"id": pid, "error": "Professor not found"})
                continue

            gpa = gpa_by_id.get(pid)

            result.append(
                {
                    "id": p.id,
                    "firstName": p.first_name,
                    "lastName": p.last_name,
                    "department": p.department,
                    # Ratings
                    "avgRating": round(float(p.avg_rating), 2),
                    "avgDifficulty": round(float(p.avg_difficulty), 2),
                    "totalReviews": p.total_reviews or 0,
                    "wouldTakeAgainPercent": (
                        round(float(p.would_take_again_percent), 1)
                        if p.would_take_again_percent is not None
                        else None
                    ),
                    # GPA
                    "avgGpa": round(float(gpa.avg_gpa), 2) if gpa and gpa.avg_gpa else None,
                    "percentAB": round(float(gpa.percent_ab), 1) if gpa and gpa.percent_ab else None,
                    "gpaStudentCount": int(gpa.total_students) if gpa and gpa.total_students else 0,
                    # Tags
                    "topTags": list(p.common_tags[:10]) if p.common_tags else [],
                    # AI summary
                    "overallSentiment": p.overall_sentiment,
                    "strengths": list(p.strengths) if p.strengths else [],
                    "complaints": list(p.complaints) if p.complaints else [],
                    # Per-course breakdowns (top 5 by review count)
                    "courseSummaries": courses_by_prof.get(pid, [])[:5],
                }
            )

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise _db_error(e, "compare_professors")
