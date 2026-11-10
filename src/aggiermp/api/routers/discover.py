import logging
import math
import json
from typing import List, Optional, Dict, Any, Literal
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import text
from sqlalchemy.orm import Session
from ...database.base import get_session
from ...core.cache import cached, TTL_15MIN, TTL_WEEK
from ...core.campuses import campus_university_sql
from pydantic import BaseModel

logger = logging.getLogger(__name__)


def _db_error(e: Exception, context: str = "") -> HTTPException:
    msg = f"DB error in {context}: {e}" if context else f"DB error: {e}"
    logger.error(msg, exc_info=True)
    return HTTPException(status_code=500, detail="An internal server error occurred.")


router: APIRouter = APIRouter(prefix="/discover")

# Constants
UCC_ATTRIBUTES = [
    "Core American History (KHIS)",
    "Core Communication (KCOM)",
    "Core Creative Arts (KCRA)",
    "Core Fed Gov/Pol Sci (KPLF)",
    "Core Lang, Phil, Culture(KLPC)",
    "Core Life/Physical Sci (KLPS)",
    "Core Local Gov/Pol Sci (KPLL)",
    "Core Mathematics (KMTH)",
    "Core Social & Beh Sci (KSOC)",
    "Univ Req-Cult Discourse (KUCD)",
    "Univ Req-Int'l&Cult Div (KICD)",
]


# Response Models
class ProfessorInfo(BaseModel):
    id: str
    firstName: str
    lastName: str
    avgRating: float
    avgDifficulty: float
    totalRatings: int
    tags: List[str]
    # GPA data (null if not available)
    avgGpa: Optional[float] = None
    percentAB: Optional[float] = None
    gpaStudentCount: Optional[int] = None
    # False when avgGpa is missing, so easiness is scored from ratings only
    hasGradeData: bool = True


class UccCourseDiscovery(BaseModel):
    dept: str
    courseNumber: str
    courseTitle: str
    credits: Optional[str]
    professor: ProfessorInfo
    # Computed scores
    easinessScore: Optional[float] = None
    confidenceScore: Optional[float] = None


class UccCategoryGroup(BaseModel):
    category: str
    courses: List[UccCourseDiscovery]


class UccFilterOptions(BaseModel):
    campuses: List[str]
    sectionTypes: List[str]


def _ucc_ttl(request: Request) -> int:
    # Seat status changes every ~15 minutes; everything else is weekly data.
    seats = request.query_params.get("seats", "any")
    return TTL_WEEK if seats == "any" else TTL_15MIN


def calculate_easiness_score(
    avg_gpa: Optional[float], avg_difficulty: float, avg_rating: float
) -> float:
    """
    Calculate composite easiness score.
    Weights: GPA/4 * 0.5, (5-difficulty)/4 * 0.3, rating/5 * 0.2.
    A missing value (None or 0) is excluded and the remaining weights are
    renormalized, so a professor with no grade history is scored from
    difficulty and rating alone instead of being capped at 50.
    Returns 0-1 scale, higher = easier; 0 if no component is present.
    """
    parts = []
    if avg_gpa:
        parts.append((0.5, avg_gpa / 4.0))
    if avg_difficulty and avg_difficulty > 0:
        parts.append((0.3, (5 - avg_difficulty) / 4.0))
    if avg_rating and avg_rating > 0:
        parts.append((0.2, avg_rating / 5.0))

    total_weight = sum(w for w, _ in parts)
    if total_weight == 0:
        return 0.0
    return sum(w * v for w, v in parts) / total_weight


def calculate_confidence_score(total_reviews: int, gpa_student_count: int) -> float:
    """
    Calculate confidence score based on data availability.
    Uses log scale to diminish returns for very high counts.
    Returns 0-1 scale.
    """
    total_data_points = (total_reviews or 0) + (gpa_student_count or 0)
    if total_data_points == 0:
        return 0.0
    # Log scale: 100 points = ~0.67, 500 points = ~0.9, 1000+ = ~1.0
    return min(1.0, math.log10(total_data_points + 1) / 3.0)


def _discover_professor_key(row: Any) -> tuple[str, str, str, str]:
    """Identify one displayed course/professor combination across duplicate DB rows."""
    first_name = " ".join(str(row.first_name or "").casefold().split())
    last_name = " ".join(str(row.last_name or "").casefold().split())
    display_name = f"{first_name} {last_name}".strip() or "tba"
    return (
        str(row.dept or "").casefold(),
        str(row.course_number or "").casefold(),
        display_name,
        str(getattr(row, "university_id", "") or ""),
    )


def _discover_row_quality(row: Any) -> tuple[int, int, int, int, int]:
    """Prefer the duplicate professor record backed by the strongest review data."""
    total_reviews = int(row.total_reviews or 0)
    avg_rating = float(row.avg_rating or 0)
    avg_difficulty = float(row.avg_difficulty or 0)
    gpa_students = int(row.gpa_student_count or 0)
    return (
        int(total_reviews > 0),
        total_reviews,
        int(avg_rating > 0),
        int(avg_difficulty > 0),
        gpa_students,
    )


def _dedupe_discover_professor_rows(rows: List[Any]) -> List[Any]:
    """Collapse fuzzy-join duplicates before computing their easiness scores."""
    best_rows: Dict[tuple[str, str, str, str], Any] = {}
    for row in rows:
        key = _discover_professor_key(row)
        current = best_rows.get(key)
        if current is None or _discover_row_quality(row) > _discover_row_quality(
            current
        ):
            best_rows[key] = row
    return list(best_rows.values())



class TermDepartment(BaseModel):
    code: str
    name: str


class ScheduleBlockInput(BaseModel):
    days: List[str]
    start: str
    end: str


class DiscoverFitRequest(BaseModel):
    course_keys: List[str]
    schedule_blocks: List[ScheduleBlockInput]
    # Section site from Howdy (e.g. "College Station", "Galveston"); filters `sections.campus`.
    campus: Optional[str] = None
    # If non-empty, only sections with at least one matching attribute_desc count as compatible.
    section_attribute_descs: Optional[List[str]] = None
    seat_filter: Literal["any", "open", "full"] = "any"


class FitSectionAttributeOptionsRequest(BaseModel):
    course_keys: List[str]


class FitSectionCampusOptionsRequest(BaseModel):
    course_keys: List[str]


class CourseProfessorsRequest(BaseModel):
    # "DEPT-NUMBER", e.g. "CSCE-121"
    course_keys: List[str]
    campus: Optional[str] = None


class DiscoverFitSection(BaseModel):
    """One section that fits the student's busy times."""

    id: str
    crn: str
    section: Optional[str] = None
    is_open: Optional[bool] = None
    instructor: Optional[str] = None
    # e.g. "MWF 09:10 AM-10:00 AM"; several meetings joined with "; ".
    meetings: Optional[str] = None
    # This section's own title; special topics (e.g. "SPTP: COMPUTER VISION") differ by section.
    title: Optional[str] = None


class DiscoverFitCourseMatch(BaseModel):
    course_key: str
    dept: str
    course_number: str
    course_title: str
    compatible_section_count: int
    sample_section_id: Optional[str] = None
    sample_crn: Optional[str] = None
    sections: List[DiscoverFitSection] = []


class UccFitCandidatesRequest(BaseModel):
    categories: List[str]
    campus: Optional[str] = None


class DiscoverFitCandidateCourse(BaseModel):
    dept: str
    courseNumber: str
    courseTitle: str
    easinessScore: float = 0.0


@router.get(
    "/{term_code}/departments",
    summary="/discover/{term_code}/departments",
)
@cached(TTL_WEEK)
async def discover_term_departments(
    request: Request, term_code: str, db: Session = Depends(get_session)
) -> List[TermDepartment]:
    """
    Get all distinct departments that have sections in a given term.
    Returns the dept code and description so they exactly match the discover endpoint filter.
    """
    try:
        query = text("""
            SELECT DISTINCT dept, dept_desc
            FROM sections
            WHERE term_code = :term_code
              AND dept IS NOT NULL
            ORDER BY dept
        """)
        result = db.execute(query, {"term_code": term_code})
        return [
            TermDepartment(code=row.dept, name=row.dept_desc or row.dept)
            for row in result
        ]
    except Exception as e:
        raise _db_error(e)


@router.post(
    "/{term_code}/ucc-fit-candidates",
    summary="/discover/{term_code}/ucc-fit-candidates",
)
async def discover_ucc_fit_candidates(
    request: Request,
    term_code: str,
    payload: UccFitCandidatesRequest,
    db: Session = Depends(get_session),
) -> List[DiscoverFitCandidateCourse]:
    """
    Lightweight UCC candidate list for Fit My Schedule.
    This intentionally avoids professor joins so it remains reliable and fast.
    """
    try:
        categories = [c for c in payload.categories if c and c.strip()]
        if not categories:
            return []

        query = text(
            r"""
            WITH candidate_sections AS (
                SELECT s.dept, s.course_number, s.course_title
                FROM sections s
                WHERE s.term_code = :term_code
                  AND s.attributes_text IS NOT NULL
                  AND (:campus IS NULL OR s.campus = :campus)
                  AND s.dept IS NOT NULL
                  AND s.course_number IS NOT NULL
                  AND EXISTS (
                      SELECT 1
                      FROM regexp_split_to_table(s.attributes_text, '\|') AS attr
                      WHERE TRIM(attr) = ANY(:categories)
                  )
            ),
            candidate_courses AS (
                SELECT dept, course_number, MAX(course_title) AS course_title
                FROM candidate_sections
                GROUP BY dept, course_number
            ),
            course_gpa AS (
                SELECT
                    g.dept,
                    g.course_number,
                    SUM(g.gpa::numeric * g.total_students)
                        / NULLIF(SUM(g.total_students), 0) AS avg_gpa
                FROM gpa_data g
                JOIN candidate_courses cc
                  ON cc.dept = g.dept AND cc.course_number = g.course_number
                WHERE g.gpa IS NOT NULL AND g.total_students > 0
                GROUP BY g.dept, g.course_number
            ),
            course_reviews AS (
                SELECT
                    r.course_code,
                    AVG(r.difficulty_rating) AS avg_difficulty,
                    AVG(
                        (COALESCE(r.clarity_rating, r.helpful_rating)
                         + COALESCE(r.helpful_rating, r.clarity_rating)) / 2.0
                    ) AS avg_rating
                FROM reviews r
                WHERE r.course_code IN (
                    SELECT dept || course_number FROM candidate_courses
                )
                GROUP BY r.course_code
            )
            SELECT
                cc.dept,
                cc.course_number,
                cc.course_title,
                cg.avg_gpa,
                cr.avg_difficulty,
                cr.avg_rating
            FROM candidate_courses cc
            LEFT JOIN course_gpa cg
              ON cg.dept = cc.dept AND cg.course_number = cc.course_number
            LEFT JOIN course_reviews cr
              ON cr.course_code = cc.dept || cc.course_number
            ORDER BY cc.dept, cc.course_number
            """
        )

        rows = db.execute(
            query,
            {
                "term_code": term_code,
                "categories": categories,
                "campus": payload.campus,
            },
        ).fetchall()

        return [
            DiscoverFitCandidateCourse(
                dept=str(r.dept),
                courseNumber=str(r.course_number),
                courseTitle=str(r.course_title or f"{r.dept} {r.course_number}"),
                easinessScore=round(
                    calculate_easiness_score(
                        float(r.avg_gpa) if r.avg_gpa else 0,
                        float(r.avg_difficulty) if r.avg_difficulty else 0,
                        float(r.avg_rating) if r.avg_rating else 0,
                    )
                    * 100,
                    1,
                ),
            )
            for r in rows
        ]
    except Exception as e:
        raise _db_error(e)


@router.post(
    "/{term_code}/fit-sections",
    summary="/discover/{term_code}/fit-sections",
)
async def discover_fit_sections(
    request: Request,
    term_code: str,
    payload: DiscoverFitRequest,
    db: Session = Depends(get_session),
) -> List[DiscoverFitCourseMatch]:
    """
    Fast schedule-fit check for a batch of candidate courses.
    Uses SQL-side conflict detection to avoid N+1 section requests.
    """
    try:
        if not payload.course_keys:
            return []

        attribute_descs = [
            a.strip()
            for a in (payload.section_attribute_descs or [])
            if a and str(a).strip()
        ]
        skip_attribute_filter = len(attribute_descs) == 0

        section_campus_filter = (payload.campus or "").strip()
        apply_campus_filter = len(section_campus_filter) > 0

        schedule_json = json.dumps(
            [
                {
                    "days": b.days,
                    "start_t": b.start,
                    "end_t": b.end,
                }
                for b in payload.schedule_blocks
            ]
        )

        query = text(
            r"""
            WITH user_schedule AS (
                SELECT
                    unnest(days) AS day,
                    start_t::time AS start_t,
                    end_t::time AS end_t
                FROM jsonb_to_recordset(CAST(:schedule_json AS jsonb))
                    AS x(days text[], start_t text, end_t text)
            ),
            candidate_sections AS (
                SELECT
                    s.id,
                    s.crn,
                    s.dept,
                    s.course_number,
                    s.course_title,
                    s.section_number,
                    s.is_open
                FROM sections s
                WHERE s.term_code = :term_code
                  AND (s.dept || '-' || s.course_number) = ANY(:course_keys)
                  AND (
                      CAST(:apply_campus_filter AS boolean) IS NOT TRUE
                      OR LOWER(TRIM(COALESCE(s.campus, ''))) = LOWER(:section_campus_filter)
                  )
                  AND (
                      CAST(:seat_filter AS text) = 'any'
                      OR (
                          CAST(:seat_filter AS text) = 'open'
                          AND s.is_open IS TRUE
                      )
                      OR (
                          CAST(:seat_filter AS text) = 'full'
                          AND s.is_open IS NOT TRUE
                      )
                  )
                  AND (
                      CAST(:skip_attribute_filter AS boolean) IS TRUE
                      OR EXISTS (
                          SELECT 1
                          FROM regexp_split_to_table(s.attributes_text, '\|') AS attr
                          WHERE TRIM(attr) = ANY(:attribute_descs)
                      )
                  )
            ),
            meeting_times AS (
                SELECT
                    cs.id AS section_id,
                    cs.dept,
                    cs.course_number,
                    cs.course_title,
                    cs.crn,
                    sm.days_of_week,
                    CASE
                        WHEN sm.begin_time IS NULL OR TRIM(sm.begin_time) = '' THEN NULL
                        WHEN TRIM(sm.begin_time) ~* '(AM|PM)$'
                            THEN TO_TIMESTAMP(TRIM(sm.begin_time), 'HH12:MI AM')::time
                        WHEN TRIM(sm.begin_time) ~ '^[0-9]{2}:[0-9]{2}$'
                            THEN TRIM(sm.begin_time)::time
                        ELSE NULL
                    END AS section_start,
                    CASE
                        WHEN sm.end_time IS NULL OR TRIM(sm.end_time) = '' THEN NULL
                        WHEN TRIM(sm.end_time) ~* '(AM|PM)$'
                            THEN TO_TIMESTAMP(TRIM(sm.end_time), 'HH12:MI AM')::time
                        WHEN TRIM(sm.end_time) ~ '^[0-9]{2}:[0-9]{2}$'
                            THEN TRIM(sm.end_time)::time
                        ELSE NULL
                    END AS section_end
                FROM candidate_sections cs
                JOIN section_meetings sm ON sm.section_id = cs.id
                    -- Final exams are not class times.
                    AND COALESCE(sm.meeting_type, '') NOT ILIKE '%%exam%%'
            ),
            conflicting_sections AS (
                SELECT DISTINCT mt.section_id
                FROM meeting_times mt
                JOIN user_schedule us
                  ON us.day = ANY(mt.days_of_week)
                WHERE mt.section_start IS NOT NULL
                  AND mt.section_end IS NOT NULL
                  AND mt.section_start < us.end_t
                  AND mt.section_end > us.start_t
            ),
            compatible_sections AS (
                SELECT DISTINCT
                    cs.id,
                    cs.crn,
                    cs.dept,
                    cs.course_number,
                    cs.course_title,
                    cs.section_number,
                    cs.is_open
                FROM candidate_sections cs
                LEFT JOIN conflicting_sections cf ON cf.section_id = cs.id
                WHERE cf.section_id IS NULL
            )
            SELECT
                (dept || '-' || course_number) AS course_key,
                dept,
                course_number,
                MIN(course_title) AS course_title,
                COUNT(*)::int AS compatible_section_count,
                MIN(id) AS sample_section_id,
                MIN(crn) AS sample_crn,
                json_agg(
                    json_build_object(
                        'id', id,
                        'crn', crn,
                        'section', section_number,
                        'title', course_title,
                        'is_open', is_open,
                        'instructor', (
                            SELECT si.instructor_name
                            FROM section_instructors si
                            WHERE si.section_id = compatible_sections.id
                            ORDER BY si.is_primary DESC NULLS LAST
                            LIMIT 1
                        ),
                        'meetings', (
                            SELECT string_agg(
                                array_to_string(sm.days_of_week, '')
                                    || ' ' || TRIM(sm.begin_time)
                                    || '-' || TRIM(sm.end_time),
                                '; ' ORDER BY sm.meeting_index
                            )
                            FROM section_meetings sm
                            WHERE sm.section_id = compatible_sections.id
                              AND COALESCE(TRIM(sm.begin_time), '') <> ''
                              AND COALESCE(sm.meeting_type, '') NOT ILIKE '%%exam%%'
                        )
                    )
                    ORDER BY section_number
                ) AS sections
            FROM compatible_sections
            GROUP BY dept, course_number
            ORDER BY dept, course_number
            """
        )

        rows = db.execute(
            query,
            {
                "term_code": term_code,
                "course_keys": payload.course_keys,
                "schedule_json": schedule_json,
                "apply_campus_filter": apply_campus_filter,
                "section_campus_filter": section_campus_filter,
                "seat_filter": payload.seat_filter,
                "skip_attribute_filter": skip_attribute_filter,
                "attribute_descs": attribute_descs if not skip_attribute_filter else [],
            },
        ).fetchall()

        return [
            DiscoverFitCourseMatch(
                course_key=str(r.course_key),
                dept=str(r.dept),
                course_number=str(r.course_number),
                course_title=str(r.course_title),
                compatible_section_count=int(r.compatible_section_count or 0),
                sample_section_id=str(r.sample_section_id) if r.sample_section_id else None,
                sample_crn=str(r.sample_crn) if r.sample_crn else None,
                sections=[
                    DiscoverFitSection(
                        id=str(s["id"]),
                        crn=str(s["crn"]),
                        section=s.get("section"),
                        is_open=s.get("is_open"),
                        instructor=s.get("instructor"),
                        meetings=s.get("meetings"),
                        title=s.get("title"),
                    )
                    for s in (
                        json.loads(r.sections)
                        if isinstance(r.sections, str)
                        else (r.sections or [])
                    )
                ],
            )
            for r in rows
        ]
    except Exception as e:
        raise _db_error(e)


@router.post(
    "/{term_code}/fit-section-attribute-options",
    summary="/discover/{term_code}/fit-section-attribute-options",
)
async def discover_fit_section_attribute_options(
    request: Request,
    term_code: str,
    payload: FitSectionAttributeOptionsRequest,
    db: Session = Depends(get_session),
) -> List[str]:
    """
    Distinct section attribute descriptions for the given courses in a term.
    Used to populate schedule-fit filters after an initial candidate list is loaded.
    """
    try:
        keys = [k for k in payload.course_keys if k and str(k).strip()]
        if not keys:
            return []

        query = text(
            r"""
            SELECT DISTINCT TRIM(attr) AS attribute_desc
            FROM sections s,
                 regexp_split_to_table(s.attributes_text, '\|') AS attr
            WHERE s.term_code = :term_code
              AND (s.dept || '-' || s.course_number) = ANY(:course_keys)
              AND s.attributes_text IS NOT NULL
              AND TRIM(attr) <> ''
            ORDER BY 1
            LIMIT 500
            """
        )
        rows = db.execute(
            query,
            {"term_code": term_code, "course_keys": keys},
        ).fetchall()
        return [str(r.attribute_desc) for r in rows if r.attribute_desc]
    except Exception as e:
        raise _db_error(e)


@router.post(
    "/{term_code}/fit-section-campus-options",
    summary="/discover/{term_code}/fit-section-campus-options",
)
async def discover_fit_section_campus_options(
    request: Request,
    term_code: str,
    payload: FitSectionCampusOptionsRequest,
    db: Session = Depends(get_session),
) -> List[str]:
    """
    Distinct section.campus values for the given courses (e.g. College Station vs Galveston).
    """
    try:
        keys = [k for k in payload.course_keys if k and str(k).strip()]
        if not keys:
            return []

        query = text(
            """
            SELECT DISTINCT TRIM(s.campus) AS campus
            FROM sections s
            WHERE s.term_code = :term_code
              AND (s.dept || '-' || s.course_number) = ANY(:course_keys)
              AND s.campus IS NOT NULL
              AND TRIM(s.campus) <> ''
            ORDER BY 1
            LIMIT 100
            """
        )
        rows = db.execute(
            query,
            {"term_code": term_code, "course_keys": keys},
        ).fetchall()
        return [str(r.campus) for r in rows if r.campus]
    except Exception as e:
        raise _db_error(e)


@router.get(
    "/{term_code}/ucc",
    summary="/discover/{term_code}/ucc",
)
@cached(TTL_WEEK, ttl_for=_ucc_ttl)
async def discover_ucc_courses(
    request: Request,
    term_code: str,
    campus: Optional[str] = None,
    seats: Literal["any", "open", "full"] = "any",
    section_type: Optional[List[str]] = Query(None),
    db: Session = Depends(get_session),
) -> List[UccCategoryGroup]:
    """
    Get all University Core Curriculum (UCC) classes for a specific term,
    grouped by category and ordered by easiness score.

    Optional section filters (a course/professor pair is kept if any of its
    sections match):
    - campus: exact section campus (e.g. "College Station")
    - seats: "open" or "full" sections only; "any" (default) keeps both
    - section_type: repeatable; section must have at least one of these attributes
    """
    try:
        campus_filter = (campus or "").strip()
        section_types = [t.strip() for t in (section_type or []) if t and t.strip()]

        query = text(r"""
            WITH ucc_sections AS (
                SELECT s.id, s.dept, s.course_number, s.course_title, s.credit_hours,
                       s.attributes_text, s.campus
                FROM sections s
                WHERE s.term_code = :term_code
                  AND s.attributes_text IS NOT NULL
                  AND EXISTS (
                      SELECT 1
                      FROM regexp_split_to_table(s.attributes_text, '\|') AS t(v)
                      WHERE TRIM(t.v) = ANY(:ucc_attributes)
                  )
                  AND (
                      CAST(:campus_filter AS text) = ''
                      OR LOWER(TRIM(COALESCE(s.campus, ''))) = LOWER(:campus_filter)
                  )
                  AND (
                      CAST(:seats AS text) = 'any'
                      OR (CAST(:seats AS text) = 'open' AND s.is_open IS TRUE)
                      OR (CAST(:seats AS text) = 'full' AND s.is_open IS NOT TRUE)
                  )
                  AND (
                      cardinality(CAST(:section_types AS text[])) = 0
                      OR EXISTS (
                          SELECT 1
                          FROM regexp_split_to_table(s.attributes_text, '\|') AS st(v)
                          WHERE TRIM(st.v) = ANY(:section_types)
                      )
                  )
            ),
            ucc_pairs AS (
                SELECT DISTINCT
                    TRIM(ucc_cat.v) AS attribute_desc,
                    us.dept,
                    us.course_number,
                    us.course_title,
                    us.credit_hours,
                    us.campus,
                    si.instructor_name
                FROM ucc_sections us
                CROSS JOIN LATERAL regexp_split_to_table(us.attributes_text, '\|') AS ucc_cat(v)
                LEFT JOIN section_instructors si ON us.id = si.section_id
                WHERE TRIM(ucc_cat.v) = ANY(:ucc_attributes)
            ),
            ucc_depts AS (
                SELECT DISTINCT dept FROM ucc_pairs
            ),
            gpa_agg AS (
                SELECT
                    g.dept,
                    g.course_number,
                    g.professor,
                    AVG(g.gpa) as avg_gpa,
                    SUM(g.grade_a + g.grade_b)::float / NULLIF(SUM(g.total_students), 0) * 100 as percent_ab,
                    SUM(g.total_students) as total_students
                FROM gpa_data g
                JOIN ucc_depts ud ON g.dept = ud.dept
                GROUP BY g.dept, g.course_number, g.professor
            )
            SELECT DISTINCT
                up.attribute_desc,
                up.dept,
                up.course_number,
                up.course_title,
                up.credit_hours,
                p.id as professor_id,
                p.university_id,
                p.first_name,
                p.last_name,
                COALESCE(psn.avg_rating, p.avg_rating, 0) as avg_rating,
                COALESCE(psn.avg_difficulty, p.avg_difficulty, 0) as avg_difficulty,
                COALESCE(psn.total_reviews, p.num_ratings, 0) as total_reviews,
                psn.common_tags,
                g.avg_gpa,
                g.percent_ab,
                g.total_students as gpa_student_count
            FROM ucc_pairs up
            LEFT JOIN professors p ON (
                up.instructor_name IS NOT NULL
                AND p.university_id = __CAMPUS_SCHOOL__
                AND up.instructor_name ILIKE p.first_name || '%%'
                AND up.instructor_name ILIKE '%%' || p.last_name
            )
            LEFT JOIN professor_summaries_new psn ON (
                p.id IS NOT NULL
                AND p.id = psn.professor_id
                AND psn.course_code = up.dept || up.course_number
            )
            LEFT JOIN gpa_agg g ON (
                p.id IS NOT NULL
                AND g.dept = up.dept
                AND g.course_number = up.course_number
                AND p.university_id = 'U2Nob29sLTEwMDM='
                AND g.professor ILIKE p.last_name || '%%'
            )
        """.replace("__CAMPUS_SCHOOL__", campus_university_sql("up.campus")))

        result = db.execute(
            query,
            {
                "term_code": term_code,
                "ucc_attributes": UCC_ATTRIBUTES,
                "campus_filter": campus_filter,
                "seats": seats,
                "section_types": section_types,
            },
        )

        # Group by category
        grouped_courses: Dict[str, List[Dict[str, Any]]] = {}
        for row in result:
            category = row.attribute_desc
            if category not in grouped_courses:
                grouped_courses[category] = []

            # extract tags (top 5)
            tags = row.common_tags[:5] if row.common_tags else []

            # Calculate scores
            avg_gpa = float(row.avg_gpa) if row.avg_gpa else None
            avg_difficulty = float(row.avg_difficulty) if row.avg_difficulty else 0.0
            avg_rating = float(row.avg_rating) if row.avg_rating else 0.0
            total_reviews = row.total_reviews or 0
            gpa_student_count = row.gpa_student_count or 0

            easiness = calculate_easiness_score(
                avg_gpa, avg_difficulty, avg_rating
            )
            confidence = calculate_confidence_score(total_reviews, gpa_student_count)

            grouped_courses[category].append(
                {
                    "dept": row.dept,
                    "courseNumber": row.course_number,
                    "courseTitle": row.course_title,
                    "credits": row.credit_hours,
                    "easinessScore": round(easiness * 100, 1),  # Convert to 0-100 scale
                    "confidenceScore": round(
                        confidence * 100, 1
                    ),  # Convert to 0-100 scale
                    "professor": {
                        "id": row.professor_id or "TBA",
                        "firstName": row.first_name or "TBA",
                        "lastName": row.last_name or "",
                        "avgRating": round(avg_rating, 2),
                        "avgDifficulty": round(avg_difficulty, 2),
                        "totalRatings": total_reviews,
                        "tags": tags,
                        "avgGpa": round(avg_gpa, 2) if avg_gpa else None,
                        "percentAB": round(float(row.percent_ab), 1)
                        if row.percent_ab
                        else None,
                        "gpaStudentCount": gpa_student_count,
                        "hasGradeData": avg_gpa is not None and avg_gpa > 0,
                    },
                }
            )

        # Sort each category by easiness score (descending) and limit to top 50
        for category in grouped_courses:
            grouped_courses[category].sort(
                key=lambda x: x["easinessScore"], reverse=True
            )
            # Limit to top 50 courses per category
            grouped_courses[category] = grouped_courses[category][:50]

        # Convert to response model
        response: List[UccCategoryGroup] = []
        for category, courses in grouped_courses.items():
            response.append(
                UccCategoryGroup(
                    category=category,
                    courses=[UccCourseDiscovery(**c) for c in courses],
                )
            )

        return response

    except Exception as e:
        raise _db_error(e)


@router.get(
    "/{term_code}/ucc-filter-options",
    summary="/discover/{term_code}/ucc-filter-options",
)
@cached(TTL_WEEK)
async def discover_ucc_filter_options(
    request: Request, term_code: str, db: Session = Depends(get_session)
) -> UccFilterOptions:
    """
    Campuses and section types (non-core attributes like Honors or Online)
    found on UCC sections in a term, for the UCC page filters.
    """
    try:
        query = text(r"""
            WITH ucc_sections AS (
                SELECT s.campus, s.attributes_text
                FROM sections s
                WHERE s.term_code = :term_code
                  AND s.attributes_text IS NOT NULL
                  AND EXISTS (
                      SELECT 1
                      FROM regexp_split_to_table(s.attributes_text, '\|') AS t(v)
                      WHERE TRIM(t.v) = ANY(:ucc_attributes)
                  )
            )
            SELECT 'campus' AS kind, TRIM(campus) AS value
            FROM ucc_sections
            WHERE campus IS NOT NULL AND TRIM(campus) <> ''
            UNION
            SELECT 'section_type' AS kind, TRIM(attr.v) AS value
            FROM ucc_sections us
            CROSS JOIN LATERAL regexp_split_to_table(us.attributes_text, '\|') AS attr(v)
            WHERE TRIM(attr.v) <> ''
              AND TRIM(attr.v) <> ALL(:ucc_attributes)
            ORDER BY 1, 2
        """)
        rows = db.execute(
            query, {"term_code": term_code, "ucc_attributes": UCC_ATTRIBUTES}
        ).fetchall()
        campuses = [str(r.value) for r in rows if r.kind == "campus"]
        # Some sections carry their campus as an attribute too; the campus filter covers those.
        campus_names = {c.lower() for c in campuses}
        return UccFilterOptions(
            campuses=campuses,
            sectionTypes=[
                str(r.value)
                for r in rows
                if r.kind == "section_type" and str(r.value).lower() not in campus_names
            ],
        )
    except Exception as e:
        raise _db_error(e)


def _discover_rows(
    db: Session,
    *,
    course_filter: str,
    gpa_filter: str,
    params: Dict[str, Any],
    limit: int = 100,
) -> List[UccCourseDiscovery]:
    """
    One row per course and professor teaching it in a term, easiest first.

    `course_filter` limits sections (alias `s`) and `gpa_filter` limits the
    gpa_data rows; both are literal SQL built by the caller with bound params.
    """
    query = text(
        """
            WITH gpa_agg AS (
                SELECT
                    dept,
                    course_number,
                    professor,
                    AVG(gpa) as avg_gpa,
                    SUM(grade_a + grade_b)::float / NULLIF(SUM(total_students), 0) * 100 as percent_ab,
                    SUM(total_students) as total_students
                FROM gpa_data
                WHERE __GPA_FILTER__
                GROUP BY dept, course_number, professor
            )
            SELECT DISTINCT
                s.dept,
                s.course_number,
                s.course_title,
                s.credit_hours,
                p.id as professor_id,
                p.university_id,
                p.first_name,
                p.last_name,
                COALESCE(psn.avg_rating, p.avg_rating, 0) as avg_rating,
                COALESCE(psn.avg_difficulty, p.avg_difficulty, 0) as avg_difficulty,
                COALESCE(psn.total_reviews, p.num_ratings, 0) as total_reviews,
                psn.common_tags,
                g.avg_gpa,
                g.percent_ab,
                g.total_students as gpa_student_count
            FROM sections s
            LEFT JOIN section_instructors si ON s.id = si.section_id
            LEFT JOIN professors p ON (
                si.instructor_name IS NOT NULL
                AND p.university_id = __CAMPUS_SCHOOL__
                AND si.instructor_name ILIKE p.first_name || '%%'
                AND si.instructor_name ILIKE '%%' || p.last_name
            )
            LEFT JOIN professor_summaries_new psn ON (
                p.id IS NOT NULL
                AND p.id = psn.professor_id
                AND psn.course_code = s.dept || s.course_number
            )
            LEFT JOIN gpa_agg g ON (
                p.id IS NOT NULL
                AND g.dept = s.dept
                AND g.course_number = s.course_number
                AND p.university_id = 'U2Nob29sLTEwMDM='
                AND g.professor ILIKE p.last_name || '%%'
            )
            WHERE s.term_code = :term_code
              AND __COURSE_FILTER__
        """.replace("__CAMPUS_SCHOOL__", campus_university_sql("s.campus"))
        .replace("__GPA_FILTER__", gpa_filter)
        .replace("__COURSE_FILTER__", course_filter)
    )

    result = db.execute(query, params)
    rows = _dedupe_discover_professor_rows(list(result))

    courses: List[Dict[str, Any]] = []
    for row in rows:
        tags = row.common_tags[:5] if row.common_tags else []

        avg_gpa = float(row.avg_gpa) if row.avg_gpa else None
        avg_difficulty = float(row.avg_difficulty) if row.avg_difficulty else 0.0
        avg_rating = float(row.avg_rating) if row.avg_rating else 0.0
        total_reviews = row.total_reviews or 0
        gpa_student_count = row.gpa_student_count or 0

        easiness = calculate_easiness_score(avg_gpa, avg_difficulty, avg_rating)
        confidence = calculate_confidence_score(total_reviews, gpa_student_count)

        courses.append(
            {
                "dept": row.dept,
                "courseNumber": row.course_number,
                "courseTitle": row.course_title,
                "credits": row.credit_hours,
                "easinessScore": round(easiness * 100, 1),
                "confidenceScore": round(confidence * 100, 1),
                "professor": {
                    "id": row.professor_id or "TBA",
                    "firstName": row.first_name or "TBA",
                    "lastName": row.last_name or "",
                    "avgRating": round(avg_rating, 2),
                    "avgDifficulty": round(avg_difficulty, 2),
                    "totalRatings": total_reviews,
                    "tags": tags,
                    "avgGpa": round(avg_gpa, 2) if avg_gpa else None,
                    "percentAB": round(float(row.percent_ab), 1)
                    if row.percent_ab
                    else None,
                    "gpaStudentCount": gpa_student_count,
                    "hasGradeData": avg_gpa is not None and avg_gpa > 0,
                },
            }
        )

    courses.sort(key=lambda x: x["easinessScore"], reverse=True)
    return [UccCourseDiscovery(**c) for c in courses[:limit]]


# Course numbers that aren't regular classes: seminars, internships, directed
# and independent study, research, special topics, theses.
_NOT_REGULAR_SUFFIXES = ("81", "84", "85", "89", "90", "91", "92", "93", "94", "95", "97", "98", "99")


@router.get(
    "/{term_code}/gpa-boosters",
    summary="/discover/{term_code}/gpa-boosters",
)
@cached(TTL_WEEK)
async def discover_gpa_boosters(
    request: Request,
    term_code: str,
    campus: Optional[str] = None,
    min_gpa: float = Query(3.5, ge=0, le=4),
    min_students: int = Query(100, ge=1),
    since_year: int = Query(2022, ge=2000),
    limit: int = Query(150, ge=1, le=300),
    db: Session = Depends(get_session),
) -> List[UccCourseDiscovery]:
    """
    Undergraduate courses offered in a term, from any department, whose
    recent average GPA is high (weighted by students), one row per professor.
    Seminars, internships, research and special topics are left out.
    """
    try:
        params: Dict[str, Any] = {
            "term_code": term_code,
            "min_gpa": min_gpa,
            "min_students": min_students,
            "since_year": since_year,
            "suffixes": list(_NOT_REGULAR_SUFFIXES),
        }
        campus_filter = ""
        if campus:
            campus_filter = "AND s.campus ILIKE :campus"
            params["campus"] = f"%{campus}%"

        boosters = db.execute(
            text(
                f"""
                WITH course_gpa AS (
                    SELECT
                        dept,
                        course_number,
                        SUM(gpa * total_students) / NULLIF(SUM(total_students), 0) AS course_gpa
                    FROM gpa_data
                    WHERE gpa IS NOT NULL
                      AND year ~ '^[0-9]{{4}}$'
                      AND year::int >= :since_year
                    GROUP BY dept, course_number
                    HAVING SUM(total_students) >= :min_students
                )
                SELECT cg.dept, cg.course_number
                FROM course_gpa cg
                WHERE cg.course_gpa >= :min_gpa
                  AND cg.course_number::text < '500'
                  AND RIGHT(cg.course_number::text, 2) <> ALL(:suffixes)
                  AND EXISTS (
                      SELECT 1 FROM sections s
                      WHERE s.term_code = :term_code
                        AND s.dept = cg.dept
                        AND s.course_number = cg.course_number
                        {campus_filter}
                  )
                ORDER BY cg.course_gpa DESC
                LIMIT :limit
                """
            ),
            {**params, "limit": limit},
        ).fetchall()
        if not boosters:
            return []

        keys = [f"{b.dept}-{b.course_number}" for b in boosters]
        row_params: Dict[str, Any] = {"term_code": term_code, "keys": keys}
        course_filter = "(s.dept || '-' || s.course_number) = ANY(:keys)"
        if campus:
            course_filter += " AND s.campus ILIKE :campus"
            row_params["campus"] = params["campus"]
        return _discover_rows(
            db,
            course_filter=course_filter,
            gpa_filter="(dept || '-' || course_number) = ANY(:keys)",
            params=row_params,
            # Several professors per course; keep every booster course.
            limit=limit * 8,
        )
    except Exception as e:
        raise _db_error(e)


@router.post(
    "/{term_code}/course-professors",
    summary="/discover/{term_code}/course-professors",
)
async def discover_course_professors(
    request: Request,
    term_code: str,
    payload: CourseProfessorsRequest,
    db: Session = Depends(get_session),
) -> List[UccCourseDiscovery]:
    """
    Easiness for every professor teaching the given courses in a term, one row
    per course and professor, so section lists can show each professor's score.
    """
    try:
        keys = [k.strip().upper() for k in payload.course_keys if k and k.strip()][:300]
        if not keys:
            return []
        params: Dict[str, Any] = {"term_code": term_code, "keys": keys}
        course_filter = "(s.dept || '-' || s.course_number) = ANY(:keys)"
        campus = (payload.campus or "").strip()
        if campus:
            course_filter += " AND s.campus ILIKE :campus"
            params["campus"] = f"%{campus}%"
        return _discover_rows(
            db,
            course_filter=course_filter,
            gpa_filter="(dept || '-' || course_number) = ANY(:keys)",
            params=params,
            limit=5000,
        )
    except Exception as e:
        raise _db_error(e)


@router.get(
    "/{term_code}/{dept_code}",
    summary="/discover/{term_code}/{dept_code}",
)
async def discover_dept_courses(
    request: Request,
    term_code: str,
    dept_code: str,
    campus: Optional[str] = None,
    include_graduate: bool = False,
    db: Session = Depends(get_session),
) -> List[UccCourseDiscovery]:
    """
    Get all courses for a specific department and term, ordered by easiness score.

    Query params:
    - campus: filter by campus name (partial match, e.g. "College Station")
    - include_graduate: include 600+ level courses (default: False)
    """
    try:
        # Build dynamic WHERE clauses
        extra_filters = []
        params: Dict[str, Any] = {"term_code": term_code, "dept_code": dept_code}

        # Always hide 491 (research/independent study)
        extra_filters.append("s.course_number != '491'")

        # Graduate courses are 600+
        if not include_graduate:
            extra_filters.append("s.course_number::text < '600'")

        # Campus filter (partial case-insensitive match)
        if campus:
            extra_filters.append("s.campus ILIKE :campus")
            params["campus"] = f"%{campus}%"

        # extra_filters contains only literal conditions built from validated inputs
        # (no user data is interpolated — campus uses a bound param :campus)
        return _discover_rows(
            db,
            course_filter="s.dept = :dept_code" + "".join(" AND " + f for f in extra_filters),
            gpa_filter="dept = :dept_code",
            params=params,
        )

    except Exception as e:
        raise _db_error(e)
