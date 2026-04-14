from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from ...database.base import get_session
from ...core.cache import cached, TTL_WEEK
from pydantic import BaseModel
from fastapi import Request
import math
import json

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


def calculate_easiness_score(
    avg_gpa: float, avg_difficulty: float, avg_rating: float
) -> float:
    """
    Calculate composite easiness score.
    Formula: (GPA/4)*0.5 + ((5-difficulty)/4)*0.3 + (rating/5)*0.2
    Returns 0-1 scale, higher = easier.
    """
    gpa_norm = (avg_gpa / 4.0) if avg_gpa else 0.0
    diff_norm = ((5 - avg_difficulty) / 4.0) if avg_difficulty else 0.0
    rating_norm = (avg_rating / 5.0) if avg_rating else 0.0

    return (gpa_norm * 0.5) + (diff_norm * 0.3) + (rating_norm * 0.2)


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
    campus: Optional[str] = None


class DiscoverFitCourseMatch(BaseModel):
    course_key: str
    dept: str
    course_number: str
    course_title: str
    compatible_section_count: int
    sample_section_id: Optional[str] = None
    sample_crn: Optional[str] = None


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
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


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
        if not payload.schedule_blocks:
            return []

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
            """
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
                    s.course_title
                FROM sections s
                WHERE s.term_code = :term_code
                  AND (s.dept || '-' || s.course_number) = ANY(:course_keys)
                  AND (:campus IS NULL OR s.campus = :campus)
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
                    cs.course_title
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
                MIN(crn) AS sample_crn
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
                "campus": payload.campus,
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
            )
            for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get(
    "/{term_code}/ucc",
    summary="/discover/{term_code}/ucc",
)
@cached(TTL_WEEK)
async def discover_ucc_courses(
    request: Request, term_code: str, db: Session = Depends(get_session)
) -> List[UccCategoryGroup]:
    """
    Get all University Core Curriculum (UCC) classes for a specific term,
    grouped by category and ordered by easiness score.
    """
    try:
        query = text("""
            WITH gpa_agg AS (
                SELECT 
                    dept,
                    course_number,
                    professor,
                    AVG(gpa) as avg_gpa,
                    SUM(grade_a + grade_b)::float / NULLIF(SUM(total_students), 0) * 100 as percent_ab,
                    SUM(total_students) as total_students
                FROM gpa_data
                GROUP BY dept, course_number, professor
            )
            SELECT DISTINCT
                sad.attribute_desc,
                s.dept,
                s.course_number,
                s.course_title,
                s.credit_hours,
                p.id as professor_id,
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
            JOIN section_attributes_detailed sad ON s.id = sad.section_id
            JOIN section_instructors si ON s.id = si.section_id
            JOIN professors p ON (
                si.instructor_name ILIKE p.first_name || '%' 
                AND si.instructor_name ILIKE '%' || p.last_name
            )
            LEFT JOIN professor_summaries_new psn ON (
                p.id = psn.professor_id 
                AND psn.course_code = s.dept || s.course_number
            )
            LEFT JOIN gpa_agg g ON (
                g.dept = s.dept
                AND g.course_number = s.course_number
                AND g.professor ILIKE p.last_name || '%'
            )
            WHERE s.term_code = :term_code
              AND sad.attribute_desc = ANY(:ucc_attributes)
        """)

        result = db.execute(
            query, {"term_code": term_code, "ucc_attributes": UCC_ATTRIBUTES}
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
                avg_gpa or 0, avg_difficulty, avg_rating
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
                        "id": row.professor_id,
                        "firstName": row.first_name,
                        "lastName": row.last_name,
                        "avgRating": round(avg_rating, 2),
                        "avgDifficulty": round(avg_difficulty, 2),
                        "totalRatings": total_reviews,
                        "tags": tags,
                        "avgGpa": round(avg_gpa, 2) if avg_gpa else None,
                        "percentAB": round(float(row.percent_ab), 1)
                        if row.percent_ab
                        else None,
                        "gpaStudentCount": gpa_student_count,
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
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


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

        where_extra = ""
        if extra_filters:
            where_extra = " AND " + " AND ".join(extra_filters)

        query = text(f"""
            WITH gpa_agg AS (
                SELECT 
                    dept,
                    course_number,
                    professor,
                    AVG(gpa) as avg_gpa,
                    SUM(grade_a + grade_b)::float / NULLIF(SUM(total_students), 0) * 100 as percent_ab,
                    SUM(total_students) as total_students
                FROM gpa_data
                GROUP BY dept, course_number, professor
            )
            SELECT DISTINCT
                s.dept,
                s.course_number,
                s.course_title,
                s.credit_hours,
                p.id as professor_id,
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
            JOIN section_instructors si ON s.id = si.section_id
            JOIN professors p ON (
                si.instructor_name ILIKE p.first_name || '%' 
                AND si.instructor_name ILIKE '%' || p.last_name
            )
            LEFT JOIN professor_summaries_new psn ON (
                p.id = psn.professor_id 
                AND psn.course_code = s.dept || s.course_number
            )
            LEFT JOIN gpa_agg g ON (
                g.dept = s.dept
                AND g.course_number = s.course_number
                AND g.professor ILIKE p.last_name || '%'
            )
            WHERE s.term_code = :term_code
              AND s.dept = :dept_code
              {where_extra}
        """)

        result = db.execute(query, params)

        courses: List[Dict[str, Any]] = []
        for row in result:
            tags = row.common_tags[:5] if row.common_tags else []

            avg_gpa = float(row.avg_gpa) if row.avg_gpa else None
            avg_difficulty = float(row.avg_difficulty) if row.avg_difficulty else 0.0
            avg_rating = float(row.avg_rating) if row.avg_rating else 0.0
            total_reviews = row.total_reviews or 0
            gpa_student_count = row.gpa_student_count or 0

            easiness = calculate_easiness_score(avg_gpa or 0, avg_difficulty, avg_rating)
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
                        "id": row.professor_id,
                        "firstName": row.first_name,
                        "lastName": row.last_name,
                        "avgRating": round(avg_rating, 2),
                        "avgDifficulty": round(avg_difficulty, 2),
                        "totalRatings": total_reviews,
                        "tags": tags,
                        "avgGpa": round(avg_gpa, 2) if avg_gpa else None,
                        "percentAB": round(float(row.percent_ab), 1)
                        if row.percent_ab
                        else None,
                        "gpaStudentCount": gpa_student_count,
                    },
                }
            )

        courses.sort(key=lambda x: x["easinessScore"], reverse=True)
        courses = courses[:100]

        return [UccCourseDiscovery(**c) for c in courses]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
