"""
FastAPI application for AggieSBP API
Provides endpoints for departments, courses, and course details with aggregated data
"""

import ast
import json
import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..database.base import check_database_health, get_session


def parse_tag_frequencies(tag_frequencies_str, professor_id=None):
    """
    Helper function to parse tag_frequencies from various formats
    """
    if not tag_frequencies_str:
        return {}

    try:
        if isinstance(tag_frequencies_str, str):
            # Try to parse as JSON first
            try:
                return json.loads(tag_frequencies_str)
            except json.JSONDecodeError:
                # If JSON parsing fails, try to fix common issues (single quotes to double quotes)
                fixed_json = tag_frequencies_str.replace("'", '"')
                try:
                    return json.loads(fixed_json)
                except json.JSONDecodeError:
                    # If still fails, try using ast.literal_eval for Python dict format
                    try:
                        return ast.literal_eval(tag_frequencies_str)
                    except (ValueError, SyntaxError):
                        if professor_id:
                            logger.warning(
                                f"Failed to parse tag_frequencies for professor {professor_id}: {tag_frequencies_str[:100]}"
                            )
                        return {}
        else:
            return tag_frequencies_str
    except Exception as e:
        if professor_id:
            logger.warning(
                f"Failed to parse tag_frequencies for professor {professor_id}: {str(e)}"
            )
        return {}


# Pydantic models for request and response bodies
class CourseCompareRequest(BaseModel):
    """Request model for comparing multiple courses"""

    course_ids: List[str]


class DepartmentInfo(BaseModel):
    """Department information response model"""

    code: str
    name: str
    courses: int
    professors: int
    avgGpa: float
    rating: float


class DepartmentsInfoResponse(BaseModel):
    """Response model for departments overview"""

    total_departments: int
    total_courses: int
    total_professors: int
    overall_avg_gpa: float
    overall_avg_rating: float
    stem_departments: int
    liberal_arts_departments: int
    top_departments: List[DepartmentInfo]


class Department(BaseModel):
    """Individual department model"""

    id: str
    code: str
    name: str
    courses: int
    professors: int
    avg_gpa: float
    rating: float
    enrollment: int


class Course(BaseModel):
    """Course information model"""

    id: str
    code: str
    title: str
    department: str
    avg_gpa: float
    total_enrollment: int
    professors: int
    rating: float


class Professor(BaseModel):
    """Professor information model"""

    id: str
    name: str
    overall_rating: float
    total_reviews: int
    would_take_again_percent: float
    departments: List[str]
    courses_taught: List[str]
    total_courses: int


class Review(BaseModel):
    """Review information model"""

    id: str
    course_code: str
    course_name: str
    department_name: str
    review_text: str
    overall_rating: float
    clarity_rating: Optional[float]
    difficulty_rating: Optional[float]
    helpful_rating: Optional[float]
    would_take_again: Optional[bool]
    grade: Optional[str]
    review_date: Optional[str]
    tags: List[str]


class HealthCheck(BaseModel):
    """Health check response model"""

    status: str
    database: Dict[str, Any]
    api_version: str


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="AggieSBP API",
    description="**Texas A&M University Course and Professor Rating API**<br>This API provides comprehensive data about Texas A&M University courses, professors, and student ratings.<br><br>**Features:**<br>- **Departments**: Browse and search university departments<br>- **Courses**: Detailed course information with GPA data and ratings<br>- **Professors**: Professor profiles with reviews and ratings<br>- **Reviews**: Student reviews and ratings for courses and professors- **Comparisons**: Compare multiple courses side by side<br><br>**Data Sources:**<br>- Rate My Professor reviews and ratings<br>- Official university GPA data<br>- Course enrollment statistics<br><br>All endpoints support filtering, pagination, and detailed search capabilities.<br>",
    version="1.0.0",
    contact={
        "name": "AggieSBP Team",
        "url": "https://github.com/yourusername/AggieSBP",
        "email": "support@AggieSBP.com",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
    servers=[
        {
            "url": "https://api-aggiesbp.servehttp.com",
            "description": "Production server",
        },
        {"url": "http://localhost:8000", "description": "Development server"},
    ],
    docs_url=None,  # Disable default Swagger UI
    redoc_url="/redoc",  # Keep ReDoc available
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db_session():
    """Dependency to get database session with performance monitoring"""
    start_time = time.time()
    session = get_session()
    try:
        yield session
        session_time = time.time() - start_time
        if session_time > 0.5:  # Log slow session creation
            logger.warning(f"Slow session creation: {session_time:.3f}s")
    finally:
        session.close()


@app.get(
    "/",
    responses={
        200: {
            "description": "Welcome message",
            "content": {
                "application/json": {
                    "example": {"message": "Welcome to AggieSBP API"},
                }
            },
        }
    },
    summary="/",
    description="API root endpoint. Returns welcome message.",
    tags=["General"],
)
async def root():
    """
    Root endpoint - API welcome message

    Returns a simple welcome message to confirm the API is running.
    """
    return {"message": "Welcome to AggieSBP API"}


@app.get(
    "/favicon.ico",
    responses={
        200: {
            "description": "Favicon file",
            "content": {"image/x-icon": {"example": "Binary favicon data"}},
        },
        404: {
            "description": "Favicon not found",
            "content": {
                "application/json": {"example": {"detail": "Favicon file not found"}}
            },
        },
    },
    summary="/favicon.ico",
    description="Serves the AggieSBP favicon file for browsers.",
    tags=["General"],
    include_in_schema=False,  # Hide from API documentation
)
async def favicon():
    """Serve the AggieSBP favicon file"""
    from pathlib import Path

    # Get the path to the static favicon file
    static_dir = Path(__file__).parent / "static"
    favicon_path = static_dir / "favicon.ico"

    if favicon_path.exists():
        return FileResponse(
            path=str(favicon_path), media_type="image/x-icon", filename="favicon.ico"
        )
    else:
        # Fallback to empty response if file doesn't exist
        raise HTTPException(status_code=404, detail="Favicon file not found")


@app.get(
    "/health",
    responses={
        200: {
            "description": "System health status",
            "content": {
                "application/json": {
                    "example": {
                        "status": "healthy",
                        "database": {
                            "status": "connected",
                            "pool_size": 20,
                            "active_connections": 3,
                        },
                        "api_version": "1.0.0",
                    }
                }
            },
        }
    },
    summary="/health",
    description="Returns system health status including database connectivity and connection pool metrics.",
    tags=["System Health"],
)
async def health_check():
    """
    Health check endpoint with database status

    Returns the current health status of the API and database connection.
    """
    try:
        db_health = check_database_health()
        return {"status": "healthy", "database": db_health, "api_version": "1.0.0"}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {"status": "unhealthy", "error": str(e), "api_version": "1.0.0"}


@app.get(
    "/db-status",
    responses={
        200: {
            "description": "Database connection pool metrics",
            "content": {
                "application/json": {
                    "example": {
                        "status": "connected",
                        "pool_size": 20,
                        "active_connections": 5,
                        "checked_out": 2,
                        "checked_in": 18,
                    },
                }
            },
        },
        500: {
            "description": "Database status check failed",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Database status check failed: Connection timeout"
                    },
                }
            },
        },
    },
    summary="/db-status",
    description="Returns detailed database connection pool status and performance metrics.",
    tags=["System Health"],
)
async def database_status():
    """
    Detailed database connection pool status

    Returns comprehensive information about the database connection pool
    and current database performance metrics.
    """
    try:
        return check_database_health()
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Database status check failed: {str(e)}"
        )


@app.get("/docs", include_in_schema=False)
async def scalar_html():
    """
    Interactive API Documentation Portal

    AggieSBP API documentation powered by Scalar. Provides comprehensive documentation
    for the Texas A&M Rate My Professor API with live testing capabilities.
    """
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>AggieSBP API Documentation</title>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <style>
            body {
                margin: 0;
                padding: 0;
            }
        </style>
    </head>
    <body>
        <script
            id="api-reference"
            data-url="/openapi.json"
            data-configuration='{
                "theme": "default",
                "layout": "modern",
                "showSidebar": true,
                "hideDownloadButton": false,
                "searchHotKey": "k",
                "darkMode": true,
                "defaultHttpClient": {
                    "targetKey": "javascript",
                    "clientKey": "fetch"
                },
                "authentication": {
                    "preferredSecurityScheme": "none"
                },
                "defaultOpenAllTags": true,
                "hideModels": true
            }'
        ></script>
        <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference@latest"></script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.get(
    "/departments_info",
    responses={
        200: {
            "description": "University overview statistics",
            "content": {
                "application/json": {
                    "example": {
                        "summary": {
                            "total_departments": 156,
                            "total_courses": 8742,
                            "total_professors": 2341,
                            "overall_avg_gpa": 3.12,
                            "overall_avg_rating": 3.8,
                        },
                        "top_departments_by_courses": [
                            {
                                "code": "ENGR",
                                "name": "Engineering",
                                "courses": 420,
                                "professors": 89,
                                "avgGpa": 2.95,
                                "rating": 3.6,
                            },
                        ],
                    },
                }
            },
        }
    },
    summary="/departments_info",
    description="Returns university-wide statistics including department counts, courses, professors, and GPA averages.",
)
async def get_departments_info(db: Session = Depends(get_db_session)):
    """
    Get aggregate statistics about all departments

    Returns comprehensive overview statistics including total counts of departments,
    courses, professors, and average ratings across the university.

    **Example Response:**
    ```json

    ```
    """
    try:
        # Get aggregate statistics across all departments
        query = text("""
            SELECT 
                COUNT(DISTINCT d.id) as total_departments,
                COALESCE(SUM(last_sem.course_count), 0) as total_courses,
                COALESCE(SUM(last_sem.professor_count), 0) as total_professors,
                ROUND(AVG(last_sem.weighted_avg_gpa)::numeric, 2) as overall_avg_gpa,
                ROUND(AVG(review_ratings.avg_professor_rating)::numeric, 1) as overall_avg_rating,
                COUNT(CASE WHEN d.short_name IN ('CSCE', 'ECEN', 'ENGR', 'MATH', 'BIOL', 'CHEM', 'PHYS', 'STAT') THEN 1 END) as stem_departments,
                COUNT(CASE WHEN d.short_name NOT IN ('CSCE', 'ECEN', 'ENGR', 'MATH', 'BIOL', 'CHEM', 'PHYS', 'STAT') THEN 1 END) as liberal_arts_departments
            FROM departments d
            LEFT JOIN (
                SELECT 
                    dept,
                    COUNT(DISTINCT course_number) as course_count,
                    COUNT(DISTINCT professor) as professor_count,
                    ROUND(
                        SUM(gpa::numeric * total_students) / NULLIF(SUM(total_students), 0), 
                        2
                    ) as weighted_avg_gpa
                FROM gpa_data 
                WHERE year = '2024' AND semester = 'FALL'
                  AND gpa IS NOT NULL AND total_students > 0
                GROUP BY dept
            ) last_sem ON d.short_name = last_sem.dept
            LEFT JOIN (
                SELECT 
                    SUBSTRING(course_code FROM '^[A-Z]+') as dept_code,
                    ROUND(AVG(p.avg_rating::numeric), 1) as avg_professor_rating
                FROM reviews r
                JOIN professors p ON r.professor_id = p.id
                WHERE p.avg_rating IS NOT NULL 
                  AND r.course_code IS NOT NULL
                  AND SUBSTRING(r.course_code FROM '^[A-Z]+') IS NOT NULL
                GROUP BY SUBSTRING(r.course_code FROM '^[A-Z]+')
            ) review_ratings ON d.short_name = review_ratings.dept_code
        """)

        result = db.execute(query)
        row = result.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="No department data found")

        # Get top departments by various metrics
        top_departments_query = text("""
            SELECT 
                d.short_name as code,
                d.long_name as name,
                COALESCE(last_sem.course_count, 0) as courses,
                COALESCE(last_sem.professor_count, 0) as professors,
                COALESCE(last_sem.weighted_avg_gpa, 3.0) as avg_gpa,
                COALESCE(review_ratings.avg_professor_rating, 3.0) as rating
            FROM departments d
            LEFT JOIN (
                SELECT 
                    dept,
                    COUNT(DISTINCT course_number) as course_count,
                    COUNT(DISTINCT professor) as professor_count,
                    ROUND(
                        SUM(gpa::numeric * total_students) / NULLIF(SUM(total_students), 0), 
                        2
                    ) as weighted_avg_gpa
                FROM gpa_data 
                WHERE year = '2024' AND semester = 'FALL'
                  AND gpa IS NOT NULL AND total_students > 0
                GROUP BY dept
            ) last_sem ON d.short_name = last_sem.dept
            LEFT JOIN (
                SELECT 
                    SUBSTRING(course_code FROM '^[A-Z]+') as dept_code,
                    ROUND(AVG(p.avg_rating::numeric), 1) as avg_professor_rating
                FROM reviews r
                JOIN professors p ON r.professor_id = p.id
                WHERE p.avg_rating IS NOT NULL 
                  AND r.course_code IS NOT NULL
                  AND SUBSTRING(r.course_code FROM '^[A-Z]+') IS NOT NULL
                GROUP BY SUBSTRING(r.course_code FROM '^[A-Z]+')
            ) review_ratings ON d.short_name = review_ratings.dept_code
            WHERE last_sem.course_count > 0
            ORDER BY last_sem.course_count DESC
            LIMIT 5
        """)

        top_departments_result = db.execute(top_departments_query)
        top_departments = []

        for dept_row in top_departments_result:
            top_departments.append(
                {
                    "code": dept_row.code,
                    "name": dept_row.name,
                    "courses": int(dept_row.courses) if dept_row.courses else 0,
                    "professors": int(dept_row.professors)
                    if dept_row.professors
                    else 0,
                    "avgGpa": float(dept_row.avg_gpa) if dept_row.avg_gpa else 3.0,
                    "rating": float(dept_row.rating) if dept_row.rating else None,
                }
            )

        # Get semester statistics
        semester_stats_query = text("""
            SELECT 
                year,
                semester,
                COUNT(DISTINCT dept) as departments_with_data,
                COUNT(DISTINCT dept || course_number) as unique_courses,
                COUNT(DISTINCT professor) as unique_professors,
                SUM(total_students) as total_enrollment
            FROM gpa_data 
            WHERE total_students > 0
            GROUP BY year, semester
            ORDER BY year DESC, 
                     CASE semester 
                         WHEN 'FALL' THEN 1 
                         WHEN 'SPRING' THEN 2 
                         WHEN 'SUMMER' THEN 3 
                     END
            LIMIT 4
        """)

        semester_stats_result = db.execute(semester_stats_query)
        semester_stats = []

        for sem_row in semester_stats_result:
            semester_stats.append(
                {
                    "year": sem_row.year,
                    "semester": sem_row.semester,
                    "departments": int(sem_row.departments_with_data),
                    "courses": int(sem_row.unique_courses),
                    "professors": int(sem_row.unique_professors),
                    "enrollment": int(sem_row.total_enrollment),
                }
            )

        return {
            "summary": {
                "total_departments": int(row.total_departments)
                if row.total_departments
                else 0,
                "total_courses": int(row.total_courses) if row.total_courses else 0,
                "total_professors": int(row.total_professors)
                if row.total_professors
                else 0,
                "overall_avg_gpa": float(row.overall_avg_gpa)
                if row.overall_avg_gpa
                else 3.0,
                "overall_avg_rating": float(row.overall_avg_rating)
                if row.overall_avg_rating
                else 3.0,
                "stem_departments": int(row.stem_departments)
                if row.stem_departments
                else 0,
                "liberal_arts_departments": int(row.liberal_arts_departments)
                if row.liberal_arts_departments
                else 0,
            },
            "top_departments_by_courses": top_departments,
            "recent_semesters": semester_stats,
            "data_sources": {
                "gpa_data": "anex.us",
                "reviews": "Rate My Professor",
                "course_catalog": "Texas A&M University",
                "last_updated": "Fall 2024",
            },
        }

    except Exception as e:
        logger.error(f"Error in departments_info: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get(
    "/departments",
    responses={
        200: {
            "description": "List of academic departments",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "id": "csce",
                            "code": "CSCE",
                            "name": "Computer Science & Engineering",
                            "courses": 89,
                            "professors": 42,
                            "avgGpa": 3.21,
                            "rating": 4.1,
                        }
                    ],
                }
            },
        }
    },
    summary="/departments",
    description="Returns list of departments with course counts, professor counts, and GPA averages. Supports search and pagination.",
)
async def get_departments(
    search: Optional[str] = None,
    limit: int = 30,
    skip: int = 0,
    db: Session = Depends(get_db_session),
):
    """
    Get all departments with aggregated statistics from anex data

    Returns department information with course counts, professor counts, and average GPA
    from the most recent semester. Supports search and pagination.

    **Query Parameters:**
    - `search`: Filter by department code or name (optional)
    - `limit`: Number of results to return (default: 30)
    - `skip`: Number of results to skip for pagination (default: 0)

    **Search Example:**
    `/departments?search=computer` returns departments matching "computer" in name or code.

    **Pagination Example:**
    `/departments?limit=10&skip=20` returns 10 departments starting from the 21st result.
    """
    try:
        # Build WHERE clause for search functionality
        where_clause = ""
        params = {"limit": limit, "skip": skip}

        if search:
            where_clause = (
                "WHERE (d.short_name ILIKE :search OR d.long_name ILIKE :search)"
            )
            params["search"] = f"%{search}%"

        # Query departments with aggregated data from last semester (Fall 2024)
        query_string = f"""
            SELECT 
                d.id,
                d.short_name as code,
                d.long_name as name,
                CASE 
                    WHEN d.short_name IN ('CSCE', 'ECEN', 'ENGR', 'MATH', 'BIOL', 'CHEM', 'PHYS', 'STAT') THEN 'stem'
                    ELSE 'liberal-arts'
                END as category,
                COALESCE(last_sem.course_count, 0) as courses,
                COALESCE(last_sem.professor_count, 0) as professors,
                COALESCE(last_sem.weighted_avg_gpa, 3.0) as avgGpa,
                COALESCE(review_ratings.avg_professor_rating, 3.0) as rating,
                d.title as description
            FROM departments d
            LEFT JOIN (
                SELECT 
                    dept,
                    COUNT(DISTINCT course_number) as course_count,
                    COUNT(DISTINCT professor) as professor_count,
                    ROUND(
                        SUM(gpa::numeric * total_students) / NULLIF(SUM(total_students), 0), 
                        2
                    ) as weighted_avg_gpa
                FROM gpa_data 
                WHERE year = '2024' AND semester = 'FALL'
                  AND gpa IS NOT NULL AND total_students > 0
                GROUP BY dept
            ) last_sem ON d.short_name = last_sem.dept
            LEFT JOIN (
                SELECT 
                    SUBSTRING(course_code FROM '^[A-Z]+') as dept_code,
                    ROUND(AVG(p.avg_rating::numeric), 1) as avg_professor_rating
                FROM reviews r
                JOIN professors p ON r.professor_id = p.id
                WHERE p.avg_rating IS NOT NULL 
                  AND r.course_code IS NOT NULL
                  AND SUBSTRING(r.course_code FROM '^[A-Z]+') IS NOT NULL
                GROUP BY SUBSTRING(r.course_code FROM '^[A-Z]+')
            ) review_ratings ON d.short_name = review_ratings.dept_code
            {where_clause}
            GROUP BY d.id, d.short_name, d.long_name, d.title, 
                     last_sem.course_count, last_sem.professor_count, last_sem.weighted_avg_gpa,
                     review_ratings.avg_professor_rating
            ORDER BY d.short_name
            LIMIT :limit OFFSET :skip
        """

        formatted_query = text(query_string)

        result = db.execute(formatted_query, params)
        departments = []

        for row in result:
            # Get top 3 courses with most sections from last semester
            top_courses_query = text("""
                SELECT 
                    dept || ' ' || course_number as course_code,
                    COUNT(*) as section_count
                FROM gpa_data
                WHERE dept = :dept_code 
                  AND year = '2024' AND semester = 'FALL'
                GROUP BY dept, course_number
                ORDER BY section_count DESC, course_number
                LIMIT 3
            """)

            top_courses_result = db.execute(top_courses_query, {"dept_code": row.code})
            top_courses = [tc.course_code for tc in top_courses_result]

            departments.append(
                {
                    "id": row.id.lower(),
                    "code": row.code,
                    "name": row.name,
                    "category": row.category,
                    "courses": int(row.courses) if row.courses else 0,
                    "professors": int(row.professors) if row.professors else 0,
                    "avgGpa": float(row.avggpa) if row.avggpa else 3.0,
                    "rating": float(row.rating) if row.rating else None,
                    "topCourses": top_courses,
                    "description": row.description,
                }
            )

        return departments

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get(
    "/courses",
    responses={
        200: {
            "description": "List of courses",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "id": "CSCE121",
                            "code": "CSCE 121",
                            "name": "Introduction to Program Design and Concepts",
                            "department": {
                                "id": "CSCE",
                                "name": "Computer Science & Engineering",
                            },
                            "credits": 4,
                            "avgGPA": 3.21,
                            "difficulty": "Moderate",
                            "enrollment": 890,
                            "sections": 12,
                            "rating": 4.1,
                        }
                    ],
                }
            },
        }
    },
    summary="/courses",
    description="Returns list of courses with GPA data, enrollment, and professor ratings. Supports department filtering, search, and pagination.",
)
async def get_courses(
    department: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 30,
    skip: int = 0,
    db: Session = Depends(get_db_session),
):
    """
    Get courses with comprehensive data from recent semesters

    Returns course information including GPA data from the last 4 semesters,
    enrollment data from the most recent semester, and professor ratings.

    **Query Parameters:**
    - `department`: Filter by department code (e.g., "CSCE", "MATH")
    - `search`: Search by course code or title
    - `limit`: Number of results to return (default: 30)
    - `skip`: Number of results to skip for pagination (default: 0)

    **Filter Examples:**
    - `/courses?department=CSCE` - All Computer Science courses
    - `/courses?search=calculus` - Courses with "calculus" in title
    - `/courses?department=MATH&limit=10` - First 10 Math courses
    """
    try:
        # Base query with aggregated data from last 4 semesters and last semester
        query_base = """
            WITH last_4_semesters_gpa AS (
                SELECT 
                    dept, 
                    course_number,
                    ROUND(
                        SUM(gpa::numeric * total_students) / NULLIF(SUM(total_students), 0), 
                        2
                    ) as weighted_avg_gpa
                FROM gpa_data 
                WHERE ((year = '2024' AND semester IN ('FALL', 'SPRING', 'SUMMER')) 
                       OR (year = '2023' AND semester = 'FALL'))
                  AND gpa IS NOT NULL AND total_students > 0
                GROUP BY dept, course_number
            ),
            last_semester_data AS (
                SELECT 
                    dept,
                    course_number,
                    SUM(total_students) as total_enrollment,
                    COUNT(*) as section_count
                FROM gpa_data 
                WHERE year = '2024' AND semester = 'FALL'
                  AND total_students > 0
                GROUP BY dept, course_number
            ),
            course_reviews AS (
                SELECT 
                    SUBSTRING(course_code FROM '^[A-Z]+') as dept_code,
                    SUBSTRING(course_code FROM '[0-9]+') as course_num,
                    ROUND(AVG(p.avg_rating::numeric), 1) as avg_professor_rating
                FROM reviews r
                JOIN professors p ON r.professor_id = p.id
                WHERE p.avg_rating IS NOT NULL 
                  AND r.course_code IS NOT NULL
                  AND SUBSTRING(r.course_code FROM '^[A-Z]+') IS NOT NULL
                  AND SUBSTRING(r.course_code FROM '[0-9]+') IS NOT NULL
                GROUP BY SUBSTRING(r.course_code FROM '^[A-Z]+'), SUBSTRING(r.course_code FROM '[0-9]+')
            )
            SELECT DISTINCT
                c.subject_short_name || c.course_number as id,
                c.subject_short_name || ' ' || c.course_number as code,
                c.course_title as name,
                c.subject_short_name as department_id,
                c.subject_long_name as department_name,
                c.subject_short_name as sort_dept,
                CASE 
                    WHEN c.course_number ~ '^[0-9]+$' THEN c.course_number::int
                    ELSE COALESCE(SUBSTRING(c.course_number FROM '^[0-9]+')::int, 9999)
                END as sort_course_num,
                4 as credits,
                CASE 
                    WHEN l4s.weighted_avg_gpa IS NOT NULL THEN l4s.weighted_avg_gpa
                    ELSE -1
                END as avgGPA,
                CASE 
                    WHEN l4s.weighted_avg_gpa IS NULL THEN 'Unknown'
                    WHEN l4s.weighted_avg_gpa >= 3.7 THEN 'Light'
                    WHEN l4s.weighted_avg_gpa >= 3.3 THEN 'Moderate'
                    WHEN l4s.weighted_avg_gpa >= 2.7 THEN 'Challenging'
                    WHEN l4s.weighted_avg_gpa >= 2.0 THEN 'Intensive'
                    ELSE 'Rigorous'
                END as difficulty,
                COALESCE(lsd.total_enrollment, 0) as enrollment,
                COALESCE(lsd.section_count, 0) as sections,
                COALESCE(cr.avg_professor_rating, 3.0) as rating,
                c.description,
                CASE 
                    WHEN c.course_number ~ '^[0-9]+$' AND c.course_number::int < 300 THEN ARRAY['Undergraduate']
                    WHEN c.course_number ~ '^[0-9]+$' AND c.course_number::int < 500 THEN ARRAY['Advanced']
                    WHEN c.course_number ~ '^[0-9]+$' AND c.course_number::int >= 500 THEN ARRAY['Graduate']
                    WHEN SUBSTRING(c.course_number FROM '^[0-9]+') IS NOT NULL THEN 
                        CASE 
                            WHEN SUBSTRING(c.course_number FROM '^[0-9]+')::int < 300 THEN ARRAY['Undergraduate']
                            WHEN SUBSTRING(c.course_number FROM '^[0-9]+')::int < 500 THEN ARRAY['Advanced']
                            ELSE ARRAY['Graduate']
                        END
                    ELSE ARRAY['Other']
                END as tags
            FROM courses c
            LEFT JOIN last_4_semesters_gpa l4s ON c.subject_short_name = l4s.dept AND c.course_number = l4s.course_number
            LEFT JOIN last_semester_data lsd ON c.subject_short_name = lsd.dept AND c.course_number = lsd.course_number
            LEFT JOIN course_reviews cr ON c.subject_short_name = cr.dept_code AND c.course_number = cr.course_num
        """

        where_conditions = []
        params = {}

        if department:
            where_conditions.append("c.subject_short_name = :department")
            params["department"] = department.upper()

        if search:
            where_conditions.append(
                "(c.subject_short_name || ' ' || c.course_number ILIKE :search OR c.course_title ILIKE :search)"
            )
            params["search"] = f"%{search}%"

        where_clause = ""
        if where_conditions:
            where_clause = " WHERE " + " AND ".join(where_conditions)

        order_clause = """
            ORDER BY sort_dept, sort_course_num
        """

        limit_clause = f" LIMIT {limit} OFFSET {skip}"

        full_query = query_base + where_clause + order_clause + limit_clause

        result = db.execute(text(full_query), params)
        courses = []

        for row in result:
            # Get section attributes for Fall 2025 (latest available data)
            section_attrs_query = text("""
                SELECT DISTINCT 
                    sa.attribute_id,
                    sa.attribute_title
                FROM section_attributes sa
                WHERE sa.dept = :dept 
                  AND sa.course_number = :course_num
                  AND sa.year = '2025' 
                  AND sa.semester = 'Fall'
                ORDER BY sa.attribute_id
            """)

            section_attrs_result = db.execute(
                section_attrs_query,
                {"dept": row.department_id, "course_num": row.code.split()[1]},
            )

            # Use attribute_title when available (already formatted as "name - code"), otherwise use attribute_id
            section_attributes = []
            for attr in section_attrs_result:
                if attr.attribute_title and attr.attribute_title.strip():
                    # Use the formatted title (already contains "name - code" format)
                    section_attributes.append(attr.attribute_title)
                else:
                    # No title available, just use the code
                    section_attributes.append(attr.attribute_id)

            courses.append(
                {
                    "id": row.id,
                    "code": row.code,
                    "name": row.name,
                    "department": {
                        "id": row.department_id,
                        "name": row.department_name,
                    },
                    "credits": row.credits,
                    "avgGPA": float(row.avggpa) if row.avggpa != -1 else -1,
                    "difficulty": row.difficulty,
                    "enrollment": int(row.enrollment),
                    "sections": int(row.sections),
                    "rating": float(row.rating),
                    "description": row.description
                    or f"Course in {row.department_name}",
                    "tags": row.tags,
                    "sectionAttributes": section_attributes,
                }
            )

        return courses

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get(
    "/course/{course_id}",
    responses={
        200: {
            "description": "Detailed course information",
            "content": {
                "application/json": {
                    "example": {
                        "code": "CSCE 121",
                        "name": "Introduction to Program Design and Concepts",
                        "credits": 4,
                        "avgGPA": 3.21,
                        "difficulty": "Moderate",
                        "enrollment": 890,
                        "sections": 12,
                        "professors": [
                            {
                                "id": "prof123",
                                "name": "Dr. Sarah Johnson",
                                "rating": 4.2,
                                "reviews": 45,
                            }
                        ],
                    },
                }
            },
        },
        404: {
            "description": "Course not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Course not found"},
                }
            },
        },
    },
    summary="/course/{course_id}",
    description="Returns detailed information for a specific course including professors, grade distributions, prerequisites, and related courses.",
)
async def get_course_details(course_id: str, db: Session = Depends(get_db_session)):
    """
    Get detailed course information with comprehensive data

    Returns complete course details including professors, grade distributions,
    prerequisites, related courses, and section attributes.

    **Path Parameters:**
    - `course_id`: Course identifier (format: CSCE121, MATH151, etc.)
    """
    try:
        # Parse course_id (e.g., "CSCE120" -> dept="CSCE", course_num="120")
        import re

        match = re.match(r"^([A-Z]+)(\d+)$", course_id.upper())
        if not match:
            raise HTTPException(
                status_code=400,
                detail="Invalid course ID format. Use format like CSCE120",
            )

        dept, course_num = match.groups()

        # Get course basic info with anex data
        course_query = text("""
            WITH last_4_semesters_gpa AS (
                SELECT 
                    dept, 
                    course_number,
                    ROUND(
                        SUM(gpa::numeric * total_students) / NULLIF(SUM(total_students), 0), 
                        2
                    ) as weighted_avg_gpa
                FROM gpa_data 
                WHERE ((year = '2024' AND semester IN ('FALL', 'SPRING', 'SUMMER')) 
                       OR (year = '2023' AND semester = 'FALL'))
                  AND gpa IS NOT NULL AND total_students > 0
                GROUP BY dept, course_number
            ),
            last_semester_data AS (
                SELECT 
                    dept,
                    course_number,
                    SUM(total_students) as total_enrollment,
                    COUNT(*) as section_count
                FROM gpa_data 
                WHERE year = '2024' AND semester = 'FALL'
                  AND total_students > 0
                GROUP BY dept, course_number
            )
            SELECT 
                c.subject_short_name || ' ' || c.course_number as code,
                c.course_title as name,
                c.description,
                4 as credits,
                CASE 
                    WHEN l4s.weighted_avg_gpa IS NOT NULL THEN l4s.weighted_avg_gpa
                    ELSE -1
                END as avgGPA,
                CASE 
                    WHEN l4s.weighted_avg_gpa IS NULL THEN 'Unknown'
                    WHEN l4s.weighted_avg_gpa >= 3.7 THEN 'Light'
                    WHEN l4s.weighted_avg_gpa >= 3.3 THEN 'Moderate'
                    WHEN l4s.weighted_avg_gpa >= 2.7 THEN 'Challenging'
                    WHEN l4s.weighted_avg_gpa >= 2.0 THEN 'Intensive'
                    ELSE 'Rigorous'
                END as difficulty,
                COALESCE(lsd.total_enrollment, 0) as enrollment,
                COALESCE(lsd.section_count, 0) as sections
            FROM courses c
            LEFT JOIN last_4_semesters_gpa l4s ON c.subject_short_name = l4s.dept AND c.course_number = l4s.course_number
            LEFT JOIN last_semester_data lsd ON c.subject_short_name = lsd.dept AND c.course_number = lsd.course_number
            WHERE c.subject_short_name = :dept AND c.course_number = :course_num
            LIMIT 1
        """)

        course_result = db.execute(
            course_query, {"dept": dept, "course_num": course_num}
        ).fetchone()

        if not course_result:
            raise HTTPException(status_code=404, detail="Course not found")

        # Get grade distribution data from anex (gpa_data) directly
        professors_query = text("""
            WITH latest_semester_per_prof AS (
                SELECT 
                    gd.professor,
                    gd.dept,
                    gd.course_number,
                    gd.year,
                    gd.semester,
                    ROW_NUMBER() OVER (
                        PARTITION BY gd.professor, gd.dept, gd.course_number 
                        ORDER BY gd.year DESC, 
                                 CASE gd.semester 
                                     WHEN 'FALL' THEN 1 
                                     WHEN 'SPRING' THEN 2 
                                     WHEN 'SUMMER' THEN 3 
                                 END
                    ) as rn
                FROM gpa_data gd
                WHERE gd.dept = :dept 
                  AND gd.course_number = :course_num
                  AND gd.total_students > 0
            ),
            professor_grades AS (
                SELECT 
                    lsp.professor as anex_name,
                    SUM(gd.grade_a) as total_a,
                    SUM(gd.grade_b) as total_b,
                    SUM(gd.grade_c) as total_c,
                    SUM(gd.grade_d) as total_d,
                    SUM(gd.grade_f) as total_f,
                    SUM(gd.grade_a + gd.grade_b + gd.grade_c + gd.grade_d + gd.grade_f) as total_grades
                FROM latest_semester_per_prof lsp
                LEFT JOIN gpa_data gd ON gd.professor = lsp.professor
                    AND gd.dept = lsp.dept
                    AND gd.course_number = lsp.course_number
                    AND gd.year = lsp.year
                    AND gd.semester = lsp.semester
                WHERE lsp.rn = 1
                GROUP BY lsp.professor
            )
            SELECT 
                ps.professor_id,
                p.first_name || ' ' || p.last_name as name,
                COALESCE(p.avg_rating, NULL) as rating,
                ps.total_reviews as reviews,
                ps.tag_frequencies,
                ps.summary_text as description,
                CASE 
                    WHEN pg.total_grades > 0 THEN ROUND((pg.total_a::numeric / pg.total_grades) * 100)
                    ELSE NULL
                END as grade_a_percent,
                CASE 
                    WHEN pg.total_grades > 0 THEN ROUND((pg.total_b::numeric / pg.total_grades) * 100)
                    ELSE NULL
                END as grade_b_percent,
                CASE 
                    WHEN pg.total_grades > 0 THEN ROUND((pg.total_c::numeric / pg.total_grades) * 100)
                    ELSE NULL
                END as grade_c_percent,
                CASE 
                    WHEN pg.total_grades > 0 THEN ROUND((pg.total_d::numeric / pg.total_grades) * 100)
                    ELSE NULL
                END as grade_d_percent,
                CASE 
                    WHEN pg.total_grades > 0 THEN ROUND((pg.total_f::numeric / pg.total_grades) * 100)
                    ELSE NULL
                END as grade_f_percent
            FROM professor_grades pg
            WHERE pg.total_grades > 0
            ORDER BY pg.total_grades DESC
            LIMIT 20
        """)

        professors_result = db.execute(
            professors_query,
            {"dept": dept, "course_num": course_num},
        )

        professors = []
        for prof in professors_result:
            professor_data = {
                "name": prof.name,
                "rating": float(prof.rating) if prof.rating else None,
                "reviews": int(prof.reviews) if prof.reviews else 0,
                "description": prof.description or "Course instruction",
                "tag_frequencies": prof.tag_frequencies,
            }

            # Only include gradeDistribution if we have actual data
            if prof.grade_a_percent is not None:
                professor_data["gradeDistribution"] = {
                    "A": int(prof.grade_a_percent),
                    "B": int(prof.grade_b_percent),
                    "C": int(prof.grade_c_percent),
                    "D": int(prof.grade_d_percent),
                    "F": int(prof.grade_f_percent),
                }

            professors.append(professor_data)

        # Get prerequisites (simplified - could be enhanced with actual prerequisite data)
        prerequisites = []
        if dept == "CSCE" and int(course_num) > 120:
            prerequisites = ["MATH 151", "CSCE 110"]
        elif dept == "MATH" and int(course_num) > 151:
            prerequisites = ["MATH 150"]
        elif int(course_num) >= 300:
            prerequisites = [f"{dept} {int(course_num) - 100}"]

        # Get section attributes for Fall 2025 (latest available data)
        section_attrs_query = text("""
            SELECT DISTINCT 
                sa.attribute_id,
                sa.attribute_title
            FROM section_attributes sa
            WHERE sa.dept = :dept 
              AND sa.course_number = :course_num
              AND sa.year = '2025' 
              AND sa.semester = 'Fall'
            ORDER BY sa.attribute_id
        """)

        section_attrs_result = db.execute(
            section_attrs_query, {"dept": dept, "course_num": course_num}
        )

        # Use attribute_title when available (already formatted as "name - code"), otherwise use attribute_id
        section_attributes = []
        for attr in section_attrs_result:
            if attr.attribute_title and attr.attribute_title.strip():
                # Use the formatted title (already contains "name - code" format)
                section_attributes.append(attr.attribute_title)
            else:
                # No title available, just use the code
                section_attributes.append(attr.attribute_id)

        # Get related courses (courses in same department with similar numbers)
        related_query = text("""
            SELECT 
                c.subject_short_name || ' ' || c.course_number as code,
                c.course_title as name,
                CASE 
                    WHEN ABS(c.course_number::int - :course_num_int) <= 10 THEN 95
                    WHEN ABS(c.course_number::int - :course_num_int) <= 50 THEN 78
                    ELSE 72
                END as similarity
            FROM courses c
            WHERE c.subject_short_name = :dept 
              AND c.course_number != :course_num
              AND ABS(c.course_number::int - :course_num_int) <= 100
            ORDER BY similarity DESC, c.course_number::int
            LIMIT 3
        """)

        related_result = db.execute(
            related_query,
            {"dept": dept, "course_num": course_num, "course_num_int": int(course_num)},
        )
        related_courses = []
        for rel in related_result:
            related_courses.append(
                {"code": rel.code, "name": rel.name, "similarity": int(rel.similarity)}
            )

        course_details = {
            "code": course_result.code,
            "name": course_result.name,
            "description": course_result.description
            or f"Course covering {course_result.name.lower()} concepts and applications.",
            "credits": course_result.credits,
            "avgGPA": float(course_result.avggpa) if course_result.avggpa != -1 else -1,
            "difficulty": course_result.difficulty,
            "enrollment": int(course_result.enrollment),
            "sections": int(course_result.sections),
            "professors": professors,
            "prerequisites": prerequisites,
            "relatedCourses": related_courses,
            "sectionAttributes": section_attributes,
        }

        return course_details

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get(
    "/course/{course_id}/professors",
    responses={
        200: {
            "description": "👥 Course Faculty Roster - All Professors Teaching This Course",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "id": "prof123",
                            "name": "Dr. Sarah Johnson",
                            "overall_rating": 4.2,
                            "total_reviews": 45,
                            "would_take_again_percent": 78.5,
                            "courses": [
                                {
                                    "course_id": "CSCE121",
                                    "course_name": "Introduction to Programming",
                                    "reviews_count": 15,
                                    "avg_rating": 4.1,
                                }
                            ],
                            "departments": ["CSCE"],
                            "recent_reviews": [
                                {
                                    "id": "review456",
                                    "course_code": "CSCE121",
                                    "course_name": "Introduction to Programming",
                                    "review_text": "Great professor! Explains concepts clearly and is very helpful during office hours.",
                                    "overall_rating": 4.5,
                                    "would_take_again": True,
                                    "grade": "A",
                                    "review_date": "2024-05-15T00:00:00",
                                    "tags": ["Helpful", "Clear", "Fair Grader"],
                                }
                            ],
                            "tag_frequencies": {"Clear": 15, "Helpful": 12, "Fair": 8},
                        }
                    ],
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {
                                    "type": "string",
                                    "description": "Professor identifier",
                                },
                                "name": {
                                    "type": "string",
                                    "description": "Professor name",
                                },
                                "overall_rating": {
                                    "type": "number",
                                    "description": "Average rating across all courses",
                                },
                                "total_reviews": {
                                    "type": "integer",
                                    "description": "Total reviews received",
                                },
                                "would_take_again_percent": {
                                    "type": "number",
                                    "description": "Student recommendation rate",
                                },
                                "courses": {
                                    "type": "array",
                                    "description": "All courses taught by professor",
                                },
                                "departments": {
                                    "type": "array",
                                    "description": "Department affiliations",
                                },
                                "recent_reviews": {
                                    "type": "array",
                                    "description": "Latest student reviews",
                                },
                                "tag_frequencies": {
                                    "type": "object",
                                    "description": "Common student feedback tags",
                                },
                            },
                        },
                    },
                }
            },
        },
        404: {
            "description": "❌ Course Not Found - Invalid Course ID",
            "content": {
                "application/json": {
                    "example": {"detail": "Course not found"},
                    "schema": {
                        "type": "object",
                        "properties": {
                            "detail": {"type": "string", "description": "Error message"}
                        },
                    },
                }
            },
        },
    },
    summary="/course/{course_id}/professors",
    description="Returns all professors teaching a specific course with ratings, reviews, and student feedback.",
)
async def get_course_professors(course_id: str, db: Session = Depends(get_db_session)):
    """
    Get all professors who teach a specific course

    Returns detailed professor information for all instructors who teach
    the specified course, including their ratings, reviews, and tag frequencies.

    **Path Parameters:**
    - `course_id`: Course identifier (format: CSCE121, MATH151, etc.)
    """
    try:
        # Parse course_id (e.g., "CSCE120" -> dept="CSCE", course_num="120")
        import re

        match = re.match(r"^([A-Z]+)(\d+)$", course_id.upper())
        if not match:
            raise HTTPException(
                status_code=400,
                detail="Invalid course ID format. Use format like CSCE120",
            )

        dept, course_num = match.groups()

        # Get professors who teach this course
        professors_query = text("""
            SELECT DISTINCT
                ps.professor_id,
                p.first_name || ' ' || p.last_name as name,
                ps.total_reviews,
                ps.avg_difficulty,
                p.avg_rating,
                ps.tag_frequencies
            FROM professor_summaries ps
            JOIN professors p ON ps.professor_id = p.id
            WHERE ps.course_code = :course_code
            ORDER BY ps.total_reviews DESC
        """)

        professors_result = db.execute(
            professors_query, {"course_code": course_id.upper()}
        )
        professor_profiles = []

        for prof_row in professors_result:
            professor_id = prof_row.professor_id

            # Get would_take_again percentage
            would_take_again_query = text("""
                SELECT 
                    ROUND(
                        AVG(CASE WHEN would_take_again = 1 THEN 100.0 ELSE 0.0 END)::numeric, 
                        1
                    ) as would_take_again_percent
                FROM reviews 
                WHERE professor_id = :professor_id 
                  AND would_take_again IS NOT NULL
            """)

            would_take_again_result = db.execute(
                would_take_again_query, {"professor_id": professor_id}
            ).fetchone()

            # Get all courses for this professor
            all_courses_query = text("""
                SELECT 
                    ps.course_code as course_id,
                    COALESCE(c.course_title, 'Course Title') as course_name,
                    ps.total_reviews as reviews_count,
                    COALESCE(p.avg_rating, NULL) as avg_rating
                FROM professor_summaries ps
                LEFT JOIN courses c ON c.subject_short_name || c.course_number = ps.course_code
                LEFT JOIN professors p ON ps.professor_id = p.id
                WHERE ps.professor_id = :professor_id
                ORDER BY ps.total_reviews DESC
            """)

            all_courses_result = db.execute(
                all_courses_query, {"professor_id": professor_id}
            )
            courses = []
            for course in all_courses_result:
                courses.append(
                    {
                        "course_id": course.course_id,
                        "course_name": course.course_name,
                        "reviews_count": int(course.reviews_count)
                        if course.reviews_count
                        else 0,
                        "avg_rating": float(course.avg_rating)
                        if course.avg_rating
                        else None,
                    }
                )

            # Get departments
            departments_query = text("""
                SELECT DISTINCT SUBSTRING(ps.course_code FROM '^[A-Z]+') as dept
                FROM professor_summaries ps
                WHERE ps.professor_id = :professor_id
            """)

            departments_result = db.execute(
                departments_query, {"professor_id": professor_id}
            )
            departments = [row.dept for row in departments_result]

            # Get recent reviews for this course-professor combination
            recent_reviews_query = text("""
                SELECT 
                    r.id,
                    r.review_text,
                    r.clarity_rating,
                    r.difficulty_rating,
                    r.helpful_rating,
                    r.would_take_again,
                    r.grade,
                    r.review_date,
                    r.course_code,
                    r.rating_tags,
                    COALESCE(c.course_title, 'Course') as course_name
                FROM reviews r
                LEFT JOIN courses c ON c.subject_short_name || c.course_number = r.course_code
                WHERE r.professor_id = :professor_id
                  AND r.course_code = :course_code
                  AND r.review_text IS NOT NULL
                  AND r.review_text != ''
                ORDER BY r.review_date DESC
                LIMIT 3
            """)

            recent_reviews_result = db.execute(
                recent_reviews_query,
                {"professor_id": professor_id, "course_code": course_id.upper()},
            )

            recent_reviews = []
            for review in recent_reviews_result:
                overall_rating = (
                    round(
                        (
                            (review.clarity_rating or 0)
                            + (6 - (review.difficulty_rating or 3))
                            + (review.helpful_rating or 0)
                        )
                        / 3,
                        1,
                    )
                    if any(
                        [
                            review.clarity_rating,
                            review.difficulty_rating,
                            review.helpful_rating,
                        ]
                    )
                    else 0
                )

                # Parse rating tags
                tags = []
                if review.rating_tags:
                    try:
                        tags = (
                            json.loads(review.rating_tags)
                            if isinstance(review.rating_tags, str)
                            else review.rating_tags
                        )
                    except:
                        tags = []

                recent_reviews.append(
                    {
                        "id": review.id,
                        "course_code": review.course_code,
                        "course_name": review.course_name,
                        "review_text": review.review_text,
                        "overall_rating": overall_rating,
                        "would_take_again": review.would_take_again == 1
                        if review.would_take_again is not None
                        else None,
                        "grade": review.grade,
                        "review_date": review.review_date.isoformat()
                        if review.review_date
                        else None,
                        "tags": tags,
                    }
                )

            # Parse tag frequencies
            tag_frequencies = parse_tag_frequencies(
                prof_row.tag_frequencies, professor_id
            )

            professor_profile = {
                "id": professor_id,
                "name": prof_row.name,
                "overall_rating": prof_row.avg_rating
                if prof_row.avg_rating is not None
                else None,
                "total_reviews": int(prof_row.total_reviews)
                if prof_row.total_reviews
                else 0,
                "would_take_again_percent": float(
                    would_take_again_result.would_take_again_percent
                )
                if would_take_again_result
                and would_take_again_result.would_take_again_percent
                else 0.0,
                "courses": courses,
                "departments": departments,
                "recent_reviews": recent_reviews,
                "tag_frequencies": tag_frequencies,
            }

            professor_profiles.append(professor_profile)

        if not professor_profiles:
            raise HTTPException(
                status_code=404, detail="No professors found for this course"
            )

        return professor_profiles

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_course_professors: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get(
    "/course/{course_id}/reviews/{professor_id}",
    responses={
        200: {
            "description": "📋 Course-Professor Review Analysis - Targeted Student Feedback",
            "content": {
                "application/json": {
                    "example": {
                        "courseCode": "CSCE121",
                        "professorId": "prof123",
                        "professorName": "Dr. Sarah Johnson",
                        "totalReviews": 45,
                        "reviews": [
                            {
                                "id": "review456",
                                "overallRating": 4.3,
                                "clarityRating": 4.5,
                                "difficultyRating": 3.0,
                                "helpfulnessRating": 4.2,
                                "wouldTakeAgain": True,
                                "attendanceMandatory": False,
                                "isOnlineClass": False,
                                "isForCredit": True,
                                "reviewText": "Great professor! Explains concepts clearly and is very helpful during office hours. Assignments are challenging but fair.",
                                "grade": "A",
                                "reviewDate": "May 2024",
                                "textbookUse": "Required",
                                "helpfulness": 12,
                                "totalVotes": 15,
                                "tags": ["Helpful", "Clear", "Fair Grader"],
                                "teacherNote": None,
                            },
                            {
                                "id": "review789",
                                "overallRating": 3.7,
                                "clarityRating": 4.0,
                                "difficultyRating": 4.0,
                                "helpfulnessRating": 3.5,
                                "wouldTakeAgain": False,
                                "attendanceMandatory": True,
                                "isOnlineClass": False,
                                "isForCredit": True,
                                "reviewText": "Course content is good but the pace is quite fast. Make sure to keep up with assignments.",
                                "grade": "B",
                                "reviewDate": "April 2024",
                                "textbookUse": "Recommended",
                                "helpfulness": 8,
                                "totalVotes": 10,
                                "tags": ["Fast Paced", "Attendance Required"],
                                "teacherNote": None,
                            },
                        ],
                        "pagination": {"limit": 50, "skip": 0, "hasMore": False},
                    },
                    "schema": {
                        "type": "object",
                        "properties": {
                            "courseCode": {
                                "type": "string",
                                "description": "Course code being reviewed",
                            },
                            "professorId": {
                                "type": "string",
                                "description": "Professor identifier",
                            },
                            "professorName": {
                                "type": "string",
                                "description": "Professor's full name",
                            },
                            "totalReviews": {
                                "type": "integer",
                                "description": "Total reviews for this course-professor combination",
                            },
                            "reviews": {
                                "type": "array",
                                "description": "Collection of student reviews",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "id": {
                                            "type": "string",
                                            "description": "Review identifier",
                                        },
                                        "overallRating": {
                                            "type": "number",
                                            "description": "Overall rating (1-5)",
                                        },
                                        "clarityRating": {
                                            "type": "number",
                                            "description": "Teaching clarity rating",
                                        },
                                        "difficultyRating": {
                                            "type": "number",
                                            "description": "Course difficulty rating",
                                        },
                                        "helpfulnessRating": {
                                            "type": "number",
                                            "description": "Professor helpfulness rating",
                                        },
                                        "wouldTakeAgain": {
                                            "type": "boolean",
                                            "description": "Would recommend",
                                        },
                                        "attendanceMandatory": {
                                            "type": "boolean",
                                            "description": "Attendance required",
                                        },
                                        "isOnlineClass": {
                                            "type": "boolean",
                                            "description": "Online format",
                                        },
                                        "isForCredit": {
                                            "type": "boolean",
                                            "description": "Taken for credit",
                                        },
                                        "reviewText": {
                                            "type": "string",
                                            "description": "Detailed review text",
                                        },
                                        "grade": {
                                            "type": "string",
                                            "description": "Grade received",
                                        },
                                        "reviewDate": {
                                            "type": "string",
                                            "description": "Review date",
                                        },
                                        "textbookUse": {
                                            "type": "string",
                                            "description": "Textbook requirement",
                                        },
                                        "helpfulness": {
                                            "type": "integer",
                                            "description": "Helpful votes",
                                        },
                                        "totalVotes": {
                                            "type": "integer",
                                            "description": "Total votes",
                                        },
                                        "tags": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                            "description": "Review tags",
                                        },
                                        "teacherNote": {
                                            "type": "string",
                                            "description": "Professor response",
                                        },
                                    },
                                },
                            },
                            "pagination": {
                                "type": "object",
                                "description": "Pagination information",
                                "properties": {
                                    "limit": {
                                        "type": "integer",
                                        "description": "Results per page",
                                    },
                                    "skip": {
                                        "type": "integer",
                                        "description": "Results skipped",
                                    },
                                    "hasMore": {
                                        "type": "boolean",
                                        "description": "More results available",
                                    },
                                },
                            },
                        },
                    },
                }
            },
        },
        404: {
            "description": "❌ Course or Professor Not Found - Invalid Combination",
            "content": {
                "application/json": {
                    "examples": {
                        "professor_not_found": {
                            "summary": "Professor Not Found",
                            "description": "The specified professor does not exist or doesn't teach this course",
                            "value": {"detail": "Professor not found"},
                        },
                        "course_not_found": {
                            "summary": "Course Not Found",
                            "description": "The specified course does not exist",
                            "value": {"detail": "Course not found"},
                        },
                    },
                    "schema": {
                        "type": "object",
                        "properties": {
                            "detail": {
                                "type": "string",
                                "description": "Error message explaining what wasn't found",
                            }
                        },
                    },
                }
            },
        },
    },
    summary="/course/{course_id}/reviews/{professor_id}",
    description="Returns student reviews for a specific course-professor combination with ratings and detailed feedback.",
)
async def get_course_professor_reviews(
    course_id: str,
    professor_id: str,
    limit: int = 50,
    skip: int = 0,
    db: Session = Depends(get_db_session),
):
    """
    Get all reviews for a specific course and professor combination

    Returns detailed student reviews for a specific professor teaching a specific course,
    including ratings, review text, grades, and helpfulness scores.

    **Path Parameters:**
    - `course_id`: Course identifier (e.g., CSCE121)
    - `professor_id`: Professor identifier

    **Query Parameters:**
    - `limit`: Number of reviews to return (default: 50)
    - `skip`: Number of reviews to skip for pagination (default: 0)
    """
    try:
        # Parse course_id (e.g., "CSCE120" -> dept="CSCE", course_num="120")
        import re

        match = re.match(r"^([A-Z]+)(\d+)$", course_id.upper())
        if not match:
            raise HTTPException(
                status_code=400,
                detail="Invalid course ID format. Use format like CSCE120",
            )

        dept, course_num = match.groups()

        # Get professor name for context
        professor_query = text("""
            SELECT first_name || ' ' || last_name as name
            FROM professors 
            WHERE id = :professor_id
        """)

        professor_result = db.execute(
            professor_query, {"professor_id": professor_id}
        ).fetchone()

        if not professor_result:
            raise HTTPException(status_code=404, detail="Professor not found")

        # Get reviews - try multiple course code formats
        reviews_query = text("""
            SELECT 
                id,
                legacy_id,
                clarity_rating,
                difficulty_rating,
                helpful_rating,
                would_take_again,
                attendance_mandatory,
                is_online_class,
                is_for_credit,
                review_text,
                grade,
                review_date,
                textbook_use,
                thumbs_up_total,
                thumbs_down_total,
                rating_tags,
                teacher_note
            FROM reviews 
            WHERE professor_id = :professor_id 
              AND (course_code = :course_code_format1 
                   OR course_code = :course_code_format2 
                   OR course_code = :course_code_format3)
            ORDER BY review_date DESC
            LIMIT :limit OFFSET :skip
        """)

        # Try different course code formats that might exist in the database
        course_formats = {
            "course_code_format1": course_id.upper(),  # CSCE120
            "course_code_format2": f"{dept}-{course_num}",  # CSCE-120
            "course_code_format3": course_num,  # 120
        }

        reviews_result = db.execute(
            reviews_query,
            {
                "professor_id": professor_id,
                "limit": limit,
                "skip": skip,
                **course_formats,
            },
        )

        reviews = []
        for review in reviews_result:
            # Calculate overall rating from individual ratings
            overall_rating = (
                round(
                    (
                        (review.clarity_rating or 0)
                        + (review.difficulty_rating or 0)
                        + (review.helpful_rating or 0)
                    )
                    / 3,
                    1,
                )
                if any(
                    [
                        review.clarity_rating,
                        review.difficulty_rating,
                        review.helpful_rating,
                    ]
                )
                else 0
            )

            # Convert would_take_again to boolean
            would_take_again = (
                review.would_take_again == 1
                if review.would_take_again is not None
                else None
            )

            # Format review date
            review_date = (
                review.review_date.strftime("%B %Y")
                if review.review_date
                else "Unknown"
            )

            # Calculate helpfulness score
            total_votes = (review.thumbs_up_total or 0) + (
                review.thumbs_down_total or 0
            )
            helpfulness = review.thumbs_up_total or 0 if total_votes > 0 else 0

            reviews.append(
                {
                    "id": review.legacy_id or review.id,
                    "overallRating": overall_rating,
                    "clarityRating": review.clarity_rating,
                    "difficultyRating": review.difficulty_rating,
                    "helpfulnessRating": review.helpful_rating,
                    "wouldTakeAgain": would_take_again,
                    "attendanceMandatory": review.attendance_mandatory,
                    "isOnlineClass": review.is_online_class,
                    "isForCredit": review.is_for_credit,
                    "reviewText": review.review_text,
                    "grade": review.grade,
                    "reviewDate": review_date,
                    "textbookUse": review.textbook_use,
                    "helpfulness": helpfulness,
                    "totalVotes": total_votes,
                    "tags": review.rating_tags or [],
                    "teacherNote": review.teacher_note,
                }
            )

        # Get total count for pagination
        count_query = text("""
            SELECT COUNT(*) as total
            FROM reviews 
            WHERE professor_id = :professor_id 
              AND (course_code = :course_code_format1 
                   OR course_code = :course_code_format2 
                   OR course_code = :course_code_format3)
        """)

        count_result = db.execute(
            count_query, {"professor_id": professor_id, **course_formats}
        ).fetchone()

        total_reviews = count_result.total if count_result else 0

        return {
            "courseCode": course_id.upper(),
            "professorId": professor_id,
            "professorName": professor_result.name,
            "totalReviews": total_reviews,
            "reviews": reviews,
            "pagination": {
                "limit": limit,
                "skip": skip,
                "hasMore": skip + len(reviews) < total_reviews,
            },
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.post(
    "/courses/compare",
    responses={
        200: {
            "description": "📊 Course Comparison Analysis - Side-by-Side Academic Metrics",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "id": "CSCE121",
                            "code": "CSCE 121",
                            "name": "Introduction to Program Design and Concepts",
                            "description": "Fundamental concepts of computer programming with an object-oriented approach using C++.",
                            "credits": 4,
                            "avgGPA": 3.21,
                            "difficulty": "Moderate",
                            "rating": 4.1,
                            "reviewCount": 45,
                            "enrollment": 890,
                            "sections": 12,
                            "professors": [
                                {
                                    "id": "prof123",
                                    "name": "Dr. Sarah Johnson",
                                    "rating": 4.2,
                                    "reviews": 45,
                                }
                            ],
                            "sectionAttributes": [
                                "Core Curriculum - CTR",
                                "Writing Intensive - W",
                            ],
                        },
                        {
                            "id": "MATH151",
                            "code": "MATH 151",
                            "name": "Engineering Mathematics I",
                            "description": "Differential and integral calculus, applications to engineering problems.",
                            "credits": 4,
                            "avgGPA": 2.95,
                            "difficulty": "Challenging",
                            "rating": 3.7,
                            "reviewCount": 32,
                            "enrollment": 1245,
                            "sections": 18,
                            "professors": [
                                {
                                    "id": "prof456",
                                    "name": "Dr. Michael Chen",
                                    "rating": 3.8,
                                    "reviews": 32,
                                }
                            ],
                            "sectionAttributes": [
                                "Core Curriculum - MATH",
                                "MPE Required",
                            ],
                        },
                    ],
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {
                                    "type": "string",
                                    "description": "Course identifier",
                                },
                                "code": {
                                    "type": "string",
                                    "description": "Course code",
                                },
                                "name": {
                                    "type": "string",
                                    "description": "Course title",
                                },
                                "description": {
                                    "type": "string",
                                    "description": "Course description",
                                },
                                "credits": {
                                    "type": "integer",
                                    "description": "Credit hours",
                                },
                                "avgGPA": {
                                    "type": "number",
                                    "description": "Average GPA",
                                },
                                "difficulty": {
                                    "type": "string",
                                    "description": "Difficulty level",
                                },
                                "rating": {
                                    "type": "number",
                                    "description": "Student rating",
                                },
                                "reviewCount": {
                                    "type": "integer",
                                    "description": "Number of reviews",
                                },
                                "enrollment": {
                                    "type": "integer",
                                    "description": "Enrollment numbers",
                                },
                                "sections": {
                                    "type": "integer",
                                    "description": "Number of sections",
                                },
                                "professors": {
                                    "type": "array",
                                    "description": "Teaching faculty",
                                },
                                "sectionAttributes": {
                                    "type": "array",
                                    "description": "Course attributes",
                                },
                            },
                        },
                    },
                }
            },
        },
        400: {
            "description": "❌ Invalid Request - Course ID Format Error",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Invalid course ID format. Use format like CSCE120"
                    },
                    "schema": {
                        "type": "object",
                        "properties": {
                            "detail": {
                                "type": "string",
                                "description": "Error message explaining the issue",
                            }
                        },
                    },
                }
            },
        },
    },
    summary="/courses/compare",
    description="Compare multiple courses side-by-side with GPA data, ratings, enrollment, and professor information.",
)
async def compare_courses(
    request: CourseCompareRequest, db: Session = Depends(get_db_session)
):
    """
    Bulk fetch course details for comparison

    Returns detailed course information for multiple courses at once,
    enabling efficient comparison of courses across various metrics.

    **Request Body:**
    ```json
    {
        "course_ids": ["CSCE121", "MATH151", "ENGL104"]
    }
    ```

    **Use Cases:**
    - Compare difficulty and GPA across multiple courses
    - Analyze professor ratings for different courses
    - Build course comparison interfaces for registration
    """
    try:
        course_details = []

        for course_id in request.course_ids:
            # Parse course_id (e.g., "CSCE120" -> dept="CSCE", course_num="120")
            import re

            match = re.match(r"^([A-Z]+)(\d+)$", course_id.upper())
            if not match:
                # Skip invalid course IDs rather than failing the entire request
                continue

            dept, course_num = match.groups()

            # Get course basic info with anex data (reuse logic from /course/{course_id})
            course_query = text("""
                WITH last_4_semesters_gpa AS (
                    SELECT 
                        dept, 
                        course_number,
                        ROUND(
                            SUM(gpa::numeric * total_students) / NULLIF(SUM(total_students), 0), 
                            2
                        ) as weighted_avg_gpa
                    FROM gpa_data 
                    WHERE ((year = '2024' AND semester IN ('FALL', 'SPRING', 'SUMMER')) 
                           OR (year = '2023' AND semester = 'FALL'))
                      AND gpa IS NOT NULL AND total_students > 0
                    GROUP BY dept, course_number
                ),
                last_semester_data AS (
                    SELECT 
                        dept,
                        course_number,
                        SUM(total_students) as total_enrollment,
                        COUNT(*) as section_count
                    FROM gpa_data 
                    WHERE year = '2024' AND semester = 'FALL'
                      AND total_students > 0
                    GROUP BY dept, course_number
                )
                SELECT 
                    c.subject_short_name || ' ' || c.course_number as code,
                    c.course_title as name,
                    c.description,
                    4 as credits,
                    CASE 
                        WHEN l4s.weighted_avg_gpa IS NOT NULL THEN l4s.weighted_avg_gpa
                        ELSE -1
                    END as avggpa,
                    CASE 
                        WHEN l4s.weighted_avg_gpa IS NULL THEN 'Unknown'
                        WHEN l4s.weighted_avg_gpa >= 3.7 THEN 'Light'
                        WHEN l4s.weighted_avg_gpa >= 3.3 THEN 'Moderate'
                        WHEN l4s.weighted_avg_gpa >= 2.7 THEN 'Challenging'
                        WHEN l4s.weighted_avg_gpa >= 2.0 THEN 'Intensive'
                        ELSE 'Rigorous'
                    END as difficulty,
                    COALESCE(lsd.total_enrollment, 0) as enrollment,
                    COALESCE(lsd.section_count, 0) as sections
                FROM courses c
                LEFT JOIN last_4_semesters_gpa l4s ON c.subject_short_name = l4s.dept AND c.course_number = l4s.course_number
                LEFT JOIN last_semester_data lsd ON c.subject_short_name = lsd.dept AND c.course_number = lsd.course_number
                WHERE c.subject_short_name = :dept AND c.course_number = :course_num
                LIMIT 1
            """)

            course_result = db.execute(
                course_query, {"dept": dept, "course_num": course_num}
            ).fetchone()

            if not course_result:
                continue  # Skip courses that don't exist

            # Get professors for this course (simplified version)
            professors_query = text("""
                SELECT 
                    ps.professor_id,
                    p.first_name || ' ' || p.last_name as name,
                    COALESCE(p.avg_rating, NULL) as rating,
                    ps.total_reviews as reviews
                FROM professor_summaries ps
                JOIN professors p ON ps.professor_id = p.id
                WHERE ps.course_code = :course_code
                ORDER BY ps.total_reviews DESC
                LIMIT 5
            """)

            professors_result = db.execute(
                professors_query, {"course_code": course_id.upper()}
            )
            professors = []
            for prof in professors_result:
                professors.append(
                    {
                        "id": prof.professor_id,
                        "name": prof.name,
                        "rating": float(prof.rating) if prof.rating else 3.0,
                        "reviews": int(prof.reviews) if prof.reviews else 0,
                    }
                )

            # Get section attributes
            section_attrs_query = text("""
                SELECT DISTINCT 
                    sa.attribute_id,
                    sa.attribute_title
                FROM section_attributes sa
                WHERE sa.dept = :dept 
                  AND sa.course_number = :course_num
                  AND sa.year = '2025' 
                  AND sa.semester = 'Fall'
                ORDER BY sa.attribute_id
            """)

            section_attrs_result = db.execute(
                section_attrs_query, {"dept": dept, "course_num": course_num}
            )
            section_attributes = []
            for attr in section_attrs_result:
                if attr.attribute_title and attr.attribute_title.strip():
                    section_attributes.append(attr.attribute_title)
                else:
                    section_attributes.append(attr.attribute_id)

            # Calculate rating based on difficulty (6 - average difficulty from reviews)
            rating_query = text("""
                SELECT 
                    ROUND((6.0 - AVG(difficulty_rating))::numeric, 1) as course_rating,
                    COUNT(*) as review_count
                FROM reviews 
                WHERE course_code = :course_code
                  AND difficulty_rating IS NOT NULL
            """)

            rating_result = db.execute(
                rating_query, {"course_code": course_id.upper()}
            ).fetchone()
            course_rating = (
                float(rating_result.course_rating)
                if rating_result and rating_result.course_rating
                else 3.0
            )
            review_count = (
                int(rating_result.review_count)
                if rating_result and rating_result.review_count
                else 0
            )

            course_detail = {
                "id": course_id.upper(),
                "code": course_result.code,
                "name": course_result.name,
                "description": course_result.description
                or f"Course covering {course_result.name.lower()} concepts and applications.",
                "credits": course_result.credits,
                "avgGPA": float(course_result.avggpa)
                if course_result.avggpa != -1
                else -1,
                "difficulty": course_result.difficulty,
                "rating": course_rating,
                "reviewCount": review_count,
                "enrollment": int(course_result.enrollment),
                "sections": int(course_result.sections),
                "professors": professors,
                "sectionAttributes": section_attributes,
            }

            course_details.append(course_detail)

        return course_details

    except Exception as e:
        logger.error(f"Error in courses compare: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get(
    "/professors",
    responses={
        200: {
            "description": "👨‍🏫 Faculty Directory - Professor Profiles with Ratings & Statistics",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "id": "prof123",
                            "name": "Dr. Sarah Johnson",
                            "overall_rating": 4.2,
                            "total_reviews": 45,
                            "departments": ["CSCE"],
                            "courses_taught": ["CSCE121", "CSCE221", "CSCE314"],
                        },
                        {
                            "id": "prof456",
                            "name": "Dr. Michael Chen",
                            "overall_rating": 3.8,
                            "total_reviews": 32,
                            "departments": ["MATH"],
                            "courses_taught": ["MATH151", "MATH152", "MATH251"],
                        },
                        {
                            "id": "prof789",
                            "name": "Dr. Emily Rodriguez",
                            "overall_rating": 4.5,
                            "total_reviews": 67,
                            "departments": ["ENGL"],
                            "courses_taught": ["ENGL104", "ENGL210", "ENGL301"],
                        },
                    ],
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {
                                    "type": "string",
                                    "description": "Unique professor identifier",
                                },
                                "name": {
                                    "type": "string",
                                    "description": "Professor's full name with title",
                                },
                                "overall_rating": {
                                    "type": "number",
                                    "description": "Average rating across all reviews (1-5 scale)",
                                    "minimum": 1.0,
                                    "maximum": 5.0,
                                },
                                "total_reviews": {
                                    "type": "integer",
                                    "description": "Total number of student reviews received",
                                },
                                "departments": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "List of departments where professor teaches",
                                },
                                "courses_taught": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Course codes taught by the professor",
                                },
                            },
                        },
                    },
                }
            },
        }
    },
    summary="/professors",
    description="Returns list of professors with ratings, review counts, departments, and courses taught. Supports search, department filtering, and pagination.",
)
async def get_professors(
    search: Optional[str] = None,
    department: Optional[str] = None,
    limit: int = 30,
    skip: int = 0,
    min_rating: Optional[float] = None,
    db: Session = Depends(get_db_session),
):
    """
    List all professors with basic statistics

    Returns a list of professors with their ratings, review counts, departments,
    and courses taught. Supports filtering and search functionality.

    **Query Parameters:**
    - `search`: Search by professor name (optional)
    - `department`: Filter by department code (e.g., "CSCE", "MATH")
    - `min_rating`: Minimum rating filter (1.0-5.0)
    - `limit`: Number of results to return (default: 30)
    - `skip`: Number of results to skip for pagination (default: 0)

    **Filter Examples:**
    - `/professors?search=johnson` - Search for professors named Johnson
    - `/professors?department=CSCE` - All Computer Science professors
    - `/professors?min_rating=4.0` - Professors with rating 4.0 or higher
    - `/professors?department=MATH&min_rating=3.5&limit=10` - Top 10 Math professors with rating ≥ 3.5
    """
    try:
        # Build WHERE conditions
        where_conditions = []
        params = {"limit": limit, "skip": skip}

        if search:
            where_conditions.append(
                "(p.first_name ILIKE :search OR p.last_name ILIKE :search OR (p.first_name || ' ' || p.last_name) ILIKE :search)"
            )
            params["search"] = f"%{search}%"

        if department:
            where_conditions.append(
                "EXISTS (SELECT 1 FROM professor_summaries ps WHERE ps.professor_id = p.id AND SUBSTRING(ps.course_code FROM '^[A-Z]+') = :department)"
            )
            params["department"] = department.upper()

        if min_rating:
            where_conditions.append("pst.overall_rating IS NOT NULL AND pst.overall_rating >= :min_rating")
            params["min_rating"] = min_rating

        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)

        # Get professors with aggregated statistics
        professors_query = text(f"""
            WITH professor_stats AS (
                SELECT 
                    ps.professor_id,
                    COUNT(DISTINCT ps.course_code) as total_courses,
                    SUM(ps.total_reviews) as total_reviews,
                    p.avg_rating as overall_rating,
                    ARRAY_AGG(DISTINCT SUBSTRING(ps.course_code FROM '^[A-Z]+')) as departments,
                    ARRAY_AGG(DISTINCT ps.course_code) as courses_taught
                FROM professor_summaries ps
                JOIN professors p ON ps.professor_id = p.id
                GROUP BY ps.professor_id, p.avg_rating
            )
            SELECT DISTINCT
                p.id,
                p.first_name || ' ' || p.last_name as name,
                p.first_name,
                p.last_name,
                COALESCE(pst.overall_rating, NULL) as overall_rating,
                COALESCE(pst.total_reviews, 0) as total_reviews,
                COALESCE(pst.departments, ARRAY[]::text[]) as departments,
                COALESCE(pst.courses_taught, ARRAY[]::text[]) as courses_taught
            FROM professors p
            LEFT JOIN professor_stats pst ON p.id = pst.professor_id
            {where_clause}
            ORDER BY COALESCE(pst.total_reviews, 0) DESC, p.last_name, p.first_name
            LIMIT :limit OFFSET :skip
        """)

        result = db.execute(professors_query, params)
        professors = []

        for row in result:
            professors.append(
                {
                    "id": row.id,
                    "name": row.name,
                    "overall_rating": float(row.overall_rating)
                    if row.overall_rating
                    else 3.0,
                    "total_reviews": int(row.total_reviews) if row.total_reviews else 0,
                    "departments": list(row.departments) if row.departments else [],
                    "courses_taught": list(row.courses_taught)
                    if row.courses_taught
                    else [],
                }
            )

        return professors

    except Exception as e:
        logger.error(f"Error in get_professors: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get(
    "/professor/find",
    responses={
        200: {
            "description": "Professor lookup result",
            "content": {
                "application/json": {
                    "example": {
                        "matches": [
                            {
                                "id": "abc123",
                                "name": "John Smith",
                                "rmpId": 456789,
                                "score": 95.5,
                            }
                        ]
                    }
                }
            },
        }
    },
    summary="/professor/find",
    description="Advanced fuzzy search for professors by name with intelligent handling of multi-part surnames and complex names. Uses PostgreSQL trigram similarity when available, falls back to token-based matching. Returns professors with RateMyProf IDs and relevance scores.",
)
async def find_professor(
    name: str = Query(..., description="Professor name to search (e.g. 'Smith, John')"),
    limit: int = 5,
    min_score: float = 20.0,
    db: Session = Depends(get_db_session),
):
    """Fuzzy lookup professor by name and return RateMyProf ID if available.

    Uses PostgreSQL similarity functions for fuzzy matching when available,
    falls back to token-based scoring for multi-part names.
    """
    import re

    from sqlalchemy.exc import ProgrammingError

    # Clean and normalize the input name
    name = name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name cannot be empty")

    # Split into tokens for fallback scoring
    tokens = [t.strip().lower() for t in re.split(r"[ ,]+", name) if t.strip()]

    try:
        # First, try using PostgreSQL's similarity function (requires pg_trgm extension)
        similarity_query = text("""
            WITH scored_professors AS (
                SELECT 
                    p.id,
                    p.first_name || ' ' || p.last_name AS name,
                    p.legacy_id AS rmp_id,
                    p.num_ratings,
                    (similarity(LOWER(p.first_name || ' ' || p.last_name), LOWER(:search_name)) * 100) AS score
                FROM professors p
                WHERE p.legacy_id IS NOT NULL
                  AND similarity(LOWER(p.first_name || ' ' || p.last_name), LOWER(:search_name)) > (:min_score / 100.0)
            )
            SELECT id, name, rmp_id, score
            FROM scored_professors
            ORDER BY score DESC, num_ratings DESC NULLS LAST
            LIMIT :limit
        """)

        result = db.execute(
            similarity_query,
            {"search_name": name, "min_score": min_score, "limit": limit},
        )

        matches = [
            {
                "id": row.id,
                "name": row.name,
                "rmpId": row.rmp_id,
                "score": round(float(row.score), 1),
            }
            for row in result
        ]

        return {"matches": matches}

    except ProgrammingError:
        # pg_trgm extension not available, fall back to token-based scoring
        pass

    # Fallback: Token-based fuzzy matching
    if not tokens:
        return {"matches": []}

    # Build scoring query based on token matches
    token_conditions = []
    params = {"limit": limit, "min_score": min_score, "total_tokens": len(tokens)}

    for idx, token in enumerate(tokens):
        token_key = f"token_{idx}"
        params[token_key] = f"%{token}%"
        token_conditions.append(
            f"CASE WHEN LOWER(p.first_name || ' ' || p.last_name) ILIKE :{token_key} THEN 1 ELSE 0 END"
        )

    token_sum = " + ".join(token_conditions)

    fallback_query = text(f"""
        WITH scored_professors AS (
            SELECT 
                p.id,
                p.first_name || ' ' || p.last_name AS name,
                p.legacy_id AS rmp_id,
                p.num_ratings,
                CASE 
                    WHEN LOWER(p.first_name || ' ' || p.last_name) = LOWER(:exact_name) THEN 100.0
                    ELSE (({token_sum}) * 100.0 / :total_tokens)
                END AS score
            FROM professors p
            WHERE p.legacy_id IS NOT NULL
        )
        SELECT id, name, rmp_id, score
        FROM scored_professors
        WHERE score >= :min_score
        ORDER BY score DESC, num_ratings DESC NULLS LAST
        LIMIT :limit
    """)

    params["exact_name"] = name

    result = db.execute(fallback_query, params)
    matches = [
        {
            "id": row.id,
            "name": row.name,
            "rmpId": row.rmp_id,
            "score": round(float(row.score), 1),
        }
        for row in result
    ]

    return {"matches": matches}


@app.get(
    "/professor/{professor_id}",
    responses={
        200: {
            "description": "👨‍🏫 Complete Professor Profile - Ratings, Reviews & Teaching Analytics",
            "content": {
                "application/json": {
                    "example": {
                        "id": "prof123",
                        "name": "Dr. Sarah Johnson",
                        "overall_rating": 4.2,
                        "total_reviews": 45,
                        "would_take_again_percent": 78.5,
                        "courses": [
                            {
                                "course_id": "CSCE121",
                                "course_name": "Introduction to Program Design and Concepts",
                                "reviews_count": 15,
                                "avg_rating": 4.1,
                            },
                            {
                                "course_id": "CSCE221",
                                "course_name": "Data Structures and Algorithms",
                                "reviews_count": 20,
                                "avg_rating": 4.3,
                            },
                            {
                                "course_id": "CSCE314",
                                "course_name": "Programming Languages",
                                "reviews_count": 10,
                                "avg_rating": 4.0,
                            },
                        ],
                        "departments": ["CSCE"],
                        "recent_reviews": [
                            {
                                "id": "review456",
                                "course_code": "CSCE121",
                                "course_name": "Introduction to Program Design and Concepts",
                                "review_text": "Great professor! Explains concepts clearly and is very helpful during office hours.",
                                "overall_rating": 4.5,
                                "would_take_again": True,
                                "grade": "A",
                                "review_date": "2024-05-15T00:00:00",
                            },
                            {
                                "id": "review789",
                                "course_code": "CSCE221",
                                "course_name": "Data Structures and Algorithms",
                                "review_text": "Challenging course but Dr. Johnson makes it engaging. Good use of examples.",
                                "overall_rating": 4.0,
                                "would_take_again": True,
                                "grade": "B+",
                                "review_date": "2024-04-22T00:00:00",
                            },
                        ],
                        "tag_frequencies": {
                            "Clear": 15,
                            "Helpful": 12,
                            "Fair": 8,
                            "Engaging": 6,
                        },
                        "overall_summary": "Dr. Johnson is consistently praised by students for her clear explanations and helpful teaching style. Students appreciate her availability during office hours and fair grading practices. Many students find her courses challenging but rewarding, with excellent preparation for advanced coursework.",
                    },
                    "schema": {
                        "type": "object",
                        "properties": {
                            "id": {
                                "type": "string",
                                "description": "Unique professor identifier",
                            },
                            "name": {
                                "type": "string",
                                "description": "Professor's full name with title",
                            },
                            "overall_rating": {
                                "type": "number",
                                "description": "Average rating across all courses (1-5 scale)",
                            },
                            "total_reviews": {
                                "type": "integer",
                                "description": "Total number of student reviews",
                            },
                            "would_take_again_percent": {
                                "type": "number",
                                "description": "Percentage of students who would take again",
                            },
                            "courses": {
                                "type": "array",
                                "description": "Courses taught by the professor",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "course_id": {
                                            "type": "string",
                                            "description": "Course identifier",
                                        },
                                        "course_name": {
                                            "type": "string",
                                            "description": "Full course name",
                                        },
                                        "reviews_count": {
                                            "type": "integer",
                                            "description": "Reviews for this course",
                                        },
                                        "avg_rating": {
                                            "type": "number",
                                            "description": "Average rating for this course",
                                        },
                                    },
                                },
                            },
                            "departments": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Department affiliations",
                            },
                            "recent_reviews": {
                                "type": "array",
                                "description": "Most recent student reviews",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "id": {
                                            "type": "string",
                                            "description": "Review identifier",
                                        },
                                        "course_code": {
                                            "type": "string",
                                            "description": "Course code",
                                        },
                                        "course_name": {
                                            "type": "string",
                                            "description": "Course name",
                                        },
                                        "review_text": {
                                            "type": "string",
                                            "description": "Student review text",
                                        },
                                        "overall_rating": {
                                            "type": "number",
                                            "description": "Review rating",
                                        },
                                        "would_take_again": {
                                            "type": "boolean",
                                            "description": "Would take again",
                                        },
                                        "grade": {
                                            "type": "string",
                                            "description": "Grade received",
                                        },
                                        "review_date": {
                                            "type": "string",
                                            "description": "Review date",
                                        },
                                    },
                                },
                            },
                            "tag_frequencies": {
                                "type": "object",
                                "description": "Most common student tags with frequencies",
                            },
                            "overall_summary": {
                                "type": "string",
                                "description": "AI-generated summary of all student reviews for this professor (null if no summary available)",
                            },
                        },
                    },
                }
            },
        },
        404: {
            "description": "❌ Professor Not Found - Invalid Professor ID",
            "content": {
                "application/json": {
                    "example": {"detail": "Professor not found"},
                    "schema": {
                        "type": "object",
                        "properties": {
                            "detail": {"type": "string", "description": "Error message"}
                        },
                    },
                }
            },
        },
    },
    summary="/professor/{professor_id}",
    description="Returns detailed professor profile with ratings, courses taught, recent reviews, tag frequencies, and overall summary.",
)
async def get_professor_profile(
    professor_id: str, db: Session = Depends(get_db_session)
):
    """
    Professor profile with comprehensive details

    Returns complete professor information including overall statistics,
    courses taught with individual ratings, and recent student reviews.

    **Path Parameters:**
    - `professor_id`: Professor identifier

    """
    try:
        # Get professor basic info
        professor_query = text("""
            SELECT 
                p.id,
                p.first_name || ' ' || p.last_name as name,
                p.first_name,
                p.last_name
            FROM professors p
            WHERE p.id = :professor_id
        """)

        professor_result = db.execute(
            professor_query, {"professor_id": professor_id}
        ).fetchone()

        if not professor_result:
            raise HTTPException(status_code=404, detail="Professor not found")

        # Get professor statistics and courses
        stats_query = text("""
            WITH professor_stats AS (
                SELECT 
                    ps.course_code,
                    ps.total_reviews,
                    ps.avg_difficulty,
                    c.course_title,
                    p.avg_rating
                FROM professor_summaries ps
                LEFT JOIN courses c ON c.subject_short_name || c.course_number = ps.course_code
                JOIN professors p ON ps.professor_id = p.id
                WHERE ps.professor_id = :professor_id
            )
            SELECT 
                COUNT(course_code) as total_courses,
                SUM(total_reviews) as total_reviews,
                AVG(avg_rating) as overall_rating,
                ARRAY_AGG(DISTINCT SUBSTRING(course_code FROM '^[A-Z]+')) as departments
            FROM professor_stats
        """)

        stats_result = db.execute(
            stats_query, {"professor_id": professor_id}
        ).fetchone()

        # Get would_take_again percentage from reviews
        would_take_again_query = text("""
            SELECT 
                ROUND(
                    AVG(CASE WHEN would_take_again = 1 THEN 100.0 ELSE 0.0 END)::numeric, 
                    1
                ) as would_take_again_percent
            FROM reviews 
            WHERE professor_id = :professor_id 
              AND would_take_again IS NOT NULL
        """)

        would_take_again_result = db.execute(
            would_take_again_query, {"professor_id": professor_id}
        ).fetchone()

        # Get courses taught by professor and overall tag frequencies
        courses_query = text("""
            SELECT 
                ps.course_code as course_id,
                COALESCE(c.course_title, 'Course Title') as course_name,
                ps.total_reviews as reviews_count,
                COALESCE(p.avg_rating, NULL) as avg_rating
            FROM professor_summaries ps
            LEFT JOIN courses c ON c.subject_short_name || c.course_number = ps.course_code
            LEFT JOIN professors p ON ps.professor_id = p.id
            WHERE ps.professor_id = :professor_id
            ORDER BY ps.total_reviews DESC
        """)

        courses_result = db.execute(courses_query, {"professor_id": professor_id})
        courses = []

        for course in courses_result:
            courses.append(
                {
                    "course_id": course.course_id,
                    "course_name": course.course_name,
                    "reviews_count": int(course.reviews_count)
                    if course.reviews_count
                    else 0,
                    "avg_rating": float(course.avg_rating)
                    if course.avg_rating
                    else 3.0,
                }
            )

        # Get tag frequencies for this professor (should be the same across all courses for a professor)
        tag_frequencies_query = text("""
            SELECT ps.tag_frequencies
            FROM professor_summaries ps
            WHERE ps.professor_id = :professor_id
              AND ps.tag_frequencies IS NOT NULL
              AND ps.tag_frequencies != ''
              AND ps.tag_frequencies != 'null'
            LIMIT 1
        """)

        tag_freq_result = db.execute(
            tag_frequencies_query, {"professor_id": professor_id}
        ).fetchone()
        overall_tag_frequencies = {}

        if tag_freq_result and tag_freq_result.tag_frequencies:
            overall_tag_frequencies = parse_tag_frequencies(
                tag_freq_result.tag_frequencies, professor_id
            )

        # Get overall summary if available
        overall_summary_query = text("""
            SELECT 
                ps.summary_text
            FROM professor_summaries ps
            WHERE ps.professor_id = :professor_id
              AND ps.summary_type = 'overall'
              AND ps.summary_text IS NOT NULL
              AND ps.summary_text != ''
            LIMIT 1
        """)

        overall_summary_result = db.execute(
            overall_summary_query, {"professor_id": professor_id}
        ).fetchone()

        overall_summary = None
        if overall_summary_result and overall_summary_result.summary_text:
            overall_summary = overall_summary_result.summary_text

        # Get recent reviews (last 5)
        recent_reviews_query = text("""
            SELECT 
                r.id,
                r.review_text,
                r.clarity_rating,
                r.difficulty_rating,
                r.helpful_rating,
                r.would_take_again,
                r.grade,
                r.review_date,
                r.course_code,
                r.rating_tags,
                COALESCE(c.course_title, 'Course') as course_name
            FROM reviews r
            LEFT JOIN courses c ON c.subject_short_name || c.course_number = r.course_code
            WHERE r.professor_id = :professor_id
              AND r.review_text IS NOT NULL
              AND r.review_text != ''
            ORDER BY r.review_date DESC
            LIMIT 5
        """)

        recent_reviews_result = db.execute(
            recent_reviews_query, {"professor_id": professor_id}
        )
        recent_reviews = []

        for review in recent_reviews_result:
            # Calculate overall rating from individual ratings
            overall_rating = (
                round(
                    (
                        (review.clarity_rating or 0)
                        + (6 - (review.difficulty_rating or 3))
                        + (review.helpful_rating or 0)
                    )
                    / 3,
                    1,
                )
                if any(
                    [
                        review.clarity_rating,
                        review.difficulty_rating,
                        review.helpful_rating,
                    ]
                )
                else 0
            )

            # Parse rating tags
            tags = []
            if review.rating_tags:
                try:
                    tags = (
                        json.loads(review.rating_tags)
                        if isinstance(review.rating_tags, str)
                        else review.rating_tags
                    )
                except:
                    tags = []

            recent_reviews.append(
                {
                    "id": review.id,
                    "course_code": review.course_code,
                    "course_name": review.course_name,
                    "review_text": review.review_text,
                    "overall_rating": overall_rating,
                    "would_take_again": review.would_take_again == 1
                    if review.would_take_again is not None
                    else None,
                    "grade": review.grade,
                    "review_date": review.review_date.isoformat()
                    if review.review_date
                    else None,
                    "tags": tags,
                }
            )

        # Build professor profile
        professor_profile = {
            "id": professor_result.id,
            "name": professor_result.name,
            "overall_rating": float(stats_result.overall_rating)
            if stats_result and stats_result.overall_rating
            else 3.0,
            "total_reviews": int(stats_result.total_reviews)
            if stats_result and stats_result.total_reviews
            else 0,
            "would_take_again_percent": float(
                would_take_again_result.would_take_again_percent
            )
            if would_take_again_result
            and would_take_again_result.would_take_again_percent
            else 0.0,
            "courses": courses,
            "departments": list(stats_result.departments)
            if stats_result and stats_result.departments and stats_result.departments[0]
            else [],
            "recent_reviews": recent_reviews,
            "tag_frequencies": overall_tag_frequencies,
            "overall_summary": overall_summary,
        }

        return professor_profile

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_professor_profile: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get(
    "/professor/{professor_id}/reviews",
    responses={
        200: {
            "description": "📝 Professor Reviews Collection - Detailed Student Feedback & Ratings",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "id": "review456",
                            "course_code": "CSCE121",
                            "course_name": "Introduction to Program Design and Concepts",
                            "department_name": "Computer Science & Engineering",
                            "review_text": "Great professor! Explains concepts clearly and is very helpful during office hours.",
                            "overall_rating": 4.3,
                            "clarity_rating": 4.5,
                            "difficulty_rating": 3.0,
                            "helpful_rating": 4.2,
                            "would_take_again": True,
                            "attendance_mandatory": False,
                            "is_online_class": False,
                            "is_for_credit": True,
                            "grade": "A",
                            "review_date": "2024-05-15T00: 00:00",
                            "textbook_use": "Required",
                            "thumbs_up": 12,
                            "thumbs_down": 3,
                            "tags": ["Helpful", "Clear", "Fair Grader"],
                            "teacher_note": None,
                        },
                        {
                            "id": "review789",
                            "course_code": "CSCE221",
                            "course_name": "Data Structures and Algorithms",
                            "department_name": "Computer Science & Engineering",
                            "review_text": "Challenging course but Dr. Johnson makes it engaging. Good use of examples.",
                            "overall_rating": 4.0,
                            "clarity_rating": 4.0,
                            "difficulty_rating": 4.0,
                            "helpful_rating": 3.8,
                            "would_take_again": True,
                            "attendance_mandatory": True,
                            "is_online_class": False,
                            "is_for_credit": True,
                            "grade": "B+",
                            "review_date": "2024-04-22T00:00:00",
                            "textbook_use": "Recommended",
                            "thumbs_up": 8,
                            "thumbs_down": 1,
                            "tags": ["Engaging", "Challenging"],
                            "teacher_note": None,
                        },
                    ],
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {
                                    "type": "string",
                                    "description": "Review identifier",
                                },
                                "course_code": {
                                    "type": "string",
                                    "description": "Course code reviewed",
                                },
                                "course_name": {
                                    "type": "string",
                                    "description": "Full course name",
                                },
                                "department_name": {
                                    "type": "string",
                                    "description": "Department name",
                                },
                                "review_text": {
                                    "type": "string",
                                    "description": "Student's written review",
                                },
                                "overall_rating": {
                                    "type": "number",
                                    "description": "Overall professor rating (1-5)",
                                },
                                "clarity_rating": {
                                    "type": "number",
                                    "description": "Teaching clarity rating",
                                },
                                "difficulty_rating": {
                                    "type": "number",
                                    "description": "Course difficulty rating",
                                },
                                "helpful_rating": {
                                    "type": "number",
                                    "description": "Helpfulness rating",
                                },
                                "would_take_again": {
                                    "type": "boolean",
                                    "description": "Would take again",
                                },
                                "attendance_mandatory": {
                                    "type": "boolean",
                                    "description": "Attendance required",
                                },
                                "is_online_class": {
                                    "type": "boolean",
                                    "description": "Online class format",
                                },
                                "is_for_credit": {
                                    "type": "boolean",
                                    "description": "Taken for credit",
                                },
                                "grade": {
                                    "type": "string",
                                    "description": "Grade received",
                                },
                                "review_date": {
                                    "type": "string",
                                    "description": "Review submission date",
                                },
                                "textbook_use": {
                                    "type": "string",
                                    "description": "Textbook requirement level",
                                },
                                "thumbs_up": {
                                    "type": "integer",
                                    "description": "Helpful votes",
                                },
                                "thumbs_down": {
                                    "type": "integer",
                                    "description": "Not helpful votes",
                                },
                                "tags": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Review tags",
                                },
                                "teacher_note": {
                                    "type": "string",
                                    "description": "Professor response",
                                },
                            },
                        },
                    },
                }
            },
        },
        404: {
            "description": "❌ Professor Not Found - Invalid Professor ID",
            "content": {
                "application/json": {
                    "example": {"detail": "Professor not found"},
                    "schema": {
                        "type": "object",
                        "properties": {
                            "detail": {"type": "string", "description": "Error message"}
                        },
                    },
                }
            },
        },
    },
    summary="/professor/{professor_id}/reviews",
    description="Returns all reviews for a professor with detailed ratings, review text, grades, and filtering options. Supports course filtering, sorting, and pagination.",
)
async def get_professor_reviews(
    professor_id: str,
    course_filter: Optional[str] = None,
    limit: int = 50,
    skip: int = 0,
    sort_by: str = "date",
    min_rating: Optional[float] = None,
    max_rating: Optional[float] = None,
    db: Session = Depends(get_db_session),
):
    """
    All reviews for professor across all courses

    Returns comprehensive list of student reviews for a professor,
    with advanced filtering and sorting options.

    **Path Parameters:**
    - `professor_id`: Professor identifier

    **Query Parameters:**
    - `course_filter`: Filter by specific course code (optional)
    - `limit`: Number of reviews to return (default: 50)
    - `skip`: Number of reviews to skip for pagination (default: 0)
    - `sort_by`: Sort order - "date", "rating", or "course" (default: "date")
    - `min_rating`: Minimum rating filter (1.0-5.0)
    - `max_rating`: Maximum rating filter (1.0-5.0)

    **Filter Examples:**
    - `/professor/prof123/reviews?course_filter=CSCE121` - Reviews for specific course
    - `/professor/prof123/reviews?min_rating=4.0` - Reviews with rating ≥ 4.0
    - `/professor/prof123/reviews?sort_by=rating&limit=10` - Top 10 highest-rated reviews
    """
    try:
        # Verify professor exists
        professor_check = db.execute(
            text("SELECT id FROM professors WHERE id = :professor_id"),
            {"professor_id": professor_id},
        ).fetchone()

        if not professor_check:
            raise HTTPException(status_code=404, detail="Professor not found")

        # Build WHERE conditions
        where_conditions = ["r.professor_id = :professor_id"]
        params = {"professor_id": professor_id, "limit": limit, "skip": skip}

        if course_filter:
            where_conditions.append("r.course_code = :course_filter")
            params["course_filter"] = course_filter.upper()

        if min_rating:
            where_conditions.append(
                "((r.clarity_rating + (6 - r.difficulty_rating) + r.helpful_rating) / 3.0) >= :min_rating"
            )
            params["min_rating"] = min_rating

        if max_rating:
            where_conditions.append(
                "((r.clarity_rating + (6 - r.difficulty_rating) + r.helpful_rating) / 3.0) <= :max_rating"
            )
            params["max_rating"] = max_rating

        where_clause = " AND ".join(where_conditions)

        # Determine sort order
        sort_order = "r.review_date DESC"
        if sort_by == "rating":
            sort_order = "((r.clarity_rating + (6 - r.difficulty_rating) + r.helpful_rating) / 3.0) DESC NULLS LAST"
        elif sort_by == "course":
            sort_order = "r.course_code ASC, r.review_date DESC"

        # Get reviews with course information
        reviews_query = text(f"""
            SELECT 
                r.id,
                r.review_text,
                r.clarity_rating,
                r.difficulty_rating,
                r.helpful_rating,
                r.would_take_again,
                r.attendance_mandatory,
                r.is_online_class,
                r.is_for_credit,
                r.grade,
                r.review_date,
                r.textbook_use,
                r.thumbs_up_total,
                r.thumbs_down_total,
                r.rating_tags,
                r.teacher_note,
                r.course_code,
                COALESCE(c.course_title, 'Course') as course_name,
                c.subject_long_name as department_name
            FROM reviews r
            LEFT JOIN courses c ON c.subject_short_name || c.course_number = r.course_code
            WHERE {where_clause}
              AND r.review_text IS NOT NULL
              AND r.review_text != ''
            ORDER BY {sort_order}
            LIMIT :limit OFFSET :skip
        """)

        result = db.execute(reviews_query, params)
        reviews = []

        for review in result:
            # Calculate overall rating from individual ratings
            overall_rating = (
                round(
                    (
                        (review.clarity_rating or 0)
                        + (6 - (review.difficulty_rating or 3))
                        + (review.helpful_rating or 0)
                    )
                    / 3,
                    1,
                )
                if any(
                    [
                        review.clarity_rating,
                        review.difficulty_rating,
                        review.helpful_rating,
                    ]
                )
                else 0
            )

            # Convert would_take_again to boolean
            would_take_again = (
                review.would_take_again == 1
                if review.would_take_again is not None
                else None
            )

            # Parse rating tags
            tags = []
            if review.rating_tags:
                try:
                    tags = (
                        json.loads(review.rating_tags)
                        if isinstance(review.rating_tags, str)
                        else review.rating_tags
                    )
                except:
                    tags = []

            review_data = {
                "id": review.id,
                "course_code": review.course_code,
                "course_name": review.course_name,
                "department_name": review.department_name,
                "review_text": review.review_text,
                "overall_rating": overall_rating,
                "clarity_rating": review.clarity_rating,
                "difficulty_rating": review.difficulty_rating,
                "helpful_rating": review.helpful_rating,
                "would_take_again": would_take_again,
                "attendance_mandatory": review.attendance_mandatory,
                "is_online_class": review.is_online_class,
                "is_for_credit": review.is_for_credit,
                "grade": review.grade,
                "review_date": review.review_date.isoformat()
                if review.review_date
                else None,
                "textbook_use": review.textbook_use,
                "thumbs_up": review.thumbs_up_total or 0,
                "thumbs_down": review.thumbs_down_total or 0,
                "tags": tags,
                "teacher_note": review.teacher_note,
            }

            reviews.append(review_data)

        return reviews

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_professor_reviews: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get(
    "/professors/search",
    responses={
        200: {
            "description": "🔍 Advanced Professor Search - Multi-Criteria Faculty Discovery",
            "content": {
                "application/json": {
                    "example": {
                        "professors": [
                            {
                                "id": "prof123",
                                "name": "Dr. Sarah Johnson",
                                "overall_rating": 4.2,
                                "total_reviews": 45,
                                "would_take_again_percent": 78.5,
                                "departments": ["CSCE"],
                                "courses_taught": ["CSCE121", "CSCE221", "CSCE314"],
                                "total_courses": 3,
                                "course_titles": "Introduction to Program Design and Concepts, Data Structures and Algorithms, Programming Languages",
                            },
                            {
                                "id": "prof456",
                                "name": "Dr. Michael Chen",
                                "overall_rating": 3.8,
                                "total_reviews": 32,
                                "would_take_again_percent": 65.0,
                                "departments": ["MATH"],
                                "courses_taught": ["MATH151", "MATH152"],
                                "total_courses": 2,
                                "course_titles": "Engineering Mathematics I, Engineering Mathematics II",
                            },
                        ],
                        "total_found": 2,
                        "search_criteria": {
                            "name": "johnson",
                            "department": None,
                            "min_rating": 3.5,
                            "courses_taught": None,
                        },
                    },
                    "schema": {
                        "type": "object",
                        "properties": {
                            "professors": {
                                "type": "array",
                                "description": "List of professors matching search criteria",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "id": {
                                            "type": "string",
                                            "description": "Professor identifier",
                                        },
                                        "name": {
                                            "type": "string",
                                            "description": "Professor name",
                                        },
                                        "overall_rating": {
                                            "type": "number",
                                            "description": "Average rating",
                                        },
                                        "total_reviews": {
                                            "type": "integer",
                                            "description": "Total reviews",
                                        },
                                        "would_take_again_percent": {
                                            "type": "number",
                                            "description": "Recommendation percentage",
                                        },
                                        "departments": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                            "description": "Department affiliations",
                                        },
                                        "courses_taught": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                            "description": "Course codes taught",
                                        },
                                        "total_courses": {
                                            "type": "integer",
                                            "description": "Number of courses taught",
                                        },
                                        "course_titles": {
                                            "type": "string",
                                            "description": "Comma-separated course titles",
                                        },
                                    },
                                },
                            },
                            "total_found": {
                                "type": "integer",
                                "description": "Total professors matching criteria",
                            },
                            "search_criteria": {
                                "type": "object",
                                "description": "Applied search filters",
                            },
                        },
                    },
                }
            },
        }
    },
    summary="/professors/search",
    description="Advanced professor search with multiple criteria including name, department, rating, and courses taught. Returns detailed professor profiles.",
)
async def search_professors(
    name: Optional[str] = None,
    department: Optional[str] = None,
    min_rating: Optional[float] = None,
    courses_taught: Optional[str] = None,
    limit: int = 30,
    skip: int = 0,
    db: Session = Depends(get_db_session),
):
    """
    Advanced professor search with multiple criteria

    Enables sophisticated search across professors using multiple filters
    including name, department, rating, and specific courses taught.

    **Query Parameters:**
    - `name`: Search by professor name (partial match)
    - `department`: Filter by department code (e.g., "CSCE", "MATH")
    - `min_rating`: Minimum rating filter (1.0-5.0)
    - `courses_taught`: Filter by specific course code
    - `limit`: Number of results to return (default: 30)
    - `skip`: Number of results to skip for pagination (default: 0)

    **Search Examples:**
    - `/professors/search?name=johnson` - Search for professors named Johnson
    - `/professors/search?department=CSCE&min_rating=4.0` - Top-rated CS professors
    - `/professors/search?courses_taught=MATH151` - Professors who teach Calculus I
    - `/professors/search?name=smith&department=ENGR&min_rating=3.8` - Combined criteria search
    """
    try:
        # Build WHERE conditions
        where_conditions = []
        params = {"limit": limit, "skip": skip}

        if name:
            where_conditions.append(
                "(p.first_name ILIKE :name OR p.last_name ILIKE :name OR (p.first_name || ' ' || p.last_name) ILIKE :name)"
            )
            params["name"] = f"%{name}%"

        if department:
            where_conditions.append(
                "EXISTS (SELECT 1 FROM professor_summaries ps WHERE ps.professor_id = p.id AND SUBSTRING(ps.course_code FROM '^[A-Z]+') = :department)"
            )
            params["department"] = department.upper()

        if courses_taught:
            where_conditions.append(
                "EXISTS (SELECT 1 FROM professor_summaries ps WHERE ps.professor_id = p.id AND ps.course_code = :courses_taught)"
            )
            params["courses_taught"] = courses_taught.upper()

        if min_rating:
            where_conditions.append("pst.overall_rating >= :min_rating")
            params["min_rating"] = min_rating

        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)

        # Advanced search query
        search_query = text(f"""
            WITH professor_stats AS (
                SELECT 
                    ps.professor_id,
                    COUNT(DISTINCT ps.course_code) as total_courses,
                    SUM(ps.total_reviews) as total_reviews,
                    p.avg_rating as overall_rating,
                    ARRAY_AGG(DISTINCT SUBSTRING(ps.course_code FROM '^[A-Z]+')) as departments,
                    ARRAY_AGG(DISTINCT ps.course_code) as courses_taught,
                    STRING_AGG(DISTINCT c.course_title, ', ') as course_titles
                FROM professor_summaries ps
                LEFT JOIN courses c ON c.subject_short_name || c.course_number = ps.course_code
                JOIN professors p ON ps.professor_id = p.id
                GROUP BY ps.professor_id, p.avg_rating
            ),
            would_take_again_stats AS (
                SELECT 
                    professor_id,
                    ROUND(
                        AVG(CASE WHEN would_take_again = 1 THEN 100.0 ELSE 0.0 END)::numeric, 
                        1
                    ) as would_take_again_percent
                FROM reviews 
                WHERE would_take_again IS NOT NULL
                GROUP BY professor_id
            )
            SELECT 
                p.id,
                p.first_name || ' ' || p.last_name as name,
                p.first_name,
                p.last_name,
                COALESCE(pst.overall_rating, NULL) as overall_rating,
                COALESCE(pst.total_reviews, 0) as total_reviews,
                COALESCE(wta.would_take_again_percent, 0.0) as would_take_again_percent,
                COALESCE(pst.departments, ARRAY[]::text[]) as departments,
                COALESCE(pst.courses_taught, ARRAY[]::text[]) as courses_taught,
                COALESCE(pst.course_titles, '') as course_titles,
                COALESCE(pst.total_courses, 0) as total_courses
            FROM professors p
            LEFT JOIN professor_stats pst ON p.id = pst.professor_id
            LEFT JOIN would_take_again_stats wta ON p.id = wta.professor_id
            {where_clause}
            ORDER BY pst.total_reviews DESC NULLS LAST, p.last_name, p.first_name
            LIMIT :limit OFFSET :skip
        """)

        result = db.execute(search_query, params)
        professors = []

        for row in result:
            professor_data = {
                "id": row.id,
                "name": row.name,
                "overall_rating": float(row.overall_rating)
                if row.overall_rating
                else 3.0,
                "total_reviews": int(row.total_reviews) if row.total_reviews else 0,
                "would_take_again_percent": float(row.would_take_again_percent)
                if row.would_take_again_percent
                else 0.0,
                "departments": list(row.departments) if row.departments else [],
                "courses_taught": list(row.courses_taught)
                if row.courses_taught
                else [],
                "total_courses": int(row.total_courses) if row.total_courses else 0,
            }

            # Add course titles if available
            if row.course_titles:
                professor_data["course_titles"] = row.course_titles

            professors.append(professor_data)

        return {
            "professors": professors,
            "total_found": len(professors),
            "search_criteria": {
                "name": name,
                "department": department,
                "min_rating": min_rating,
                "courses_taught": courses_taught,
            },
        }

    except Exception as e:
        logger.error(f"Error in search_professors: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get(
    "/professors/compare",
    responses={
        200: {
            "description": "👥 Professor Comparison Dashboard - Side-by-Side Faculty Analysis",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "id": "prof123",
                            "name": "Dr. Sarah Johnson",
                            "overall_rating": 4.2,
                            "total_reviews": 45,
                            "would_take_again_percent": 78.5,
                            "courses": [
                                {
                                    "course_id": "CSCE121",
                                    "course_name": "Introduction to Programming",
                                    "reviews_count": 15,
                                    "avg_rating": 4.1,
                                },
                                {
                                    "course_id": "CSCE221",
                                    "course_name": "Data Structures",
                                    "reviews_count": 20,
                                    "avg_rating": 4.3,
                                },
                            ],
                            "departments": ["CSCE"],
                            "recent_reviews": [
                                {
                                    "id": "review456",
                                    "course_code": "CSCE121",
                                    "course_name": "Introduction to Programming",
                                    "review_text": "Great professor! Explains concepts clearly and is very helpful during office hours.",
                                    "overall_rating": 4.5,
                                    "would_take_again": True,
                                    "grade": "A",
                                    "review_date": "2024-05-15T00:00:00",
                                }
                            ],
                            "tag_frequencies": {"Clear": 15, "Helpful": 12, "Fair": 8},
                        },
                        {
                            "id": "prof456",
                            "name": "Dr. Michael Chen",
                            "overall_rating": 3.8,
                            "total_reviews": 32,
                            "would_take_again_percent": 65.0,
                            "courses": [
                                {
                                    "course_id": "MATH151",
                                    "course_name": "Engineering Mathematics I",
                                    "reviews_count": 18,
                                    "avg_rating": 3.7,
                                },
                                {
                                    "course_id": "MATH152",
                                    "course_name": "Engineering Mathematics II",
                                    "reviews_count": 14,
                                    "avg_rating": 3.9,
                                },
                            ],
                            "departments": ["MATH"],
                            "recent_reviews": [
                                {
                                    "id": "review789",
                                    "course_code": "MATH151",
                                    "course_name": "Engineering Mathematics I",
                                    "review_text": "Challenging but fair. Homework helps prepare for exams.",
                                    "overall_rating": 4.0,
                                    "would_take_again": True,
                                    "grade": "B+",
                                    "review_date": "2024-04-22T00:00:00",
                                    "tags": ["Challenging", "Fair", "Helpful"],
                                }
                            ],
                            "tag_frequencies": {
                                "Challenging": 10,
                                "Fair": 8,
                                "Clear": 6,
                            },
                        },
                    ],
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {
                                    "type": "string",
                                    "description": "Professor identifier",
                                },
                                "name": {
                                    "type": "string",
                                    "description": "Professor full name",
                                },
                                "overall_rating": {
                                    "type": "number",
                                    "description": "Average rating across all courses",
                                },
                                "total_reviews": {
                                    "type": "integer",
                                    "description": "Total student reviews",
                                },
                                "would_take_again_percent": {
                                    "type": "number",
                                    "description": "Student recommendation rate",
                                },
                                "courses": {
                                    "type": "array",
                                    "description": "Teaching portfolio with performance metrics",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "course_id": {
                                                "type": "string",
                                                "description": "Course identifier",
                                            },
                                            "course_name": {
                                                "type": "string",
                                                "description": "Course title",
                                            },
                                            "reviews_count": {
                                                "type": "integer",
                                                "description": "Reviews for this course",
                                            },
                                            "avg_rating": {
                                                "type": "number",
                                                "description": "Average rating for this course",
                                            },
                                        },
                                    },
                                },
                                "departments": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Department affiliations",
                                },
                                "recent_reviews": {
                                    "type": "array",
                                    "description": "Latest student feedback",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "id": {
                                                "type": "string",
                                                "description": "Review ID",
                                            },
                                            "course_code": {
                                                "type": "string",
                                                "description": "Course code",
                                            },
                                            "course_name": {
                                                "type": "string",
                                                "description": "Course name",
                                            },
                                            "review_text": {
                                                "type": "string",
                                                "description": "Review content",
                                            },
                                            "overall_rating": {
                                                "type": "number",
                                                "description": "Review rating",
                                            },
                                            "would_take_again": {
                                                "type": "boolean",
                                                "description": "Recommendation",
                                            },
                                            "grade": {
                                                "type": "string",
                                                "description": "Grade received",
                                            },
                                            "review_date": {
                                                "type": "string",
                                                "description": "Review date",
                                            },
                                        },
                                    },
                                },
                                "tag_frequencies": {
                                    "type": "object",
                                    "description": "Common student feedback themes",
                                },
                            },
                        },
                    },
                }
            },
        },
        400: {
            "description": "❌ Bad Request - Invalid Professor Comparison Input",
            "content": {
                "application/json": {
                    "examples": {
                        "no_ids": {
                            "summary": "❌ No Professors Specified",
                            "description": "Request must include at least one professor ID",
                            "value": {"detail": "No professor IDs provided"},
                        },
                        "too_many_ids": {
                            "summary": "❌ Too Many Professors",
                            "description": "Maximum of 10 professors can be compared at once",
                            "value": {
                                "detail": "Too many professor IDs. Maximum 10 allowed."
                            },
                        },
                    },
                    "schema": {
                        "type": "object",
                        "properties": {
                            "detail": {
                                "type": "string",
                                "description": "Error description and resolution guidance",
                            }
                        },
                    },
                }
            },
        },
        404: {
            "description": "❌ Professors Not Found - Invalid Professor IDs",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "No valid professors found for the provided IDs"
                    },
                    "schema": {
                        "type": "object",
                        "properties": {
                            "detail": {
                                "type": "string",
                                "description": "Error message explaining missing professors",
                            }
                        },
                    },
                }
            },
        },
    },
    summary="/professors/compare",
    description="Compare up to 10 professors side-by-side with ratings, courses taught, reviews, and tag frequencies. Query format: ?ids=prof1,prof2,prof3",
)
async def compare_professors(
    ids: str = Query(
        ...,
        description="Comma-separated list of professor IDs (max 10)",
        example="prof123,prof456,prof789",
    ),
    db: Session = Depends(get_db_session),
):
    """
    Compare multiple professors by their IDs

    This endpoint allows you to fetch detailed information for multiple professors
    at once, enabling efficient comparison of professors across various metrics.

    **Features:**
    - Returns the same detailed structure as the individual professor endpoint
    - Supports up to 10 professors per request
    - Gracefully handles invalid professor IDs by skipping them
    - Includes professor ratings, courses taught, recent reviews, and statistics

    **Use Cases:**
    - Compare professors before course registration
    - Analyze teaching effectiveness across multiple instructors
    - Build professor comparison interfaces

    **Example Usage:**
    - `/professors/compare?ids=prof123,prof456`
    - `/professors/compare?ids=smith_j1,johnson_m2,davis_l3`
    """
    try:
        # Parse comma-separated professor IDs
        professor_ids = [pid.strip() for pid in ids.split(",") if pid.strip()]

        if not professor_ids:
            raise HTTPException(status_code=400, detail="No professor IDs provided")

        if len(professor_ids) > 10:  # Reasonable limit to prevent abuse
            raise HTTPException(
                status_code=400, detail="Too many professor IDs. Maximum 10 allowed."
            )

        professor_profiles = []

        # Process each professor ID
        for professor_id in professor_ids:
            try:
                # Get professor basic info
                professor_query = text("""
                    SELECT 
                        p.id,
                        p.first_name || ' ' || p.last_name as name,
                        p.first_name,
                        p.last_name
                    FROM professors p
                    WHERE p.id = :professor_id
                """)

                professor_result = db.execute(
                    professor_query, {"professor_id": professor_id}
                ).fetchone()

                if not professor_result:
                    # Skip professors that don't exist instead of failing the entire request
                    continue

                # Get professor statistics and courses
                stats_query = text("""
                    WITH professor_stats AS (
                        SELECT 
                            ps.course_code,
                            ps.total_reviews,
                            ps.avg_difficulty,
                            c.course_title,
                            p.avg_rating
                        FROM professor_summaries ps
                        LEFT JOIN courses c ON c.subject_short_name || c.course_number = ps.course_code
                        JOIN professors p ON ps.professor_id = p.id
                        WHERE ps.professor_id = :professor_id
                    )
                    SELECT 
                        COUNT(course_code) as total_courses,
                        SUM(total_reviews) as total_reviews,
                        AVG(avg_rating) as overall_rating,
                        ARRAY_AGG(DISTINCT SUBSTRING(course_code FROM '^[A-Z]+')) as departments
                    FROM professor_stats
                """)

                stats_result = db.execute(
                    stats_query, {"professor_id": professor_id}
                ).fetchone()

                # Get would_take_again percentage from reviews
                would_take_again_query = text("""
                    SELECT 
                        ROUND(
                            AVG(CASE WHEN would_take_again = 1 THEN 100.0 ELSE 0.0 END)::numeric, 
                            1
                        ) as would_take_again_percent
                    FROM reviews 
                    WHERE professor_id = :professor_id 
                      AND would_take_again IS NOT NULL
                """)

                would_take_again_result = db.execute(
                    would_take_again_query, {"professor_id": professor_id}
                ).fetchone()

                # Get courses taught by professor
                courses_query = text("""
                    SELECT 
                        ps.course_code as course_id,
                        COALESCE(c.course_title, 'Course Title') as course_name,
                        ps.total_reviews as reviews_count,
                        COALESCE(p.avg_rating, NULL) as avg_rating
                    FROM professor_summaries ps
                    LEFT JOIN courses c ON c.subject_short_name || c.course_number = ps.course_code
                    LEFT JOIN professors p ON ps.professor_id = p.id
                    WHERE ps.professor_id = :professor_id
                    ORDER BY ps.total_reviews DESC
                """)

                courses_result = db.execute(
                    courses_query, {"professor_id": professor_id}
                )
                courses = []

                for course in courses_result:
                    courses.append(
                        {
                            "course_id": course.course_id,
                            "course_name": course.course_name,
                            "reviews_count": int(course.reviews_count)
                            if course.reviews_count
                            else 0,
                            "avg_rating": float(course.avg_rating)
                            if course.avg_rating
                            else 3.0,
                        }
                    )

                # Get tag frequencies for this professor
                tag_frequencies_query = text("""
                    SELECT ps.tag_frequencies
                    FROM professor_summaries ps
                    WHERE ps.professor_id = :professor_id
                      AND ps.tag_frequencies IS NOT NULL
                    LIMIT 1
                """)

                tag_freq_result = db.execute(
                    tag_frequencies_query, {"professor_id": professor_id}
                ).fetchone()
                overall_tag_frequencies = {}

                if tag_freq_result and tag_freq_result.tag_frequencies:
                    overall_tag_frequencies = parse_tag_frequencies(
                        tag_freq_result.tag_frequencies, professor_id
                    )

                # Get recent reviews (last 5)
                recent_reviews_query = text("""
                    SELECT 
                        r.id,
                        r.review_text,
                        r.clarity_rating,
                        r.difficulty_rating,
                        r.helpful_rating,
                        r.would_take_again,
                        r.grade,
                        r.review_date,
                        r.course_code,
                        r.rating_tags,
                        COALESCE(c.course_title, 'Course') as course_name
                    FROM reviews r
                    LEFT JOIN courses c ON c.subject_short_name || c.course_number = r.course_code
                    WHERE r.professor_id = :professor_id
                      AND r.review_text IS NOT NULL
                      AND r.review_text != ''
                    ORDER BY r.review_date DESC
                    LIMIT 5
                """)

                recent_reviews_result = db.execute(
                    recent_reviews_query, {"professor_id": professor_id}
                )
                recent_reviews = []

                for review in recent_reviews_result:
                    # Calculate overall rating from individual ratings
                    overall_rating = (
                        round(
                            (
                                (review.clarity_rating or 0)
                                + (6 - (review.difficulty_rating or 3))
                                + (review.helpful_rating or 0)
                            )
                            / 3,
                            1,
                        )
                        if any(
                            [
                                review.clarity_rating,
                                review.difficulty_rating,
                                review.helpful_rating,
                            ]
                        )
                        else 0
                    )

                    # Parse rating tags
                    tags = []
                    if review.rating_tags:
                        try:
                            tags = (
                                json.loads(review.rating_tags)
                                if isinstance(review.rating_tags, str)
                                else review.rating_tags
                            )
                        except:
                            tags = []

                    recent_reviews.append(
                        {
                            "id": review.id,
                            "course_code": review.course_code,
                            "course_name": review.course_name,
                            "review_text": review.review_text,
                            "overall_rating": overall_rating,
                            "would_take_again": review.would_take_again == 1
                            if review.would_take_again is not None
                            else None,
                            "grade": review.grade,
                            "review_date": review.review_date.isoformat()
                            if review.review_date
                            else None,
                            "tags": tags,
                        }
                    )

                # Build professor profile
                professor_profile = {
                    "id": professor_result.id,
                    "name": professor_result.name,
                    "overall_rating": float(stats_result.overall_rating)
                    if stats_result and stats_result.overall_rating
                    else 3.0,
                    "total_reviews": int(stats_result.total_reviews)
                    if stats_result and stats_result.total_reviews
                    else 0,
                    "would_take_again_percent": float(
                        would_take_again_result.would_take_again_percent
                    )
                    if would_take_again_result
                    and would_take_again_result.would_take_again_percent
                    else 0.0,
                    "courses": courses,
                    "departments": list(stats_result.departments)
                    if stats_result
                    and stats_result.departments
                    and stats_result.departments[0]
                    else [],
                    "recent_reviews": recent_reviews,
                    "tag_frequencies": overall_tag_frequencies,
                }

                professor_profiles.append(professor_profile)

            except Exception as e:
                logger.warning(f"Error processing professor {professor_id}: {str(e)}")
                # Continue processing other professors instead of failing the entire request
                continue

        if not professor_profiles:
            raise HTTPException(
                status_code=404, detail="No valid professors found for the provided IDs"
            )

        return professor_profiles

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in compare_professors: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)
