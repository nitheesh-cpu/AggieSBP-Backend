from datetime import datetime
import os
from sqlalchemy import (
    ARRAY,
    DateTime,
    create_engine,
    Column,
    String,
    Integer,
    Float,
    Boolean,
    select,
    text,
    ForeignKey,
    update,
    Text,
)
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Session as SQLAlchemySession
from sqlalchemy.dialects.postgresql import insert, JSON
from typing import List, Any, Dict
import logging

from ..models.schema import Review, University, Professor
from dotenv import load_dotenv

load_dotenv()

# Configure logging for database performance monitoring
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class UniversityDB(Base):
    __tablename__ = "universities"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    legacy_school_id = Column(Integer, nullable=True)
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)

    def __repr__(self) -> str:
        return f"<University(id='{self.id}', name='{self.name}')>"


class ProfessorDB(Base):
    __tablename__ = "professors"

    id = Column(String, primary_key=True)
    university_id = Column(String, ForeignKey("universities.id"), nullable=False)
    legacy_id = Column(Integer, nullable=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    department = Column(String, nullable=True)
    avg_rating = Column(Float, nullable=True)
    avg_difficulty = Column(Float, nullable=True)
    num_ratings = Column(Integer, nullable=False, default=0)
    would_take_again_percent = Column(Float, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self) -> str:
        return f"<Professor(id='{self.id}', name='{self.first_name} {self.last_name}')>"


class ReviewDB(Base):
    __tablename__ = "reviews"

    id = Column(String, primary_key=True)
    legacy_id = Column(Integer, nullable=True)
    professor_id = Column(String, ForeignKey("professors.id"), nullable=False)
    course_code = Column(String, nullable=True)
    clarity_rating = Column(Float, nullable=True)
    difficulty_rating = Column(Float, nullable=True)
    helpful_rating = Column(Float, nullable=True)
    would_take_again = Column(Integer, nullable=True)
    attendance_mandatory = Column(String, nullable=True)
    is_online_class = Column(Boolean, nullable=False)
    is_for_credit = Column(Boolean, nullable=False)
    review_text = Column(String, nullable=True)
    grade = Column(String, nullable=True)
    review_date = Column(DateTime, nullable=True)
    textbook_use = Column(Integer, nullable=True)
    thumbs_up_total = Column(Integer, nullable=True)
    thumbs_down_total = Column(Integer, nullable=True)
    rating_tags: Column[List[str]] = Column(ARRAY(String), nullable=True)
    admin_reviewed_at = Column(DateTime, nullable=True)
    flag_status = Column(String, nullable=True)
    created_by_user = Column(Boolean, nullable=False)
    teacher_note = Column(String, nullable=True)

    def __repr__(self) -> str:
        return f"<Review(id='{self.id}', professor_id='{self.professor_id}', course_code='{self.course_code}')>"


class DepartmentDB(Base):
    __tablename__ = "departments"

    id = Column(String, primary_key=True)
    short_name = Column(String, nullable=False)
    long_name = Column(String, nullable=False)
    title = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self) -> str:
        return f"<Department(id='{self.id}', short_name='{self.short_name}', long_name='{self.long_name}', title='{self.title}')>"


class CourseDB(Base):
    __tablename__ = "courses"

    id = Column(String, primary_key=True)
    subject_long_name = Column(String, nullable=False)
    subject_short_name = Column(String, nullable=False)
    subject_id = Column(String, ForeignKey("departments.id"), nullable=False)
    course_number = Column(String, nullable=False)
    course_topic = Column(String, nullable=True)
    course_display_title = Column(String, nullable=False)
    course_title = Column(String, nullable=False)
    course_title_long = Column(String, nullable=False)
    description = Column(String, nullable=True)
    has_topics = Column(Boolean, nullable=False)
    has_corequisites = Column(Boolean, nullable=True)
    has_prerequisites = Column(Boolean, nullable=True)
    has_restrictions = Column(Boolean, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)


class GpaDataDB(Base):
    __tablename__ = "gpa_data"

    id = Column(String, primary_key=True)
    dept = Column(String, nullable=False)
    course_number = Column(String, nullable=False)
    section = Column(String, nullable=False)
    professor = Column(String, nullable=False)
    year = Column(String, nullable=False)
    semester = Column(String, nullable=False)
    gpa = Column(Float, nullable=True)
    grade_a = Column(Integer, nullable=False, default=0)
    grade_b = Column(Integer, nullable=False, default=0)
    grade_c = Column(Integer, nullable=False, default=0)
    grade_d = Column(Integer, nullable=False, default=0)
    grade_f = Column(Integer, nullable=False, default=0)
    grade_i = Column(Integer, nullable=False, default=0)
    grade_s = Column(Integer, nullable=False, default=0)
    grade_u = Column(Integer, nullable=False, default=0)
    grade_q = Column(Integer, nullable=False, default=0)
    grade_x = Column(Integer, nullable=False, default=0)
    total_students = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self) -> str:
        return f"<GpaData(id='{self.id}', course='{self.dept} {self.course_number}', prof='{self.professor}', gpa='{self.gpa}')>"


class SectionAttributeDB(Base):
    __tablename__ = "section_attributes"
    id = Column(String, primary_key=True)  # e.g. 'CSCE_121_500_Fall2025_KCOM'
    dept = Column(String, nullable=False)
    course_number = Column(String, nullable=False)
    section = Column(String, nullable=False)
    year = Column(String, nullable=False)
    semester = Column(String, nullable=False)
    attribute_id = Column(String, nullable=False)  # e.g. 'KCOM'
    attribute_title = Column(String, nullable=True)
    attribute_value = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self) -> str:
        return f"<SectionAttribute(id='{self.id}', course='{self.dept} {self.course_number}', section='{self.section}', attr='{self.attribute_id}')>"


class TermDB(Base):
    """Database model for academic terms from Howdy API"""

    __tablename__ = "terms"

    term_code = Column(String, primary_key=True)  # e.g., "202611"
    term_desc = Column(String, nullable=False)  # e.g., "Spring 2026 - College Station"
    start_date = Column(DateTime, nullable=True)  # STVTERM_START_DATE
    end_date = Column(DateTime, nullable=True)  # STVTERM_END_DATE
    academic_year = Column(String, nullable=True)  # STVTERM_ACYR_CODE (e.g., "2025")
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self) -> str:
        return f"<Term(code='{self.term_code}', desc='{self.term_desc}')>"


class SectionDB(Base):
    """Database model for course sections from Howdy API"""

    __tablename__ = "sections"

    id = Column(String, primary_key=True)  # e.g., "202611_56508" (term_code + CRN)
    term_code = Column(String, nullable=False, index=True)  # e.g., "202611"
    crn = Column(String, nullable=False)  # Course Reference Number
    dept = Column(String, nullable=False, index=True)  # Department code (e.g., "CSCE")
    dept_desc = Column(
        String, nullable=True
    )  # Department description (e.g., "CSCE - Computer Sci & Engr")
    course_number = Column(String, nullable=False)  # Course number (e.g., "221")
    section_number = Column(String, nullable=False)  # Section number (e.g., "501")
    course_title = Column(String, nullable=True)  # SWV_CLASS_SEARCH_TITLE

    # Credit hours
    credit_hours = Column(
        String, nullable=True
    )  # HRS_COLUMN_FIELD - displayed hours (always populated)
    hours_low = Column(
        Integer, nullable=True
    )  # SWV_CLASS_SEARCH_HOURS_LOW (always populated)
    hours_high = Column(
        Integer, nullable=True
    )  # SWV_CLASS_SEARCH_HOURS_HIGH (rarely populated)

    # Section info
    campus = Column(String, nullable=True)  # SWV_CLASS_SEARCH_SITE (93% populated)
    part_of_term = Column(
        String, nullable=True
    )  # SWV_CLASS_SEARCH_PTRM (always populated)
    session_type = Column(
        String, nullable=True
    )  # SWV_CLASS_SEARCH_SESSION (always populated)
    schedule_type = Column(
        String, nullable=True
    )  # SWV_CLASS_SEARCH_SCHD (LEC, LAB, SEM - always populated)
    instruction_type = Column(
        String, nullable=True
    )  # SWV_CLASS_SEARCH_INST_TYPE (always populated)

    # Availability
    is_open = Column(
        Boolean, nullable=False, default=False
    )  # STUSEAT_OPEN == "Y" (always populated)

    # Syllabus
    has_syllabus = Column(
        Boolean, nullable=False, default=False
    )  # SWV_CLASS_SEARCH_HAS_SYL_IND == "Y"
    syllabus_url = Column(Text, nullable=True)  # Constructed from CRN and term

    # Attributes
    attributes_text = Column(
        Text, nullable=True
    )  # SWV_CLASS_SEARCH_ATTRIBUTES (pipe-delimited)

    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self) -> str:
        return f"<Section(id='{self.id}', course='{self.dept} {self.course_number}', section='{self.section_number}', open={self.is_open})>"


class SectionInstructorDB(Base):
    """Database model for section-to-instructor mapping"""

    __tablename__ = "section_instructors"

    id = Column(
        String, primary_key=True
    )  # e.g., "202611_56508_584135" (term_code + CRN + PIDM)
    section_id = Column(String, ForeignKey("sections.id"), nullable=False, index=True)
    term_code = Column(String, nullable=False)
    crn = Column(String, nullable=False)
    instructor_name = Column(
        String, nullable=False, index=True
    )  # Parsed from NAME (remove "(P)" suffix)
    instructor_pidm = Column(Integer, nullable=True)  # MORE field
    has_cv = Column(Boolean, nullable=True)  # HAS_CV == "Y"
    cv_url = Column(Text, nullable=True)  # Constructed URL
    is_primary = Column(Boolean, default=False)  # First instructor in list
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self) -> str:
        return f"<SectionInstructor(id='{self.id}', instructor='{self.instructor_name}', primary={self.is_primary})>"


class SectionMeetingDB(Base):
    """Database model for section meeting times and locations"""

    __tablename__ = "section_meetings"

    id = Column(
        String, primary_key=True
    )  # e.g., "202611_56508_0" (term_code + CRN + meeting_index)
    section_id = Column(String, ForeignKey("sections.id"), nullable=False, index=True)
    term_code = Column(String, nullable=False)
    crn = Column(String, nullable=False)
    meeting_index = Column(Integer, nullable=False)  # Order in the array
    credit_hours_session = Column(Integer, nullable=True)  # SSRMEET_CREDIT_HR_SESS
    days_of_week: Column[List[str]] = Column(
        ARRAY(String), nullable=True
    )  # Array of day codes (M, T, W, R, F, S, U)
    begin_time = Column(String, nullable=True)  # SSRMEET_BEGIN_TIME
    end_time = Column(String, nullable=True)  # SSRMEET_END_TIME
    start_date = Column(String, nullable=True)  # SSRMEET_START_DATE
    end_date = Column(String, nullable=True)  # SSRMEET_END_DATE
    building_code = Column(String, nullable=True)  # SSRMEET_BLDG_CODE
    room_code = Column(String, nullable=True)  # SSRMEET_ROOM_CODE
    meeting_type = Column(
        String, nullable=True
    )  # SSRMEET_MTYP_CODE (Lecture, Examination)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self) -> str:
        return f"<SectionMeeting(id='{self.id}', type='{self.meeting_type}', days={self.days_of_week})>"


class SectionAttributeDetailedDB(Base):
    """Database model for detailed section attributes with descriptions"""

    __tablename__ = "section_attributes_detailed"

    id = Column(String, primary_key=True)  # section_id + "_" + attr_code
    section_id = Column(String, ForeignKey("sections.id"), nullable=False, index=True)
    term_code = Column(String, nullable=False)
    crn = Column(String, nullable=False)
    attribute_code = Column(
        String, nullable=False, index=True
    )  # SSRATTR_ATTR_CODE (e.g., "DIST")
    attribute_desc = Column(
        String, nullable=True
    )  # STVATTR_DESC (e.g., "Distance Education")
    created_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self) -> str:
        return f"<SectionAttributeDetailed(section='{self.section_id}', code='{self.attribute_code}', desc='{self.attribute_desc}')>"


class SectionPrereqDB(Base):
    """Database model for section prerequisites"""

    __tablename__ = "section_prereqs"

    id = Column(String, primary_key=True)  # section_id
    section_id = Column(
        String, ForeignKey("sections.id"), nullable=False, index=True, unique=True
    )
    term_code = Column(String, nullable=False)
    crn = Column(String, nullable=False)
    prereqs_text = Column(Text, nullable=True)  # P_PRE_REQS_OUT (human-readable)
    prereqs_json = Column(JSON, nullable=True)  # Full structured data if available
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self) -> str:
        return f"<SectionPrereq(section='{self.section_id}', prereqs='{self.prereqs_text[:50] if self.prereqs_text else None}...')>"


class SectionRestrictionDB(Base):
    """Database model for section restrictions (all types in one table)"""

    __tablename__ = "section_restrictions"

    id = Column(
        String, primary_key=True
    )  # section_id + "_" + restriction_type + "_" + index
    section_id = Column(String, ForeignKey("sections.id"), nullable=False, index=True)
    term_code = Column(String, nullable=False)
    crn = Column(String, nullable=False)
    restriction_type = Column(
        String, nullable=False, index=True
    )  # 'program', 'college', 'level', etc.
    restriction_code = Column(String, nullable=True)  # The code value
    restriction_desc = Column(String, nullable=True)  # Description
    include_exclude = Column(String, nullable=True)  # 'I' for include, 'E' for exclude
    created_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self) -> str:
        return f"<SectionRestriction(section='{self.section_id}', type='{self.restriction_type}', code='{self.restriction_code}')>"


class SectionBookstoreLinkDB(Base):
    """Database model for section bookstore/textbook links"""

    __tablename__ = "section_bookstore_links"

    id = Column(String, primary_key=True)  # section_id
    section_id = Column(
        String, ForeignKey("sections.id"), nullable=False, index=True, unique=True
    )
    term_code = Column(String, nullable=False)
    crn = Column(String, nullable=False)
    bookstore_url = Column(Text, nullable=True)
    link_data = Column(JSON, nullable=True)  # Full response data
    created_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self) -> str:
        return f"<SectionBookstoreLink(section='{self.section_id}', url='{self.bookstore_url}')>"


class ProfessorSummaryNewDB(Base):
    """Database model for new hierarchical professor summaries (separate rows per course)"""

    __tablename__ = "professor_summaries_new"

    id = Column(
        String, primary_key=True
    )  # UUID or hash: professor_id + course_code (or just professor_id for overall)
    professor_id = Column(
        String, ForeignKey("professors.id"), nullable=False, index=True
    )
    course_code = Column(
        String, nullable=True, index=True
    )  # NULL for overall summary, course code for course-specific

    # Overall summary fields (populated when course_code is NULL)
    overall_sentiment = Column(String, nullable=True)
    strengths: Column[List[str]] = Column(ARRAY(Text), nullable=True)  # List of strings
    complaints: Column[List[str]] = Column(
        ARRAY(Text), nullable=True
    )  # List of strings
    consistency = Column(String, nullable=True)

    # Course-specific summary fields (populated when course_code is NOT NULL)
    teaching = Column(Text, nullable=True)
    exams = Column(Text, nullable=True)
    grading = Column(Text, nullable=True)
    workload = Column(Text, nullable=True)
    personality = Column(Text, nullable=True)
    policies = Column(Text, nullable=True)
    other = Column(Text, nullable=True)

    # Common fields
    confidence = Column(Float, nullable=False)
    total_reviews = Column(Integer, nullable=False, default=0)

    # New statistics fields
    avg_rating = Column(Float, nullable=True)
    avg_difficulty = Column(Float, nullable=True)
    common_tags: Column[List[str]] = Column(ARRAY(String), nullable=True)
    tag_frequencies = Column(JSON, nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self) -> str:
        if self.course_code:
            return f"<ProfessorSummaryNew(professor_id='{self.professor_id}', course='{self.course_code}', confidence={self.confidence})>"
        else:
            return f"<ProfessorSummaryNew(professor_id='{self.professor_id}', overall, confidence={self.confidence})>"


# Global engine instance for connection pooling
_engine = None
_session_factory = None


def create_db_engine() -> Any:
    """Create database engine with connection pooling for better performance"""
    global _engine

    if _engine is not None:
        return _engine

    url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(
        os.getenv("POSTGRES_USER"),
        os.getenv("POSTGRES_PASSWORD"),
        os.getenv("POSTGRES_HOST"),
        os.getenv("POSTGRES_PORT"),
        os.getenv("POSTGRES_DATABASE"),
    )

    # Log connection for debugging
    logger.info("Creating database engine with connection pooling")

    # Create engine with connection pooling configuration
    _engine = create_engine(
        url,
        # Connection pool settings for better performance
        pool_size=10,  # Number of persistent connections to maintain
        max_overflow=20,  # Additional connections when pool is full
        pool_timeout=30,  # Seconds to wait for connection from pool
        pool_recycle=3600,  # Recycle connections after 1 hour
        pool_pre_ping=True,  # Validate connections before use
        # Performance optimizations
        echo=False,  # Set to True for SQL query logging (debug only)
        future=True,  # Use SQLAlchemy 2.0 style
        # Connection arguments for PostgreSQL optimization
        connect_args={
            "application_name": "aggiermp_api",
            "connect_timeout": 10,
        },
    )

    # Create tables if they don't exist
    Base.metadata.create_all(_engine)

    logger.info("Database engine created with pool_size=10, max_overflow=20")
    return _engine


def get_session_factory() -> sessionmaker:
    """Get session factory with connection pooling"""
    global _session_factory

    if _session_factory is not None:
        return _session_factory

    engine = create_db_engine()
    _session_factory = sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,  # Keep objects accessible after commit
    )

    return _session_factory


def get_session() -> Any:
    """Get database session from connection pool"""
    SessionFactory = get_session_factory()
    return SessionFactory()


# Performance monitoring decorator
def monitor_db_performance(func: Any) -> Any:
    """Decorator to monitor database query performance"""
    import time
    import functools

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            if execution_time > 1.0:  # Log slow queries (>1 second)
                logger.warning(f"Slow query in {func.__name__}: {execution_time:.2f}s")
            else:
                logger.debug(f"Query {func.__name__}: {execution_time:.3f}s")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(
                f"Query error in {func.__name__} after {execution_time:.3f}s: {str(e)}"
            )
            raise

    return wrapper


# Database health check function
def check_database_health() -> Dict[str, Any]:
    """Check database connection health and pool status"""
    try:
        engine = create_db_engine()

        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()

        # Get pool status (handle different pool types)
        pool = engine.pool
        try:
            pool_status = {
                "pool_size": pool.size(),
                "checked_in": pool.checkedin(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
                "pool_type": str(type(pool).__name__),
            }
        except AttributeError:
            # Fallback for different pool implementations
            pool_status = {"pool_type": str(type(pool).__name__), "status": "active"}

        logger.info(f"Database health check passed. Pool status: {pool_status}")
        return {"status": "healthy", "pool_status": pool_status}

    except Exception as e:
        logger.error(f"Database health check failed: {str(e)}")
        return {"status": "unhealthy", "error": str(e)}


# Configuration for local vs remote database
def get_database_config() -> Dict[str, int]:
    """Get database configuration with environment-specific optimizations"""
    host = os.getenv("POSTGRES_HOST", "localhost")
    is_local = host in ["localhost", "127.0.0.1", "::1"]

    if is_local:
        # Local database optimizations
        return {
            "pool_size": 5,  # Fewer connections needed locally
            "max_overflow": 10,  # Less overflow needed
            "pool_timeout": 10,  # Faster timeout for local connections
            "pool_recycle": 7200,  # Longer recycle time for stable local connections
        }
    else:
        # Remote database optimizations
        return {
            "pool_size": 10,  # More connections for remote database
            "max_overflow": 20,  # More overflow for network latency
            "pool_timeout": 30,  # Longer timeout for network delays
            "pool_recycle": 3600,  # Shorter recycle for remote connections
        }


def upsert_universities(
    session: SQLAlchemySession, universities: List[University]
) -> List[dict]:
    """
    Upsert universities into the database.
    Insert new records and update existing ones based on ID.
    """
    query_all = session.execute(select(UniversityDB))
    all_db_universities = query_all.scalars().all()
    insert_universities: list[dict] = []
    update_universities: list[dict] = []

    for university in universities:
        db_university_obj = next(
            (x for x in all_db_universities if x.id == university.id),
            None,
        )

        if db_university_obj:
            if (
                db_university_obj.name != university.name
                or db_university_obj.legacy_school_id != university.legacy_school_id
                or db_university_obj.city != university.city
                or db_university_obj.state != university.state
            ):
                update_universities.append(
                    {
                        "id": university.id,
                        "name": university.name,
                        "legacy_school_id": university.legacy_school_id,
                        "city": university.city,
                        "state": university.state,
                        "updated_at": datetime.now(),
                    }
                )
        else:
            insert_universities.append(
                {
                    "id": university.id,
                    "name": university.name,
                    "legacy_school_id": university.legacy_school_id,
                    "city": university.city,
                    "state": university.state,
                }
            )

    print("Number of universities to insert: ", len(insert_universities))
    print("Number of universities to update: ", len(update_universities))

    if insert_universities:
        session.execute(insert(UniversityDB), insert_universities)
    if update_universities:
        session.execute(update(UniversityDB), update_universities)

    session.commit()
    return insert_universities + update_universities


def upsert_professors(
    session: SQLAlchemySession, professors: List[Professor]
) -> List[dict]:
    """
    Upsert professors into the database.
    Insert new records and update existing ones based on ID.
    """
    # Ensure transaction is clean
    try:
        if session.in_transaction():
            session.execute(text("SELECT 1"))
    except Exception:
        session.rollback()

    try:
        query_all = session.execute(select(ProfessorDB))
        all_db_profs = query_all.scalars().all()
        insert_professors: list[dict] = []
        update_professors: list[dict] = []

        for professor in professors:
            # Generate UUID if no ID provided
            if not professor.id:
                raise ValueError("Professor ID is required")

            db_prof_obj = next(
                (x for x in all_db_profs if x.id == professor.id),
                None,
            )

            if db_prof_obj:
                # if the new professor has more ratings, update the existing professor
                if professor.num_ratings > db_prof_obj.num_ratings:
                    update_professors.append(
                        {
                            "id": professor.id,
                            "avg_rating": professor.avg_rating,
                            "avg_difficulty": professor.avg_difficulty,
                            "num_ratings": professor.num_ratings,
                            "would_take_again_percent": professor.would_take_again_percent,
                            "updated_at": datetime.now(),
                        }
                    )
            else:
                insert_professors.append(
                    {
                        "id": professor.id,
                        "university_id": professor.university_id,
                        "legacy_id": professor.legacy_id,
                        "first_name": professor.first_name,
                        "last_name": professor.last_name,
                        "department": professor.department,
                        "avg_rating": professor.avg_rating,
                        "avg_difficulty": professor.avg_difficulty,
                        "num_ratings": professor.num_ratings,
                        "would_take_again_percent": professor.would_take_again_percent,
                        "created_at": datetime.now(),
                        "updated_at": datetime.now(),
                    }
                )

        print("Number of professors to insert: ", len(insert_professors))
        print("Number of professors to update: ", len(update_professors))

        if insert_professors:
            session.execute(insert(ProfessorDB), insert_professors)
        if update_professors:
            session.execute(update(ProfessorDB), update_professors)

        session.commit()
        return insert_professors + update_professors
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass
        raise


def upsert_reviews(session: SQLAlchemySession, reviews: List[Review]) -> List[dict]:
    """
    Upsert reviews into the database.
    Insert new records and update existing ones based on ID.
    """
    # Ensure transaction is clean
    try:
        if session.in_transaction():
            session.execute(text("SELECT 1"))
    except Exception:
        session.rollback()

    insert_reviews: list[dict] = []

    for review in reviews:
        insert_reviews.append(
            {
                **review.model_dump(),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
        )

    try:
        if insert_reviews:
            session.execute(insert(ReviewDB), insert_reviews)
            session.commit()
        return insert_reviews
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass
        raise
