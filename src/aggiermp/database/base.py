from datetime import datetime
import os
from sqlalchemy import ARRAY, DateTime, create_engine, Column, String, Integer, Float, Boolean, select, text, ForeignKey, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from pydantic import BaseModel, Field
from typing import Optional, List
import uuid
from sqlalchemy import URL
import logging

from ..models.schema import Review, University, Professor
from dotenv import load_dotenv
load_dotenv()

# Configure logging for database performance monitoring
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()

class UniversityDB(Base):
    __tablename__ = 'universities'
    
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    legacy_school_id = Column(Integer, nullable=True)
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)

    def __repr__(self):
        return f"<University(id='{self.id}', name='{self.name}')>"

class ProfessorDB(Base):
    __tablename__ = 'professors'
    
    id = Column(String, primary_key=True)
    university_id = Column(String, ForeignKey('universities.id'), nullable=False)
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

    def __repr__(self):
        return f"<Professor(id='{self.id}', name='{self.first_name} {self.last_name}')>"
    
class ReviewDB(Base):
    __tablename__ = 'reviews'
    
    id = Column(String, primary_key=True)
    legacy_id = Column(Integer, nullable=True)
    professor_id = Column(String, ForeignKey('professors.id'), nullable=False)
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
    rating_tags = Column(ARRAY(String), nullable=True)
    admin_reviewed_at = Column(DateTime, nullable=True)
    flag_status = Column(String, nullable=True)
    created_by_user = Column(Boolean, nullable=False)
    teacher_note = Column(String, nullable=True)
    
    def __repr__(self):
        return f"<Review(id='{self.id}', professor_id='{self.professor_id}', course_code='{self.course_code}')>"
    
class DepartmentDB(Base):
    __tablename__ = 'departments'
    
    id = Column(String, primary_key=True)
    short_name = Column(String, nullable=False)
    long_name = Column(String, nullable=False)
    title = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)
    
    def __repr__(self):
        return f"<Department(id='{self.id}', short_name='{self.short_name}', long_name='{self.long_name}', title='{self.title}')>"

class CourseDB(Base):
    __tablename__ = 'courses'
    
    id = Column(String, primary_key=True)
    subject_long_name = Column(String, nullable=False)
    subject_short_name = Column(String, nullable=False)
    subject_id = Column(String, ForeignKey('departments.id'), nullable=False)
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
    __tablename__ = 'gpa_data'
    
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
    
    def __repr__(self):
        return f"<GpaData(id='{self.id}', course='{self.dept} {self.course_number}', prof='{self.professor}', gpa='{self.gpa}')>"

class SectionAttributeDB(Base):
    __tablename__ = 'section_attributes'
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

    def __repr__(self):
        return f"<SectionAttribute(id='{self.id}', course='{self.dept} {self.course_number}', section='{self.section}', attr='{self.attribute_id}')>"

# Global engine instance for connection pooling
_engine = None
_session_factory = None

def create_db_engine():
    """Create database engine with connection pooling for better performance"""
    global _engine
    
    if _engine is not None:
        return _engine
    
    url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(
        os.getenv("POSTGRES_USER"),
        os.getenv("POSTGRES_PASSWORD"),
        os.getenv("POSTGRES_HOST"),
        os.getenv("POSTGRES_PORT"),
        os.getenv("POSTGRES_DATABASE")
    )
    
    # Log connection for debugging
    logger.info(f"Creating database engine with connection pooling")
    
    # Create engine with connection pooling configuration
    _engine = create_engine(
        url,
        # Connection pool settings for better performance
        pool_size=10,           # Number of persistent connections to maintain
        max_overflow=20,        # Additional connections when pool is full
        pool_timeout=30,        # Seconds to wait for connection from pool
        pool_recycle=3600,      # Recycle connections after 1 hour
        pool_pre_ping=True,     # Validate connections before use
        # Performance optimizations
        echo=False,             # Set to True for SQL query logging (debug only)
        future=True,            # Use SQLAlchemy 2.0 style
        # Connection arguments for PostgreSQL optimization
        connect_args={
            "application_name": "aggiermp_api",
            "connect_timeout": 10,
        }
    )
    
    # Create tables if they don't exist
    Base.metadata.create_all(_engine)
    
    logger.info(f"Database engine created with pool_size=10, max_overflow=20")
    return _engine

def get_session_factory():
    """Get session factory with connection pooling"""
    global _session_factory
    
    if _session_factory is not None:
        return _session_factory
    
    engine = create_db_engine()
    _session_factory = sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False  # Keep objects accessible after commit
    )
    
    return _session_factory

def get_session():
    """Get database session from connection pool"""
    SessionFactory = get_session_factory()
    return SessionFactory()

# Performance monitoring decorator
def monitor_db_performance(func):
    """Decorator to monitor database query performance"""
    import time
    import functools
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
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
            logger.error(f"Query error in {func.__name__} after {execution_time:.3f}s: {str(e)}")
            raise
    return wrapper

# Database health check function
def check_database_health():
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
                "pool_type": str(type(pool).__name__)
            }
        except AttributeError:
            # Fallback for different pool implementations
            pool_status = {
                "pool_type": str(type(pool).__name__),
                "status": "active"
            }
        
        logger.info(f"Database health check passed. Pool status: {pool_status}")
        return {"status": "healthy", "pool_status": pool_status}
        
    except Exception as e:
        logger.error(f"Database health check failed: {str(e)}")
        return {"status": "unhealthy", "error": str(e)}

# Configuration for local vs remote database
def get_database_config():
    """Get database configuration with environment-specific optimizations"""
    host = os.getenv("POSTGRES_HOST", "localhost")
    is_local = host in ["localhost", "127.0.0.1", "::1"]
    
    if is_local:
        # Local database optimizations
        return {
            "pool_size": 5,         # Fewer connections needed locally
            "max_overflow": 10,     # Less overflow needed
            "pool_timeout": 10,     # Faster timeout for local connections
            "pool_recycle": 7200,   # Longer recycle time for stable local connections
        }
    else:
        # Remote database optimizations
        return {
            "pool_size": 10,        # More connections for remote database
            "max_overflow": 20,     # More overflow for network latency
            "pool_timeout": 30,     # Longer timeout for network delays
            "pool_recycle": 3600,   # Shorter recycle for remote connections
        }

def upsert_universities(session, universities: List[University]):
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
                (
                    x
                    for x in all_db_universities
                    if x.id == university.id
                ),
                None,
            )
        
        if db_university_obj:
            if db_university_obj.name != university.name or \
                db_university_obj.legacy_school_id != university.legacy_school_id or \
                db_university_obj.city != university.city or \
                db_university_obj.state != university.state:
                    
                update_universities.append({
                    'id': university.id,
                    'name': university.name,
                    'legacy_school_id': university.legacy_school_id,
                    'city': university.city,
                    'state': university.state,
                    'updated_at': datetime.now()
                })
        else:
            insert_universities.append({
                'id': university.id,
                'name': university.name,
                'legacy_school_id': university.legacy_school_id,
                'city': university.city,
                'state': university.state
            })
    
    print("Number of universities to insert: ", len(insert_universities))
    print("Number of universities to update: ", len(update_universities))
    
    if insert_universities:
        session.execute(insert(UniversityDB), insert_universities)
    if update_universities:
        session.execute(update(UniversityDB), update_universities)
    
    session.commit()
    return insert_universities + update_universities

def upsert_professors(session, professors: List[Professor]):
    """
    Upsert professors into the database.
    Insert new records and update existing ones based on ID.
    """
    query_all = session.execute(select(ProfessorDB))
    all_db_profs = query_all.scalars().all()
    insert_professors: list[dict] = []
    update_professors: list[dict] = []
    
    for professor in professors:
        # Generate UUID if no ID provided
        if not professor.id:
            raise ValueError("Professor ID is required")
            
        db_prof_obj = next(
                (
                    x
                    for x in all_db_profs
                    if x.id == professor.id
                ),
                None,
            )
        
        if db_prof_obj:
            # if the new professor has more ratings, update the existing professor
            if professor.num_ratings > db_prof_obj.num_ratings:
                update_professors.append({
                    'id': professor.id,
                    'avg_rating': professor.avg_rating,
                    'avg_difficulty': professor.avg_difficulty,
                    'num_ratings': professor.num_ratings,
                    'would_take_again_percent': professor.would_take_again_percent,
                    'updated_at': datetime.now()
                })
        else:
            insert_professors.append({
                'id': professor.id,
                'university_id': professor.university_id,
                'legacy_id': professor.legacy_id,
                'first_name': professor.first_name,
                'last_name': professor.last_name,
                'department': professor.department,
                'avg_rating': professor.avg_rating,
                'avg_difficulty': professor.avg_difficulty,
                'num_ratings': professor.num_ratings,
                'would_take_again_percent': professor.would_take_again_percent,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            })
    
    print("Number of professors to insert: ", len(insert_professors))
    print("Number of professors to update: ", len(update_professors))
    
    if insert_professors:
        session.execute(insert(ProfessorDB), insert_professors)
    if update_professors:
        session.execute(update(ProfessorDB), update_professors)
    
    session.commit()
    return insert_professors + update_professors

def upsert_reviews(session, reviews: List[Review]):
    """
    Upsert reviews into the database.
    Insert new records and update existing ones based on ID.
    """
    insert_reviews: list[dict] = []

    for review in reviews:
        insert_reviews.append({
            **review.model_dump(),
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })
    
    if insert_reviews:
        session.execute(insert(ReviewDB), insert_reviews)
        session.commit()
    return insert_reviews
