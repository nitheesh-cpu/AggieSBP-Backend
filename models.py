from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum
import uuid

class SummaryType(str, Enum):
    OVERALL = "overall"
    COURSE_SPECIFIC = "course_specific"

class University(BaseModel):
    """University model"""
    id: Optional[str] = None
    name: str
    rmp_school_id: Optional[str] = None
    registration_system_url: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class Professor(BaseModel):
    """Professor model - Updated to match PostgreSQL schema"""
    id: Optional[str] = None
    university_id: str
    rmp_id: str
    rmp_legacy_id: Optional[int] = None
    first_name: str
    last_name: str
    department: Optional[str] = None
    avg_rating: Optional[float] = None
    avg_difficulty: Optional[float] = None
    num_ratings: int = 0
    would_take_again_percent: Optional[float] = None
    last_scraped: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }
        
class ProfessorUpdate(BaseModel):
    """Professor model - Updated to match PostgreSQL schema"""
    id: Optional[str] = None
    university_id: str
    rmp_id: str
    rmp_legacy_id: Optional[int] = None
    first_name: str
    last_name: str
    department: Optional[str] = None
    avg_rating: Optional[float] = None
    avg_difficulty: Optional[float] = None
    num_ratings: int = 0
    would_take_again_percent: Optional[float] = None
    created_at: Optional[datetime] = None


    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class Course(BaseModel):
    """Course model"""
    id: Optional[str] = None
    university_id: str
    course_code: str
    course_name: Optional[str] = None
    department: Optional[str] = None
    normalized_code: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class ProfessorCourseStats(BaseModel):
    """Professor course statistics model"""
    id: Optional[str] = None
    professor_id: str
    course_code: str
    course_count: int = 0
    normalized_code: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class ProfessorCourse(BaseModel):
    """Professor-Course mapping model"""
    id: Optional[str] = None
    professor_id: str
    course_id: str
    created_at: Optional[datetime] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class Review(BaseModel):
    """Review model with comprehensive RMP fields - Updated to match PostgreSQL schema"""
    id: Optional[str] = None
    professor_id: str
    course_code: Optional[str] = None
    rmp_review_id: Optional[str] = None
    rmp_legacy_id: Optional[int] = None
    clarity_rating: Optional[float] = None
    difficulty_rating: Optional[float] = None
    helpful_rating: Optional[float] = None
    would_take_again: Optional[str] = None  # "Yes", "No", or null
    attendance_mandatory: Optional[str] = None  # "Yes", "No", or null
    is_online_class: bool = False
    is_for_credit: bool = True
    review_text: Optional[str] = None
    grade: Optional[str] = None
    review_date: Optional[datetime] = None
    textbook_use: int = -1  # -1 = not specified
    thumbs_up_total: int = 0
    thumbs_down_total: int = 0
    rating_tags: Optional[Dict[str, Any]] = None  # JSONB type in PostgreSQL
    admin_reviewed_at: Optional[datetime] = None
    flag_status: Optional[str] = None
    created_by_user: bool = False
    teacher_note: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class ReviewTag(BaseModel):
    """Review tag model"""
    id: Optional[str] = None
    review_id: str
    tag: str
    created_at: Optional[datetime] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

class Summary(BaseModel):
    """Summary model for AI-generated content"""
    id: Optional[str] = None
    professor_id: str
    course_code: Optional[str] = None
    summary_type: SummaryType
    summary_text: str
    key_points: Optional[Dict[str, Any]] = None
    metrics: Optional[Dict[str, Any]] = None
    generated_at: Optional[datetime] = None
    model_used: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }

# Table name mapping for PostgreSQL
TABLE_NAMES = {
    'universities': 'universities',
    'professors': 'professors',
    'courses': 'courses',
    'professor_course_stats': 'professor_course_stats',
    'professor_courses': 'professor_courses',
    'reviews': 'reviews',
    'review_tags': 'review_tags',
    'summaries': 'summaries'
}

# Model mapping for type checking
MODEL_MAPPING = {
    'universities': University,
    'professors': Professor,
    'courses': Course,
    'professor_course_stats': ProfessorCourseStats,
    'professor_courses': ProfessorCourse,
    'reviews': Review,
    'review_tags': ReviewTag,
    'summaries': Summary
} 