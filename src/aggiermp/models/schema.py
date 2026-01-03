from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator, ConfigDict
from enum import Enum


class SummaryType(str, Enum):
    OVERALL = "overall"
    COURSE_SPECIFIC = "course_specific"


class University(BaseModel):
    """University model"""

    id: Optional[str]
    name: str
    legacy_school_id: Optional[int] = Field(alias="legacyId")
    city: Optional[str]
    state: Optional[str]

    model_config = ConfigDict(populate_by_name=True)


class Professor(BaseModel):
    """Professor model - Updated to match PostgreSQL schema"""

    id: str
    university_id: str
    legacy_id: Optional[int] = Field(alias="legacyId")
    first_name: str = Field(alias="firstName")
    last_name: str = Field(alias="lastName")
    department: Optional[str] = Field(alias="department")
    avg_rating: Optional[float] = Field(alias="avgRating")
    avg_difficulty: Optional[float] = Field(alias="avgDifficulty")
    num_ratings: int = Field(alias="numRatings")
    would_take_again_percent: Optional[float] = Field(alias="wouldTakeAgainPercent")

    model_config = ConfigDict(populate_by_name=True)


class Course(BaseModel):
    """Course model"""

    id: Optional[str] = None
    university_id: str
    course_code: str
    course_name: Optional[str] = None
    department: Optional[str] = None
    normalized_code: Optional[str] = None


class ProfessorCourseStats(BaseModel):
    """Professor course statistics model"""

    id: Optional[str] = None
    professor_id: str
    course_code: str
    course_count: int = 0
    normalized_code: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


class ProfessorCourse(BaseModel):
    """Professor-Course mapping model"""

    id: Optional[str] = None
    professor_id: str
    course_id: str
    created_at: Optional[datetime] = None

    model_config = ConfigDict(populate_by_name=True)


class Review(BaseModel):
    """Review model with comprehensive RMP fields - Updated to match PostgreSQL schema"""

    id: Optional[str] = None
    legacy_id: Optional[int] = Field(alias="legacyId")
    professor_id: str
    course_code: Optional[str] = Field(alias="class")
    clarity_rating: Optional[float] = Field(alias="clarityRating")
    difficulty_rating: Optional[float] = Field(alias="difficultyRating")
    helpful_rating: Optional[float] = Field(alias="helpfulRating")
    would_take_again: Optional[int] = Field(
        alias="wouldTakeAgain"
    )  # "Yes", "No", or null
    attendance_mandatory: Optional[str] = Field(alias="attendanceMandatory")
    is_online_class: bool = Field(alias="isForOnlineClass")
    is_for_credit: bool = Field(alias="isForCredit")
    review_text: Optional[str] = Field(alias="comment")
    grade: Optional[str] = None
    review_date: Optional[datetime] = Field(alias="date")
    textbook_use: Optional[int] = Field(alias="textbookUse")
    thumbs_up_total: int = Field(alias="thumbsUpTotal")
    thumbs_down_total: int = Field(alias="thumbsDownTotal")
    rating_tags: Optional[List[str]] = Field(
        alias="ratingTags"
    )  # Will store as list of strings
    admin_reviewed_at: Optional[datetime] = Field(alias="adminReviewedAt")
    flag_status: Optional[str] = Field(alias="flagStatus")
    created_by_user: bool = Field(alias="createdByUser")

    @field_validator("rating_tags", mode="before")
    @classmethod
    def parse_rating_tags(cls, v):
        """Parse rating tags string into list of individual tags"""
        if v is None:
            return None

        if isinstance(v, list):
            return v

        if isinstance(v, str):
            if not v.strip():
                return None
            tags = [tag.strip() for tag in v.split("--") if tag.strip()]
            return tags if tags else None

        return None

    @field_validator("review_date", "admin_reviewed_at", mode="before")
    @classmethod
    def parse_datetime(cls, v):
        """Parse datetime strings from RMP API format to datetime objects"""
        if v is None:
            return None

        # If it's already a datetime object, return as-is
        if isinstance(v, datetime):
            return v

        # If it's a string, try to parse it
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None

            # Handle RMP format: "2024-12-12 23:47:15 +0000 UTC"
            if v.endswith(" +0000 UTC"):
                try:
                    # Remove the timezone suffix and parse
                    date_part = v.replace(" +0000 UTC", "")
                    return datetime.strptime(date_part, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass

            # Try ISO format as fallback
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError:
                pass

            # Try other common formats
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d",
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(v, fmt)
                except ValueError:
                    continue

        # If we can't parse it, return None
        return None

    model_config = ConfigDict(populate_by_name=True)


class ReviewTag(BaseModel):
    """Review tag model"""

    id: Optional[str] = None
    review_id: str
    tag: str
    created_at: Optional[datetime] = None

    model_config = ConfigDict(populate_by_name=True)


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
    llm_model_used: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(populate_by_name=True)


# Table name mapping for PostgreSQL
TABLE_NAMES = {
    "universities": "universities",
    "professors": "professors",
    "courses": "courses",
    "professor_course_stats": "professor_course_stats",
    "professor_courses": "professor_courses",
    "reviews": "reviews",
    "review_tags": "review_tags",
    "summaries": "summaries",
}

# Model mapping for type checking
MODEL_MAPPING = {
    "universities": University,
    "professors": Professor,
    "courses": Course,
    "professor_course_stats": ProfessorCourseStats,
    "professor_courses": ProfessorCourse,
    "reviews": Review,
    "review_tags": ReviewTag,
    "summaries": Summary,
}
