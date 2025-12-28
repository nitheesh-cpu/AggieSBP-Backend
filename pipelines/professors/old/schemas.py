"""
Schemas for professors pipeline.

Contains Pydantic models and dataclasses for data validation and structure.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class ReviewData:
    """Data class for review information used in summarization"""
    id: str
    professor_id: str
    course_code: Optional[str]
    review_text: Optional[str]
    clarity_rating: Optional[float]
    difficulty_rating: Optional[float]
    helpful_rating: Optional[float]
    rating_tags: Optional[List[str]]
    grade: Optional[str]


@dataclass
class ReviewCollectionStats:
    """Statistics for review collection process"""
    total_professors: int = 0
    total_reviews_collected: int = 0
    successful_professors: int = 0
    failed_professors: int = 0
    skipped_professors: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

