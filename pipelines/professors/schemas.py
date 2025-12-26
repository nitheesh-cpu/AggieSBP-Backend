"""
Output schemas for hierarchical summarization system.
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class ClusterSummary:
    """Summary of a single semantic cluster"""
    cluster_type: str
    summary: str
    review_count: int
    sentiment: str  # "positive", "negative", "mixed"
    confidence: float


@dataclass
class CourseSummary:
    """Structured summary for a specific course"""
    course: str
    teaching: Optional[str] = None
    exams: Optional[str] = None
    grading: Optional[str] = None
    workload: Optional[str] = None
    personality: Optional[str] = None
    policies: Optional[str] = None
    other: Optional[str] = None
    confidence: float = 0.0
    total_reviews: int = 0


@dataclass
class ProfessorSummary:
    """Overall summary for a professor across all courses"""
    professor_id: str
    overall_sentiment: str
    strengths: List[str]
    complaints: List[str]
    consistency: str
    confidence: float
    course_summaries: List[CourseSummary]


@dataclass
class ProcessedReview:
    """Cleaned and processed review"""
    review_id: str
    professor_id: str
    course_code: Optional[str]
    text: str
    original_text: str
    word_count: int

