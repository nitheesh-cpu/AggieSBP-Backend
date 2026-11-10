"""
Output schemas for hierarchical summarization system.
"""

from dataclasses import dataclass, field
from typing import Any, List, Optional, Dict


@dataclass
class SummaryClaim:
    """One generated claim with source-review provenance and verification."""

    text: str
    evidence_review_ids: List[str]
    topic: str = "other"
    support_probability: Optional[float] = None
    overgeneralization_probability: Optional[float] = None
    disagreement_erasure_probability: Optional[float] = None
    sentiment: Optional[str] = None
    sentiment_confidence: Optional[float] = None
    importance: Optional[float] = None
    importance_confidence: Optional[float] = None
    accepted: bool = True


@dataclass
class ClusterSummary:
    """Summary of a single semantic cluster"""

    cluster_type: str
    summary: str
    review_count: int
    sentiment: str  # "positive", "negative", "mixed"
    confidence: float
    claims: List[SummaryClaim] = field(default_factory=list)
    topics: List[str] = field(default_factory=list)
    disagreement_probability: float = 0.0
    importance: float = 0.0


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
    # Stats
    avg_rating: Optional[float] = None
    avg_difficulty: Optional[float] = None
    common_tags: Optional[List[str]] = None
    tag_frequencies: Optional[Dict[str, int]] = None
    # Structured-summary fields (single-request pipeline)
    tips: Optional[List[str]] = None
    review_stats: Optional[Dict[str, Any]] = None
    # Field name -> one list of cited RMP review IDs per claim
    citations: Optional[Dict[str, Any]] = None


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
    total_reviews: int = 0
    # Stats
    avg_rating: Optional[float] = None
    avg_difficulty: Optional[float] = None
    common_tags: Optional[List[str]] = None
    tag_frequencies: Optional[Dict[str, int]] = None
    # Structured-summary fields (single-request pipeline)
    tldr: Optional[str] = None
    review_stats: Optional[Dict[str, Any]] = None
    citations: Optional[Dict[str, Any]] = None
    # Request and claim counters for logging; not stored
    generation_stats: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProcessedReview:
    """Cleaned and processed review"""

    review_id: str
    professor_id: str
    course_code: Optional[str]
    text: str
    original_text: str
    word_count: int
    # Stats
    original_rating: Optional[float] = None
    original_difficulty: Optional[float] = None
    tags: Optional[List[str]] = None


@dataclass
class ReviewData:
    """Raw review data structure for legacy summarizer"""

    id: str
    professor_id: str
    course_code: Optional[str]
    review_text: str
    clarity_rating: float
    difficulty_rating: float
    helpful_rating: float
    rating_tags: List[str]
    grade: Optional[str]
