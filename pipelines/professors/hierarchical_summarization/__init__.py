"""
Hierarchical summarization system for professor reviews.
"""

from pipelines.professors.hierarchical_summarization.pipeline import HierarchicalSummarizationPipeline
from pipelines.professors.schemas import (
    ProfessorSummary,
    CourseSummary,
    ClusterSummary,
)

__all__ = [
    "HierarchicalSummarizationPipeline",
    "ProfessorSummary",
    "CourseSummary",
    "ClusterSummary",
]

