"""Hierarchical summarization system for professor reviews."""

from typing import Any

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


def __getattr__(name: str) -> Any:
    """Avoid loading transformer models when importing lightweight helpers."""
    if name == "HierarchicalSummarizationPipeline":
        from pipelines.professors.hierarchical_summarization.pipeline import (
            HierarchicalSummarizationPipeline,
        )

        return HierarchicalSummarizationPipeline
    raise AttributeError(name)
