"""One schema for constrained generation and local response validation."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Sentiment = Literal["positive", "negative", "mixed", "neutral"]
OverallTopic = Literal[
    "teaching", "exams", "grading", "workload", "policies", "personality", "other"
]

# Course fields in generation order. "attendance_policies" is stored in the
# existing ``policies`` column.
COURSE_FIELDS = ("exams", "grading", "workload", "attendance_policies", "teaching")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class CourseClaim(_Strict):
    text: str = Field(min_length=1)
    ids: list[str] = Field(min_length=1, max_length=4)
    sentiment: Sentiment


class Tip(_Strict):
    text: str = Field(min_length=1)
    ids: list[str] = Field(min_length=1, max_length=4)


class CourseEntry(_Strict):
    course: str
    exams: list[CourseClaim] = Field(max_length=2)
    grading: list[CourseClaim] = Field(max_length=2)
    workload: list[CourseClaim] = Field(max_length=2)
    attendance_policies: list[CourseClaim] = Field(max_length=2)
    teaching: list[CourseClaim] = Field(max_length=2)
    tips: list[Tip] = Field(max_length=3)


class OverallClaim(_Strict):
    text: str = Field(min_length=1)
    ids: list[str] = Field(min_length=1, max_length=4)
    topic: OverallTopic


class Tldr(_Strict):
    text: str = Field(min_length=1)
    ids: list[str] = Field(min_length=1, max_length=6)


class ProfessorSummaryOutput(_Strict):
    # Details come before the TL;DR so the model writes the headline last.
    courses: list[CourseEntry]
    strengths: list[OverallClaim] = Field(max_length=5)
    watch_outs: list[OverallClaim] = Field(max_length=5)
    tldr: Tldr


def professor_summary_response_format() -> dict:
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "professor_summary",
            "strict": True,
            "schema": ProfessorSummaryOutput.model_json_schema(),
        },
    }
