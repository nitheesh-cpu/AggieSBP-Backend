"""
API exposure of the structured professor-summary fields (tldr, tips,
review_stats, citations) with and without migration 001 applied.

These tests use a fake session, so they need no database:
    pytest --noconftest tests/test_structured_summary_api.py
"""

import asyncio
import inspect
from typing import Any, Dict, List, Optional

import pytest
from sqlalchemy.orm import Session, undefer

import aggiermp.api.main as api
import aggiermp.api.structured_summaries as structured
from aggiermp.database.base import ProfessorSummaryNewDB

REVIEW_STATS = {
    "would_take_again_pct": 82.5,
    "would_take_again_n": 40,
    "grade_distribution": {"A": 12, "B": 5},
    "grades_reported_n": 17,
    "attendance_mandatory_pct": 0.0,
    "attendance_n": 0,
    "textbook_used_pct": 25.0,
    "textbook_n": 8,
    "avg_rating_recent": 4.4,
    "recent_n": 10,
    "avg_rating_older": 3.9,
    "older_n": 30,
    "first_review_date": "2015-09-01",
    "last_review_date": "2026-05-01",
}
CITATIONS = {"tldr": [["r1", "r2"]], "watch_outs": [["r3"]]}


class Row:
    """A result row; unknown columns read as None, like a nullable column."""

    def __init__(self, **values: Any) -> None:
        self.__dict__.update(values)

    def __getattr__(self, name: str) -> Any:
        return None


class Result:
    def __init__(self, rows: Optional[List[Any]] = None, scalar: Any = 0) -> None:
        self.rows = rows or []
        self._scalar = scalar

    def fetchone(self) -> Any:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> List[Any]:
        return list(self.rows)

    def scalar(self) -> Any:
        return self._scalar

    def __iter__(self) -> Any:
        return iter(self.rows)


class FakeSession:
    """Answers each SQL statement from the first route whose key it contains."""

    def __init__(self, migrated: bool, routes: Dict[str, Result]) -> None:
        columns = structured.STRUCTURED_SUMMARY_COLUMNS if migrated else ("tldr",)
        self.routes = {
            "information_schema.columns": Result([Row(column_name=c) for c in columns]),
            **routes,
        }
        self.statements: List[str] = []

    def execute(self, statement: Any, params: Any = None) -> Result:
        sql = str(statement)
        self.statements.append(sql)
        for key, result in self.routes.items():
            if key in sql:
                return result
        return Result()

    def rollback(self) -> None:
        pass


@pytest.fixture(autouse=True)
def reset_column_check(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(structured, "_columns_present", False)
    monkeypatch.setattr(structured, "_checked_at", None)


def call(endpoint: Any, **kwargs: Any) -> Dict[str, Any]:
    # Skip the rate-limit and cache decorators; they need a real request.
    return asyncio.run(inspect.unwrap(endpoint)(request=None, **kwargs))


def profile_session(migrated: bool) -> FakeSession:
    overall = Row(
        overall_sentiment="Clear lecturer with tough exams.",
        strengths=["Teaching: clear lectures"],
        complaints=["Exams: long"],
        consistency="Consistent",
        confidence=0.8,
        total_reviews=40,
        tldr="Take her if you want to learn; study for exams." if migrated else None,
        review_stats=REVIEW_STATS if migrated else None,
        citations=CITATIONS if migrated else None,
    )
    course = Row(
        course_id="CSCE121",
        course_name="Intro",
        reviews_count=40,
        avg_rating=4.2,
        tips=["Go to office hours", ""] if migrated else None,
    )
    return FakeSession(
        migrated,
        {
            "WHERE p.id = :professor_id": Result([Row(id="P1", name="Ada Lovelace")]),
            "ps.overall_sentiment": Result([overall]),
            "ps.total_reviews as reviews_count": Result([course]),
        },
    )


def test_professor_profile_includes_structured_fields_when_migrated() -> None:
    db = profile_session(migrated=True)
    data = call(api.get_professor_profile, professor_id="P1", db=db)

    summary = data["overallSummary"]
    assert summary["tldr"] == "Take her if you want to learn; study for exams."
    assert summary["reviewStats"] == REVIEW_STATS
    assert summary["citations"] == CITATIONS
    assert summary["strengths"] == ["Teaching: clear lectures"]
    assert data["courses"][0]["tips"] == ["Go to office hours"]
    assert any("ps.tldr, ps.review_stats, ps.citations" in s for s in db.statements)


def test_professor_profile_works_without_migration() -> None:
    db = profile_session(migrated=False)
    data = call(api.get_professor_profile, professor_id="P1", db=db)

    summary = data["overallSummary"]
    assert summary["sentiment"] == "Clear lecturer with tough exams."
    assert summary["tldr"] is None
    assert summary["reviewStats"] is None
    assert summary["citations"] is None
    assert data["courses"][0]["tips"] == []
    # The missing columns must never be referenced, or Postgres rejects the query.
    assert not any("ps.tldr" in s or "ps.tips" in s for s in db.statements)


def course_session(migrated: bool) -> FakeSession:
    professor = Row(
        professor_id="P1",
        name="Ada Lovelace",
        rating=4.2,
        rmp_id="123",
        overall_review_count=50,
        course_rating=4.1,
        total_reviews=40,
        confidence=0.8,
        teaching="Clear",
        exams="Hard",
        policies="Attendance is graded.",
        tips=["Start projects early"] if migrated else None,
        review_stats=REVIEW_STATS if migrated else None,
        citations=CITATIONS if migrated else None,
    )
    course = Row(
        code="CSCE 121",
        name="Intro",
        credits=4,
        avggpa=3.1,
        difficulty="Moderate",
        enrollment=300,
        sections=6,
    )
    return FakeSession(
        migrated,
        {
            "recent_semesters": Result([course]),
            "SELECT COUNT(*) FROM professor_summaries_new": Result(scalar=1),
            "professor_grades": Result([professor]),
        },
    )


@pytest.mark.parametrize("migrated", [True, False])
def test_course_details_course_summary_fields(migrated: bool) -> None:
    db = course_session(migrated)
    data = call(api.get_course_details, course_id="CSCE121", db=db)

    course_summary = data["professors"][0]["courseSummary"]
    assert course_summary["policies"] == "Attendance is graded."
    if migrated:
        assert course_summary["tips"] == ["Start projects early"]
        assert course_summary["reviewStats"] == REVIEW_STATS
        assert course_summary["citations"] == CITATIONS
    else:
        assert course_summary["tips"] == []
        assert course_summary["reviewStats"] is None
        assert course_summary["citations"] is None
        assert not any("ps.tips" in s for s in db.statements)


def test_column_check_caches_positive_and_rechecks_negative(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [1000.0]
    monkeypatch.setattr(structured.time, "monotonic", lambda: clock[0])

    missing = FakeSession(migrated=False, routes={})
    assert structured.structured_summary_columns_present(missing) is False
    assert structured.structured_summary_columns_present(missing) is False
    assert len(missing.statements) == 1  # negative answer reused for a while

    migrated = FakeSession(migrated=True, routes={})
    clock[0] += structured.RECHECK_SECONDS + 1
    assert structured.structured_summary_columns_present(migrated) is True
    clock[0] += 10_000
    assert structured.structured_summary_columns_present(migrated) is True
    assert len(migrated.statements) == 1  # positive answer cached for good


def test_column_check_failure_reports_missing() -> None:
    class Broken(FakeSession):
        def execute(self, statement: Any, params: Any = None) -> Result:
            raise RuntimeError("connection lost")

    assert structured.structured_summary_columns_present(Broken(True, {})) is False


def test_structured_fields_do_not_touch_rows_without_migration() -> None:
    class Exploding:
        def __getattr__(self, name: str) -> Any:
            raise AssertionError(f"read deferred column {name}")

    assert structured.structured_overall_fields(Exploding(), present=False) == {
        "tldr": None,
        "reviewStats": None,
        "citations": None,
    }
    assert structured.structured_course_fields(Exploding(), present=False)["tips"] == []


def test_structured_fields_parse_json_strings_and_drop_empties() -> None:
    row = Row(tldr="", review_stats='{"recent_n": 3}', citations="not json", tips=None)
    assert structured.structured_overall_fields(row, present=True) == {
        "tldr": None,
        "reviewStats": {"recent_n": 3},
        "citations": None,
    }
    assert structured.structured_course_fields(row, present=True)["tips"] == []


def test_deferred_columns_load_only_when_undeferred() -> None:
    session = Session()
    plain = str(session.query(ProfessorSummaryNewDB))
    for column in structured.STRUCTURED_SUMMARY_COLUMNS:
        assert f"professor_summaries_new.{column}" not in plain

    options = [
        undefer(getattr(ProfessorSummaryNewDB, column))
        for column in structured.STRUCTURED_SUMMARY_COLUMNS
    ]
    loaded = str(session.query(ProfessorSummaryNewDB).options(*options))
    for column in structured.STRUCTURED_SUMMARY_COLUMNS:
        assert f"professor_summaries_new.{column}" in loaded


def test_course_professor_details_includes_structured_fields() -> None:
    from types import SimpleNamespace
    from unittest.mock import Mock

    from aggiermp.core.campuses import TEXAS_AM_ID
    from test_campus_matching import details_endpoint, query

    profile = SimpleNamespace(
        id="P1", first_name="Ada", last_name="Lovelace", university_id=TEXAS_AM_ID,
        num_ratings=50, avg_rating=4.2, legacy_id=1,
    )
    overall = SimpleNamespace(
        total_reviews=40, overall_sentiment="Clear", strengths=[], complaints=[],
        consistency=None, confidence=0.8, tldr="Worth it.",
        review_stats=REVIEW_STATS, citations=CITATIONS,
    )
    course = SimpleNamespace(
        course_code="MATH151", avg_rating=4.0, teaching="Clear", exams=None,
        grading=None, workload=None, personality=None, policies=None,
        other="Tips: Do the homework", total_reviews=12, confidence=0.7,
        tips=["Do the homework"], review_stats=REVIEW_STATS, citations=None,
    )
    db = Mock()
    db.execute.return_value.fetchall.return_value = [
        Row(column_name=c) for c in structured.STRUCTURED_SUMMARY_COLUMNS
    ]
    queries = [
        query(first=SimpleNamespace(term_desc="Fall 2026 - College Station")),
        query([SimpleNamespace(instructor_name="Ada Lovelace", section_number="500",
                               campus="College Station")]),
        query([profile]), query(first=overall), query([course]), query(),
    ]
    db.query.side_effect = queries

    result = asyncio.run(details_endpoint()(None, "202631", "MATH151", db, None))

    professor = result["professors"][0]
    assert professor["overallSummary"]["tldr"] == "Worth it."
    assert professor["overallSummary"]["reviewStats"] == REVIEW_STATS
    assert professor["courseSummary"]["tips"] == ["Do the homework"]
    assert professor["courseSummary"]["other"] == "Tips: Do the homework"
    # Both summary queries asked for the deferred columns.
    for summary_query in queries[3:5]:
        (options,) = [call.args for call in summary_query.options.call_args_list]
        assert len(options) == len(structured.STRUCTURED_SUMMARY_COLUMNS)
