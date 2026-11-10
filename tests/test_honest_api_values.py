"""
Missing data comes back as null instead of a made-up 3.0, per-course numbers
are really per course, and review ratings leave difficulty out.

DB-free (fake session):
    pytest --noconftest tests/test_honest_api_values.py
"""

import asyncio
import inspect
import json
from types import SimpleNamespace

import aggiermp.api.main as api
from aggiermp.api.routers import discover
from test_structured_summary_api import FakeSession, Result, Row, call


def test_review_quality_ignores_difficulty() -> None:
    review = SimpleNamespace(clarity_rating=5.0, helpful_rating=5.0, difficulty_rating=5.0)
    assert api._review_quality(review) == 5.0


def test_review_quality_uses_whichever_quality_field_exists() -> None:
    review = SimpleNamespace(clarity_rating=None, helpful_rating=2.0, difficulty_rating=1.0)
    assert api._review_quality(review) == 2.0


def test_review_quality_is_none_without_quality_fields() -> None:
    review = SimpleNamespace(clarity_rating=None, helpful_rating=None, difficulty_rating=3.0)
    assert api._review_quality(review) is None


def professor_session(course: Row, gpa_rows: list) -> FakeSession:
    return FakeSession(
        True,
        {
            "WHERE p.id = :professor_id": Result(
                [Row(id="P1", name="Paula deWitte", first_name="Paula", last_name="deWitte")]
            ),
            "gd.professor = :anex_name": Result(gpa_rows),
            "ps.total_reviews as reviews_count": Result([course]),
        },
    )


def test_professor_courses_use_course_rating_and_gpa() -> None:
    course = Row(course_id="CSCE402", course_name=None, reviews_count=11, avg_rating=4.6)
    gpa = Row(course_code="CSCE402", avg_gpa=3.71, students=420, last_year="2025")
    db = professor_session(course, [gpa])

    data = call(api.get_professor_profile, professor_id="P1", db=db)

    (entry,) = data["courses"]
    assert entry["avg_rating"] == 4.6
    assert entry["course_name"] is None  # no "Course Title" placeholder
    assert entry["avg_gpa"] == 3.71
    assert entry["gpa_students"] == 420
    # The per-course query must not fall back to the professor's overall rating.
    (courses_sql,) = [s for s in db.statements if "reviews_count" in s]
    assert "p.avg_rating" not in courses_sql
    assert "ps.course_code IS NOT NULL" in courses_sql
    # anex names are "LASTNAME F".
    assert any("gd.professor = :anex_name" in s for s in db.statements)


def test_professor_missing_values_are_null_not_three() -> None:
    course = Row(course_id="CSCE702", course_name="Cyber Law", reviews_count=2, avg_rating=None)
    db = professor_session(course, [])

    data = call(api.get_professor_profile, professor_id="P1", db=db)

    assert data["overall_rating"] is None
    assert data["would_take_again_percent"] is None
    (entry,) = data["courses"]
    assert entry["avg_rating"] is None
    assert entry["avg_gpa"] is None


def course_detail_session(**course_fields: object) -> FakeSession:
    course = Row(code="CSCE 121", name="Intro", credits=4, difficulty="Moderate",
                 enrollment=0, sections=0, **course_fields)
    return FakeSession(
        False,
        {
            "recent_semesters": Result([course]),
            "SELECT COUNT(*) FROM professor_summaries_new": Result(scalar=0),
        },
    )


def test_course_detail_reports_all_time_gpa_scope() -> None:
    db = course_detail_session(avggpa=3.05, gpa_scope="all", gpa_last_year="2023",
                               enrollment_year="2025", sections_term_code="202711")

    data = call(api.get_course_details, course_id="CSCE121", db=db)

    assert data["avgGPA"] == 3.05
    assert data["gpaScope"] == "all"
    assert data["gpaLastYear"] == "2023"
    assert data["sectionsTermCode"] == "202711"
    (course_sql,) = [s for s in db.statements if "recent_semesters" in s]
    assert "all_time_gpa" in course_sql
    # Section attributes follow the course's latest semester, not a fixed one.
    assert not any("'2025'" in s for s in db.statements)


def test_course_detail_without_grade_data() -> None:
    db = course_detail_session(avggpa=-1, gpa_scope=None)

    data = call(api.get_course_details, course_id="CSCE121", db=db)

    assert data["avgGPA"] == -1
    assert data["gpaScope"] is None


def test_fit_sections_with_no_busy_times_returns_every_section() -> None:
    sections = [
        {"id": "202711_1", "crn": "1", "section": "501", "is_open": False,
         "instructor": "Ada Lovelace", "meetings": "MWF 09:10 AM-10:00 AM"},
    ]
    row = Row(course_key="HIST-105", dept="HIST", course_number="105",
              course_title="US History", compatible_section_count=1,
              sample_section_id="202711_1", sample_crn="1",
              sections=json.dumps(sections))
    db = FakeSession(False, {"user_schedule": Result([row])})

    matches = asyncio.run(inspect.unwrap(discover.discover_fit_sections)(
        request=None, term_code="202711",
        payload=discover.DiscoverFitRequest(course_keys=["HIST-105"], schedule_blocks=[]),
        db=db,
    ))

    (match,) = matches
    assert match.compatible_section_count == 1
    (section,) = match.sections
    assert section.crn == "1" and section.is_open is False
    assert section.meetings == "MWF 09:10 AM-10:00 AM"


def test_ucc_fit_candidates_use_full_easiness_formula() -> None:
    row = Row(dept="HIST", course_number="105", course_title="US History",
              avg_gpa=3.6, avg_difficulty=2.0, avg_rating=4.5)
    db = FakeSession(False, {"candidate_courses": Result([row])})

    (course,) = asyncio.run(inspect.unwrap(discover.discover_ucc_fit_candidates)(
        request=None, term_code="202711",
        payload=discover.UccFitCandidatesRequest(categories=["Core American History (KHIS)"]),
        db=db,
    ))

    expected = discover.calculate_easiness_score(3.6, 2.0, 4.5) * 100
    assert course.easinessScore == round(expected, 1)
    assert course.easinessScore > 50  # GPA alone could never pass 50
