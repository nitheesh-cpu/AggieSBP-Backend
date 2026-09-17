from types import SimpleNamespace

from aggiermp.api.routers.discover import _dedupe_discover_professor_rows


def _row(
    *,
    professor_id: str,
    first_name: str = "Robert",
    last_name: str = "Lightfoot",
    avg_rating: float = 0,
    avg_difficulty: float = 0,
    total_reviews: int = 0,
    gpa_student_count: int = 200,
) -> SimpleNamespace:
    return SimpleNamespace(
        dept="CSCE",
        course_number="331",
        professor_id=professor_id,
        first_name=first_name,
        last_name=last_name,
        avg_rating=avg_rating,
        avg_difficulty=avg_difficulty,
        total_reviews=total_reviews,
        gpa_student_count=gpa_student_count,
    )


def test_prefers_review_backed_duplicate_professor_record() -> None:
    sparse = _row(professor_id="duplicate", avg_rating=0, total_reviews=0)
    reviewed = _row(
        professor_id="canonical",
        avg_rating=3.3,
        avg_difficulty=2.9,
        total_reviews=42,
    )

    assert _dedupe_discover_professor_rows([sparse, reviewed]) == [reviewed]
    assert _dedupe_discover_professor_rows([reviewed, sparse]) == [reviewed]


def test_keeps_distinct_professors_for_the_same_course() -> None:
    robert = _row(professor_id="robert")
    alice = _row(
        professor_id="alice",
        first_name="Alice",
        last_name="Smith",
        avg_rating=4.5,
        total_reviews=10,
    )

    assert _dedupe_discover_professor_rows([robert, alice]) == [robert, alice]
