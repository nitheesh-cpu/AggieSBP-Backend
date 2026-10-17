"""Deterministic review statistics computed without any language model.

Every statistic is computed from the raw stored reviews (before text filtering
or deduplication), because short or duplicate texts still carry valid metadata.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Mapping, Optional

RECENT_WINDOW_DAYS = 730

_GRADE_ALIASES = {
    "A+": "A+",
    "A": "A",
    "A-": "A-",
    "B+": "B+",
    "B": "B",
    "B-": "B-",
    "C+": "C+",
    "C": "C",
    "C-": "C-",
    "D+": "D+",
    "D": "D",
    "D-": "D-",
    "F": "F",
    "PASS": "Pass",
    "FAIL": "Fail",
    "DROP/WITHDRAWAL": "Drop/Withdrawal",
    "INCOMPLETE": "Incomplete",
    "AUDIT/NO GRADE": "Audit/No Grade",
}
GRADE_ORDER = list(dict.fromkeys(_GRADE_ALIASES.values()))

_ATTENDANCE = {"mandatory": True, "y": True, "non mandatory": False, "n": False}

# RMP has encoded textbook use three ways. Since 2016 it is binary: 0/5 until
# 2021, then -1/3. Review text confirms -1/0 mean "no" and 3/5 mean "yes".
# Before 2016 it was a 1-5 usage scale, which is skipped as ambiguous.
_TEXTBOOK = {-1: False, 0: False, 3: True, 5: True}
_TEXTBOOK_BINARY_SINCE = datetime(2016, 1, 1)


def _value(review: Any, key: str) -> Any:
    if isinstance(review, Mapping):
        return review.get(key)
    return getattr(review, key, None)


def _date(review: Any) -> Optional[datetime]:
    value = _value(review, "review_date")
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(
                tzinfo=None
            )
        except ValueError:
            return None
    return None


def review_quality(review: Any) -> Optional[float]:
    """RMP's overall quality: the mean of clarity and helpfulness."""
    clarity = _value(review, "clarity_rating")
    helpful = _value(review, "helpful_rating")
    if clarity is None or helpful is None:
        return None
    return (float(clarity) + float(helpful)) / 2.0


def normalize_grade(grade: Any) -> Optional[str]:
    """Map a self-reported grade to a display label; None means not reported."""
    if not isinstance(grade, str):
        return None
    key = grade.strip().upper().replace("_", " ")
    return _GRADE_ALIASES.get(key)


def _percent(hits: int, total: int) -> Optional[float]:
    return round(100.0 * hits / total, 1) if total else None


def _average(values: list[float]) -> Optional[float]:
    return round(sum(values) / len(values), 2) if values else None


def compute_review_stats(
    reviews: Iterable[Any], now: Optional[datetime] = None
) -> Dict[str, Any]:
    """Return JSON-serializable statistics for a set of raw review rows."""
    reviews = list(reviews)
    now = now or datetime.now()
    cutoff = now - timedelta(days=RECENT_WINDOW_DAYS)

    take_again = [
        int(value)
        for value in (_value(review, "would_take_again") for review in reviews)
        if value in (0, 1)
    ]
    grades = Counter(
        grade
        for grade in (normalize_grade(_value(r, "grade")) for r in reviews)
        if grade
    )
    attendance = [
        _ATTENDANCE[key]
        for key in (
            str(_value(review, "attendance_mandatory") or "").strip().lower()
            for review in reviews
        )
        if key in _ATTENDANCE
    ]
    textbook = []
    for review in reviews:
        value = _value(review, "textbook_use")
        date = _date(review)
        if value in _TEXTBOOK and date is not None and date >= _TEXTBOOK_BINARY_SINCE:
            textbook.append(_TEXTBOOK[value])

    recent, older = [], []
    for review in reviews:
        quality = review_quality(review)
        date = _date(review)
        if quality is None or date is None:
            continue
        (recent if date >= cutoff else older).append(quality)

    dates = [date for date in (_date(review) for review in reviews) if date]
    return {
        "review_count": len(reviews),
        "would_take_again_pct": _percent(sum(take_again), len(take_again)),
        "would_take_again_n": len(take_again),
        "grade_distribution": {
            grade: grades[grade] for grade in GRADE_ORDER if grades[grade]
        },
        "grades_reported_n": sum(grades.values()),
        "attendance_mandatory_pct": _percent(sum(attendance), len(attendance)),
        "attendance_n": len(attendance),
        "textbook_used_pct": _percent(sum(textbook), len(textbook)),
        "textbook_n": len(textbook),
        "recent_window_years": RECENT_WINDOW_DAYS // 365,
        "avg_rating_recent": _average(recent),
        "recent_n": len(recent),
        "avg_rating_older": _average(older),
        "older_n": len(older),
        "first_review_date": min(dates).date().isoformat() if dates else None,
        "last_review_date": max(dates).date().isoformat() if dates else None,
    }
