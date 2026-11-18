"""Find registrar instructors by name, including ones with no review profile.

Professor search only covers review profiles. About half of the professor
name searches that found nothing were real instructors teaching this term
with no reviews yet (e.g. "hee tak" for Hee Yeon Tak). This lists them with
the courses they teach and, when the grade reports have them, their GPA.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

from aggiermp.core.name_matching import professor_name_score

GPA_NAME_THRESHOLD = 0.82


def _like(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def _words(raw: str) -> List[str]:
    return [w for w in re.split(r"[\s,\-.]+", raw.lower()) if len(w) >= 2]


def instructor_name_condition(
    raw: str, column: str = "si.instructor_name", key: str = "name"
) -> Tuple[str, Dict[str, Any]]:
    """WHERE clause matching the typed text, or every typed word in any order.

    Registrar names are "First Middle Last", so "Tak, Hee Yeon", "hee tak"
    and "chiang-yu" (hyphenated given names) all need the any-order form.
    """
    text_ = raw.strip()
    params: Dict[str, Any] = {key: _like(text_)}
    conditions = [f"{column} ILIKE :{key}"]
    words = _words(text_)
    if len(words) >= 2:
        parts = []
        for i, word in enumerate(words):
            params[f"{key}_w{i}"] = _like(word)
            parts.append(f"{column} ILIKE :{key}_w{i}")
        conditions.append(" AND ".join(parts))
    return "(" + " OR ".join(f"({c})" for c in conditions) + ")", params


@dataclass
class Instructor:
    name: str
    pidm: Optional[int]
    term_code: str = ""
    campuses: set = field(default_factory=set)
    # course code ("ENGL104") -> title
    courses: Dict[str, str] = field(default_factory=dict)
    grades: Optional[Dict[str, Any]] = None


def _rank(instructor: Instructor, query: str) -> Tuple[int, int, int, str]:
    name = instructor.name.lower()
    tokens = re.split(r"[\s\-.]+", name)
    words = _words(query) or [query.lower().strip()]
    prefix_hits = sum(1 for w in words if any(t.startswith(w) for t in tokens))
    return (
        -int(query.lower().strip() in name),
        -prefix_hits,
        -len(instructor.courses),
        instructor.name,
    )


def group_instructors(rows: Iterable[Any], query: str, limit: int) -> List[Instructor]:
    """One entry per person (name and PIDM), best name matches first.

    ``rows`` have instructor_name, instructor_pidm, term_code, dept,
    course_number, course_title and campus.
    """
    people: Dict[Tuple[str, Optional[int]], Instructor] = {}
    for row in rows:
        key = (row.instructor_name, row.instructor_pidm)
        person = people.setdefault(key, Instructor(row.instructor_name, row.instructor_pidm))
        person.term_code = max(person.term_code, row.term_code or "")
        if row.campus and row.campus.strip():
            person.campuses.add(row.campus.strip())
        code = f"{row.dept}{row.course_number}"
        if code not in person.courses or not person.courses[code]:
            person.courses[code] = (row.course_title or "").strip()
    ranked = sorted(people.values(), key=lambda p: _rank(p, query))
    return ranked[:limit]


def attach_grades(instructors: List[Instructor], gpa_rows: Iterable[Any]) -> None:
    """Sum the official grades of each instructor's courses.

    ``gpa_rows`` have professor ("TAK H"), dept, course_number, gpa,
    total_students and grade_a..grade_f. Grade reports exist for College
    Station only and name instructors as "LAST I", so a row counts only for
    a course the instructor teaches and when the name fits.
    """
    by_course: Dict[str, List[Any]] = {}
    for row in gpa_rows:
        by_course.setdefault(f"{row.dept}{row.course_number}", []).append(row)
    for person in instructors:
        if person.campuses and "College Station" not in person.campuses:
            continue
        rows = [
            row
            for code in person.courses
            for row in by_course.get(code, [])
            if professor_name_score(row.professor, person.name) >= GPA_NAME_THRESHOLD
        ]
        total = sum(row.total_students or 0 for row in rows)
        if not total:
            continue
        graded = [row for row in rows if row.gpa is not None]
        graded_students = sum(row.total_students or 0 for row in graded)
        person.grades = {
            "avgGpa": round(
                sum(row.gpa * (row.total_students or 0) for row in graded) / graded_students, 2
            ) if graded_students else None,
            "totalStudents": total,
            "distribution": {
                letter: sum(getattr(row, f"grade_{letter.lower()}") or 0 for row in rows)
                for letter in "ABCDF"
            },
        }


def to_json(person: Instructor, term_names: Dict[str, str]) -> Dict[str, Any]:
    return {
        "name": person.name,
        "campus": ", ".join(sorted(person.campuses)) or None,
        "term": term_names.get(person.term_code) or person.term_code,
        "courses": [
            {"code": code, "title": title or None}
            for code, title in sorted(person.courses.items())
        ],
        "grades": person.grades,
    }
