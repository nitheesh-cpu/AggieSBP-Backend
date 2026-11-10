"""Decide which RateMyProfessors profile belongs to a registrar instructor.

Names alone cannot tell two people apart: Texas A&M has a "Yu Zhang" who
teaches AGEC and a different "Yu Zhang" who teaches CSCE, and each has an
RMP profile. The registrar gives every instructor a stable PIDM, so we know
which subjects each person has taught. A profile's reviews tell us which
subjects its person taught. A profile is attached only when that evidence
points to this instructor and not to a same-name colleague. When the
evidence cannot decide, no profile is attached: showing nothing is better
than showing another person's reviews.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from aggiermp.core.name_matching import parse_person_name, professor_name_score
from aggiermp.core.subjects import normalize_course_code, parse_course_code

NAME_MATCH_THRESHOLD = 0.82

# Cached responses that depend on these rules must change key when the rules
# change, or a fix stays invisible until the old entries expire.
MATCHING_VERSION = hashlib.sha1(
    b"".join(
        (Path(__file__).parent / name).read_bytes()
        for name in ("professor_identity.py", "name_matching.py", "subjects.py")
    )
).hexdigest()[:10]


@dataclass(frozen=True)
class Person:
    """A registrar instructor and every subject they have taught."""

    name: str
    subjects: frozenset[str]


@dataclass(frozen=True)
class Profile:
    """An RMP profile and the courses its reviews mention."""

    id: str
    name: str
    subjects: frozenset[str]
    course_codes: frozenset[str]


def _same_name(left: str, right: str) -> bool:
    """True when surname and first given name agree exactly (not just fuzzily)."""
    a, b = parse_person_name(left), parse_person_name(right)
    if "".join(a.surname) != "".join(b.surname):
        return False
    return bool(a.given and b.given and a.given[0] == b.given[0])


def _fit(profile: Profile, person: Person) -> tuple[bool, bool, float]:
    """How well a person explains a profile: taught its subjects, then name."""
    return (
        bool(profile.subjects & person.subjects),
        _same_name(profile.name, person.name),
        professor_name_score(profile.name, person.name),
    )


def choose_profile(
    instructor: Person,
    course_code: str,
    profiles: Sequence[Profile],
    namesakes: Sequence[Person] = (),
) -> Optional[Profile]:
    """Pick the profile for ``instructor`` teaching ``course_code``, or None.

    ``instructor.subjects`` must include the subject of ``course_code``.
    ``namesakes`` are other instructors (other PIDMs) with similar names.
    """
    match = re.match(r"^([A-Z]+)(\d{3})", normalize_course_code(course_code))
    target_subject = match.group(1) if match else ""
    target_course = match.group(1) + match.group(2) if match else ""

    named = [
        profile
        for profile in profiles
        if professor_name_score(instructor.name, profile.name) >= NAME_MATCH_THRESHOLD
    ]
    rivals = [
        person
        for person in namesakes
        if professor_name_score(instructor.name, person.name) >= NAME_MATCH_THRESHOLD
    ]

    # Drop profiles a same-name colleague explains at least as well.
    remaining = [
        profile
        for profile in named
        if not any(
            _fit(profile, rival) >= _fit(profile, instructor) for rival in rivals
        )
    ]

    supported = [p for p in remaining if p.subjects & instructor.subjects]
    if supported:

        def strength(profile: Profile) -> tuple[bool, bool, bool]:
            return (
                target_course in profile.course_codes,
                target_subject in profile.subjects,
                _same_name(profile.name, instructor.name),
            )

        best = max(strength(profile) for profile in supported)
        winners = [p for p in supported if strength(p) == best]
        return winners[0] if len(winners) == 1 else None

    # No review evidence for this instructor. Trust the name only when it is
    # exact and unique on both sides: one instructor and one profile have it.
    # (A nickname like "Josh" for "Joshua" needs review evidence instead.)
    if rivals:
        return None
    exact = [p for p in remaining if _same_name(p.name, instructor.name)]
    return exact[0] if len(exact) == 1 else None


def _valid_subjects(db: Any) -> frozenset[str]:
    from sqlalchemy import text

    rows = db.execute(
        text("SELECT id FROM departments UNION SELECT DISTINCT dept FROM sections")
    ).fetchall()
    return frozenset(str(row[0]).upper().strip() for row in rows if row[0])


def load_people(
    db: Any, instructor_name: str, pidm: Optional[int], subject: str
) -> tuple[Person, list[Person]]:
    """Load the instructor's teaching history and that of similar-named people."""
    from sqlalchemy import or_
    from aggiermp.database.base import SectionDB, SectionInstructorDB

    surname = parse_person_name(instructor_name).surname
    longest = max(surname, key=len) if surname else ""
    filters = (
        [SectionInstructorDB.instructor_name.ilike(f"%{longest}%")] if longest else []
    )
    if pidm is not None:
        filters.append(SectionInstructorDB.instructor_pidm == pidm)
    rows = (
        db.query(
            SectionInstructorDB.instructor_pidm,
            SectionInstructorDB.instructor_name,
            SectionDB.dept,
        )
        .join(SectionDB, SectionInstructorDB.section_id == SectionDB.id)
        .filter(or_(*filters))
        .distinct()
        .all()
        if filters
        else []
    )

    names: dict[Any, str] = {}
    subjects: dict[Any, set[str]] = {}
    for row_pidm, row_name, dept in rows:
        # Without a PIDM the name is the only identity we have.
        key = row_pidm if row_pidm is not None else ("name", row_name)
        names.setdefault(key, row_name)
        subjects.setdefault(key, set()).add(str(dept).upper())

    own_key = pidm if pidm is not None else ("name", instructor_name)
    own_subjects = frozenset(subjects.get(own_key, set()) | {subject.upper()})
    instructor = Person(instructor_name, own_subjects)
    namesakes = [
        Person(names[key], frozenset(subjects[key]))
        for key in names
        if key != own_key
        and professor_name_score(instructor_name, names[key]) >= NAME_MATCH_THRESHOLD
    ]
    return instructor, namesakes


def load_profiles(db: Any, candidates: Iterable[Any]) -> list[Profile]:
    """Attach review evidence to ProfessorDB candidate rows."""
    from aggiermp.database.base import ProfessorSummaryNewDB, ReviewDB

    candidates = list(candidates)
    if not candidates:
        return []
    ids = [candidate.id for candidate in candidates]
    valid = _valid_subjects(db)
    raw_codes = (
        db.query(ReviewDB.professor_id, ReviewDB.course_code)
        .filter(ReviewDB.professor_id.in_(ids), ReviewDB.course_code.isnot(None))
        .distinct()
        .all()
        + db.query(
            ProfessorSummaryNewDB.professor_id, ProfessorSummaryNewDB.course_code
        )
        .filter(
            ProfessorSummaryNewDB.professor_id.in_(ids),
            ProfessorSummaryNewDB.course_code.isnot(None),
        )
        .distinct()
        .all()
    )

    subjects: dict[str, set[str]] = {pid: set() for pid in ids}
    codes: dict[str, set[str]] = {pid: set() for pid in ids}
    for profile_id, raw in raw_codes:
        parsed = parse_course_code(raw, valid)
        if parsed:
            subjects[profile_id].add(parsed[0])
            codes[profile_id].add(parsed[0] + parsed[1])
    return [
        Profile(
            id=candidate.id,
            name=f"{candidate.first_name} {candidate.last_name}",
            subjects=frozenset(subjects[candidate.id]),
            course_codes=frozenset(codes[candidate.id]),
        )
        for candidate in candidates
    ]
