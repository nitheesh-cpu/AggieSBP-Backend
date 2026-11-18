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

import functools
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from aggiermp.core.name_matching import (
    _normalize_token, parse_person_name, professor_name_score,
)
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
    # False for sections listed without a PIDM: such a row names someone but
    # cannot say which person, so it may block a duplicate merge, never prove one.
    has_pidm: bool = True


@dataclass(frozen=True)
class Profile:
    """An RMP profile and the courses its reviews mention."""

    id: str
    name: str
    subjects: frozenset[str]
    course_codes: frozenset[str]
    # Used only to find duplicate profiles of one person (see plan_profile_merges).
    university_id: str = ""
    department: str = ""
    num_ratings: int = 0
    legacy_id: Optional[int] = None
    # The review site has the first and last names swapped (see
    # profile_match_name); ``name`` is then given in the corrected order.
    swapped: bool = False


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
    # A swapped profile name ("FURTH HOWARD") always needs review evidence:
    # "Thomas James" may be someone other than "James Thomas".
    if rivals:
        return None
    exact = [
        p for p in remaining
        if not p.swapped and _same_name(p.name, instructor.name)
    ]
    return exact[0] if len(exact) == 1 else None


def profile_match_name(
    instructor_name: str, first_name: Optional[str], last_name: Optional[str]
) -> Optional[tuple[str, bool]]:
    """The profile's name in the order that matches the instructor, and
    whether that order is swapped; None when neither order matches.

    Normally "First Last". Some review profiles have the two swapped:
    "FURTH HOWARD" is the registrar's "Brett H. Furth". When the profile's
    first name is exactly the instructor's surname, "Last First" is tried
    too. Such a match is only a candidate: choose_profile still needs review
    evidence for it (see Profile.swapped).
    """
    first_name, last_name = (first_name or "").strip(), (last_name or "").strip()
    normal = f"{first_name} {last_name}"
    if professor_name_score(instructor_name, normal) >= NAME_MATCH_THRESHOLD:
        return normal, False
    surname = "".join(parse_person_name(instructor_name).surname)
    first = "".join(_normalize_token(token) for token in first_name.split())
    if not surname or first != surname or not last_name:
        return None
    swapped = f"{last_name} {first_name}"
    if professor_name_score(instructor_name, swapped) >= NAME_MATCH_THRESHOLD:
        return swapped, True
    return None


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


def load_profiles(
    db: Any, candidates: Iterable[Any], aliases: Optional[dict[str, str]] = None
) -> list[Profile]:
    """Attach review evidence to ProfessorDB candidate rows.

    With ``aliases`` (confirmed merges, alias id -> canonical id), merged
    entries become one profile: an alias candidate is replaced by its
    canonical row, and every merged entry's reviews count as evidence for it.
    """
    from aggiermp.database.base import ProfessorDB, ProfessorSummaryNewDB, ReviewDB

    candidates = list(candidates)
    if not candidates:
        return []
    members: dict[str, list[str]] = {}
    if aliases:
        by_id = {candidate.id: candidate for candidate in candidates}
        missing = {
            aliases[c.id] for c in candidates if c.id in aliases
        } - set(by_id)
        if missing:
            for row in db.query(ProfessorDB).filter(ProfessorDB.id.in_(missing)).all():
                by_id[row.id] = row
        candidates = [c for c in by_id.values() if c.id not in aliases]
        for alias_id, canonical_id in aliases.items():
            members.setdefault(canonical_id, []).append(alias_id)
    ids = [candidate.id for candidate in candidates]
    owner = {pid: pid for pid in ids}
    for pid in ids:
        for alias_id in members.get(pid, []):
            owner[alias_id] = pid
    ids = list(owner)
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

    subjects: dict[str, set[str]] = {pid: set() for pid in owner.values()}
    codes: dict[str, set[str]] = {pid: set() for pid in owner.values()}
    for profile_id, raw in raw_codes:
        parsed = parse_course_code(raw, valid)
        if parsed:
            subjects[owner[profile_id]].add(parsed[0])
            codes[owner[profile_id]].add(parsed[0] + parsed[1])
    return [
        Profile(
            id=candidate.id,
            name=f"{candidate.first_name} {candidate.last_name}",
            subjects=frozenset(subjects[candidate.id]),
            course_codes=frozenset(codes[candidate.id]),
            university_id=str(getattr(candidate, "university_id", "") or ""),
            department=str(getattr(candidate, "department", "") or ""),
            num_ratings=int(getattr(candidate, "num_ratings", 0) or 0),
            legacy_id=getattr(candidate, "legacy_id", None),
        )
        for candidate in candidates
    ]


# ---------------------------------------------------------------------------
# Duplicate RMP profiles of one person
#
# RMP sometimes holds two teacher entries for one person ("Abigail Songok" and
# "Abigael Songok", both teaching CHEM 119), often with a spelling variant or
# under two departments. Each entry becomes its own professor row, so reviews
# split across two pages and choose_profile() sees two equally good profiles
# and attaches neither. merge_duplicates.py proposes pairs found here; a person
# confirms each one in professor_aliases before the *_merged views combine it.
#
# A false merge shows one person's reviews under another, which is worse than a
# missed duplicate, so the rules are deliberately narrow:
#   * the first names are identical, or differ by a known-safe spelling edit;
#     nicknames, initials and empty profiles are never merged;
#   * both profiles have reviews for at least one shared course;
#   * no other registrar instructor with that surname and a compatible first
#     name taught the subjects involved (sections without a PIDM count as
#     possible other people);
#   * every pair in a merged group has this evidence directly.
# ---------------------------------------------------------------------------

# Name parsing is the slow part of planning merges over thousands of
# profiles; names repeat constantly, so parse each one once.
_parsed = functools.lru_cache(maxsize=None)(parse_person_name)


_NICKNAMES: tuple[frozenset[str], ...] = tuple(
    frozenset(group.split())
    for group in (
        "william bill will billy",
        "robert bob rob bobby robbie",
        "james jim jimmy jamie",
        "michael mike",
        "christopher chris",
        "christina christine chris tina",
        "thomas tom tommy",
        "richard rick rich dick",
        "joseph joe joey",
        "daniel dan danny",
        "david dave",
        "stephen steven steve",
        "matthew matt",
        "andrew andy drew",
        "anthony tony",
        "edward ed eddie ted",
        "elizabeth liz beth betsy lizzy",
        "katherine catherine kathryn kate kathy katie cathy",
        "jennifer jen jenny",
        "margaret maggie peggy meg",
        "patricia patty trish",
        "samuel sam",
        "benjamin ben",
        "alexander alex",
        "nicholas nick",
        "jonathan jon",
        "timothy tim",
        "kenneth ken kenny",
        "ronald ron",
        "donald don",
        "gregory greg",
        "jeffrey jeff",
        "lawrence larry",
        "charles chuck charlie",
        "susan sue",
        "deborah debbie deb",
        "rebecca becky",
        "joshua josh",
        "zachary zach zack",
        "frederick fred",
        "douglas doug",
        "gerald jerry",
        "raymond ray",
        "phillip philip phil",
        "jacob jake",
    )
)

_VOWELS = frozenset("aeiouy")

# Pairs one vowel apart that are usually two names, often a man's and a
# woman's. The ending rules in _spelling_variant catch most others.
_DIFFERENT_NAMES = frozenset(
    frozenset(pair.split())
    for pair in (
        "francis frances",
        "jesse jessie",
        "jose josie",
        "marian marion",
        "louis luis",
        "ahmad ahmed",
        "stephan stephen",
        "elena elina",
        "jaime jamie",
    )
)

_TITLES = frozenset({"dr", "prof", "professor", "mr", "mrs", "ms", "miss", "mx"})
_SUFFIXES = frozenset({"jr", "sr", "ii", "iii", "iv", "v"})


@functools.lru_cache(maxsize=None)
def _clean_name(name: str) -> tuple[str, frozenset[str]]:
    """Strip titles ("Dr.") and return the name with its suffixes (Jr., III).

    Keeps the registrar's "Last, First" comma, including when it sits on a
    removed suffix ("Smith Jr., John" -> "Smith, John").
    """
    kept: list[str] = []
    suffixes = set()
    for token in (name or "").split():
        bare = re.sub(r"[^a-z]", "", token.lower())
        if bare in _TITLES or bare in _SUFFIXES:
            if bare in _SUFFIXES:
                suffixes.add(bare)
            if token.endswith(",") and kept and not kept[-1].endswith(","):
                kept[-1] += ","
            continue
        kept.append(token)
    return " ".join(kept).strip(" ,"), frozenset(suffixes)


def _one_edit(left: str, right: str) -> Optional[tuple[str, int, str, str]]:
    """Describe a single-character edit between two names, or None.

    Returns (kind, index, old, new): "sub" for a changed letter, "indel" for
    a letter present in only one name (old is that letter), or "swap" for
    two swapped neighbours.
    """
    if len(left) == len(right):
        diffs = [i for i, (a, b) in enumerate(zip(left, right)) if a != b]
        if len(diffs) == 1:
            i = diffs[0]
            return ("sub", i, left[i], right[i])
        if (
            len(diffs) == 2
            and diffs[1] == diffs[0] + 1
            and left[diffs[0]] == right[diffs[1]]
            and left[diffs[1]] == right[diffs[0]]
        ):
            i = diffs[0]
            return ("swap", i, left[i : i + 2], right[i : i + 2])
        return None
    if abs(len(left) - len(right)) != 1:
        return None
    longer, shorter = (left, right) if len(left) > len(right) else (right, left)
    for i in range(len(longer)):
        if longer[:i] + longer[i + 1 :] == shorter:
            return ("indel", i, longer[i], "")
    return None


def _spelling_variant(left: str, right: str) -> Optional[str]:
    """How two first names are one name spelled two ways, or None.

    "spelling": an added or dropped "h" (Sara/Sarah, Jonathan/Johnathan) or
    doubled consonant (Colin/Collin). These rarely make another name.
    "vowel": one vowel changed, added or dropped away from the end of the
    name (Abigail/Abigael, Kristen/Kristin). Vowel changes often do make
    another name (Maria/Marie, Young/Yong), so duplicate_evidence also needs
    the registrar to confirm them by exact name.
    """
    if left[:1] != right[:1] or min(len(left), len(right)) < 4:
        return None
    if frozenset((left, right)) in _DIFFERENT_NAMES:
        return None
    edit = _one_edit(left, right)
    if edit is None:
        return None
    kind, i, old, new = edit
    longer = left if len(left) >= len(right) else right
    if kind == "indel":
        doubled = (i > 0 and longer[i - 1] == old) or longer[i + 1 : i + 2] == old
        if old == "h" or (doubled and old not in _VOWELS):
            return "spelling"
    # Everything else must be a vowel edit that leaves the last letter alone:
    # the ending is where names mark gender or become another name
    # (Terry/Terri, Andre/Andrea, Julia/Julie, and Rene/Renee or Jesse/Jessie,
    # where the edit lands on the last letter of the shorter name).
    if min(len(left), len(right)) < 5 and len(left) == len(right):
        return None  # Short names: Dana/Dina, Mark/Mary.
    if left[-1] != right[-1] or i >= min(len(left), len(right)) - 1:
        return None
    if kind == "sub" and old in _VOWELS and new in _VOWELS:
        return "vowel"
    if kind == "indel" and old in _VOWELS:
        return "vowel"
    return None


@functools.lru_cache(maxsize=None)
def name_variant(left: str, right: str) -> Optional[str]:
    """How two profile names can belong to one person, or None.

    Returns "same", "spelling", "vowel", "nickname" or "initial"; only the
    first three can lead to a merge. Surnames must be identical (spaces and
    hyphens aside), suffixes (Jr., III) must agree, and a second given name,
    when both names have one, must agree too. Titles (Dr.) are ignored.
    """
    left, left_suffixes = _clean_name(left)
    right, right_suffixes = _clean_name(right)
    if left_suffixes != right_suffixes:
        return None
    a, b = _parsed(left), _parsed(right)
    if not a.surname or "".join(a.surname) != "".join(b.surname):
        return None
    if not a.given or not b.given:
        return None
    if len(a.given) > 1 and len(b.given) > 1:
        middle_a, middle_b = a.given[1], b.given[1]
        if middle_a != middle_b and not (
            min(len(middle_a), len(middle_b)) == 1 and middle_a[0] == middle_b[0]
        ):
            return None
    first_a, first_b = a.given[0], b.given[0]
    if first_a == first_b:
        return "same"
    if min(len(first_a), len(first_b)) == 1:
        return "initial" if first_a[0] == first_b[0] else None
    variant = _spelling_variant(first_a, first_b)
    if variant:
        return variant
    if any(first_a in group and first_b in group for group in _NICKNAMES):
        return "nickname"
    return None


_MERGEABLE_KINDS = frozenset({"same", "spelling", "vowel"})


@functools.lru_cache(maxsize=None)
def _first_name(name: str) -> str:
    given = _parsed(_clean_name(name)[0]).given
    return given[0] if given else ""


@functools.lru_cache(maxsize=None)
def _surname_tokens(name: str) -> frozenset[str]:
    surname = _parsed(_clean_name(name)[0]).surname
    return frozenset(surname) | ({"".join(surname)} if surname else frozenset())


def _could_be(person: Person, profile: Profile) -> bool:
    """Could this registrar instructor be the person behind ``profile``?

    Deliberately loose (a shared surname part and the same first initial),
    because here a match only ever blocks a merge.
    """
    if not _surname_tokens(person.name) & _surname_tokens(profile.name):
        return False
    return _first_name(person.name)[:1] == _first_name(profile.name)[:1]


def duplicate_evidence(
    left: Profile, right: Profile, people: Sequence[Person] = ()
) -> Optional[str]:
    """Why two RMP profiles belong to one person, or None when unproven.

    ``people`` are registrar instructors with every subject they taught. The
    returned text is what a person reads before confirming the merge, so it
    states exactly what matched.
    """
    if not left.university_id or left.university_id != right.university_id:
        return None
    # Both profiles must have reviews for a shared course. An empty profile
    # with a familiar name is as likely a new hire as a duplicate. (Checked
    # first: it is the cheapest test.)
    shared_codes = left.course_codes & right.course_codes
    if not shared_codes:
        return None
    kind = name_variant(left.name, right.name)
    if kind not in _MERGEABLE_KINDS:
        return None

    subjects = left.subjects | right.subjects
    namesakes = [
        person
        for person in people
        if person.subjects & subjects
        and (_could_be(person, left) or _could_be(person, right))
    ]
    if len(namesakes) > 1:
        return None  # More than one possible person taught these subjects.

    names = f"{_clean_name(left.name)[0]} / {_clean_name(right.name)[0]}"
    courses = ", ".join(sorted(shared_codes))
    if namesakes:
        person = namesakes[0]
        if not person.has_pidm:
            return None  # A section without a PIDM cannot prove who it was.
        exact = [name_variant(person.name, p.name) == "same" for p in (left, right)]
        # The registrar name must be exactly one of the two names (both, when
        # the profile names are identical); a different spelling or nickname
        # could be another person.
        if not any(exact) or (kind == "same" and not all(exact)):
            return None
        check = (
            "CHECK: the first names differ by one vowel, which is sometimes a "
            "different name; "
            if kind == "vowel"
            else ""
        )
        return (
            f"{check}{kind} name ({names}); both reviewed for {courses}; the only registrar "
            f"instructor with a compatible name is {person.name} "
            f"(taught {', '.join(sorted(person.subjects & subjects))})"
        )

    # Nobody compatible in the registrar data. Identical names and safe
    # spelling edits may still merge on a shared course; vowel changes need
    # the registrar to confirm them.
    if kind == "vowel":
        return None
    return (
        f"{kind} name ({names}); both reviewed for {courses}; no registrar record "
        "of anyone with a compatible name (registrar data starts in 2025)"
    )


@dataclass(frozen=True)
class ProfileMerge:
    """Show ``alias_id`` under ``canonical_id``."""

    canonical_id: str
    alias_id: str
    evidence: str


def _canonical_rank(profile: Profile) -> tuple[int, int, int, str]:
    # Most reviews first (most-linked page); then the older RMP entry.
    legacy = profile.legacy_id if profile.legacy_id is not None else 2**62
    return (-profile.num_ratings, -len(profile.course_codes), legacy, profile.id)


def plan_profile_merges(
    profiles: Sequence[Profile], people: Sequence[Person] = ()
) -> tuple[list[ProfileMerge], list[list[str]]]:
    """Decide which profiles to show as one.

    Returns the merges and the groups skipped as ambiguous: linked profiles
    where some pair lacks direct evidence (A matches B and B matches C, but
    A and C are not shown to be one person).
    """
    by_surname: dict[tuple[str, str], list[Profile]] = {}
    for profile in profiles:
        surname = "".join(_parsed(_clean_name(profile.name)[0]).surname)
        if surname:
            by_surname.setdefault((profile.university_id, surname), []).append(profile)

    # Index registrar instructors by surname part once; scanning every
    # instructor for every surname group took minutes on production data.
    people_by_token: dict[str, list[Person]] = {}
    for person in people:
        for token in _surname_tokens(person.name):
            people_by_token.setdefault(token, []).append(person)

    merges: list[ProfileMerge] = []
    ambiguous: list[list[str]] = []
    for (_, surname), group in sorted(by_surname.items()):
        if len(group) < 2:
            continue
        candidates = {
            id(person): person
            for profile in group
            for token in _surname_tokens(profile.name)
            for person in people_by_token.get(token, [])
        }
        nearby = [
            person
            for person in candidates.values()
            if any(_could_be(person, profile) for profile in group)
        ]
        evidence: dict[frozenset[str], str] = {}
        for i, left in enumerate(group):
            for right in group[i + 1 :]:
                found = duplicate_evidence(left, right, nearby)
                if found:
                    evidence[frozenset((left.id, right.id))] = found

        # Connected components over evidence edges.
        parent = {p.id: p.id for p in group}

        def root(node: str) -> str:
            while parent[node] != node:
                parent[node] = parent[parent[node]]
                node = parent[node]
            return node

        for pair in evidence:
            a, b = tuple(pair)
            parent[root(a)] = root(b)
        components: dict[str, list[Profile]] = {}
        for profile in group:
            components.setdefault(root(profile.id), []).append(profile)

        for members in components.values():
            if len(members) < 2:
                continue
            # Every pair needs its own evidence; a hub linking two profiles
            # that are not shown to be one person merges nothing.
            if any(
                frozenset((a.id, b.id)) not in evidence
                for i, a in enumerate(members)
                for b in members[i + 1 :]
            ):
                ambiguous.append(sorted(m.id for m in members))
                continue
            canonical = min(members, key=_canonical_rank)
            for member in sorted(members, key=_canonical_rank):
                if member is not canonical:
                    merges.append(
                        ProfileMerge(
                            canonical.id,
                            member.id,
                            evidence[frozenset((canonical.id, member.id))],
                        )
                    )
    return merges, ambiguous


def load_all_people(db: Any) -> list[Person]:
    """Every registrar instructor with every subject taught: one per PIDM,
    plus one per name for sections listed without a PIDM (has_pidm=False).

    PIDM-less rows stay apart even when one PIDM has the same name: they may
    be that person or someone else, so they can only block a merge.
    """
    from aggiermp.database.base import SectionDB, SectionInstructorDB

    rows = (
        db.query(
            SectionInstructorDB.instructor_pidm,
            SectionInstructorDB.instructor_name,
            SectionDB.dept,
        )
        .join(SectionDB, SectionInstructorDB.section_id == SectionDB.id)
        .distinct()
        .all()
    )
    names: dict[Any, str] = {}
    subjects: dict[Any, set[str]] = {}
    for pidm, name, dept in rows:
        if not name:
            continue
        key = pidm if pidm is not None else ("name", name)
        names.setdefault(key, name)
        subjects.setdefault(key, set()).add(str(dept).upper())
    return [
        Person(names[key], frozenset(subjects[key]), has_pidm=not isinstance(key, tuple))
        for key in names
    ]


# ---------------------------------------------------------------------------
# Aliases left behind by merges
# ---------------------------------------------------------------------------


def _aliases_table_exists(db: Any) -> bool:
    """Checked up front instead of catching a failed SELECT, since rolling
    back after one would also discard the caller's uncommitted work."""
    from sqlalchemy import text

    try:
        found = db.execute(text("SELECT to_regclass('professor_aliases')")).scalar()
    except Exception:
        return False
    # to_regclass gives the table name, or NULL when it's missing. Anything
    # else (a Mock session in tests) counts as missing.
    return isinstance(found, str)


def load_aliases(db: Any) -> dict[str, str]:
    """Confirmed merges: alias id -> canonical id. Proposed or rejected
    merges are ignored, as they are by the *_merged views."""
    from sqlalchemy import text

    if not _aliases_table_exists(db):
        return {}
    rows = db.execute(
        text(
            "SELECT alias_id, canonical_id FROM professor_aliases "
            "WHERE status = 'confirmed'"
        )
    ).fetchall()
    return {str(alias): str(canonical) for alias, canonical in rows}


def resolve_professor_id(db: Any, professor_id: str) -> str:
    """The id a professor is shown under: its canonical id when a confirmed
    merge folded it into another entry, else the id itself."""
    from sqlalchemy import text

    if not _aliases_table_exists(db):
        return professor_id
    row = db.execute(
        text(
            "SELECT canonical_id FROM professor_aliases "
            "WHERE alias_id = :id AND status = 'confirmed'"
        ),
        {"id": professor_id},
    ).fetchone()
    return str(row[0]) if row else professor_id
