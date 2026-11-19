"""Decide which RateMyProfessors profile belongs to a registrar instructor.

Names alone cannot tell two people apart: Texas A&M has a "Yu Zhang" who
teaches AGEC and a different "Yu Zhang" who teaches CSCE, and each has an
RMP profile. The registrar gives every instructor a stable PIDM, so we know
which subjects each person has taught. A profile's reviews tell us which
subjects its person taught. A profile is attached only when that evidence
points to this instructor and not to a same-name colleague. When the
evidence cannot decide, no profile is attached: showing nothing is better
than showing another person's reviews.

Same-name colleagues (rivals) are found with a stricter name test than the
one that finds candidate profiles (see _similar_person): a middle initial
or a fuzzy spelling must not make "Brian A. Anderson" a rival of "Arden
Anderson". Rivals only ever block a profile, never attach one.
"""

from __future__ import annotations

import functools
import hashlib
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from aggiermp.core.name_matching import (
    _SURNAME_PARTICLES, NICKNAME_GROUPS, ParsedName, _normalize_token,
    name_parses, nickname_alternates, parse_person_name, professor_name_score,
)
from aggiermp.core.subjects import current_subject, normalize_course_code, parse_course_code
from aggiermp.core.instructor_links import LoadedLinks, instructor_key

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
    # The swapped name fits only through an initial ("Brett H. Furth" for
    # "FURTH HOWARD"): an assumption, so the matcher never picks it. The
    # unmatched-instructors report proposes it for a person to confirm.
    needs_course: bool = False
    # ``name`` in RMP's own split, "Last, First" (see rmp_name); empty when
    # unknown. match_instructor scores instructors against this form.
    match_name: str = ""


def _same_name(left: str, right: str) -> bool:
    """True when surname and first given name agree exactly (not just fuzzily).

    Every reading of a name counts (see name_parses), but the whole surname
    must agree: "Heidi K. Vanden Brink" is exactly "Vanden Brink, Heidi",
    while "John Taylor Smith" is not exactly "Taylor, John". Given names
    written apart or joined ("Hee Yeon", "Heeyeon") are one name.
    """
    return any(
        _same_parsed_name(a, b) for a in name_parses(left) for b in name_parses(right)
    )


def _same_parsed_name(a: ParsedName, b: ParsedName) -> bool:
    if "".join(a.surname) != "".join(b.surname):
        return False
    if not a.given or not b.given:
        return False
    if a.given[0] == b.given[0]:
        return True
    joined = "".join(t for t in a.given if len(t) > 1)
    return bool(joined) and joined == "".join(t for t in b.given if len(t) > 1)


def _usable_given(name: ParsedName) -> tuple[str, ...]:
    # A middle initial stands for a name only when the first given name is
    # an initial too ("M. E. Rister"); "Brian A." is not "Arden".
    if not name.given:
        return ()
    first_is_initial = len(name.given[0]) == 1
    return tuple(
        token
        for i, token in enumerate(name.given)
        if i == 0 or len(token) > 1 or first_is_initial
    )


def _given_names_agree(a: ParsedName, b: ParsedName) -> bool:
    joined = "".join(t for t in a.given if len(t) > 1)
    if joined and joined == "".join(t for t in b.given if len(t) > 1):
        return True
    for x in _usable_given(a):
        for y in _usable_given(b):
            if x == y:
                return True
            if len(x) == 1 or len(y) == 1:
                if x[0] == y[0]:
                    return True
            elif _spelling_variant(x, y) or any(
                # Any two names of one nickname line: Steve, Stephen, Steven.
                x in group and y in group for group in _NICKNAMES
            ):
                return True
    return False


def _surnames_agree(a: ParsedName, b: ParsedName) -> bool:
    if "".join(a.surname) == "".join(b.surname):
        return True
    # A whole part of a compound surname: "Garcia" and "Garcia-Lopez".
    core_a = {t for t in a.surname if t not in _SURNAME_PARTICLES}
    core_b = {t for t in b.surname if t not in _SURNAME_PARTICLES}
    return bool(core_a & core_b)


def _similar_person(left: str, right: str) -> bool:
    """Could two registrar names be one person's name? Used to find rivals.

    Surnames must be equal (spaces and hyphens aside) or share a whole part.
    First given names must agree exactly, by initial when one of them is an
    initial, as related names (Steve/Stephen/Steven), or by a one-letter
    spelling variant (Abigail/Abigael); a middle
    initial counts only when that person's first given name is an initial.
    """
    return any(
        _surnames_agree(a, b) and _given_names_agree(a, b)
        for a in name_parses(left)
        for b in name_parses(right)
    )


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
    return explain_choice(instructor, course_code, profiles, namesakes)[0]


def _subjects_text(subjects: Iterable[str]) -> str:
    return ", ".join(sorted(subjects)) or "none"


def explain_choice(
    instructor: Person,
    course_code: str,
    profiles: Sequence[Profile],
    namesakes: Sequence[Person] = (),
    name_alone: bool = True,
) -> tuple[Optional[Profile], dict[str, str]]:
    """choose_profile's decision plus, per profile not chosen, why not.

    With ``name_alone=False`` (a profile at another campus's RMP school) an
    exact, unique name without review evidence is not enough.
    """
    match = re.match(r"^([A-Z]+)(\d{3})", normalize_course_code(course_code))
    # Renamed subjects (PSYC -> PBSI) count as their current code.
    target_subject = current_subject(match.group(1)) if match else ""
    target_course = target_subject + match.group(2) if match else ""
    reasons: dict[str, str] = {}

    named = []
    for profile in profiles:
        score = professor_name_score(instructor.name, profile.name)
        if score >= NAME_MATCH_THRESHOLD:
            named.append(profile)
        else:
            reasons[profile.id] = f"name score {score:.2f} is below {NAME_MATCH_THRESHOLD}"
    rivals = [person for person in namesakes if _similar_person(instructor.name, person.name)]

    # Drop profiles a same-name colleague explains at least as well.
    remaining = []
    for profile in named:
        rival = next(
            (r for r in rivals if _fit(profile, r) >= _fit(profile, instructor)), None
        )
        if rival is None:
            remaining.append(profile)
        else:
            reasons[profile.id] = (
                f"another instructor, {rival.name} (taught "
                f"{_subjects_text(rival.subjects)}), fits its reviews at least as well"
            )

    def no_evidence(profile: Profile) -> str:
        if profile.needs_course:
            return (
                "its swapped RMP name fits only through a middle initial, which is an "
                "assumption, so it is never matched automatically (it is proposed for "
                f"review instead); it has reviews for {_subjects_text(profile.course_codes)}"
            )
        return (
            f"its reviews are for {_subjects_text(profile.subjects)}; the instructor "
            f"taught {_subjects_text(instructor.subjects)}"
        )

    supported = [
        p for p in remaining if p.subjects & instructor.subjects and not p.needs_course
    ]
    if supported:

        def strength(profile: Profile) -> tuple[bool, bool, bool]:
            return (
                target_course in profile.course_codes,
                target_subject in profile.subjects,
                _same_name(profile.name, instructor.name),
            )

        best = max(strength(profile) for profile in supported)
        winners = [p for p in supported if strength(p) == best]
        chosen = winners[0] if len(winners) == 1 else None
        for profile in remaining:
            if profile is chosen:
                continue
            if profile.needs_course:
                reasons[profile.id] = no_evidence(profile)
            elif profile not in supported:
                reasons[profile.id] = (
                    no_evidence(profile) + "; another profile has reviews in a subject they taught"
                )
            elif profile not in winners:
                reasons[profile.id] = (
                    f"another profile has stronger review evidence for {target_course}"
                )
            else:
                reasons[profile.id] = (
                    f"tied with {len(winners) - 1} other profile(s) on review evidence"
                )
        return chosen, reasons

    # No review evidence for this instructor. Trust the name only when it is
    # exact and unique on both sides: one instructor and one profile have it.
    # (A nickname like "Josh" for "Joshua" needs review evidence instead.)
    # A swapped profile name ("FURTH HOWARD") always needs review evidence:
    # "Thomas James" may be someone other than "James Thomas".
    exact = [
        p for p in remaining
        if not p.swapped and _same_name(p.name, instructor.name)
    ]
    chosen = exact[0] if len(exact) == 1 and not rivals and name_alone else None
    for profile in remaining:
        if profile is chosen:
            continue
        why = no_evidence(profile)
        if profile.needs_course:
            reasons[profile.id] = why
            continue
        if not name_alone:
            why += "; at another campus's RMP school the name alone is not enough"
        elif rivals:
            why += (
                "; without that evidence the name is not enough, since "
                f"{', '.join(sorted(r.name for r in rivals))} has a similar name"
            )
        elif profile.swapped:
            why += "; a swapped RMP name needs review evidence"
        elif profile not in exact:
            why += "; the name is not an exact match (initial, nickname or spelling)"
        else:
            why += f"; {len(exact)} profiles have this exact name"
        reasons[profile.id] = why
    return chosen, reasons


_RMP_SUFFIX = re.compile(r"\s*,\s*(?:jr|sr|ii|iii|iv|v)\.?\s*$", re.IGNORECASE)


def rmp_name(first_name: Optional[str], last_name: Optional[str]) -> str:
    """An RMP profile's name as "Last, First", keeping RMP's own split.

    Re-parsing "First Last" would read "Alejandro Castillo Ayala" as surname
    "Ayala". A trailing ", Jr." on the last name is dropped first.
    """
    first = (first_name or "").replace(",", " ").strip()
    last = _RMP_SUFFIX.sub("", (last_name or "").strip()).replace(",", " ").strip()
    if not last:
        return first
    return f"{last}, {first}" if first else last


def _without_initials(name: str) -> str:
    parsed = parse_person_name(name)
    given = " ".join(t for t in parsed.given if len(t) > 1)
    return f"{' '.join(parsed.surname)}, {given}"


def profile_match_name(
    instructor_name: str, first_name: Optional[str], last_name: Optional[str]
) -> Optional[tuple[str, bool]]:
    """The profile's name in the order that matches the instructor, and
    whether that order is swapped; None when neither order matches.

    Normally RMP's own split, "Last, First" (see rmp_name). Some review
    profiles have the two swapped: "FURTH HOWARD" is the registrar's
    "Brett H. Furth". When the profile's first name is exactly the
    instructor's surname, "Last First" is tried too. Such a match is only a
    candidate: choose_profile still needs review evidence for it (see
    Profile.swapped), and never picks it when it fits only through an
    initial (see swapped_needs_course).
    """
    first_name, last_name = (first_name or "").strip(), (last_name or "").strip()
    normal = rmp_name(first_name, last_name)
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


def swapped_needs_course(instructor_name: str, swapped_name: str) -> bool:
    """True when a swapped profile name fits the instructor only through an
    initial: "Socolofsky Scott" fits "Andee S. Scott" by the "S." alone.
    Such a profile is never matched automatically (see Profile.needs_course)."""
    return (
        professor_name_score(_without_initials(instructor_name), swapped_name)
        < NAME_MATCH_THRESHOLD
    )


def _valid_subjects(db: Any) -> frozenset[str]:
    from sqlalchemy import text

    rows = db.execute(
        text("SELECT id FROM departments UNION SELECT DISTINCT dept FROM sections")
    ).fetchall()
    return frozenset(str(row[0]).upper().strip() for row in rows if row[0])


def surname_tokens(instructor_name: str) -> list[str]:
    """Every surname word of every reading of the name (see name_parses),
    particles left out unless the surname is nothing else ("De")."""
    tokens: dict[str, None] = {}
    for parsed in name_parses(instructor_name):
        core = [t for t in parsed.surname if t not in _SURNAME_PARTICLES] or list(parsed.surname)
        tokens.update(dict.fromkeys(core))
    return list(tokens)


def given_initials(instructor_name: str) -> list[str]:
    """First letters a matching profile's first name may start with.

    The first given name's, a full middle name's (people who go by it:
    "J. Michael Smith" is "Michael Smith"), their nicknames' ("Bill" for
    William), and middle initials only when the first name is an initial.
    """
    given = parse_person_name(instructor_name).given
    if not given:
        return []
    first_is_initial = len(given[0]) == 1
    initials: dict[str, None] = {}
    for i, token in enumerate(given):
        if len(token) == 1:
            if i == 0 or first_is_initial:
                initials[token] = None
            continue
        initials[token[0]] = None
        initials.update(dict.fromkeys(sorted(n[0] for n in nickname_alternates(token))))
    return list(initials)


def load_people(
    db: Any, instructor_name: str, pidm: Optional[int], subject: str
) -> tuple[Person, list[Person]]:
    """Load the instructor's teaching history and that of similar-named people."""
    from sqlalchemy import or_
    from aggiermp.database.base import SectionDB, SectionInstructorDB

    # Letters only, so "O'Brien" and "Moreira-Tamayo" contain their parts.
    letters = _letters(SectionInstructorDB.instructor_name)
    filters = [letters.like(f"%{token}%") for token in surname_tokens(instructor_name)]
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
        subjects.setdefault(key, set()).add(current_subject(str(dept)))

    own_key = pidm if pidm is not None else ("name", instructor_name)
    own_subjects = frozenset(subjects.get(own_key, set()) | {current_subject(subject)})
    instructor = Person(instructor_name, own_subjects)
    namesakes = [
        Person(names[key], frozenset(subjects[key]))
        for key in names
        if key != own_key and names[key] and _similar_person(instructor_name, names[key])
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
            # "First Last" for the duplicate-merge code; matching uses match_name.
            name=f"{candidate.first_name} {candidate.last_name}",
            subjects=frozenset(subjects[candidate.id]),
            course_codes=frozenset(codes[candidate.id]),
            university_id=str(getattr(candidate, "university_id", "") or ""),
            department=str(getattr(candidate, "department", "") or ""),
            num_ratings=int(getattr(candidate, "num_ratings", 0) or 0),
            legacy_id=getattr(candidate, "legacy_id", None),
            match_name=rmp_name(candidate.first_name, candidate.last_name),
        )
        for candidate in candidates
    ]


@dataclass
class InstructorMatch:
    """The profile chosen for one instructor teaching one course, and why."""

    professor: Any = None  # ProfessorDB row, or None
    # False when the registrar name has no surname: the course page skips it.
    shown: bool = True
    # Rows returned by the SQL name prefilter, and those whose name matched.
    pool: list[Any] = field(default_factory=list)
    candidates: list[Any] = field(default_factory=list)
    profiles: list[Profile] = field(default_factory=list)
    # Candidate id -> profile name in the corrected order (swapped RMP names).
    swapped_names: dict[str, str] = field(default_factory=dict)
    # Profile id -> why it was not chosen.
    rejected: dict[str, str] = field(default_factory=dict)
    # Why no profile was chosen; empty when one was.
    reason: str = ""


# Accented letters folded, and name punctuation dropped, in SQL. One
# translate() call: characters past the end of _UNACCENTED are deleted. It is
# much cheaper than regexp_replace on the section_instructors scan.
_ACCENTED = "áàâäãåāéèêëēíìîïīóòôöõøōúùûüūñçýÿłđı"
_UNACCENTED = "aaaaaaaeeeeeiiiiiooooooouuuuuncyyldi"
_DROPPED = " '’`-.,"


def _letters(column: Any) -> Any:
    """SQL: the column lowercased, accents folded, spaces and punctuation dropped."""
    from sqlalchemy import func

    return func.translate(func.lower(column), _ACCENTED + _DROPPED, _UNACCENTED)


def match_instructor(
    db: Any,
    instructor_name: str,
    pidm: Optional[int],
    university_id: Optional[str],
    dept: str,
    course_code: str,
    aliases: Optional[dict[str, str]] = None,
    cache: Optional[dict[Any, Any]] = None,
    name_alone: bool = True,
    rejected_ids: frozenset[str] = frozenset(),
) -> InstructorMatch:
    """Find the profile a course page shows for an instructor.

    Used by the course professors endpoint and the unmatched-instructors
    report, so both decide the same way. ``cache`` (optional) keeps query
    results between calls for a batch job; without it every call runs the
    same queries in the same order. ``rejected_ids`` are profiles a person
    turned down for this instructor (instructor_links): they are removed
    from the candidates before choosing.
    """
    from sqlalchemy import and_, or_
    from aggiermp.database.base import ProfessorDB

    cache = {} if cache is None else cache
    parsed = parse_person_name(instructor_name)
    if not parsed.surname:
        return InstructorMatch(shown=False, reason="the registrar name has no surname")

    pool_key = ("pool", instructor_name, university_id)
    if pool_key not in cache:
        # Query broad candidate pool, then score in Python for robust matching.
        tokens = surname_tokens(instructor_name)
        last_name, first_name = _letters(ProfessorDB.last_name), _letters(ProfessorDB.first_name)
        surname_filters = [
            last_name.like(f"{initial}%")
            for initial in dict.fromkeys(token[:1] for token in tokens)
        ]
        # A profile whose last name only contains the surname: RMP's
        # "Dugat-Wickliff" for the registrar's "Tanya V. Wickliff".
        surname_filters += [
            last_name.like(f"%{token}%") for token in tokens if len(token) >= 3
        ]
        name_filters = [or_(*surname_filters)]
        initials = given_initials(instructor_name)
        if initials:
            name_filters.append(or_(*(first_name.like(f"{i}%") for i in initials)))
        # Also profiles with the names swapped: first_name is the surname.
        swapped_filter = first_name == "".join(parsed.surname)
        prof_query = db.query(ProfessorDB)
        prof_query = prof_query.filter(ProfessorDB.university_id == university_id)
        prof_query = prof_query.filter(or_(and_(*name_filters), swapped_filter))
        pool = list(prof_query.all())
        names: dict[str, str] = {}
        swapped_names: dict[str, str] = {}
        candidates = []
        for candidate in pool:
            matched = profile_match_name(
                instructor_name, candidate.first_name, candidate.last_name
            )
            if matched:
                candidates.append(candidate)
                names[candidate.id] = matched[0]
                if matched[1]:
                    swapped_names[candidate.id] = matched[0]
        cache[pool_key] = {
            "pool": pool, "candidates": candidates, "names": names, "swapped": swapped_names,
        }
    entry = cache[pool_key]
    # The pool is shared by everyone with this name; rejections are per person.
    turned_down = set(rejected_ids)
    turned_down |= {(aliases or {}).get(pid, pid) for pid in rejected_ids}
    result = InstructorMatch(
        pool=entry["pool"],
        candidates=[
            c for c in entry["candidates"]
            if c.id not in turned_down and (aliases or {}).get(c.id, c.id) not in turned_down
        ] if turned_down else entry["candidates"],
        swapped_names=entry["swapped"],
    )
    if not result.candidates:
        result.reason = (
            "every name candidate was rejected in the admin console"
            if entry["candidates"]
            else "no profile at this school has a matching name"
        )
        return result

    # A name match is not an identity match: check whose teaching history
    # each profile's reviews fit.
    people_key = ("people", instructor_name, pidm, dept)
    if people_key not in cache:
        cache[people_key] = load_people(db, instructor_name, pidm, dept)
    instructor, namesakes = cache[people_key]
    if "profiles" not in entry:
        # Score RMP's own name split; a merge's canonical row may not be a
        # name candidate, so it brings its own.
        entry["profiles"] = [
            replace(
                profile,
                name=result.swapped_names[profile.id],
                swapped=True,
                needs_course=swapped_needs_course(
                    instructor_name, result.swapped_names[profile.id]
                ),
            )
            if profile.id in result.swapped_names
            else replace(
                profile,
                name=entry["names"].get(profile.id) or profile.match_name or profile.name,
            )
            for profile in load_profiles(db, entry["candidates"], aliases)
        ]
    result.profiles = (
        [p for p in entry["profiles"] if p.id not in turned_down]
        if turned_down
        else entry["profiles"]
    )
    chosen, result.rejected = explain_choice(
        instructor, course_code, result.profiles, namesakes, name_alone
    )
    if chosen is None:
        result.reason = "no name candidate's reviews identify this instructor"
        return result
    # The canonical row of a merge may not be a name candidate.
    result.professor = next((c for c in result.candidates if c.id == chosen.id), None)
    if result.professor is None:
        row_key = ("row", chosen.id)
        if row_key not in cache:
            cache[row_key] = db.get(ProfessorDB, chosen.id)
        result.professor = cache[row_key]
    return result


def _decided_match(
    db: Any,
    instructor_name: str,
    pidm: Optional[int],
    course_code: str,
    aliases: Optional[dict[str, str]],
    cache: Optional[dict[Any, Any]],
    links: "LoadedLinks",
) -> Optional[InstructorMatch]:
    """The match a confirmed decision dictates, or None to run the matcher.

    A confirmed link whose profile no longer exists falls back to the
    matcher (the admin console lists it under Conflicts).
    """
    from aggiermp.database.base import ProfessorDB

    if not parse_person_name(instructor_name).surname:
        return InstructorMatch(shown=False, reason="the registrar name has no surname")
    key = instructor_key(instructor_name, pidm)
    if key in links.no_profile:
        return InstructorMatch(reason="marked in the admin console as having no RMP profile")
    confirmed_ids = links.confirmed.get(key)
    if not confirmed_ids:
        return None
    aliases = aliases or {}
    professor_ids = tuple(dict.fromkeys(aliases.get(pid, pid) for pid in confirmed_ids))
    cache = {} if cache is None else cache
    professors = {}
    for professor_id in professor_ids:
        row_key = ("row", professor_id)
        if row_key not in cache:
            cache[row_key] = db.get(ProfessorDB, professor_id)
        if cache[row_key] is not None:
            professors[professor_id] = cache[row_key]
    if not professors:
        return None
    if len(professors) == 1:
        return InstructorMatch(professor=next(iter(professors.values())))

    evidence_key = ("confirmed_profiles", professor_ids)
    if evidence_key not in cache:
        cache[evidence_key] = load_profiles(db, professors.values(), aliases)
    course = normalize_course_code(course_code)
    # Prefer reviews for this course, then the profile with more ratings.
    # load_profiles preserves candidate order, so ties favor the latest link.
    best = max(cache[evidence_key], key=lambda p: (course in p.course_codes, p.num_ratings))
    return InstructorMatch(professor=professors[best.id])


def match_instructor_any_campus(
    db: Any,
    instructor_name: str,
    pidm: Optional[int],
    university_id: Optional[str],
    dept: str,
    course_code: str,
    aliases: Optional[dict[str, str]] = None,
    cache: Optional[dict[Any, Any]] = None,
    links: Optional["LoadedLinks"] = None,
) -> InstructorMatch:
    """match_instructor at the campus's RMP school, then at College Station's.

    Sections at locations with no RMP school (McAllen, Dallas, Fort Worth,
    Bryan, partner colleges) have university_id None, and some Galveston
    faculty are reviewed under College Station. Their profile is found at
    College Station, but only on review evidence, never on the name alone.

    ``links`` (load_instructor_links, loaded once per request) are a
    person's decisions: confirmed links take precedence, a confirmed "no
    profile" shows none, and rejected profiles are never picked.
    """
    from aggiermp.core.campuses import TEXAS_AM_ID

    rejected_ids: frozenset[str] = frozenset()
    if links:
        decided = _decided_match(db, instructor_name, pidm, course_code, aliases, cache, links)
        if decided is not None:
            return decided
        rejected_ids = links.rejected_ids(instructor_key(instructor_name, pidm))

    primary = None
    if university_id is not None:
        primary = match_instructor(
            db, instructor_name, pidm, university_id, dept, course_code, aliases, cache,
            rejected_ids=rejected_ids,
        )
        if primary.professor is not None or not primary.shown or university_id == TEXAS_AM_ID:
            return primary
    fallback = match_instructor(
        db, instructor_name, pidm, TEXAS_AM_ID, dept, course_code, aliases, cache,
        name_alone=False, rejected_ids=rejected_ids,
    )
    if fallback.professor is not None or primary is None:
        return fallback
    return primary


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


# Any two names in one nickname line are related (Larry/Lawrence, and also
# Christina/Christine): a "nickname" variant never merges.
_NICKNAMES: tuple[frozenset[str], ...] = tuple(
    formal | nicknames for formal, nicknames in NICKNAME_GROUPS
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
