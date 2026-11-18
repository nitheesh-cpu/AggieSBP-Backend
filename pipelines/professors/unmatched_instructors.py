"""
List instructors teaching this term whose course cards show no review profile.

Read-only: nothing is written to the database or Redis. For every instructor
with sections in the current term(s), the course page matcher
(aggiermp.core.professor_identity.match_instructor, the same function the
course professors endpoint uses) runs once per course they teach. An
instructor counts as matched when any of their courses gets a profile; the
rest are reported, sorted by impact (students, then sections).

With --rmp, each unmatched instructor is also searched on RateMyProfessors at
their school (one request per name, throttled, cached in a JSON file so a
rerun only searches new names) and classified:

  rmp_missing_from_db    RMP has a plausible profile that our professors table
                         lacks (an ingestion gap)
  rmp_in_db_not_matched  the RMP profile is in our table but the matcher
                         rejected or never considered it (a matching gap; the
                         reason is shown)
  not_on_rmp             no plausible RMP profile (near hits are listed)
  unchecked              --rmp was not used, or the search failed

RMP's search is fuzzy and returns one page of hits; the report includes a
manual search link for each instructor to double-check.

Usage on the VM (from /home/opc/backend; Postgres settings come from .env):

    # Fast run: matcher only, no RMP requests
    .venv/bin/python -m pipelines.professors.unmatched_instructors \\
        --out-html /home/opc/reports/unmatched.html \\
        --out-csv /home/opc/reports/unmatched.csv

    # Full run: also search RMP (about 1.5 s per new name; reruns reuse the cache)
    .venv/bin/python -m pipelines.professors.unmatched_instructors --rmp \\
        --rmp-cache /home/opc/reports/rmp_search_cache.json \\
        --out-html /home/opc/reports/unmatched.html \\
        --out-csv /home/opc/reports/unmatched.csv

    # Other options: --term 202711 (repeatable), --campus Galveston,
    # --rmp-delay 2.0, --rmp-max 200 (stop after 200 new searches)

Copy the report to your computer and open it in a browser:

    scp opc@<vm-host>:/home/opc/reports/unmatched.html .
"""

from __future__ import annotations

import argparse
import base64
import binascii
import csv
import html
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional
from urllib.parse import quote

project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from aggiermp.core import professor_identity
from aggiermp.core.campuses import campus_from_term_description, university_id_for_campus
from aggiermp.core.name_matching import _normalize_token, parse_person_name, professor_name_score
from aggiermp.core.professor_identity import (
    NAME_MATCH_THRESHOLD,
    InstructorMatch,
    load_aliases,
    match_instructor,
    profile_match_name,
)

logger = logging.getLogger("unmatched_instructors")

UNCHECKED = "unchecked"
RMP_MISSING = "rmp_missing_from_db"
RMP_IN_DB = "rmp_in_db_not_matched"
NOT_ON_RMP = "not_on_rmp"
CATEGORIES = (RMP_MISSING, RMP_IN_DB, NOT_ON_RMP, UNCHECKED)
CATEGORY_HELP = {
    RMP_MISSING: "RMP has a plausible profile that is not in our professors table (ingestion gap)",
    RMP_IN_DB: "the RMP profile is in our table but the matcher did not pick it (matching gap)",
    NOT_ON_RMP: "no plausible RMP profile at this school",
    UNCHECKED: "RMP not searched (run with --rmp)",
}

SITE_URL = "https://aggieschedulebuilderplus.vercel.app"
RMP_URL = "https://www.ratemyprofessors.com"
# Section columns that could hold enrollment; none exist yet (Nov 2026).
STUDENT_COLUMNS = ("enrollment", "actual_enrollment", "enrolled", "seats_taken")
MIN_RMP_DELAY = 1.0
MAX_CONSECUTIVE_RMP_FAILURES = 3


# ---------------------------------------------------------------------------
# Terms and instructors
# ---------------------------------------------------------------------------


def pick_current_terms(term_codes: Iterable[str], override: Optional[Iterable[str]] = None) -> list[str]:
    """The latest term per campus digit (the last digit of the code), or ``override``."""
    if override:
        return sorted({str(code).strip() for code in override if str(code).strip()})
    latest: dict[str, str] = {}
    for code in term_codes:
        code = str(code or "").strip()
        if not re.fullmatch(r"\d{6}", code):
            continue
        if code > latest.get(code[-1], ""):
            latest[code[-1]] = code
    return sorted(latest.values())


def course_code(dept: str, number: str) -> str:
    return re.sub(r"\s+", "", f"{dept}{number}").upper()


@dataclass
class Instructor:
    name: str
    pidm: Optional[int]
    campus: str
    university_id: str
    terms: set[str] = field(default_factory=set)
    # Course code -> (dept, title)
    courses: dict[str, tuple[str, str]] = field(default_factory=dict)
    sections: set[str] = field(default_factory=set)
    # Section id -> students, when the sections table has an enrollment column.
    section_students: dict[str, int] = field(default_factory=dict)
    matches: dict[str, InstructorMatch] = field(default_factory=dict)
    matched_id: Optional[str] = None
    category: str = UNCHECKED
    note: str = ""
    hits: list[dict[str, Any]] = field(default_factory=list)
    near_hits: list[dict[str, Any]] = field(default_factory=list)
    rejected: list[dict[str, str]] = field(default_factory=list)

    @property
    def students(self) -> Optional[int]:
        return sum(self.section_students.values()) if self.section_students else None

    @property
    def school_id(self) -> Optional[int]:
        return legacy_school_id(self.university_id)


def group_instructors(
    rows: Iterable[Any], term_campus: dict[str, Optional[str]], campus_filter: Optional[str] = None
) -> tuple[list[Instructor], int]:
    """One Instructor per (name, PIDM, campus), as the course page groups them.

    Returns the instructors and how many rows were skipped because their
    campus has no RMP school.
    """
    instructors: dict[tuple[str, Optional[int], str], Instructor] = {}
    skipped = 0
    wanted = (campus_filter or "").strip().casefold()
    for row in rows:
        name = (row.instructor_name or "").strip()
        if not name:
            continue
        campus = (row.campus or "").strip() or term_campus.get(row.term_code) or ""
        university_id = university_id_for_campus(campus)
        if wanted and campus.casefold() != wanted:
            continue
        if university_id is None:
            skipped += 1
            continue
        # Same key as the endpoint: the raw name, so the matcher sees what the page sees.
        key = (row.instructor_name, row.instructor_pidm, campus)
        instructor = instructors.get(key)
        if instructor is None:
            instructor = instructors[key] = Instructor(
                row.instructor_name, row.instructor_pidm, campus, university_id
            )
        instructor.terms.add(row.term_code)
        instructor.courses.setdefault(
            course_code(row.dept, row.course_number),
            (str(row.dept).upper(), (row.course_title or "").strip()),
        )
        instructor.sections.add(row.section_id)
        students = getattr(row, "students", None)
        if students is not None:
            instructor.section_students[row.section_id] = int(students)
    return list(instructors.values()), skipped


def match_courses(
    db: Any, instructor: Instructor, aliases: dict[str, str], cache: dict[Any, Any]
) -> None:
    """Run the course page matcher for each course; stop at the first match."""
    for code in sorted(instructor.courses):
        dept = instructor.courses[code][0]
        result = match_instructor(
            db, instructor.name, instructor.pidm, instructor.university_id,
            dept, code, aliases, cache,
        )
        instructor.matches[code] = result
        if result.professor is not None:
            instructor.matched_id = result.professor.id
            return


def combine_reasons(per_course: dict[str, str]) -> str:
    """One reason when every course agrees, else "CODE: reason; ..."."""
    distinct = set(per_course.values())
    if len(distinct) == 1:
        return distinct.pop()
    return "; ".join(f"{code}: {reason}" for code, reason in sorted(per_course.items()))


def why_unmatched(instructor: Instructor) -> str:
    return combine_reasons({code: m.reason for code, m in instructor.matches.items()})


def _display_name(row: Any) -> str:
    return f"{(row.first_name or '').strip()} {(row.last_name or '').strip()}".strip()


def rejected_candidates(instructor: Instructor, limit: int = 8) -> list[dict[str, str]]:
    """Profiles the matcher considered and turned down, with reasons.

    Name candidates first, then prefilter rows whose name came close.
    """
    reasons: dict[str, dict[str, str]] = {}
    names: dict[str, str] = {}
    for code, match in instructor.matches.items():
        for profile in match.profiles:
            names.setdefault(profile.id, profile.name)
            if profile.id in match.rejected:
                reasons.setdefault(profile.id, {})[code] = match.rejected[profile.id]
    result = [
        {"id": pid, "name": names[pid], "reason": combine_reasons(per_course)}
        for pid, per_course in reasons.items()
    ]
    near = []
    seen = set(names)
    for match in instructor.matches.values():
        candidate_ids = {c.id for c in match.candidates}
        for row in match.pool:
            if row.id in seen or row.id in candidate_ids:
                continue
            seen.add(row.id)
            score = professor_name_score(instructor.name, _display_name(row))
            if score > 0:
                near.append((score, {
                    "id": row.id,
                    "name": _display_name(row),
                    "reason": f"name check failed (score {score:.2f} < {NAME_MATCH_THRESHOLD})",
                }))
    near.sort(key=lambda item: -item[0])
    return (result + [item for _, item in near])[:limit]


def rejection_reason(row: Any, instructor: Instructor, aliases: dict[str, str]) -> str:
    """Why the matcher did not pick ``row`` (a ProfessorDB row) for any course."""
    if row.university_id != instructor.university_id:
        return f"our row is listed under another school ({row.university_id})"
    canonical = aliases.get(row.id, row.id)
    per_course: dict[str, str] = {}
    for code, match in instructor.matches.items():
        if not match.shown:
            per_course[code] = match.reason
        elif canonical in match.rejected:
            per_course[code] = match.rejected[canonical]
        elif row.id not in {c.id for c in match.pool}:
            per_course[code] = (
                "not fetched by the name prefilter (surname initial and first initial)"
            )
        elif row.id not in {c.id for c in match.candidates}:
            score = professor_name_score(instructor.name, _display_name(row))
            per_course[code] = f"name check failed (score {score:.2f} < {NAME_MATCH_THRESHOLD})"
        else:
            per_course[code] = match.reason or "not chosen"
    reason = combine_reasons(per_course)
    if canonical != row.id:
        reason = f"merged into {canonical}; {reason}"
    return reason


# ---------------------------------------------------------------------------
# RateMyProfessors
# ---------------------------------------------------------------------------


def legacy_school_id(university_id: Optional[str]) -> Optional[int]:
    """1003 for "U2Nob29sLTEwMDM=" (base64 of "School-1003")."""
    try:
        decoded = base64.b64decode(university_id or "", validate=True).decode()
    except (binascii.Error, UnicodeDecodeError):
        return None
    match = re.fullmatch(r"School-(\d+)", decoded)
    return int(match.group(1)) if match else None


def rmp_profile_url(legacy_id: Any) -> str:
    return f"{RMP_URL}/professor/{int(legacy_id)}"


def rmp_search_url(school_id: Optional[int], name: str) -> str:
    path = f"/search/professors/{int(school_id)}" if school_id else "/search/professors"
    return f"{RMP_URL}{path}?q={quote(search_text(name), safe='')}"


def site_professor_url(professor_id: str) -> str:
    return f"{SITE_URL}/professor/{quote(professor_id, safe='=')}"


def search_text(name: str) -> str:
    """Registrar name as "First [Middle] Last", without initials, for RMP's search."""
    name = (name or "").strip()
    if "," in name:
        surname, given = name.split(",", 1)
        name = f"{given} {surname}"
    tokens = [t for t in name.split() if len(re.sub(r"[^A-Za-z]", "", t)) > 1]
    return " ".join(tokens) or name


def _hit_name(hit: dict[str, Any]) -> str:
    return f"{(hit.get('firstName') or '').strip()} {(hit.get('lastName') or '').strip()}".strip()


def classify_hits(
    name: str, hits: Iterable[dict[str, Any]], near_limit: int = 5
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split RMP hits into plausible matches and near hits.

    Plausible uses the matcher's own name rule (profile_match_name, which
    also accepts RMP's swapped first/last names). Near hits share a surname
    part or score above zero.
    """
    surname = set(parse_person_name(name).surname)
    plausible, near = [], []
    for hit in hits:
        full = _hit_name(hit)
        matched = profile_match_name(name, hit.get("firstName"), hit.get("lastName"))
        score = professor_name_score(name, matched[0] if matched else full)
        item = {**hit, "name": full, "score": round(score, 2), "swapped": bool(matched and matched[1])}
        if matched:
            plausible.append(item)
            continue
        parsed = parse_person_name(full)
        tokens = set(parsed.surname) | {_normalize_token(t) for t in (hit.get("firstName") or "").split()}
        if score > 0 or surname & tokens:
            near.append(item)
    plausible.sort(key=lambda h: (-h["score"], -(h.get("numRatings") or 0)))
    near.sort(key=lambda h: (-h["score"], -(h.get("numRatings") or 0)))
    return plausible, near[:near_limit]


def classify(checked: bool, plausible: list[dict[str, Any]]) -> str:
    """Category for an unmatched instructor. Each plausible hit carries
    ``db_id`` (our professors.id, or None when the profile is missing).
    A missing profile wins over a rejected one: ingest first, then re-check."""
    if not checked:
        return UNCHECKED
    if not plausible:
        return NOT_ON_RMP
    if any(hit.get("db_id") is None for hit in plausible):
        return RMP_MISSING
    return RMP_IN_DB


HIT_FIELDS = ("id", "legacyId", "firstName", "lastName", "department", "numRatings", "avgRating")


class RmpSearcher:
    """Throttled, cached RMP name search. Never more than one request per
    ``delay`` seconds; gives up after a few consecutive failures."""

    def __init__(
        self,
        cache_path: Optional[Path],
        delay: float = 1.5,
        max_searches: Optional[int] = None,
        client_factory: Optional[Callable[[], Any]] = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.cache_path = cache_path
        self.delay = max(delay, MIN_RMP_DELAY)
        self.max_searches = max_searches
        self.client_factory = client_factory
        self.sleep = sleep
        self.clock = clock
        self.client: Any = None
        self.last_request: Optional[float] = None
        self.searches = 0
        self.failures = 0
        self.disabled = False
        self.searches_cache: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.cache_path or not self.cache_path.exists():
            return {}
        try:
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as error:
            logger.warning("Ignoring unreadable RMP cache %s: %s", self.cache_path, error)
            return {}
        return dict(data.get("searches") or {})

    def _save(self) -> None:
        if not self.cache_path:
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.cache_path.with_name(self.cache_path.name + ".tmp")
        temp.write_text(
            json.dumps({"version": 1, "searches": self.searches_cache}, indent=1, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temp, self.cache_path)

    @staticmethod
    def key(text: str, university_id: str) -> str:
        return f"{university_id}|{text.casefold()}"

    def _wait(self) -> None:
        if self.last_request is not None:
            remaining = self.delay - (self.clock() - self.last_request)
            if remaining > 0:
                self.sleep(remaining)

    def search(self, text: str, university_id: str) -> Optional[list[dict[str, Any]]]:
        """Hits for ``text`` at the school, or None when not searched."""
        key = self.key(text, university_id)
        if key in self.searches_cache:
            return list(self.searches_cache[key]["hits"])
        if self.disabled or (
            self.max_searches is not None and self.searches >= self.max_searches
        ):
            return None
        try:
            if self.client is None:
                if self.client_factory is None:
                    from pipelines.professors.scrapers import RMPReviewCollector

                    self.client_factory = RMPReviewCollector
                self._wait()
                self.client = self.client_factory()  # fetches RMP cookies: one request
                self.last_request = self.clock()
            self._wait()
            self.searches += 1
            raw = self.client.search_professors(text, university_id)
        except Exception as error:
            self.failures += 1
            logger.warning("RMP search failed for %r: %s", text, error)
            if self.failures >= MAX_CONSECUTIVE_RMP_FAILURES:
                logger.error("Stopping RMP searches after %d failures in a row", self.failures)
                self.disabled = True
            return None
        finally:
            self.last_request = self.clock()
        self.failures = 0
        hits = [{name: hit.get(name) for name in HIT_FIELDS} for hit in raw]
        self.searches_cache[key] = {
            "searched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "hits": hits,
        }
        self._save()
        return hits


def check_rmp(
    db: Any, instructors: list[Instructor], searcher: RmpSearcher, aliases: dict[str, str]
) -> None:
    """Search RMP for each unmatched instructor and classify them."""
    from aggiermp.database.base import ProfessorDB

    found: dict[int, list[dict[str, Any]]] = {}
    for index, instructor in enumerate(instructors, 1):
        hits = searcher.search(search_text(instructor.name), instructor.university_id)
        if index % 50 == 0:
            logger.info("RMP: %d/%d instructors checked", index, len(instructors))
        if hits is None:
            instructor.note = "RMP not searched (search failed, or --rmp-max reached)"
            continue
        instructor.hits, instructor.near_hits = classify_hits(instructor.name, hits)
        instructor.category = NOT_ON_RMP  # refined below once DB rows are known
        for hit in instructor.hits:
            if hit.get("legacyId") is not None:
                found.setdefault(int(hit["legacyId"]), []).append(hit)

    rows_by_legacy: dict[int, list[Any]] = {}
    legacy_ids = sorted(found)
    for start in range(0, len(legacy_ids), 500):
        batch = legacy_ids[start:start + 500]
        for row in db.query(ProfessorDB).filter(ProfessorDB.legacy_id.in_(batch)).all():
            rows_by_legacy.setdefault(int(row.legacy_id), []).append(row)

    for instructor in instructors:
        if instructor.category == UNCHECKED:
            continue
        for hit in instructor.hits:
            rows = rows_by_legacy.get(int(hit["legacyId"])) if hit.get("legacyId") else None
            row = None
            if rows:
                row = next((r for r in rows if r.university_id == instructor.university_id), rows[0])
            hit["db_id"] = row.id if row else None
            hit["db_reason"] = rejection_reason(row, instructor, aliases) if row else ""
        instructor.category = classify(True, instructor.hits)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def sort_key(instructor: Instructor) -> tuple[int, int, str]:
    return (-(instructor.students or 0), -len(instructor.sections), instructor.name.casefold())


def _hit_text(hit: dict[str, Any]) -> str:
    text = f"{hit['name']} ({hit.get('legacyId')}, {hit.get('numRatings') or 0} ratings, score {hit['score']:.2f})"
    if "db_id" in hit:
        text += f" [in our table as {hit['db_id']}: {hit['db_reason']}]" if hit["db_id"] else " [not in our table]"
    return text


CSV_FIELDS = (
    "name", "pidm", "campus", "terms", "courses", "sections", "students", "category",
    "why_unmatched", "rmp_hits", "near_hits", "rejected_candidates", "rmp_search_url",
)


def csv_row(instructor: Instructor) -> dict[str, Any]:
    return {
        "name": instructor.name,
        "pidm": instructor.pidm if instructor.pidm is not None else "",
        "campus": instructor.campus,
        "terms": " ".join(sorted(instructor.terms)),
        "courses": " ".join(sorted(instructor.courses)),
        "sections": len(instructor.sections),
        "students": instructor.students if instructor.students is not None else "",
        "category": instructor.category,
        "why_unmatched": why_unmatched(instructor),
        "rmp_hits": " | ".join(_hit_text(h) for h in instructor.hits),
        "near_hits": " | ".join(_hit_text(h) for h in instructor.near_hits),
        "rejected_candidates": " | ".join(
            f"{r['name']} ({r['id']}): {r['reason']}" for r in instructor.rejected
        ),
        "rmp_search_url": rmp_search_url(instructor.school_id, instructor.name),
    }


def write_csv(path: Path, instructors: list[Instructor]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for instructor in instructors:
            writer.writerow(csv_row(instructor))


def _e(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _link(url: str, text: str) -> str:
    return f'<a href="{_e(url)}" target="_blank" rel="noopener noreferrer">{_e(text)}</a>'


def _hit_html(hit: dict[str, Any]) -> str:
    label = f"{hit['name']} ({hit.get('department') or '-'}, {hit.get('numRatings') or 0} ratings)"
    link = _link(rmp_profile_url(hit["legacyId"]), label) if hit.get("legacyId") else _e(label)
    score = "score {:.2f}{}".format(hit["score"], " swapped" if hit.get("swapped") else "")
    parts = [link, f'<span class="dim">{_e(score)}</span>']
    if "db_id" in hit:
        if hit["db_id"]:
            parts.append(
                "in our table as " + _link(site_professor_url(hit["db_id"]), hit["db_id"])
                + f': <span class="why">{_e(hit["db_reason"])}</span>'
            )
        else:
            parts.append('<span class="missing">not in our table</span>')
    return "<li>" + " ".join(parts) + "</li>"


def _list(items: list[str]) -> str:
    return "<ul>" + "".join(items) + "</ul>" if items else '<span class="dim">none</span>'


def _row_html(instructor: Instructor) -> str:
    courses = "<br>".join(
        f"{_e(code)} <span class=\"dim\">{_e(title)}</span>"
        for code, (_, title) in sorted(instructor.courses.items())
    )
    rejected = [
        "<li>" + _link(site_professor_url(r["id"]), r["name"])
        + f' <span class="why">{_e(r["reason"])}</span></li>'
        for r in instructor.rejected
    ]
    students = instructor.students
    load = f"{len(instructor.sections)} section{'' if len(instructor.sections) == 1 else 's'}"
    if students is not None:
        load = f"{students} students<br>{load}"
    pidm = f'<br><span class="dim">PIDM {_e(instructor.pidm)}</span>' if instructor.pidm is not None else ""
    note = f'<br><span class="dim">{_e(instructor.note)}</span>' if instructor.note else ""
    return (
        "<tr>"
        f"<td><strong>{_e(instructor.name)}</strong>{pidm}</td>"
        f"<td>{_e(instructor.campus)}</td>"
        f"<td>{courses}</td>"
        f"<td class=\"num\">{load}</td>"
        f"<td><span class=\"cat cat-{_e(instructor.category)}\">{_e(instructor.category)}</span>{note}"
        f"<br><span class=\"why\">{_e(why_unmatched(instructor))}</span></td>"
        f"<td>{_list([_hit_html(h) for h in instructor.hits])}</td>"
        f"<td>{_list([_hit_html(h) for h in instructor.near_hits])}</td>"
        f"<td>{_list(rejected)}</td>"
        f"<td>{_link(rmp_search_url(instructor.school_id, instructor.name), 'Search RMP')}</td>"
        "</tr>"
    )


STYLE = """
:root { color-scheme: light dark; --bg: #fff; --fg: #1d1d1f; --dim: #6e6e73; --line: #d2d2d7;
  --head: #f5f5f7; --accent: #500000; --warn: #b3261e; }
@media (prefers-color-scheme: dark) { :root { --bg: #161617; --fg: #f5f5f7; --dim: #a1a1a6;
  --line: #3a3a3c; --head: #232325; --accent: #e3a3a3; --warn: #ff8a80; } }
body { margin: 0; padding: 16px; background: var(--bg); color: var(--fg);
  font: 14px/1.4 system-ui, -apple-system, "Segoe UI", sans-serif; }
h1 { font-size: 20px; margin: 0 0 4px; }
a { color: var(--accent); }
.dim { color: var(--dim); }
.why { color: var(--dim); font-size: 12px; }
.missing { color: var(--warn); }
.summary { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0; padding: 0; list-style: none; }
.summary button { font: inherit; color: inherit; background: var(--head); border: 1px solid var(--line);
  border-radius: 6px; padding: 6px 10px; cursor: pointer; text-align: left; }
.summary b { font-size: 18px; display: block; }
input[type=search] { font: inherit; padding: 6px 8px; width: min(420px, 100%); box-sizing: border-box;
  border: 1px solid var(--line); border-radius: 6px; background: var(--bg); color: var(--fg); }
.wrap { overflow-x: auto; margin-top: 12px; }
table { border-collapse: collapse; width: 100%; min-width: 1100px; }
th, td { border-bottom: 1px solid var(--line); padding: 6px 8px; vertical-align: top; text-align: left; }
th { background: var(--head); position: sticky; top: 0; }
td.num { white-space: nowrap; }
ul { margin: 0; padding-left: 16px; }
.cat { font-weight: 600; white-space: nowrap; }
.cat-rmp_missing_from_db, .cat-rmp_in_db_not_matched { color: var(--warn); }
"""

SCRIPT = """
const box = document.getElementById('q');
const shown = document.getElementById('shown');
function apply() {
  const q = box.value.trim().toLowerCase();
  let n = 0;
  for (const row of document.querySelectorAll('tbody tr')) {
    const hit = !q || row.textContent.toLowerCase().includes(q);
    row.hidden = !hit;
    if (hit) n++;
  }
  shown.textContent = n;
}
box.addEventListener('input', apply);
for (const b of document.querySelectorAll('[data-filter]')) {
  b.addEventListener('click', () => { box.value = b.dataset.filter; apply(); });
}
"""


def render_html(instructors: list[Instructor], summary: dict[str, Any]) -> str:
    counts = summary["categories"]
    cards = [
        f'<li><button type="button" data-filter=""><b>{_e(summary["unmatched"])}</b>'
        f'unmatched of {_e(summary["instructors"])}</button></li>'
    ] + [
        f'<li><button type="button" data-filter="{_e(name)}" title="{_e(CATEGORY_HELP[name])}">'
        f"<b>{_e(counts.get(name, 0))}</b>{_e(name)}</button></li>"
        for name in CATEGORIES
    ]
    header = (
        "<tr><th>Instructor</th><th>Campus</th><th>Courses</th><th>Load</th><th>Category / why</th>"
        "<th>Plausible RMP hits</th><th>Near hits</th><th>Rejected candidates</th><th>Manual</th></tr>"
    )
    rows = "".join(_row_html(i) for i in instructors)
    meta = (
        f"Terms {_e(', '.join(summary['terms']))} &middot; generated {_e(summary['generated_at'])}"
        f" &middot; {_e(summary['matched'])} matched"
    )
    if summary.get("skipped_rows"):
        meta += f" &middot; {_e(summary['skipped_rows'])} section rows skipped (campus without an RMP school)"
    return (
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        "<title>Unmatched instructors</title>"
        f"<style>{STYLE}</style></head><body>"
        "<h1>Instructors without a review profile</h1>"
        f"<div class=\"dim\">{meta}</div>"
        f"<ul class=\"summary\">{''.join(cards)}</ul>"
        "<input id=\"q\" type=\"search\" placeholder=\"Filter by name, course, category...\" "
        "aria-label=\"Filter rows\"> <span class=\"dim\"><span id=\"shown\">"
        f"{len(instructors)}</span> shown</span>"
        f"<div class=\"wrap\"><table><thead>{header}</thead><tbody>{rows}</tbody></table></div>"
        f"<script>{SCRIPT}</script></body></html>\n"
    )


def build_summary(
    total: int, unmatched: list[Instructor], terms: list[str], skipped_rows: int
) -> dict[str, Any]:
    categories: dict[str, int] = {}
    for instructor in unmatched:
        categories[instructor.category] = categories.get(instructor.category, 0) + 1
    return {
        "terms": list(terms),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "instructors": total,
        "matched": total - len(unmatched),
        "unmatched": len(unmatched),
        "categories": categories,
        "skipped_rows": skipped_rows,
    }


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def load_rows(db: Any, terms: list[str]) -> list[Any]:
    from aggiermp.database.base import SectionDB, SectionInstructorDB

    columns = [
        SectionInstructorDB.instructor_name,
        SectionInstructorDB.instructor_pidm,
        SectionInstructorDB.term_code,
        SectionDB.id.label("section_id"),
        SectionDB.dept,
        SectionDB.course_number,
        SectionDB.course_title,
        SectionDB.campus,
    ]
    student_column = next(
        (getattr(SectionDB, name) for name in STUDENT_COLUMNS if hasattr(SectionDB, name)), None
    )
    if student_column is not None:
        columns.append(student_column.label("students"))
    return (
        db.query(*columns)
        .join(SectionDB, SectionInstructorDB.section_id == SectionDB.id)
        .filter(SectionInstructorDB.term_code.in_(terms))
        .all()
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List current-term instructors whose course cards have no review profile"
    )
    parser.add_argument("--term", action="append", help="Term code (repeatable); default: latest per campus")
    parser.add_argument("--campus", help='Only this campus, e.g. "College Station" or Galveston')
    parser.add_argument("--rmp", action="store_true", help="Search RMP for each unmatched instructor")
    parser.add_argument(
        "--rmp-cache", type=Path, default=Path.home() / "unmatched_rmp_cache.json",
        help="JSON file of earlier RMP searches (default: ~/unmatched_rmp_cache.json)",
    )
    parser.add_argument("--rmp-delay", type=float, default=1.5, help="Seconds between RMP requests (min 1)")
    parser.add_argument("--rmp-max", type=int, help="Stop after this many new RMP searches")
    parser.add_argument("--out-csv", type=Path, help="Write the report as CSV")
    parser.add_argument("--out-html", type=Path, help="Write the report as a self-contained HTML page")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from aggiermp.database.base import SectionInstructorDB, TermDB, get_session

    session = get_session()
    original_valid_subjects = professor_identity._valid_subjects
    try:
        available = [row[0] for row in session.query(SectionInstructorDB.term_code).distinct().all()]
        terms = pick_current_terms(available, args.term)
        if not terms:
            print("No terms with instructors found.")
            return
        term_campus = {
            code: campus_from_term_description(desc)
            for code, desc in session.query(TermDB.term_code, TermDB.term_desc)
            .filter(TermDB.term_code.in_(terms))
            .all()
        }
        logger.info("Terms: %s", ", ".join(terms))
        instructors, skipped_rows = group_instructors(load_rows(session, terms), term_campus, args.campus)
        logger.info("%d instructors to check", len(instructors))

        # Read-only run: the subject list cannot change underneath it, so
        # load it once instead of once per instructor.
        valid = original_valid_subjects(session)
        professor_identity._valid_subjects = lambda db: valid
        aliases = load_aliases(session)
        cache: dict[Any, Any] = {}
        for index, instructor in enumerate(instructors, 1):
            match_courses(session, instructor, aliases, cache)
            if index % 200 == 0:
                logger.info("Matched %d/%d instructors", index, len(instructors))
        unmatched = sorted((i for i in instructors if i.matched_id is None), key=sort_key)
        for instructor in unmatched:
            instructor.rejected = rejected_candidates(instructor)

        if args.rmp:
            searcher = RmpSearcher(args.rmp_cache, args.rmp_delay, args.rmp_max)
            check_rmp(session, unmatched, searcher, aliases)
            logger.info("RMP: %d new searches, cache %s", searcher.searches, args.rmp_cache)

        summary = build_summary(len(instructors), unmatched, terms, skipped_rows)
        if args.out_csv:
            write_csv(args.out_csv, unmatched)
        if args.out_html:
            args.out_html.parent.mkdir(parents=True, exist_ok=True)
            args.out_html.write_text(render_html(unmatched, summary), encoding="utf-8")

        print(f"Terms: {', '.join(terms)}")
        print(f"Instructors: {summary['instructors']}  matched: {summary['matched']}  "
              f"unmatched: {summary['unmatched']}")
        for name in CATEGORIES:
            if summary["categories"].get(name):
                print(f"  {name}: {summary['categories'][name]}")
        if skipped_rows:
            print(f"Skipped {skipped_rows} section rows at campuses without an RMP school")
        for path in (args.out_csv, args.out_html):
            if path:
                print(f"Wrote {path}")
    finally:
        professor_identity._valid_subjects = original_valid_subjects
        session.close()


if __name__ == "__main__":
    main()
