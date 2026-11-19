"""Reviewed links between registrar instructors and RateMyProfessors profiles.

The course page matcher (professor_identity) attaches a profile only on
evidence. Candidates that rest on an assumption (a middle initial standing in
for the RMP first name, a swapped RMP name that fits only through an initial,
a fuzzy RMP search hit) are stored in ``instructor_profile_links`` as
proposals by pipelines/professors/unmatched_instructors.py --write-proposals.
A person decides in the admin console (routers/admin_links.py):

* confirmed links: the course page chooses among those profiles;
* confirmed "no profile" (professor_id NULL): the course page shows none;
* rejected pair: the matcher never picks that profile for that instructor.

Shared by the matcher (load_instructor_links), the report (plan_proposals)
and the admin API (list_instructor_links, change_links). Decisions are made
in pure ``plan_*`` functions that return row operations, so they can be
tested without a database; ``apply_ops`` runs them.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import quote

from .instructor_ai_report import attach_ai_reviews, instructor_ai_priority

TABLE = "instructor_profile_links"
KINDS = ("plausible", "near", "rejected", "middle_initial", "swapped", "manual")
STATUSES = ("proposed", "confirmed", "rejected")
TABS = ("proposed", "confirmed", "rejected", "conflict")
MISSING_TABLE = (
    "instructor_profile_links does not exist yet: run migration 005_instructor_links.sql"
)

# When one profile turns up as several kinds of candidate, the row keeps the
# most telling one: a name the matcher's rule accepts, then the assumption
# behind it, then what the matcher said, then a loose RMP hit.
KIND_PRIORITY = {
    "manual": 0, "plausible": 1, "middle_initial": 2, "swapped": 3, "rejected": 4, "near": 5,
}

RMP_URL = "https://www.ratemyprofessors.com"
COLLEGE_STATION_SCHOOL = 1003
GALVESTON_SCHOOL = 1006


# ---------------------------------------------------------------------------
# Keys and URLs
# ---------------------------------------------------------------------------


def instructor_key(name: Optional[str], pidm: Optional[int]) -> str:
    """"pidm:<pidm>", or "name:<registrar name>" for a section without a PIDM."""
    if pidm is not None:
        return f"pidm:{int(pidm)}"
    return f"name:{(name or '').strip()}"


def search_text(name: str) -> str:
    """Registrar name as "First [Middle] Last", without initials, for RMP's search."""
    name = (name or "").strip()
    if "," in name:
        surname, given = name.split(",", 1)
        name = f"{given} {surname}"
    tokens = [t for t in name.split() if len(re.sub(r"[^A-Za-z]", "", t)) > 1]
    return " ".join(tokens) or name


def school_for_campus(campus: Optional[str]) -> int:
    """RMP school to search: Galveston's for Galveston, else College Station's
    (locations without an RMP school are reviewed under College Station)."""
    names = {part.strip().casefold() for part in (campus or "").split(",")}
    return GALVESTON_SCHOOL if names == {"galveston"} else COLLEGE_STATION_SCHOOL


def rmp_search_url(name: str, campus: Optional[str]) -> str:
    school = school_for_campus(campus)
    return f"{RMP_URL}/search/professors/{school}?q={quote(search_text(name), safe='')}"


# ---------------------------------------------------------------------------
# What the course page matcher reads
# ---------------------------------------------------------------------------


@dataclass
class LoadedLinks:
    """Decisions the matcher applies, loaded once per request."""

    # instructor key -> confirmed professor ids, most recent first
    confirmed: Dict[str, List[str]] = field(default_factory=dict)
    # instructor keys confirmed as having no RMP profile
    no_profile: Set[str] = field(default_factory=set)
    # instructor key -> professor ids rejected for that instructor
    rejected: Dict[str, Set[str]] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return bool(self.confirmed or self.no_profile or self.rejected)

    def rejected_ids(self, key: str) -> frozenset:
        return frozenset(self.rejected.get(key, ()))

    def rejected_only(self) -> "LoadedLinks":
        """The rejected pairs alone: what the matcher would pick on its own,
        without the confirmed decisions overriding it."""
        return LoadedLinks(rejected=self.rejected)


def links_table_exists(db: Any) -> bool:
    """Checked up front instead of catching a failed SELECT, since rolling
    back after one would also discard the caller's uncommitted work."""
    from sqlalchemy import text

    try:
        found = db.execute(text(f"SELECT to_regclass('{TABLE}')")).scalar()
    except Exception:
        return False
    # The table name, or NULL when missing. Anything else (a Mock session in
    # tests) counts as missing.
    return isinstance(found, str)


def load_instructor_links(db: Any) -> LoadedLinks:
    """Confirmed links, "no profile" keys and rejected pairs; empty when the
    table does not exist. Proposed rows never affect the course page."""
    from sqlalchemy import text

    links = LoadedLinks()
    if not links_table_exists(db):
        return links
    rows = db.execute(
        text(
            f"SELECT instructor_key, professor_id, status FROM {TABLE} "
            "WHERE status IN ('confirmed', 'rejected') "
            "ORDER BY reviewed_at DESC NULLS LAST, id DESC"
        )
    ).fetchall()
    for key, professor_id, status in rows:
        if status == "rejected":
            if professor_id:
                links.rejected.setdefault(key, set()).add(str(professor_id))
        elif professor_id:
            links.confirmed.setdefault(key, []).append(str(professor_id))
        else:
            links.no_profile.add(key)
    return links


# ---------------------------------------------------------------------------
# Rows and operations
# ---------------------------------------------------------------------------


@dataclass
class LinkRow:
    id: Optional[int]
    instructor_key: str
    instructor_name: str
    instructor_pidm: Optional[int] = None
    campus: Optional[str] = None
    courses: List[str] = field(default_factory=list)
    sections: Optional[int] = None
    professor_id: Optional[str] = None
    rmp_legacy_id: Optional[int] = None
    kind: str = "manual"
    score: Optional[float] = None
    reason: Optional[str] = None
    evidence: Dict[str, Any] = field(default_factory=dict)
    status: str = "proposed"
    source: str = "report"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    reviewed_at: Optional[datetime] = None


@dataclass
class Op:
    """One row change: insert ``row``, update ``fields`` of row ``row_id``, or delete it."""

    action: str  # "insert" | "update" | "delete"
    row_id: Optional[int] = None
    fields: Dict[str, Any] = field(default_factory=dict)
    row: Optional[LinkRow] = None


INSERT_COLUMNS = (
    "instructor_key", "instructor_name", "instructor_pidm", "campus", "courses", "sections",
    "professor_id", "rmp_legacy_id", "kind", "score", "reason", "evidence", "status",
    "source", "reviewed_at",
)
UPDATABLE = frozenset(INSERT_COLUMNS) - {"instructor_key", "professor_id", "source"}


def _param(column: str) -> str:
    if column == "courses":
        return "CAST(:courses AS text[])"
    if column == "evidence":
        return "CAST(:evidence AS jsonb)"
    return f":{column}"


def _value(column: str, value: Any) -> Any:
    if column == "evidence":
        return json.dumps(value or {}, sort_keys=True)
    if column == "courses":
        return list(value or [])
    return value


def apply_ops(db: Any, ops: Sequence[Op], commit: bool = True) -> None:
    from sqlalchemy import text

    for op in ops:
        if op.action == "insert":
            assert op.row is not None
            values = {c: _value(c, getattr(op.row, c)) for c in INSERT_COLUMNS}
            db.execute(
                text(
                    f"INSERT INTO {TABLE} ({', '.join(INSERT_COLUMNS)}) VALUES "
                    f"({', '.join(_param(c) for c in INSERT_COLUMNS)}) "
                    "ON CONFLICT (instructor_key, (COALESCE(professor_id, ''))) DO NOTHING"
                ),
                values,
            )
        elif op.action == "update":
            unknown = set(op.fields) - UPDATABLE
            if unknown:
                raise ValueError(f"cannot update {sorted(unknown)}")
            if not op.fields:
                continue
            sets = ", ".join(f"{c} = {_param(c)}" for c in sorted(op.fields))
            values = {c: _value(c, v) for c, v in op.fields.items()}
            values["row_id"] = op.row_id
            db.execute(
                text(f"UPDATE {TABLE} SET {sets}, updated_at = now() WHERE id = :row_id"),
                values,
            )
        elif op.action == "delete":
            db.execute(text(f"DELETE FROM {TABLE} WHERE id = :row_id"), {"row_id": op.row_id})
        else:
            raise ValueError(f"unknown action {op.action!r}")
    if commit:
        db.commit()


ROW_COLUMNS = (
    "id", "instructor_key", "instructor_name", "instructor_pidm", "campus", "courses",
    "sections", "professor_id", "rmp_legacy_id", "kind", "score", "reason", "evidence",
    "status", "source", "created_at", "updated_at", "reviewed_at",
)


def _row(mapping: Any) -> LinkRow:
    values = {c: mapping[c] for c in ROW_COLUMNS}
    evidence = values["evidence"]
    if isinstance(evidence, str):
        evidence = json.loads(evidence or "{}")
    values["evidence"] = dict(evidence or {})
    values["courses"] = list(values["courses"] or [])
    return LinkRow(**values)


def fetch_rows(db: Any, keys: Optional[Iterable[str]] = None) -> List[LinkRow]:
    """Stored rows, all or for these instructor keys."""
    from sqlalchemy import text

    columns = ", ".join(ROW_COLUMNS)
    if keys is None:
        result = db.execute(text(f"SELECT {columns} FROM {TABLE}"))
    else:
        result = db.execute(
            text(f"SELECT {columns} FROM {TABLE} WHERE instructor_key = ANY(:keys)"),
            {"keys": list(keys)},
        )
    return [_row(m) for m in result.mappings().all()]


def fetch_professors(db: Any, ids: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    """Our professors rows for these ids: name, department, ratings."""
    from sqlalchemy import text

    id_list = sorted({i for i in ids if i})
    if not id_list:
        return {}
    rows = db.execute(
        text(
            "SELECT id, legacy_id, first_name, last_name, department, num_ratings, avg_rating "
            "FROM professors WHERE id = ANY(:ids)"
        ),
        {"ids": id_list},
    ).mappings().all()
    return {
        row["id"]: {
            "name": " ".join(
                p.strip() for p in (row["first_name"], row["last_name"]) if p and p.strip()
            ),
            "legacyId": row["legacy_id"],
            "department": row["department"],
            "numRatings": row["num_ratings"],
            "avgRating": row["avg_rating"],
        }
        for row in rows
    }


# ---------------------------------------------------------------------------
# Listing and grouping (the admin console)
# ---------------------------------------------------------------------------

_STATUS_ORDER = {"confirmed": 0, "proposed": 1, "rejected": 2}


def _iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _latest(row: LinkRow) -> datetime:
    floor = datetime.min.replace(tzinfo=timezone.utc)
    stamps = [
        t if t.tzinfo else t.replace(tzinfo=timezone.utc)
        for t in (row.updated_at, row.reviewed_at, row.created_at)
        if t is not None
    ]
    return max(stamps, default=floor)


def _candidate(row: LinkRow, professors: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    professor = professors.get(row.professor_id or "")
    evidence = row.evidence or {}

    def pick(name: str) -> Any:
        if professor is not None and professor.get(name) is not None:
            return professor[name]
        return evidence.get(name)

    return {
        "professorId": row.professor_id,
        "rmpLegacyId": row.rmp_legacy_id or pick("legacyId"),
        "name": (professor or {}).get("name") or evidence.get("rmpName") or "",
        "department": pick("department"),
        "numRatings": pick("numRatings"),
        "avgRating": pick("avgRating"),
        "kind": row.kind,
        "score": row.score,
        "reason": row.reason,
        "inDatabase": professor is not None,
        "status": row.status,
        "courseOverlap": list(evidence.get("courseOverlap") or []),
        "reviewedCourses": list(evidence.get("reviewedCourses") or []),
    }


def build_instructors(
    rows: Sequence[LinkRow], professors: Dict[str, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """One InstructorLinks object per instructor key (see the API contract),
    with its tab and, in the Conflicts tab, why it is there."""
    by_key: Dict[str, List[LinkRow]] = {}
    for row in rows:
        by_key.setdefault(row.instructor_key, []).append(row)
    names = {
        key: max(items, key=_latest).instructor_name for key, items in by_key.items()
    }
    confirmed_for: Dict[str, Set[str]] = {}
    for row in rows:
        if row.status == "confirmed" and row.professor_id:
            confirmed_for.setdefault(row.professor_id, set()).add(row.instructor_key)

    result = []
    for key, items in by_key.items():
        info = max(items, key=_latest)
        confirmed = sorted(
            (r for r in items if r.status == "confirmed"), key=_latest, reverse=True
        )
        linked_ids = [r.professor_id for r in confirmed if r.professor_id]
        no_profile = any(r.professor_id is None for r in confirmed)
        proposed = [r for r in items if r.status == "proposed" and r.professor_id]
        problems = []
        if confirmed:
            if no_profile and linked_ids:
                problems.append("marked as having no RMP profile and also linked to a profile")
            for professor_id in linked_ids:
                if professor_id not in professors:
                    problems.append(
                        f"the linked profile {professor_id} is not in our professors "
                        "table (removed, or not imported yet)"
                    )
                others = sorted(
                    names[k] for k in confirmed_for[professor_id] - {key}
                )
                if others:
                    problems.append(
                        f"the linked profile {professor_id} is also confirmed for " + ", ".join(others)
                    )
            if proposed:
                problems.append(
                    f"{len(proposed)} new candidate{'s' if len(proposed) != 1 else ''} "
                    "since the decision"
                )
            tab = "conflict" if problems else "confirmed"
        elif proposed:
            tab = "proposed"
        else:
            tab = "rejected"

        candidates = sorted(
            (r for r in items if r.professor_id),
            key=lambda r: (
                _STATUS_ORDER.get(r.status, 9),
                -len((r.evidence or {}).get("courseOverlap") or []),
                KIND_PRIORITY.get(r.kind, 9),
                -(r.score or 0),
                r.professor_id or "",
            ),
        )
        result.append({
            "key": key,
            "name": info.instructor_name,
            "pidm": info.instructor_pidm,
            "campus": info.campus,
            "courses": sorted(info.courses or []),
            "sections": info.sections,
            "tab": tab,
            "linkedProfessorId": linked_ids[0] if linked_ids else None,
            "linkedProfessorIds": linked_ids,
            "noProfile": no_profile,
            "conflict": "; ".join(problems) or None,
            "rmpSearchUrl": rmp_search_url(info.instructor_name, info.campus),
            "updatedAt": _iso(max(_latest(r) for r in items)),
            "candidates": [_candidate(r, professors) for r in candidates],
        })
    return result


def _matches(instructor: Dict[str, Any], q: str) -> bool:
    if not q:
        return True
    needle = q.casefold()
    haystack = [instructor["name"], instructor["key"], *instructor["courses"]]
    haystack += [c["name"] for c in instructor["candidates"]]
    return any(needle in str(text).casefold() for text in haystack if text)


def load_spring_section_counts(db: Any) -> Dict[str, int]:
    """Count distinct sections across campuses in the latest loaded Spring."""
    from sqlalchemy import text

    rows = db.execute(text("""
        WITH spring_terms AS (
            SELECT term_code, substr(term_code, 1, 4) AS term_year
            FROM terms
            WHERE lower(term_desc) LIKE 'spring %'
        ), latest_spring AS (
            SELECT max(term_year) AS term_year
            FROM spring_terms AS t
            WHERE EXISTS (SELECT 1 FROM sections AS s WHERE s.term_code = t.term_code)
        )
        SELECT CASE WHEN si.instructor_pidm IS NOT NULL
                    THEN 'pidm:' || CAST(si.instructor_pidm AS text)
                    ELSE 'name:' || trim(si.instructor_name)
               END AS instructor_key,
               count(DISTINCT si.section_id) AS section_count
        FROM section_instructors AS si
        JOIN spring_terms USING (term_code)
        JOIN latest_spring USING (term_year)
        GROUP BY instructor_key
    """))
    return dict(rows.all())


def sort_instructors(
    instructors: List[Dict[str, Any]], spring_sections: Dict[str, int],
) -> List[Dict[str, Any]]:
    return sorted(
        instructors,
        key=lambda i: (
            -spring_sections.get(i["key"], 0), -(i["sections"] or 0),
            i["name"].casefold(), i["key"],
        ),
    )


def select_tab(
    instructors: List[Dict[str, Any]],
    tab: str = "proposed",
    q: str = "",
    limit: int = 200,
    offset: int = 0,
    spring_sections: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """The GET /admin/instructor-links response. Counts follow the name filter."""
    filtered = [i for i in instructors if _matches(i, (q or "").strip())]
    counts = {name: 0 for name in TABS}
    for instructor in filtered:
        counts[instructor["tab"]] += 1
    in_tab = sort_instructors([i for i in filtered if i["tab"] == tab], spring_sections or {})
    if tab in {"proposed", "conflict"}:
        in_tab.sort(key=instructor_ai_priority)
    return {
        "instructors": in_tab[offset:offset + limit],
        "counts": counts,
        "total": len(in_tab),
    }


def load_instructors(db: Any) -> List[Dict[str, Any]]:
    rows = fetch_rows(db)
    return build_instructors(rows, fetch_professors(db, (r.professor_id for r in rows)))


def list_instructor_links(
    db: Any, tab: str = "proposed", q: str = "", limit: int = 200, offset: int = 0,
    ai_reviews: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    instructors = load_instructors(db)
    if ai_reviews:
        attach_ai_reviews(instructors, ai_reviews)
    return select_tab(instructors, tab, q, limit, offset, load_spring_section_counts(db))


# ---------------------------------------------------------------------------
# Admin decisions
# ---------------------------------------------------------------------------


class LinkError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def _metadata(items: Sequence[LinkRow]) -> Dict[str, Any]:
    info = max(items, key=_latest)
    return {
        "instructor_key": info.instructor_key,
        "instructor_name": info.instructor_name,
        "instructor_pidm": info.instructor_pidm,
        "campus": info.campus,
        "courses": list(info.courses or []),
        "sections": info.sections,
    }


def _reject_others(
    items: Sequence[LinkRow], keep: Set[Optional[str]], now: datetime,
) -> List[Op]:
    """A new decision turns every other open candidate and decision down; a
    previous "no profile" row is removed."""
    ops = []
    for row in items:
        if row.professor_id in keep:
            continue
        if row.professor_id is None:
            ops.append(Op("delete", row.id))
        elif row.status in ("proposed", "confirmed"):
            ops.append(Op("update", row.id, {"status": "rejected", "reviewed_at": now}))
    return ops


def plan_links(
    items: Sequence[LinkRow],
    professor_ids: Sequence[str],
    now: datetime,
    professors: Dict[str, Dict[str, Any]],
) -> List[Op]:
    """Confirm the chosen profiles and resolve the remaining proposals together."""
    if not items:
        raise LinkError(404, "unknown instructor key")
    by_id = {r.professor_id: r for r in items if r.professor_id is not None}
    selected = dict.fromkeys(professor_ids)
    if not selected:
        raise LinkError(422, "select at least one professor")
    for pid in selected:
        if pid not in by_id and pid not in professors:
            raise LinkError(404, f"unknown professor {pid}")

    # When only reconfirming existing links, attach any new rejections to a
    # decision's existing timestamp so undo can still find them.
    existing_stamps = [
        by_id[pid].reviewed_at for pid in selected
        if pid in by_id and by_id[pid].status == "confirmed" and by_id[pid].reviewed_at is not None
    ]
    if len(existing_stamps) == len(selected):
        now = max(existing_stamps)
    keep: Set[Optional[str]] = set(selected)
    ops = _reject_others(items, keep, now)
    for pid in selected:
        target = by_id.get(pid)
        if target is None:
            professor = professors[pid]
            evidence = {
                "rmpName": professor.get("name"),
                "department": professor.get("department"),
                "numRatings": professor.get("numRatings"),
                "avgRating": professor.get("avgRating"),
            }
            ops.append(Op("insert", row=LinkRow(
                id=None, **_metadata(items), professor_id=pid,
                rmp_legacy_id=professor.get("legacyId"), kind="manual",
                reason="linked by hand in the admin console", evidence=evidence,
                status="confirmed", source="admin", reviewed_at=now,
            )))
        elif target.status != "confirmed" or target.reviewed_at is None:
            ops.append(Op("update", target.id, {"status": "confirmed", "reviewed_at": now}))
    return ops


def plan_no_profile(items: Sequence[LinkRow], now: datetime) -> List[Op]:
    """Confirm that the instructor has no RMP profile."""
    if not items:
        raise LinkError(404, "unknown instructor key")
    current = next((r for r in items if r.professor_id is None), None)
    if current is not None and current.reviewed_at is not None:
        now = current.reviewed_at  # Keep the timestamp used by undo.
    ops = _reject_others(items, {None}, now)
    if current is None:
        ops.append(Op("insert", row=LinkRow(
            id=None, **_metadata(items), professor_id=None, kind="manual",
            reason="no RMP profile (admin console)", status="confirmed", source="admin",
            reviewed_at=now,
        )))
    elif current.reviewed_at is None:
        ops.append(Op("update", current.id, {"status": "confirmed", "reviewed_at": now}))
    return ops


def plan_reject(items: Sequence[LinkRow], professor_ids: Iterable[str], now: datetime) -> List[Op]:
    """Turn these candidates down (a confirmed link among them is undone)."""
    if not items:
        raise LinkError(404, "unknown instructor key")
    by_id = {r.professor_id: r for r in items if r.professor_id}
    wanted = list(dict.fromkeys(professor_ids))
    unknown = [pid for pid in wanted if pid not in by_id]
    if unknown:
        raise LinkError(404, f"not a candidate of this instructor: {', '.join(unknown)}")
    return [
        Op("update", by_id[pid].id, {"status": "rejected", "reviewed_at": now})
        for pid in wanted
        if by_id[pid].status != "rejected"
    ]


def plan_undo(items: Sequence[LinkRow]) -> List[Op]:
    """Put a confirmed decision back to proposed, with the candidates it
    turned down (rejected in the same action, so with the same reviewed_at).

    Without a confirmed decision, every rejected candidate becomes proposed
    again (the Rejected tab's undo).
    """
    if not items:
        raise LinkError(404, "unknown instructor key")
    reopen = {"status": "proposed", "reviewed_at": None}
    confirmed = [r for r in items if r.status == "confirmed"]
    if not confirmed:
        return [Op("update", r.id, dict(reopen)) for r in items if r.status == "rejected"]
    stamps = {r.reviewed_at for r in confirmed if r.reviewed_at is not None}
    ops = []
    for row in items:
        if row.status == "confirmed":
            ops.append(Op("delete", row.id) if row.professor_id is None else Op("update", row.id, dict(reopen)))
        elif row.status == "rejected" and row.reviewed_at in stamps:
            ops.append(Op("update", row.id, dict(reopen)))
    return ops


def change_links(
    db: Any,
    action: str,
    key: str,
    professor_id: Optional[str] = None,
    professor_ids: Sequence[str] = (),
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Apply an admin decision; returns the instructor afterwards (InstructorLinks)."""
    from sqlalchemy import text

    # Serialize decisions for this instructor, including inserts for manual links.
    db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"{TABLE}:{key}"},
    )
    now = now or datetime.now(timezone.utc)
    items = fetch_rows(db, [key])
    if not items:
        raise LinkError(404, "unknown instructor key")
    if action == "link":
        if professor_id is not None:
            # Legacy single-profile requests replace the previous decision.
            selected = [professor_id]
        else:
            selected = [
                r.professor_id for r in items if r.status == "confirmed" and r.professor_id
            ]
            selected.extend(professor_ids)
        existing_ids = {r.professor_id for r in items}
        professors = fetch_professors(db, (pid for pid in selected if pid not in existing_ids))
        ops = plan_links(items, selected, now, professors)
    elif action == "no-profile":
        ops = plan_no_profile(items, now)
    elif action == "reject":
        ops = plan_reject(items, professor_ids, now)
    elif action == "reject-all":
        pending_ids = (r.professor_id for r in items if r.status == "proposed" and r.professor_id)
        ops = plan_reject(items, pending_ids, now)
    elif action == "undo":
        ops = plan_undo(items)
    else:
        raise ValueError(f"unknown action {action!r}")
    apply_ops(db, ops, commit=False)
    instructor = next((i for i in load_instructors(db) if i["key"] == key), None)
    if instructor is None:
        raise LinkError(404, "unknown instructor key")
    db.commit()
    return instructor


# ---------------------------------------------------------------------------
# Proposals from the unmatched-instructors report
# ---------------------------------------------------------------------------


@dataclass
class Proposal:
    """One candidate profile for one instructor, as the report found it."""

    key: str
    name: str
    pidm: Optional[int]
    campus: Optional[str]
    courses: List[str]
    sections: Optional[int]
    professor_id: str
    rmp_legacy_id: Optional[int]
    kind: str
    score: Optional[float]
    reason: Optional[str]
    evidence: Dict[str, Any] = field(default_factory=dict)

    def as_row(self) -> LinkRow:
        return LinkRow(
            id=None, instructor_key=self.key, instructor_name=self.name,
            instructor_pidm=self.pidm, campus=self.campus, courses=list(self.courses),
            sections=self.sections, professor_id=self.professor_id,
            rmp_legacy_id=self.rmp_legacy_id, kind=self.kind, score=self.score,
            reason=self.reason, evidence=dict(self.evidence), status="proposed", source="report",
        )


def plan_proposals(
    existing: Sequence[LinkRow],
    proposals: Sequence[Proposal],
    auto_matched: Iterable[str] = (),
) -> Tuple[List[Op], Dict[str, int]]:
    """Row operations that store the report's candidates.

    * A pair already confirmed or rejected is never proposed again.
    * An instructor with a confirmed decision is left alone, except that a
      candidate never stored before is added as proposed: the admin
      console then shows the instructor under Conflicts.
    * Other proposed rows are refreshed with the latest evidence.
    * ``auto_matched`` keys (the matcher now attaches a profile by itself)
      lose their still-proposed rows, unless a decision exists.
    """
    by_key: Dict[str, List[LinkRow]] = {}
    for row in existing:
        by_key.setdefault(row.instructor_key, []).append(row)
    decided = {
        key for key, rows in by_key.items() if any(r.status == "confirmed" for r in rows)
    }
    counts = {"inserted": 0, "updated": 0, "deleted": 0, "unchanged": 0, "conflicts": 0}
    ops: List[Op] = []
    seen: Set[Tuple[str, str]] = set()
    conflict_keys: Set[str] = set()
    for proposal in proposals:
        pair = (proposal.key, proposal.professor_id)
        if pair in seen or not proposal.professor_id:
            continue
        seen.add(pair)
        current = next(
            (r for r in by_key.get(proposal.key, []) if r.professor_id == proposal.professor_id),
            None,
        )
        if current is None:
            ops.append(Op("insert", row=proposal.as_row()))
            counts["inserted"] += 1
            if proposal.key in decided:
                conflict_keys.add(proposal.key)
            continue
        if current.status != "proposed" or proposal.key in decided:
            counts["unchanged"] += 1
            continue
        fields = {
            "instructor_name": proposal.name,
            "instructor_pidm": proposal.pidm,
            "campus": proposal.campus,
            "courses": list(proposal.courses),
            "sections": proposal.sections,
            "rmp_legacy_id": proposal.rmp_legacy_id,
            "kind": proposal.kind,
            "score": proposal.score,
            "reason": proposal.reason,
            "evidence": dict(proposal.evidence),
        }
        changed = {
            name: value for name, value in fields.items()
            if getattr(current, name) != value
        }
        if changed:
            ops.append(Op("update", current.id, changed))
            counts["updated"] += 1
        else:
            counts["unchanged"] += 1
    proposed_keys = {p.key for p in proposals}
    for key in set(auto_matched) - decided - proposed_keys:
        for row in by_key.get(key, []):
            if row.status == "proposed":
                ops.append(Op("delete", row.id))
                counts["deleted"] += 1
    counts["conflicts"] = len(conflict_keys)
    return ops, counts


def write_proposals(
    db: Any, proposals: Sequence[Proposal], auto_matched: Iterable[str] = ()
) -> Dict[str, int]:
    """Store the report's proposals (see plan_proposals) and commit."""
    if not links_table_exists(db):
        raise LinkError(409, MISSING_TABLE)
    ops, counts = plan_proposals(fetch_rows(db), proposals, auto_matched)
    apply_ops(db, ops)
    return counts
