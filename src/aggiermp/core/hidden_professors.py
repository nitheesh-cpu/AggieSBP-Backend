"""Professors hidden from the site (migration 006_hidden_professors.sql).

The *_merged views already drop hidden professors. Use these helpers where
code reads the raw professors / reviews / professor_summaries_new tables
(the course page matcher, pipelines): a hidden id must behave like "no
profile" there too.

A hidden entry hides its whole merge group: the canonical id and every
confirmed alias. Before migration 006 runs, nothing is hidden.
"""

from typing import Any, Dict, Iterable, Optional, Set

from sqlalchemy import text

_HIDDEN_IDS_SQL = """
WITH hidden_groups AS (
    SELECT COALESCE(a.canonical_id, h.professor_id) AS canonical_id
    FROM hidden_professors h
    LEFT JOIN professor_aliases a
        ON a.alias_id = h.professor_id AND a.status = 'confirmed'
)
SELECT professor_id FROM hidden_professors
UNION
SELECT canonical_id FROM hidden_groups
UNION
SELECT a.alias_id
FROM professor_aliases a
JOIN hidden_groups g ON g.canonical_id = a.canonical_id
WHERE a.status = 'confirmed'
"""

_HIDDEN_IDS_NO_ALIASES_SQL = "SELECT professor_id FROM hidden_professors"


def _table_exists(db: Any, name: str) -> bool:
    return bool(db.execute(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": name}).scalar())


def hidden_professor_ids(db: Any) -> Set[str]:
    """Every professor id that must not be shown (whole merge groups).

    `db` is a SQLAlchemy Session or Connection. Returns an empty set when the
    hidden_professors table does not exist yet.
    """
    if not _table_exists(db, "hidden_professors"):
        return set()
    sql = _HIDDEN_IDS_SQL if _table_exists(db, "professor_aliases") else _HIDDEN_IDS_NO_ALIASES_SQL
    return {str(row[0]) for row in db.execute(text(sql)).fetchall()}


def is_professor_hidden(db: Any, professor_id: str) -> bool:
    """True when this id, or any entry merged with it, is hidden."""
    if not professor_id:
        return False
    return str(professor_id) in hidden_professor_ids(db)


def _exists(db: Any, name: str) -> bool:
    return _table_exists(db, name)


def merge_group(db: Any, professor_id: str) -> Set[str]:
    """The id, its confirmed canonical, and every confirmed alias of that canonical."""
    group = {professor_id}
    if not _exists(db, "professor_aliases"):
        return group
    canonical = db.execute(
        text(
            "SELECT canonical_id FROM professor_aliases "
            "WHERE alias_id = :id AND status = 'confirmed'"
        ),
        {"id": professor_id},
    ).scalar()
    canonical = str(canonical) if canonical else professor_id
    group.add(canonical)
    rows = db.execute(
        text(
            "SELECT alias_id FROM professor_aliases "
            "WHERE canonical_id = :canonical AND status = 'confirmed'"
        ),
        {"canonical": canonical},
    ).fetchall()
    group.update(str(r[0]) for r in rows)
    return group


def delete_stored_content(db: Any, ids: Iterable[str]) -> Dict[str, int]:
    """Delete our copy of these ids' reviews, summaries and pending summary
    rebuilds. Does not commit. Returns counts per table."""
    ids = sorted(set(ids))
    if not ids:
        return {"reviews": 0, "summaries": 0, "pendingRebuilds": 0}
    reviews = db.execute(
        text("DELETE FROM reviews WHERE professor_id = ANY(:ids)"), {"ids": ids}
    ).rowcount
    summaries = db.execute(
        text("DELETE FROM professor_summaries_new WHERE professor_id = ANY(:ids)"),
        {"ids": ids},
    ).rowcount
    rebuilds = 0
    if _exists(db, "admin_summary_rebuilds"):
        rebuilds = db.execute(
            text("DELETE FROM admin_summary_rebuilds WHERE professor_id = ANY(:ids)"),
            {"ids": ids},
        ).rowcount
    return {
        "reviews": reviews or 0,
        "summaries": summaries or 0,
        "pendingRebuilds": rebuilds or 0,
    }


def hide_professor(db: Any, professor_id: str, reason: str) -> Dict[str, Any]:
    """Hide the professor's whole merge group. Does not commit or clear caches.

    Records the entry (re-hiding updates the reason) and deletes our copy of
    the group's reviews, summaries and pending rebuilds. The professors row
    and professor_aliases stay, so the nightly import does not recreate the
    person as new. Requires the hidden_professors table (migration 006).
    """
    group = merge_group(db, professor_id)
    db.execute(
        text(
            "INSERT INTO hidden_professors (professor_id, reason) "
            "VALUES (:id, :reason) "
            "ON CONFLICT (professor_id) DO UPDATE SET reason = EXCLUDED.reason"
        ),
        {"id": professor_id, "reason": reason},
    )
    return {"groupIds": sorted(group), "deleted": delete_stored_content(db, group)}


def unhide_professor(db: Any, professor_id: str) -> Optional[Set[str]]:
    """Remove the hidden entry. Does not commit or clear caches.

    Returns the merge group (for cache clearing), or None if it was not
    hidden. Reviews come back on the next review fetch; summaries need a
    rebuild.
    """
    removed = db.execute(
        text("DELETE FROM hidden_professors WHERE professor_id = :id"),
        {"id": professor_id},
    ).rowcount
    if not removed:
        return None
    return merge_group(db, professor_id)
