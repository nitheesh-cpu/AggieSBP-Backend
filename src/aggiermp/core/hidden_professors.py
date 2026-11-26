"""Professors hidden from the site (migration 006_hidden_professors.sql).

The *_merged views already drop hidden professors. Use these helpers where
code reads the raw professors / reviews / professor_summaries_new tables
(the course page matcher, pipelines): a hidden id must behave like "no
profile" there too.

A hidden entry hides its whole merge group: the canonical id and every
confirmed alias. Before migration 006 runs, nothing is hidden.
"""

from typing import Any, Set

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
