"""Everything that stores data about a signed-in user, and how to remove it.

User data is keyed by the SuperTokens user id. Postgres tables that hold it are
listed in USER_DATA_TABLES; tests/test_account_deletion.py fails if a new table
with a ``user_id`` column appears without being added here.

Redis: the seat-watch code (routers/users.py track_section) adds the user id to
``trackers:{term}:{crn}`` and ``{term}:{crn}`` to ``tracked_sections``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import text

logger = logging.getLogger(__name__)

# (table, column holding the SuperTokens user id). Fixed list: never built from input.
USER_DATA_TABLES: Tuple[Tuple[str, str], ...] = (
    ("user_tracked_sections", "user_id"),  # seat watches
    ("user_subscriptions", "user_id"),  # push devices
    ("user_planner_state", "user_id"),  # saved record + plan
    ("user_schedules", "user_id"),  # saved schedules
    ("push_notifications", "user_id"),  # push delivery log
)

TRACKED_SECTIONS_KEY = "tracked_sections"


def extract_crn(section_id: str) -> Optional[str]:
    """CRN from ``TERM-CRN-DEPT-NUM-SEC`` (extension) or ``TERM_CRN`` (DB) ids."""
    if "-" in section_id:
        parts = section_id.split("-")
        return parts[1] if len(parts) >= 2 else None
    if "_" in section_id:
        parts = section_id.split("_")
        return parts[1] if len(parts) >= 2 else None
    return None


def tracker_key(term: str, crn: str) -> str:
    return f"trackers:{term}:{crn}"


def _table_exists(db: Any, table: str) -> bool:
    return db.execute(text("SELECT to_regclass(:t)"), {"t": table}).scalar() is not None


def delete_user_rows(db: Any, user_id: str) -> Tuple[Dict[str, int], List[Tuple[str, str]]]:
    """Delete the user's rows from every user table in the caller's transaction.

    Does NOT commit. Returns ({table: rows_deleted}, [(term_code, crn)] of the
    seat watches that were removed, for Redis cleanup). A table that does not
    exist yet (an unapplied migration) has no data and is skipped.
    """
    counts: Dict[str, int] = {}
    watched: List[Tuple[str, str]] = []
    for table, column in USER_DATA_TABLES:
        if not _table_exists(db, table):
            logger.warning("account deletion: table %s does not exist, skipping", table)
            counts[table] = 0
            continue
        if table == "user_tracked_sections":
            result = db.execute(
                text(f"DELETE FROM {table} WHERE {column} = :u RETURNING section_id, term_code"),
                {"u": user_id},
            )
            rows = result.fetchall()
            counts[table] = len(rows)
            for section_id, term_code in rows:
                crn = extract_crn(section_id or "")
                if crn and term_code:
                    watched.append((term_code, crn))
        else:
            result = db.execute(text(f"DELETE FROM {table} WHERE {column} = :u"), {"u": user_id})
            counts[table] = result.rowcount or 0
    return counts, watched


async def release_trackers(redis_client: Any, pairs: Iterable[Tuple[str, str, str]]) -> None:
    """Remove (user_id, term, crn) entries from the Redis tracker sets.

    ``tracked_sections`` loses a member only once no tracker remains for it.
    Best effort: failures are logged, never raised.
    """
    if redis_client is None:
        return
    seen: Set[Tuple[str, str, str]] = set()
    for user_id, term, crn in pairs:
        if (user_id, term, crn) in seen:
            continue
        seen.add((user_id, term, crn))
        try:
            key = tracker_key(term, crn)
            await redis_client.srem(key, user_id)
            if not await redis_client.scard(key):
                await redis_client.srem(TRACKED_SECTIONS_KEY, f"{term}:{crn}")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Redis tracker cleanup failed for %s:%s: %s", term, crn, exc)


async def get_redis_or_none() -> Any:
    try:
        from .cache import get_redis

        return await get_redis()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Redis unavailable for tracker cleanup: %s", exc)
        return None
