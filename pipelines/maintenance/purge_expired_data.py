"""
purge_expired_data.py
=====================
Retention purge for data that would otherwise grow forever. Run daily from cron:

    15 3 * * * cronitor exec <KEY> /home/opc/backend/.venv/bin/python /home/opc/backend/pipelines/maintenance/purge_expired_data.py

    python pipelines/maintenance/purge_expired_data.py --dry-run   # only counts

NOT purged on purpose: accounts, planner state, schedules, push devices.
Users keep those until they delete their account (DELETE /users/me).

Exit code is non-zero if any rule fails so cron monitoring notices.
"""

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

project_root = Path(__file__).resolve().parent.parent.parent
for p in (project_root, project_root / "src"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))
load_dotenv(project_root / ".env")

from sqlalchemy import text  # noqa: E402

from aggiermp.core.user_data import extract_crn, release_trackers  # noqa: E402
from aggiermp.database.base import get_session  # noqa: E402

# ---------------------------------------------------------------------------
# Retention rules (change here; keep frontend/src/app/privacy/page.tsx in sync)
# ---------------------------------------------------------------------------
PUSH_LOG_RETENTION_DAYS = 180  # push_notifications rows older than this are deleted
WATCH_GRACE_DAYS_AFTER_TERM_END = 30  # seat watches go this long after terms.end_date
# A watch whose term has no end_date (or is missing from `terms`) cannot be dated
# from the term, so it is judged by the watch's own age instead.
WATCH_FALLBACK_MAX_AGE_DAYS = 365
ADMIN_JOB_RETENTION_DAYS = 90  # finished admin_summary_jobs older than this ...
ADMIN_JOB_KEEP_NEWEST = 100  # ... unless among the newest N (what the admin page shows)

BATCH_SIZE = 1000
BATCH_PAUSE_SECONDS = 0.2
LOCK_TIMEOUT = "5s"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("purge_expired_data")

PUSH_LOG_WHERE = "t.sent_at < now() - make_interval(days => :days)"

WATCH_FROM = "user_tracked_sections t LEFT JOIN terms tm ON tm.term_code = t.term_code"
WATCH_WHERE = """(
    (tm.end_date IS NOT NULL AND tm.end_date < now() - make_interval(days => :grace))
    OR (tm.end_date IS NULL AND t.created_at < now() - make_interval(days => :fallback))
)"""

ADMIN_JOB_WHERE = """t.status IN ('succeeded','failed','quota_exhausted')
    AND t.created_at < now() - make_interval(days => :days)
    AND t.created_at < :boundary"""


def _exists(db: Any, table: str) -> bool:
    return db.execute(text("SELECT to_regclass(:t)"), {"t": table}).scalar() is not None


def _batched_delete(db: Any, table: str, frm: str, where: str, params: Dict[str, Any],
                    returning: str, dry_run: bool) -> Tuple[int, List[Tuple]]:
    """Count (dry run) or delete in batches of BATCH_SIZE, one short transaction each."""
    if dry_run:
        n = db.execute(text(f"SELECT count(*) FROM {frm} WHERE {where}"), params).scalar()
        return int(n or 0), []
    total, rows_out = 0, []
    while True:
        res = db.execute(
            text(
                f"DELETE FROM {table} WHERE id IN "
                f"(SELECT t.id FROM {frm} WHERE {where} LIMIT {BATCH_SIZE}) RETURNING {returning}"
            ),
            params,
        )
        rows = res.fetchall()
        db.commit()
        total += len(rows)
        rows_out.extend(tuple(r) for r in rows)
        if len(rows) < BATCH_SIZE:
            return total, rows_out
        time.sleep(BATCH_PAUSE_SECONDS)


def purge_push_log(db: Any, dry_run: bool) -> int:
    if not _exists(db, "push_notifications"):
        return 0
    n, _ = _batched_delete(db, "push_notifications", "push_notifications t", PUSH_LOG_WHERE,
                           {"days": PUSH_LOG_RETENTION_DAYS}, "id", dry_run)
    return n


def purge_expired_watches(db: Any, dry_run: bool) -> Tuple[int, List[Tuple[str, str, str]]]:
    """Returns (count, [(user_id, term_code, crn)]) for Redis cleanup."""
    if not _exists(db, "user_tracked_sections"):
        return 0, []
    n, rows = _batched_delete(
        db, "user_tracked_sections", WATCH_FROM, WATCH_WHERE,
        {"grace": WATCH_GRACE_DAYS_AFTER_TERM_END, "fallback": WATCH_FALLBACK_MAX_AGE_DAYS},
        "user_id, section_id, term_code", dry_run,
    )
    pairs = []
    for user_id, section_id, term_code in rows:
        crn = extract_crn(section_id or "")
        if crn and term_code:
            pairs.append((user_id, term_code, crn))
    return n, pairs


def purge_admin_jobs(db: Any, dry_run: bool) -> int:
    if not _exists(db, "admin_summary_jobs"):
        return 0
    # Fixed once up front: the creation time of the Nth newest job. Recomputing the
    # "newest N" per batch would drift as rows disappear and delete too much.
    boundary = db.execute(
        text("SELECT created_at FROM admin_summary_jobs ORDER BY created_at DESC OFFSET :o LIMIT 1"),
        {"o": ADMIN_JOB_KEEP_NEWEST - 1},
    ).scalar()
    if boundary is None:
        return 0
    n, _ = _batched_delete(db, "admin_summary_jobs", "admin_summary_jobs t", ADMIN_JOB_WHERE,
                           {"days": ADMIN_JOB_RETENTION_DAYS, "boundary": boundary},
                           "id", dry_run)
    return n


async def _release(pairs: List[Tuple[str, str, str]]) -> None:
    from aggiermp.core.cache import get_redis

    await release_trackers(await get_redis(), pairs)


def run(db: Any, dry_run: bool = False) -> Dict[str, Any]:
    """Run every rule independently; a failure in one does not skip the others."""
    summary: Dict[str, Any] = {"dry_run": dry_run, "errors": []}
    db.execute(text(f"SET lock_timeout = '{LOCK_TIMEOUT}'"))
    pairs: List[Tuple[str, str, str]] = []
    rules = (
        ("push_notifications", lambda: purge_push_log(db, dry_run)),
        ("user_tracked_sections", lambda: purge_expired_watches(db, dry_run)),
        ("admin_summary_jobs", lambda: purge_admin_jobs(db, dry_run)),
    )
    for name, fn in rules:
        try:
            out = fn()
            if isinstance(out, tuple):
                out, got = out
                pairs.extend(got)
            summary[name] = out
        except Exception as exc:  # noqa: BLE001
            db.rollback()
            logger.error("rule %s failed: %s", name, exc, exc_info=True)
            summary[name] = None
            summary["errors"].append(f"{name}: {exc}")
    if pairs and not dry_run:
        try:
            asyncio.run(_release(pairs))
        except Exception as exc:  # noqa: BLE001  (best effort)
            logger.warning("Redis tracker cleanup failed: %s", exc)
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description="Delete data past its retention period.")
    ap.add_argument("--dry-run", action="store_true", help="only count what would be deleted")
    args = ap.parse_args()

    db = get_session()
    try:
        s = run(db, args.dry_run)
    finally:
        db.close()

    verb = "would delete" if args.dry_run else "deleted"
    print(f"purge_expired_data ({'DRY RUN' if args.dry_run else 'live'})")
    print(f"  push_notifications older than {PUSH_LOG_RETENTION_DAYS}d: {verb} {s['push_notifications']}")
    print(f"  seat watches >{WATCH_GRACE_DAYS_AFTER_TERM_END}d after term end "
          f"(no end date: older than {WATCH_FALLBACK_MAX_AGE_DAYS}d): {verb} {s['user_tracked_sections']}")
    print(f"  finished admin jobs older than {ADMIN_JOB_RETENTION_DAYS}d "
          f"(newest {ADMIN_JOB_KEEP_NEWEST} kept): {verb} {s['admin_summary_jobs']}")
    if s["errors"]:
        print("FAILED: " + "; ".join(s["errors"]))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
