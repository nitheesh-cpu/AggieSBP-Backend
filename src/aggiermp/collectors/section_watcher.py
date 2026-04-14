"""
section_watcher.py
==================
Optimised script that checks all actively-watched sections and sends a
web-push notification to each user whose watched section just opened.

Design goals
------------
* Single DB round-trip to get every watched section + its current open
  status in one query (no N+1 queries).
* Parallel push delivery via a thread pool so network latency of one
  subscription never blocks others.
* "Already notified" guard: a `last_notified_open_at` column on
  `user_tracked_sections` prevents repeated spam when a section stays
  open across multiple runs.

Usage
-----
    # Run once (e.g. from a cron job / scheduler):
    python -m aggiermp.collectors.section_watcher

    # Or import and call programmatically:
    from aggiermp.collectors.section_watcher import run_watcher
    run_watcher()
"""

from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import quote

from dotenv import load_dotenv
from pywebpush import WebPushException, webpush
from sqlalchemy import text

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("section_watcher")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
VAPID_PRIVATE_KEY: str = os.getenv("VAPID_PRIVATE_KEY", "")
VAPID_CONTACT: str = os.getenv("VAPID_CONTACT_EMAIL", "mailto:support@AggieSBP.com")
MAX_PUSH_WORKERS: int = 20  # concurrent push threads


# ---------------------------------------------------------------------------
# DB helpers (lazy import to avoid circular deps at import time)
# ---------------------------------------------------------------------------
def _get_session():  # type: ignore[return]
    from aggiermp.database.base import get_session  # noqa: PLC0415
    return get_session()


# ---------------------------------------------------------------------------
# Ensure the last_notified_open_at column exists
# ---------------------------------------------------------------------------
_CHECK_COLUMN_SQL = text("""
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'user_tracked_sections'
      AND column_name = 'last_notified_open_at'
""")


def _ensure_migration(db: Any) -> None:
    """Add last_notified_open_at column if it doesn't already exist."""
    try:
        row = db.execute(_CHECK_COLUMN_SQL).fetchone()
        if row:
            logger.debug("Column last_notified_open_at already exists, skipping migration.")
            return

        logger.info("Adding last_notified_open_at column to user_tracked_sections...")
        # Use a short lock timeout so we fail fast instead of hanging forever
        db.execute(text("SET lock_timeout = '5s'"))
        db.execute(text(
            "ALTER TABLE user_tracked_sections "
            "ADD COLUMN IF NOT EXISTS last_notified_open_at TIMESTAMPTZ"
        ))
        db.commit()
        logger.info("Migration complete.")
    except Exception as exc:
        logger.warning("Migration skipped: %s", exc)
        db.rollback()


# ---------------------------------------------------------------------------
# Core query – one round-trip for everything we need
# ---------------------------------------------------------------------------
_WATCH_QUERY = text("""
    SELECT
        uts.id           AS track_id,
        uts.user_id,
        uts.section_id,
        uts.last_notified_open_at,
        s.is_open,
        s.dept,
        s.course_number,
        s.section_number,
        s.course_title,
        s.crn,
        s.term_code,
        term.term_desc   AS term_desc
    FROM user_tracked_sections uts
    JOIN sections s ON (
        -- section_id format: TERMCODE-CRN-DEPT-COURSENUM-SECNUM
        -- sections.id format: TERMCODE_CRN
        s.id = split_part(uts.section_id, '-', 1) || '_' || split_part(uts.section_id, '-', 2)
        OR s.id = uts.section_id
        OR s.id = uts.term_code || '_' || uts.section_id
    )
    LEFT JOIN terms term ON term.term_code = s.term_code
    WHERE uts.status = 'active'
""")


# ---------------------------------------------------------------------------
# Push notification helper (runs in thread pool)
# ---------------------------------------------------------------------------
def _send_push(sub_row: Any, payload: str) -> tuple[str, bool]:
    """Send one push notification. Returns (endpoint, success)."""
    subscription_info = {
        "endpoint": sub_row.endpoint,
        "keys": {"p256dh": sub_row.p256dh, "auth": sub_row.auth},
    }
    try:
        webpush(
            subscription_info=subscription_info,
            data=payload,
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims={"sub": VAPID_CONTACT},
        )
        return sub_row.endpoint, True
    except WebPushException as exc:
        status = exc.response.status_code if exc.response else None
        logger.warning("Push failed (status=%s): %s", status, exc)
        return sub_row.endpoint, False
    except Exception as exc:
        logger.error("Push error: %s", exc)
        return sub_row.endpoint, False


# Seat-alert taps open College Scheduler (see _scheduler_cart_url).
_COLLEGE_SCHEDULER_TERMS_URL = "https://tamu.collegescheduler.com/terms"
_COLLEGE_SCHEDULER_ORIGIN = "https://tamu.collegescheduler.com"


def _scheduler_cart_url(row: Any) -> str:
    """Cart page for this term, e.g. /terms/Fall%202026%20-%20College%20Station/cart."""
    desc = getattr(row, "term_desc", None)
    if desc is None:
        return _COLLEGE_SCHEDULER_TERMS_URL
    desc = str(desc).strip()
    if not desc:
        return _COLLEGE_SCHEDULER_TERMS_URL
    segment = quote(desc, safe="")
    return f"{_COLLEGE_SCHEDULER_ORIGIN}/terms/{segment}/cart"


def _build_payload(row: Any) -> str:
    return json.dumps(
        {
            "title": "Seat Available! 🎉",
            "body": (
                f"{row.dept} {row.course_number}.{row.section_number} "
                f"({row.course_title}) just opened up!"
            ),
            "url": _scheduler_cart_url(row),
            "crn": row.crn,
            "sectionId": row.section_id,
        }
    )


# ---------------------------------------------------------------------------
# Main watcher logic
# ---------------------------------------------------------------------------
def run_watcher() -> Dict[str, Any]:
    """
    Check all watched sections and push notifications for newly-opened ones.

    Returns a summary dict:
        {
            "checked": int,       # total active watches
            "newly_open": int,    # sections that just became open
            "notified": int,      # push attempts that succeeded
            "elapsed_s": float,
        }
    """
    t0 = time.perf_counter()

    if not VAPID_PRIVATE_KEY:
        logger.error("VAPID_PRIVATE_KEY not set – cannot send push notifications.")
        return {"error": "VAPID_PRIVATE_KEY not configured"}

    db = _get_session()
    try:
        _ensure_migration(db)
        logger.info("Querying active watches...")

        # ── DIAGNOSTIC: dump raw tracked rows + verify JOIN ─────────────
        raw_tracked = db.execute(text(
            "SELECT id, user_id, section_id, term_code, status FROM user_tracked_sections WHERE status = 'active'"
        )).fetchall()
        logger.info("RAW tracked rows (%d): %s", len(raw_tracked),
                    [(r.section_id, r.status) for r in raw_tracked])

        if raw_tracked:
            sid_sample = raw_tracked[0].section_id
            match = db.execute(text("SELECT id, is_open FROM sections WHERE id = :sid"),
                               {"sid": sid_sample}).fetchone()
            logger.info("Sections lookup for section_id=%r → %s", sid_sample, match)

        # ── 1. Fetch all active watches with section status ──────────────
        rows = db.execute(_WATCH_QUERY).fetchall()
        logger.info("Fetched %d active watch row(s).", len(rows))

        # ── 2. Filter: section is open AND we haven't notified for this
        #              open event yet (last_notified_open_at is null OR
        #              the section was closed since the last notification,
        #              meaning it became open *again* — we detect that by
        #              checking is_open vs. last_notified state via the
        #              timestamp: if is_open is True and we last notified
        #              before is_open became True we'd need a
        #              `became_open_at` column; simplest safe approach
        #              is: notify if is_open AND last_notified_open_at IS
        #              NULL, then set it; clear it from a separate
        #              poll when is_open becomes False.)
        # ────────────────────────────────────────────────────────────────
        needs_notify: List[Any] = [
            r for r in rows
            if r.is_open and r.last_notified_open_at is None
        ]

        # ── 3. Clear stale notifications for sections that closed again ──
        closed_track_ids = [
            r.track_id for r in rows
            if not r.is_open and r.last_notified_open_at is not None
        ]
        if closed_track_ids:
            db.execute(
                text(
                    "UPDATE user_tracked_sections "
                    "SET last_notified_open_at = NULL "
                    "WHERE id = ANY(:ids)"
                ),
                {"ids": closed_track_ids},
            )
            db.commit()
            logger.info(
                "Reset last_notified_open_at for %d closed section(s).",
                len(closed_track_ids),
            )

        if not needs_notify:
            elapsed = time.perf_counter() - t0
            logger.info("No new openings found. Completed in %.2fs.", elapsed)
            return {
                "checked": len(rows),
                "newly_open": 0,
                "notified": 0,
                "elapsed_s": round(elapsed, 3),
            }

        logger.info("%d section(s) newly opened, fetching subscriptions...", len(needs_notify))

        # ── 4. Collect unique user_ids and fetch all their subscriptions
        #       in ONE query ───────────────────────────────────────────
        user_ids = list({r.user_id for r in needs_notify})
        sub_rows = db.execute(
            text(
                "SELECT user_id, endpoint, p256dh, auth "
                "FROM user_subscriptions "
                "WHERE user_id = ANY(:user_ids)"
            ),
            {"user_ids": user_ids},
        ).fetchall()

        # Index subscriptions by user_id for fast lookup
        subs_by_user: Dict[str, List[Any]] = {}
        for sub in sub_rows:
            subs_by_user.setdefault(sub.user_id, []).append(sub)

        logger.info(
            "Found %d subscription(s) across %d user(s).",
            len(sub_rows),
            len(user_ids),
        )

        # ── 5. Dispatch push notifications in parallel ───────────────────
        push_tasks: List[tuple[Any, str]] = []  # (sub_row, payload)
        for row in needs_notify:
            payload = _build_payload(row)
            for sub in subs_by_user.get(row.user_id, []):
                push_tasks.append((sub, payload))

        notified_count = 0
        stale_endpoints: List[str] = []

        with ThreadPoolExecutor(max_workers=min(MAX_PUSH_WORKERS, len(push_tasks) or 1)) as pool:
            future_map = {
                pool.submit(_send_push, sub, payload): sub
                for sub, payload in push_tasks
            }
            for future in as_completed(future_map):
                endpoint, ok = future.result()
                if ok:
                    notified_count += 1
                else:
                    stale_endpoints.append(endpoint)

        # ── 6. Remove stale subscriptions (expired/invalid) ─────────────
        if stale_endpoints:
            db.execute(
                text(
                    "DELETE FROM user_subscriptions WHERE endpoint = ANY(:eps)"
                ),
                {"eps": stale_endpoints},
            )
            db.commit()
            logger.info("Removed %d stale subscription(s).", len(stale_endpoints))

        # ── 7. Stamp successfully-notified watches ───────────────────────
        now = datetime.utcnow()
        notified_track_ids = [r.track_id for r in needs_notify]
        db.execute(
            text(
                "UPDATE user_tracked_sections "
                "SET last_notified_open_at = :now "
                "WHERE id = ANY(:ids)"
            ),
            {"now": now, "ids": notified_track_ids},
        )
        db.commit()

        elapsed = time.perf_counter() - t0
        logger.info(
            "Done. checked=%d newly_open=%d notified=%d elapsed=%.2fs",
            len(rows),
            len(needs_notify),
            notified_count,
            elapsed,
        )
        return {
            "checked": len(rows),
            "newly_open": len(needs_notify),
            "notified": notified_count,
            "elapsed_s": round(elapsed, 3),
        }

    except Exception as exc:
        logger.exception("Watcher failed: %s", exc)
        db.rollback()
        return {"error": str(exc)}
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    summary = run_watcher()
    print("\n=== Section Watcher Summary ===")
    for k, v in summary.items():
        print(f"  {k:12s}: {v}")
