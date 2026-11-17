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
* Every push attempt is recorded in `push_notifications` (one batched
  INSERT per run, see migrations/003_push_notifications.sql) as 'sent',
  'expired' (subscription gone: 404/410) or 'failed'. Recording is best
  effort and runs after delivery, so it can never block a notification.

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
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import sys
from pathlib import Path
from dotenv import load_dotenv

# Add project src to path so we can import 'aggiermp'
project_root = Path(__file__).resolve().parent.parent.parent.parent
src_dir = project_root / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

# Load .env explicitly from the backend root
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)

from pywebpush import WebPushException, webpush
from sqlalchemy import text

from aggiermp.core.push_endpoints import PUSH_TIMEOUT_SECONDS, is_allowed_push_endpoint

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
STATUS_SENT = "sent"
STATUS_EXPIRED = "expired"  # push service says the subscription is gone (404/410)
STATUS_FAILED = "failed"
_GONE_STATUS_CODES = (404, 410)
_MAX_ERROR_CHARS = 300


@dataclass
class PushResult:
    endpoint: str
    status: str  # STATUS_SENT | STATUS_EXPIRED | STATUS_FAILED
    error: Optional[str] = None
    # The subscription can never work again and should be deleted.
    remove_subscription: bool = False


def _short_error(message: Any) -> str:
    return " ".join(str(message).split())[:_MAX_ERROR_CHARS]


def _send_push(sub_row: Any, payload: str) -> PushResult:
    """Send one push notification and classify the outcome."""
    endpoint = sub_row.endpoint
    if not is_allowed_push_endpoint(endpoint):
        logger.warning("Skipping non-push-service endpoint")
        return PushResult(
            endpoint, STATUS_FAILED, "endpoint not on push-service allowlist",
            remove_subscription=True,
        )
    subscription_info = {
        "endpoint": endpoint,
        "keys": {"p256dh": sub_row.p256dh, "auth": sub_row.auth},
    }
    try:
        webpush(
            subscription_info=subscription_info,
            data=payload,
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims={"sub": VAPID_CONTACT},
            timeout=PUSH_TIMEOUT_SECONDS,
        )
        return PushResult(endpoint, STATUS_SENT)
    except WebPushException as exc:
        # requests.Response is falsy for 4xx/5xx, so compare with None.
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None) if response is not None else None
        logger.warning("Push failed (status=%s): %s", status, exc)
        if status in _GONE_STATUS_CODES:
            return PushResult(
                endpoint, STATUS_EXPIRED, f"HTTP {status}", remove_subscription=True
            )
        prefix = f"HTTP {status}: " if status is not None else ""
        return PushResult(
            endpoint, STATUS_FAILED, _short_error(f"{prefix}{getattr(exc, 'message', exc)}")
        )
    except Exception as exc:
        logger.error("Push error: %s", exc)
        return PushResult(
            endpoint, STATUS_FAILED, _short_error(f"{type(exc).__name__}: {exc}")
        )


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


def _course_label(row: Any) -> str:
    """e.g. "CSCE 121.501"."""
    return f"{row.dept} {row.course_number}.{row.section_number}"


def _build_payload(row: Any, notification_id: Optional[str] = None) -> str:
    # course/term/notificationId let the service worker report
    # notification-shown/click/dismiss to Umami (frontend/public/sw.js).
    # Nothing here identifies the user.
    payload: Dict[str, Any] = {
        "title": "Seat Available! 🎉",
        "body": f"{_course_label(row)} ({row.course_title}) just opened up!",
        "url": _scheduler_cart_url(row),
        "crn": row.crn,
        "sectionId": row.section_id,
        "course": _course_label(row),
        "term": row.term_code,
    }
    if notification_id:
        payload["notificationId"] = notification_id
    return json.dumps(payload)


# ---------------------------------------------------------------------------
# Delivery log (push_notifications table, migration 003)
# ---------------------------------------------------------------------------
_PUSH_LOG_TABLE_SQL = text("SELECT to_regclass('public.push_notifications')")
_INSERT_PUSH_LOG_SQL = text("""
    INSERT INTO push_notifications
        (id, user_id, track_id, section_id, term_code, crn, course,
         sent_at, status, error)
    VALUES
        (CAST(:id AS uuid), :user_id, :track_id, :section_id, :term_code, :crn,
         :course, :sent_at, :status, :error)
""")


def _push_log_record(
    notification_id: str, row: Any, result: PushResult, sent_at: datetime
) -> Dict[str, Any]:
    def _str(value: Any) -> Optional[str]:
        return None if value is None else str(value)

    return {
        "id": notification_id,
        "user_id": str(row.user_id),
        "track_id": _str(row.track_id),
        "section_id": _str(row.section_id),
        "term_code": _str(row.term_code),
        "crn": _str(row.crn),
        "course": _course_label(row),
        "sent_at": sent_at,
        "status": result.status,
        "error": result.error,
    }


def _record_push_attempts(db: Any, records: List[Dict[str, Any]]) -> int:
    """Insert one push_notifications row per attempt, in a single batch.

    Best effort: a missing table (migration 003 not applied yet) or any DB
    error is logged once and swallowed, so it never affects delivery.
    Returns the number of rows written.
    """
    if not records:
        return 0
    try:
        if db.execute(_PUSH_LOG_TABLE_SQL).scalar() is None:
            logger.warning(
                "push_notifications table missing; skipped logging %d push "
                "attempt(s). Apply migrations/003_push_notifications.sql.",
                len(records),
            )
            return 0
        # Bound the cost: logging must never hold the run for long.
        db.execute(text("SET LOCAL statement_timeout = '5s'"))
        db.execute(_INSERT_PUSH_LOG_SQL, records)
        db.commit()
        return len(records)
    except Exception as exc:
        logger.warning(
            "Could not log %d push attempt(s): %s", len(records), _short_error(exc)
        )
        try:
            db.rollback()
        except Exception:
            pass
        return 0


# ---------------------------------------------------------------------------
# Main watcher logic
# ---------------------------------------------------------------------------
def run_watcher() -> Dict[str, Any]:
    """
    Check all watched sections and push notifications for newly-opened ones.

    Returns a summary dict:
        {
            "checked": int,          # total active watches
            "newly_open": int,       # sections that just became open
            "notified": int,         # push attempts that succeeded (== sent)
            "sent": int,             # push service accepted the message
            "expired": int,          # subscription gone (404/410), removed
            "failed": int,           # any other error
            "no_subscription": int,  # newly-open watches whose user has no device
            "logged": int,           # rows written to push_notifications
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
                "sent": 0,
                "expired": 0,
                "failed": 0,
                "no_subscription": 0,
                "logged": 0,
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
        # One notification id per (watch, device): it is both the
        # push_notifications row id and the payload's notificationId.
        push_tasks: List[tuple[str, Any, Any, str]] = []  # (id, watch, sub, payload)
        no_subscription = 0
        for row in needs_notify:
            subs = subs_by_user.get(row.user_id, [])
            if not subs:
                no_subscription += 1
            for sub in subs:
                notification_id = str(uuid.uuid4())
                push_tasks.append(
                    (notification_id, row, sub, _build_payload(row, notification_id))
                )

        counts = {STATUS_SENT: 0, STATUS_EXPIRED: 0, STATUS_FAILED: 0}
        stale_endpoints: List[str] = []
        log_records: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=min(MAX_PUSH_WORKERS, len(push_tasks) or 1)) as pool:
            future_map = {
                pool.submit(_send_push, sub, payload): (notification_id, row, sub)
                for notification_id, row, sub, payload in push_tasks
            }
            for future in as_completed(future_map):
                notification_id, row, sub = future_map[future]
                try:
                    result = future.result()
                except Exception as exc:  # _send_push catches everything; just in case
                    result = PushResult(
                        sub.endpoint, STATUS_FAILED, _short_error(f"{type(exc).__name__}: {exc}")
                    )
                counts[result.status] = counts.get(result.status, 0) + 1
                if result.remove_subscription:
                    stale_endpoints.append(result.endpoint)
                log_records.append(
                    _push_log_record(notification_id, row, result, datetime.now(timezone.utc))
                )
        notified_count = counts[STATUS_SENT]

        # ── 6. Remove subscriptions that can never work again (404/410, or
        #       not a push-service URL). Transient failures keep theirs. ──
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

        # ── 8. Record every attempt (best effort, after delivery) ────────
        logged = _record_push_attempts(db, log_records)

        elapsed = time.perf_counter() - t0
        logger.info(
            "Done. checked=%d newly_open=%d sent=%d expired=%d failed=%d "
            "no_subscription=%d logged=%d elapsed=%.2fs",
            len(rows),
            len(needs_notify),
            counts[STATUS_SENT],
            counts[STATUS_EXPIRED],
            counts[STATUS_FAILED],
            no_subscription,
            logged,
            elapsed,
        )
        return {
            "checked": len(rows),
            "newly_open": len(needs_notify),
            "notified": notified_count,
            "sent": counts[STATUS_SENT],
            "expired": counts[STATUS_EXPIRED],
            "failed": counts[STATUS_FAILED],
            "no_subscription": no_subscription,
            "logged": logged,
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
        print(f"  {k:15s}: {v}")
