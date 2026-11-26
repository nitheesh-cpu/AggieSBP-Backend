"""Run with python -m aggiermp.core.admin_worker from the backend directory."""

import json
import logging
import os
import socket
import threading
import time
import traceback
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from ..database.base import get_session, create_db_engine
from .hidden_professors import hidden_professor_ids
from .summary_lock import summary_execution_lock, SummaryBusyError
from .config import settings

logger = logging.getLogger(__name__)


class CacheUnavailableError(RuntimeError):
    pass


def log_rebuild_exception(job_id, exc):
    """Log diagnostic metadata without exception messages or traceback source."""
    frames = traceback.extract_tb(exc.__traceback__)
    origin = next(
        (
            frame
            for frame in reversed(frames)
            if {"aggiermp", "pipelines"}.intersection(Path(frame.filename).parts)
        ),
        frames[-1] if frames else None,
    )
    location = (
        f"{Path(origin.filename).name}:{origin.lineno} in {origin.name}"
        if origin
        else "unknown location"
    )
    detail = ""
    if (
        isinstance(exc, ModuleNotFoundError)
        and exc.name
        and all(part.isidentifier() for part in exc.name.split("."))
    ):
        detail = f"; missing module {exc.name}"
    elif isinstance(exc, DBAPIError):
        sqlstate = getattr(exc.orig, "pgcode", None)
        if isinstance(sqlstate, str) and len(sqlstate) == 5 and sqlstate.isalnum():
            detail = f"; SQLSTATE {sqlstate}"
    logger.warning(
        "Summary rebuild failed for job %s (%s at %s%s)",
        job_id,
        type(exc).__name__,
        location,
        detail,
    )


def ensure_cache_available():
    import redis

    try:
        with redis.Redis.from_url(
            settings.redis_url or "redis://localhost:6379",
            socket_connect_timeout=5,
            socket_timeout=5,
        ) as client:
            client.ping()
    except Exception as exc:
        raise CacheUnavailableError(
            "Cache unavailable; restore Redis before retrying."
        ) from exc


def clear_summary_cache(professor_ids=None, db=None):
    """Clear only what shows these professors (and their merge aliases).

    Fail the job if invalidation fails; never hide a stale public result.
    With no ids this clears every API key (kept for callers that need it).
    """
    from .cache_invalidation import (
        CacheInvalidationError,
        invalidate_all,
        invalidate_professors,
    )

    try:
        if professor_ids is None:
            invalidate_all(strict=True)
        else:
            invalidate_professors(professor_ids, db=db, strict=True)
    except CacheInvalidationError as exc:
        raise CacheUnavailableError(str(exc)) from exc


def heartbeat(stop):
    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    while not stop.is_set():
        try:
            with get_session() as db:
                db.execute(
                    text(
                        "INSERT INTO admin_worker_heartbeats(worker_id) VALUES(:id) ON CONFLICT(worker_id) DO UPDATE SET heartbeat_at=now()"
                    ),
                    {"id": worker_id},
                )
                db.commit()
        except Exception:
            logger.warning("Unable to update worker heartbeat")
        stop.wait(20)


def save(db, job_id, completed, failed, logs, status="running", error=None):
    db.execute(
        text(
            """UPDATE admin_summary_jobs SET completed_ids=CAST(:completed AS jsonb),
      failed_ids=CAST(:failed AS jsonb), logs=CAST(:logs AS jsonb), status=:status, error=:error,
      finished_at=CASE WHEN :status='running' THEN NULL ELSE now() END WHERE id=CAST(:id AS uuid)"""
        ),
        dict(
            id=job_id,
            completed=json.dumps(completed),
            failed=json.dumps(failed),
            logs=json.dumps(logs[-100:]),
            status=status,
            error=error,
        ),
    )
    db.commit()


def rebuild_one(db, professor_id):
    """Resolve current membership, including an alias merged after enqueue."""
    from pipelines.professors.upsert_reviews_and_summaries import (
        upsert_reviews_and_summaries,
    )
    from ..database.base import ProfessorSummaryNewDB

    canonical = (
        db.execute(
            text(
                "SELECT canonical_id FROM professor_aliases WHERE alias_id=:id AND status='confirmed'"
            ),
            {"id": professor_id},
        ).scalar()
        or professor_id
    )
    # A professor hidden after the job was queued: no AI call, no summary.
    # Drop the queue rows so it does not come back as pending.
    if {professor_id, canonical} & hidden_professor_ids(db):
        db.execute(
            text("DELETE FROM admin_summary_rebuilds WHERE professor_id = ANY(:ids)"),
            {"ids": [professor_id, canonical]},
        )
        db.commit()
        return "hidden"
    revision = db.execute(
        text(
            "SELECT revision,changed_at FROM admin_summary_rebuilds WHERE professor_id=:id"
        ),
        {"id": canonical},
    ).first()
    count = db.execute(
        text(
            """SELECT count(*) FROM reviews WHERE professor_id=:id OR professor_id IN
      (SELECT alias_id FROM professor_aliases WHERE canonical_id=:id AND status='confirmed')"""
        ),
        {"id": canonical},
    ).scalar()
    db.commit()
    if not count:
        db.query(ProfessorSummaryNewDB).filter(
            ProfessorSummaryNewDB.professor_id == canonical
        ).delete(synchronize_session=False)
        db.commit()
        result = {"summaries_generated": 1}
    else:
        result = upsert_reviews_and_summaries(
            professor_ids=[canonical],
            session=db,
            stored_reviews_only=True,
            invalidate_cache=False,
            skip_if_no_new_reviews=False,
            resume=False,
        )
    if result.get("quota_exhausted"):
        return "quota_exhausted"
    if (
        result.get("error")
        or result.get("errors")
        or result.get("summaries_generated", 0) < 1
    ):
        return "failed"
    try:
        clear_summary_cache([professor_id, canonical], db)
    except Exception as exc:
        raise CacheUnavailableError(
            "Cache unavailable; restore Redis before retrying."
        ) from exc
    if revision:
        db.execute(
            text(
                "DELETE FROM admin_summary_rebuilds WHERE professor_id=:id AND revision=:revision AND changed_at=:changed"
            ),
            {
                "id": canonical,
                "revision": revision.revision,
                "changed": revision.changed_at,
            },
        )
        db.commit()
    if db.execute(
        text(
            "SELECT EXISTS(SELECT 1 FROM admin_summary_rebuilds WHERE professor_id=:id)"
        ),
        {"id": canonical},
    ).scalar():
        db.commit()
        return "stale"
    db.commit()
    return "succeeded"


def run_once():
    # Taking the shared lock before claiming also prevents recovery of a live job.
    try:
        with summary_execution_lock(create_db_engine()):
            with get_session() as db:
                db.execute(
                    text(
                        "UPDATE admin_summary_jobs SET status='queued', error='Worker interrupted; resuming unfinished professors.' WHERE status='running'"
                    )
                )
                row = (
                    db.execute(
                        text(
                            "SELECT * FROM admin_summary_jobs WHERE status='queued' ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1"
                        )
                    )
                    .mappings()
                    .first()
                )
                if not row:
                    db.commit()
                    return False
                job_id = str(row["id"])
                completed, failed, logs = (
                    list(row["completed_ids"]),
                    [],
                    list(row["logs"]),
                )
                db.execute(
                    text(
                        "UPDATE admin_summary_jobs SET status='running', started_at=COALESCE(started_at,now()),error=NULL WHERE id=CAST(:id AS uuid)"
                    ),
                    {"id": job_id},
                )
                db.commit()
                quota = False
                try:
                    ensure_cache_available()
                except CacheUnavailableError:
                    save(
                        db,
                        job_id,
                        completed,
                        failed,
                        logs + ["Cache check failed before generation."],
                        "failed",
                        "Cache unavailable; restore Redis before retrying.",
                    )
                    return True
                cache_failed = False
                merge_changed = False
                for professor_id in row["professor_ids"]:
                    if professor_id in completed:
                        continue
                    try:
                        outcome = rebuild_one(db, professor_id)
                    except CacheUnavailableError:
                        db.rollback()
                        outcome = "failed"
                        cache_failed = True
                    except Exception as exc:
                        log_rebuild_exception(job_id, exc)
                        db.rollback()
                        outcome = "failed"
                    logs.append(f"Professor {professor_id}: {outcome}")
                    if outcome in ("succeeded", "hidden"):
                        completed.append(professor_id)
                    else:
                        failed.append(professor_id)
                    quota = outcome == "quota_exhausted"
                    merge_changed = merge_changed or outcome == "stale"
                    save(db, job_id, completed, failed, logs)
                    if quota or cache_failed:
                        break
                status = (
                    "quota_exhausted" if quota else "failed" if failed else "succeeded"
                )
                error = (
                    "AI quota exhausted; retry after allowance resets."
                    if quota
                    else (
                        "Some summaries failed; retry unfinished professors."
                        if failed
                        else None
                    )
                )
                if cache_failed:
                    error = "Cache invalidation failed; restore Redis before retrying unfinished professors."
                elif merge_changed and not quota:
                    error = "Merges changed during generation; retry unfinished professors with their latest reviews."
                save(db, job_id, completed, failed, logs, status, error)
                return True
    except SummaryBusyError:
        return False


def main():
    logging.basicConfig(level=logging.INFO)
    stop = threading.Event()
    thread = threading.Thread(target=heartbeat, args=(stop,), daemon=True)
    thread.start()
    try:
        while True:
            try:
                if not run_once():
                    time.sleep(5)
            except Exception:
                logger.warning("Worker cannot access queue; retrying in 10 seconds")
                time.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
