"""Durable, bounded admin summary requests. No arbitrary command execution."""

import json
import uuid
from sqlalchemy import text


def ready(db):
    return db.execute(
        text(
            "SELECT to_regclass('admin_summary_jobs') IS NOT NULL AND to_regclass('admin_summary_rebuilds') IS NOT NULL"
        )
    ).scalar()


def serialize(row):
    value = dict(row)

    def stamp(key):
        return value[key].isoformat() if value[key] else None

    return dict(
        id=str(value["id"]),
        status=value["status"],
        professorIds=value["professor_ids"],
        completedIds=value["completed_ids"],
        failedIds=value["failed_ids"],
        total=len(value["professor_ids"]),
        completed=len(value["completed_ids"]),
        createdAt=stamp("created_at"),
        startedAt=stamp("started_at"),
        finishedAt=stamp("finished_at"),
        error=value["error"],
        logs=value["logs"],
    )


def get_job(db, job_id):
    row = (
        db.execute(
            text("SELECT * FROM admin_summary_jobs WHERE id=CAST(:id AS uuid)"),
            {"id": str(job_id)},
        )
        .mappings()
        .first()
    )
    return serialize(row) if row else None


def enqueue(db, professor_ids):
    ids = sorted(set(professor_ids))
    # Serialize submissions across API processes, including partially overlapping batches.
    db.execute(text("SELECT pg_advisory_xact_lock(72483012)"))
    aliases = dict(
        db.execute(
            text(
                "SELECT alias_id,canonical_id FROM professor_aliases WHERE status='confirmed' AND alias_id=ANY(:ids)"
            ),
            {"ids": ids},
        ).all()
    )
    ids = sorted({aliases.get(pid, pid) for pid in ids})
    rows = (
        db.execute(
            text(
                "SELECT * FROM admin_summary_jobs WHERE status IN ('queued','running') ORDER BY created_at"
            )
        )
        .mappings()
        .all()
    )
    for row in rows:
        unfinished = set(row["professor_ids"]) - set(row["completed_ids"])
        if set(ids).issubset(unfinished):
            db.commit()
            return {"job": serialize(row), "deduplicated": True}
        if set(ids).intersection(unfinished):
            raise ValueError(
                "Some selected professors already have an active job. Wait for it to finish."
            )
    known = (
        db.execute(text("SELECT id FROM professors WHERE id=ANY(:ids)"), {"ids": ids})
        .scalars()
        .all()
    )
    if len(known) != len(ids):
        raise ValueError("One or more professor IDs do not exist.")
    job_id = str(uuid.uuid4())
    db.execute(
        text(
            "INSERT INTO admin_summary_jobs(id,professor_ids) VALUES(CAST(:id AS uuid),CAST(:ids AS jsonb))"
        ),
        {"id": job_id, "ids": json.dumps(ids)},
    )
    db.commit()
    return {"job": get_job(db, job_id), "deduplicated": False}


def pending(db):
    rows = db.execute(
        text("""SELECT r.*, concat_ws(' ',p.first_name,p.last_name) AS name
        FROM admin_summary_rebuilds r JOIN professors p ON p.id=r.professor_id
        WHERE NOT EXISTS(SELECT 1 FROM professor_aliases a WHERE a.alias_id=r.professor_id AND a.status='confirmed')
        ORDER BY r.changed_at DESC""")
    ).mappings()
    return [
        dict(
            professorId=r["professor_id"],
            name=r["name"],
            reason=r["reason"],
            changedAt=r["changed_at"].isoformat(),
            revision=r["revision"],
        )
        for r in rows
    ]
