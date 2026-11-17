"""Bookkeeping for duplicate-professor merges in professor_aliases.

Shared by pipelines/professors/merge_duplicates.py (cron: propose new
merges) and the admin API (/admin/professor-merges: review, confirm,
reject). Which pairs are duplicates is decided in professor_identity.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import text

STATUSES = ("proposed", "confirmed", "rejected")


@dataclass(frozen=True)
class StatusChange:
    alias_id: str
    canonical_id: Optional[str]
    status: str  # the new status, or "skipped"
    problem: Optional[str] = None


def stored_aliases(db: Any) -> Dict[str, Any]:
    """Every row of professor_aliases by alias id, whatever its status."""
    rows = db.execute(
        text("SELECT alias_id, canonical_id, evidence, status FROM professor_aliases")
    ).fetchall()
    return {row.alias_id: row for row in rows}


def chain_problem(
    alias_id: str, canonical_id: str, confirmed: Dict[str, str]
) -> Optional[str]:
    """Why confirming alias -> canonical would chain merges, or None.

    The views map each id one step, so a canonical id must never itself be
    a confirmed alias, and an alias must not be the canonical of others.
    """
    if canonical_id in confirmed:
        return f"{canonical_id} is itself merged into {confirmed[canonical_id]}"
    shown_under_alias = [a for a, c in confirmed.items() if c == alias_id]
    if shown_under_alias:
        return f"{alias_id} is the canonical id of {', '.join(shown_under_alias)}"
    return None


def propose(db: Any, merges: Iterable[Any]) -> int:
    """Store new merges as 'proposed'. Pairs already stored, with any status,
    are left alone, so a rejected pair is never proposed again."""
    existing = stored_aliases(db)
    added = 0
    for merge in merges:
        if merge.alias_id in existing:
            continue
        db.execute(
            text(
                "INSERT INTO professor_aliases (alias_id, canonical_id, evidence, status) "
                "VALUES (:alias, :canonical, :evidence, 'proposed')"
            ),
            {
                "alias": merge.alias_id,
                "canonical": merge.canonical_id,
                "evidence": merge.evidence,
            },
        )
        added += 1
    db.commit()
    return added


def set_status(db: Any, alias_ids: Iterable[str], status: str) -> List[StatusChange]:
    """Confirm or reject stored merges. Confirming never creates a chain."""
    if status not in ("confirmed", "rejected"):
        raise ValueError(f"status must be confirmed or rejected, not {status!r}")
    existing = stored_aliases(db)
    confirmed = {
        a: row.canonical_id for a, row in existing.items() if row.status == "confirmed"
    }
    changes: List[StatusChange] = []
    for alias_id in dict.fromkeys(alias_ids):
        row = existing.get(alias_id)
        if row is None:
            changes.append(
                StatusChange(alias_id, None, "skipped", "not in professor_aliases")
            )
            continue
        if status == "confirmed":
            problem = chain_problem(alias_id, row.canonical_id, confirmed)
            if problem:
                changes.append(StatusChange(alias_id, row.canonical_id, "skipped", problem))
                continue
            confirmed[alias_id] = row.canonical_id
        else:
            confirmed.pop(alias_id, None)
        db.execute(
            text(
                "UPDATE professor_aliases SET status = :status, reviewed_at = now() "
                "WHERE alias_id = :alias"
            ),
            {"status": status, "alias": alias_id},
        )
        changes.append(StatusChange(alias_id, row.canonical_id, status))
    db.commit()
    return changes


def rebuild_command(canonical_ids: Iterable[str]) -> Optional[str]:
    """The pipeline command that rebuilds these professors' summaries, which
    must happen after merges change so summaries cover the merged reviews."""
    ids = list(dict.fromkeys(canonical_ids))
    if not ids:
        return None
    return (
        "python -m pipelines.professors.upsert_reviews_and_summaries "
        "--stored-reviews-only --force-update --professor-id " + " ".join(ids)
    )


def rmp_number(professor_id: str, legacy_id: Optional[int]) -> Optional[int]:
    """The number in a RateMyProfessors URL (ratemyprofessors.com/professor/N).

    That is the stored legacy id; our ids also encode it ("Teacher-N" in
    base64), which covers rows where legacy_id is missing.
    """
    if legacy_id:
        return int(legacy_id)
    try:
        decoded = base64.b64decode(professor_id + "=" * (-len(professor_id) % 4)).decode()
    except (ValueError, UnicodeDecodeError):
        return None
    prefix, _, number = decoded.partition("-")
    return int(number) if prefix == "Teacher" and number.isdigit() else None


def list_merges(db: Any, status: Optional[str] = None) -> List[Dict[str, Any]]:
    """Stored merges with both professors' names, departments and counts."""
    where = "WHERE a.status = :status" if status else ""
    rows = db.execute(
        text(
            f"""
            SELECT
                a.alias_id, a.canonical_id, a.evidence, a.status,
                a.created_at, a.reviewed_at,
                pa.first_name AS alias_first, pa.last_name AS alias_last,
                pa.department AS alias_department, pa.num_ratings AS alias_ratings,
                pa.legacy_id AS alias_legacy_id,
                pc.first_name AS canonical_first, pc.last_name AS canonical_last,
                pc.department AS canonical_department, pc.num_ratings AS canonical_ratings,
                pc.legacy_id AS canonical_legacy_id
            FROM professor_aliases a
            LEFT JOIN professors pa ON pa.id = a.alias_id
            LEFT JOIN professors pc ON pc.id = a.canonical_id
            {where}
            ORDER BY a.reviewed_at DESC NULLS LAST, a.created_at DESC, a.alias_id
            """
        ),
        {"status": status} if status else {},
    ).fetchall()

    def name(first: Optional[str], last: Optional[str]) -> str:
        return " ".join(part.strip() for part in (first, last) if part and part.strip())

    return [
        {
            "aliasId": row.alias_id,
            "canonicalId": row.canonical_id,
            "evidence": row.evidence,
            "status": row.status,
            "needsCheck": row.evidence.startswith("CHECK"),
            "createdAt": row.created_at.isoformat() if row.created_at else None,
            "reviewedAt": row.reviewed_at.isoformat() if row.reviewed_at else None,
            "alias": {
                "name": name(row.alias_first, row.alias_last),
                "department": row.alias_department,
                "ratings": row.alias_ratings or 0,
                "rmpId": rmp_number(row.alias_id, row.alias_legacy_id),
            },
            "canonical": {
                "name": name(row.canonical_first, row.canonical_last),
                "department": row.canonical_department,
                "ratings": row.canonical_ratings or 0,
                "rmpId": rmp_number(row.canonical_id, row.canonical_legacy_id),
            },
        }
        for row in rows
    ]
