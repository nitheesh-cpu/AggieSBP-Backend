"""Nightly RMP removal watch: profiles RateMyProfessors no longer has.

A stored professor missing from a complete school listing is looked up
directly. Only when RMP returns nothing for it (and a control lookup works)
does it get a row in rmp_missing_profiles. A second confirmation on a later
day is needed before anything is hidden, and nothing is changed at all
without ``apply``. See migration 007_rmp_missing_profiles.sql and D-049.

Guards, because a flaky RMP night must never hide real professors:
  * only schools whose whole listing was read are checked;
  * a school with an unusually large number of candidates is skipped;
  * a control lookup of a profile that is in the listing must succeed;
  * a lookup that errors (None) changes nothing;
  * one miss is not enough, and misses count at most once per day;
  * profiles hidden by hand are never touched, and only automatic hides are
    ever undone when a profile reappears.
"""

from datetime import date, datetime, time
from typing import Any, Dict, Iterable, List, Optional, Set

from sqlalchemy import text

from aggiermp.core.hidden_professors import (
    delete_stored_content,
    hidden_professor_ids,
    hide_professor,
    merge_group,
    unhide_professor,
)
from pipelines.professors.scrapers import RMPReviewCollector

AUTO_REASON = "Removed from RateMyProfessors (detected automatically)"
MERGED_REASON = "RMP removed a merged profile"
MIN_CAP = 25
CAP_FRACTION = 0.02
MISSES_TO_ACT = 2


def _table_exists(db: Any, name: str) -> bool:
    return bool(
        db.execute(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": name}).scalar()
    )


def candidate_cap(stored_count: int) -> float:
    return max(MIN_CAP, CAP_FRACTION * stored_count)


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _canonical_id(db: Any, professor_id: str) -> str:
    if not _table_exists(db, "professor_aliases"):
        return professor_id
    canonical = db.execute(
        text(
            "SELECT canonical_id FROM professor_aliases "
            "WHERE alias_id = :id AND status = 'confirmed'"
        ),
        {"id": professor_id},
    ).scalar()
    return str(canonical) if canonical else professor_id


def _names(db: Any, ids: Iterable[str]) -> Dict[str, str]:
    ids = sorted(set(ids))
    if not ids:
        return {}
    try:
        rows = db.execute(
            text("SELECT id, first_name, last_name FROM professors WHERE id = ANY(:ids)"),
            {"ids": ids},
        ).fetchall()
    except Exception:
        return {}
    return {str(r[0]): f"{r[1]} {r[2]}" for r in rows}


def check_removed_profiles(
    session: Any,
    listings: Dict[str, Dict[str, Any]],
    *,
    apply: bool,
    dry_run: bool = False,
    today: Optional[date] = None,
    collector_factory=RMPReviewCollector,
) -> Dict[str, Any]:
    """Find stored profiles RMP removed; record misses; act when ``apply``.

    ``listings`` is {school_id: {teacher_id: rating_count}} for schools whose
    listing completed. Commits per professor. Returns counts, ``errors``,
    ``affected_ids`` (ids whose caches need clearing) and ``hidden_ids``
    (every id hidden by this run, whole merge groups).
    """
    from pipelines.professors import review_sync

    result: Dict[str, Any] = {
        "profiles_missing": 0,
        "profiles_hidden": 0,
        "profiles_content_deleted": 0,
        "profiles_restored": 0,
        "schools_skipped": 0,
        "errors": [],
        "affected_ids": set(),
        "hidden_ids": set(),
    }
    if not listings:
        return result
    if not _table_exists(session, "rmp_missing_profiles") or not _table_exists(
        session, "hidden_professors"
    ):
        print(
            "RMP removal watch skipped: apply migrations 006 and 007 "
            "(rmp_missing_profiles / hidden_professors missing)",
            flush=True,
        )
        return result

    errors: List[str] = result["errors"]
    writes = not dry_run
    acting = apply and not dry_run
    today = today or date.today()
    stamp = (
        datetime.now() if today == date.today() else datetime.combine(today, time(12))
    )

    rows: Dict[str, Dict[str, Any]] = {
        str(r[0]): {"misses": int(r[1]), "last": _as_date(r[2]), "action": r[3]}
        for r in session.execute(
            text(
                "SELECT professor_id, misses, last_missing_at, action "
                "FROM rmp_missing_profiles"
            )
        ).fetchall()
    }
    hidden_all = hidden_professor_ids(session)
    hidden_reasons: Dict[str, str] = {
        str(r[0]): r[1]
        for r in session.execute(
            text("SELECT professor_id, reason FROM hidden_professors")
        ).fetchall()
    }
    live_ids: Set[str] = {tid for listing in listings.values() for tid in listing}

    collector_holder: List[Any] = []

    def collector() -> Any:
        if not collector_holder:
            collector_holder.append(collector_factory())
        return collector_holder[0]

    def fail(message: str) -> None:
        errors.append(message)
        print(f"  {message}", flush=True)

    def delete_row(professor_id: str) -> None:
        if writes and professor_id in rows:
            session.execute(
                text("DELETE FROM rmp_missing_profiles WHERE professor_id = :id"),
                {"id": professor_id},
            )
            session.commit()
        rows.pop(professor_id, None)

    try:
        # Recovery: profiles back in a listing are no longer missing.
        for tid in sorted(live_ids):
            reason = hidden_reasons.get(tid)
            if tid not in rows and reason != AUTO_REASON:
                continue
            try:
                if tid in rows:
                    delete_row(tid)
                if reason == AUTO_REASON:
                    if acting:
                        group = unhide_professor(session, tid)
                        session.commit()
                        if group is not None:
                            result["profiles_restored"] += 1
                            result["affected_ids"].update(group)
                            hidden_all -= group
                            print(
                                f"  restored {tid}: back on RMP (reviews return "
                                "on the next fetch, summary needs a rebuild)",
                                flush=True,
                            )
                    else:
                        print(f"  would restore {tid}: back on RMP", flush=True)
            except Exception as exc:
                session.rollback()
                fail(f"Restoring {tid} failed: {exc}")

        confirmed: List[str] = []
        for school_id, listing in listings.items():
            stored = {
                str(r[0])
                for r in session.execute(
                    text("SELECT id FROM professors WHERE university_id = :school"),
                    {"school": school_id},
                ).fetchall()
            }
            candidates = sorted(stored - set(listing) - hidden_all)
            if not candidates:
                continue
            cap = candidate_cap(len(stored))
            if len(candidates) > cap:
                result["schools_skipped"] += 1
                fail(
                    f"RMP removal watch skipped school {school_id}: "
                    f"{len(candidates)} profiles missing from its listing "
                    f"(limit {int(cap)}); check RMP before trusting this"
                )
                continue
            if not listing:
                result["schools_skipped"] += 1
                fail(f"RMP removal watch skipped school {school_id}: empty listing")
                continue
            control = sorted(listing)[0]
            try:
                control_ok = review_sync.teacher_exists(collector(), control) is True
            except Exception:
                control_ok = False
            if not control_ok:
                result["schools_skipped"] += 1
                fail(
                    f"RMP removal watch skipped school {school_id}: control "
                    "lookup failed"
                )
                continue

            for pid in candidates:
                try:
                    exists = review_sync.teacher_exists(collector(), pid)
                except Exception:
                    exists = None
                if exists is None:
                    continue
                if exists is True:
                    delete_row(pid)
                    continue
                result["profiles_missing"] += 1
                row = rows.get(pid)
                if row is None:
                    rows[pid] = {"misses": 1, "last": today, "action": None}
                    if writes:
                        session.execute(
                            text(
                                "INSERT INTO rmp_missing_profiles "
                                "(professor_id, first_missing_at, last_missing_at, misses) "
                                "VALUES (:id, :ts, :ts, 1)"
                            ),
                            {"id": pid, "ts": stamp},
                        )
                        session.commit()
                else:
                    if row["last"] is None or row["last"] < today:
                        row["misses"] += 1
                    row["last"] = today
                    if writes:
                        session.execute(
                            text(
                                "UPDATE rmp_missing_profiles SET misses = :misses, "
                                "last_missing_at = :ts WHERE professor_id = :id"
                            ),
                            {"id": pid, "misses": row["misses"], "ts": stamp},
                        )
                        session.commit()
                confirmed.append(pid)

        todo = [
            pid
            for pid in confirmed
            if rows[pid]["misses"] >= MISSES_TO_ACT and rows[pid]["action"] is None
        ]
        names = _names(session, todo)
        for pid in todo:
            try:
                group = merge_group(session, pid)
                others = group - {pid}
                merged = False
                for other in sorted(others):
                    if other in live_ids or review_sync.teacher_exists(
                        collector(), other
                    ) is True:
                        merged = True
                        break
                label = f"{pid} {names.get(pid, '')}".strip()
                if not acting:
                    if merged:
                        print(
                            f"  would delete stored reviews of merged profile {label}",
                            flush=True,
                        )
                    else:
                        print(f"  would hide {label}", flush=True)
                    continue
                canonical = _canonical_id(session, pid)
                if merged:
                    delete_stored_content(session, [pid])
                    action = "content_deleted"
                else:
                    hide_professor(session, pid, AUTO_REASON)
                    action = "hidden"
                session.execute(
                    text(
                        "UPDATE rmp_missing_profiles SET action = :action, "
                        "acted_at = :ts WHERE professor_id = :id"
                    ),
                    {"id": pid, "action": action, "ts": stamp},
                )
                if merged:
                    from pipelines.professors.upsert_reviews_and_summaries import (
                        queue_summary_rebuilds,
                        rebuild_queue_available,
                    )

                    if rebuild_queue_available(session):
                        queue_summary_rebuilds(
                            session, [(canonical, MERGED_REASON, True)]
                        )
                    else:
                        session.commit()
                else:
                    session.commit()
                rows[pid]["action"] = action
                result["affected_ids"].update(group)
                if merged:
                    result["profiles_content_deleted"] += 1
                    print(
                        f"  deleted stored reviews of merged profile {label}",
                        flush=True,
                    )
                else:
                    result["profiles_hidden"] += 1
                    result["hidden_ids"].update(group)
                    print(f"  hid {label} (removed from RMP)", flush=True)
            except Exception as exc:
                session.rollback()
                fail(f"Removal action for {pid} failed: {exc}")
    finally:
        for c in collector_holder:
            try:
                c.session.close()
            except Exception:
                pass

    print(
        f"RMP removal watch: {result['profiles_missing']} missing, "
        f"{result['profiles_hidden']} hidden, "
        f"{result['profiles_content_deleted']} content deleted, "
        f"{result['profiles_restored']} restored, "
        f"{result['schools_skipped']} schools skipped"
        f"{'' if acting else ' (report only)'}",
        flush=True,
    )
    return result
