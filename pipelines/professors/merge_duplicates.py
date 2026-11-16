"""
Find and merge duplicate RateMyProfessors profiles of one person.

RMP sometimes has two teacher entries for one person ("Abigail Songok" and
"Abigael Songok", both teaching CHEM 119), and each became its own professor
row. Merges never change the professors, reviews or summaries tables: a merge
is a row in professor_aliases (alias id -> canonical id), and the API reads
the *_merged views, which show one professor per person. See
src/aggiermp/database/migrations/002_professor_aliases.sql.

A merge goes through two steps so a person checks it before the site changes:

1. --propose stores the pairs found by the rules in
   aggiermp.core.professor_identity (plan_profile_merges) as 'proposed'.
   Proposed merges have no effect on the site.
2. --confirm makes listed merges live; --reject turns them down for good
   (a rejected pair is never proposed again). Confirming or rejecting a merge
   again later is how it is undone or redone.

After confirming, rebuild the merged professors' summaries (the command is
printed) and clear the API cache (DELETE /cache with the admin key).

Usage (from backend/):
    python -m pipelines.professors.merge_duplicates                 # dry run
    python -m pipelines.professors.merge_duplicates --propose
    python -m pipelines.professors.merge_duplicates --list
    python -m pipelines.professors.merge_duplicates --confirm ALIAS_ID [...]
    python -m pipelines.professors.merge_duplicates --reject ALIAS_ID [...]
"""

import sys
from pathlib import Path
from typing import Any, Dict, List

project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from sqlalchemy import text

from aggiermp.core.professor_identity import (
    ProfileMerge,
    load_all_people,
    load_profiles,
    plan_profile_merges,
)
from aggiermp.database.base import ProfessorDB, get_session


def plan(
    session: Any, university_id: str | None = None
) -> tuple[List[ProfileMerge], List[List[str]], Dict[str, Any]]:
    query = session.query(ProfessorDB)
    if university_id:
        query = query.filter(ProfessorDB.university_id == university_id)
    professors = query.all()
    by_id = {professor.id: professor for professor in professors}
    profiles = load_profiles(session, professors)
    people = load_all_people(session)
    merges, ambiguous = plan_profile_merges(profiles, people)
    return merges, ambiguous, by_id


def stored_aliases(session: Any) -> Dict[str, Any]:
    """Every row of professor_aliases by alias id, whatever its status."""
    rows = session.execute(
        text("SELECT alias_id, canonical_id, evidence, status FROM professor_aliases")
    ).fetchall()
    return {row.alias_id: row for row in rows}


def chain_problem(
    alias_id: str, canonical_id: str, confirmed: Dict[str, str]
) -> str | None:
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


def describe(session: Any, pid: str) -> str:
    row = session.execute(
        text(
            "SELECT first_name, last_name, department, num_ratings "
            "FROM professors WHERE id = :id"
        ),
        {"id": pid},
    ).fetchone()
    if row is None:
        return f"{pid} (no such professor)"
    return (
        f"{row.first_name} {row.last_name} "
        f"[{row.department or '-'}, {row.num_ratings or 0} ratings] {pid}"
    )


def propose(session: Any, merges: List[ProfileMerge]) -> int:
    existing = stored_aliases(session)
    added = 0
    for merge in merges:
        if merge.alias_id in existing:
            continue  # Already proposed, confirmed or rejected.
        session.execute(
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
    session.commit()
    return added


def set_status(session: Any, alias_ids: List[str], status: str) -> List[str]:
    """Confirm or reject stored merges; returns the canonical ids touched."""
    existing = stored_aliases(session)
    confirmed = {
        a: row.canonical_id for a, row in existing.items() if row.status == "confirmed"
    }
    touched: List[str] = []
    for alias_id in alias_ids:
        row = existing.get(alias_id)
        if row is None:
            print(f"  SKIPPED {alias_id}: not in professor_aliases (run --propose first)")
            continue
        if status == "confirmed":
            problem = chain_problem(alias_id, row.canonical_id, confirmed)
            if problem:
                print(f"  SKIPPED {alias_id}: {problem}")
                continue
            confirmed[alias_id] = row.canonical_id
        else:
            confirmed.pop(alias_id, None)
        session.execute(
            text(
                "UPDATE professor_aliases SET status = :status, reviewed_at = now() "
                "WHERE alias_id = :alias"
            ),
            {"status": status, "alias": alias_id},
        )
        print(f"  {status}: {describe(session, alias_id)}")
        print(f"      -> {describe(session, row.canonical_id)}")
        touched.append(row.canonical_id)
    session.commit()
    return list(dict.fromkeys(touched))


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Find and merge duplicate RMP professor profiles")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--propose", action="store_true", help="Store new merges as 'proposed'")
    action.add_argument("--list", action="store_true", help="Show stored merges")
    action.add_argument("--confirm", nargs="+", metavar="ALIAS_ID", help="Make these merges live")
    action.add_argument("--reject", nargs="+", metavar="ALIAS_ID", help="Turn these merges down")
    parser.add_argument("--university-id", help="Only this RMP school id (planning only)")
    parser.add_argument(
        "--status",
        choices=["proposed", "confirmed", "rejected"],
        help="With --list, only merges with this status",
    )
    args = parser.parse_args()

    session = get_session()
    try:
        if args.list:
            for alias_id, row in sorted(stored_aliases(session).items(), key=lambda kv: kv[1].canonical_id):
                if args.status and row.status != args.status:
                    continue
                print(f"[{row.status}] {describe(session, alias_id)}")
                print(f"      -> {describe(session, row.canonical_id)}")
                print(f"         {row.evidence}")
            return

        if args.confirm or args.reject:
            status = "confirmed" if args.confirm else "rejected"
            canonical_ids = set_status(session, args.confirm or args.reject, status)
            if canonical_ids:
                print(
                    "\nRebuild these professors' summaries, then clear the API cache "
                    "(DELETE /cache with the admin key):\n"
                    "  python -m pipelines.professors.upsert_reviews_and_summaries "
                    "--stored-reviews-only --force-update --professor-id "
                    + " ".join(canonical_ids)
                )
            return

        merges, ambiguous, by_id = plan(session, args.university_id)
        existing = stored_aliases(session)

        def label(pid: str) -> str:
            row = by_id.get(pid)
            if row is None:
                return pid
            return f"{row.first_name} {row.last_name} [{row.department or '-'}, {row.num_ratings} ratings] {pid}"

        new = [m for m in merges if m.alias_id not in existing]
        print(f"{len(merges)} merges found, {len(new)} not yet stored")
        for merge in merges:
            state = existing[merge.alias_id].status if merge.alias_id in existing else "new"
            print(f"  [{state}] {label(merge.alias_id)}\n    -> {label(merge.canonical_id)}\n       {merge.evidence}")
        if ambiguous:
            print(f"\n{len(ambiguous)} linked groups skipped (names cannot all be one person):")
            for group in ambiguous:
                print("  " + " | ".join(label(pid) for pid in group))

        if args.propose:
            added = propose(session, merges)
            print(
                f"\nStored {added} new proposed merges. Nothing changes on the site "
                "until they are confirmed: review them with --list --status proposed, "
                "then --confirm or --reject each alias id."
            )
        else:
            print("\nDry run: nothing written. Re-run with --propose to store these for review.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
