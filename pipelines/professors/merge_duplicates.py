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
printed) and clear the API cache (DELETE /cache/clear with X-Admin-Key).

Run --propose from cron; review and confirm on the site's /admin/merges page
(admin key required), which also clears the cache, or with the flags below.

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
from aggiermp.core.professor_merges import (  # noqa: F401 (chain_problem: tests)
    chain_problem,
    propose,
    rebuild_command,
    set_status,
    stored_aliases,
)
from aggiermp.core.cache_invalidation import invalidate_professors
from aggiermp.core.hidden_professors import hidden_professor_ids
from aggiermp.database.base import ProfessorDB, get_session


def plan(
    session: Any, university_id: str | None = None
) -> tuple[List[ProfileMerge], List[List[str]], Dict[str, Any]]:
    query = session.query(ProfessorDB)
    if university_id:
        query = query.filter(ProfessorDB.university_id == university_id)
    hidden_ids = hidden_professor_ids(session)
    all_professors = query.all()
    # Hidden professors are never proposed for merging.
    professors = [p for p in all_professors if p.id not in hidden_ids]
    print(f"Hidden professors skipped: {len(all_professors) - len(professors)}")
    by_id = {professor.id: professor for professor in professors}
    profiles = load_profiles(session, professors)
    people = load_all_people(session)
    merges, ambiguous = plan_profile_merges(profiles, people)
    return merges, ambiguous, by_id


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
            changes = set_status(session, args.confirm or args.reject, status)
            for change in changes:
                if change.status == "skipped":
                    print(f"  SKIPPED {change.alias_id}: {change.problem}")
                    continue
                print(f"  {change.status}: {describe(session, change.alias_id)}")
                print(f"      -> {describe(session, change.canonical_id)}")
            done = [c for c in changes if c.status == status]
            if done:
                outcome = invalidate_professors(
                    {c.alias_id for c in done} | {c.canonical_id for c in done},
                    db=session,
                )
                print(
                    f"\nCache: {outcome.keys_deleted} API keys cleared, website "
                    f"pages {outcome.revalidation} ({outcome.pages_revalidated})"
                )
            command = rebuild_command(c.canonical_id for c in done)
            if command:
                print(
                    "\nRebuild these professors' summaries (the cache is cleared "
                    "again for each one that finishes):\n  " + command
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
