"""
Backfill departments on professor_summaries_new rows whose course_code is a bare
number (e.g. "119" instead of "CHEM119").

Reviews on RateMyProfessors often give only the course number. Earlier pipeline
runs couldn't infer the department, so those course summaries were stored
without one, which hides the subject on professor pages and breaks the join to
grade data. This renames those rows in place using the same inference the
pipeline now uses (CourseNormalizer.infer_department_with_source), so no
summaries are regenerated.

Rows are skipped, not merged, when the professor already has a summary under
the inferred code (or two bare rows resolve to the same code).

Usage:
    python pipelines/professors/backfill_course_departments.py            # dry run
    python pipelines/professors/backfill_course_departments.py --report out.csv
    python pipelines/professors/backfill_course_departments.py --apply
"""

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from sqlalchemy import text

from aggiermp.database.base import get_session
from pipelines.professors.hierarchical_summarization.course_normalizer import (
    CourseNormalizer,
)
from pipelines.professors.upsert import generate_summary_id


def plan_backfill(session) -> list[dict]:
    """Work out the new course code for every bare-number summary row."""
    normalizer = CourseNormalizer(session=session)
    rows = session.execute(
        text(r"""
            SELECT ps.id, ps.professor_id, ps.course_code, ps.total_reviews,
                   p.first_name, p.last_name, p.department
            FROM professor_summaries_new ps
            JOIN professors p ON p.id = ps.professor_id
            WHERE ps.course_code ~ '^\d+[A-Z]*$'
            ORDER BY p.last_name, p.first_name, ps.course_code
        """)
    ).fetchall()
    existing = {
        (row.professor_id, row.course_code)
        for row in session.execute(
            text(
                "SELECT professor_id, course_code FROM professor_summaries_new "
                "WHERE course_code IS NOT NULL"
            )
        )
    }

    plan = []
    claimed: set[tuple[str, str]] = set()
    for row in rows:
        dept, source = normalizer.infer_department_with_source(
            row.course_code, professor_id=row.professor_id
        )
        new_code = (
            normalizer.normalize_course_code(f"{dept}{row.course_code}")
            if dept
            else None
        )
        if not new_code:
            status = "unresolved"
        elif (row.professor_id, new_code) in existing | claimed:
            status = "collision"
        else:
            status = "rename"
            claimed.add((row.professor_id, new_code))
        plan.append(
            {
                "status": status,
                "source": source or "",
                "professor": f"{row.first_name} {row.last_name}",
                "professor_department": row.department or "",
                "old_code": row.course_code,
                "new_code": new_code or "",
                "total_reviews": row.total_reviews,
                "professor_id": row.professor_id,
                "old_id": row.id,
                "new_id": generate_summary_id(row.professor_id, new_code)
                if new_code
                else "",
            }
        )
    return plan


def apply_backfill(session, plan: list[dict]) -> int:
    renames = [item for item in plan if item["status"] == "rename"]
    for item in renames:
        session.execute(
            text("""
                UPDATE professor_summaries_new
                SET id = :new_id, course_code = :new_code, updated_at = NOW()
                WHERE id = :old_id
            """),
            item,
        )
    session.commit()
    return len(renames)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--apply", action="store_true", help="write the renames (default: dry run)"
    )
    parser.add_argument("--report", help="write every planned row to this CSV file")
    args = parser.parse_args()

    session = get_session()
    try:
        plan = plan_backfill(session)

        print(f"Bare-number summary rows: {len(plan)}")
        for status, count in Counter(item["status"] for item in plan).most_common():
            print(f"  {status}: {count}")
        sources = Counter(item["source"] for item in plan if item["source"])
        print("Resolved by: " + ", ".join(f"{s}={n}" for s, n in sources.items()))

        if args.report:
            with open(args.report, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(plan[0]) if plan else [])
                writer.writeheader()
                writer.writerows(plan)
            print(f"Report written to {args.report}")

        if args.apply:
            renamed = apply_backfill(session, plan)
            print(f"Renamed {renamed} rows. Clear the API cache to show them.")
        else:
            print("Dry run: nothing written. Re-run with --apply to rename.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
