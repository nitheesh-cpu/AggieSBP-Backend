"""
Upsert script for GPA data from anex.us.

This script:
1. Finds the newest term anex.us has published
2. Reads which (course, term) pairs are already in gpa_data
3. Fetches only courses that could have something new, showing progress
4. Bulk upserts the new records into gpa_data

Usage:
    python pipelines/gpa/upsert_gpa_data.py          # incremental (fast)
    python pipelines/gpa/upsert_gpa_data.py --full   # refetch every course and
                                                     # re-upsert every record
"""

import argparse
import asyncio
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from aggiermp.database.base import GpaDataDB, get_session
from pipelines.gpa.anex_scraping import (
    MAX_CONCURRENT_REQUESTS,
    extract_class_records,
    fetch_all_courses_concurrent,
    get_newest_semester,
    make_http_session,
    term_key,
)
from pipelines.gpa.schemas import GpaDataSchema

load_dotenv()

# Configuration
BULK_INSERT_SIZE = 2000  # Records per batch/commit

Course = Tuple[str, str]
Term = Tuple[str, str]


def load_courses_and_existing_terms() -> Tuple[List[Course], Dict[Course, Set[Term]]]:
    """
    Return (all numeric courses from the courses table,
            {course: set of (year, semester) already in gpa_data}).
    """
    session = get_session()
    try:
        courses = [
            (row.dept, row.course_number)
            for row in session.execute(
                text("""
                    SELECT DISTINCT subject_id AS dept, course_number
                    FROM courses
                    WHERE subject_id IS NOT NULL
                      AND course_number ~ '^[0-9]+$'
                    ORDER BY subject_id, course_number
                """)
            )
        ]

        existing_terms: Dict[Course, Set[Term]] = defaultdict(set)
        for row in session.execute(
            text("""
                SELECT DISTINCT dept, course_number, year, semester
                FROM gpa_data
            """)
        ):
            existing_terms[(row.dept, row.course_number)].add((row.year, row.semester))

        return courses, existing_terms
    finally:
        session.close()


def select_courses_to_fetch(
    courses: List[Course],
    existing_terms: Dict[Course, Set[Term]],
    newest_term: Optional[Term],
    full: bool,
) -> List[Course]:
    """
    Skip courses that already have the newest anex.us term stored; nothing
    newer can exist for them. Everything else (no data yet, or behind) is fetched.
    """
    if full or newest_term is None:
        return list(courses)

    newest_key = term_key(*newest_term)
    to_fetch = []
    for course in courses:
        terms = existing_terms.get(course)
        if terms and max(term_key(*t) for t in terms) >= newest_key:
            continue
        to_fetch.append(course)
    return to_fetch


def to_db_row(record: Dict[str, Any]) -> Dict[str, Any]:
    """Validate a scraped record and return the column dict for gpa_data."""
    return GpaDataSchema(**record).model_dump()


def chunks(lst: List[Any], n: int) -> Iterator[List[Any]]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def bulk_upsert_records(records: List[Dict[str, Any]]) -> int:
    """
    Upsert records in batches on one connection. Raises on a failed batch
    instead of silently reporting 0 inserted.
    """
    # The same id twice in one INSERT ... ON CONFLICT DO UPDATE makes Postgres
    # reject the whole statement, so dedupe first (last one wins).
    rows = list({r["id"]: to_db_row(r) for r in records}.values())
    if len(rows) != len(records):
        print(f"[INFO] Dropped {len(records) - len(rows)} duplicate ids")

    stmt = insert(GpaDataDB)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            col: stmt.excluded[col]
            for col in (
                "gpa",
                "grade_a",
                "grade_b",
                "grade_c",
                "grade_d",
                "grade_f",
                "grade_i",
                "grade_s",
                "grade_u",
                "grade_q",
                "grade_x",
                "total_students",
                "updated_at",
            )
        },
    )

    total_batches = (len(rows) + BULK_INSERT_SIZE - 1) // BULK_INSERT_SIZE
    print(f"[INFO] Upserting {len(rows)} records in {total_batches} batches...")

    session = get_session()
    written = 0
    try:
        for batch_num, batch in enumerate(chunks(rows, BULK_INSERT_SIZE), start=1):
            # executemany form: SQLAlchemy batches this into multi-row INSERTs
            session.execute(stmt, batch)
            session.commit()
            written += len(batch)
            print(f"  > Batch {batch_num}/{total_batches} OK ({written}/{len(rows)})", flush=True)
    except Exception:
        session.rollback()
        print(f"[ERROR] Batch failed after {written} records were committed")
        raise
    finally:
        session.close()

    return written


async def main_async(full: bool = False) -> None:
    print("=" * 60)
    print("GPA Data Upsert" + (" (FULL)" if full else ""))
    print("=" * 60)
    print(f"[INFO] Max concurrent requests: {MAX_CONCURRENT_REQUESTS}")
    print(f"[INFO] Bulk insert size: {BULK_INSERT_SIZE} records")

    start_time = datetime.now()

    print("\n[STEP 1] Fetching newest semester from anex.us...")
    async with make_http_session(1) as http:
        newest_term = await get_newest_semester(http)
    if newest_term:
        print(f"[OK] Newest semester on anex.us: {newest_term[1]} {newest_term[0]}")
    else:
        print("[WARNING] Could not determine newest semester; fetching every course")

    print("\n[STEP 2] Reading courses and existing GPA terms from the database...")
    courses, existing_terms = load_courses_and_existing_terms()
    to_fetch = select_courses_to_fetch(courses, existing_terms, newest_term, full)
    with_data = sum(1 for c in to_fetch if c in existing_terms)
    print(f"[INFO] {len(courses)} courses, {len(existing_terms)} already have GPA data")
    print(
        f"[INFO] Fetching {len(to_fetch)} courses "
        f"({with_data} behind the newest term, {len(to_fetch) - with_data} with no data yet); "
        f"skipping {len(courses) - len(to_fetch)} up to date"
    )

    if not to_fetch:
        print("[OK] Everything is up to date.")
        return

    print("\n[STEP 3] Fetching course data from anex.us...")
    responses = await fetch_all_courses_concurrent(to_fetch)

    print("\n[STEP 4] Extracting new records...")
    all_records: List[Dict[str, Any]] = []
    for response in responses:
        skip = None if full else existing_terms.get(response["course"])
        all_records.extend(extract_class_records(response, skip))
    print(f"[INFO] {len(all_records)} records to upsert")

    written = 0
    if all_records:
        print("\n[STEP 5] Upserting into gpa_data...")
        written = bulk_upsert_records(all_records)

    duration = datetime.now() - start_time
    print("\n" + "=" * 60)
    print("GPA UPSERT COMPLETE")
    print("=" * 60)
    print(f"{'Duration':<20}: {duration}")
    print(f"{'Courses fetched':<20}: {len(to_fetch)}")
    print(f"{'Courses with data':<20}: {len(responses)}")
    print(f"{'Records upserted':<20}: {written}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--full",
        action="store_true",
        help="Refetch every course and re-upsert every record (fills gaps, slow)",
    )
    args = parser.parse_args()
    try:
        asyncio.run(main_async(full=args.full))
    except KeyboardInterrupt:
        print("\n\n[INFO] Operation interrupted by user")


if __name__ == "__main__":
    main()
