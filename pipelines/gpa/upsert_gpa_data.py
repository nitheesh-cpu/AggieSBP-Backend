"""
Upsert script for GPA data from anex.us.

This script:
1. Finds the newest term anex.us has published
2. Reads which (course, term) pairs are already in gpa_data
3. Fetches courses at a configurable pace
4. Bulk upserts each batch and optionally saves a full-run checkpoint

Usage:
    python pipelines/gpa/upsert_gpa_data.py          # incremental
    python pipelines/gpa/upsert_gpa_data.py --full --checkpoint gpa-progress.json
"""

import argparse
import asyncio
import json
import math
import sys
import tempfile
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
    DEFAULT_REQUEST_INTERVAL,
    RequestPacer,
    extract_class_records,
    fetch_course_data,
    get_newest_semester,
    make_http_session,
    term_key,
)
from pipelines.gpa.schemas import GpaDataSchema

load_dotenv()

# Configuration
BULK_INSERT_SIZE = 2000  # Records per batch/commit
COURSE_BATCH_SIZE = 50

Course = Tuple[str, str]
Term = Tuple[str, str]


def load_courses_and_existing_terms(
    full: bool = False,
) -> Tuple[List[Course], Dict[Course, Set[Term]]]:
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
        if full:
            return courses, existing_terms

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


def load_checkpoint(path: Optional[Path]) -> Set[Course]:
    if path is None or not path.exists():
        return set()
    payload = json.loads(path.read_text(encoding="utf-8"))
    courses = payload.get("completed_courses") if isinstance(payload, dict) else None
    if not isinstance(courses, list) or any(
        not isinstance(course, list)
        or len(course) != 2
        or not all(isinstance(value, str) for value in course)
        for course in courses
    ):
        raise ValueError(f"Invalid GPA checkpoint: {path}")
    return {(dept, number) for dept, number in courses}


def save_checkpoint(path: Path, completed: Set[Course]) -> None:
    temp = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    )
    temporary_path = Path(temp.name)
    try:
        with temp:
            json.dump({"completed_courses": sorted(completed)}, temp)
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


async def main_async(
    full: bool = False,
    request_interval: float = DEFAULT_REQUEST_INTERVAL,
    checkpoint: Optional[Path] = None,
) -> None:
    print("GPA Data Upsert" + (" (FULL)" if full else ""))
    print(f"[INFO] Request interval: {request_interval:g}s")
    start_time = datetime.now()

    completed = load_checkpoint(checkpoint)
    courses, existing_terms = load_courses_and_existing_terms(full=full)
    newest_term = None
    pacer = RequestPacer(request_interval)
    async with make_http_session() as http:
        if not full:
            newest_term = await get_newest_semester(http, pacer)
            if newest_term:
                print(f"[OK] Newest semester on anex.us: {newest_term[1]} {newest_term[0]}")
            else:
                print("[WARNING] Could not determine newest semester. Fetching all courses.")

        to_fetch = [
            course
            for course in select_courses_to_fetch(courses, existing_terms, newest_term, full)
            if course not in completed
        ]
        print(
            f"[INFO] {len(to_fetch)} of {len(courses)} courses to fetch"
            f" ({len(completed)} in checkpoint)",
            flush=True,
        )

        written = 0
        processed = 0
        errors = 0
        stop_fetching = False
        for batch in chunks(to_fetch, COURSE_BATCH_SIZE):
            records: List[Dict[str, Any]] = []
            finished: Set[Course] = set()
            for dept, number in batch:
                course = (dept, number)
                response = await fetch_course_data(http, dept, number, pacer)
                try:
                    if response.get("success"):
                        records.extend(
                            extract_class_records(response, existing_terms.get(course), strict=full)
                        )
                    elif not response.get("no_data"):
                        raise ValueError(response["error"])
                except ValueError as exc:
                    errors += 1
                    print(f"[ERROR] {dept} {number}: {exc}", flush=True)
                    if full or response.get("retries_exhausted"):
                        stop_fetching = True
                        break
                else:
                    finished.add(course)

            if records:
                written += bulk_upsert_records(records)
            completed.update(finished)
            if checkpoint is not None and finished:
                save_checkpoint(checkpoint, completed)
            processed += len(finished)
            print(f"[INFO] Processed {processed}/{len(to_fetch)} remaining courses", flush=True)
            if stop_fetching:
                break

    if errors:
        raise SystemExit(
            f"GPA UPSERT INCOMPLETE. {errors} course fetch or parsing errors."
            + (f" Resume with the same checkpoint: {checkpoint}" if checkpoint else "")
        )
    print(f"GPA UPSERT COMPLETE. {written} records upserted in {datetime.now() - start_time}.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--full",
        action="store_true",
        help="Refetch every course and re-upsert every record",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=DEFAULT_REQUEST_INTERVAL,
        metavar="SECONDS",
        help="Minimum time between request starts (default: %(default)s)",
    )
    parser.add_argument(
        "--checkpoint",
        type=Path,
        help="Save or resume a full run using this file. Use a new file for each fresh run.",
    )
    args = parser.parse_args()
    if not math.isfinite(args.request_interval) or args.request_interval <= 0:
        parser.error("--request-interval must be finite and greater than zero")
    if args.checkpoint is not None and not args.full:
        parser.error("--checkpoint requires --full")
    if args.checkpoint is not None and not args.checkpoint.parent.is_dir():
        parser.error("--checkpoint must be in an existing directory")
    try:
        asyncio.run(main_async(args.full, args.request_interval, args.checkpoint))
    except KeyboardInterrupt:
        print("\n[INFO] Operation interrupted by user")
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
