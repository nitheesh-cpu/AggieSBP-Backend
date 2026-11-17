"""
Upsert script for GPA data from anex.us.

Default (incremental) mode only requests courses that were taught in a term
anex.us has published (per the sections table) but have no gpa_data rows for
that term, and only courses that already have some GPA history (courses that
never had grades on anex.us are mostly research/thesis/S-U courses).

anex.us blocks an IP after a burst of requests, so requests are paced and a
block is waited out with growing cooldowns instead of failing the run.

Usage:
    python pipelines/gpa/upsert_gpa_data.py --checkpoint gpa-progress.json
    python pipelines/gpa/upsert_gpa_data.py --include-no-history
    python pipelines/gpa/upsert_gpa_data.py --full --checkpoint gpa-full.json
"""

import argparse
import asyncio
import json
import math
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timedelta
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
    make_http_session,
    term_key,
)
from pipelines.gpa.schemas import GpaDataSchema

load_dotenv()

# Configuration
BULK_INSERT_SIZE = 2000  # Records per batch/commit
COURSE_BATCH_SIZE = 25  # Courses fetched between DB upserts/checkpoint saves
DEFAULT_COOLDOWN_MINUTES = 10.0  # First wait after a block; doubles each time
MAX_COOLDOWN_MINUTES = 60.0
MAX_CONSECUTIVE_BLOCKS = 6  # Give up (checkpoint kept) after this many in a row

# sections.term_code is YYYY + semester digit + campus digit, e.g. 202531
SECTION_SEMESTERS = {"1": "SPRING", "2": "SUMMER", "3": "FALL"}

Course = Tuple[str, str]
Term = Tuple[str, str]


def load_all_courses() -> List[Course]:
    session = get_session()
    try:
        return [
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
    finally:
        session.close()


def load_existing_terms() -> Dict[Course, Set[Term]]:
    """{course: set of (year, semester) already in gpa_data}"""
    session = get_session()
    try:
        existing: Dict[Course, Set[Term]] = defaultdict(set)
        for row in session.execute(
            text("SELECT DISTINCT dept, course_number, year, semester FROM gpa_data")
        ):
            existing[(row.dept, row.course_number)].add((row.year, row.semester))
        return existing
    finally:
        session.close()


def load_taught_terms() -> Dict[Course, Set[Term]]:
    """{course: set of (year, semester) it had sections in}, from the sections table."""
    session = get_session()
    try:
        taught: Dict[Course, Set[Term]] = defaultdict(set)
        for row in session.execute(
            text("""
                SELECT DISTINCT dept, course_number, term_code
                FROM sections
                WHERE course_number ~ '^[0-9]+$'
            """)
        ):
            semester = SECTION_SEMESTERS.get(row.term_code[4:5])
            if semester:
                taught[(row.dept, row.course_number)].add((row.term_code[:4], semester))
        return taught
    finally:
        session.close()


def select_missing_courses(
    taught: Dict[Course, Set[Term]],
    existing: Dict[Course, Set[Term]],
    newest_term: Term,
    include_no_history: bool,
) -> Tuple[List[Course], Dict[Term, int]]:
    """
    Courses taught in a published term (<= newest_term) with no gpa_data rows
    for that term. Returns the courses and a per-term count of missing pairs.
    """
    newest_key = term_key(*newest_term)
    to_fetch: List[Course] = []
    missing_by_term: Dict[Term, int] = defaultdict(int)
    for course in sorted(taught):
        stored = existing.get(course, set())
        if not stored and not include_no_history:
            continue
        missing = {
            t for t in taught[course] if term_key(*t) <= newest_key and t not in stored
        }
        if missing:
            to_fetch.append(course)
            for t in missing:
                missing_by_term[t] += 1
    return to_fetch, dict(missing_by_term)


def to_db_row(record: Dict[str, Any]) -> Dict[str, Any]:
    """Validate a scraped record and return the column dict for gpa_data."""
    return GpaDataSchema(**record).model_dump()


def chunks(lst: List[Any], n: int) -> Iterator[List[Any]]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def invalidate_gpa_caches(course_codes: Set[str]) -> None:
    """Clear cached API responses and website pages for courses with new GPA data.

    Professor pages show per-course grades too, so they refresh as well. Runs
    once at the end of a run. Never raises.
    """
    if not course_codes:
        return
    try:
        from aggiermp.core.cache_invalidation import invalidate_courses

        outcome = invalidate_courses(course_codes, include_professor_pages=True)
        print(
            f"[CACHE] {len(course_codes)} courses: {outcome.keys_deleted} keys cleared, "
            f"website {outcome.revalidation}",
            flush=True,
        )
    except Exception as exc:
        print(f"[CACHE] invalidation failed: {exc}", flush=True)


def bulk_upsert_records(records: List[Dict[str, Any]]) -> int:
    """
    Upsert records in batches on one connection. Raises on a failed batch
    instead of silently reporting 0 inserted.
    """
    # The same id twice in one INSERT ... ON CONFLICT DO UPDATE makes Postgres
    # reject the whole statement, so dedupe first (last one wins).
    rows = list({r["id"]: to_db_row(r) for r in records}.values())

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

    session = get_session()
    written = 0
    try:
        for batch in chunks(rows, BULK_INSERT_SIZE):
            # executemany form: SQLAlchemy batches this into multi-row INSERTs
            session.execute(stmt, batch)
            session.commit()
            written += len(batch)
    except Exception:
        session.rollback()
        print(f"[ERROR] Upsert failed after {written} records were committed")
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


class GaveUp(Exception):
    pass


class BlockHandler:
    """Waits out anex.us blocks with growing cooldowns and slows the pacer."""

    def __init__(self, pacer: RequestPacer, cooldown_minutes: float):
        self.pacer = pacer
        self.base_cooldown = cooldown_minutes * 60
        self.consecutive = 0
        self.total = 0

    async def fetch(self, http: Any, dept: str, number: str) -> Dict[str, Any]:
        while True:
            response = await fetch_course_data(http, dept, number, self.pacer)
            if not response.get("blocked"):
                self.consecutive = 0
                return response

            self.consecutive += 1
            self.total += 1
            if self.consecutive > MAX_CONSECUTIVE_BLOCKS:
                raise GaveUp(f"still blocked after {MAX_CONSECUTIVE_BLOCKS} cooldowns")
            cooldown = min(
                self.base_cooldown * 2 ** (self.consecutive - 1),
                MAX_COOLDOWN_MINUTES * 60,
            )
            cooldown = max(cooldown, response.get("retry_after") or 0)
            self.pacer.slow_down()
            resume_at = (datetime.now() + timedelta(seconds=cooldown)).strftime("%H:%M")
            print(
                f"[BLOCKED] {dept} {number}: {response['error']}. Cooling down "
                f"{cooldown / 60:.0f} min (until ~{resume_at}), then retrying at "
                f"{self.pacer.interval:.1f}s between requests.",
                flush=True,
            )
            await asyncio.sleep(cooldown)


async def main_async(
    full: bool,
    include_no_history: bool,
    request_interval: float,
    cooldown_minutes: float,
    checkpoint: Optional[Path],
) -> None:
    print("GPA Data Upsert" + (" (FULL)" if full else ""), flush=True)
    start_time = datetime.now()

    completed = load_checkpoint(checkpoint)
    pacer = RequestPacer(request_interval)
    blocks = BlockHandler(pacer, cooldown_minutes)

    async with make_http_session() as http:
        if full:
            courses = load_all_courses()
            skip_terms: Dict[Course, Set[Term]] = {}
        else:
            # MATH 151 is taught every term, so its newest term is anex's newest
            try:
                sample = await blocks.fetch(http, "MATH", "151")
            except GaveUp as exc:
                raise SystemExit(f"GPA UPSERT STOPPED: anex.us {exc}.") from None
            if not sample.get("success"):
                raise SystemExit(f"Could not read MATH 151 from anex.us: {sample.get('error')}")
            newest_class = max(
                sample["data"]["classes"],
                key=lambda c: term_key(c.get("year"), c.get("semester")),
            )
            newest = (str(newest_class["year"]), str(newest_class["semester"]).upper())
            print(f"[INFO] Newest term on anex.us: {newest[1]} {newest[0]}")

            skip_terms = load_existing_terms()
            courses, missing_by_term = select_missing_courses(
                load_taught_terms(), skip_terms, newest, include_no_history
            )
            for term in sorted(missing_by_term, key=lambda t: term_key(*t)):
                print(
                    f"  {term[1]:<6} {term[0]}: {missing_by_term[term]} "
                    "courses taught but missing"
                )

        await fetch_and_upsert(http, blocks, courses, completed, skip_terms, full, checkpoint)

    print(f"Finished in {datetime.now() - start_time}.")


async def fetch_and_upsert(
    http: Any,
    blocks: "BlockHandler",
    courses: List[Course],
    completed: Set[Course],
    skip_terms: Dict[Course, Set[Term]],
    full: bool,
    checkpoint: Optional[Path],
) -> None:
    touched: Set[str] = set()  # course codes that got records written
    try:
        await _fetch_and_upsert(
            http, blocks, courses, completed, skip_terms, full, checkpoint, touched
        )
    finally:
        # Also after a stop or error: batches already written are live.
        invalidate_gpa_caches(touched)


async def _fetch_and_upsert(
    http: Any,
    blocks: "BlockHandler",
    courses: List[Course],
    completed: Set[Course],
    skip_terms: Dict[Course, Set[Term]],
    full: bool,
    checkpoint: Optional[Path],
    touched: Set[str],
) -> None:
    request_interval = blocks.pacer.interval
    to_fetch = [c for c in courses if c not in completed]
    print(
        f"[INFO] {len(to_fetch)} courses to fetch ({len(courses) - len(to_fetch)} already "
        f"done in checkpoint), ~{len(to_fetch) * request_interval * 1.25 / 3600:.1f}h "
        f"at {request_interval:g}s/request before any cooldowns",
        flush=True,
    )
    if not to_fetch:
        print("[OK] Nothing to fetch.")
        return

    written = 0
    processed = 0
    failed: List[str] = []
    fetch_start = time.monotonic()

    for batch in chunks(to_fetch, COURSE_BATCH_SIZE):
        records: List[Dict[str, Any]] = []
        finished: Set[Course] = set()
        stop: Optional[BaseException] = None
        for course in batch:
            dept, number = course
            try:
                response = await blocks.fetch(http, dept, number)
            except (asyncio.CancelledError, KeyboardInterrupt, GaveUp) as exc:
                stop = exc
                break
            try:
                if response.get("success"):
                    records.extend(
                        extract_class_records(
                            response, skip_terms.get(course), strict=full
                        )
                    )
                elif not response.get("no_data"):
                    raise ValueError(response["error"])
            except ValueError as exc:
                # Not checkpointed, so the next run tries it again
                failed.append(f"{dept} {number}: {exc}")
                print(f"[ERROR] {dept} {number}: {exc}", flush=True)
            else:
                finished.add(course)

        # Save what this batch got before stopping or moving on
        if records:
            written += bulk_upsert_records(records)
            touched.update(f"{r['dept']}{r['course_number']}" for r in records)
        completed.update(finished)
        if checkpoint is not None and finished:
            save_checkpoint(checkpoint, completed)
        processed += len(finished)

        elapsed = time.monotonic() - fetch_start
        rate = processed / elapsed if elapsed else 0
        eta = (len(to_fetch) - processed) / rate / 3600 if rate else 0
        print(
            f"[INFO] {processed}/{len(to_fetch)} courses, {written} records upserted, "
            f"~{eta:.1f}h left",
            flush=True,
        )

        if isinstance(stop, GaveUp):
            raise SystemExit(
                f"GPA UPSERT STOPPED: anex.us {stop}."
                + (f" Resume later with --checkpoint {checkpoint}" if checkpoint else "")
            )
        if stop is not None:
            raise stop

    print(
        f"GPA UPSERT COMPLETE. {written} records upserted, "
        f"{blocks.total} blocks waited out."
    )
    if failed:
        print(f"[WARNING] {len(failed)} courses failed and will be retried next run:")
        for line in failed[:20]:
            print(f"    {line}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Refetch every course in the courses table and re-upsert every record",
    )
    parser.add_argument(
        "--include-no-history",
        action="store_true",
        help="Also fetch taught courses that have never had any GPA data",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=DEFAULT_REQUEST_INTERVAL,
        metavar="SECONDS",
        help="Minimum time between requests (up to 50%% random extra is added)",
    )
    parser.add_argument(
        "--cooldown",
        type=float,
        default=DEFAULT_COOLDOWN_MINUTES,
        metavar="MINUTES",
        help="First wait after anex.us blocks us; doubles on each consecutive block",
    )
    parser.add_argument(
        "--checkpoint",
        type=Path,
        help="Save/resume progress in this file. Use a new file for each fresh run.",
    )
    args = parser.parse_args()
    for name in ("request_interval", "cooldown"):
        value = getattr(args, name)
        if not math.isfinite(value) or value <= 0:
            parser.error(f"--{name.replace('_', '-')} must be finite and greater than zero")
    if args.full and args.include_no_history:
        parser.error("--include-no-history has no effect with --full")
    if args.checkpoint is not None and not args.checkpoint.parent.is_dir():
        parser.error("--checkpoint must be in an existing directory")
    try:
        asyncio.run(
            main_async(
                args.full,
                args.include_no_history,
                args.request_interval,
                args.cooldown,
                args.checkpoint,
            )
        )
    except KeyboardInterrupt:
        print("\n[INFO] Operation interrupted by user")
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
