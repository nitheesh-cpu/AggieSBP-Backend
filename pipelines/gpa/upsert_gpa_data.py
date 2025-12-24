"""
Upsert script for GPA data from anex.us.

This script:
1. Queries the database to find missing courses and new semesters
2. Fetches missing data in parallel
3. Bulk inserts the data into gpa_data table
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

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
)
from pipelines.gpa.schemas import GpaDataSchema

load_dotenv()

# Configuration
BULK_INSERT_SIZE = 5000  # Records per batch


def get_missing_courses_and_semesters() -> List[Tuple[str, str, str, str]]:
    """
    Query database to find:
    1. Courses in courses table that don't exist in gpa_data
    2. New semesters that haven't been fetched

    Returns list of (dept, course_number, year, semester) tuples.
    """
    session = get_session()

    try:
        # Query 1: Find courses that exist in courses table but not in gpa_data
        missing_courses_query = text("""
            SELECT DISTINCT 
                c.subject_id as dept,
                c.course_number,
                CAST(c.course_number AS INTEGER) as course_number_int,
                NULL::text as year,
                NULL::text as semester
            FROM courses c
            WHERE c.subject_id IS NOT NULL 
              AND c.course_number IS NOT NULL
              AND c.course_number ~ '^[0-9]+$'
              AND NOT EXISTS (
                  SELECT 1 
                  FROM gpa_data gd 
                  WHERE gd.dept = c.subject_id 
                    AND gd.course_number = c.course_number
              )
            ORDER BY dept, course_number_int
        """)

        result = session.execute(missing_courses_query)
        missing_courses = [(row.dept, row.course_number, None, None) for row in result]

        # Query 2: Find courses that exist but are missing newer semesters
        # Get the latest year/semester in gpa_data for each course
        # Latest = highest year, and if same year, highest semester priority (SUMMER=1 > SPRING=2 > FALL=3)
        latest_semesters_query = text("""
            WITH latest_semesters AS (
                SELECT DISTINCT ON (dept, course_number)
                    dept,
                    course_number,
                    year,
                    semester
                FROM gpa_data
                ORDER BY 
                    dept, 
                    course_number,
                    year::int DESC,
                    CASE semester 
                        WHEN 'SUMMER' THEN 1 
                        WHEN 'SPRING' THEN 2 
                        WHEN 'FALL' THEN 3 
                        ELSE 4 
                    END ASC
            )
            SELECT 
                c.subject_id as dept,
                c.course_number,
                CAST(c.course_number AS INTEGER) as course_number_int,
                ls.year as latest_year,
                ls.semester as latest_semester
            FROM courses c
            JOIN latest_semesters ls ON c.subject_id = ls.dept AND c.course_number = ls.course_number
            WHERE c.subject_id IS NOT NULL 
              AND c.course_number IS NOT NULL
              AND c.course_number ~ '^[0-9]+$'
            ORDER BY dept, course_number_int
        """)

        result = session.execute(latest_semesters_query)
        courses_with_data = [
            (row.dept, row.course_number, row.latest_year, row.latest_semester)
            for row in result
        ]

        print(f"[INFO] Found {len(missing_courses)} courses with no GPA data")
        print(f"[INFO] Found {len(courses_with_data)} courses with existing GPA data")

        # Combine both lists
        all_courses_to_fetch = missing_courses + courses_with_data

        return all_courses_to_fetch

    except Exception as e:
        print(f"[ERROR] Error querying database: {e}")
        import traceback

        traceback.print_exc()
        return []
    finally:
        session.close()


def convert_to_schema(record_dict: Dict[str, Any]) -> GpaDataSchema:
    """
    Convert a dictionary record to GpaDataSchema.
    """
    return GpaDataSchema(
        id=record_dict["id"],
        dept=record_dict["dept"],
        course_number=record_dict["course_number"],
        section=record_dict["section"],
        professor=record_dict["professor"],
        year=record_dict["year"],
        semester=record_dict["semester"],
        gpa=record_dict.get("gpa"),
        grade_a=record_dict.get("grade_a", 0),
        grade_b=record_dict.get("grade_b", 0),
        grade_c=record_dict.get("grade_c", 0),
        grade_d=record_dict.get("grade_d", 0),
        grade_f=record_dict.get("grade_f", 0),
        grade_i=record_dict.get("grade_i", 0),
        grade_s=record_dict.get("grade_s", 0),
        grade_u=record_dict.get("grade_u", 0),
        grade_q=record_dict.get("grade_q", 0),
        grade_x=record_dict.get("grade_x", 0),
        total_students=record_dict.get("total_students", 0),
    )


def bulk_insert_records(records: List[Dict[str, Any]]) -> int:
    """
    Insert a batch of records into the database using bulk insert with ON CONFLICT.
    """
    if not records:
        return 0

    session = get_session()
    try:
        # Convert to schema and then to dict for database
        gpa_records = []
        for record in records:
            schema = convert_to_schema(record)
            gpa_records.append(schema.dict())

        stmt = insert(GpaDataDB).values(gpa_records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_=dict(
                gpa=stmt.excluded.gpa,
                grade_a=stmt.excluded.grade_a,
                grade_b=stmt.excluded.grade_b,
                grade_c=stmt.excluded.grade_c,
                grade_d=stmt.excluded.grade_d,
                grade_f=stmt.excluded.grade_f,
                grade_i=stmt.excluded.grade_i,
                grade_s=stmt.excluded.grade_s,
                grade_u=stmt.excluded.grade_u,
                grade_q=stmt.excluded.grade_q,
                grade_x=stmt.excluded.grade_x,
                total_students=stmt.excluded.total_students,
                updated_at=stmt.excluded.updated_at,
            ),
        )
        session.execute(stmt)
        session.commit()
        return len(records)

    except Exception as e:
        print(f"[ERROR] Error in bulk insert: {e}")
        session.rollback()
        return 0
    finally:
        session.close()


def chunks(lst: List, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def main_async():
    """
    Main async function - fetch missing data then bulk insert.
    """
    print("=" * 60)
    print("GPA Data Fetch - Missing Courses and New Semesters")
    print("=" * 60)
    print(f"[INFO] Max concurrent requests: {MAX_CONCURRENT_REQUESTS}")
    print(f"[INFO] Bulk insert size: {BULK_INSERT_SIZE} records")
    print("=" * 60)

    # Step 1: Get newest semester from anex.us
    print("\n[STEP 1] Fetching newest semester from anex.us...")
    import aiohttp

    async with aiohttp.ClientSession() as session:
        newest_year, newest_semester = await get_newest_semester(session)
        if newest_year and newest_semester:
            print(f"[OK] Newest semester found: {newest_semester} {newest_year}")
        else:
            print(
                "[WARNING] Could not determine newest semester, will fetch all missing data"
            )

    # Step 2: Find missing courses and semesters
    print("\n[STEP 2] Finding missing courses and semesters in database...")
    courses_to_fetch = get_missing_courses_and_semesters()

    if not courses_to_fetch:
        print("[OK] No missing courses found. All courses have GPA data!")
        return

    print(f"[INFO] Found {len(courses_to_fetch)} courses to fetch")

    # Step 3: Fetch all course data concurrently
    print("\n[STEP 3] Fetching course data from anex.us...")
    start_time = datetime.now()
    successful_responses = await fetch_all_courses_concurrent(courses_to_fetch)

    if not successful_responses:
        print("[ERROR] No successful responses received!")
        return

    # Step 4: Extract all class records
    print(f"\n[STEP 4] Processing {len(successful_responses)} successful responses...")
    all_records = []

    # Create a map of course to latest semester for filtering
    course_latest_semester = {
        (dept, number): (year, semester)
        for dept, number, year, semester in courses_to_fetch
        if year and semester
    }

    for response in successful_responses:
        course = response.get("course")
        if course:
            dept, number = course
            latest = course_latest_semester.get((dept, number))
            if latest:
                year, semester = latest
                records = extract_class_records(response, year, semester)
            else:
                # No existing data, get all records
                records = extract_class_records(response)
            all_records.extend(records)

    print(f"[INFO] Extracted {len(all_records)} total class records")

    if not all_records:
        print("[WARNING] No records to insert!")
        return

    # Step 5: Bulk insert in chunks
    print(
        f"\n[STEP 5] Bulk inserting records ({BULK_INSERT_SIZE} records per batch)..."
    )

    total_inserted = 0
    chunk_num = 0

    for chunk in chunks(all_records, BULK_INSERT_SIZE):
        chunk_num += 1
        chunk_size = len(chunk)

        print(f"[INFO] Inserting batch {chunk_num} ({chunk_size} records)...", end=" ")

        inserted = bulk_insert_records(chunk)
        total_inserted += inserted

        if inserted == chunk_size:
            print("[OK]")
        else:
            print(f"[WARNING] {inserted}/{chunk_size} inserted")

    # Final summary
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 60)
    print("[OK] Operation Complete!")
    print("=" * 60)
    print(f"[INFO] Duration: {duration}")
    print(f"[INFO] Courses processed: {len(successful_responses)}")
    print(f"[INFO] Records inserted: {total_inserted}")
    print(
        f"[INFO] Speed: {len(courses_to_fetch) / duration.total_seconds():.2f} courses/second"
    )

    if total_inserted > 0:
        print(f"\n[OK] SUCCESS! {total_inserted} GPA records added to database!")


def main():
    """Main function wrapper"""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\n\n[INFO] Operation interrupted by user")
    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
