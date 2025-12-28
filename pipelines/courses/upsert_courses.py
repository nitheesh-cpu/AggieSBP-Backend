"""
Upsert script for departments and courses to new optimized tables.

This script:
1. Scrapes all departments from the TAMU course catalog
2. Upserts departments to `departments` table
3. For each department, scrapes all courses (in parallel)
4. Bulk upserts courses to `courses` table

Optimized with:
- Parallel scraping using asyncio and ThreadPoolExecutor
- Bulk database operations
- Collect all data first, then upsert in batches
"""

import asyncio
import importlib.util
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import ARRAY, insert
from sqlalchemy.ext.declarative import declarative_base

# Import from course-catalog-scraping module (hyphenated filename)
course_scraping_path = Path(__file__).parent / "course_catalog_scraping.py"
spec = importlib.util.spec_from_file_location(
    "course_catalog_scraping", course_scraping_path
)
course_catalog_scraping = importlib.util.module_from_spec(spec)
spec.loader.exec_module(course_catalog_scraping)
get_all_departments = course_catalog_scraping.get_all_departments
get_courses_from_department = course_catalog_scraping.get_courses_from_department

from dotenv import load_dotenv

from aggiermp.database.base import create_db_engine, get_session
from pipelines.courses.schemas import CourseSchema, DepartmentSchema

load_dotenv()

# Create new Base for new tables
BaseNew = declarative_base()


class DepartmentNewDB(BaseNew):
    """Optimized department table without redundant columns."""

    __tablename__ = "departments"

    id = Column(String, primary_key=True)
    title = Column(String, nullable=False)
    long_name = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self):
        return f"<DepartmentNew(id='{self.id}', title='{self.title}', long_name='{self.long_name}')>"


class CourseNewDB(BaseNew):
    """Optimized course table matching CourseSchema structure."""

    __tablename__ = "courses"

    # Basic course identification
    id = Column(String, primary_key=True)
    code = Column(String, nullable=False)  # e.g., "CSCE 221"
    name = Column(String, nullable=False)  # Course name/title

    # Department information
    subject_id = Column(String, ForeignKey("departments.id"), nullable=False)
    subject_long_name = Column(String, nullable=False)
    # Note: subject_short_name is redundant (always equals subject_id), so omitted

    # Course number
    course_number = Column(String, nullable=False)

    # Credit hours and scheduling
    credits = Column(Integer, nullable=True)
    lecture_hours = Column(Integer, nullable=True)
    lab_hours = Column(Integer, nullable=True)
    other_hours = Column(Integer, nullable=True)

    # Course description
    description = Column(Text, nullable=True)

    # Prerequisites (stored as JSON strings or arrays)
    prerequisites = Column(Text, nullable=True)  # Text description
    prerequisite_courses = Column(ARRAY(String), nullable=True)  # Flat list
    prerequisite_groups = Column(Text, nullable=True)  # JSON string for nested lists

    # Corequisites
    corequisites = Column(Text, nullable=True)  # Text description
    corequisite_courses = Column(ARRAY(String), nullable=True)  # Flat list
    corequisite_groups = Column(Text, nullable=True)  # JSON string for nested lists

    # Cross-listings
    cross_listings = Column(ARRAY(String), nullable=True)

    # Legacy fields for compatibility (can be derived from above)
    course_topic = Column(String, nullable=True)
    course_display_title = Column(String, nullable=True)  # Can be generated
    course_title = Column(String, nullable=True)  # Same as name
    course_title_long = Column(String, nullable=True)  # Can be generated
    has_corequisites = Column(Boolean, nullable=True)  # Based on corequisite_courses
    has_prerequisites = Column(Boolean, nullable=True)  # Based on prerequisite_courses

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self):
        return f"<CourseNew(id='{self.id}', code='{self.code}', name='{self.name}')>"


def drop_new_tables():
    """Drop the new tables if they exist."""
    engine = create_db_engine()
    BaseNew.metadata.drop_all(engine)
    print("✓ Dropped existing tables: departments, courses")


def create_new_tables():
    """Create the new optimized tables if they don't exist."""
    engine = create_db_engine()
    BaseNew.metadata.create_all(engine)
    print("✓ Created/verified new tables: departments, courses")


def convert_department_to_schema(dept_dict: dict) -> DepartmentSchema:
    """Convert scraped department dict to DepartmentSchema."""
    # Format: dept_dict has 'id', 'title', and 'long_name'
    return DepartmentSchema(
        id=dept_dict["id"],
        title=dept_dict["title"],
        long_name=dept_dict["long_name"],
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )


def convert_course_to_schema(
    course_dict: dict, department_code: str, department_long_name: str
) -> CourseSchema:
    """Convert scraped course dict to CourseSchema."""
    # Extract course number from code (e.g., "CSCE 221" -> "221")
    code = course_dict.get("code", "")
    course_number = code.split()[-1] if code else ""

    # Generate course ID: subject_code + course_number (e.g., "CSCE221")
    course_id = f"{department_code}{course_number}"

    return CourseSchema(
        id=course_id,
        code=code,
        name=course_dict.get("name", ""),
        subject_short_name=department_code,
        subject_long_name=department_long_name,
        subject_id=department_code,  # subject_id = subject_short_name (they're always equal)
        course_number=course_number,
        credits=course_dict.get("credits"),
        lecture_hours=course_dict.get("lecture_hours"),
        lab_hours=course_dict.get("lab_hours"),
        other_hours=course_dict.get("other_hours"),
        description=course_dict.get("description"),
        prerequisites=course_dict.get("prerequisites"),
        prerequisite_courses=course_dict.get("prerequisite_courses", []),
        prerequisite_groups=course_dict.get("prerequisite_groups", []),
        corequisites=course_dict.get("corequisites"),
        corequisite_courses=course_dict.get("corequisite_courses", []),
        corequisite_groups=course_dict.get("corequisite_groups", []),
        cross_listings=course_dict.get("cross_listings", []),
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )


def upsert_departments(departments: list[DepartmentSchema]):
    """Upsert departments to departments table."""
    session = get_session()

    try:
        # Ensure transaction is clean
        try:
            if session.in_transaction():
                session.execute(text("SELECT 1"))
        except Exception:
            session.rollback()

        # Get all existing departments
        existing_depts = session.execute(select(DepartmentNewDB)).scalars().all()
        existing_ids = {dept.id for dept in existing_depts}

        insert_departments = []
        update_departments = []

        for dept in departments:
            # Only include fields that exist in DepartmentNewDB
            dept_dict = {
                "id": dept.id,
                "title": dept.title,
                "long_name": dept.long_name,
                "created_at": dept.created_at,
                "updated_at": dept.updated_at,
            }

            if dept.id in existing_ids:
                # Update existing
                update_departments.append(
                    {
                        "id": dept.id,
                        "title": dept.title,
                        "long_name": dept.long_name,
                        "updated_at": datetime.now(),
                    }
                )
            else:
                # Insert new
                insert_departments.append(dept_dict)

        if insert_departments:
            session.execute(insert(DepartmentNewDB), insert_departments)
            print(f"  ✓ Inserted {len(insert_departments)} new departments")

        if update_departments:
            session.execute(update(DepartmentNewDB), update_departments)
            print(f"  ✓ Updated {len(update_departments)} existing departments")

        session.commit()
        return len(insert_departments) + len(update_departments)

    except Exception as e:
        try:
            session.rollback()
        except Exception:
            pass
        print(f"  ✗ Error upserting departments: {e}")
        raise
    finally:
        session.close()


def convert_course_to_dict(course: CourseSchema, existing_ids: set) -> dict:
    """Convert CourseSchema to database dict format."""
    # Convert CourseSchema to database format
    course_dict = {
        # Basic identification
        "id": course.id,
        "code": course.code,
        "name": course.name,
        # Department information
        "subject_id": course.subject_id,
        "subject_long_name": course.subject_long_name,
        # Course number
        "course_number": course.course_number,
        # Credit hours and scheduling
        "credits": course.credits,
        "lecture_hours": course.lecture_hours,
        "lab_hours": course.lab_hours,
        "other_hours": course.other_hours,
        # Description
        "description": course.description,
        # Prerequisites
        "prerequisites": course.prerequisites,
        "prerequisite_courses": course.prerequisite_courses
        if course.prerequisite_courses
        else None,
        "prerequisite_groups": json.dumps(course.prerequisite_groups)
        if course.prerequisite_groups
        else None,
        # Corequisites
        "corequisites": course.corequisites,
        "corequisite_courses": course.corequisite_courses
        if course.corequisite_courses
        else None,
        "corequisite_groups": json.dumps(course.corequisite_groups)
        if course.corequisite_groups
        else None,
        # Cross-listings
        "cross_listings": course.cross_listings if course.cross_listings else None,
        # Legacy/compatibility fields (derived from above)
        "course_topic": None,  # Not in schema
        "course_display_title": f"{course.course_number} {course.name}",
        "course_title": course.name,
        # course_title_long format: "ACCT - Accounting 329 - Cost Management and Analysis"
        # subject_long_name already includes the code (e.g., "ACCT - Accounting")
        # So we just need: subject_long_name + course_number + course name
        "course_title_long": f"{course.subject_long_name} {course.course_number} - {course.name}",
        # Set has_corequisites and has_prerequisites based on whether arrays have data
        "has_corequisites": bool(course.corequisite_courses),
        "has_prerequisites": bool(course.prerequisite_courses),
        # Timestamps
        "created_at": course.created_at,
        "updated_at": course.updated_at,
    }

    if course.id in existing_ids:
        # Update existing
        course_dict["updated_at"] = datetime.now()
        return course_dict, "update"
    else:
        # Insert new
        return course_dict, "insert"


def bulk_upsert_courses(all_courses: List[CourseSchema], batch_size: int = 1000):
    """Bulk upsert all courses to courses table using PostgreSQL ON CONFLICT."""
    session = get_session()

    try:
        # Ensure transaction is clean
        try:
            if session.in_transaction():
                session.execute(text("SELECT 1"))
        except Exception:
            session.rollback()
        print(f"\n  Preparing bulk upsert for {len(all_courses)} courses...")

        # Convert all courses to dicts (no need to check existing - ON CONFLICT handles it)
        all_course_dicts = []
        for course in all_courses:
            course_dict, _ = convert_course_to_dict(course, set())
            # Always set updated_at for upsert
            course_dict["updated_at"] = datetime.now()
            all_course_dicts.append(course_dict)

        print(
            f"  Upserting {len(all_course_dicts)} courses in batches of {batch_size}..."
        )

        # Use PostgreSQL's ON CONFLICT for true upsert
        # Execute in batches for better performance
        total_processed = 0
        for i in range(0, len(all_course_dicts), batch_size):
            batch = all_course_dicts[i : i + batch_size]
            # Use PostgreSQL's insert().on_conflict_do_update() for true upsert
            stmt = insert(CourseNewDB).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "code": stmt.excluded.code,
                    "name": stmt.excluded.name,
                    "subject_id": stmt.excluded.subject_id,
                    "subject_long_name": stmt.excluded.subject_long_name,
                    "course_number": stmt.excluded.course_number,
                    "credits": stmt.excluded.credits,
                    "lecture_hours": stmt.excluded.lecture_hours,
                    "lab_hours": stmt.excluded.lab_hours,
                    "other_hours": stmt.excluded.other_hours,
                    "description": stmt.excluded.description,
                    "prerequisites": stmt.excluded.prerequisites,
                    "prerequisite_courses": stmt.excluded.prerequisite_courses,
                    "prerequisite_groups": stmt.excluded.prerequisite_groups,
                    "corequisites": stmt.excluded.corequisites,
                    "corequisite_courses": stmt.excluded.corequisite_courses,
                    "corequisite_groups": stmt.excluded.corequisite_groups,
                    "cross_listings": stmt.excluded.cross_listings,
                    "course_topic": stmt.excluded.course_topic,
                    "course_display_title": stmt.excluded.course_display_title,
                    "course_title": stmt.excluded.course_title,
                    "course_title_long": stmt.excluded.course_title_long,
                    "has_corequisites": stmt.excluded.has_corequisites,
                    "has_prerequisites": stmt.excluded.has_prerequisites,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
            session.execute(stmt)
            total_processed += len(batch)
            print(f"    Processed batch {i // batch_size + 1} ({len(batch)} courses)")

        session.commit()
        print(f"  ✓ Successfully upserted {total_processed} courses")
        return total_processed

    except Exception as e:
        try:
            session.rollback()
        except Exception:
            pass
        print(f"  ✗ Error bulk upserting courses: {e}")
        import traceback

        traceback.print_exc()
        raise
    finally:
        session.close()


def upsert_courses(courses: list[CourseSchema], department_code: str):
    """Upsert courses to courses table (legacy function - kept for compatibility)."""
    session = get_session()

    try:
        # Ensure transaction is clean
        try:
            if session.in_transaction():
                session.execute(text("SELECT 1"))
        except Exception:
            session.rollback()
        # Get all existing courses
        existing_courses = session.execute(select(CourseNewDB)).scalars().all()
        existing_ids = {course.id for course in existing_courses}

        insert_courses = []
        update_courses = []

        for course in courses:
            # Convert CourseSchema to database format
            course_dict = {
                # Basic identification
                "id": course.id,
                "code": course.code,
                "name": course.name,
                # Department information
                "subject_id": course.subject_id,
                "subject_long_name": course.subject_long_name,
                # Course number
                "course_number": course.course_number,
                # Credit hours and scheduling
                "credits": course.credits,
                "lecture_hours": course.lecture_hours,
                "lab_hours": course.lab_hours,
                "other_hours": course.other_hours,
                # Description
                "description": course.description,
                # Prerequisites
                "prerequisites": course.prerequisites,
                "prerequisite_courses": course.prerequisite_courses
                if course.prerequisite_courses
                else None,
                "prerequisite_groups": json.dumps(course.prerequisite_groups)
                if course.prerequisite_groups
                else None,
                # Corequisites
                "corequisites": course.corequisites,
                "corequisite_courses": course.corequisite_courses
                if course.corequisite_courses
                else None,
                "corequisite_groups": json.dumps(course.corequisite_groups)
                if course.corequisite_groups
                else None,
                # Cross-listings
                "cross_listings": course.cross_listings
                if course.cross_listings
                else None,
                # Legacy/compatibility fields (derived from above)
                "course_topic": None,  # Not in schema
                "course_display_title": f"{course.course_number} {course.name}",
                "course_title": course.name,
                # course_title_long format: "ACCT - Accounting 329 - Cost Management and Analysis"
                # subject_long_name already includes the code (e.g., "ACCT - Accounting")
                # So we just need: subject_long_name + course_number + course name
                "course_title_long": f"{course.subject_long_name} {course.course_number} - {course.name}",
                # Set has_corequisites and has_prerequisites based on whether arrays have data
                "has_corequisites": bool(course.corequisite_courses),
                "has_prerequisites": bool(course.prerequisite_courses),
                # Timestamps
                "created_at": course.created_at,
                "updated_at": course.updated_at,
            }

            if course.id in existing_ids:
                # Update existing
                course_dict["updated_at"] = datetime.now()
                update_courses.append(course_dict)
            else:
                # Insert new
                insert_courses.append(course_dict)

        if insert_courses:
            session.execute(insert(CourseNewDB), insert_courses)
            print(f"    ✓ Inserted {len(insert_courses)} new courses")

        if update_courses:
            session.execute(update(CourseNewDB), update_courses)
            print(f"    ✓ Updated {len(update_courses)} existing courses")

        session.commit()
        return len(insert_courses) + len(update_courses)

    except Exception as e:
        try:
            session.rollback()
        except Exception:
            pass
        print(f"    ✗ Error upserting courses for {department_code}: {e}")
        raise
    finally:
        session.close()


async def scrape_department_courses(
    dept: dict, executor: ThreadPoolExecutor
) -> Tuple[str, str, List[dict]]:
    """Scrape courses for a single department (async wrapper)."""
    dept_code = dept["id"]
    dept_url = dept["url"]

    # Run synchronous scraping in thread pool
    loop = asyncio.get_event_loop()
    try:
        courses_raw = await loop.run_in_executor(
            executor, get_courses_from_department, dept_url
        )
        return dept_code, dept["long_name"], courses_raw
    except Exception as e:
        print(f"      ✗ Error scraping {dept_code}: {e}")
        return dept_code, dept["long_name"], []


async def scrape_all_courses_parallel(
    departments_raw: List[dict], max_concurrent: int = 10
) -> List[Tuple[str, str, List[dict]]]:
    """Scrape courses for all departments in parallel."""
    print(f"\n  Scraping courses with {max_concurrent} concurrent workers...")

    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)

    async def scrape_with_semaphore(dept: dict, executor: ThreadPoolExecutor):
        async with semaphore:
            return await scrape_department_courses(dept, executor)

    # Use ThreadPoolExecutor for I/O-bound scraping
    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        tasks = [scrape_with_semaphore(dept, executor) for dept in departments_raw]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out exceptions and return successful results
    successful_results = []
    failed_count = 0

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            failed_count += 1
            dept_code = departments_raw[i]["id"]
            print(f"      ✗ Failed to scrape {dept_code}: {result}")
        else:
            successful_results.append(result)

    if failed_count > 0:
        print(f"  ⚠ {failed_count} departments failed to scrape")

    return successful_results


def main():
    """Main function to scrape and upsert all departments and courses."""
    import time

    start_time = time.time()
    print("=" * 60)
    print("Starting Optimized Department and Course Upsert Process")
    print("=" * 60)

    # Create tables if they don't exist (proper upsert - don't drop existing data)
    print("\n1. Creating/verifying tables (if they don't exist)...")
    create_new_tables()

    # Get all departments
    print("\n2. Scraping all departments...")
    departments_raw = get_all_departments()
    print(f"   Found {len(departments_raw)} departments")

    if not departments_raw:
        print("   ✗ No departments found. Exiting.")
        return

    # Convert to schemas
    print("\n3. Converting departments to schemas...")
    departments = [convert_department_to_schema(dept) for dept in departments_raw]

    # Upsert departments
    print("\n4. Upserting departments to departments...")
    dept_count = upsert_departments(departments)
    print(f"   ✓ Total departments processed: {dept_count}")

    # PHASE 1: Scrape all courses in parallel
    print("\n5. PHASE 1: Scraping all courses in parallel...")
    all_courses_data = asyncio.run(
        scrape_all_courses_parallel(departments_raw, max_concurrent=10)
    )

    total_courses_scraped = sum(len(courses) for _, _, courses in all_courses_data)
    print(
        f"   ✓ Scraped {total_courses_scraped} total courses from {len(all_courses_data)} departments"
    )

    # PHASE 2: Convert all courses to schemas
    print("\n6. PHASE 2: Converting all courses to schemas...")
    all_courses = []

    for dept_code, dept_long_name, courses_raw in all_courses_data:
        if courses_raw:
            courses = [
                convert_course_to_schema(course, dept_code, dept_long_name)
                for course in courses_raw
            ]
            all_courses.extend(courses)
            print(f"   ✓ Converted {len(courses)} courses for {dept_code}")

    print(f"   ✓ Total courses converted: {len(all_courses)}")

    # PHASE 3: Bulk upsert all courses
    print("\n7. PHASE 3: Bulk upserting all courses...")
    total_courses = bulk_upsert_courses(all_courses, batch_size=1000)

    # Final summary
    end_time = time.time()
    duration = end_time - start_time

    print("\n" + "=" * 60)
    print("Upsert Process Complete!")
    print("=" * 60)
    print(f"Total departments: {dept_count}")
    print(f"Total courses: {total_courses}")
    print(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    if duration > 0:
        print(f"Speed: {len(all_courses)/duration:.1f} courses/second")
    print("\n✓ Data has been upserted to:")
    print("  - departments")
    print("  - courses")


if __name__ == "__main__":
    main()
