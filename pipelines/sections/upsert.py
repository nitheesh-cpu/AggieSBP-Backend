"""
Upsert operations for section data from Howdy API.

Handles upserting:
- Sections (main section data)
- Section instructors
- Section meetings
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text

from aggiermp.database.base import (
    TermDB,
    SectionDB,
    SectionInstructorDB,
    SectionMeetingDB,
    SectionAttributeDetailedDB,
    SectionPrereqDB,
    SectionRestrictionDB,
    SectionBookstoreLinkDB,
    get_session,
)
from pipelines.sections.schemas import SectionSchema, TermSchema, SectionDetailsSchema
from pipelines.sections.scraper import (
    get_all_sections,
    get_all_terms,
    fetch_section_details_sync,
)


def upsert_terms(terms: List[TermSchema], session=None) -> Dict[str, int]:
    """
    Upsert terms to database.

    Args:
        terms: List of TermSchema objects to upsert
        session: Optional database session

    Returns:
        Dictionary with counts of upserted records
    """
    close_session = False
    if session is None:
        session = get_session()
        close_session = True

    # Check transaction state first
    try:
        session.execute(text("SELECT 1"))
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass

    results = {"terms_upserted": 0, "errors": []}

    if not terms:
        return results

    try:
        from datetime import datetime as dt

        now = dt.now()

        # Prepare term records
        term_records = []
        for term in terms:
            # Parse dates if provided
            start_date = None
            end_date = None
            if term.start_date:
                try:
                    start_date = dt.fromisoformat(
                        term.start_date.replace("Z", "+00:00")
                    )
                except (ValueError, AttributeError):
                    pass
            if term.end_date:
                try:
                    end_date = dt.fromisoformat(term.end_date.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    pass

            term_records.append(
                {
                    "term_code": term.term_code,
                    "term_desc": term.term_desc,
                    "start_date": start_date,
                    "end_date": end_date,
                    "academic_year": term.academic_year,
                    "created_at": now,
                    "updated_at": now,
                }
            )

        if term_records:
            try:
                stmt = insert(TermDB).values(term_records)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["term_code"],
                    set_={
                        "term_desc": stmt.excluded.term_desc,
                        "start_date": stmt.excluded.start_date,
                        "end_date": stmt.excluded.end_date,
                        "academic_year": stmt.excluded.academic_year,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
                session.execute(stmt)
                session.commit()
                results["terms_upserted"] = len(term_records)
            except Exception as e:
                session.rollback()
                results["errors"].append(f"Error upserting terms: {e}")

        return results

    except Exception as e:
        try:
            session.rollback()
        except Exception:
            pass
        results["errors"].append(f"Error upserting terms: {e}")
        return results

    finally:
        if close_session:
            session.close()


def upsert_sections(sections: List[SectionSchema], session=None) -> Dict[str, int]:
    """
    Upsert sections and related data to database.

    Args:
        sections: List of SectionSchema objects to upsert
        session: Optional database session

    Returns:
        Dictionary with counts of upserted records
    """
    close_session = False
    if session is None:
        session = get_session()
        close_session = True

    # Check transaction state first
    try:
        session.execute(text("SELECT 1"))
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass

    results = {
        "sections_upserted": 0,
        "instructors_upserted": 0,
        "meetings_upserted": 0,
        "errors": [],
    }

    if not sections:
        return results

    try:
        now = datetime.now()

        # Prepare section records
        section_records = []
        instructor_records = []
        meeting_records = []

        for section in sections:
            # Section record
            section_records.append(
                {
                    "id": section.id,
                    "term_code": section.term_code,
                    "crn": section.crn,
                    "dept": section.dept,
                    "dept_desc": section.dept_desc,
                    "course_number": section.course_number,
                    "section_number": section.section_number,
                    "course_title": section.course_title,
                    "credit_hours": section.credit_hours,
                    "hours_low": section.hours_low,
                    "hours_high": section.hours_high,
                    "campus": section.campus,
                    "part_of_term": section.part_of_term,
                    "session_type": section.session_type,
                    "schedule_type": section.schedule_type,
                    "instruction_type": section.instruction_type,
                    "is_open": section.is_open,
                    "has_syllabus": section.has_syllabus,
                    "syllabus_url": section.syllabus_url,
                    "attributes_text": section.attributes_text,
                    "created_at": now,
                    "updated_at": now,
                }
            )

            # Instructor records
            for instructor in section.instructors:
                pidm = instructor.pidm or 0
                instructor_id = f"{section.term_code}_{section.crn}_{pidm}"
                instructor_records.append(
                    {
                        "id": instructor_id,
                        "section_id": section.id,
                        "term_code": section.term_code,
                        "crn": section.crn,
                        "instructor_name": instructor.name,
                        "instructor_pidm": instructor.pidm,
                        "has_cv": instructor.has_cv,
                        "cv_url": instructor.cv_url,
                        "is_primary": instructor.is_primary,
                        "created_at": now,
                        "updated_at": now,
                    }
                )

            # Meeting records
            for meeting in section.meetings:
                meeting_id = (
                    f"{section.term_code}_{section.crn}_{meeting.meeting_index}"
                )
                meeting_records.append(
                    {
                        "id": meeting_id,
                        "section_id": section.id,
                        "term_code": section.term_code,
                        "crn": section.crn,
                        "meeting_index": meeting.meeting_index,
                        "credit_hours_session": meeting.credit_hours_session,
                        "days_of_week": meeting.days_of_week
                        if meeting.days_of_week
                        else [],
                        "begin_time": meeting.begin_time,
                        "end_time": meeting.end_time,
                        "start_date": meeting.start_date,
                        "end_date": meeting.end_date,
                        "building_code": meeting.building_code,
                        "room_code": meeting.room_code,
                        "meeting_type": meeting.meeting_type,
                        "created_at": now,
                        "updated_at": now,
                    }
                )

        # Upsert sections in batches
        if section_records:
            batch_size = 500
            for i in range(0, len(section_records), batch_size):
                batch = section_records[i : i + batch_size]
                try:
                    stmt = insert(SectionDB).values(batch)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"],
                        set_={
                            "dept_desc": stmt.excluded.dept_desc,
                            "course_title": stmt.excluded.course_title,
                            "credit_hours": stmt.excluded.credit_hours,
                            "hours_low": stmt.excluded.hours_low,
                            "hours_high": stmt.excluded.hours_high,
                            "campus": stmt.excluded.campus,
                            "part_of_term": stmt.excluded.part_of_term,
                            "session_type": stmt.excluded.session_type,
                            "schedule_type": stmt.excluded.schedule_type,
                            "instruction_type": stmt.excluded.instruction_type,
                            "is_open": stmt.excluded.is_open,
                            "has_syllabus": stmt.excluded.has_syllabus,
                            "syllabus_url": stmt.excluded.syllabus_url,
                            "attributes_text": stmt.excluded.attributes_text,
                            "updated_at": stmt.excluded.updated_at,
                        },
                    )
                    session.execute(stmt)
                    results["sections_upserted"] += len(batch)
                except Exception as e:
                    session.rollback()
                    results["errors"].append(
                        f"Error upserting sections batch {i//batch_size}: {e}"
                    )
                    continue

            session.commit()

        # Upsert instructors in batches
        if instructor_records:
            batch_size = 1000
            for i in range(0, len(instructor_records), batch_size):
                batch = instructor_records[i : i + batch_size]
                try:
                    stmt = insert(SectionInstructorDB).values(batch)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"],
                        set_={
                            "instructor_name": stmt.excluded.instructor_name,
                            "instructor_pidm": stmt.excluded.instructor_pidm,
                            "has_cv": stmt.excluded.has_cv,
                            "cv_url": stmt.excluded.cv_url,
                            "is_primary": stmt.excluded.is_primary,
                            "updated_at": stmt.excluded.updated_at,
                        },
                    )
                    session.execute(stmt)
                    results["instructors_upserted"] += len(batch)
                except Exception as e:
                    session.rollback()
                    results["errors"].append(
                        f"Error upserting instructors batch {i//batch_size}: {e}"
                    )
                    continue

            session.commit()

        # Upsert meetings in batches
        if meeting_records:
            batch_size = 1000
            for i in range(0, len(meeting_records), batch_size):
                batch = meeting_records[i : i + batch_size]
                try:
                    stmt = insert(SectionMeetingDB).values(batch)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"],
                        set_={
                            "credit_hours_session": stmt.excluded.credit_hours_session,
                            "days_of_week": stmt.excluded.days_of_week,
                            "begin_time": stmt.excluded.begin_time,
                            "end_time": stmt.excluded.end_time,
                            "start_date": stmt.excluded.start_date,
                            "end_date": stmt.excluded.end_date,
                            "building_code": stmt.excluded.building_code,
                            "room_code": stmt.excluded.room_code,
                            "meeting_type": stmt.excluded.meeting_type,
                            "updated_at": stmt.excluded.updated_at,
                        },
                    )
                    session.execute(stmt)
                    results["meetings_upserted"] += len(batch)
                except Exception as e:
                    session.rollback()
                    results["errors"].append(
                        f"Error upserting meetings batch {i//batch_size}: {e}"
                    )
                    continue

            session.commit()

        return results

    except Exception as e:
        try:
            session.rollback()
        except Exception:
            pass
        results["errors"].append(f"Error upserting sections: {e}")
        return results

    finally:
        if close_session:
            session.close()


def upsert_all_sections(
    term_codes: Optional[List[str]] = None,
    semester_filter: Optional[List[str]] = None,
    max_workers: int = 4,
    include_all_terms: bool = False,
) -> Dict[str, any]:
    """
    Fetch and upsert all sections for specified terms.
    Also upserts term information to the terms table.

    Args:
        term_codes: Optional list of term codes. If None, fetches current terms.
        semester_filter: Optional filter for semester descriptions
        max_workers: Number of parallel workers for fetching
        include_all_terms: If True, upserts ALL terms (not just filtered ones)

    Returns:
        Dictionary with results
    """
    print("Starting sections upsert...")

    # First, fetch and upsert terms
    print("Fetching terms...")
    all_terms = get_all_terms(
        current_only=not include_all_terms, semester_filter=semester_filter
    )
    if all_terms:
        session = get_session()
        try:
            term_result = upsert_terms(all_terms, session)
            print(f"Upserted {term_result['terms_upserted']} terms")
        finally:
            session.close()

    # Fetch sections
    sections_by_term = get_all_sections(
        term_codes=term_codes, semester_filter=semester_filter, max_workers=max_workers
    )

    if not sections_by_term:
        return {"error": "No sections fetched", "terms": 0, "sections": 0}

    # Combine all sections
    all_sections = []
    for term_code, sections in sections_by_term.items():
        all_sections.extend(sections)

    print(f"Upserting {len(all_sections)} sections...")

    # Upsert
    session = get_session()
    try:
        results = upsert_sections(all_sections, session)
        results["terms_processed"] = len(sections_by_term)
        results["terms_upserted"] = (
            term_result.get("terms_upserted", 0) if all_terms else 0
        )

        print(
            f"Done: {results['sections_upserted']} sections, "
            f"{results['instructors_upserted']} instructors, "
            f"{results['meetings_upserted']} meetings"
        )

        if results["errors"]:
            print(f"Errors: {len(results['errors'])}")
            for error in results["errors"][:5]:
                print(f"  - {error}")

        return results
    finally:
        session.close()


def upsert_section_details(
    details_list: List[SectionDetailsSchema], session=None
) -> Dict[str, int]:
    """
    Upsert section details (attributes, prereqs, restrictions, bookstore links).

    Args:
        details_list: List of SectionDetailsSchema objects
        session: Optional database session

    Returns:
        Dictionary with counts of upserted records
    """
    close_session = False
    if session is None:
        session = get_session()
        close_session = True

    # Check transaction state first
    try:
        session.execute(text("SELECT 1"))
    except Exception:
        try:
            session.rollback()
        except Exception:
            pass

    results = {
        "attributes_upserted": 0,
        "prereqs_upserted": 0,
        "restrictions_upserted": 0,
        "bookstore_links_upserted": 0,
        "errors": [],
    }

    if not details_list:
        return results

    try:
        now = datetime.now()

        # Prepare records
        attribute_records = []
        prereq_records = []
        restriction_records = []
        bookstore_records = []

        for details in details_list:
            # Attributes
            for attr in details.attributes:
                attribute_records.append(
                    {
                        "id": attr.id,
                        "section_id": attr.section_id,
                        "term_code": attr.term_code,
                        "crn": attr.crn,
                        "attribute_code": attr.attribute_code,
                        "attribute_desc": attr.attribute_desc,
                        "created_at": now,
                    }
                )

            # Prereqs
            if details.prereqs and details.prereqs.prereqs_text:
                prereq_records.append(
                    {
                        "id": details.prereqs.id,
                        "section_id": details.prereqs.section_id,
                        "term_code": details.prereqs.term_code,
                        "crn": details.prereqs.crn,
                        "prereqs_text": details.prereqs.prereqs_text,
                        "prereqs_json": details.prereqs.prereqs_json,
                        "created_at": now,
                        "updated_at": now,
                    }
                )

            # Restrictions
            for restr in details.restrictions:
                restriction_records.append(
                    {
                        "id": restr.id,
                        "section_id": restr.section_id,
                        "term_code": restr.term_code,
                        "crn": restr.crn,
                        "restriction_type": restr.restriction_type,
                        "restriction_code": restr.restriction_code,
                        "restriction_desc": restr.restriction_desc,
                        "include_exclude": restr.include_exclude,
                        "created_at": now,
                    }
                )

            # Bookstore link
            if details.bookstore_link and details.bookstore_link.link_data:
                bookstore_records.append(
                    {
                        "id": details.bookstore_link.id,
                        "section_id": details.bookstore_link.section_id,
                        "term_code": details.bookstore_link.term_code,
                        "crn": details.bookstore_link.crn,
                        "bookstore_url": details.bookstore_link.bookstore_url,
                        "link_data": details.bookstore_link.link_data,
                        "created_at": now,
                    }
                )

        # Upsert attributes
        if attribute_records:
            batch_size = 1000
            for i in range(0, len(attribute_records), batch_size):
                batch = attribute_records[i : i + batch_size]
                try:
                    stmt = insert(SectionAttributeDetailedDB).values(batch)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"],
                        set_={
                            "attribute_desc": stmt.excluded.attribute_desc,
                        },
                    )
                    session.execute(stmt)
                    results["attributes_upserted"] += len(batch)
                except Exception as e:
                    session.rollback()
                    results["errors"].append(
                        f"Error upserting attributes batch {i//batch_size}: {e}"
                    )
                    continue
            session.commit()

        # Upsert prereqs
        if prereq_records:
            batch_size = 500
            for i in range(0, len(prereq_records), batch_size):
                batch = prereq_records[i : i + batch_size]
                try:
                    stmt = insert(SectionPrereqDB).values(batch)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"],
                        set_={
                            "prereqs_text": stmt.excluded.prereqs_text,
                            "prereqs_json": stmt.excluded.prereqs_json,
                            "updated_at": stmt.excluded.updated_at,
                        },
                    )
                    session.execute(stmt)
                    results["prereqs_upserted"] += len(batch)
                except Exception as e:
                    session.rollback()
                    results["errors"].append(
                        f"Error upserting prereqs batch {i//batch_size}: {e}"
                    )
                    continue
            session.commit()

        # Upsert restrictions
        if restriction_records:
            batch_size = 1000
            for i in range(0, len(restriction_records), batch_size):
                batch = restriction_records[i : i + batch_size]
                try:
                    stmt = insert(SectionRestrictionDB).values(batch)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"],
                        set_={
                            "restriction_code": stmt.excluded.restriction_code,
                            "restriction_desc": stmt.excluded.restriction_desc,
                            "include_exclude": stmt.excluded.include_exclude,
                        },
                    )
                    session.execute(stmt)
                    results["restrictions_upserted"] += len(batch)
                except Exception as e:
                    session.rollback()
                    results["errors"].append(
                        f"Error upserting restrictions batch {i//batch_size}: {e}"
                    )
                    continue
            session.commit()

        # Upsert bookstore links
        if bookstore_records:
            batch_size = 500
            for i in range(0, len(bookstore_records), batch_size):
                batch = bookstore_records[i : i + batch_size]
                try:
                    stmt = insert(SectionBookstoreLinkDB).values(batch)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["id"],
                        set_={
                            "bookstore_url": stmt.excluded.bookstore_url,
                            "link_data": stmt.excluded.link_data,
                        },
                    )
                    session.execute(stmt)
                    results["bookstore_links_upserted"] += len(batch)
                except Exception as e:
                    session.rollback()
                    results["errors"].append(
                        f"Error upserting bookstore links batch {i//batch_size}: {e}"
                    )
                    continue
            session.commit()

        return results

    except Exception as e:
        try:
            session.rollback()
        except Exception:
            pass
        results["errors"].append(f"Error upserting section details: {e}")
        return results

    finally:
        if close_session:
            session.close()


def upsert_all_section_details(
    term_codes: Optional[List[str]] = None,
    semester_filter: Optional[List[str]] = None,
    max_concurrent: int = 50,
    sections: Optional[List[SectionSchema]] = None,
) -> Dict[str, any]:
    """
    Fetch and upsert section details for specified terms.

    Args:
        term_codes: Optional list of term codes. If None, fetches current terms.
        semester_filter: Optional filter for semester descriptions
        max_concurrent: Maximum concurrent API requests
        sections: Optional pre-fetched list of sections (skips fetching)

    Returns:
        Dictionary with results
    """
    print("Starting section details upsert...")

    # Fetch sections if not provided
    if sections is None:
        sections_by_term = get_all_sections(
            term_codes=term_codes, semester_filter=semester_filter, max_workers=4
        )

        if not sections_by_term:
            return {"error": "No sections fetched", "sections": 0}

        # Combine all sections
        sections = []
        for term_code, term_sections in sections_by_term.items():
            sections.extend(term_sections)

    print(f"Fetching details for {len(sections)} sections...")

    # Progress callback
    def progress(completed, total):
        print(f"  Progress: {completed}/{total} sections ({100*completed/total:.1f}%)")

    # Fetch details
    details_list = fetch_section_details_sync(
        sections, max_concurrent=max_concurrent, progress_callback=progress
    )

    print(f"Upserting details for {len(details_list)} sections...")

    # Upsert
    session = get_session()
    try:
        results = upsert_section_details(details_list, session)
        results["sections_processed"] = len(sections)

        print(
            f"Done: {results['attributes_upserted']} attributes, "
            f"{results['prereqs_upserted']} prereqs, "
            f"{results['restrictions_upserted']} restrictions, "
            f"{results['bookstore_links_upserted']} bookstore links"
        )

        if results["errors"]:
            print(f"Errors: {len(results['errors'])}")
            for error in results["errors"][:5]:
                print(f"  - {error}")

        return results
    finally:
        session.close()


def delete_old_sections(keep_term_codes: List[str], session=None) -> int:
    """
    Delete sections and all related data for terms not in the keep list.
    Useful for cleaning up old semester data.

    Args:
        keep_term_codes: List of term codes to keep
        session: Optional database session

    Returns:
        Number of sections deleted
    """
    close_session = False
    if session is None:
        session = get_session()
        close_session = True

    try:
        # Delete detail tables first (foreign key constraints)
        session.execute(
            text(
                "DELETE FROM section_attributes_detailed WHERE term_code NOT IN :terms"
            ),
            {"terms": tuple(keep_term_codes)},
        )
        print("  Deleted old section attributes")

        session.execute(
            text("DELETE FROM section_prereqs WHERE term_code NOT IN :terms"),
            {"terms": tuple(keep_term_codes)},
        )
        print("  Deleted old section prereqs")

        session.execute(
            text("DELETE FROM section_restrictions WHERE term_code NOT IN :terms"),
            {"terms": tuple(keep_term_codes)},
        )
        print("  Deleted old section restrictions")

        session.execute(
            text("DELETE FROM section_bookstore_links WHERE term_code NOT IN :terms"),
            {"terms": tuple(keep_term_codes)},
        )
        print("  Deleted old section bookstore links")

        # Delete meetings
        session.execute(
            text("DELETE FROM section_meetings WHERE term_code NOT IN :terms"),
            {"terms": tuple(keep_term_codes)},
        )
        print("  Deleted old section meetings")

        # Delete instructors
        session.execute(
            text("DELETE FROM section_instructors WHERE term_code NOT IN :terms"),
            {"terms": tuple(keep_term_codes)},
        )
        print("  Deleted old section instructors")

        # Delete sections
        result = session.execute(
            text("DELETE FROM sections WHERE term_code NOT IN :terms"),
            {"terms": tuple(keep_term_codes)},
        )

        session.commit()
        deleted_count = result.rowcount
        print(f"Deleted {deleted_count} old sections")
        return deleted_count

    except Exception as e:
        session.rollback()
        print(f"Error deleting old sections: {e}")
        return 0

    finally:
        if close_session:
            session.close()


def main():
    """Main entry point for sections upsert"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch and upsert section data from Howdy API"
    )
    parser.add_argument(
        "--terms", nargs="+", help="Specific term codes to fetch (e.g., 202611 202621)"
    )
    parser.add_argument(
        "--semesters",
        nargs="+",
        help="Semester filters (e.g., 'Spring 2026' 'Fall 2025')",
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="Number of parallel workers"
    )
    parser.add_argument(
        "--list-terms", action="store_true", help="List available terms and exit"
    )
    parser.add_argument(
        "--cleanup", action="store_true", help="Delete sections for old terms"
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Also fetch and upsert section details (slower)",
    )
    parser.add_argument(
        "--details-only",
        action="store_true",
        help="Only fetch section details (skip main sections)",
    )
    parser.add_argument(
        "--concurrent", type=int, default=50, help="Max concurrent requests for details"
    )

    args = parser.parse_args()

    if args.list_terms:
        print("Fetching available terms...")
        terms = get_all_terms(current_only=False)
        print(f"\nFound {len(terms)} terms:")
        for term in sorted(terms, key=lambda t: t.term_code, reverse=True):
            print(f"  {term.term_code}: {term.term_desc}")
        return

    sections = None

    # Run the main upsert (unless --details-only)
    if not args.details_only:
        results = upsert_all_sections(
            term_codes=args.terms,
            semester_filter=args.semesters,
            max_workers=args.workers,
        )

        if "error" in results:
            print(f"Error: {results['error']}")
            sys.exit(1)

    # Fetch and upsert section details (if --details or --details-only)
    if args.details or args.details_only:
        print("\n" + "=" * 50)
        details_results = upsert_all_section_details(
            term_codes=args.terms,
            semester_filter=args.semesters,
            max_concurrent=args.concurrent,
            sections=sections,
        )

        if "error" in details_results:
            print(f"Error: {details_results['error']}")
            sys.exit(1)

    # Optionally cleanup old terms
    if args.cleanup and args.terms:
        delete_old_sections(args.terms)


if __name__ == "__main__":
    main()
