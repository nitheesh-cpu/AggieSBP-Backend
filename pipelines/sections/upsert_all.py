"""
Main pipeline script for sections data.

This script fetches and upserts all section data including:
1. Terms metadata
2. Basic section data (sections, instructors, meetings)
3. Section details (attributes, prereqs, restrictions, bookstore links)

Usage:
    # Fetch only basic section data for current terms
    python -m pipelines.sections.upsert_all

    # Fetch basic + details for specific terms
    python -m pipelines.sections.upsert_all --terms 202611 202621 --details

    # Fetch only details (skip basic data)
    python -m pipelines.sections.upsert_all --terms 202611 --details-only

    # List available terms
    python -m pipelines.sections.upsert_all --list-terms
"""

import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pipelines.sections.scraper import (
    get_all_terms,
    get_all_sections,
    fetch_section_details_sync,
)
from pipelines.sections.upsert import (
    upsert_terms,
    upsert_sections,
    upsert_section_details,
)
from aggiermp.database.base import get_session


def run_full_pipeline(
    term_codes: Optional[List[str]] = None,
    semester_filter: Optional[List[str]] = None,
    include_details: bool = False,
    details_only: bool = False,
    max_workers: int = 4,
    max_concurrent: int = 50,
) -> dict:
    """
    Run the full sections pipeline.

    Args:
        term_codes: Optional list of specific term codes
        semester_filter: Optional list of semester descriptions to filter
        include_details: Whether to also fetch section details
        details_only: Only fetch details, skip basic section data
        max_workers: Parallel workers for section fetching
        max_concurrent: Max concurrent requests for detail fetching

    Returns:
        Dictionary with pipeline results
    """
    start_time = time.time()
    results: Dict[str, Any] = {
        "start_time": datetime.now().isoformat(),
        "terms_upserted": 0,
        "sections_upserted": 0,
        "instructors_upserted": 0,
        "meetings_upserted": 0,
        "attributes_upserted": 0,
        "prereqs_upserted": 0,
        "restrictions_upserted": 0,
        "bookstore_links_upserted": 0,
        "errors": [],
    }

    session = get_session()

    try:
        # Step 1: Fetch and upsert terms
        print("=" * 60)
        print("Step 1: Fetching terms...")
        print("=" * 60)
        terms = get_all_terms(current_only=True, semester_filter=semester_filter)

        if terms:
            term_result = upsert_terms(terms, session)
            results["terms_upserted"] = term_result.get("terms_upserted", 0)
            print(f"Upserted {results['terms_upserted']} terms")

            # Determine which terms to process
            if term_codes:
                print(f"Using specified term codes: {term_codes}")
            else:
                term_codes = [t.term_code for t in terms]
                print(f"Using current terms: {term_codes}")
        else:
            print("No terms found!")
            return results

        sections = None

        # Step 2: Fetch and upsert basic section data (unless details_only)
        if not details_only:
            print("\n" + "=" * 60)
            print("Step 2: Fetching sections...")
            print("=" * 60)

            sections_by_term = get_all_sections(
                term_codes=term_codes,
                semester_filter=semester_filter,
                max_workers=max_workers,
            )

            if sections_by_term:
                # Combine all sections
                sections = []
                for term_code, term_sections in sections_by_term.items():
                    sections.extend(term_sections)

                print(f"\nUpserting {len(sections)} sections...")
                section_result = upsert_sections(sections, session)

                results["sections_upserted"] = section_result.get(
                    "sections_upserted", 0
                )
                results["instructors_upserted"] = section_result.get(
                    "instructors_upserted", 0
                )
                results["meetings_upserted"] = section_result.get(
                    "meetings_upserted", 0
                )
                results["errors"].extend(section_result.get("errors", []))

                print(f"  Sections: {results['sections_upserted']}")
                print(f"  Instructors: {results['instructors_upserted']}")
                print(f"  Meetings: {results['meetings_upserted']}")
            else:
                print("No sections found!")

        # Step 3: Fetch and upsert section details (if requested)
        if include_details or details_only:
            print("\n" + "=" * 60)
            print("Step 3: Fetching section details...")
            print("=" * 60)

            # If we haven't fetched sections yet, fetch them now
            if sections is None:
                sections_by_term = get_all_sections(
                    term_codes=term_codes,
                    semester_filter=semester_filter,
                    max_workers=max_workers,
                )
                sections = []
                for term_code, term_sections in sections_by_term.items():
                    sections.extend(term_sections)

            if sections:
                print(f"Fetching details for {len(sections)} sections...")

                def progress_callback(completed: int, total: int) -> None:
                    pct = 100 * completed / total
                    print(f"  Progress: {completed}/{total} ({pct:.1f}%)")

                details_list = fetch_section_details_sync(
                    sections,
                    max_concurrent=max_concurrent,
                    progress_callback=progress_callback,
                )

                print("\nUpserting details...")
                details_result = upsert_section_details(details_list, session)

                results["attributes_upserted"] = details_result.get(
                    "attributes_upserted", 0
                )
                results["prereqs_upserted"] = details_result.get("prereqs_upserted", 0)
                results["restrictions_upserted"] = details_result.get(
                    "restrictions_upserted", 0
                )
                results["bookstore_links_upserted"] = details_result.get(
                    "bookstore_links_upserted", 0
                )
                results["errors"].extend(details_result.get("errors", []))

                print(f"  Attributes: {results['attributes_upserted']}")
                print(f"  Prereqs: {results['prereqs_upserted']}")
                print(f"  Restrictions: {results['restrictions_upserted']}")
                print(f"  Bookstore links: {results['bookstore_links_upserted']}")

        # Done
        elapsed = time.time() - start_time
        results["elapsed_seconds"] = elapsed
        results["end_time"] = datetime.now().isoformat()

        print("\n" + "=" * 60)
        print("Pipeline Complete!")
        print("=" * 60)
        print(f"Time: {elapsed:.1f} seconds")
        print(f"Terms: {results['terms_upserted']}")
        print(f"Sections: {results['sections_upserted']}")
        print(f"Instructors: {results['instructors_upserted']}")
        print(f"Meetings: {results['meetings_upserted']}")
        if include_details or details_only:
            print(f"Attributes: {results['attributes_upserted']}")
            print(f"Prereqs: {results['prereqs_upserted']}")
            print(f"Restrictions: {results['restrictions_upserted']}")
            print(f"Bookstore Links: {results['bookstore_links_upserted']}")
        if results["errors"]:
            print(f"Errors: {len(results['errors'])}")
            for err in results["errors"][:5]:
                print(f"  - {err}")

        return results

    except KeyboardInterrupt:
        print("\n\nInterrupted by user!")
        try:
            session.rollback()
        except Exception as e:
            print(f"Failed to rollback: {e}")
        raise

    except Exception as e:
        try:
            session.rollback()
        except Exception as rollback_error:
            print(f"Failed to rollback: {rollback_error}")
            results["errors"].append(str(rollback_error))
        results["errors"].append(str(e))
        return results

    finally:
        session.close()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch and upsert section data from Howdy API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch basic section data for current terms
  python -m pipelines.sections.upsert_all

  # Fetch with details for specific terms
  python -m pipelines.sections.upsert_all --terms 202611 --details

  # Fetch only details (assumes sections already exist)
  python -m pipelines.sections.upsert_all --terms 202611 --details-only

  # List available terms
  python -m pipelines.sections.upsert_all --list-terms
""",
    )
    parser.add_argument(
        "--terms", nargs="+", help="Specific term codes (e.g., 202611 202621)"
    )
    parser.add_argument(
        "--semesters", nargs="+", help="Semester filters (e.g., 'Spring 2026')"
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="Parallel workers for section fetching"
    )
    parser.add_argument(
        "--concurrent", type=int, default=50, help="Max concurrent requests for details"
    )
    parser.add_argument(
        "--details", action="store_true", help="Also fetch section details"
    )
    parser.add_argument(
        "--details-only",
        action="store_true",
        help="Only fetch details, skip basic data",
    )
    parser.add_argument(
        "--list-terms", action="store_true", help="List available terms and exit"
    )

    args = parser.parse_args()

    if args.list_terms:
        print("Fetching available terms...")
        terms = get_all_terms(current_only=False)
        print(f"\nFound {len(terms)} terms:")
        for term in sorted(terms, key=lambda t: t.term_code, reverse=True)[:30]:
            end = f" (ends {term.end_date})" if term.end_date else ""
            print(f"  {term.term_code}: {term.term_desc}{end}")
        return

    try:
        run_full_pipeline(
            term_codes=args.terms,
            semester_filter=args.semesters,
            include_details=args.details,
            details_only=args.details_only,
            max_workers=args.workers,
            max_concurrent=args.concurrent,
        )
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(1)


if __name__ == "__main__":
    main()
