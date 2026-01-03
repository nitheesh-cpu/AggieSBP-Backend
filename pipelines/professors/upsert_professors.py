"""
Upsert script for professors.

This script:
1. Scrapes all professors from Rate My Professor for a university
2. Upserts new/updated professors to the professors table

Optimized with batch processing for better performance.
"""

import os
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from aggiermp.database.base import ProfessorDB, get_session, upsert_professors
from pipelines.professors.scrapers import RMPReviewCollector


def upsert_all_professors(university_id: str, session=None):
    """
    Scrape and upsert all professors for a university.

    Args:
        university_id: University ID (e.g., "U2Nob29sLTEwMDM=" for Texas A&M)
        session: Optional database session

    Returns:
        Dictionary with results
    """
    close_session = False
    if session is None:
        session = get_session()
        close_session = True

    try:
        print("Scraping professors...", end=" ", flush=True)
        collector = RMPReviewCollector()
        professors = collector.get_all_professors(university_id, limit=1000)
        print(f"{len(professors)} found", end=" ", flush=True)

        # Optimize: Only query IDs instead of full objects
        existing_ids = {row[0] for row in session.query(ProfessorDB.id).all()}
        new_count = sum(1 for prof in professors if prof.id not in existing_ids)

        # Batch upsert
        upsert_professors(session, professors)
        session.commit()  # Commit the upsert
        updated_count = len(professors) - new_count

        print(f"({new_count} new, {updated_count} updated)")

        return {
            "total": len(professors),
            "new": new_count,
            "updated": updated_count,
            "professor_ids": [prof.id for prof in professors],
        }

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()
        # Rollback on error
        try:
            session.rollback()
        except Exception:
            pass
        return {
            "total": 0,
            "new": 0,
            "updated": 0,
            "professor_ids": [],
            "error": str(e),
        }

    finally:
        if close_session:
            session.close()


def main():
    """Main function - default to Texas A&M"""
    # Default to Texas A&M University ID
    # You can find this by searching on RMP or using the get_university_by_name function
    TEXAS_AM_ID = "U2Nob29sLTEwMDM="  # Texas A&M University

    # Allow override via environment variable
    university_id = os.getenv("UNIVERSITY_ID", TEXAS_AM_ID)

    result = upsert_all_professors(university_id)

    if "error" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
