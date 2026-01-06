"""
Complete upsert workflow for professors, reviews, and summaries.

This script runs the full workflow:
1. Scrapes all professors
2. Upserts professors to database
3. Gets new reviews for all professors
4. Upserts reviews to database
5. Generates summaries using hierarchical summarization
6. Upserts summaries to database

Handles transaction errors gracefully with proper rollback.

Note: The actual database upsert logic (including fixes for array column None handling
and transaction rollback) is in pipelines/professors/upsert.py in the
upsert_professor_summary() function. This script orchestrates the workflow.
"""

import os
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pipelines.professors.upsert_professors import upsert_all_professors
from pipelines.professors.upsert_reviews_and_summaries import (
    upsert_reviews_and_summaries,
)


def main() -> None:
    """Run complete workflow"""
    import argparse

    parser = argparse.ArgumentParser(description="Complete professor pipeline workflow")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Number of parallel workers for review fetching",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Batch size for processing professors",
    )
    parser.add_argument(
        "--no-resume", action="store_true", help="Don't resume from checkpoint"
    )
    parser.add_argument(
        "--clear-checkpoint",
        action="store_true",
        help="Clear checkpoint and start fresh",
    )
    args = parser.parse_args()

    # Set environment variables for performance
    os.environ["OMP_NUM_THREADS"] = "4"
    os.environ["TOKENIZERS_PARALLELISM"] = "true"

    # Default to Texas A&M University ID
    TEXAS_AM_ID = "U2Nob29sLTEwMDM="  # Texas A&M University
    university_id = os.getenv("UNIVERSITY_ID", TEXAS_AM_ID)

    print("Starting workflow...")
    print(
        f"Configuration: max_workers={args.max_workers}, batch_size={args.batch_size}"
    )

    try:
        # Step 1: Upsert professors
        professor_result = upsert_all_professors(university_id)

        if "error" in professor_result:
            print("ERROR: Professor upsert failed. Aborting.")
            sys.exit(1)

        # Step 2: Upsert reviews and summaries (optimized with parallel fetching and batching)
        professor_ids = professor_result.get("professor_ids")
        review_result = upsert_reviews_and_summaries(
            professor_ids=professor_ids,
            max_workers=args.max_workers,
            batch_size=args.batch_size,
            resume=not args.no_resume,
            clear_checkpoint_on_start=args.clear_checkpoint,
        )

        if "error" in review_result:
            print("ERROR: Review/summary upsert failed.")
            sys.exit(1)

        # Final summary
        print(
            f"\nDone: {review_result.get('professors_processed', 0)} professors, "
            f"{review_result.get('reviews_added', 0)} reviews, "
            f"{review_result.get('summaries_generated', 0)} summaries"
        )

    except KeyboardInterrupt:
        print("\n\nWorkflow interrupted by user.")
        sys.exit(130)  # Standard exit code for SIGINT
    except Exception as e:
        print(f"\n\nFatal error in workflow: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
