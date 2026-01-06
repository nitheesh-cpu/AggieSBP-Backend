"""
Upsert script for reviews and summaries.

This script:
1. Gets new reviews for all professors (in parallel)
2. Upserts reviews to the reviews table (batched)
3. Runs the hierarchical summarization pipeline (batched)
4. Upserts summaries to professor_summaries_new table (batched)

Optimized for processing thousands of professors efficiently.
"""

import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Set, Tuple, Dict, Any

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from aggiermp.database.base import ProfessorDB, ReviewDB, get_session, upsert_reviews
from pipelines.professors.scrapers import RMPReviewCollector
from pipelines.professors.hierarchical_summarization import (
    HierarchicalSummarizationPipeline,
)
from pipelines.professors.upsert import upsert_professor_summary

# Checkpoint file for resume functionality
CHECKPOINT_FILE = Path(__file__).parent / ".review_summary_checkpoint.json"


def load_checkpoint() -> Set[str]:
    """Load set of successfully processed professor IDs from checkpoint file"""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                data = json.load(f)
                return set(data.get("processed_professors", []))
        except Exception as e:
            print(f"Warning: Could not load checkpoint: {e}")
    return set()


def save_checkpoint(processed_professors: Set[str]) -> None:
    """Save set of successfully processed professor IDs to checkpoint file"""
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump({"processed_professors": list(processed_professors)}, f)
    except Exception as e:
        print(f"Warning: Could not save checkpoint: {e}")


def clear_checkpoint() -> None:
    """Clear the checkpoint file"""
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


def get_new_reviews_for_professor(
    professor_id: str, existing_review_ids: Set[str]
) -> Tuple[str, List]:
    """
    Get new reviews for a professor that aren't already in the database.
    Thread-safe version that takes existing IDs as parameter.

    Args:
        professor_id: Professor ID
        existing_review_ids: Set of existing review IDs for this professor

    Returns:
        Tuple of (professor_id, list of new Review objects)
    """
    try:
        # Create a new collector instance (one per thread)
        collector = RMPReviewCollector()
        # Get only new reviews (optimized fetch)
        new_reviews = collector.get_new_reviews(professor_id, list(existing_review_ids))

        return (professor_id, new_reviews)
    except Exception as e:
        print(f"  ERROR fetching reviews for {professor_id[:20]}: {e}")
        return (professor_id, [])


def batch_upsert_reviews(session: Any, reviews_batch: List[Any]) -> None:
    """Batch upsert reviews for better performance"""
    if not reviews_batch:
        return
    upsert_reviews(session, reviews_batch)


def upsert_reviews_and_summaries(
    professor_ids: List[str] | None = None,
    session: Any = None,
    skip_if_no_new_reviews: bool = True,
    resume: bool = True,
    clear_checkpoint_on_start: bool = False,
    max_workers: int = 10,  # Parallel review fetching
    batch_size: int = 50,  # Batch size for review fetching and summarization
) -> Dict[str, Any]:
    """
    Get new reviews and generate summaries for professors.
    Optimized with parallel review fetching and batched processing.

    Args:
        professor_ids: Optional list of professor IDs to process.
                      If None, processes all professors in database.
        session: Optional database session
        skip_if_no_new_reviews: If True, skip professors with no new reviews
        resume: If True, skip professors that were already successfully processed
        clear_checkpoint_on_start: If True, clear checkpoint file at start
        max_workers: Number of parallel workers for review fetching
        batch_size: Batch size for processing professors

    Returns:
        Dictionary with results
    """
    close_session = False
    if session is None:
        session = get_session()
        close_session = True

    try:
        if clear_checkpoint_on_start:
            clear_checkpoint()

        # Load checkpoint if resuming and allowed
        use_checkpoint = resume and professor_ids is None
        processed_professors = load_checkpoint() if use_checkpoint else set()

        if processed_professors:
            print(f"Resuming: {len(processed_professors)} professors already processed")

        # Get professor IDs to process
        if professor_ids is None:
            professors = session.query(ProfessorDB).all()
            professor_ids = [prof.id for prof in professors]

        # Filter out already processed professors
        remaining_professors = [
            pid for pid in professor_ids if pid not in processed_professors
        ]
        print(f"Processing {len(remaining_professors)}/{len(professor_ids)} professors")

        # Initialize pipeline (shared across batches)
        pipeline = HierarchicalSummarizationPipeline(session=session)

        # Track results
        results: Dict[str, Any] = {
            "professors_processed": 0,
            "reviews_added": 0,
            "summaries_generated": 0,
            "professors_skipped": 0,
            "errors": [],
        }

        # Pre-fetch existing review IDs for all professors (batch database query)
        print("Loading existing review IDs...")
        all_existing_reviews = session.query(ReviewDB.professor_id, ReviewDB.id).all()
        professor_existing_reviews: Dict[str, Set[str]] = {}
        for prof_id, review_id in all_existing_reviews:
            if prof_id not in professor_existing_reviews:
                professor_existing_reviews[prof_id] = set()
            professor_existing_reviews[prof_id].add(review_id)

        # Process in batches
        total_batches = (len(remaining_professors) + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            batch_start = batch_num * batch_size
            batch_end = min(batch_start + batch_size, len(remaining_professors))
            batch_professors = remaining_professors[batch_start:batch_end]

            print(
                f"\nBatch {batch_num + 1}/{total_batches}: Processing {len(batch_professors)} professors"
            )

            # STEP 1: Parallel review fetching
            print("  Fetching reviews (parallel)...", end=" ", flush=True)
            new_reviews_dict: Dict[str, List] = {}

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        get_new_reviews_for_professor,
                        prof_id,
                        professor_existing_reviews.get(prof_id, set()),
                    ): prof_id
                    for prof_id in batch_professors
                }

                for future in as_completed(futures):
                    prof_id, new_reviews = future.result()
                    if new_reviews:
                        new_reviews_dict[prof_id] = new_reviews

            # STEP 2: Batch upsert new reviews
            all_new_reviews = []
            for prof_id, reviews in new_reviews_dict.items():
                all_new_reviews.extend(reviews)

            if all_new_reviews:
                print(f"{len(all_new_reviews)} new reviews found", end=" ", flush=True)
                batch_upsert_reviews(session, all_new_reviews)
                session.commit()  # Commit reviews batch
                results["reviews_added"] += len(all_new_reviews)

                # Update existing review IDs for next batch
                for review in all_new_reviews:
                    if review.professor_id not in professor_existing_reviews:
                        professor_existing_reviews[review.professor_id] = set()
                    professor_existing_reviews[review.professor_id].add(review.id)
            else:
                print("0 new reviews", end=" ", flush=True)

            # STEP 3: Get all reviews for professors and process summaries (sequential for GPU)
            print("\n  Generating summaries...", flush=True)
            professors_to_summarize = []

            if skip_if_no_new_reviews:
                # Only process professors with new reviews or no existing summary
                professors_to_summarize = [
                    prof_id
                    for prof_id in batch_professors
                    if prof_id in new_reviews_dict
                    or prof_id not in processed_professors
                ]
            else:
                professors_to_summarize = batch_professors

            for i, professor_id in enumerate(professors_to_summarize, 1):
                try:
                    print(
                        f"    [{i}/{len(professors_to_summarize)}] {professor_id[:20]}...",
                        end=" ",
                        flush=True,
                    )

                    # Get all reviews for this professor
                    all_reviews = (
                        session.query(ReviewDB)
                        .filter_by(professor_id=professor_id)
                        .all()
                    )

                    if not all_reviews:
                        print("(no reviews)")
                        results["professors_skipped"] += 1
                        continue

                    # Convert to dict list
                    raw_reviews = [
                        {
                            "id": review.id,
                            "professor_id": review.professor_id,
                            "course_code": review.course_code,
                            "review_text": review.review_text,
                        }
                        for review in all_reviews
                    ]

                    # Smart Skip: Check if we actually need to regenerate summary
                    # Check both overall AND per-course counts are in sync
                    if not new_reviews_dict.get(professor_id):
                        from aggiermp.database.base import ProfessorSummaryNewDB

                        # Get existing summaries for this professor
                        existing_summaries = (
                            session.query(
                                ProfessorSummaryNewDB.course_code,
                                ProfessorSummaryNewDB.total_reviews,
                            )
                            .filter(ProfessorSummaryNewDB.professor_id == professor_id)
                            .all()
                        )

                        # Build dict of course -> summary count
                        summary_counts = {
                            row.course_code: row.total_reviews
                            for row in existing_summaries
                        }

                        # Build dict of course -> actual review count
                        actual_counts: Dict[str | None, int] = {}
                        for review in raw_reviews:
                            code = review.get("course_code")
                            actual_counts[code] = actual_counts.get(code, 0) + 1
                        actual_counts[None] = len(raw_reviews)  # Overall count

                        # Check if all counts match
                        counts_match = True
                        for code, count in actual_counts.items():
                            if summary_counts.get(code) != count:
                                counts_match = False
                                break

                        if counts_match and len(summary_counts) > 0:
                            print("(up to date, skipping)", end=" ")
                            results["professors_processed"] += 1
                            processed_professors.add(professor_id)
                            continue

                    # Generate summary
                    professor_summary = pipeline.process_professor_reviews(
                        raw_reviews, professor_id
                    )

                    # Upsert summary (includes its own commit)
                    success = upsert_professor_summary(professor_summary, session)

                    if success:
                        results["summaries_generated"] += 1
                        results["professors_processed"] += 1
                        processed_professors.add(professor_id)
                        print("OK")
                    else:
                        results["errors"].append(
                            f"Failed to save summary for {professor_id}"
                        )
                        print("FAILED")
                        # Rollback any partial transaction
                        try:
                            session.rollback()
                        except Exception:
                            pass

                    # Save checkpoint periodically (every 10 professors)
                    if results["professors_processed"] % 10 == 0 and use_checkpoint:
                        save_checkpoint(processed_professors)

                except Exception as e:
                    error_msg = f"Error processing professor {professor_id}: {e}"
                    print(f"ERROR: {e}")
                    results["errors"].append(error_msg)
                    # Rollback on error
                    try:
                        session.rollback()
                    except Exception:
                        pass

            # Save checkpoint after each batch
            if use_checkpoint:
                save_checkpoint(processed_professors)

        # Clear checkpoint on successful completion
        if use_checkpoint:
            print("Processing complete. Clearing checkpoint.")
            clear_checkpoint()

        # Print summary
        print(
            f"\nComplete: {results['professors_processed']} processed, "
            f"{results['reviews_added']} reviews added, "
            f"{results['summaries_generated']} summaries, "
            f"{len(results['errors'])} errors"
        )

        return results

    except Exception as e:
        print(f"\n[ERROR] Failed: {e}")
        import traceback

        traceback.print_exc()
        return {"error": str(e)}

    finally:
        if close_session:
            session.close()


def main() -> None:
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description="Upsert reviews and summaries")
    parser.add_argument(
        "--no-resume", action="store_true", help="Don't resume from checkpoint"
    )
    parser.add_argument(
        "--clear-checkpoint",
        action="store_true",
        help="Clear checkpoint and start fresh",
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="Force update summaries even if no new reviews",
    )
    parser.add_argument(
        "--professor-id", type=str, help="Process only a specific professor ID"
    )
    args = parser.parse_args()

    # Set environment variables for performance
    os.environ["OMP_NUM_THREADS"] = "4"
    os.environ["TOKENIZERS_PARALLELISM"] = "true"

    # Process all professors (or specify a list of IDs)
    professor_ids = [args.professor_id] if args.professor_id else None

    result = upsert_reviews_and_summaries(
        professor_ids=professor_ids,
        resume=(not args.no_resume and not args.force_update and not args.professor_id),
        clear_checkpoint_on_start=args.clear_checkpoint,
        skip_if_no_new_reviews=not args.force_update,
    )

    if "error" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
