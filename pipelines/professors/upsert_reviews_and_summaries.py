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
import logging
from datetime import datetime, timedelta
from math import ceil
import os
import sys
import time
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from pathlib import Path
from typing import List, Set, Tuple, Dict, Any

# Some dependencies configure INFO logging while this module is being imported.
# Quiet them before importing the database, model, and HTTP client modules below.
NOISY_LOGGERS = (
    "httpx",
    "httpx2",
    "typesafe_sdk",
    "sentence_transformers",
    "transformers",
    "urllib3",
    "aggiermp.database.base",
)
for logger_name in NOISY_LOGGERS:
    logging.getLogger(logger_name).setLevel(logging.WARNING)

from dotenv import load_dotenv

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
load_dotenv(project_root / ".env")
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from aggiermp.database.base import ProfessorDB, ReviewDB, get_session, upsert_reviews
from pipelines.professors.scrapers import RMPReviewCollector
from pipelines.professors.hierarchical_summarization import (
    HierarchicalSummarizationPipeline,
)
from pipelines.professors.hierarchical_summarization.ai_clients import (
    TamuAIQuotaExceeded,
)
from pipelines.professors.upsert import upsert_professor_summary
from pipelines.professors.review_sync import fetch_rating_counts, needs_review_fetch
from aggiermp.core.name_matching import parse_person_name, professor_names_match

# Checkpoint file for resume functionality
CHECKPOINT_FILE = Path(__file__).parent / ".review_summary_checkpoint.json"


def configure_console_logging(verbose: bool = False) -> None:
    """Keep routine dependency traffic out of the operator-facing console."""
    quiet_level = logging.INFO if verbose else logging.WARNING
    for name in NOISY_LOGGERS:
        logging.getLogger(name).setLevel(quiet_level)

    pipeline_level = logging.DEBUG if verbose else logging.WARNING
    logging.getLogger("pipelines.professors.hierarchical_summarization").setLevel(
        pipeline_level
    )


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
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=CHECKPOINT_FILE.parent,
            prefix=".summary-checkpoint-",
            suffix=".tmp",
            delete=False,
        ) as f:
            temporary_path = Path(f.name)
            json.dump({"processed_professors": sorted(processed_professors)}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary_path, CHECKPOINT_FILE)
    except Exception as e:
        print(f"Warning: Could not save checkpoint: {e}")
    finally:
        if "temporary_path" in locals():
            temporary_path.unlink(missing_ok=True)


def clear_checkpoint() -> None:
    """Clear the checkpoint file"""
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


def current_instructor_professor_ids(
    session: Any, now: datetime | None = None
) -> Set[str]:
    """Professors teaching a section in the current or next term.

    Terms count when they have not ended and start within ~6 months, which
    covers the running semester plus the one students are registering for.
    """
    from sqlalchemy import text

    now = now or datetime.now()
    try:
        names = [
            row[0]
            for row in session.execute(
                text(
                    "SELECT DISTINCT si.instructor_name FROM section_instructors si "
                    "JOIN terms t ON t.term_code = si.term_code "
                    "WHERE t.end_date >= :now AND t.start_date <= :horizon"
                ),
                {"now": now, "horizon": now + timedelta(days=183)},
            )
            if row[0]
        ]
        professors = session.execute(
            text("SELECT id, first_name, last_name FROM professors")
        ).fetchall()
    except Exception as exc:
        session.rollback()
        print(f"Warning: could not read current sections for ordering: {exc}")
        return set()

    # Index by surname tokens, then confirm with the shared name matcher.
    by_surname: Dict[str, List[Tuple[str, str]]] = {}
    for professor_id, first_name, last_name in professors:
        full_name = f"{first_name} {last_name}"
        for token in parse_person_name(full_name).surname:
            by_surname.setdefault(token, []).append((professor_id, full_name))
    matched: Set[str] = set()
    for name in names:
        for token in parse_person_name(name).surname:
            for professor_id, full_name in by_surname.get(token, []):
                if professor_id not in matched and professor_names_match(
                    name, full_name
                ):
                    matched.add(professor_id)
    return matched


def prioritize_current_instructors(
    session: Any, professor_ids: List[str]
) -> Tuple[List[str], int]:
    """Stable reorder: current/next-term instructors first, then DB order."""
    current = current_instructor_professor_ids(session)
    first = [pid for pid in professor_ids if pid in current]
    rest = [pid for pid in professor_ids if pid not in current]
    return first + rest, len(first)


def print_summary_for_review(professor_summary: Any) -> None:
    """Dry-run output: the stored fields plus request and claim counters."""
    data = asdict(professor_summary)
    text = json.dumps(data, indent=2, default=str, ensure_ascii=False)
    # A Windows console (cp1252) cannot print every character in review text.
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    print(text.encode(encoding, errors="replace").decode(encoding))


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
    collector = None
    try:
        # Create a new collector instance (one per thread)
        collector = RMPReviewCollector()
        # Get only new reviews (optimized fetch)
        new_reviews = collector.get_new_reviews(professor_id, list(existing_review_ids))

        return (professor_id, new_reviews)
    except Exception as e:
        raise RuntimeError(f"Review fetch failed for {professor_id}") from e
    finally:
        if collector is not None:
            collector.session.close()


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
    max_workers: int = 4,  # Parallel review fetching
    batch_size: int = 50,  # Batch size for review fetching and summarization
    max_summaries: int = 0,  # Zero means unlimited
    stored_reviews_only: bool = False,
    min_new_review_percent: float = 10.0,
    dry_run: bool = False,
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
        max_summaries: Maximum professor summaries to generate in this run.
                       Zero means unlimited.
        min_new_review_percent: New reviews required as a percentage of the
                                last overall summary's count; force bypasses it.
        dry_run: Generate and print summaries from stored reviews without
                 writing reviews, summaries, or checkpoints.

    Returns:
        Dictionary with results
    """
    close_session = False
    if not 0 <= min_new_review_percent <= 100:
        raise ValueError("min_new_review_percent must be between 0 and 100")
    if session is None:
        session = get_session()
        close_session = True

    if dry_run:
        stored_reviews_only = True
        skip_if_no_new_reviews = False
        resume = False
    try:
        if clear_checkpoint_on_start and not dry_run:
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
            # Students need professors with upcoming sections first.
            professor_ids, current_count = prioritize_current_instructors(
                session, professor_ids
            )
            if current_count:
                print(
                    f"Ordering: {current_count} professors teaching current/next-term "
                    "sections first"
                )

        # Filter out already processed professors
        remaining_professors = [
            pid for pid in professor_ids if pid not in processed_professors
        ]
        # Use actual stored rows, not potentially stale remote rating counts.
        all_existing_reviews = session.query(ReviewDB.professor_id, ReviewDB.id).all()
        professor_existing_reviews: Dict[str, Set[str]] = {}
        for prof_id, review_id in all_existing_reviews:
            professor_existing_reviews.setdefault(prof_id, set()).add(review_id)
        empty_professors = 0
        if stored_reviews_only:
            eligible = [
                pid
                for pid in remaining_professors
                if professor_existing_reviews.get(pid)
            ]
            empty_professors = len(remaining_professors) - len(eligible)
            remaining_professors = eligible
            if empty_professors:
                print(
                    f"Stored-only: excluded {empty_professors} professors with no stored reviews"
                )
        mode = "incremental" if skip_if_no_new_reviews else "force update"
        typesafe_enabled = bool(os.getenv("TYPESAFE_API_KEY")) and os.getenv(
            "TYPESAFE_VERIFY", "0"
        ).strip().lower() in {"1", "true", "yes", "on"}
        providers = [
            (
                "TAMU AI (1 request/professor)"
                if os.getenv("TAMU_AI_API_KEY")
                else "local BART"
            ),
            "TypeSafe verification" if typesafe_enabled else "no TypeSafe",
        ]
        if dry_run:
            providers.append("DRY RUN (no writes)")
        print(
            f"Summarization run: {len(remaining_professors)}/{len(professor_ids)} "
            f"professors | {mode} | {' + '.join(providers)}"
        )

        # Track results
        results: Dict[str, Any] = {
            "professors_processed": 0,
            "reviews_added": 0,
            "summaries_generated": 0,
            "professors_skipped": empty_professors,
            "quota_exhausted": False,
            "tamu_requests": 0,
            "claims_accepted": 0,
            "claims_rejected": 0,
            "errors": [],
        }
        stop_requested = False

        if not remaining_professors:
            print("Nothing to summarize.")
            return results

        # Initialize models only after determining there is work to do.
        print("Preparing models...", end=" ", flush=True)
        pipeline = HierarchicalSummarizationPipeline(session=session)
        print("ready")

        remote_counts = {}
        if not stored_reviews_only:
            print("Checking rating counts by school...", flush=True)
            remote_counts = fetch_rating_counts(session, remaining_professors)

        # Process in batches
        total_batches = (len(remaining_professors) + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            batch_start = batch_num * batch_size
            batch_end = min(batch_start + batch_size, len(remaining_professors))
            batch_professors = remaining_professors[batch_start:batch_end]

            print(
                f"\nBatch {batch_num + 1}/{total_batches} ({len(batch_professors)} professors)"
            )

            # STEP 1: Parallel review fetching
            fetch_professors = (
                []
                if stored_reviews_only
                else [
                    pid
                    for pid in batch_professors
                    if needs_review_fetch(
                        len(professor_existing_reviews.get(pid, set())),
                        remote_counts.get(pid),
                    )
                ]
            )
            print(
                f"  Reviews: {len(fetch_professors)} professors to fetch; {len(batch_professors) - len(fetch_professors)} skipped ({'stored-only mode' if stored_reviews_only else 'matching counts'})",
                flush=True,
            )
            new_reviews_dict: Dict[str, List] = {}
            failed_fetches = set()

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        get_new_reviews_for_professor,
                        prof_id,
                        professor_existing_reviews.get(prof_id, set()),
                    ): prof_id
                    for prof_id in fetch_professors
                }

                for future in as_completed(futures):
                    try:
                        prof_id, new_reviews = future.result()
                    except Exception as exc:
                        prof_id = futures[future]
                        failed_fetches.add(prof_id)
                        results["errors"].append(
                            f"Review fetch failed for {prof_id}: {exc}"
                        )
                        print(
                            f"  Review fetch failed for {prof_id}; leaving it for retry",
                            flush=True,
                        )
                        continue
                    if new_reviews:
                        new_reviews_dict[prof_id] = new_reviews

            # STEP 2: Batch upsert new reviews
            all_new_reviews = []
            for prof_id, reviews in new_reviews_dict.items():
                all_new_reviews.extend(reviews)

            if all_new_reviews:
                print(f"{len(all_new_reviews)} new", flush=True)
                batch_upsert_reviews(session, all_new_reviews)
                session.commit()  # Commit reviews batch
                results["reviews_added"] += len(all_new_reviews)

                # Update existing review IDs for next batch
                for review in all_new_reviews:
                    if review.professor_id not in professor_existing_reviews:
                        professor_existing_reviews[review.professor_id] = set()
                    professor_existing_reviews[review.professor_id].add(review.id)
            else:
                print("no new reviews", flush=True)

            # STEP 3: Get all reviews for professors and process summaries (sequential for GPU)
            print("  Summaries:", flush=True)
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

            professors_to_summarize = [
                pid for pid in professors_to_summarize if pid not in failed_fetches
            ]

            jobs = []
            for i, professor_id in enumerate(professors_to_summarize, 1):
                if (
                    max_summaries
                    and results["summaries_generated"] + len(jobs) >= max_summaries
                ):
                    print(f"Reached max_summaries={max_summaries}; saving checkpoint")
                    stop_requested = True
                    break
                try:
                    # Get all reviews for this professor
                    all_reviews = (
                        session.query(ReviewDB)
                        .filter_by(professor_id=professor_id)
                        .all()
                    )

                    print(
                        f"    [{i}/{len(professors_to_summarize)}] "
                        f"{professor_id[:20]}... | {len(all_reviews)} reviews",
                        flush=True,
                    )

                    if not all_reviews:
                        print("      skipped | no reviews")
                        results["professors_skipped"] += 1
                        continue

                    # Convert to dict list
                    raw_reviews = [
                        {
                            "id": review.id,
                            "professor_id": review.professor_id,
                            "course_code": review.course_code,
                            "review_text": review.review_text,
                            "clarity_rating": review.clarity_rating,
                            "difficulty_rating": review.difficulty_rating,
                            "helpful_rating": review.helpful_rating,
                            "rating_tags": review.rating_tags,
                            # Metadata for the Python-computed review stats
                            "would_take_again": review.would_take_again,
                            "grade": review.grade,
                            "attendance_mandatory": review.attendance_mandatory,
                            "textbook_use": review.textbook_use,
                            "review_date": review.review_date,
                        }
                        for review in all_reviews
                    ]

                    # Smart Skip: Check if we actually need to regenerate summary
                    # Check both overall AND per-course counts are in sync
                    if skip_if_no_new_reviews:
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

                        # Compare with the last saved summary so deferred reviews
                        # accumulate even when this run fetched no new rows.
                        summarized_count = summary_counts.get(None)
                        if summarized_count and len(raw_reviews) > summarized_count:
                            added = len(raw_reviews) - summarized_count
                            required = max(
                                1, ceil(summarized_count * min_new_review_percent / 100)
                            )
                            if added < required:
                                print(
                                    f"      deferred | {added}/{required} new reviews required ({min_new_review_percent:g}% of {summarized_count} summarized)"
                                )
                                results["professors_skipped"] += 1
                                # Do not checkpoint deferrals as completed: the
                                # next run must reconsider accumulated evidence.
                                continue

                        # Compare only the overall count: course rows are keyed by
                        # normalized codes (e.g. ASTR111), so comparing them with
                        # raw RMP codes (ASTRONOMY111) forced needless regeneration.
                        counts_match = summarized_count == len(raw_reviews)

                        if counts_match and len(summary_counts) > 0:
                            print("      skipped | up to date")
                            results["professors_processed"] += 1
                            processed_professors.add(professor_id)
                            continue

                    jobs.append((professor_id, raw_reviews))

                except TamuAIQuotaExceeded as e:
                    print(f"      stopped | TAMU AI allowance exhausted: {e}")
                    results["quota_exhausted"] = True
                    stop_requested = True
                    break
                except Exception as e:
                    error_msg = f"Error processing professor {professor_id}: {e}"
                    print(f"      failed | {e}")
                    results["errors"].append(error_msg)
                    # Rollback on error
                    try:
                        session.rollback()
                    except Exception:
                        pass

            started_at = time.perf_counter()
            client = pipeline.summarizer.tamu_client
            tamu_before = client.requests_made if client else 0
            retries_before = client.retries_made + client.json_retries if client else 0
            fallbacks_before = pipeline.summarizer.fallback_clusters
            try:
                for (
                    professor_id,
                    professor_summary,
                    error,
                ) in pipeline.process_professor_batch(jobs):
                    try:
                        if error is not None:
                            raise error
                        if dry_run:
                            print_summary_for_review(professor_summary)
                        elif not upsert_professor_summary(professor_summary, session):
                            raise RuntimeError("database save failed")
                        results["summaries_generated"] += 1
                        results["professors_processed"] += 1
                        processed_professors.add(professor_id)
                        if use_checkpoint:
                            save_checkpoint(processed_professors)
                        generation = (
                            getattr(professor_summary, "generation_stats", None) or {}
                        )
                        claim_note = ""
                        if generation:
                            rejected = generation.get(
                                "claims_rejected_citation", 0
                            ) + generation.get("claims_rejected_support", 0)
                            claim_note = (
                                f" | TAMU requests {generation.get('requests', 0)}"
                                f" | claims {generation.get('claims_accepted', 0)} accepted,"
                                f" {rejected} rejected"
                            )
                            results["claims_accepted"] += generation.get(
                                "claims_accepted", 0
                            )
                            results["claims_rejected"] += rejected
                        print(
                            f"      {professor_id} | {'generated' if dry_run else 'saved'} | {len(professor_summary.course_summaries)} courses | {professor_summary.overall_sentiment} | confidence {professor_summary.confidence:.0%}{claim_note}",
                            flush=True,
                        )
                    except TamuAIQuotaExceeded:
                        raise
                    except Exception as exc:
                        results["errors"].append(
                            f"Error processing professor {professor_id}: {exc}"
                        )
                        print(f"      {professor_id} | failed | {exc}", flush=True)
                        session.rollback()
            except TamuAIQuotaExceeded as exc:
                print(f"      stopped | TAMU AI allowance exhausted: {exc}")
                results["quota_exhausted"] = True
                stop_requested = True
            if jobs:
                calls = (client.requests_made if client else 0) - tamu_before
                retries = (
                    client.retries_made + client.json_retries if client else 0
                ) - retries_before
                fallbacks = pipeline.summarizer.fallback_clusters - fallbacks_before
                results["tamu_requests"] += calls
                print(
                    f"  Batch totals: TAMU requests {calls} | retries {retries} | local fallbacks {fallbacks} | {time.perf_counter() - started_at:.1f}s",
                    flush=True,
                )

            # Save checkpoint after each batch
            if use_checkpoint:
                save_checkpoint(processed_professors)

            if stop_requested:
                break

        # Clear checkpoint on successful completion
        if use_checkpoint and not stop_requested and not results["errors"]:
            print("Processing complete. Clearing checkpoint.")
            clear_checkpoint()

        # Print summary
        run_status = (
            "Stopped (TAMU allowance exhausted)"
            if results["quota_exhausted"]
            else "Complete"
        )
        print(
            f"\n{run_status}: {results['professors_processed']} processed, "
            f"{results['reviews_added']} reviews added, "
            f"{results['summaries_generated']} summaries, "
            f"{results['tamu_requests']} TAMU requests, "
            f"{len(results['errors'])} errors"
        )

        return results

    except KeyboardInterrupt:
        if "use_checkpoint" in locals() and use_checkpoint:
            save_checkpoint(processed_professors)
        print("\nInterrupted; completed professors have been checkpointed.")
        raise
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
        "--professor-id",
        nargs="+",
        help="Process only these professor IDs (space separated)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Summarize stored reviews and print the results without writing anything",
    )
    parser.add_argument(
        "--max-summaries",
        type=int,
        default=int(os.getenv("TAMU_AI_MAX_SUMMARIES_PER_RUN", "0")),
        help="Stop after generating this many professor summaries (0 = unlimited)",
    )
    parser.add_argument(
        "--stored-reviews-only",
        action="store_true",
        help="Use stored reviews without contacting RMP; combine with --force-update to regenerate summaries",
    )
    parser.add_argument(
        "--min-new-review-percent",
        type=float,
        default=float(os.getenv("SUMMARY_MIN_NEW_REVIEW_PERCENT", "10")),
        help="Regenerate existing summaries after this percentage of new reviews accumulates (default: 10; 0 = any new review)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show model, clustering, and dependency request details",
    )
    args = parser.parse_args()
    if not 0 <= args.min_new_review_percent <= 100:
        parser.error("--min-new-review-percent must be between 0 and 100")

    configure_console_logging(args.verbose)

    # Set environment variables for performance
    os.environ["OMP_NUM_THREADS"] = "4"
    os.environ["TOKENIZERS_PARALLELISM"] = "true"

    # Process all professors (or specify a list of IDs)
    professor_ids = args.professor_id or None

    result = upsert_reviews_and_summaries(
        professor_ids=professor_ids,
        # Forced regeneration still needs checkpoints so a quota-limited run can
        # continue with the next professor after the TAMU allowance resets.
        resume=(not args.no_resume and not args.professor_id),
        clear_checkpoint_on_start=args.clear_checkpoint,
        skip_if_no_new_reviews=not args.force_update,
        max_summaries=args.max_summaries,
        stored_reviews_only=args.stored_reviews_only,
        min_new_review_percent=args.min_new_review_percent,
        dry_run=args.dry_run,
    )

    if "error" in result or result.get("errors"):
        sys.exit(1)


if __name__ == "__main__":
    main()
