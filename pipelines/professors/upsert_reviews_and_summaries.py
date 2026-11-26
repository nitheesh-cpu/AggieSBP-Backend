"""
Upsert script for reviews and summaries.

This script:
1. Gets new reviews for all professors (in parallel)
2. Upserts reviews to the reviews table (batched)
3. Runs the hierarchical summarization pipeline (batched)
4. Upserts summaries to professor_summaries_new table (batched)

With --fetch-only, steps 3-4 are replaced by queueing professors in
admin_summary_rebuilds so the admin worker rebuilds them later (no AI calls).
A full --fetch-only run first imports professors new on RMP (see
upsert_professors.py), so new profiles get reviews and summaries too.

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

from aggiermp.database.base import (
    ProfessorDB,
    ProfessorSummaryNewDB,
    ReviewDB,
    get_session,
    upsert_reviews,
)
from pipelines.professors.scrapers import RMPReviewCollector
from pipelines.professors.hierarchical_summarization import (
    HierarchicalSummarizationPipeline,
)
from pipelines.professors.hierarchical_summarization.ai_clients import (
    TamuAIQuotaExceeded,
)
from pipelines.professors.upsert import upsert_professor_summary
from pipelines.professors.review_sync import fetch_rating_counts, needs_review_fetch
from aggiermp.core.hidden_professors import hidden_professor_ids
from aggiermp.core.name_matching import parse_person_name, professor_names_match
from aggiermp.core.professor_identity import load_aliases
from aggiermp.core.summary_lock import serialized_summary_run

logger = logging.getLogger(__name__)

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


def summary_deferral(
    summarized_count: int | None, review_count: int, min_new_review_percent: float
) -> Tuple[int, int] | None:
    """The single rule for holding back a summary rebuild.

    Returns (added, required) when an existing summary was built from
    `summarized_count` reviews, `review_count` are stored now, and fewer than
    `min_new_review_percent` percent were added; None means "do not defer".
    """
    if summarized_count and review_count > summarized_count:
        added = review_count - summarized_count
        required = max(1, ceil(summarized_count * min_new_review_percent / 100))
        if added < required:
            return added, required
    return None


def batch_upsert_reviews(session: Any, reviews_batch: List[Any]) -> None:
    """Batch upsert reviews for better performance"""
    if not reviews_batch:
        return
    upsert_reviews(session, reviews_batch)


def without_hidden(
    items: List[Any], hidden_ids: Set[str], key=lambda item: item
) -> List[Any]:
    """Drop entries whose professor id is hidden (takedown requests)."""
    if not hidden_ids:
        return list(items)
    return [item for item in items if key(item) not in hidden_ids]


@serialized_summary_run
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
    invalidate_cache: bool = True,
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
        invalidate_cache: Clear the API cache and website pages for every
                 professor whose reviews or summary were saved (done once at
                 the end of the run, also after an error or interrupt). The
                 admin worker passes False because it clears per professor.

    Returns:
        Dictionary with results
    """
    close_session = False
    changed_professor_ids: Set[str] = set()
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

        # Confirmed duplicate merges (see pipelines/professors/merge_duplicates.py).
        # Every RMP entry is still fetched and stored as-is; summaries are
        # built once per merged professor from all of its entries' reviews.
        aliases = load_aliases(session)
        merged_members: Dict[str, List[str]] = {}
        for alias_id, canonical_id in aliases.items():
            merged_members.setdefault(canonical_id, []).append(alias_id)

        # Filter out already processed professors
        remaining_professors = [
            pid for pid in professor_ids if pid not in processed_professors
        ]
        # Hidden professors get no review fetch, no review insert, no summary.
        hidden_ids = hidden_professor_ids(session)
        visible_professors = without_hidden(remaining_professors, hidden_ids)
        hidden_skipped = len(remaining_professors) - len(visible_professors)
        remaining_professors = visible_professors
        print(f"Hidden professors skipped: {hidden_skipped}")
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
                if any(
                    professor_existing_reviews.get(member)
                    for member in [
                        aliases.get(pid, pid),
                        *merged_members.get(aliases.get(pid, pid), []),
                    ]
                )
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
            "hidden_skipped": hidden_skipped,
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
            all_new_reviews = without_hidden(
                all_new_reviews, hidden_ids, lambda review: review.professor_id
            )

            if all_new_reviews:
                print(f"{len(all_new_reviews)} new", flush=True)
                batch_upsert_reviews(session, all_new_reviews)
                session.commit()  # Commit reviews batch
                results["reviews_added"] += len(all_new_reviews)
                changed_professor_ids.update(r.professor_id for r in all_new_reviews)

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

            professors_to_summarize = list(
                dict.fromkeys(
                    aliases.get(pid, pid)
                    for pid in professors_to_summarize
                    if pid not in failed_fetches
                )
            )

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
                    # A merged professor's reviews come from every merged entry.
                    # filter_by for one member keeps test mocks valid (docs/context/gotchas.md G-004).
                    member_ids = [professor_id] + merged_members.get(professor_id, [])
                    review_query = session.query(ReviewDB)
                    all_reviews = (
                        review_query.filter(ReviewDB.professor_id.in_(member_ids))
                        if len(member_ids) > 1
                        else review_query.filter_by(professor_id=professor_id)
                    ).all()

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
                            "professor_id": professor_id,
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
                        deferral = summary_deferral(
                            summarized_count, len(raw_reviews), min_new_review_percent
                        )
                        if deferral:
                            added, required = deferral
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
                        if not dry_run:
                            changed_professor_ids.add(professor_id)
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
        if invalidate_cache and not dry_run and changed_professor_ids:
            try:
                from aggiermp.core.cache_invalidation import invalidate_professors

                outcome = invalidate_professors(changed_professor_ids, db=session)
                print(
                    f"Cache: {len(changed_professor_ids)} professors invalidated, "
                    f"{outcome.keys_deleted} API keys cleared, website "
                    f"{outcome.revalidation}",
                    flush=True,
                )
            except Exception as exc:  # never fail the run over a cache problem
                print(f"Cache invalidation failed: {exc}", flush=True)
        if close_session:
            session.close()


def plan_rebuilds(
    totals: Dict[str, int],
    summarized: Dict[str, int | None],
    new_ids: Set[str],
    min_new_review_percent: float,
) -> List[Tuple[str, str, bool]]:
    """Decide which merged professors need a summary rebuild.

    totals: canonical professor id -> stored review count (all merged entries).
    summarized: canonical id -> review count behind its overall summary (absent
        or 0 when it has no summary yet).
    new_ids: canonical ids that received new reviews in this run.

    Returns (professor_id, reason, bump) rows. bump is True when new reviews
    arrived, so an already pending rebuild must be marked changed again;
    professors that merely lack a summary are queued once without churning
    their revision every night.
    """
    plan: List[Tuple[str, str, bool]] = []
    for professor_id, total in totals.items():
        summarized_count = summarized.get(professor_id)
        is_new = professor_id in new_ids
        if not total or not (is_new or not summarized_count):
            continue
        if not summarized_count:
            plan.append((professor_id, f"No summary yet ({total} reviews)", is_new))
        elif total == summarized_count or summary_deferral(
            summarized_count, total, min_new_review_percent
        ):
            continue
        elif total > summarized_count:
            added = total - summarized_count
            percent = added * 100 / summarized_count
            noun = "review" if added == 1 else "reviews"
            plan.append((professor_id, f"{added} new {noun} (+{percent:.0f}%)", True))
        else:
            plan.append(
                (
                    professor_id,
                    f"Review count changed ({summarized_count} to {total})",
                    True,
                )
            )
    return plan


QUEUE_BUMP_SQL = (
    "INSERT INTO admin_summary_rebuilds(professor_id, reason) VALUES (:id, :reason) "
    "ON CONFLICT (professor_id) DO UPDATE SET "
    "revision = admin_summary_rebuilds.revision + 1, "
    "reason = EXCLUDED.reason, changed_at = now()"
)
QUEUE_KEEP_SQL = (
    "INSERT INTO admin_summary_rebuilds(professor_id, reason) VALUES (:id, :reason) "
    "ON CONFLICT (professor_id) DO NOTHING"
)


def rebuild_queue_available(session: Any) -> bool:
    from sqlalchemy import text

    return bool(
        session.execute(
            text("SELECT to_regclass('admin_summary_rebuilds') IS NOT NULL")
        ).scalar()
    )


def queue_summary_rebuilds(session: Any, plan: List[Tuple[str, str, bool]]) -> int:
    """Write the plan to admin_summary_rebuilds in one transaction.

    The admin worker deletes a queue row only if revision and changed_at are
    unchanged since it started, so bumping the revision on conflict keeps a
    professor pending when new reviews land during a rebuild.
    """
    from sqlalchemy import text

    try:
        for professor_id, reason, bump in plan:
            session.execute(
                text(QUEUE_BUMP_SQL if bump else QUEUE_KEEP_SQL),
                {"id": professor_id, "reason": reason},
            )
        session.commit()
    except Exception:
        session.rollback()
        raise
    return len(plan)


def fetch_reviews_and_queue_rebuilds(
    professor_ids: List[str] | None = None,
    session: Any = None,
    max_workers: int = 4,
    batch_size: int = 50,
    min_new_review_percent: float = 10.0,
    dry_run: bool = False,
    invalidate_cache: bool = True,
    discover_professors: bool = True,
) -> Dict[str, Any]:
    """Nightly mode: store new RMP reviews and queue summary rebuilds.

    For a full run (no ``professor_ids``), professors new on RMP at each
    campus are imported first. Without that, a profile created after the
    last manual upsert_professors.py run (Allison Goetz, RMP 5108901) never
    reached the professors table, so no course page could show it.

    No AI provider is created or called. Reviews are fetched for professors
    whose RMP rating count changed (as in the full pipeline), stored, their
    caches cleared, and professors whose reviews would trigger a summary under
    the usual threshold are added to admin_summary_rebuilds for the admin
    worker. It is deliberately not wrapped in serialized_summary_run: it never
    generates summaries, review inserts are idempotent, and the queue write
    happens after the reviews are committed, so a worker that sees a bumped
    revision also sees the reviews. It need not wait for a summary run.
    """
    if not 0 <= min_new_review_percent <= 100:
        raise ValueError("min_new_review_percent must be between 0 and 100")
    close_session = session is None
    if session is None:
        session = get_session()
    changed_professor_ids: Set[str] = set()
    results: Dict[str, Any] = {
        "professors_checked": 0,
        "professors_fetched": 0,
        "reviews_added": 0,
        "professors_queued": 0,
        "professors_added": 0,
        "queue_unavailable": False,
        "errors": [],
    }
    try:
        if professor_ids is None and discover_professors:
            if dry_run:
                print("Dry run: not importing professors new on RMP", flush=True)
            else:
                from pipelines.professors.campuses import get_university_ids
                from pipelines.professors.upsert_professors import upsert_all_professors

                for university_id in get_university_ids(None):
                    imported = upsert_all_professors(university_id, session)
                    if "error" in imported:
                        # Review fetching for known professors still runs.
                        results["errors"].append(
                            f"Professor import failed for {university_id}: {imported['error']}"
                        )
                    results["professors_added"] += imported.get("new", 0)
        if professor_ids is None:
            professor_ids = [prof.id for prof in session.query(ProfessorDB).all()]
        # Hidden professors are not fetched, stored or queued. Their professors
        # row still updates in the import above.
        hidden_ids = hidden_professor_ids(session)
        visible_ids = without_hidden(professor_ids, hidden_ids)
        results["hidden_skipped"] = len(professor_ids) - len(visible_ids)
        professor_ids = visible_ids
        print(f"Hidden professors skipped: {results['hidden_skipped']}", flush=True)
        results["professors_checked"] = len(professor_ids)

        aliases = load_aliases(session)
        merged_members: Dict[str, List[str]] = {}
        for alias_id, canonical_id in aliases.items():
            merged_members.setdefault(canonical_id, []).append(alias_id)

        existing: Dict[str, Set[str]] = {}
        for prof_id, review_id in session.query(
            ReviewDB.professor_id, ReviewDB.id
        ).all():
            existing.setdefault(prof_id, set()).add(review_id)
        summarized: Dict[str, int | None] = {
            row[0]: row[1]
            for row in session.query(
                ProfessorSummaryNewDB.professor_id, ProfessorSummaryNewDB.total_reviews
            )
            .filter(ProfessorSummaryNewDB.course_code.is_(None))
            .all()
        }
        canonical_scope = list(
            dict.fromkeys(aliases.get(pid, pid) for pid in professor_ids)
        )

        def totals_for(canonical_ids: List[str]) -> Dict[str, int]:
            return {
                cid: sum(
                    len(existing.get(member, ()))
                    for member in [cid, *merged_members.get(cid, [])]
                )
                for cid in canonical_ids
            }

        queue_ok = rebuild_queue_available(session)
        if not queue_ok:
            results["queue_unavailable"] = True
            logger.warning(
                "admin_summary_rebuilds does not exist (apply migration "
                "004_admin_jobs.sql); reviews are fetched but no summary "
                "rebuilds will be queued"
            )

        def queue(plan: List[Tuple[str, str, bool]]) -> None:
            plan = without_hidden(plan, hidden_ids, lambda row: row[0])
            if not plan or not queue_ok:
                return
            if dry_run:
                for pid, reason, _ in plan:
                    print(f"  would queue {pid}: {reason}", flush=True)
                results["professors_queued"] += len(plan)
                return
            try:
                results["professors_queued"] += queue_summary_rebuilds(session, plan)
            except Exception as exc:
                results["errors"].append(f"Queueing rebuilds failed: {exc}")
                print(f"  Queueing rebuilds failed: {exc}", flush=True)

        print(
            f"Fetch-only run: {len(professor_ids)} professors"
            f"{' | DRY RUN (no writes)' if dry_run else ''}",
            flush=True,
        )
        # Professors that already have reviews but never got a summary.
        queue(
            plan_rebuilds(
                totals_for(canonical_scope), summarized, set(), min_new_review_percent
            )
        )

        print("Checking rating counts by school...", flush=True)
        remote_counts = fetch_rating_counts(session, professor_ids)
        to_fetch = [
            pid
            for pid in professor_ids
            if needs_review_fetch(len(existing.get(pid, set())), remote_counts.get(pid))
        ]
        print(f"Reviews: {len(to_fetch)} professors to fetch", flush=True)

        for start in range(0, len(to_fetch), batch_size):
            batch = to_fetch[start : start + batch_size]
            new_reviews: List[Any] = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        get_new_reviews_for_professor, pid, existing.get(pid, set())
                    ): pid
                    for pid in batch
                }
                for future in as_completed(futures):
                    try:
                        _, fetched = future.result()
                    except Exception as exc:
                        results["errors"].append(
                            f"Review fetch failed for {futures[future]}: {exc}"
                        )
                        print(f"  Review fetch failed for {futures[future]}", flush=True)
                        continue
                    new_reviews.extend(fetched)
            results["professors_fetched"] += len(batch)
            new_reviews = without_hidden(
                new_reviews, hidden_ids, lambda review: review.professor_id
            )
            if not new_reviews:
                continue

            if not dry_run:
                try:
                    batch_upsert_reviews(session, new_reviews)
                except Exception as exc:
                    # Nothing from this batch is stored; the next run retries it.
                    results["errors"].append(f"Review save failed: {exc}")
                    print(f"  Review save failed: {exc}", flush=True)
                    continue
                changed_professor_ids.update(r.professor_id for r in new_reviews)
            for review in new_reviews:
                existing.setdefault(review.professor_id, set()).add(review.id)
            results["reviews_added"] += len(new_reviews)
            print(f"  {len(new_reviews)} new reviews", flush=True)

            new_canonical = list(
                dict.fromkeys(
                    aliases.get(r.professor_id, r.professor_id) for r in new_reviews
                )
            )
            queue(
                plan_rebuilds(
                    totals_for(new_canonical),
                    summarized,
                    set(new_canonical),
                    min_new_review_percent,
                )
            )

        verb = "would be queued" if dry_run else "queued"
        print(
            f"\nFetch-only complete: {results['professors_checked']} professors checked, "
            f"{results['professors_fetched']} fetched, "
            f"{results['reviews_added']} reviews added, "
            f"{results['professors_queued']} professors {verb} for summary rebuild, "
            f"{len(results['errors'])} errors",
            flush=True,
        )
        return results
    except Exception as exc:
        print(f"\n[ERROR] Fetch-only failed: {exc}")
        import traceback

        traceback.print_exc()
        results["error"] = str(exc)
        return results
    finally:
        if invalidate_cache and not dry_run and changed_professor_ids:
            try:
                from aggiermp.core.cache_invalidation import invalidate_professors

                outcome = invalidate_professors(changed_professor_ids, db=session)
                print(
                    f"Cache: {len(changed_professor_ids)} professors invalidated, "
                    f"{outcome.keys_deleted} API keys cleared, website "
                    f"{outcome.revalidation}",
                    flush=True,
                )
            except Exception as exc:  # never fail the run over a cache problem
                print(f"Cache invalidation failed: {exc}", flush=True)
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
        "--fetch-only",
        action="store_true",
        help="Nightly mode: fetch and store new RMP reviews, clear caches and queue "
        "professors for the admin summary worker; no AI is called. Works with --dry-run",
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

    if args.fetch_only:
        result = fetch_reviews_and_queue_rebuilds(
            professor_ids=professor_ids,
            min_new_review_percent=args.min_new_review_percent,
            dry_run=args.dry_run,
        )
        if "error" in result or result.get("errors"):
            sys.exit(1)
        return

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
