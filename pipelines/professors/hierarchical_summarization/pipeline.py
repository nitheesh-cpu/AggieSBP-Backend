"""
Main pipeline orchestrator for hierarchical summarization.
"""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from collections import deque
from contextlib import nullcontext
from threading import BoundedSemaphore
from dataclasses import dataclass, field, replace
import logging
import os
from typing import Any, Dict, List, NamedTuple, Optional

import numpy as np

logger = logging.getLogger(__name__)

from pipelines.professors.hierarchical_summarization.preprocess import (
    ReviewPreprocessor,
)
from pipelines.professors.hierarchical_summarization.embeddings import (
    EmbeddingGenerator,
)
from pipelines.professors.hierarchical_summarization.clustering import ReviewClusterer
from pipelines.professors.hierarchical_summarization.summarizer import (
    HierarchicalSummarizer,
)
from pipelines.professors.hierarchical_summarization.course_normalizer import (
    CourseNormalizer,
)
from pipelines.professors.hierarchical_summarization.ai_clients import (
    TypeSafeReviewJudge,
    TamuAIError,
    TamuAIInvalidJSON,
    TamuAIQuotaExceeded,
    TamuAITruncated,
)
from pipelines.professors.hierarchical_summarization.professor_request import (
    PartResult,
    ProfessorRequestPart,
    RequestReview,
    build_request_parts,
    map_part_response,
    near_duplicate,
    self_contained,
)
from pipelines.professors.hierarchical_summarization.response_schema import (
    COURSE_FIELDS,
)
from pipelines.professors.hierarchical_summarization.review_stats import (
    _date as review_date,
    compute_review_stats,
    normalize_grade,
    review_quality,
)
from pipelines.professors.hierarchical_summarization.config import (
    CLUSTER_TYPES,
    SUMMARY_MAX_ITEMS_PER_TOPIC,
    SUMMARY_SIMILARITY_THRESHOLD,
)
from pipelines.professors.schemas import (
    ProcessedReview,
    CourseSummary,
    ProfessorSummary,
    SummaryClaim,
)

# Course fields map onto the existing summary columns.
COURSE_COLUMNS = {
    "exams": "exams",
    "grading": "grading",
    "workload": "workload",
    "attendance_policies": "policies",
    "teaching": "teaching",
}


class PreparedProfessor(NamedTuple):
    """Local preparation output; ``units`` are the remote jobs to run.

    Legacy (local BART) units are course -> clusters. Structured units are
    request-part key -> ProfessorRequestPart, usually exactly one.
    """

    professor_id: str
    professor_reviews: List[ProcessedReview]
    total_reviews: int
    units: Dict[str, Any]
    review_counts_by_course: Dict[str, int]
    structured: Optional["StructuredContext"] = None


@dataclass
class StructuredContext:
    raw_reviews: List[dict]
    codes_by_review: Dict[str, List[str]]
    review_texts: Dict[str, str]
    dropped_reviews: int = 0


@dataclass
class PartOutcome:
    """Remote results for one request part, possibly split after a failure."""

    results: List[PartResult] = field(default_factory=list)
    requests: int = 0
    support_rejected: int = 0
    unverified: int = 0


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def headline_sentiment(
    ratings: List[float], ratings_by_course: Dict[str, List[float]]
) -> Optional[str]:
    """Rating-based headline; None when there are no ratings."""
    if not ratings:
        return None
    avg_rating = sum(ratings) / len(ratings)
    positive = sum(rating >= 4.0 for rating in ratings)
    negative = sum(rating <= 2.0 for rating in ratings)
    if avg_rating >= 3.5 and positive >= max(1, negative) * 1.25:
        return "Generally positive"
    if avg_rating <= 2.5 and negative >= max(1, positive) * 1.25:
        return "Generally negative"
    # "Varies by course" is only true when course averages actually differ.
    averages = [
        sum(values) / len(values)
        for values in ratings_by_course.values()
        if len(values) >= 3
    ]
    if len(averages) >= 2 and max(averages) - min(averages) >= 1.0:
        return "Mixed - varies by course"
    return "Mixed reviews"


class HierarchicalSummarizationPipeline:
    """Orchestrates the full hierarchical summarization pipeline"""

    def __init__(self, session: Any = None) -> None:
        """
        Initialize the pipeline.

        Args:
            session: Optional database session for loading cross-listings
        """
        self.embedding_generator = EmbeddingGenerator()
        # Deduplication and clustering use the same sentence-transformer. Sharing
        # it avoids loading a second copy of the model into RAM/VRAM.
        self.preprocessor = ReviewPreprocessor(
            embedding_model=self.embedding_generator.model
        )
        self.clusterer = ReviewClusterer()
        self.summarizer = HierarchicalSummarizer()
        self.course_normalizer = CourseNormalizer(session=session)
        self.typesafe_judge = TypeSafeReviewJudge.from_env()
        self._course_evidence: Dict[str, List[Dict[str, Any]]] = {}
        self._api_gate = BoundedSemaphore(
            max(1, min(4, int(os.getenv("SUMMARY_API_WORKERS", "4"))))
        )

    def process_professor_reviews(
        self, raw_reviews: List[dict], professor_id: str
    ) -> ProfessorSummary:
        prepared = self.prepare_professor_reviews(raw_reviews, professor_id)
        if isinstance(prepared, PreparedProfessor) and prepared.structured:
            results = {
                key: self._prepare_unit(unit) for key, unit in prepared.units.items()
            }
            return self.finish_professor_reviews(prepared, results)
        return self.finish_professor_reviews(prepared)

    @property
    def structured_mode(self) -> bool:
        """One TAMU request per professor whenever TAMU is configured."""
        summarizer = getattr(self, "summarizer", None)
        return getattr(summarizer, "tamu_client", None) is not None

    def prepare_professor_reviews(self, raw_reviews, professor_id):
        """
        Process all reviews for a professor through the full pipeline.

        Args:
            raw_reviews: List of raw review dicts
            professor_id: Professor ID

        Returns:
            ProfessorSummary object
        """
        # Get professor's department for smart course code inference
        professor_dept = None
        if self.course_normalizer.session:
            try:
                from aggiermp.database.base import ProfessorDB

                professor = (
                    self.course_normalizer.session.query(ProfessorDB)
                    .filter_by(id=professor_id)
                    .first()
                )
                if professor and professor.department:
                    professor_dept = professor.department.upper().strip()
            except Exception:
                pass

        if self.structured_mode:
            return self._prepare_structured(raw_reviews, professor_id, professor_dept)

        # Step 1: Preprocess
        self._course_evidence = {}
        processed_reviews = self.preprocessor.process_reviews(
            raw_reviews, deduplicate=True
        )

        if not processed_reviews:
            return ProfessorSummary(
                professor_id=professor_id,
                overall_sentiment="No reviews available",
                strengths=[],
                complaints=[],
                consistency="Unknown",
                confidence=0.0,
                total_reviews=len(raw_reviews),
                course_summaries=[],
            )

        # Professor-level ratings and tags should count each source review once,
        # even when a cross-listed course expands into multiple course records.
        professor_reviews = processed_reviews

        # Step 1.5: Normalize course codes (duplicate reviews with multiple course numbers)
        expanded_reviews = []

        for review in processed_reviews:
            original_code = review.course_code
            normalized_codes = self.course_normalizer.normalize_course_codes(
                original_code, professor_id=professor_id, professor_dept=professor_dept
            )

            for normalized_code in normalized_codes:
                # Keep ratings, difficulty, and tags when expanding cross-listed
                # course codes; these still feed each course's statistics.
                expanded_reviews.append(
                    self._review_for_course(review, normalized_code)
                )

        processed_reviews = expanded_reviews

        # Step 2: Generate embeddings
        embeddings = self.embedding_generator.generate_embeddings_for_reviews(
            processed_reviews, use_cache=True
        )

        # Step 3: Cluster by course
        course_clusters = self.clusterer.cluster_by_course(
            processed_reviews, embeddings
        )

        # Calculate actual review counts per course (before clustering drops noise)
        review_counts_by_course: Dict[str, int] = {}
        for review in processed_reviews:
            course = review.course_code or "UNKNOWN"
            review_counts_by_course[course] = review_counts_by_course.get(course, 0) + 1

        return PreparedProfessor(
            professor_id,
            professor_reviews,
            len(raw_reviews),
            course_clusters,
            review_counts_by_course,
        )

    def finish_professor_reviews(self, prepared, course_results=None):
        if isinstance(prepared, ProfessorSummary):
            return prepared
        if getattr(prepared, "structured", None) is not None:
            return self._finish_structured(prepared, course_results or {})
        (
            professor_id,
            professor_reviews,
            total_reviews,
            course_clusters,
            review_counts_by_course,
        ) = prepared[:5]
        self._course_evidence = {}
        # All local model work and mutable evidence assembly stays on the caller.
        course_summaries = self._generate_course_summaries(
            course_clusters, review_counts_by_course, prepared_results=course_results
        )

        # Step 5: Generate professor summary
        professor_summary = self._generate_professor_summary(
            professor_id,
            course_summaries,
            professor_reviews,
            total_review_count=total_reviews,
        )

        return professor_summary

    def process_professor_batch(self, jobs):
        """Share a bounded API queue across professors; yield completed ones.

        Preprocessing, assembly, and caller-owned database writes are sequential.
        Only _prepare_unit executes in worker threads: usually one TAMU request
        per professor, or one per course for the local BART path. Errors belong
        to one professor; quota errors stop the batch before incomplete saves.
        """
        contexts, results, remaining = {}, {}, {}
        ready = deque()
        for professor_id, raw_reviews in jobs:
            try:
                context = self.prepare_professor_reviews(raw_reviews, professor_id)
                if isinstance(context, ProfessorSummary):
                    yield professor_id, context, None
                    continue
                contexts[professor_id] = context
                results[professor_id] = {}
                remaining[professor_id] = deque(context[3].items())
                if remaining[professor_id]:
                    ready.append(professor_id)
                else:
                    yield professor_id, self.finish_professor_reviews(context, {}), None
            except Exception as exc:
                yield professor_id, None, exc

        workers = max(1, min(4, int(os.getenv("SUMMARY_API_WORKERS", "4"))))
        failed = set()
        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending = {}

            def fill():
                while ready and len(pending) < workers:
                    professor_id = ready.popleft()
                    if professor_id in failed:
                        continue
                    code, unit = remaining[professor_id].popleft()
                    pending[executor.submit(self._prepare_unit, unit)] = (
                        professor_id,
                        code,
                    )
                    if remaining[professor_id]:
                        ready.append(professor_id)

            try:
                fill()
                while pending:
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    # Quota always wins over refilling or saving any results in
                    # this completion group; successful earlier saves stand.
                    for future in done:
                        if isinstance(future.exception(), TamuAIQuotaExceeded):
                            raise future.exception()
                    completed = []
                    for future in done:
                        professor_id, code = pending.pop(future)
                        if professor_id in failed:
                            continue
                        try:
                            results[professor_id][code] = future.result()
                        except Exception as exc:
                            failed.add(professor_id)
                            completed.append((professor_id, None, exc))
                            continue
                        if len(results[professor_id]) == len(contexts[professor_id][3]):
                            completed.append(
                                (professor_id, contexts[professor_id], None)
                            )
                    fill()
                    for professor_id, context, error in completed:
                        if error is not None:
                            yield professor_id, None, error
                        else:
                            try:
                                summary = self.finish_professor_reviews(
                                    context, results[professor_id]
                                )
                            except TamuAIQuotaExceeded:
                                raise
                            except Exception as exc:
                                yield professor_id, None, exc
                            else:
                                yield professor_id, summary, None
            finally:
                for future in pending:
                    future.cancel()

    def _prepare_unit(self, unit):
        if isinstance(unit, ProfessorRequestPart):
            return self._request_part(unit)
        return self._prepare_course(unit)

    def _prepare_course(self, clusters):
        with getattr(self, "_api_gate", nullcontext()):
            return self._prepare_course_api(clusters)

    def _prepare_course_api(self, clusters):
        """Legacy local path: optional TypeSafe topic/sentiment judgments."""
        judgments = (
            self.typesafe_judge.classify_clusters(clusters)
            if self.typesafe_judge is not None
            else {}
        )
        cluster_types, cluster_topics, disagreement, sentiments = {}, {}, {}, {}
        for cluster_id, reviews in clusters.items():
            judgment = judgments.get(cluster_id)
            topic = (
                judgment.topic
                if judgment
                else self.clusterer.identify_cluster_type(reviews)
            )
            cluster_types[cluster_id] = topic
            cluster_topics[cluster_id] = (
                judgment.relevant_topics(self.typesafe_judge.topic_threshold)
                if judgment
                else [topic]
            )
            disagreement[cluster_id] = bool(
                judgment
                and judgment.disagreement_probability
                >= self.typesafe_judge.risk_threshold
            )
            if judgment:
                sentiments[cluster_id] = judgment.sentiment
        # Local (no TAMU) path: BART generates text during assembly.
        generated: Dict[int, Any] = {}
        verifications = (
            self.typesafe_judge.verify_claims(clusters, generated)
            if self.typesafe_judge is not None and generated
            else {}
        )
        return (
            judgments,
            cluster_types,
            cluster_topics,
            disagreement,
            sentiments,
            generated,
            verifications,
        )

    def _prepared_courses(self, course_clusters):
        """Bound remote work in flight and preserve course order for assembly."""
        workers = max(1, min(4, int(os.getenv("SUMMARY_API_WORKERS", "4"))))
        items = iter(course_clusters.items())
        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending = []
            try:
                for _ in range(workers):
                    item = next(items, None)
                    if item is not None:
                        pending.append(
                            (*item, executor.submit(self._prepare_course, item[1]))
                        )
                while pending:
                    course_code, clusters, future = pending.pop(0)
                    prepared = future.result()
                    item = next(items, None)
                    if item is not None:
                        pending.append(
                            (*item, executor.submit(self._prepare_course, item[1]))
                        )
                    # Keep workers occupied while the main thread assembles results.
                    yield course_code, clusters, prepared
            finally:
                for _, _, future in pending:
                    future.cancel()

    def _generate_course_summaries(
        self,
        course_clusters: Dict[str, Dict[int, List[ProcessedReview]]],
        review_counts_by_course: Dict[str, int],
        prepared_results=None,
    ) -> List[CourseSummary]:
        """Generate structured summaries for each course"""
        course_summaries = []

        prepared_courses = (
            self._prepared_courses(course_clusters)
            if prepared_results is None
            else (
                (code, clusters, prepared_results[code])
                for code, clusters in course_clusters.items()
            )
        )
        for course_code, clusters, prepared in prepared_courses:
            (
                judgments,
                cluster_types,
                cluster_topics,
                preserve_disagreement,
                sentiments,
                generated,
                claim_verifications,
            ) = prepared

            # Summarize clusters
            cluster_summaries = self.summarizer.summarize_clusters(
                clusters,
                cluster_types,
                sentiments,
                cluster_topics,
                preserve_disagreement,
                generated=generated,
            )

            # Verify generated claims independently. This preserves supported
            # claims when a sibling claim fails instead of replacing everything.
            if self.typesafe_judge is not None:
                for cluster_id, cluster_summary in zip(
                    clusters.keys(), cluster_summaries
                ):
                    judgment = judgments.get(cluster_id)
                    if judgment is not None:
                        cluster_summary.topics = judgment.relevant_topics(
                            self.typesafe_judge.topic_threshold
                        )
                        cluster_summary.disagreement_probability = (
                            judgment.disagreement_probability
                        )
                        cluster_summary.importance = judgment.importance

                    accepted_claims = []
                    supports = []
                    for claim_index, claim in enumerate(cluster_summary.claims):
                        verification = claim_verifications.get(
                            (cluster_id, claim_index)
                        )
                        if verification is None:
                            # A configured verifier must not silently accept an
                            # unchecked claim after an API or response failure.
                            claim.accepted = False
                            continue
                        claim.support_probability = verification.support_probability
                        claim.overgeneralization_probability = (
                            verification.overgeneralization_probability
                        )
                        claim.disagreement_erasure_probability = (
                            verification.disagreement_erasure_probability
                        )
                        if (
                            verification.topic in CLUSTER_TYPES
                            and verification.topic_confidence
                            >= self.typesafe_judge.topic_threshold
                        ):
                            claim.topic = verification.topic
                        claim.sentiment = verification.sentiment
                        claim.sentiment_confidence = verification.sentiment_confidence
                        claim.importance = verification.importance
                        claim.importance_confidence = verification.importance_confidence
                        # Only low support rejects a claim; the risk scores
                        # are kept for diagnostics but no longer discard text.
                        claim.accepted = (
                            verification.support_probability
                            >= self.typesafe_judge.support_threshold
                        )
                        if claim.accepted:
                            accepted_claims.append(claim)
                            supports.append(verification.support_probability)

                    if cluster_summary.claims:
                        if accepted_claims:
                            cluster_summary.summary = " ".join(
                                claim.text for claim in accepted_claims
                            )
                        else:
                            cluster_summary.summary = (
                                self.summarizer.extractive_summary(
                                    clusters[cluster_id], max_sentences=4
                                )
                            )
                            cluster_summary.claims = []

                    if supports:
                        evidence_confidence = sum(supports) / len(supports)
                        topic_confidence = (
                            judgment.topic_confidence
                            if judgment is not None
                            else evidence_confidence
                        )
                        cluster_summary.confidence = min(
                            evidence_confidence, topic_confidence
                        )

                # BART/extractive fallbacks do not produce structured claims.
                legacy_summaries = {
                    cluster_id: cluster_summary.summary
                    for cluster_id, cluster_summary in zip(
                        clusters.keys(), cluster_summaries
                    )
                    if not cluster_summary.claims
                }
                if legacy_summaries:
                    with getattr(self, "_api_gate", nullcontext()):
                        support = self.typesafe_judge.verify_summaries(
                            clusters, legacy_summaries
                        )
                    for cluster_id, cluster_summary in zip(
                        clusters.keys(), cluster_summaries
                    ):
                        probability = support.get(cluster_id)
                        if probability is not None:
                            judgment = judgments.get(cluster_id)
                            if probability < self.typesafe_judge.support_threshold:
                                cluster_summary.summary = (
                                    self.summarizer.extractive_summary(
                                        clusters[cluster_id], max_sentences=4
                                    )
                                )
                            topic_confidence = (
                                judgment.topic_confidence
                                if judgment is not None
                                else probability
                            )
                            cluster_summary.confidence = min(
                                probability, topic_confidence
                            )

            course_evidence: List[Dict[str, Any]] = []
            for cluster_summary in cluster_summaries:
                accepted_claims = [
                    claim for claim in cluster_summary.claims if claim.accepted
                ]
                if accepted_claims:
                    evidence_share = cluster_summary.review_count / len(accepted_claims)
                    for claim in accepted_claims:
                        course_evidence.append(
                            {
                                "topic": claim.topic,
                                "text": claim.text,
                                "sentiment": claim.sentiment
                                or cluster_summary.sentiment,
                                "importance": (
                                    claim.importance
                                    if claim.importance is not None
                                    else cluster_summary.importance
                                ),
                                "confidence": cluster_summary.confidence,
                                # A cluster's evidence mass is shared across its
                                # claims so verbose output is not counted repeatedly.
                                "review_count": evidence_share,
                            }
                        )
                else:
                    course_evidence.append(
                        {
                            "topic": cluster_summary.cluster_type,
                            "text": cluster_summary.summary,
                            "sentiment": cluster_summary.sentiment,
                            "importance": cluster_summary.importance,
                            "confidence": cluster_summary.confidence,
                            "review_count": cluster_summary.review_count,
                        }
                    )
            self._course_evidence[course_code] = course_evidence

            # Use actual review count for this course (not just clustered reviews)
            actual_review_count = review_counts_by_course.get(course_code, 0)

            # Build structured course summary
            course_summary = CourseSummary(
                course=course_code,
                total_reviews=actual_review_count,
            )

            # Consolidate across clusters before composing each topic. Appending
            # every cluster claim repeats paraphrases of the same finding.
            for topic in CLUSTER_TYPES:
                candidates = [
                    item for item in course_evidence if item["topic"] == topic
                ]
                if candidates:
                    setattr(
                        course_summary, topic, self._compose_course_topic(candidates)
                    )

            # Calculate overall confidence
            if cluster_summaries:
                weights = [
                    summary.review_count * (1.0 + summary.importance / 4.0)
                    for summary in cluster_summaries
                ]
                course_summary.confidence = sum(
                    summary.confidence * weight
                    for summary, weight in zip(cluster_summaries, weights)
                ) / sum(weights)
            else:
                course_summary.confidence = 0.0

            # Calculate course stats
            all_cluster_reviews = []
            for reviews in clusters.values():
                all_cluster_reviews.extend(reviews)

            avg_rating = None
            avg_difficulty = None
            common_tags = []
            tag_frequencies = {}

            if all_cluster_reviews:
                ratings = [
                    r.original_rating
                    for r in all_cluster_reviews
                    if r.original_rating is not None
                ]
                if ratings:
                    avg_rating = sum(ratings) / len(ratings)

                difficulties = [
                    r.original_difficulty
                    for r in all_cluster_reviews
                    if r.original_difficulty is not None
                ]
                if difficulties:
                    avg_difficulty = sum(difficulties) / len(difficulties)

                all_tags = []
                for r in all_cluster_reviews:
                    if r.tags:
                        all_tags.extend(r.tags)

                if all_tags:
                    tag_counts = Counter(all_tags)
                    tag_frequencies = dict(tag_counts)
                    common_tags = [tag for tag, _ in tag_counts.most_common(5)]

            course_summaries.append(
                CourseSummary(
                    course=course_code,
                    teaching=course_summary.teaching,
                    exams=course_summary.exams,
                    grading=course_summary.grading,
                    workload=course_summary.workload,
                    personality=course_summary.personality,
                    policies=course_summary.policies,
                    other=course_summary.other,
                    confidence=course_summary.confidence,
                    total_reviews=course_summary.total_reviews,
                    avg_rating=avg_rating,
                    avg_difficulty=avg_difficulty,
                    common_tags=common_tags,
                    tag_frequencies=tag_frequencies,
                )
            )

        return course_summaries

    # ------------------------------------------------------------------
    # Structured path: one TAMU request per professor
    # ------------------------------------------------------------------

    def _prepare_structured(
        self, raw_reviews: List[dict], professor_id: str, professor_dept
    ) -> PreparedProfessor:
        """Local work only: normalize, deduplicate, group, and render requests."""
        codes_by_review: Dict[str, List[str]] = {}
        for raw in raw_reviews:
            codes_by_review[raw.get("id", "")] = (
                self.course_normalizer.normalize_course_codes(
                    raw.get("course_code"),
                    professor_id=professor_id,
                    professor_dept=professor_dept,
                )
            )
        processed = self.preprocessor.process_reviews(raw_reviews, deduplicate=True)
        raw_by_id = {raw.get("id", ""): raw for raw in raw_reviews}
        request_reviews = []
        for review in processed:
            raw = raw_by_id.get(review.review_id, {})
            request_reviews.append(
                RequestReview(
                    local_id="",
                    review_id=review.review_id,
                    text=review.text,
                    courses=codes_by_review.get(review.review_id) or ["UNKNOWN"],
                    date=review_date(raw),
                    quality=review_quality(raw),
                    grade=normalize_grade(raw.get("grade")),
                )
            )
        client = self.summarizer.tamu_client
        parts = build_request_parts(
            professor_id,
            request_reviews,
            max_input_chars=client.max_input_chars,
            clip_chars=_env_int("SUMMARY_REVIEW_CLIP_CHARS", 600),
            max_courses_per_request=_env_int("SUMMARY_MAX_COURSES_PER_REQUEST", 8),
        )
        review_counts: Dict[str, int] = {}
        for codes in codes_by_review.values():
            for code in codes:
                review_counts[code] = review_counts.get(code, 0) + 1
        return PreparedProfessor(
            professor_id=professor_id,
            professor_reviews=processed,
            total_reviews=len(raw_reviews),
            units={f"part{index}": part for index, part in enumerate(parts)},
            review_counts_by_course=review_counts,
            structured=StructuredContext(
                raw_reviews=raw_reviews,
                codes_by_review=codes_by_review,
                review_texts={review.review_id: review.text for review in processed},
                dropped_reviews=sum(part.dropped_reviews for part in parts),
            ),
        )

    def _request_part(self, part: ProfessorRequestPart) -> PartOutcome:
        """Remote work for one request part: TAMU, then optional verification."""
        client = self.summarizer.tamu_client
        outcome = PartOutcome()
        with getattr(self, "_api_gate", nullcontext()):
            client.take_thread_requests()
            try:
                outcome.results = self._generate_part(client, part)
            finally:
                outcome.requests = client.take_thread_requests()
                client.close_thread_session()
            if self.typesafe_judge is not None:
                self._verify_part(part, outcome)
        return outcome

    def _generate_part(self, client, part: ProfessorRequestPart) -> List[PartResult]:
        base_tokens = client.max_output_tokens
        max_tokens = min(8_000, base_tokens + 500 * (len(part.groups) - 1))
        for attempt in range(2):
            try:
                output = client.generate_professor_summary(
                    part.render(), max_tokens=max_tokens
                )
                return [map_part_response(part, output)]
            except TamuAIQuotaExceeded:
                raise
            except TamuAITruncated as exc:
                if len(part.groups) > 1:
                    # Output too long: split by course group, never by cluster.
                    logger.warning(
                        "TAMU output for %s was truncated; splitting %d course groups",
                        part.professor_id,
                        len(part.groups),
                    )
                    return [
                        result
                        for half in part.split()
                        for result in self._generate_part(client, half)
                    ]
                if attempt == 0 and max_tokens < 8_000:
                    max_tokens = min(8_000, max_tokens * 2)
                    continue
                raise TamuAIError(
                    f"TAMU summary for {part.professor_id} failed: {exc}"
                ) from exc
            except TamuAIInvalidJSON as exc:
                # The client already spent its one strict retry; the professor
                # fails and is retried next run instead of saving a fallback.
                raise TamuAIError(
                    f"TAMU summary for {part.professor_id} failed: {exc}"
                ) from exc
        raise AssertionError("unreachable")

    def _verify_part(self, part: ProfessorRequestPart, outcome: PartOutcome) -> None:
        """Reject a claim only when TypeSafe reports low evidence support."""
        claims: List[SummaryClaim] = []
        seen = set()
        for result in outcome.results:
            candidates = [
                *(
                    c
                    for fields in result.courses.values()
                    for items in fields.values()
                    for c in items
                ),
                *result.strengths,
                *result.watch_outs,
                *([result.tldr] if result.tldr else []),
            ]
            for claim in candidates:
                if id(claim) not in seen:
                    seen.add(id(claim))
                    claims.append(claim)
        texts = {review.review_id: review.text for review in part.reviews}
        support = self.typesafe_judge.verify_claim_support(claims, texts)
        threshold = self.typesafe_judge.support_threshold
        for index, claim in enumerate(claims):
            probability = support.get(index)
            if probability is None:
                # Verification is an optional check: an unanswered claim keeps
                # the local citation check it already passed.
                outcome.unverified += 1
                continue
            claim.support_probability = probability
            if probability < threshold:
                claim.accepted = False
                outcome.support_rejected += 1

    @staticmethod
    def _join(claims: List[SummaryClaim]) -> Optional[str]:
        text = " ".join(self_contained(claim.text) for claim in claims)
        return text or None

    @staticmethod
    def _distinct(claims: List[SummaryClaim], also_avoid=()) -> List[SummaryClaim]:
        kept: List[SummaryClaim] = []
        for claim in claims:
            if not claim.accepted:
                continue
            if near_duplicate(claim.text, [c.text for c in kept]):
                continue
            if also_avoid and near_duplicate(claim.text, also_avoid, threshold=0.8):
                continue
            kept.append(claim)
        return kept

    @staticmethod
    def _review_confidence(review_count: int, supports: List[float]) -> float:
        coverage = min(0.95, 0.5 + 0.45 * min(1.0, review_count / 20))
        if supports:
            coverage *= sum(supports) / len(supports)
        return round(coverage, 3)

    @staticmethod
    def _rating_stats(raw_reviews: List[dict], tag_limit: int):
        ratings = [q for q in (review_quality(r) for r in raw_reviews) if q is not None]
        difficulties = [
            float(r["difficulty_rating"])
            for r in raw_reviews
            if r.get("difficulty_rating") is not None
        ]
        tags = Counter(tag for r in raw_reviews for tag in (r.get("rating_tags") or []))
        return (
            sum(ratings) / len(ratings) if ratings else None,
            sum(difficulties) / len(difficulties) if difficulties else None,
            [tag for tag, _ in tags.most_common(tag_limit)],
            dict(tags),
            ratings,
        )

    def _finish_structured(
        self, prepared: PreparedProfessor, unit_results: Dict[str, PartOutcome]
    ) -> ProfessorSummary:
        context = prepared.structured
        outcomes = [unit_results[key] for key in prepared.units if key in unit_results]
        results = [result for outcome in outcomes for result in outcome.results]

        # Merge course claims across parts (only OTHER-group routing overlaps).
        course_claims: Dict[str, Dict[str, List[SummaryClaim]]] = {}
        for result in results:
            for code, fields in result.courses.items():
                for name, items in fields.items():
                    course_claims.setdefault(code, {}).setdefault(name, []).extend(
                        items
                    )

        raw_by_course: Dict[str, List[dict]] = {}
        for raw in context.raw_reviews:
            for code in context.codes_by_review.get(raw.get("id", ""), []):
                raw_by_course.setdefault(code, []).append(raw)

        course_summaries: List[CourseSummary] = []
        ratings_by_course: Dict[str, List[float]] = {}
        all_course_texts: List[str] = []
        field_sentiments: Dict[str, set] = {}
        for code, raws in sorted(
            raw_by_course.items(), key=lambda kv: (-len(kv[1]), kv[0])
        ):
            fields = course_claims.get(code, {})
            columns: Dict[str, Optional[str]] = {}
            citations: Dict[str, List[List[str]]] = {}
            supports: List[float] = []
            for name in (*COURSE_FIELDS, "tips"):
                kept = self._distinct(fields.get(name, []))
                if kept:
                    citations[name] = [c.evidence_review_ids for c in kept]
                    all_course_texts.extend(c.text for c in kept)
                    supports.extend(
                        c.support_probability
                        for c in kept
                        if c.support_probability is not None
                    )
                    if name != "tips":
                        field_sentiments.setdefault(name, set()).update(
                            c.sentiment for c in kept if c.sentiment
                        )
                if name == "tips":
                    tips = [self_contained(c.text) for c in kept]
                else:
                    columns[COURSE_COLUMNS[name]] = self._join(kept)
            avg_rating, avg_difficulty, common_tags, tag_frequencies, ratings = (
                self._rating_stats(raws, 5)
            )
            ratings_by_course[code] = ratings
            course_summaries.append(
                CourseSummary(
                    course=code,
                    **columns,
                    # Shown under "Other" until the frontend renders tips.
                    other=" ".join(tips) or None,
                    tips=tips,
                    confidence=self._review_confidence(len(raws), supports),
                    total_reviews=len(raws),
                    avg_rating=avg_rating,
                    avg_difficulty=avg_difficulty,
                    common_tags=common_tags,
                    tag_frequencies=tag_frequencies,
                    review_stats=compute_review_stats(raws),
                    citations=citations,
                )
            )

        def overall(name: str) -> List[SummaryClaim]:
            merged = [claim for result in results for claim in getattr(result, name)]
            return self._distinct(merged, also_avoid=all_course_texts)[:5]

        strengths = overall("strengths")
        watch_outs = overall("watch_outs")
        tldr_source = max(
            (r for r in results if r.tldr is not None and r.tldr.accepted),
            key=lambda r: r.review_count,
            default=None,
        )
        tldr = tldr_source.tldr if tldr_source else None

        avg_rating, avg_difficulty, common_tags, tag_frequencies, ratings = (
            self._rating_stats(context.raw_reviews, 10)
        )
        overall_sentiment = headline_sentiment(ratings, ratings_by_course) or (
            "No ratings available"
        )
        real_courses = [c for c in course_summaries if c.total_reviews >= 3]
        if len(real_courses) <= 1:
            consistency = "Single course data available"
        elif len(real_courses) <= 3:
            consistency = "Limited course data - patterns emerging"
        elif any(
            {"positive", "negative"} <= values for values in field_sentiments.values()
        ):
            consistency = "Patterns vary across courses"
        else:
            consistency = "Patterns consistent across multiple courses"

        supports = [
            c.support_probability
            for c in [*strengths, *watch_outs, *([tldr] if tldr else [])]
            if c.support_probability is not None
        ]
        generated = sum(r.generated_claims for r in results)
        citation_rejected = sum(r.rejected_citations for r in results)
        support_rejected = sum(o.support_rejected for o in outcomes)
        stats = {
            "requests": sum(o.requests for o in outcomes),
            "request_parts": len(prepared.units),
            "reviews_sent": sum(r.review_count for r in results),
            "reviews_dropped_for_budget": context.dropped_reviews,
            "claims_generated": generated,
            "claims_rejected_citation": citation_rejected,
            "claims_rejected_support": support_rejected,
            "claims_unverified": sum(o.unverified for o in outcomes),
            "claims_accepted": generated - citation_rejected - support_rejected,
        }
        logger.info(
            "Professor %s: %d TAMU request(s), %d claims accepted, %d rejected "
            "(%d citation, %d support), %d unverified",
            prepared.professor_id,
            stats["requests"],
            stats["claims_accepted"],
            citation_rejected + support_rejected,
            citation_rejected,
            support_rejected,
            stats["claims_unverified"],
        )

        return ProfessorSummary(
            professor_id=prepared.professor_id,
            overall_sentiment=overall_sentiment,
            # The frontend reads "Topic: text" prefixes.
            strengths=[
                f"{c.topic.capitalize()}: {self_contained(c.text)}" for c in strengths
            ],
            complaints=[
                f"{c.topic.capitalize()}: {self_contained(c.text)}" for c in watch_outs
            ],
            consistency=consistency,
            confidence=self._review_confidence(stats["reviews_sent"], supports),
            course_summaries=course_summaries,
            total_reviews=prepared.total_reviews,
            avg_rating=avg_rating,
            avg_difficulty=avg_difficulty,
            common_tags=common_tags,
            tag_frequencies=tag_frequencies,
            tldr=self_contained(tldr.text) if tldr else None,
            review_stats=compute_review_stats(context.raw_reviews),
            citations={
                "tldr": tldr.evidence_review_ids if tldr else [],
                "strengths": [c.evidence_review_ids for c in strengths],
                "watch_outs": [c.evidence_review_ids for c in watch_outs],
            },
            generation_stats=stats,
        )

    @staticmethod
    def _review_for_course(
        review: ProcessedReview, course_code: str
    ) -> ProcessedReview:
        """Copy a review to a normalized course without dropping its metadata."""
        return replace(review, course_code=course_code)

    def _compose_course_topic(self, candidates: List[Dict[str, Any]]) -> str:
        """Select up to three distinct source claims, preserving disagreement.

        The 90-word target is soft: a complete claim or a contrasting viewpoint
        may exceed it. Never cut a sentence or rewrite its qualifications.
        """
        ranked = sorted(
            candidates,
            key=lambda item: (
                max(1.0, float(item["importance"]))
                * max(0.1, float(item["confidence"]))
                * max(1.0, float(item["review_count"]))
            ),
            reverse=True,
        )
        unique = []
        seen = set()
        for item in ranked:
            normalized = " ".join(item["text"].casefold().split())
            if normalized and normalized not in seen:
                unique.append(item)
                seen.add(normalized)
        if not unique:
            return ""

        vectors = None
        if len(unique) > 1:
            try:
                vectors = self.embedding_generator.model.encode(
                    [item["text"] for item in unique],
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                    show_progress_bar=False,
                )
            except Exception as exc:
                logger.warning("Course summary deduplication unavailable: %s", exc)

        # Reserve space for a balanced claim, or both explicit polarities. A
        # minority experience must not vanish merely because it ranks lower.
        required = []
        mixed = next(
            (i for i, item in enumerate(unique) if item["sentiment"] == "mixed"), None
        )
        if mixed is not None:
            required.append(mixed)
        else:
            for sentiment in ("positive", "negative"):
                index = next(
                    (
                        i
                        for i, item in enumerate(unique)
                        if item["sentiment"] == sentiment
                    ),
                    None,
                )
                if index is not None:
                    required.append(index)

        selected = []
        words = 0
        for index in required + [i for i in range(len(unique)) if i not in required]:
            item = unique[index]
            if vectors is not None and any(
                # Similar embeddings can describe opposite experiences; only
                # suppress paraphrases with matching sentiment.
                item["sentiment"] == unique[previous]["sentiment"]
                and float(np.dot(vectors[index], vectors[previous]))
                >= SUMMARY_SIMILARITY_THRESHOLD
                for previous in selected
            ):
                continue
            count = len(item["text"].split())
            if selected and index not in required and words + count > 90:
                continue
            selected.append(index)
            words += count
            if len(selected) == 3:
                break
        return " ".join(
            self_contained(unique[index]["text"]) for index in sorted(selected)
        )

    def _generate_professor_summary(
        self,
        professor_id: str,
        course_summaries: List[CourseSummary],
        all_reviews: List[ProcessedReview],
        total_review_count: int,
    ) -> ProfessorSummary:
        """Generate overall professor summary from course summaries"""

        # Rank reusable typed evidence rather than searching generated prose for
        # a small set of positive and negative keywords.
        ranked_evidence = []
        for course_code, evidence_items in self._course_evidence.items():
            for item in evidence_items:
                weight = (
                    max(1.0, float(item["importance"]))
                    * max(0.1, float(item["confidence"]))
                    * max(1.0, float(item["review_count"]))
                )
                ranked_evidence.append((weight, course_code, item))

        positive_score = sum(
            weight
            for weight, _, item in ranked_evidence
            if item["sentiment"] == "positive"
        )
        negative_score = sum(
            weight
            for weight, _, item in ranked_evidence
            if item["sentiment"] == "negative"
        )

        def ranked_items(sentiment: str) -> List[str]:
            candidates = [
                item
                for _, _, item in sorted(
                    ranked_evidence, key=lambda evidence: evidence[0], reverse=True
                )
                if item["sentiment"] == sentiment
            ]
            if not candidates:
                return []

            embeddings = None
            embedding_generator = getattr(self, "embedding_generator", None)
            if embedding_generator is not None:
                try:
                    embeddings = embedding_generator.model.encode(
                        [item["text"] for item in candidates],
                        convert_to_numpy=True,
                        normalize_embeddings=True,
                        show_progress_bar=False,
                    )
                except Exception as exc:
                    logger.warning("Semantic summary deduplication failed: %s", exc)

            values: List[str] = []
            seen = set()
            selected_indices: List[int] = []
            topic_counts: Dict[str, int] = {}
            for index, item in enumerate(candidates):
                value = f"{item['topic'].capitalize()}: {item['text']}"
                normalized = value.casefold().strip()
                topic = str(item["topic"])
                if normalized in seen:
                    continue
                if topic_counts.get(topic, 0) >= SUMMARY_MAX_ITEMS_PER_TOPIC:
                    continue
                if embeddings is not None and selected_indices:
                    max_similarity = max(
                        float(np.dot(embeddings[index], embeddings[selected_index]))
                        for selected_index in selected_indices
                    )
                    if max_similarity >= SUMMARY_SIMILARITY_THRESHOLD:
                        continue

                values.append(value)
                seen.add(normalized)
                selected_indices.append(index)
                topic_counts[topic] = topic_counts.get(topic, 0) + 1
                if len(values) == 5:
                    break
            return values

        strengths = ranked_items("positive")
        complaints = ranked_items("negative")

        ratings = [
            r.original_rating for r in all_reviews if r.original_rating is not None
        ]
        ratings_by_course: Dict[str, List[float]] = {}
        for review in all_reviews:
            if review.original_rating is not None:
                ratings_by_course.setdefault(review.course_code or "", []).append(
                    review.original_rating
                )
        overall_sentiment = headline_sentiment(ratings, ratings_by_course)
        if overall_sentiment is None:
            # Synthetic and legacy inputs may not include ratings.
            if positive_score > negative_score * 1.35:
                overall_sentiment = "Generally positive"
            elif negative_score > positive_score * 1.35:
                overall_sentiment = "Generally negative"
            else:
                overall_sentiment = "Mixed reviews"

        # Assess consistency
        if len(course_summaries) == 1:
            consistency = "Single course data available"
        elif len(course_summaries) <= 3:
            consistency = "Limited course data - patterns emerging"
        else:
            topic_sentiments: Dict[str, set[str]] = {}
            for _, _, item in ranked_evidence:
                topic_sentiments.setdefault(item["topic"], set()).add(item["sentiment"])
            conflicting = any(
                "positive" in sentiments and "negative" in sentiments
                for sentiments in topic_sentiments.values()
            )
            consistency = (
                "Patterns vary across courses"
                if conflicting
                else "Patterns consistent across multiple courses"
            )

        # Calculate overall confidence
        if course_summaries:
            confidence = sum(cs.confidence for cs in course_summaries) / len(
                course_summaries
            )
        else:
            confidence = 0.0

        # Calculate global stats
        avg_rating = None
        avg_difficulty = None
        common_tags = []
        tag_frequencies = {}

        if all_reviews:
            if ratings:
                avg_rating = sum(ratings) / len(ratings)

            difficulties = [
                r.original_difficulty
                for r in all_reviews
                if r.original_difficulty is not None
            ]
            if difficulties:
                avg_difficulty = sum(difficulties) / len(difficulties)

            all_tags = []
            for r in all_reviews:
                if r.tags:
                    all_tags.extend(r.tags)

            if all_tags:
                tag_counts = Counter(all_tags)
                tag_frequencies = dict(tag_counts)
                common_tags = [tag for tag, _ in tag_counts.most_common(10)]

        return ProfessorSummary(
            professor_id=professor_id,
            overall_sentiment=overall_sentiment,
            strengths=strengths[:5],  # Top 5
            complaints=complaints[:5],  # Top 5
            consistency=consistency,
            confidence=confidence,
            course_summaries=course_summaries,
            total_reviews=total_review_count,
            avg_rating=avg_rating,
            avg_difficulty=avg_difficulty,
            common_tags=common_tags,
            tag_frequencies=tag_frequencies,
        )

    def process_single_course(
        self, raw_reviews: List[dict], course_code: str
    ) -> CourseSummary:
        """
        Process reviews for a single course.

        Args:
            raw_reviews: List of raw review dicts for the course
            course_code: Course code

        Returns:
            CourseSummary object
        """
        logger.debug("Processing course %s", course_code)

        # Preprocess
        processed_reviews = self.preprocessor.process_reviews(
            raw_reviews, deduplicate=True
        )

        if not processed_reviews:
            return CourseSummary(course=course_code, confidence=0.0, total_reviews=0)

        # Generate embeddings
        embeddings = self.embedding_generator.generate_embeddings_for_reviews(
            processed_reviews
        )

        # Cluster
        clusters = self.clusterer.cluster_reviews(processed_reviews, embeddings)
        summaries = self._generate_course_summaries(
            {course_code: clusters}, {course_code: len(processed_reviews)}
        )
        return summaries[0]
