"""
Main pipeline orchestrator for hierarchical summarization.
"""

from typing import List, Dict, Any
from collections import Counter

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
from pipelines.professors.schemas import (
    ProcessedReview,
    CourseSummary,
    ProfessorSummary,
)


class HierarchicalSummarizationPipeline:
    """Orchestrates the full hierarchical summarization pipeline"""

    def __init__(self, session: Any = None) -> None:
        """
        Initialize the pipeline.

        Args:
            session: Optional database session for loading cross-listings
        """
        self.preprocessor = ReviewPreprocessor()
        self.embedding_generator = EmbeddingGenerator()
        self.clusterer = ReviewClusterer()
        self.summarizer = HierarchicalSummarizer()
        self.course_normalizer = CourseNormalizer(session=session)

    def process_professor_reviews(
        self, raw_reviews: List[dict], professor_id: str
    ) -> ProfessorSummary:
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

        # Step 1: Preprocess
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

        # Step 1.5: Normalize course codes (duplicate reviews with multiple course numbers)
        expanded_reviews = []

        for review in processed_reviews:
            original_code = review.course_code
            normalized_codes = self.course_normalizer.normalize_course_codes(
                original_code, professor_id=professor_id, professor_dept=professor_dept
            )

            for normalized_code in normalized_codes:
                expanded_review = ProcessedReview(
                    review_id=review.review_id,
                    professor_id=review.professor_id,
                    course_code=normalized_code,
                    text=review.text,
                    original_text=review.original_text,
                    word_count=review.word_count,
                )
                expanded_reviews.append(expanded_review)

        processed_reviews = expanded_reviews

        # Step 2: Generate embeddings
        embeddings = self.embedding_generator.generate_embeddings_for_reviews(
            processed_reviews, use_cache=True
        )

        # Step 3: Cluster by course
        course_clusters = self.clusterer.cluster_by_course(
            processed_reviews, embeddings
        )

        # Step 4: Generate course summaries
        course_summaries = self._generate_course_summaries(course_clusters)

        # Step 5: Generate professor summary
        professor_summary = self._generate_professor_summary(
            professor_id,
            course_summaries,
            processed_reviews,
            total_review_count=len(raw_reviews),
        )

        return professor_summary

    def _generate_course_summaries(
        self, course_clusters: Dict[str, Dict[int, List[ProcessedReview]]]
    ) -> List[CourseSummary]:
        """Generate structured summaries for each course"""
        course_summaries = []

        for course_code, clusters in course_clusters.items():
            # Identify cluster types
            cluster_types = {}
            for cluster_id, cluster_reviews in clusters.items():
                cluster_type = self.clusterer.identify_cluster_type(cluster_reviews)
                cluster_types[cluster_id] = cluster_type

            # Summarize clusters
            cluster_summaries = self.summarizer.summarize_clusters(
                clusters, cluster_types
            )

            # Build structured course summary
            course_summary = CourseSummary(
                course=course_code,
                total_reviews=sum(len(reviews) for reviews in clusters.values()),
            )

            # Organize summaries by type
            for cluster_summary in cluster_summaries:
                if cluster_summary.cluster_type == "teaching":
                    course_summary.teaching = cluster_summary.summary
                elif cluster_summary.cluster_type == "exams":
                    course_summary.exams = cluster_summary.summary
                elif cluster_summary.cluster_type == "grading":
                    course_summary.grading = cluster_summary.summary
                elif cluster_summary.cluster_type == "workload":
                    course_summary.workload = cluster_summary.summary
                elif cluster_summary.cluster_type == "personality":
                    course_summary.personality = cluster_summary.summary
                elif cluster_summary.cluster_type == "policies":
                    course_summary.policies = cluster_summary.summary
                else:
                    course_summary.other = cluster_summary.summary

            # Calculate overall confidence
            if cluster_summaries:
                course_summary.confidence = sum(
                    cs.confidence for cs in cluster_summaries
                ) / len(cluster_summaries)
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

    def _generate_professor_summary(
        self,
        professor_id: str,
        course_summaries: List[CourseSummary],
        all_reviews: List[ProcessedReview],
        total_review_count: int,
    ) -> ProfessorSummary:
        """Generate overall professor summary from course summaries"""

        # Extract strengths and complaints from course summaries
        strengths = []
        complaints = []

        for course_summary in course_summaries:
            # Analyze each field for sentiment
            for field_name in [
                "teaching",
                "exams",
                "grading",
                "workload",
                "personality",
            ]:
                field_value = getattr(course_summary, field_name)
                if not field_value:
                    continue

                text_lower = field_value.lower()
                positive_indicators = [
                    "good",
                    "great",
                    "excellent",
                    "clear",
                    "fair",
                    "helpful",
                ]
                negative_indicators = [
                    "bad",
                    "terrible",
                    "confusing",
                    "unfair",
                    "difficult",
                    "heavy",
                ]

                if any(indicator in text_lower for indicator in positive_indicators):
                    strengths.append(f"{field_name.capitalize()}: {field_value[:100]}")
                if any(indicator in text_lower for indicator in negative_indicators):
                    complaints.append(f"{field_name.capitalize()}: {field_value[:100]}")

        # Determine overall sentiment
        positive_count = len(strengths)
        negative_count = len(complaints)

        if positive_count > negative_count * 1.5:
            overall_sentiment = "Generally positive"
        elif negative_count > positive_count * 1.5:
            overall_sentiment = "Generally negative"
        else:
            overall_sentiment = "Mixed - varies by course"

        # Assess consistency
        if len(course_summaries) == 1:
            consistency = "Single course data available"
        elif len(course_summaries) <= 3:
            consistency = "Limited course data - patterns emerging"
        else:
            # Check if patterns are consistent across courses
            consistency = "Patterns consistent across multiple courses"

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
            ratings = [
                r.original_rating for r in all_reviews if r.original_rating is not None
            ]
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
        print(f"\nProcessing course: {course_code}")

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

        # Identify cluster types
        cluster_types = {}
        for cluster_id, cluster_reviews in clusters.items():
            cluster_type = self.clusterer.identify_cluster_type(cluster_reviews)
            cluster_types[cluster_id] = cluster_type

        # Summarize clusters
        cluster_summaries = self.summarizer.summarize_clusters(clusters, cluster_types)

        # Build course summary
        course_summary = CourseSummary(
            course=course_code, total_reviews=len(processed_reviews)
        )

        for cluster_summary in cluster_summaries:
            if cluster_summary.cluster_type == "teaching":
                course_summary.teaching = cluster_summary.summary
            elif cluster_summary.cluster_type == "exams":
                course_summary.exams = cluster_summary.summary
            elif cluster_summary.cluster_type == "grading":
                course_summary.grading = cluster_summary.summary
            elif cluster_summary.cluster_type == "workload":
                course_summary.workload = cluster_summary.summary
            elif cluster_summary.cluster_type == "personality":
                course_summary.personality = cluster_summary.summary
            elif cluster_summary.cluster_type == "policies":
                course_summary.policies = cluster_summary.summary
            else:
                course_summary.other = cluster_summary.summary

        if cluster_summaries:
            course_summary.confidence = sum(
                cs.confidence for cs in cluster_summaries
            ) / len(cluster_summaries)

        return course_summary
