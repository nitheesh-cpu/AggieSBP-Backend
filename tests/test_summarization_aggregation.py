import unittest
import threading
from unittest.mock import patch, Mock
from types import SimpleNamespace

import numpy as np

from pipelines.professors.hierarchical_summarization.pipeline import (
    HierarchicalSummarizationPipeline,
)
from pipelines.professors.schemas import (
    CourseSummary,
    ProcessedReview,
    SummaryClaim,
    ClusterSummary,
)


class ProfessorAggregationTests(unittest.TestCase):
    def test_missing_verification_uses_source_text_not_unchecked_claim(self):
        pipeline = object.__new__(HierarchicalSummarizationPipeline)
        claim = SummaryClaim(
            text="Unchecked generated assertion.",
            evidence_review_ids=["r"],
            topic="teaching",
        )
        summary = ClusterSummary(
            cluster_type="teaching",
            summary=claim.text,
            review_count=1,
            sentiment="positive",
            confidence=0.6,
            claims=[claim],
        )
        review = SimpleNamespace(
            original_rating=None, original_difficulty=None, tags=[]
        )
        clusters = {0: [review]}
        pipeline._prepared_courses = lambda _: iter(
            [("A", clusters, ({}, {0: "teaching"}, {}, {}, {}, {0: [claim]}, {}))]
        )
        pipeline.summarizer = Mock()
        pipeline.summarizer.summarize_clusters.return_value = [summary]
        pipeline.summarizer.extractive_summary.return_value = "Actual source text."
        pipeline.typesafe_judge = Mock()
        pipeline.typesafe_judge.verify_summaries.return_value = {}
        pipeline._course_evidence = {}
        pipeline._compose_course_topic = lambda candidates: candidates[0]["text"]
        result = pipeline._generate_course_summaries({"A": clusters}, {"A": 1})
        self.assertFalse(claim.accepted)
        self.assertEqual(result[0].teaching, "Actual source text.")
        self.assertEqual(summary.claims, [])

    def test_course_api_preparation_overlaps_and_preserves_order(self):
        pipeline = object.__new__(HierarchicalSummarizationPipeline)
        barrier = threading.Barrier(2)

        def prepare(clusters):
            barrier.wait(timeout=5)
            return clusters

        pipeline._prepare_course = prepare
        courses = {"A": {1: []}, "B": {2: []}}
        with patch.dict("os.environ", {"SUMMARY_API_WORKERS": "2"}):
            results = list(pipeline._prepared_courses(courses))
        self.assertEqual([row[0] for row in results], ["A", "B"])
        self.assertEqual([row[2] for row in results], list(courses.values()))

    def test_course_topic_removes_paraphrases_but_keeps_opposite_experience(self):
        pipeline = object.__new__(HierarchicalSummarizationPipeline)
        pipeline.embedding_generator = SimpleNamespace(
            model=SimpleNamespace(
                encode=lambda texts, **kwargs: np.array(
                    [[1.0, 0.0], [1.0, 0.0], [1.0, 0.0]]
                )
            )
        )
        candidates = [
            dict(
                text="Lectures are confusing.",
                sentiment="negative",
                importance=4,
                confidence=0.9,
                review_count=20,
            ),
            dict(
                text="Students struggle to follow lectures.",
                sentiment="negative",
                importance=4,
                confidence=0.9,
                review_count=10,
            ),
            dict(
                text="Some students find lectures clear.",
                sentiment="positive",
                importance=4,
                confidence=0.9,
                review_count=1,
            ),
        ]
        result = pipeline._compose_course_topic(candidates)
        self.assertEqual(
            result, "Lectures are confusing. Some students find lectures clear."
        )

    def test_course_topic_preserves_whole_claims_and_limits_output(self):
        pipeline = object.__new__(HierarchicalSummarizationPipeline)
        pipeline.embedding_generator = SimpleNamespace(
            model=SimpleNamespace(encode=lambda texts, **kwargs: np.eye(len(texts)))
        )
        candidates = [
            dict(
                text=f"Finding {i}: " + "useful " * 32 + "details.",
                sentiment="neutral",
                importance=4,
                confidence=0.9,
                review_count=10 - i,
            )
            for i in range(5)
        ]
        result = pipeline._compose_course_topic(candidates)
        self.assertEqual(result, candidates[0]["text"] + " " + candidates[1]["text"])

    def test_course_topic_fallback_is_bounded_and_retains_mixed_claim(self):
        pipeline = object.__new__(HierarchicalSummarizationPipeline)
        candidates = [
            dict(
                text=f"Finding {i}.",
                sentiment="neutral",
                importance=4,
                confidence=0.9,
                review_count=10,
            )
            for i in range(6)
        ]
        candidates.append(
            dict(
                text="Students disagree about lecture clarity.",
                sentiment="mixed",
                importance=1,
                confidence=0.8,
                review_count=1,
            )
        )
        result = pipeline._compose_course_topic(candidates)
        self.assertIn("Students disagree about lecture clarity.", result)
        self.assertEqual(result.count("Finding"), 2)

    def test_semantically_deduplicates_ranked_summary_items(self):
        pipeline = object.__new__(HierarchicalSummarizationPipeline)
        pipeline.embedding_generator = SimpleNamespace(
            model=SimpleNamespace(
                encode=lambda texts, **kwargs: np.array(
                    [
                        [1.0, 0.0],
                        [0.99, 0.01],
                        [0.0, 1.0],
                    ]
                )
            )
        )
        pipeline._course_evidence = {
            "TEST 101": [
                {
                    "topic": "personality",
                    "text": "Office hours are helpful.",
                    "sentiment": "positive",
                    "importance": 4.0,
                    "confidence": 0.9,
                    "review_count": 10.0,
                },
                {
                    "topic": "teaching",
                    "text": "Students receive useful help during office hours.",
                    "sentiment": "positive",
                    "importance": 3.8,
                    "confidence": 0.9,
                    "review_count": 9.0,
                },
                {
                    "topic": "exams",
                    "text": "Exam guides match the tested material.",
                    "sentiment": "positive",
                    "importance": 3.0,
                    "confidence": 0.8,
                    "review_count": 5.0,
                },
            ]
        }

        result = pipeline._generate_professor_summary(
            "professor-1",
            [CourseSummary(course="TEST 101", confidence=0.9, total_reviews=10)],
            [],
            total_review_count=10,
        )

        self.assertEqual(len(result.strengths), 2)
        self.assertIn("Office hours are helpful", result.strengths[0])
        self.assertIn("Exam guides", result.strengths[1])

    def test_course_normalization_preserves_review_metadata(self):
        review = ProcessedReview(
            review_id="review-1",
            professor_id="professor-1",
            course_code="CSCE 121/221",
            text="Useful class",
            original_text="Useful class",
            word_count=2,
            original_rating=4.5,
            original_difficulty=3.0,
            tags=["Helpful"],
        )

        expanded = HierarchicalSummarizationPipeline._review_for_course(
            review, "CSCE121"
        )

        self.assertEqual(expanded.course_code, "CSCE121")
        self.assertEqual(expanded.original_rating, 4.5)
        self.assertEqual(expanded.original_difficulty, 3.0)
        self.assertEqual(expanded.tags, ["Helpful"])

    def test_uses_typed_sentiment_and_importance_instead_of_keywords(self):
        pipeline = object.__new__(HierarchicalSummarizationPipeline)
        pipeline._course_evidence = {
            "TEST 101": [
                {
                    "topic": "teaching",
                    "text": "Examples connect abstract ideas to practical problems.",
                    "sentiment": "positive",
                    "importance": 4.0,
                    "confidence": 0.9,
                    "review_count": 12,
                }
            ],
            "TEST 202": [
                {
                    "topic": "grading",
                    "text": "Rubric deductions were not explained.",
                    "sentiment": "negative",
                    "importance": 2.0,
                    "confidence": 0.8,
                    "review_count": 3,
                }
            ],
        }
        courses = [
            CourseSummary(course="TEST 101", confidence=0.9, total_reviews=12),
            CourseSummary(course="TEST 202", confidence=0.8, total_reviews=3),
        ]

        result = pipeline._generate_professor_summary(
            "professor-1", courses, [], total_review_count=15
        )

        self.assertEqual(result.overall_sentiment, "Generally positive")
        self.assertIn("Examples connect", result.strengths[0])
        self.assertIn("Rubric deductions", result.complaints[0])

    def test_does_not_truncate_strength_or_complaint_text(self):
        pipeline = object.__new__(HierarchicalSummarizationPipeline)
        full_text = (
            "The instructor consistently explains difficult material with detailed "
            "examples, patient answers, useful office hours, and practical exercises "
            "that help students understand the entire course sequence."
        )
        pipeline._course_evidence = {
            "TEST 101": [
                {
                    "topic": "teaching",
                    "text": full_text,
                    "sentiment": "positive",
                    "importance": 4.0,
                    "confidence": 0.9,
                    "review_count": 10.0,
                }
            ]
        }

        result = pipeline._generate_professor_summary(
            "professor-1",
            [CourseSummary(course="TEST 101", confidence=0.9, total_reviews=10)],
            [],
            total_review_count=10,
        )

        self.assertTrue(result.strengths[0].endswith(full_text))

    def test_uses_review_ratings_for_the_headline_sentiment(self):
        pipeline = object.__new__(HierarchicalSummarizationPipeline)
        pipeline._course_evidence = {
            "TEST 101": [
                {
                    "topic": "workload",
                    "text": "Several assignments take substantial time.",
                    "sentiment": "negative",
                    "importance": 4.0,
                    "confidence": 0.9,
                    "review_count": 50.0,
                }
            ]
        }
        reviews = [
            ProcessedReview(
                review_id=str(index),
                professor_id="professor-1",
                course_code="TEST 101",
                text="Review",
                original_text="Review",
                word_count=1,
                original_rating=rating,
            )
            for index, rating in enumerate([5.0, 4.0, 3.0, 2.0, 1.0])
        ]

        result = pipeline._generate_professor_summary(
            "professor-1",
            [CourseSummary(course="TEST 101", confidence=0.9, total_reviews=5)],
            reviews,
            total_review_count=5,
        )

        # One course cannot "vary by course".
        self.assertEqual(result.overall_sentiment, "Mixed reviews")
        self.assertEqual(result.avg_rating, 3.0)


if __name__ == "__main__":
    unittest.main()
