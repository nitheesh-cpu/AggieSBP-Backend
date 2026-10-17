import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pipelines.professors.hierarchical_summarization import ai_clients
from pipelines.professors.hierarchical_summarization.ai_clients import (
    TypeSafeReviewJudge,
    ClaimVerification,
)
from pipelines.professors.schemas import SummaryClaim


def review(index):
    return SimpleNamespace(
        review_id=str(index), text="review text " * 70, original_rating=None
    )


class TypeSafeBatchingTests(unittest.TestCase):
    def test_large_classification_keeps_all_reviews_and_disagreement(self):
        judge = TypeSafeReviewJudge()
        judge.max_state_bytes = 2100
        seen = []

        def evaluate(state, questions):
            reviews = state["clusters"]["1"]["reviews"]
            seen.extend(r["review_id"] for r in reviews)
            sentiment = (
                "negative"
                if any(r["review_id"] == "5" for r in reviews)
                else "positive"
            )
            return SimpleNamespace(
                choices={
                    "topic_1": SimpleNamespace(choice="teaching", confidence=0.9),
                    "sentiment_1": SimpleNamespace(choice=sentiment, confidence=0.9),
                },
                nouls={},
                scores={},
            )

        with patch.object(judge, "_evaluate", side_effect=evaluate):
            result = judge.classify_clusters({1: [review(i) for i in range(6)]})
        self.assertEqual(set(seen), {str(i) for i in range(6)})
        self.assertEqual(result[1].sentiment, "mixed")
        self.assertEqual(result[1].disagreement_probability, 1)

    def test_token_limit_splits_questions_and_merges_answers(self):
        judge = TypeSafeReviewJudge()
        client = Mock()

        def evaluate(state, questions):
            self.assertEqual(set(state["clusters"]), {"1"})
            if len(questions) > 1:
                raise RuntimeError('400 {"error_type":"max_tokens_exceeded"}')
            return SimpleNamespace(
                nouls={key: SimpleNamespace(noul=0.9) for key in questions}
            )

        client.system_one.side_effect = evaluate
        factory = Mock()
        factory.return_value.__enter__ = Mock(return_value=client)
        factory.return_value.__exit__ = Mock(return_value=False)
        with patch.object(ai_clients, "TypeSafeClient", factory):
            result = judge._evaluate(
                {"clusters": {"1": {"reviews": []}, "2": {"reviews": ["unrelated"]}}},
                {
                    "a_1": SimpleNamespace(instructions="A"),
                    "b_1": SimpleNamespace(instructions="B"),
                },
            )
        self.assertEqual(set(result.nouls), {"a_1", "b_1"})
        self.assertEqual(client.system_one.call_count, 3)

    def test_large_verification_retains_citations_and_checks_every_review(self):
        judge = TypeSafeReviewJudge()
        judge.max_state_bytes = 5000
        seen = []

        def verify(clusters, claims, **kwargs):
            ids = [r.review_id for r in clusters[1]]
            self.assertIn("0", ids)
            self.assertTrue(kwargs["_bounded"])
            seen.extend(ids)
            risk = 0.9 if "7" in ids else 0.1
            return {
                (1, 0): ClaimVerification(
                    0.95, 0.1, risk, "teaching", 0.9, "positive", 0.9, 3, 0.9
                )
            }

        with patch.object(judge, "verify_claims", side_effect=verify):
            result = judge._verify_large_claims(
                {1: [review(i) for i in range(8)]},
                {1: [SummaryClaim("Clear lectures.", ["0"], "teaching")]},
            )
        self.assertEqual(set(seen), {str(i) for i in range(8)})
        self.assertEqual(result[(1, 0)].disagreement_erasure_probability, 0.9)

    def test_missing_verification_shard_does_not_accept_claim(self):
        judge = TypeSafeReviewJudge()
        judge.max_state_bytes = 5000
        with patch.object(judge, "verify_claims", return_value={}):
            result = judge._verify_large_claims(
                {1: [review(i) for i in range(8)]},
                {1: [SummaryClaim("Clear lectures.", ["0"], "teaching")]},
            )
        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
