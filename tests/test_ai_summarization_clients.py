from concurrent.futures import ThreadPoolExecutor
import requests
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from pipelines.professors.hierarchical_summarization import ai_clients
from pipelines.professors.hierarchical_summarization.ai_clients import (
    TamuAIChatClient,
    TamuAIError,
    TamuAIQuotaExceeded,
    TypeSafeReviewJudge,
)
from pipelines.professors.schemas import SummaryClaim


class FakeResponse:
    def __init__(self, status_code, body, text=""):
        self.status_code = status_code
        self._body = body
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("request failed")

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, response):
        self.responses = response if isinstance(response, list) else [response]
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses[min(len(self.calls) - 1, len(self.responses) - 1)]


def make_review(text, review_id="review-1"):
    return SimpleNamespace(text=text, review_id=review_id)


class TamuAIChatClientTests(unittest.TestCase):
    @patch.object(ai_clients.time, "sleep")
    def test_timeout_recovers(self, sleep):
        client = self.make_client(FakeResponse(200, {}))
        response = FakeResponse(
            200, {"choices": [{"message": {"content": '{"clusters": []}'}}]}
        )
        with patch.object(
            client._injected_session,
            "post",
            side_effect=[requests.Timeout("timeout"), response],
        ):
            self.assertEqual(client._post([]), {"clusters": []})
        self.assertEqual(client.requests_made, 2)

    def make_client(self, responses, **kwargs):
        return TamuAIChatClient(
            api_key="test",
            base_url="https://example.test",
            model="test",
            session=FakeSession(responses),
            **kwargs,
        )

    @patch.object(ai_clients.time, "sleep")
    def test_transient_server_error_recovers_before_fallback(self, sleep):
        client = self.make_client(
            [
                FakeResponse(500, {}),
                FakeResponse(
                    200, {"choices": [{"message": {"content": '{"clusters": []}'}}]}
                ),
            ]
        )
        self.assertEqual(client._post([]), {"clusters": []})
        self.assertEqual(client.requests_made, 2)
        self.assertEqual(client.retries_made, 1)
        sleep.assert_called_once_with(1)

    @patch.object(ai_clients.time, "sleep")
    def test_retries_are_bounded(self, sleep):
        client = self.make_client(FakeResponse(503, {}))
        with self.assertRaises(TamuAIError):
            client._post([])
        self.assertEqual(client.requests_made, 3)

    def test_auth_errors_do_not_retry(self):
        client = self.make_client(FakeResponse(401, {}))
        with self.assertRaises(TamuAIError):
            client._post([])
        self.assertEqual(client.requests_made, 1)

    def test_quota_stops_subsequent_requests(self):
        client = self.make_client(FakeResponse(429, {}))
        for _ in range(2):
            with self.assertRaises(TamuAIQuotaExceeded):
                client._post([])
        self.assertEqual(client.requests_made, 1)

    def test_budget_is_atomic_under_concurrency(self):
        client = self.make_client(FakeResponse(200, {}), max_requests=3)

        def consume(_):
            try:
                client._consume_request_budget()
                return True
            except TamuAIQuotaExceeded:
                return False

        with ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(sum(pool.map(consume, range(50))), 3)
        self.assertEqual(client.requests_made, 3)

    def test_length_finish_reason_is_reported_as_truncation(self):
        client = self.make_client(
            FakeResponse(
                200,
                {
                    "choices": [
                        {
                            "message": {"content": '{"clusters": []}'},
                            "finish_reason": "length",
                        }
                    ]
                },
            )
        )
        with self.assertRaisesRegex(ai_clients.TamuAIInvalidJSON, "token limit"):
            client._post([])

    def test_empty_verification_does_not_construct_sdk_client(self):
        with patch.object(ai_clients, "TypeSafeClient") as sdk:
            judge = TypeSafeReviewJudge()
            self.assertEqual(judge.verify_claims({}, {}), {})
            self.assertEqual(judge.verify_claims({1: []}, {1: []}), {})
            self.assertEqual(judge.classify_clusters({}), {})
            self.assertEqual(judge.verify_summaries({}, {}), {})
            sdk.assert_not_called()

    def test_enforces_local_request_budget(self):
        session = FakeSession(
            FakeResponse(
                200,
                {"choices": [{"message": {"content": '{"clusters": []}'}}]},
            )
        )
        client = TamuAIChatClient(
            api_key="secret",
            base_url="https://example.test/v1/chat/completions",
            model="test-model",
            max_requests=1,
            session=session,
        )

        client._post([])
        with self.assertRaises(TamuAIQuotaExceeded):
            client._post([])

    def test_rejects_non_json_model_output(self):
        session = FakeSession(
            FakeResponse(200, {"choices": [{"message": {"content": "not json"}}]})
        )
        client = TamuAIChatClient(
            api_key="secret",
            base_url="https://example.test/v1",
            model="test-model",
            session=session,
        )

        with self.assertRaises(TamuAIError):
            client._post([])

    def test_parses_tamu_sse_fallback(self):
        sse = "\n".join(
            [
                'data: {"choices":[{"delta":{"content":"{\\"clusters\\":"}}]}',
                'data: {"choices":[{"delta":{"content":"[]}"}}]}',
                "data: [DONE]",
            ]
        )
        session = FakeSession(FakeResponse(200, ValueError(), text=sse))
        client = TamuAIChatClient(
            api_key="secret",
            base_url="https://chat-api.tamu.ai/openai",
            model="test-model",
            session=session,
        )

        self.assertEqual(client._post([]), {"clusters": []})
        request = session.calls[0][1]
        self.assertFalse(request["json"]["stream"])
        self.assertEqual(request["params"], {"bypass_filter": "false"})


class FakeQuestion:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class FakeTypeSafeClient:
    response = None
    last_state = None
    last_questions = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def system_one(self, *, state, questions):
        type(self).last_state = state
        type(self).last_questions = questions
        return type(self).response


class TypeSafeReviewJudgeTests(unittest.TestCase):
    @patch.object(ai_clients, "Choice", FakeQuestion)
    @patch.object(ai_clients, "TypeSafeClient", FakeTypeSafeClient)
    def test_classifies_topic_and_sentiment_in_one_call(self):
        FakeTypeSafeClient.response = SimpleNamespace(
            choices={
                "topic_4": SimpleNamespace(choice="teaching", confidence=0.91),
                "sentiment_4": SimpleNamespace(choice="positive", confidence=0.84),
            }
        )
        judge = TypeSafeReviewJudge()

        result = judge.classify_clusters(
            {4: [make_review("Clear explanations and useful examples.")]}
        )

        self.assertEqual(result[4].topic, "teaching")
        self.assertEqual(result[4].sentiment, "positive")
        self.assertAlmostEqual(result[4].topic_confidence, 0.91)
        self.assertTrue(
            {"topic_4", "sentiment_4"}.issubset(FakeTypeSafeClient.last_questions)
        )

    @patch.object(ai_clients, "Noul", FakeQuestion)
    @patch.object(ai_clients, "TypeSafeClient", FakeTypeSafeClient)
    def test_verifies_summary_support_probability(self):
        FakeTypeSafeClient.response = SimpleNamespace(
            nouls={"supported_1": SimpleNamespace(noul=0.88)}
        )
        judge = TypeSafeReviewJudge()

        result = judge.verify_summaries(
            {1: [make_review("Weekly quizzes were used.")]},
            {1: "The course used weekly quizzes."},
        )

        self.assertEqual(result, {1: 0.88})
        self.assertEqual(
            FakeTypeSafeClient.last_state["clusters"]["1"]["summary"],
            "The course used weekly quizzes.",
        )

    @patch.object(ai_clients, "Noul", FakeQuestion)
    @patch.object(ai_clients, "Score", FakeQuestion)
    @patch.object(ai_clients, "TypeSafeClient", FakeTypeSafeClient)
    def test_verifies_each_claim_independently(self):
        FakeTypeSafeClient.response = SimpleNamespace(
            choices={
                "claim_topic_1_0": SimpleNamespace(choice="exams", confidence=0.9)
            },
            nouls={
                "claim_supported_1_0": SimpleNamespace(noul=0.93),
                "claim_overgeneralized_1_0": SimpleNamespace(noul=0.12),
                "claim_erases_disagreement_1_0": SimpleNamespace(noul=0.08),
                "claim_positive_1_0": SimpleNamespace(noul=0.91),
                "claim_negative_1_0": SimpleNamespace(noul=0.06),
                "claim_mixed_1_0": SimpleNamespace(noul=0.04),
            },
            scores={
                "claim_importance_1_0": SimpleNamespace(score=3.4, confidence=0.82)
            },
        )
        judge = TypeSafeReviewJudge()
        claim = SummaryClaim(
            text="Weekly quizzes were used.",
            topic="exams",
            evidence_review_ids=["review-1"],
        )

        result = judge.verify_claims(
            {1: [make_review("Weekly quizzes were used.")]}, {1: [claim]}
        )

        self.assertAlmostEqual(result[(1, 0)].support_probability, 0.93)
        self.assertAlmostEqual(result[(1, 0)].overgeneralization_probability, 0.12)
        self.assertEqual(result[(1, 0)].topic, "exams")
        self.assertEqual(result[(1, 0)].sentiment, "positive")
        self.assertAlmostEqual(result[(1, 0)].importance, 3.4)

    def test_combines_independent_claim_valence_without_forcing_a_side(self):
        self.assertEqual(
            TypeSafeReviewJudge._claim_sentiment(0.83, 0.22, 0.78)[0], "mixed"
        )
        self.assertEqual(TypeSafeReviewJudge._claim_sentiment(0.18, 0.22)[0], "neutral")
        self.assertEqual(
            TypeSafeReviewJudge._claim_sentiment(0.7, 0.48, 0.2)[0], "mixed"
        )
        self.assertEqual(
            TypeSafeReviewJudge._claim_sentiment(0.71, 0.2, 0.1)[0], "positive"
        )


if __name__ == "__main__":
    unittest.main()
