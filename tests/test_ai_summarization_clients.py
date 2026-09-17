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


class FakeResponse:
    def __init__(self, status_code, body, text=""):
        self.status_code = status_code
        self._body = body
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("request failed")

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def make_review(text):
    return SimpleNamespace(text=text)


class TamuAIChatClientTests(unittest.TestCase):
    def test_batches_clusters_and_parses_json(self):
        content = (
            '{"clusters": [{"cluster_id": "2", '
            '"summary": "Exams closely follow the study guides."}]}'
        )
        session = FakeSession(
            FakeResponse(200, {"choices": [{"message": {"content": content}}]})
        )
        client = TamuAIChatClient(
            api_key="secret",
            base_url="https://example.test/v1",
            model="test-model",
            session=session,
        )

        result = client.summarize_clusters(
            {2: [make_review("The exams closely followed the study guide.")]},
            {2: "exams"},
        )

        self.assertEqual(result, {2: "Exams closely follow the study guides."})
        self.assertEqual(
            session.calls[0][0], "https://example.test/v1/chat/completions"
        )
        request = session.calls[0][1]["json"]
        self.assertEqual(request["model"], "test-model")
        self.assertNotIn("response_format", request)

    def test_treats_429_as_daily_quota_stop(self):
        client = TamuAIChatClient(
            api_key="secret",
            base_url="https://example.test/v1",
            model="test-model",
            session=FakeSession(FakeResponse(429, {})),
        )

        with self.assertRaises(TamuAIQuotaExceeded):
            client.summarize_clusters(
                {0: [make_review("Useful review text.")]}, {0: "other"}
            )

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

        client.summarize_clusters({}, {})
        with self.assertRaises(TamuAIQuotaExceeded):
            client.summarize_clusters({}, {})

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
            client.summarize_clusters({}, {})

    def test_parses_tamu_sse_fallback(self):
        sse = "\n".join(
            [
                'data: {"choices":[{"delta":{"content":"{\\\"clusters\\\":"}}]}',
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

        self.assertEqual(client.summarize_clusters({}, {}), {})
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
        self.assertEqual(
            set(FakeTypeSafeClient.last_questions), {"topic_4", "sentiment_4"}
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


if __name__ == "__main__":
    unittest.main()
