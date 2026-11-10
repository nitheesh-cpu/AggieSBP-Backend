import unittest
from unittest.mock import patch

from test_ai_summarization_clients import FakeResponse, FakeSession
from pipelines.professors.hierarchical_summarization.ai_clients import (
    TamuAIChatClient,
    TamuAIError,
    TamuAIInvalidJSON,
    TamuAIQuotaExceeded,
)


class StructuredSummaryTests(unittest.TestCase):
    def test_budget_error_envelope_stops_run_for_success_and_error_status(self):
        body = {
            "error": {
                "message": "Budget has been exceeded! Current cost: 10.028, Max budget: 10.0",
                "type": "budget_exceeded",
                "param": None,
                "code": "400",
            }
        }
        for status in (200, 400):
            with self.subTest(status=status):
                client = self.client()
                session = FakeSession(FakeResponse(status, body))
                client._injected_session = session
                for _ in range(2):
                    with self.assertRaises(TamuAIQuotaExceeded):
                        client.generate_professor_summary("## CSCE121 (1 reviews)")
                self.assertEqual(len(session.calls), 1)
                self.assertEqual(client.retries_made, 0)

    def test_stream_budget_error_is_not_misreported_as_malformed_json(self):
        client = self.client()
        client._injected_session = FakeSession(
            FakeResponse(
                200,
                None,
                'data: {"error":{"type":"budget_exceeded","message":"Budget has been exceeded!"}}',
            )
        )
        with self.assertRaises(TamuAIQuotaExceeded):
            client._post([])
        self.assertTrue(client._quota_exhausted)

    def test_server_error_envelope_is_retried(self):
        client = self.client()
        client._injected_session = FakeSession(
            [
                FakeResponse(200, {"error": {"type": "server_error", "code": "500"}}),
                FakeResponse(
                    200, {"choices": [{"message": {"content": '{"clusters": []}'}}]}
                ),
            ]
        )
        with patch(
            "pipelines.professors.hierarchical_summarization.ai_clients.time.sleep"
        ):
            self.assertEqual(client._post([]), {"clusters": []})
        self.assertEqual(client.retries_made, 1)

    def client(self):
        return TamuAIChatClient(
            api_key="test",
            base_url="https://example.test",
            model="test",
            session=FakeSession(FakeResponse(200, {})),
        )

    def test_malformed_sse_event_is_not_silently_dropped(self):
        response = FakeResponse(
            200, None, 'data: {bad}\ndata: {"choices":[{"delta":{"content":"{}"}}]}'
        )
        with self.assertRaises(TamuAIInvalidJSON):
            self.client()._response_body(response)

    def test_usage_sse_event_is_accepted_and_finish_reason_preserved(self):
        response = FakeResponse(
            200,
            None,
            "\n".join(
                [
                    'data: {"choices":[{"delta":{"content":"{}"},"finish_reason":"length"}]}',
                    'data: {"choices":[],"usage":{"total_tokens":5}}',
                    "data: [DONE]",
                ]
            ),
        )
        body = self.client()._response_body(response)
        self.assertEqual(body["choices"][0]["finish_reason"], "length")

    def test_refusal_does_not_enter_json_repair_loop(self):
        client = self.client()
        client._injected_session = FakeSession(
            FakeResponse(
                200,
                {"choices": [{"message": {"refusal": "declined", "content": None}}]},
            )
        )
        with self.assertRaisesRegex(TamuAIError, "declined"):
            client._post([])
        self.assertEqual(client.requests_made, 1)


if __name__ == "__main__":
    unittest.main()
