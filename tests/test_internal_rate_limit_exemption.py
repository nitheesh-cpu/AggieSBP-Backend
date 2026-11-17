"""X-Internal-Key exempts website SSR requests from the per-IP rate limits (run with --noconftest)."""

import unittest
from unittest.mock import patch

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from aggiermp.api import main
from aggiermp.api.main import InternalExemptLimiter
from aggiermp.core.config import settings


def build_app() -> TestClient:
    limiter = InternalExemptLimiter(key_func=get_remote_address)
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore

    @app.get("/async")
    @limiter.limit("2/minute")
    async def async_route(request: Request):
        return {"ok": True}

    @app.get("/sync")
    @limiter.limit("2/minute")
    def sync_route(request: Request):
        return {"ok": True}

    return TestClient(app)


class InternalKeyExemptionTests(unittest.TestCase):
    def statuses(self, client, path, headers=None, n=4):
        return [client.get(path, headers=headers or {}).status_code for _ in range(n)]

    def test_main_app_uses_exempting_limiter(self):
        self.assertIsInstance(main.limiter, InternalExemptLimiter)

    def test_valid_key_skips_limits(self):
        with patch.object(settings, "internal_api_key", "s3cret"):
            client = build_app()
            for path in ("/async", "/sync"):
                self.assertEqual(
                    self.statuses(client, path, {"X-Internal-Key": "s3cret"}),
                    [200, 200, 200, 200],
                )

    def test_wrong_or_missing_key_is_limited(self):
        with patch.object(settings, "internal_api_key", "s3cret"):
            client = build_app()
            self.assertEqual(
                self.statuses(client, "/async", {"X-Internal-Key": "nope"}),
                [200, 200, 429, 429],
            )
            client = build_app()
            self.assertEqual(self.statuses(client, "/sync"), [200, 200, 429, 429])

    def test_unset_setting_exempts_nothing(self):
        with patch.object(settings, "internal_api_key", None):
            client = build_app()
            self.assertEqual(
                self.statuses(client, "/async", {"X-Internal-Key": ""}),
                [200, 200, 429, 429],
            )
        with patch.object(settings, "internal_api_key", ""):
            client = build_app()
            self.assertEqual(
                self.statuses(client, "/sync", {"X-Internal-Key": ""}),
                [200, 200, 429, 429],
            )

    def test_internal_requests_dont_consume_client_budget(self):
        with patch.object(settings, "internal_api_key", "s3cret"):
            client = build_app()
            self.assertEqual(
                self.statuses(client, "/async", {"X-Internal-Key": "s3cret"}, n=5),
                [200] * 5,
            )
            self.assertEqual(self.statuses(client, "/async", n=3), [200, 200, 429])


if __name__ == "__main__":
    unittest.main()
