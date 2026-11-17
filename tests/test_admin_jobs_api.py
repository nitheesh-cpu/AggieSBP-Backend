"""HTTP auth and validation tests, without application database fixtures."""

import unittest
from unittest.mock import Mock, patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from aggiermp.api.routers import admin_jobs as api
from aggiermp.core.config import settings


class JobApiTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(api.router)
        self.db = Mock()
        app.dependency_overrides[api.database] = lambda: self.db
        self.client = TestClient(app)
        key = patch.object(settings, "admin_api_key", "test-admin-key")
        key.start()
        self.addCleanup(key.stop)
        self.headers = {"X-Admin-Key": "test-admin-key"}

    def test_missing_and_wrong_keys_never_enqueue(self):
        with patch.object(api.jobs, "enqueue") as enqueue:
            for headers in ({}, {"X-Admin-Key": "wrong"}):
                response = self.client.post(
                    "/admin/summary-jobs",
                    headers=headers,
                    json={"professor_ids": ["p"]},
                )
                self.assertEqual(response.status_code, 401)
            enqueue.assert_not_called()

    def test_empty_and_oversized_batches_rejected(self):
        with patch.object(api.jobs, "enqueue") as enqueue:
            for ids in ([], ["p"] * 501):
                response = self.client.post(
                    "/admin/summary-jobs",
                    headers=self.headers,
                    json={"professor_ids": ids},
                )
                self.assertEqual(response.status_code, 422)
            enqueue.assert_not_called()

    def test_unknown_job_and_invalid_identifier(self):
        with patch.object(api.jobs, "get_job", return_value=None):
            response = self.client.get(
                "/admin/summary-jobs/00000000-0000-0000-0000-000000000001",
                headers=self.headers,
            )
            self.assertEqual(response.status_code, 404)
        response = self.client.get(
            "/admin/summary-jobs/not-a-uuid", headers=self.headers
        )
        self.assertEqual(response.status_code, 422)

    def test_overlap_is_conflict_and_rolls_back(self):
        with patch.object(
            api.jobs, "enqueue", side_effect=ValueError("Already active")
        ):
            response = self.client.post(
                "/admin/summary-jobs",
                headers=self.headers,
                json={"professor_ids": ["p"]},
            )
        self.assertEqual(response.status_code, 409)
        self.db.rollback.assert_called_once()


if __name__ == "__main__":
    unittest.main()
