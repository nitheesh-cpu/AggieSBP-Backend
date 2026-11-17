"""Run with unittest to avoid the integration suite's live database fixture."""

import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from aggiermp.api.routers.admin_monitoring import router
from aggiermp.core import admin_monitoring as monitoring
from aggiermp.core.config import settings


class MonitorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        for key, value in (
            ("cronitor_api_key", "private-key"),
            ("cronitor_monitor_keys", "one,two"),
            ("cronitor_environment", "production"),
        ):
            patcher = patch.object(settings, key, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    async def test_concurrent_requests_share_one_refresh(self):
        reader = monitoring.MonitorReader()
        reader.fetch = AsyncMock(return_value=[{"key": "one"}])
        results = await asyncio.gather(*(reader.read() for _ in range(8)))
        self.assertTrue(all(r["status"] == "ok" for r in results))
        reader.fetch.assert_awaited_once_with(
            ["one", "two"], "private-key", "production"
        )

    async def test_fetch_assembles_split_chunks_and_sets_safe_transport_options(self):
        async def chunks(size):
            yield b'{"key":"one",'
            yield b'"passing":true}'

        response = AsyncMock()
        response.status = 200
        response.raise_for_status = Mock()
        response.content.iter_chunked = chunks
        context = AsyncMock()
        context.__aenter__.return_value = response
        client = Mock()
        client.get.return_value = context
        client_context = AsyncMock()
        client_context.__aenter__.return_value = client
        with patch.object(
            monitoring.aiohttp, "ClientSession", return_value=client_context
        ):
            result = await monitoring.MonitorReader().fetch(
                ["one"], "private-key", "production"
            )
        self.assertEqual(result[0]["status"], "healthy")
        self.assertFalse(client.get.call_args.kwargs["allow_redirects"])
        self.assertEqual(
            client.get.call_args.kwargs["headers"]["Cronitor-Version"], "2025-11-28"
        )
        self.assertEqual(client.get.call_args.kwargs["params"]["env"], "production")

    async def test_fetch_rejects_oversized_body(self):
        async def chunks(size):
            yield b" " * 1_000_001

        response = AsyncMock()
        response.status = 200
        response.raise_for_status = Mock()
        response.content.iter_chunked = chunks
        context = AsyncMock()
        context.__aenter__.return_value = response
        client = Mock()
        client.get.return_value = context
        client_context = AsyncMock()
        client_context.__aenter__.return_value = client
        with patch.object(
            monitoring.aiohttp, "ClientSession", return_value=client_context
        ):
            with self.assertRaisesRegex(ValueError, "too large"):
                await monitoring.MonitorReader().fetch(
                    ["one"], "private-key", "production"
                )

    async def test_outage_preserves_but_marks_old_data_and_throttles_retries(self):
        reader = monitoring.MonitorReader()
        reader.fetch = AsyncMock(return_value=[{"key": "one"}])
        original = await reader.read()
        reader.expires = 0
        reader.fetch.side_effect = RuntimeError("secret private-key response")
        stale = await reader.read()
        self.assertEqual(stale["status"], "stale")
        self.assertEqual(stale["fetchedAt"], original["fetchedAt"])
        self.assertEqual(stale["monitors"], original["monitors"])
        self.assertNotIn("private-key", str(stale))
        await reader.read()
        self.assertEqual(reader.fetch.await_count, 2)

    async def test_first_failure_and_missing_config_are_not_healthy(self):
        reader = monitoring.MonitorReader()
        reader.fetch = AsyncMock(side_effect=RuntimeError("secret"))
        self.assertEqual((await reader.read())["status"], "unavailable")
        with patch.object(settings, "cronitor_api_key", None):
            result = await reader.read()
        self.assertEqual(result["status"], "not_configured")
        self.assertEqual(result["monitors"], [])

    async def test_credential_change_discards_previous_snapshot(self):
        reader = monitoring.MonitorReader()
        reader.fetch = AsyncMock(return_value=[{"key": "one"}])
        await reader.read()
        reader.fetch.side_effect = RuntimeError()
        with patch.object(settings, "cronitor_api_key", "different"):
            result = await reader.read()
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["monitors"], [])

    async def test_partial_health_failure_keeps_other_probes(self):
        reader = monitoring.HealthReader()
        with patch.object(
            monitoring, "database_and_worker", side_effect=RuntimeError("db password")
        ), patch.object(
            monitoring, "redis_health", AsyncMock(return_value={"status": "healthy"})
        ):
            result = await reader.read()
        self.assertEqual(result["database"]["status"], "unavailable")
        self.assertEqual(result["worker"]["status"], "unavailable")
        self.assertEqual(result["redis"]["status"], "healthy")
        self.assertNotIn("password", str(result))


class NormalizationTests(unittest.TestCase):
    def test_freshness_reports_stored_timestamps_and_preserves_missing_dates(self):
        session = Mock()
        session.execute.return_value.one.return_value = (
            datetime(2026, 11, 17, 18, 0),
            None,
            datetime(2026, 11, 16, 0, 0, tzinfo=timezone.utc),
        )
        with patch.object(monitoring, "get_session") as get_session:
            get_session.return_value.__enter__.return_value = session
            self.assertEqual(
                monitoring.read_data_timestamps(),
                {
                    "coursesUpdatedAt": "2026-11-17T18:00:00+00:00",
                    "sectionsUpdatedAt": None,
                    "newestReviewAt": "2026-11-16T00:00:00+00:00",
                },
            )

    def test_failure_takes_precedence_and_duration_units(self):
        result = monitoring.normalize_monitor(
            {
                "passing": False,
                "running": True,
                "latest_event": {"stamp": 1700000000, "metrics": {"duration": 2.5}},
                "latest_invocations": [
                    {"started_at": "2026-11-17T00:00:00Z", "duration": 2500}
                ],
            },
            "a/b",
        )
        self.assertEqual(result["status"], "failing")
        self.assertEqual(result["lastDurationSeconds"], 2.5)
        self.assertEqual(result["recentRuns"][0]["durationSeconds"], 2.5)
        self.assertTrue(result["url"].endswith("a%2Fb"))

    def test_unknown_is_not_healthy_and_no_raw_fields_escape(self):
        result = monitoring.normalize_monitor(
            {"request": {"headers": {"secret": "credential"}}}, "x"
        )
        self.assertEqual(result["status"], "unknown")
        self.assertNotIn("credential", str(result))
        self.assertIsNone(monitoring.iso(float("nan")))

    def test_worker_states_and_sessions_closed(self):
        for heartbeat, expected in [
            (None, "not_started"),
            (datetime.now(timezone.utc), "healthy"),
            (datetime.now(timezone.utc) - timedelta(seconds=120), "stale"),
        ]:
            session = Mock()
            session.execute.side_effect = [
                Mock(),
                Mock(),
                Mock(scalar=lambda: "admin_worker_heartbeats"),
                Mock(scalar=lambda: heartbeat),
            ]
            with patch.object(monitoring, "get_session", return_value=session):
                database, worker = monitoring.database_and_worker()
            self.assertEqual(database["status"], "healthy")
            self.assertEqual(worker["status"], expected)
            session.close.assert_called_once()

    def test_missing_worker_migration_does_not_make_database_unhealthy(self):
        session = Mock()
        session.execute.side_effect = [Mock(), Mock(), Mock(scalar=lambda: None)]
        with patch.object(monitoring, "get_session", return_value=session):
            database, worker = monitoring.database_and_worker()
        self.assertEqual(database["status"], "healthy")
        self.assertEqual(worker["status"], "not_configured")


class AuthenticationTests(unittest.TestCase):
    def test_all_routes_require_key_and_do_not_probe_on_denial(self):
        app = FastAPI()
        app.include_router(router)
        with TestClient(app) as client, patch.object(
            settings, "admin_api_key", "admin-secret"
        ), patch.object(
            monitoring.monitor_reader, "read", AsyncMock(return_value={"status": "ok"})
        ) as probe:
            for path in (
                "/admin/operations/monitors",
                "/admin/operations/health",
                "/admin/operations/freshness",
            ):
                self.assertEqual(client.get(path).status_code, 401)
                self.assertEqual(
                    client.get(path, headers={"X-Admin-Key": "wrong"}).status_code, 401
                )
            probe.assert_not_called()
            response = client.get(
                "/admin/operations/monitors", headers={"X-Admin-Key": "admin-secret"}
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_freshness_returns_service_unavailable_when_database_probe_fails(self):
        app = FastAPI()
        app.include_router(router)
        with TestClient(app) as client, patch.object(
            settings, "admin_api_key", "admin-secret"
        ), patch.object(
            monitoring.freshness_reader, "read", AsyncMock(return_value=None)
        ):
            response = client.get(
                "/admin/operations/freshness", headers={"X-Admin-Key": "admin-secret"}
            )
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.headers["Cache-Control"], "no-store")


class FreshnessReaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_requests_share_one_database_probe(self):
        reader = monitoring.FreshnessReader()
        dates = {"coursesUpdatedAt": "2026-11-17T18:00:00+00:00"}
        with patch.object(monitoring, "read_data_timestamps", return_value=dates) as read:
            result = await asyncio.gather(*(reader.read() for _ in range(4)))
            self.assertEqual(result, [dates] * 4)
            read.assert_called_once()


class PartialFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_failing_monitor_is_unavailable_not_healthy(self):
        def fake_get(url, **kwargs):
            context = AsyncMock()
            if url.endswith("/bad"):
                context.__aenter__.side_effect = RuntimeError("boom private-key")
                return context
            response = AsyncMock()
            response.status = 200
            response.raise_for_status = Mock()

            async def chunks(size):
                yield b'{"key":"good","passing":true,"schedules":["*/15 * * * *"]}'

            response.content.iter_chunked = chunks
            context.__aenter__.return_value = response
            return context

        client = Mock()
        client.get.side_effect = fake_get
        client_context = AsyncMock()
        client_context.__aenter__.return_value = client
        with patch.object(
            monitoring.aiohttp, "ClientSession", return_value=client_context
        ):
            result = await monitoring.MonitorReader().fetch(
                ["good", "bad"], "private-key", "production"
            )
        self.assertEqual([m["status"] for m in result], ["healthy", "unavailable"])
        self.assertEqual(result[0]["schedule"], "*/15 * * * *")
        self.assertNotIn("private-key", repr(result))

    def test_schedule_supports_both_api_versions(self):
        self.assertEqual(monitoring.schedule_text({"schedule": "0 * * * *"}), "0 * * * *")
        self.assertEqual(monitoring.schedule_text({"schedules": ["a", "b"]}), "a, b")
        self.assertIsNone(monitoring.schedule_text({"schedules": [1]}))

    async def test_api_key_never_in_reader_result_even_on_failure(self):
        reader = monitoring.MonitorReader()
        reader.fetch = AsyncMock(side_effect=RuntimeError("private-key leaked"))
        with patch.object(settings, "cronitor_api_key", "private-key"), patch.object(
            settings, "cronitor_monitor_keys", "one"
        ):
            result = await reader.read()
        self.assertEqual(result["status"], "unavailable")
        self.assertNotIn("private-key", repr(result))


if __name__ == "__main__":
    unittest.main()
