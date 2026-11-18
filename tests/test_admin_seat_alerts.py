"""Isolated tests for the admin seat-alert history endpoint."""

import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from aggiermp.api.routers import admin_monitoring
from aggiermp.core import admin_seat_alerts
from aggiermp.core.config import settings


class SeatAlertHistoryTests(unittest.TestCase):
    def test_selects_buckets_and_preserves_zero_counts(self):
        stamp = datetime(2026, 11, 17, tzinfo=timezone.utc)
        for period, unit, zone, size in (
            ("day", "hour", "UTC", 24),
            ("week", "day", "America/Chicago", 7),
            ("month", "day", "America/Chicago", 30),
        ):
            with (
                self.subTest(period=period),
                patch.object(admin_seat_alerts, "get_session") as get_session,
            ):
                session = Mock()
                session.execute.return_value.all.return_value = [(stamp, 0)]
                get_session.return_value.__enter__.return_value = session
                result = admin_seat_alerts.read_seat_alerts(period)
                self.assertEqual(
                    session.execute.call_args.args[1],
                    {"unit": unit, "zone": zone, "step": f"1 {unit}", "size": size},
                )
                self.assertEqual(
                    result,
                    {
                        "timeZone": "America/Chicago",
                        "buckets": [{"startAt": stamp.isoformat(), "sent": 0}],
                    },
                )

    def test_route_requires_admin_key_and_valid_period(self):
        app = FastAPI()
        app.include_router(admin_monitoring.router)
        path = "/admin/operations/seat-alerts"
        headers = {"X-Admin-Key": "test-key"}
        with (
            TestClient(app) as client,
            patch.object(settings, "admin_api_key", "test-key"),
            patch.object(admin_monitoring, "read_seat_alerts") as probe,
        ):
            self.assertEqual(client.get(path).status_code, 401)
            self.assertEqual(
                client.get(f"{path}?period=year", headers=headers).status_code, 422
            )
            probe.assert_not_called()
            probe.return_value = {"timeZone": "America/Chicago", "buckets": []}
            response = client.get(path, headers=headers)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers["Cache-Control"], "no-store")
            probe.assert_called_once_with("week")
            probe.side_effect = SQLAlchemyError("connection details")
            response = client.get(f"{path}?period=day", headers=headers)
            self.assertEqual(response.status_code, 503)
            self.assertNotIn("connection details", response.text)
