"""Every admin route must demand X-Admin-Key. Routes are enumerated from the app,
so a newly added /admin route is covered without editing this file.

Run with: PYTHONPATH=src pytest --noconftest tests/test_admin_auth.py
(--noconftest skips the integration suite's live-database fixtures.)
No database, Redis or network is used: sessions are mocked and sockets blocked.
"""

import re
import socket
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from aggiermp.api.main import app
from aggiermp.api.routers import admin_monitoring
from aggiermp.core.admin_auth import require_admin_key
from aggiermp.core.config import settings
from aggiermp.database.base import request_session

KEY = "test-admin-key"
SITE_ORIGIN = "http://localhost:3000"
EXTRA_PROTECTED = {"/cache/clear"}
SAMPLE = {
    "uuid": "00000000-0000-0000-0000-000000000000",
    "int": "1",
}


def uses_admin_key(dependant) -> bool:
    if dependant.call is require_admin_key:
        return True
    return any(uses_admin_key(d) for d in dependant.dependencies)


def admin_routes():
    found = []
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        if route.path.startswith("/admin") or route.path in EXTRA_PROTECTED:
            for method in sorted(route.methods - {"HEAD", "OPTIONS"}):
                found.append((method, route))
    return found


def concrete(path: str) -> str:
    return re.sub(r"\{[^}]+\}", SAMPLE["uuid"], path)


class AdminAuthTests(unittest.TestCase):
    def setUp(self):
        self.addCleanup(
            setattr, app, "dependency_overrides", dict(app.dependency_overrides)
        )
        app.dependency_overrides[request_session] = lambda: MagicMock()
        limiter = getattr(app.state, "limiter", None)
        if limiter is not None:
            self.addCleanup(setattr, limiter, "enabled", limiter.enabled)
            limiter.enabled = False
        # Name resolution and outbound connections fail fast. (socket.connect
        # itself stays usable: asyncio's own loop needs it on Windows.)
        for name in ("getaddrinfo", "create_connection"):
            blocker = patch.object(
                socket, name, side_effect=OSError("network blocked")
            )
            blocker.start()
            self.addCleanup(blocker.stop)
        self.client = TestClient(app, raise_server_exceptions=False)

    def call(self, method, route, headers=None):
        return self.client.request(
            method, concrete(route.path), headers=headers or {}, json={}
        )

    def test_routes_were_found(self):
        paths = {route.path for _, route in admin_routes()}
        self.assertIn("/cache/clear", paths)
        self.assertIn("/admin/check", paths)
        self.assertIn("/admin/summary-jobs", paths)
        self.assertIn("/admin/operations/monitors", paths)

    def test_every_admin_route_depends_on_the_admin_key(self):
        for method, route in admin_routes():
            with self.subTest(route=f"{method} {route.path}"):
                self.assertTrue(uses_admin_key(route.dependant))

    def test_missing_key_is_401(self):
        with patch.object(settings, "admin_api_key", KEY):
            for method, route in admin_routes():
                with self.subTest(route=f"{method} {route.path}"):
                    self.assertEqual(self.call(method, route).status_code, 401)

    def test_wrong_key_is_401(self):
        with patch.object(settings, "admin_api_key", KEY):
            for method, route in admin_routes():
                for wrong in ("wrong", KEY + "x", KEY[:-1], ""):
                    with self.subTest(route=f"{method} {route.path}", key=wrong):
                        response = self.call(method, route, {"X-Admin-Key": wrong})
                        self.assertEqual(response.status_code, 401)

    def test_unset_key_is_503_even_when_a_key_is_sent(self):
        for unset in (None, ""):
            with patch.object(settings, "admin_api_key", unset):
                for method, route in admin_routes():
                    for headers in ({}, {"X-Admin-Key": "anything"}):
                        with self.subTest(route=f"{method} {route.path}", unset=unset):
                            self.assertEqual(
                                self.call(method, route, headers).status_code, 503
                            )

    def test_right_key_passes_authentication(self):
        with patch.object(settings, "admin_api_key", KEY), patch.object(
            admin_monitoring.freshness_reader, "read", AsyncMock(return_value={})
        ), patch.object(admin_monitoring, "read_seat_alerts", return_value={}):
            for method, route in admin_routes():
                with self.subTest(route=f"{method} {route.path}"):
                    response = self.call(method, route, {"X-Admin-Key": KEY})
                    self.assertNotIn(response.status_code, (401, 503))

    def test_cors_preflight_allows_admin_key_header(self):
        response = self.client.options(
            "/admin/check",
            headers={
                "Origin": SITE_ORIGIN,
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "X-Admin-Key",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers.get("access-control-allow-origin"), SITE_ORIGIN
        )
        self.assertIn(
            "x-admin-key", response.headers["access-control-allow-headers"].lower()
        )

    def test_cors_preflight_allows_delete_for_cache_clear(self):
        response = self.client.options(
            "/cache/clear",
            headers={
                "Origin": SITE_ORIGIN,
                "Access-Control-Request-Method": "DELETE",
                "Access-Control-Request-Headers": "X-Admin-Key",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("DELETE", response.headers["access-control-allow-methods"])


if __name__ == "__main__":
    unittest.main()
