"""Unit tests for /admin/hidden-professors. No database, Redis or network.

Run with: PYTHONPATH="src;." .venv/Scripts/python -m pytest --noconftest tests/test_admin_hidden_professors.py
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from aggiermp.api.routers import admin_hidden
from aggiermp.core.admin_auth import require_admin_key
from aggiermp.core.config import settings
from aggiermp.database.base import request_session

KEY = "test-admin-key"
H = {"X-Admin-Key": KEY}


class FakeDB:
    """Answers queries by substring; records every statement."""

    def __init__(self, tables=None, canonical=None, aliases=(), known=True, removed=1):
        self.tables = (
            tables
            if tables is not None
            else {"hidden_professors", "professor_aliases", "admin_summary_rebuilds"}
        )
        self.canonical = canonical
        self.aliases = aliases
        self.known = known
        self.removed = removed
        self.calls = []
        self.commit = MagicMock()
        self.rollback = MagicMock()

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.calls.append((sql, params))
        r = MagicMock()
        if "to_regclass" in sql:
            r.scalar.return_value = params["name"] in self.tables
        elif "SELECT canonical_id" in sql:
            r.scalar.return_value = self.canonical
        elif "SELECT alias_id" in sql:
            r.fetchall.return_value = [(a,) for a in self.aliases]
        elif "SELECT 1 FROM professors" in sql:
            r.first.return_value = (1,) if self.known else None
        elif "DELETE FROM hidden_professors" in sql:
            r.rowcount = self.removed
        elif sql.startswith("DELETE"):
            r.rowcount = 3
        elif "FROM hidden_professors h" in sql:
            r.fetchall.return_value = [("p1", "why", None, "Ann Lee")]
        return r

    def sql(self, needle):
        return [c for c in self.calls if needle in c[0]]


@pytest.fixture
def make_client():
    def _make(db):
        app = FastAPI()
        app.include_router(admin_hidden.router)
        app.dependency_overrides[request_session] = lambda: db
        return TestClient(app, raise_server_exceptions=False)

    with patch.object(settings, "admin_api_key", KEY):
        yield _make


def test_hide_inserts_deletes_and_clears_cache(make_client):
    db = FakeDB(canonical="canon", aliases=["a1", "canon"])
    inv = AsyncMock()
    with patch.object(admin_hidden, "ainvalidate_professors", inv):
        r = make_client(db).post(
            "/admin/hidden-professors",
            headers=H,
            json={"professor_id": "a1", "reason": "request"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["known"] is True and body["cacheCleared"] is True
    assert body["groupIds"] == ["a1", "canon"]
    assert body["deleted"] == {"reviews": 3, "summaries": 3, "pendingRebuilds": 3}
    assert db.sql("INSERT INTO hidden_professors")
    assert db.sql("DELETE FROM reviews")[0][1] == {"ids": ["a1", "canon"]}
    assert db.sql("DELETE FROM professor_summaries_new")
    assert db.sql("DELETE FROM admin_summary_rebuilds")
    assert not db.sql("DELETE FROM professors ")
    assert not db.sql("professor_aliases SET")
    db.commit.assert_called_once()
    inv.assert_awaited_once()
    assert set(inv.await_args.args[0]) == {"a1", "canon"}


def test_hide_reports_cache_failure_but_keeps_change(make_client):
    db = FakeDB(known=False)
    with patch.object(
        admin_hidden, "ainvalidate_professors", AsyncMock(side_effect=RuntimeError("redis"))
    ):
        r = make_client(db).post(
            "/admin/hidden-professors", headers=H, json={"professor_id": "x", "reason": "r"}
        )
    assert r.status_code == 200
    assert r.json()["cacheCleared"] is False and r.json()["known"] is False
    db.commit.assert_called_once()


def test_hide_skips_rebuild_table_when_missing(make_client):
    db = FakeDB(tables={"hidden_professors"})
    with patch.object(admin_hidden, "ainvalidate_professors", AsyncMock()):
        r = make_client(db).post(
            "/admin/hidden-professors", headers=H, json={"professor_id": "x", "reason": "r"}
        )
    assert r.status_code == 200
    assert r.json()["deleted"]["pendingRebuilds"] == 0
    assert not db.sql("admin_summary_rebuilds WHERE")


def test_hide_validates_body(make_client):
    c = make_client(FakeDB())
    assert c.post("/admin/hidden-professors", headers=H, json={"professor_id": "x"}).status_code == 422
    assert (
        c.post(
            "/admin/hidden-professors",
            headers=H,
            json={"professor_id": "x", "reason": "a" * 501},
        ).status_code
        == 422
    )


def test_unhide(make_client):
    db = FakeDB()
    inv = AsyncMock()
    with patch.object(admin_hidden, "ainvalidate_professors", inv):
        r = make_client(db).delete("/admin/hidden-professors/p1", headers=H)
    assert r.status_code == 200 and r.json()["removed"] is True
    db.commit.assert_called_once()
    inv.assert_awaited_once()


def test_unhide_not_hidden_is_404(make_client):
    r = make_client(FakeDB(removed=0)).delete("/admin/hidden-professors/p1", headers=H)
    assert r.status_code == 404


def test_list(make_client):
    r = make_client(FakeDB()).get("/admin/hidden-professors", headers=H)
    assert r.status_code == 200
    assert r.json() == {
        "items": [{"professorId": "p1", "reason": "why", "createdAt": None, "name": "Ann Lee"}]
    }


def test_missing_table_is_503(make_client):
    c = make_client(FakeDB(tables=set()))
    assert c.get("/admin/hidden-professors", headers=H).status_code == 503
    assert (
        c.post(
            "/admin/hidden-professors", headers=H, json={"professor_id": "x", "reason": "r"}
        ).status_code
        == 503
    )
    r = c.delete("/admin/hidden-professors/x", headers=H)
    assert r.status_code == 503 and "006" in r.json()["detail"]


def test_db_error_is_generic(make_client):
    db = FakeDB()
    db.execute = MagicMock(side_effect=RuntimeError("secret detail"))
    r = make_client(db).get("/admin/hidden-professors", headers=H)
    assert r.status_code == 500 and "secret" not in r.text


def test_requires_admin_key(make_client):
    c = make_client(FakeDB())
    for method, path in [
        ("get", "/admin/hidden-professors"),
        ("post", "/admin/hidden-professors"),
        ("delete", "/admin/hidden-professors/x"),
    ]:
        assert getattr(c, method)(path).status_code == 401
        assert getattr(c, method)(path, headers={"X-Admin-Key": "bad"}).status_code == 401


def test_every_route_depends_on_admin_key():
    def uses(d):
        return d.call is require_admin_key or any(uses(x) for x in d.dependencies)

    assert admin_hidden.router.routes
    for route in admin_hidden.router.routes:
        assert uses(route.dependant), route.path
