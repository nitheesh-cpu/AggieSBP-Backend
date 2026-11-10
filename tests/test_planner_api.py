"""/users/me/planner with a fake session and an in-memory table (run with --noconftest)."""

import json
import unittest
from types import SimpleNamespace

from fastapi.testclient import TestClient

from aggiermp.api.main import app
from aggiermp.api.routers import planner


class FakeDB:
    """Just enough of user_planner_state for the router's statements."""

    def __init__(self):
        self.rows = {}
        self.commits = 0

    def execute(self, stmt, params):
        sql = " ".join(str(stmt).split())
        row = self.rows.get(params["user_id"] if "user_id" in params else params["u"])
        if sql.startswith("SELECT record, record_rev, plan, plan_rev"):
            r = row and SimpleNamespace(**row)
            return SimpleNamespace(fetchone=lambda: r)
        if sql.startswith("SELECT"):
            doc = sql.split()[1].rstrip(",")
            r = row and (row[doc], row[f"{doc}_rev"])
            return SimpleNamespace(fetchone=lambda: r)
        if sql.startswith("INSERT"):
            assert "ON CONFLICT (user_id) DO NOTHING" in sql
            self.rows.setdefault(params["u"], {"record": {}, "record_rev": 0, "plan": {}, "plan_rev": 0})
            return None
        if sql.startswith("UPDATE"):
            doc = sql.split("SET ")[1].split(" ")[0]
            row[doc] = json.loads(params["v"])
            row[f"{doc}_rev"] += 1
            return None
        raise AssertionError(f"unexpected SQL: {sql}")

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass


def course(code, **kw):
    return {"code": code, "status": "completed", "updatedAt": "2026-11-06T10:00:00Z", **kw}


class PlannerApiTests(unittest.TestCase):
    def setUp(self):
        self.db = FakeDB()
        self.user = "user-1"
        app.dependency_overrides[planner.get_session] = lambda: self.db
        app.dependency_overrides[planner.require_session] = lambda: SimpleNamespace(
            get_user_id=lambda: self.user
        )
        self.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.clear()

    def put_record(self, courses, base_rev, **record):
        return self.client.put(
            "/users/me/planner/record",
            json={"record": {"courses": courses, **record}, "base_rev": base_rev},
        )

    def test_new_user_gets_empty_state(self):
        r = self.client.get("/users/me/planner")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), {"record": None, "record_rev": 0, "plan": None, "plan_rev": 0})

    def test_save_then_read_back(self):
        r = self.put_record([course("CSCE 121", grade="A", term="20253")], 0, classification="junior")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["rev"], 1)
        state = self.client.get("/users/me/planner").json()
        self.assertEqual(state["record_rev"], 1)
        self.assertEqual(state["record"]["courses"][0]["code"], "CSCE 121")
        self.assertEqual(state["record"]["classification"], "junior")
        self.assertEqual(state["plan_rev"], 0)

    def test_stale_revision_conflicts_with_current_copy(self):
        self.put_record([course("CSCE 121")], 0)
        self.put_record([course("CSCE 121"), course("CSCE 221")], 1)
        r = self.put_record([course("MATH 151")], 1)  # other device still on rev 1
        self.assertEqual(r.status_code, 409)
        detail = r.json()["detail"]
        self.assertEqual(detail["rev"], 2)
        self.assertEqual([c["code"] for c in detail["value"]["courses"]], ["CSCE 121", "CSCE 221"])
        # Retrying on the current revision succeeds.
        self.assertEqual(self.put_record([course("MATH 151")], 2).status_code, 200)

    def test_first_save_must_start_from_zero(self):
        self.assertEqual(self.put_record([course("CSCE 121")], 3).status_code, 409)

    def test_record_and_plan_revisions_are_independent(self):
        self.put_record([course("CSCE 121")], 0)
        r = self.client.put("/users/me/planner/plan", json={"plan": {"watchlist": ["STAT 211"]}, "base_rev": 0})
        self.assertEqual((r.status_code, r.json()["rev"]), (200, 1))
        state = self.client.get("/users/me/planner").json()
        self.assertEqual((state["record_rev"], state["plan_rev"]), (1, 1))
        self.assertEqual(state["plan"], {"watchlist": ["STAT 211"]})

    def test_users_are_isolated(self):
        self.put_record([course("CSCE 121")], 0)
        self.user = "user-2"
        self.assertIsNone(self.client.get("/users/me/planner").json()["record"])

    def test_validation(self):
        bad = [
            [course("csce121")],  # code format
            [course("CSCE 121", status="dropped")],
            [course("CSCE 121", term="202631")],  # campus code, not a term key
            [course("CSCE 121", grade="A+")],
            [course("CSCE 121"), course("CSCE 121")],  # duplicate
        ]
        for courses in bad:
            with self.subTest(courses=courses):
                self.assertEqual(self.put_record(courses, 0).status_code, 422)
        big = {"notes": "x" * (planner.MAX_PLAN_BYTES + 1)}
        r = self.client.put("/users/me/planner/plan", json={"plan": big, "base_rev": 0})
        self.assertEqual(r.status_code, 422)
        self.assertEqual(self.db.rows, {})

    def test_requires_session(self):
        del app.dependency_overrides[planner.require_session]
        self.assertEqual(self.client.get("/users/me/planner").status_code, 401)
        r = self.client.put("/users/me/planner/record", json={"record": {"courses": []}, "base_rev": 0})
        self.assertEqual(r.status_code, 401)
        self.assertEqual(self.db.rows, {})


if __name__ == "__main__":
    unittest.main()
