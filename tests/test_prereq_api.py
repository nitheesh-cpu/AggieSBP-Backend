"""GET /courses/prereqs with a fake DB (run with --noconftest)."""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from aggiermp.api.main import app
from aggiermp.api.routers import prereqs


def C(code, concurrent=False):
    return {"type": "course", "code": code, "min_grade": None, "concurrent": concurrent}


CATALOG = {
    "CSCE 411": {
        "prereq_tree": {"type": "and", "children": [C("CSCE 221"), C("CSCE 222")]},
        "coreq_tree": None,
        "prereq_parse": {"confidence": "high"},
    },
    "CSCE 221": {
        "prereq_tree": {"type": "or", "children": [C("CSCE 120"), C("CSCE 121")]},
        "coreq_tree": None,
        "prereq_parse": {"confidence": "low"},
    },
    "CSCE 222": {"prereq_tree": None, "coreq_tree": {"type": "and", "children": [C("MATH 151", True)]}, "prereq_parse": None},
    "CSCE 121": {"prereq_tree": None, "coreq_tree": None, "prereq_parse": None},
    "MATH 151": {"prereq_tree": None, "coreq_tree": None, "prereq_parse": None},
    # CSCE 120 is referenced but deliberately absent.
}


class FakeDB:
    def __init__(self):
        self.queries = []

    def execute(self, _sql, params):
        self.queries.append(sorted(params["codes"]))
        rows = []
        for code in params["codes"]:
            if code in CATALOG:
                d = CATALOG[code]
                rows.append(SimpleNamespace(
                    code=code, name=f"{code} name", credits=3,
                    prerequisites="text", corequisites=None,
                    prereq_tree=d["prereq_tree"], coreq_tree=d["coreq_tree"],
                    prereq_parse=d["prereq_parse"], cross_listings=None,
                ))
        return SimpleNamespace(fetchall=lambda: rows)


class PrereqApiTests(unittest.TestCase):
    def setUp(self):
        self.db = FakeDB()
        app.dependency_overrides[prereqs.get_session] = lambda: self.db
        self.redis = patch("aggiermp.core.cache.get_redis", AsyncMock(return_value=None))
        self.redis.start()
        self.client = TestClient(app)

    def tearDown(self):
        self.redis.stop()
        app.dependency_overrides.clear()

    def test_depth_zero_returns_only_requested(self):
        r = self.client.get("/courses/prereqs", params={"codes": "csce411"})
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(list(body["courses"]), ["CSCE 411"])
        self.assertEqual(body["courses"]["CSCE 411"]["confidence"], "high")
        self.assertEqual(body["courses"]["CSCE 411"]["prereqTree"]["type"], "and")
        self.assertEqual(self.db.queries, [["CSCE 411"]])

    def test_depth_walks_prereq_and_coreq_trees(self):
        body = self.client.get("/courses/prereqs", params={"codes": "CSCE 411", "depth": 2}).json()
        self.assertEqual(
            sorted(body["courses"]),
            ["CSCE 121", "CSCE 221", "CSCE 222", "CSCE 411", "MATH 151"],
        )
        self.assertEqual(body["missing"], ["CSCE 120"])
        self.assertFalse(body["truncated"])
        self.assertEqual(
            self.db.queries,
            [["CSCE 411"], ["CSCE 221", "CSCE 222"], ["CSCE 120", "CSCE 121", "MATH 151"]],
        )

    def test_total_cap_truncates(self):
        with patch.object(prereqs, "MAX_TOTAL", 2):
            body = self.client.get("/courses/prereqs", params={"codes": "CSCE411", "depth": 3}).json()
        self.assertEqual(len(body["courses"]), 2)
        self.assertTrue(body["truncated"])

    def test_rejects_bad_input(self):
        self.assertEqual(self.client.get("/courses/prereqs", params={"codes": "nope"}).status_code, 400)
        self.assertEqual(self.client.get("/courses/prereqs", params={"codes": ","}).status_code, 400)
        many = ",".join(f"CSCE {100 + i}" for i in range(101))
        self.assertEqual(self.client.get("/courses/prereqs", params={"codes": many}).status_code, 400)
        self.assertEqual(
            self.client.get("/courses/prereqs", params={"codes": "CSCE411", "depth": 9}).status_code, 422
        )


if __name__ == "__main__":
    unittest.main()
