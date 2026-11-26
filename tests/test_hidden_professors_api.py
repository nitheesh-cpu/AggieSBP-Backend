"""Hidden professors at the course page call site (stubs, no database)."""

import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from aggiermp.core.campuses import TEXAS_AM_ID
from aggiermp.core.instructor_links import load_hidden_ids

from test_campus_matching import details_endpoint, evidence, query


def profile(pid):
    return SimpleNamespace(
        id=pid, first_name="Darryl", last_name="De Ruiter",
        university_id=TEXAS_AM_ID, num_ratings=12, avg_rating=4.5, legacy_id=99,
    )


def run(hidden):
    db = Mock()
    db.query.side_effect = [
        query(first=SimpleNamespace(term_desc="Spring 2027 - College Station")),
        query([SimpleNamespace(instructor_name="Darryl De Ruiter", instructor_pidm=1,
                               section_number="500", campus="College Station")]),
        query([profile("p1")]), query(), query(), query(),
    ]
    patchers = evidence() + [
        patch("aggiermp.core.instructor_links.load_hidden_ids", lambda d: frozenset(hidden)),
    ]
    for p in patchers:
        p.start()
    try:
        result = asyncio.run(details_endpoint()(None, "202711", "ANTH409", db, "College Station"))
    finally:
        for p in patchers:
            p.stop()
    return result["professors"]


class HiddenCoursePageTests(unittest.TestCase):
    def test_visible_professor_is_matched(self):
        self.assertEqual(run(set())[0]["id"], "p1")

    def test_hidden_professor_is_listed_without_profile(self):
        professors = run({"p1"})
        self.assertEqual(len(professors), 1)
        p = professors[0]
        self.assertEqual(p["name"], "Darryl De Ruiter")
        self.assertIsNone(p["id"])
        self.assertIsNone(p["rating"])
        self.assertIsNone(p["rmpId"])
        self.assertEqual(p["totalReviews"], 0)
        self.assertIsNone(p["courseSummary"])
        self.assertIsNone(p["overallSummary"])
        self.assertEqual(p["otherCourseSummaries"], [])

    def test_other_hidden_ids_do_not_affect_match(self):
        self.assertEqual(run({"someone-else"})[0]["id"], "p1")


class LoadHiddenIdsTests(unittest.TestCase):
    def test_mock_session_gives_empty_set(self):
        self.assertEqual(load_hidden_ids(Mock()), frozenset())

    def test_missing_table_gives_empty_set(self):
        db = Mock()
        db.execute.return_value.scalar.return_value = None
        self.assertEqual(load_hidden_ids(db), frozenset())

    def test_failed_check_gives_empty_set(self):
        db = Mock()
        db.execute.side_effect = RuntimeError("boom")
        self.assertEqual(load_hidden_ids(db), frozenset())

    def test_existing_table_uses_hidden_professor_ids(self):
        db = Mock()
        db.execute.return_value.scalar.return_value = "hidden_professors"
        with patch("aggiermp.core.hidden_professors.hidden_professor_ids",
                   return_value={"a", "b"}) as m:
            self.assertEqual(load_hidden_ids(db), frozenset({"a", "b"}))
        m.assert_called_once_with(db)


if __name__ == "__main__":
    unittest.main()
