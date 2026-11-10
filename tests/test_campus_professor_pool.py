import unittest
from unittest.mock import Mock, patch

from pipelines.professors.campuses import (
    TEXAS_AM_ID, TEXAS_AM_GALVESTON_ID, get_university_ids,
)
from pipelines.professors.scrapers import RMPReviewCollector
from pipelines.professors.upsert_professors import upsert_all_professors


def professor_node(pid, school=TEXAS_AM_GALVESTON_ID):
    return {
        "id": pid, "legacyId": 123, "firstName": "Test", "lastName": "Professor",
        "department": "Math", "avgRating": 4.0, "numRatings": 2,
        "avgDifficulty": 3.0, "wouldTakeAgainPercent": 100,
        "school": {"id": school},
    }


def page(nodes, more=False, cursor=None):
    return {"data": {"search": {"teachers": {
        "edges": [{"node": node} for node in nodes],
        "pageInfo": {"hasNextPage": more, "endCursor": cursor},
    }}}}


class CampusPoolTests(unittest.TestCase):
    def collector(self, pages):
        collector = RMPReviewCollector.__new__(RMPReviewCollector)
        collector.url = "https://example.test"
        collector.headers = {}
        collector.cookies = {}
        collector.session = Mock()
        collector.session.post.side_effect = [
            Mock(json=Mock(return_value=body)) for body in pages
        ]
        return collector

    def test_default_includes_both_campuses_and_preserves_override(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(get_university_ids(), [TEXAS_AM_ID, TEXAS_AM_GALVESTON_ID])
            self.assertEqual(get_university_ids("1006"), [TEXAS_AM_GALVESTON_ID])
        with patch.dict("os.environ", {"UNIVERSITY_ID": TEXAS_AM_ID}):
            self.assertEqual(get_university_ids(), [TEXAS_AM_ID])

    def test_pages_are_complete_deduplicated_and_campus_scoped(self):
        collector = self.collector([
            page([professor_node("p1"), professor_node("wrong", TEXAS_AM_ID)], True, "next"),
            page([professor_node("p1"), professor_node("p2")]),
        ])
        result = collector.get_all_professors(TEXAS_AM_GALVESTON_ID)
        self.assertEqual([p.id for p in result], ["p1", "p2"])
        self.assertTrue(all(p.university_id == TEXAS_AM_GALVESTON_ID for p in result))
        variables = collector.session.post.call_args_list[0].kwargs["json"]["variables"]
        self.assertFalse(variables["query"]["fallback"])

    def test_graphql_failure_does_not_look_like_an_empty_campus(self):
        collector = self.collector([{"errors": [{"message": "unavailable"}]}])
        with self.assertRaises(ValueError):
            collector.get_all_professors(TEXAS_AM_GALVESTON_ID)

    def test_repeated_cursor_fails_instead_of_looping(self):
        collector = self.collector([page([], True, "same"), page([], True, "same")])
        with self.assertRaises(ValueError):
            collector.get_all_professors(TEXAS_AM_GALVESTON_ID)

    def test_new_campus_is_registered_before_professor_upsert(self):
        session = Mock()
        session.query.return_value.filter.return_value.first.return_value = None
        session.query.return_value.all.return_value = []
        events = []
        module = "pipelines.professors.upsert_professors"
        with patch(f"{module}.RMPReviewCollector") as collector, \
             patch(f"{module}.upsert_universities", side_effect=lambda *_: events.append("campus")), \
             patch(f"{module}.upsert_professors", side_effect=lambda *_: events.append("professors")):
            collector.return_value.get_all_professors.return_value = []
            result = upsert_all_professors(TEXAS_AM_GALVESTON_ID, session)
        self.assertNotIn("error", result)
        self.assertEqual(events, ["campus", "professors"])


if __name__ == "__main__":
    unittest.main()
