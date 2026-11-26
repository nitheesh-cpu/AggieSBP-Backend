import unittest
from unittest.mock import Mock, patch

import requests

from pipelines.professors import review_sync
from pipelines.professors.review_sync import (
    deleted_review_ids,
    fetch_rating_counts,
    fetch_school_listings,
    school_listing,
    school_rating_counts,
    teacher_exists,
)


def listing_page(nodes, more=False, cursor=None):
    return {
        "data": {
            "search": {
                "teachers": {
                    "edges": [{"node": n} for n in nodes],
                    "pageInfo": {"hasNextPage": more, "endCursor": cursor},
                }
            }
        }
    }


def tnode(pid, count, school="s"):
    return {"id": pid, "numRatings": count, "school": {"id": school}}


def ratings_page(ids, total, more=False, cursor=None):
    return {
        "data": {
            "node": {
                "id": "T1",
                "numRatings": total,
                "ratings": {
                    "edges": [{"node": {"id": i}} for i in ids],
                    "pageInfo": {"hasNextPage": more, "endCursor": cursor},
                },
            }
        }
    }


def resp(body):
    return Mock(json=Mock(return_value=body), raise_for_status=Mock())


def collector_with(items):
    collector = Mock(url="https://example.test", headers={}, cookies={})
    effects = []
    for item in items:
        if isinstance(item, Exception):
            effects.append(item)
        else:
            effects.append(resp(item))
    collector.session.post.side_effect = effects
    return collector


class SchoolListingTests(unittest.TestCase):
    def test_complete_listing_filters_school_and_keeps_invalid_counts_as_none(self):
        c = collector_with(
            [
                listing_page([tnode("a", 3)], True, "c1"),
                listing_page(
                    [tnode("b", None), tnode("x", 9, "other"), tnode("c", -1)]
                ),
            ]
        )
        self.assertEqual(school_listing(c, "s"), {"a": 3, "b": None, "c": None})

    def test_rating_counts_only_valid_ints(self):
        c = collector_with([listing_page([tnode("a", 3), tnode("b", None)])])
        self.assertEqual(school_rating_counts(c, "s"), {"a": 3})

    def test_failed_page_raises(self):
        c = collector_with(
            [listing_page([tnode("a", 3)], True, "c1"), requests.HTTPError("503")]
        )
        with self.assertRaises(requests.HTTPError):
            school_listing(c, "s")

    def test_graphql_errors_raise(self):
        with self.assertRaises(ValueError):
            school_listing(collector_with([{"errors": [{"message": "x"}]}]), "s")

    def test_invalid_and_stuck_pagination_raise(self):
        bad = listing_page([tnode("a", 1)])
        bad["data"]["search"]["teachers"]["pageInfo"]["hasNextPage"] = "yes"
        with self.assertRaises(ValueError):
            school_listing(collector_with([bad]), "s")
        stuck = [
            listing_page([tnode("a", 1)], True, "c1"),
            listing_page([tnode("b", 1)], True, "c1"),
        ]
        with self.assertRaises(ValueError):
            school_listing(collector_with(stuck), "s")
        with self.assertRaises(ValueError):
            school_listing(collector_with([listing_page([], True, None)]), "s")


class FetchListingsTests(unittest.TestCase):
    def session(self, rows):
        s = Mock()
        s.query.return_value.filter.return_value.all.return_value = rows
        return s

    def run_fetch(self, fn, rows, per_school):
        collectors = []

        def make():
            c = Mock()
            collectors.append(c)
            return c

        def fake_listing(collector, school_id):
            result = per_school[school_id]
            if isinstance(result, Exception):
                raise result
            return result

        with patch.object(
            review_sync, "RMPReviewCollector", side_effect=make
        ), patch.object(review_sync, "school_listing", side_effect=fake_listing):
            out = fn(self.session(rows), ["p1", "p2", "p3"])
        return out, collectors

    def test_failed_school_dropped_and_collectors_closed(self):
        rows = [("p1", "s1"), ("p2", "s2"), ("p3", None)]
        out, collectors = self.run_fetch(
            fetch_school_listings,
            rows,
            {"s1": {"p1": 2, "q": None}, "s2": RuntimeError("boom")},
        )
        self.assertEqual(out, {"s1": {"p1": 2, "q": None}})
        self.assertEqual(len(collectors), 2)
        for c in collectors:
            c.session.close.assert_called_once()

    def test_fetch_rating_counts_unchanged_output(self):
        rows = [("p1", "s1"), ("p2", "s2")]
        out, _ = self.run_fetch(
            fetch_rating_counts,
            rows,
            {"s1": {"p1": 2, "q": None}, "s2": {"p2": 0}},
        )
        self.assertEqual(out, {"p1": 2, "p2": 0})

    def test_no_ids_returns_empty(self):
        self.assertEqual(fetch_school_listings(Mock(), []), {})
        self.assertEqual(fetch_rating_counts(Mock(), []), {})


class TeacherExistsTests(unittest.TestCase):
    def test_true_for_teacher_node(self):
        body = {"data": {"node": {"__typename": "Teacher", "id": "T1"}}}
        self.assertIs(teacher_exists(collector_with([body]), "T1"), True)

    def test_false_when_node_null(self):
        body = {"data": {"node": None}}
        self.assertIs(teacher_exists(collector_with([body]), "T1"), False)

    def test_none_cases(self):
        cases = [
            requests.HTTPError("500"),
            requests.Timeout(),
            {"errors": [{"message": "x"}], "data": {"node": None}},
            {"data": None},
            {"data": {}},
            ["not", "a", "dict"],
            {"data": {"node": {"__typename": "School"}}},
        ]
        for case in cases:
            with self.subTest(case=repr(case)):
                self.assertIsNone(teacher_exists(collector_with([case]), "T1"))

    def test_raise_for_status_error_is_none(self):
        r = Mock(raise_for_status=Mock(side_effect=requests.HTTPError("403")))
        c = Mock(url="u", headers={}, cookies={})
        c.session.post.return_value = r
        self.assertIsNone(teacher_exists(c, "T1"))


class DeletedReviewIdsTests(unittest.TestCase):
    def test_multi_page_success(self):
        c = collector_with(
            [
                ratings_page(["r1", "r2"], 3, True, "c1"),
                ratings_page(["r3"], 3),
            ]
        )
        self.assertEqual(deleted_review_ids(c, "T1", {"r1", "r3", "gone"}), {"gone"})
        calls = c.session.post.call_args_list
        self.assertEqual(calls[1].kwargs["json"]["variables"]["cursor"], "c1")

    def test_nothing_deleted_is_empty_set(self):
        c = collector_with([ratings_page(["r1", "r2"], 2)])
        self.assertEqual(deleted_review_ids(c, "T1", {"r1", "r2"}), set())

    def test_short_listing_is_none(self):
        c = collector_with([ratings_page(["r1"], 2)])
        self.assertIsNone(deleted_review_ids(c, "T1", {"r1", "r2"}))

    def test_listing_longer_than_count_is_trusted(self):
        # Live RMP: numRatings 753 while the list had 754 ratings.
        c = collector_with([ratings_page(["r1", "r2", "r3"], 2)])
        self.assertEqual(deleted_review_ids(c, "T1", {"r1", "r4"}), {"r4"})

    def test_node_null_is_none(self):
        c = collector_with([{"data": {"node": None}}])
        self.assertIsNone(deleted_review_ids(c, "T1", {"r1"}))

    def test_non_advancing_cursor_is_none(self):
        c = collector_with(
            [
                ratings_page(["r1"], 2, True, "c1"),
                ratings_page(["r2"], 2, True, "c1"),
            ]
        )
        self.assertIsNone(deleted_review_ids(c, "T1", {"r1"}))

    def test_failures_are_none(self):
        c = collector_with([requests.Timeout()])
        self.assertIsNone(deleted_review_ids(c, "T1", {"r1"}))
        c = collector_with(
            [ratings_page(["r1"], 2, True, "c1"), requests.HTTPError("500")]
        )
        self.assertIsNone(deleted_review_ids(c, "T1", {"r1"}))
        c = collector_with([{"errors": [{"message": "x"}]}])
        self.assertIsNone(deleted_review_ids(c, "T1", {"r1"}))
        c = collector_with([{"data": {"node": {"id": "T1"}}}])
        self.assertIsNone(deleted_review_ids(c, "T1", {"r1"}))


if __name__ == "__main__":
    unittest.main()
