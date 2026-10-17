import unittest
from unittest.mock import Mock
from pipelines.professors.review_sync import needs_review_fetch, school_rating_counts


def page(nodes, more=False, cursor=None):
    return {
        "data": {
            "search": {
                "teachers": {
                    "edges": [{"node": node} for node in nodes],
                    "pageInfo": {"hasNextPage": more, "endCursor": cursor},
                }
            }
        }
    }


def node(pid, count, school="s"):
    return {"id": pid, "numRatings": count, "school": {"id": school}}


class ReviewSyncTests(unittest.TestCase):
    def collector(self, pages):
        collector = Mock(url="https://example.test", headers={}, cookies={})
        collector.session.post.side_effect = [
            Mock(json=Mock(return_value=body)) for body in pages
        ]
        return collector

    def test_exact_equality_and_initial_bootstrap(self):
        for local, remote, expected in [
            (10, 10, False),
            (10, 11, True),
            (10, 9, True),
            (10, 12, True),
            (10, 8, True),
            (0, 1, True),
            (0, 0, False),
            (10, None, True),
        ]:
            with self.subTest(local=local, remote=remote):
                self.assertEqual(needs_review_fetch(local, remote), expected)

    def test_paginated_snapshot_filters_other_schools_and_invalid_counts(self):
        collector = self.collector(
            [
                page([node("p", 4)], True, "next"),
                page([node("q", 5), node("other", 6, "other"), node("invalid", None)]),
            ]
        )
        self.assertEqual(school_rating_counts(collector, "s"), {"p": 4, "q": 5})
        calls = collector.session.post.call_args_list
        self.assertEqual(calls[1].kwargs["json"]["variables"]["cursor"], "next")
        self.assertFalse(calls[0].kwargs["json"]["variables"]["query"]["fallback"])

    def test_graphql_error_is_not_an_empty_snapshot(self):
        with self.assertRaises(ValueError):
            school_rating_counts(
                self.collector([{"errors": [{"message": "failure"}]}]), "s"
            )

    def test_repeated_cursor_does_not_loop_forever(self):
        collector = self.collector([page([], True, "same"), page([], True, "same")])
        with self.assertRaises(ValueError):
            school_rating_counts(collector, "s")
        self.assertEqual(collector.session.post.call_count, 2)


if __name__ == "__main__":
    unittest.main()
