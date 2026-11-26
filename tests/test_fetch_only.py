"""--fetch-only: store new RMP reviews, clear caches, queue summary rebuilds.

The RMP collector, database session and cache are mocked. The queue SQL is
exercised for real when FETCH_ONLY_TEST_DATABASE_URL points at a disposable
PostgreSQL instance (each run uses its own schema).
"""

import logging
import os
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import Mock, patch

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from pipelines.professors import upsert_reviews_and_summaries as runner

URL = os.environ.get("FETCH_ONLY_TEST_DATABASE_URL")


class FakeQuery:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, *_):
        return self

    def all(self):
        return self.rows


class FakeSession:
    """Answers the handful of queries the fetch-only run makes."""

    def __init__(self, professors, reviews, summaries):
        self.professors = professors
        self.reviews = reviews  # [(professor_id, review_id)]
        self.summaries = summaries  # {professor_id: overall review count}

    def query(self, *args):
        if args[0] is runner.ProfessorDB:
            return FakeQuery([SimpleNamespace(id=p) for p in self.professors])
        if args[0] is runner.ReviewDB.professor_id:
            return FakeQuery(self.reviews)
        return FakeQuery(list(self.summaries.items()))

    def close(self):
        pass


def review(professor_id, review_id):
    return SimpleNamespace(id=review_id, professor_id=professor_id)


def reviews_for(professor_id, count, prefix=None):
    return [(professor_id, f"{prefix or professor_id}-{i}") for i in range(count)]


class FetchOnlyTests(unittest.TestCase):
    def run_fetch(
        self,
        professors,
        stored,
        summaries,
        new,
        aliases=None,
        dry_run=False,
        queue_available=True,
        fetch_errors=(),
        percent=10.0,
        discovery=None,
    ):
        session = FakeSession(professors, stored, summaries)
        pipeline = Mock()
        patches = {
            "pipeline": patch.object(
                runner, "HierarchicalSummarizationPipeline", pipeline
            ),
            "summary_save": patch.object(runner, "upsert_professor_summary"),
            "aliases": patch.object(runner, "load_aliases", return_value=aliases or {}),
            "counts": patch.object(runner, "fetch_rating_counts", return_value={}),
            "hidden": patch.object(runner, "hidden_professor_ids", return_value=set()),
            # A full run first imports professors new on RMP; never hit RMP here.
            "discover": patch(
                "pipelines.professors.upsert_professors.upsert_all_professors",
                side_effect=discovery or (lambda uid, session: {"new": 0}),
            ),
            "fetch": patch.object(
                runner,
                "get_new_reviews_for_professor",
                side_effect=self.fetcher(new, fetch_errors),
            ),
            "upsert": patch.object(runner, "batch_upsert_reviews"),
            "available": patch.object(
                runner, "rebuild_queue_available", return_value=queue_available
            ),
            "queue": patch.object(
                runner, "queue_summary_rebuilds", side_effect=lambda s, plan: len(plan)
            ),
            "invalidate": patch(
                "aggiermp.core.cache_invalidation.invalidate_professors",
                return_value=SimpleNamespace(keys_deleted=3, revalidation="ok"),
            ),
        }
        mocks = {name: p.start() for name, p in patches.items()}
        for p in patches.values():
            self.addCleanup(p.stop)
        result = runner.fetch_reviews_and_queue_rebuilds(
            session=session, dry_run=dry_run, min_new_review_percent=percent
        )
        return result, mocks

    @staticmethod
    def fetcher(new, errors):
        def fetch(professor_id, _existing):
            if professor_id in errors:
                raise RuntimeError("rmp down")
            return professor_id, new.get(professor_id, [])

        return fetch

    @staticmethod
    def queued(mocks):
        return {
            pid: (reason, bump)
            for call in mocks["queue"].call_args_list
            for pid, reason, bump in call.args[1]
        }

    def test_stores_reviews_and_never_calls_ai(self):
        result, m = self.run_fetch(
            ["p"],
            reviews_for("p", 10),
            {"p": 10},
            {"p": [review("p", "n1"), review("p", "n2")]},
        )
        stored = m["upsert"].call_args.args[1]
        self.assertEqual([r.id for r in stored], ["n1", "n2"])
        self.assertEqual(result["reviews_added"], 2)
        m["pipeline"].assert_not_called()
        m["summary_save"].assert_not_called()

    def test_threshold_decides_who_is_queued(self):
        stored = reviews_for("big", 100) + reviews_for("small", 100)
        new = {
            "big": [review("big", f"b{i}") for i in range(12)],  # 12%: queued
            "small": [review("small", f"s{i}") for i in range(5)],  # 5%: deferred
        }
        result, m = self.run_fetch(
            ["big", "small"], stored, {"big": 100, "small": 100}, new
        )
        self.assertEqual(self.queued(m), {"big": ("12 new reviews (+12%)", True)})
        self.assertEqual(result["professors_queued"], 1)

    def test_threshold_percent_is_configurable(self):
        new = {"p": [review("p", "n1")]}
        _, strict = self.run_fetch(["p"], reviews_for("p", 100), {"p": 100}, new)
        self.assertEqual(self.queued(strict), {})
        _, any_review = self.run_fetch(
            ["p"], reviews_for("p", 100), {"p": 100}, new, percent=0
        )
        self.assertEqual(self.queued(any_review), {"p": ("1 new review (+1%)", True)})

    def test_professor_without_summary_is_queued_even_without_new_reviews(self):
        result, m = self.run_fetch(
            ["fresh", "empty"], reviews_for("fresh", 8), {}, {}
        )
        self.assertEqual(self.queued(m), {"fresh": ("No summary yet (8 reviews)", False)})
        self.assertEqual(result["professors_queued"], 1)

    def test_merged_alias_reviews_queue_canonical_id(self):
        stored = reviews_for("canon", 20) + reviews_for("alias", 10)
        new = {"alias": [review("alias", f"a{i}") for i in range(6)]}  # 6/30 = 20%
        result, m = self.run_fetch(
            ["canon", "alias"],
            stored,
            {"canon": 30},
            new,
            aliases={"alias": "canon"},
        )
        # canonical total is 36 against 30 summarized; the alias is never queued
        self.assertEqual(self.queued(m), {"canon": ("6 new reviews (+20%)", True)})
        # the cache is cleared for the entry that actually got the reviews
        self.assertEqual(set(m["invalidate"].call_args.args[0]), {"alias"})

    def test_cache_invalidated_for_professors_with_new_reviews_only(self):
        stored = reviews_for("a", 10) + reviews_for("b", 10)
        _, m = self.run_fetch(
            ["a", "b"],
            stored,
            {"a": 10, "b": 10},
            {"a": [review("a", "x")]},
        )
        self.assertEqual(set(m["invalidate"].call_args.args[0]), {"a"})

    def test_no_new_reviews_means_no_invalidation(self):
        _, m = self.run_fetch(["a"], reviews_for("a", 10), {"a": 10}, {})
        m["invalidate"].assert_not_called()

    def test_dry_run_writes_nothing(self):
        result, m = self.run_fetch(
            ["p", "fresh"],
            reviews_for("p", 10) + reviews_for("fresh", 3),
            {"p": 10},
            {"p": [review("p", "n1"), review("p", "n2")]},
            dry_run=True,
        )
        m["upsert"].assert_not_called()
        m["queue"].assert_not_called()
        m["invalidate"].assert_not_called()
        m["pipeline"].assert_not_called()
        self.assertEqual(result["reviews_added"], 2)
        self.assertEqual(result["professors_queued"], 2)  # counted, not written

    def test_full_run_imports_new_rmp_professors_first(self):
        # Allison Goetz (RMP 5108901) was created after the last professor
        # import, so no course page could show her.
        result, mocks = self.run_fetch(
            ["p1"], {}, {}, {},
            discovery=lambda uid, session: {"new": 2},
        )
        self.assertEqual(mocks["discover"].call_count, 2)  # both campuses
        self.assertEqual(result["professors_added"], 4)
        self.assertEqual(result["errors"], [])

    def test_professor_import_failure_is_reported_and_reviews_still_fetched(self):
        result, mocks = self.run_fetch(
            ["p1"], {}, {}, {},
            discovery=lambda uid, session: {"new": 0, "error": "rmp down"},
        )
        self.assertEqual(len(result["errors"]), 2)
        self.assertIn("Professor import failed", result["errors"][0])
        self.assertEqual(result["professors_checked"], 1)

    def test_dry_run_does_not_import_professors(self):
        _, mocks = self.run_fetch(["p1"], {}, {}, {}, dry_run=True)
        mocks["discover"].assert_not_called()

    def test_missing_table_warns_once_and_does_not_fail(self):
        with self.assertLogs(runner.logger, level=logging.WARNING) as logs:
            result, m = self.run_fetch(
                ["p", "q"],
                reviews_for("p", 10),
                {"p": 10},
                {
                    "p": [review("p", f"n{i}") for i in range(3)],
                    "q": [review("q", "n9")],
                },
                queue_available=False,
            )
        self.assertEqual(len(logs.records), 1)
        m["queue"].assert_not_called()
        self.assertEqual(result["reviews_added"], 4)
        self.assertEqual(result["professors_queued"], 0)
        self.assertEqual(result["errors"], [])
        self.assertNotIn("error", result)

    def test_fetch_failure_is_reported_and_others_continue(self):
        result, m = self.run_fetch(
            ["bad", "good"],
            reviews_for("good", 10),
            {"good": 10},
            {"good": [review("good", f"n{i}") for i in range(2)]},
            fetch_errors={"bad"},
        )
        self.assertEqual(len(result["errors"]), 1)
        self.assertEqual(result["reviews_added"], 2)
        self.assertEqual(set(self.queued(m)), {"good"})

    def test_main_exits_nonzero_on_errors(self):
        with patch.object(
            runner,
            "fetch_reviews_and_queue_rebuilds",
            return_value={"errors": ["boom"]},
        ), patch("sys.argv", ["prog", "--fetch-only"]), patch.object(
            runner, "configure_console_logging"
        ):
            with self.assertRaises(SystemExit) as ctx:
                runner.main()
        self.assertEqual(ctx.exception.code, 1)

    def test_main_routes_flags(self):
        with patch.object(
            runner,
            "fetch_reviews_and_queue_rebuilds",
            return_value={"errors": []},
        ) as fetch, patch.object(runner, "upsert_reviews_and_summaries") as full, patch(
            "sys.argv",
            ["prog", "--fetch-only", "--dry-run", "--min-new-review-percent", "5"],
        ), patch.object(runner, "configure_console_logging"):
            runner.main()
        full.assert_not_called()
        self.assertTrue(fetch.call_args.kwargs["dry_run"])
        self.assertEqual(fetch.call_args.kwargs["min_new_review_percent"], 5.0)

    def test_fetch_only_is_not_wrapped_in_summary_lock(self):
        self.assertFalse(hasattr(runner.fetch_reviews_and_queue_rebuilds, "__wrapped__"))
        self.assertTrue(hasattr(runner.upsert_reviews_and_summaries, "__wrapped__"))


class PlanTests(unittest.TestCase):
    def test_uses_the_same_deferral_rule_as_the_full_pipeline(self):
        for summarized, total, percent, deferred in [
            (100, 109, 10, True),
            (100, 110, 10, False),
            (3, 4, 10, False),  # at least one review is always enough
            (100, 101, 0, False),
        ]:
            self.assertEqual(
                bool(runner.summary_deferral(summarized, total, percent)), deferred
            )
            plan = runner.plan_rebuilds({"p": total}, {"p": summarized}, {"p"}, percent)
            self.assertEqual(bool(plan), not deferred)

    def test_accumulated_deferred_reviews_queue_on_next_new_review(self):
        # 8 earlier deferred reviews plus 2 new ones cross 10% of 100.
        plan = runner.plan_rebuilds({"p": 110}, {"p": 100}, {"p"}, 10)
        self.assertEqual(plan, [("p", "10 new reviews (+10%)", True)])


@unittest.skipUnless(URL, "Requires explicit isolated FETCH_ONLY_TEST_DATABASE_URL")
class QueueSqlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = "fetch_only_test_" + uuid.uuid4().hex
        root = create_engine(URL)
        with root.begin() as connection:
            connection.execute(text(f"CREATE SCHEMA {cls.schema}"))
        cls.engine = create_engine(
            URL, connect_args={"options": f"-csearch_path={cls.schema}"}
        )
        cls.sessions = sessionmaker(bind=cls.engine)
        with cls.engine.begin() as connection:
            connection.execute(text("CREATE TABLE professors(id text PRIMARY KEY)"))
            connection.execute(
                text(
                    "CREATE TABLE professor_aliases(alias_id text PRIMARY KEY, "
                    "canonical_id text NOT NULL, status text NOT NULL)"
                )
            )

    @classmethod
    def tearDownClass(cls):
        cls.engine.dispose()
        root = create_engine(URL)
        with root.begin() as connection:
            connection.execute(text(f"DROP SCHEMA {cls.schema} CASCADE"))
        root.dispose()

    def setUp(self):
        with self.engine.begin() as connection:
            connection.execute(text("DROP TABLE IF EXISTS admin_summary_rebuilds"))

    def create_table(self):
        # Same definition as migration 004_admin_jobs.sql.
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO professors VALUES ('p1'),('p2') ON CONFLICT DO NOTHING"
                )
            )
            connection.execute(
                text(
                    "CREATE TABLE admin_summary_rebuilds ("
                    " professor_id text PRIMARY KEY REFERENCES professors(id) ON DELETE CASCADE,"
                    " revision bigint NOT NULL DEFAULT 1,"
                    " reason text NOT NULL DEFAULT 'Professor merge changed',"
                    " changed_at timestamptz NOT NULL DEFAULT now())"
                )
            )

    def rows(self):
        with self.engine.begin() as connection:
            return {
                r.professor_id: (r.revision, r.reason)
                for r in connection.execute(
                    text("SELECT * FROM admin_summary_rebuilds")
                )
            }

    def test_table_detection(self):
        with self.sessions() as session:
            self.assertFalse(runner.rebuild_queue_available(session))
        self.create_table()
        with self.sessions() as session:
            self.assertTrue(runner.rebuild_queue_available(session))

    def test_insert_then_bump_revision_on_conflict(self):
        self.create_table()
        with self.sessions() as session:
            runner.queue_summary_rebuilds(
                session, [("p1", "12 new reviews (+15%)", True)]
            )
        self.assertEqual(self.rows(), {"p1": (1, "12 new reviews (+15%)")})
        with self.engine.begin() as connection:
            before = connection.execute(
                text("SELECT changed_at FROM admin_summary_rebuilds")
            ).scalar()
        with self.sessions() as session:
            runner.queue_summary_rebuilds(session, [("p1", "20 new reviews (+25%)", True)])
        self.assertEqual(self.rows(), {"p1": (2, "20 new reviews (+25%)")})
        with self.engine.begin() as connection:
            after = connection.execute(
                text("SELECT changed_at FROM admin_summary_rebuilds")
            ).scalar()
        self.assertGreater(after, before)

    def test_no_bump_rows_leave_existing_entry_alone(self):
        self.create_table()
        with self.sessions() as session:
            runner.queue_summary_rebuilds(session, [("p1", "No summary yet (8 reviews)", False)])
            runner.queue_summary_rebuilds(session, [("p1", "No summary yet (9 reviews)", False)])
        self.assertEqual(self.rows(), {"p1": (1, "No summary yet (8 reviews)")})

    def test_failed_queue_rolls_back(self):
        self.create_table()
        with self.sessions() as session:
            with self.assertRaises(Exception):
                runner.queue_summary_rebuilds(
                    session,
                    [("p1", "ok", True), ("missing-professor", "fk violation", True)],
                )
        self.assertEqual(self.rows(), {})


if __name__ == "__main__":
    unittest.main()
