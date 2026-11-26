"""Hidden professors are skipped by pipelines and the summary worker.

Everything is mocked: no database, no network, no AI.
"""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from aggiermp.core import admin_jobs, admin_worker
from pipelines.professors import merge_duplicates
from pipelines.professors import upsert as upsert_module
from pipelines.professors import upsert_reviews_and_summaries as runner

from test_fetch_only import FakeSession, review, reviews_for

HIDDEN = {"h"}


def hidden_patch():
    return patch.object(runner, "hidden_professor_ids", return_value=set(HIDDEN))


class FetchOnlyHiddenTests(unittest.TestCase):
    def run_fetch(self, fetched):
        session = FakeSession(["p", "h"], reviews_for("h", 5), {})
        calls = []

        def fetch(professor_id, _existing):
            calls.append(professor_id)
            return professor_id, fetched.get(professor_id, [])

        with hidden_patch(), patch.object(
            runner, "load_aliases", return_value={}
        ), patch.object(
            runner, "fetch_rating_counts", return_value={}
        ), patch.object(
            runner, "get_new_reviews_for_professor", side_effect=fetch
        ), patch.object(
            runner, "batch_upsert_reviews"
        ) as upsert, patch.object(
            runner, "rebuild_queue_available", return_value=True
        ), patch.object(
            runner, "queue_summary_rebuilds", side_effect=lambda s, plan: len(plan)
        ) as queue, patch(
            "aggiermp.core.cache_invalidation.invalidate_professors",
            return_value=SimpleNamespace(keys_deleted=0, revalidation="ok"),
        ), patch(
            "pipelines.professors.upsert_professors.upsert_all_professors",
            side_effect=lambda uid, session: {"new": 0},
        ):
            result = runner.fetch_reviews_and_queue_rebuilds(session=session)
        queued = [row[0] for call in queue.call_args_list for row in call.args[1]]
        stored = [r.id for call in upsert.call_args_list for r in call.args[1]]
        return result, calls, stored, queued

    def test_hidden_professor_is_not_fetched_stored_or_queued(self):
        result, calls, stored, queued = self.run_fetch(
            {"p": [review("p", "n1")], "h": [review("h", "x1")]}
        )
        self.assertEqual(calls, ["p"])
        self.assertEqual(stored, ["n1"])
        self.assertNotIn("h", queued)
        self.assertEqual(result["hidden_skipped"], 1)
        self.assertEqual(result["professors_checked"], 1)
        self.assertEqual(result["errors"], [])

    def test_hidden_review_returned_by_a_fetch_is_not_inserted(self):
        _, _, stored, queued = self.run_fetch(
            {"p": [review("p", "n1"), review("h", "x1")]}
        )
        self.assertEqual(stored, ["n1"])
        self.assertNotIn("h", queued)

    def test_no_hidden_professors_changes_nothing(self):
        session = FakeSession(["p"], [], {})
        with patch.object(
            runner, "hidden_professor_ids", return_value=set()
        ), patch.object(runner, "load_aliases", return_value={}), patch.object(
            runner, "fetch_rating_counts", return_value={}
        ), patch.object(
            runner,
            "get_new_reviews_for_professor",
            side_effect=lambda pid, _e: (pid, []),
        ) as fetch, patch.object(
            runner, "rebuild_queue_available", return_value=False
        ):
            result = runner.fetch_reviews_and_queue_rebuilds(
                session=session, professor_ids=["p"]
            )
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(result["hidden_skipped"], 0)


class FullRunHiddenTests(unittest.TestCase):
    def run_full(self, ids, stored_only=True):
        session = Mock()
        session.query.return_value.all.return_value = [(i, f"r-{i}") for i in ids]
        pipeline = Mock()
        pipeline.summarizer.tamu_client = None
        pipeline.summarizer.fallback_clusters = 0
        sent = []

        def batch(jobs):
            sent.extend(job[0] if isinstance(job, tuple) else job for job in jobs)
            return iter([])

        pipeline.process_professor_batch.side_effect = batch
        with hidden_patch(), patch.object(
            runner, "load_aliases", return_value={}
        ), patch.object(
            runner, "HierarchicalSummarizationPipeline", return_value=pipeline
        ), patch.object(
            runner, "fetch_rating_counts", return_value={}
        ), patch.object(
            runner, "get_new_reviews_for_professor", side_effect=lambda p, _e: (p, [])
        ) as fetch, patch.object(
            runner, "upsert_professor_summary", return_value=True
        ), patch.object(
            runner, "batch_upsert_reviews"
        ):
            result = runner.upsert_reviews_and_summaries(
                professor_ids=ids,
                session=session,
                stored_reviews_only=stored_only,
                skip_if_no_new_reviews=False,
                resume=False,
                invalidate_cache=False,
            )
        return result, fetch, pipeline, sent

    def test_only_hidden_professor_means_nothing_to_do(self):
        result, fetch, pipeline, _ = self.run_full(["h"])
        self.assertEqual(result["hidden_skipped"], 1)
        self.assertEqual(fetch.call_count, 0)
        pipeline.process_professor_batch.assert_not_called()

    def test_hidden_professor_is_not_fetched_even_when_not_stored_only(self):
        result, fetch, _, _ = self.run_full(["p", "h"], stored_only=False)
        self.assertEqual(result["hidden_skipped"], 1)
        fetched = [call.args[0] for call in fetch.call_args_list]
        self.assertNotIn("h", fetched)

    def test_hidden_professor_is_not_sent_to_the_summarizer(self):
        result, _, _, sent = self.run_full(["p", "h"])
        self.assertEqual(result["hidden_skipped"], 1)
        self.assertNotIn("h", [str(job) for job in sent])
        self.assertTrue(all("h" != getattr(j, "professor_id", j) for j in sent))


class ProcessAllHiddenTests(unittest.TestCase):
    def test_process_all_professors_skips_hidden(self):
        session = Mock()
        session.query.return_value.filter_by.return_value.all.return_value = []
        pipeline = Mock()
        with patch(
            "aggiermp.core.hidden_professors.hidden_professor_ids",
            return_value=set(HIDDEN),
        ):
            results = upsert_module.process_all_professors(
                ["h", "p"], pipeline, session
            )
        asked = [
            call.kwargs["professor_id"]
            for call in session.query.return_value.filter_by.call_args_list
        ]
        self.assertEqual(asked, ["p"])
        self.assertEqual(results["hidden_skipped"], 1)
        pipeline.process_professor_reviews.assert_not_called()


class WorkerHiddenTests(unittest.TestCase):
    def test_rebuild_one_skips_hidden_without_ai_or_summary(self):
        db = Mock()
        db.execute.return_value.scalar.return_value = None  # no alias row
        with patch.object(
            admin_worker, "hidden_professor_ids", return_value=set(HIDDEN)
        ), patch(
            "pipelines.professors.upsert_reviews_and_summaries.upsert_reviews_and_summaries"
        ) as summarize, patch.object(
            admin_worker, "clear_summary_cache"
        ) as clear:
            outcome = admin_worker.rebuild_one(db, "h")
        self.assertEqual(outcome, "hidden")
        summarize.assert_not_called()
        clear.assert_not_called()
        db.query.assert_not_called()
        sql = str(db.execute.call_args_list[-1].args[0])
        self.assertIn("DELETE FROM admin_summary_rebuilds", sql)
        db.commit.assert_called()

    def test_rebuild_one_skips_alias_of_hidden_group(self):
        db = Mock()
        db.execute.return_value.scalar.return_value = "h"  # alias -> hidden canonical
        with patch.object(
            admin_worker, "hidden_professor_ids", return_value=set(HIDDEN)
        ), patch(
            "pipelines.professors.upsert_reviews_and_summaries.upsert_reviews_and_summaries"
        ) as summarize:
            self.assertEqual(admin_worker.rebuild_one(db, "alias"), "hidden")
        summarize.assert_not_called()


class AdminJobsHiddenTests(unittest.TestCase):
    def test_enqueue_refuses_hidden_professor(self):
        db = Mock()
        db.execute.return_value.all.return_value = []
        with patch.object(
            admin_jobs, "hidden_professor_ids", return_value=set(HIDDEN)
        ):
            with self.assertRaises(ValueError):
                admin_jobs.enqueue(db, ["h", "p"])
        inserts = [
            call
            for call in db.execute.call_args_list
            if "INSERT INTO admin_summary_jobs" in str(call.args[0])
        ]
        self.assertEqual(inserts, [])

    def test_pending_hides_hidden_professor(self):
        stamp = SimpleNamespace(isoformat=lambda: "t")
        rows = [
            {"professor_id": pid, "name": pid, "reason": "r", "changed_at": stamp, "revision": 1}
            for pid in ("h", "p")
        ]
        db = Mock()
        db.execute.return_value.mappings.return_value = rows
        with patch.object(
            admin_jobs, "hidden_professor_ids", return_value=set(HIDDEN)
        ):
            found = admin_jobs.pending(db)
        self.assertEqual([r["professorId"] for r in found], ["p"])


class MergeProposalHiddenTests(unittest.TestCase):
    def test_plan_leaves_hidden_professors_out(self):
        session = Mock()
        session.query.return_value.all.return_value = [
            SimpleNamespace(id="h"),
            SimpleNamespace(id="p"),
        ]
        with patch.object(
            merge_duplicates, "hidden_professor_ids", return_value=set(HIDDEN)
        ), patch.object(
            merge_duplicates, "load_profiles", return_value=[]
        ) as profiles, patch.object(
            merge_duplicates, "load_all_people", return_value=[]
        ), patch.object(
            merge_duplicates, "plan_profile_merges", return_value=([], [])
        ):
            _, _, by_id = merge_duplicates.plan(session)
        self.assertEqual(list(by_id), ["p"])
        self.assertEqual([p.id for p in profiles.call_args.args[1]], ["p"])


if __name__ == "__main__":
    unittest.main()
