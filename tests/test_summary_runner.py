import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pipelines.professors import upsert_reviews_and_summaries as runner


class RunnerTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(runner, "fetch_rating_counts", return_value={})
        self.counts = patcher.start()
        self.addCleanup(patcher.stop)

    def test_quota_stop_preserves_checkpoint_and_does_not_save_fallback(self):
        session = Mock()
        query = Mock()
        query.all.return_value = []
        query.filter_by.return_value.all.return_value = [
            SimpleNamespace(
                id="r",
                professor_id="p",
                course_code="CS101",
                review_text="Synthetic clear lecture review.",
                clarity_rating=4,
                difficulty_rating=3,
                helpful_rating=4,
                rating_tags=[],
                would_take_again=1,
                grade="A",
                attendance_mandatory="mandatory",
                textbook_use=3,
                review_date=None,
            )
        ]
        session.query.side_effect = lambda *args: (
            SimpleNamespace(
                all=lambda: [
                    SimpleNamespace(id="done"),
                    SimpleNamespace(id="p"),
                    SimpleNamespace(id="q"),
                ]
            )
            if len(args) == 1 and args[0] is runner.ProfessorDB
            else query
        )
        pipeline = Mock()
        pipeline.summarizer.tamu_client = None
        pipeline.summarizer.fallback_clusters = 0
        pipeline.process_professor_batch.side_effect = runner.TamuAIQuotaExceeded(
            "Budget exhausted"
        )
        with patch.object(
            runner, "HierarchicalSummarizationPipeline", return_value=pipeline
        ), patch.object(runner, "load_checkpoint", return_value={"done"}), patch.object(
            runner, "save_checkpoint"
        ) as save, patch.object(
            runner, "clear_checkpoint"
        ) as clear, patch.object(
            runner,
            "get_new_reviews_for_professor",
            side_effect=lambda pid, _: (pid, []),
        ), patch.object(
            runner, "upsert_professor_summary"
        ) as upsert:
            result = runner.upsert_reviews_and_summaries(
                session=session, skip_if_no_new_reviews=False
            )
        self.assertTrue(result["quota_exhausted"])
        self.assertEqual(result["summaries_generated"], 0)
        pipeline.process_professor_batch.assert_called_once()
        save.assert_called_with({"done"})
        clear.assert_not_called()
        upsert.assert_not_called()

    def test_checkpoint_atomic_replace_preserves_previous_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.json"
            with patch.object(runner, "CHECKPOINT_FILE", path):
                runner.save_checkpoint({"old"})
                with patch.object(
                    runner.os, "replace", side_effect=OSError("simulated failure")
                ):
                    runner.save_checkpoint({"new"})
                self.assertEqual(runner.load_checkpoint(), {"old"})
                self.assertEqual(list(Path(directory).iterdir()), [path])

    def run_professor(
        self, force, stored=False, actual=1, summarized=1, percent=10, fetched=False
    ):
        review = SimpleNamespace(
            id="r",
            professor_id="p",
            course_code="CS101",
            review_text="A useful synthetic review about clear lectures.",
            clarity_rating=4,
            difficulty_rating=3,
            helpful_rating=4,
            rating_tags=[],
            would_take_again=1,
            grade="A",
            attendance_mandatory="mandatory",
            textbook_use=3,
            review_date=None,
        )
        session = Mock()
        session.query.return_value.all.return_value = [
            ("p", f"r{i}") for i in range(actual)
        ]
        session.query.return_value.filter_by.return_value.all.return_value = [
            review
        ] * actual
        session.query.return_value.filter.return_value.all.return_value = (
            [
                SimpleNamespace(course_code=None, total_reviews=summarized),
                SimpleNamespace(course_code="CS101", total_reviews=summarized),
            ]
            if summarized is not None
            else []
        )
        pipeline = Mock()
        pipeline.summarizer.tamu_client = None
        pipeline.summarizer.fallback_clusters = 0
        pipeline.process_professor_reviews.return_value = SimpleNamespace(
            course_summaries=[], overall_sentiment="positive", confidence=0.8
        )
        pipeline.process_professor_batch.side_effect = lambda jobs: iter(
            [("p", pipeline.process_professor_reviews.return_value, None)]
            if jobs
            else []
        )
        with patch.object(
            runner, "HierarchicalSummarizationPipeline", return_value=pipeline
        ), patch.object(
            runner,
            "get_new_reviews_for_professor",
            return_value=("p", [review] if fetched else []),
        ) as fetch, patch.object(
            runner, "upsert_professor_summary", return_value=True
        ), patch.object(
            runner, "batch_upsert_reviews"
        ):
            result = runner.upsert_reviews_and_summaries(
                professor_ids=["p"],
                session=session,
                skip_if_no_new_reviews=not force,
                stored_reviews_only=stored,
                min_new_review_percent=percent,
            )
        self.fetch_calls = fetch.call_count
        return result, pipeline

    def test_bulk_matching_count_skips_fetch_but_force_still_summarizes(self):
        self.counts.return_value = {"p": 1}
        result, _ = self.run_professor(True)
        self.assertEqual(self.fetch_calls, 0)
        self.assertEqual(result["summaries_generated"], 1)

    def test_stored_only_never_contacts_rmp(self):
        result, _ = self.run_professor(True, stored=True)
        self.counts.assert_not_called()
        self.assertEqual(self.fetch_calls, 0)
        self.assertEqual(result["summaries_generated"], 1)

    def test_force_update_regenerates_matching_review_counts(self):
        result, pipeline = self.run_professor(True)
        self.assertEqual(result["summaries_generated"], 1)
        self.assertEqual(len(pipeline.process_professor_batch.call_args.args[0]), 1)

    def test_incremental_run_skips_matching_review_counts(self):
        result, pipeline = self.run_professor(False)
        self.assertEqual(result["summaries_generated"], 0)
        self.assertEqual(pipeline.process_professor_batch.call_args.args[0], [])

    def test_threshold_uses_accumulated_reviews_and_rounds_up(self):
        for old, current, expected in [
            (100, 101, 0),
            (100, 109, 0),
            (100, 110, 1),
            (10, 11, 1),
            (11, 12, 0),
            (11, 13, 1),
        ]:
            with self.subTest(old=old, current=current):
                result, _ = self.run_professor(
                    False, stored=True, actual=current, summarized=old
                )
                self.assertEqual(result["summaries_generated"], expected)

    def test_threshold_also_applies_to_freshly_fetched_reviews(self):
        result, _ = self.run_professor(False, actual=101, summarized=100, fetched=True)
        self.assertEqual(result["summaries_generated"], 0)
        self.assertEqual(result["professors_skipped"], 1)

    def test_missing_summary_force_and_disabled_threshold(self):
        for force, old, percent in [
            (False, None, 10),
            (True, 100, 10),
            (False, 100, 0),
        ]:
            with self.subTest(force=force, old=old, percent=percent):
                result, _ = self.run_professor(
                    force, stored=True, actual=101, summarized=old, percent=percent
                )
                self.assertEqual(result["summaries_generated"], 1)


if __name__ == "__main__":
    unittest.main()
