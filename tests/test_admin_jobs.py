"""Isolated PostgreSQL integration tests. Never uses the application DB setting.

Set ADMIN_JOBS_TEST_DATABASE_URL explicitly to a disposable PostgreSQL instance.
Every run creates and drops its own unique schema.
"""

import os
from pathlib import Path
import unittest
import uuid
from unittest.mock import patch
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker
from aggiermp.core import admin_jobs, admin_worker
from aggiermp.core.summary_lock import summary_execution_lock, SummaryBusyError

URL = os.environ.get("ADMIN_JOBS_TEST_DATABASE_URL")


class WorkerLoggingTests(unittest.TestCase):
    def test_exception_diagnostics_omit_messages_and_query_data(self):
        class DatabaseFailure(Exception):
            pgcode = "42703"

        sensitive = "confidential review text"
        for error, detail in (
            (
                ModuleNotFoundError(sensitive, name="sentence_transformers"),
                "missing module sentence_transformers",
            ),
            (
                DBAPIError(sensitive, {"value": sensitive}, DatabaseFailure(sensitive)),
                "SQLSTATE 42703",
            ),
        ):
            with self.subTest(detail=detail), self.assertLogs(admin_worker.logger) as log:
                try:
                    raise error
                except Exception as exc:
                    admin_worker.log_rebuild_exception("sample-job", exc)
            output = log.output[0]
            self.assertIn("sample-job", output)
            self.assertIn(type(error).__name__, output)
            self.assertIn("test_admin_jobs.py:", output)
            self.assertIn(detail, output)
            self.assertNotIn(sensitive, output)


@unittest.skipUnless(URL, "Requires explicit isolated ADMIN_JOBS_TEST_DATABASE_URL")
class JobIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = "admin_jobs_test_" + uuid.uuid4().hex
        cls.root = create_engine(URL)
        with cls.root.begin() as connection:
            connection.execute(text(f"CREATE SCHEMA {cls.schema}"))
        cls.engine = create_engine(
            URL, connect_args={"options": f"-csearch_path={cls.schema}"}
        )
        cls.sessions = sessionmaker(bind=cls.engine)
        with cls.engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TABLE professors(id text PRIMARY KEY, first_name text, last_name text)"
                )
            )
            connection.execute(
                text(
                    "CREATE TABLE professor_aliases(alias_id text PRIMARY KEY, canonical_id text NOT NULL, status text NOT NULL)"
                )
            )
            connection.execute(
                text(
                    "CREATE TABLE reviews(id text PRIMARY KEY, professor_id text NOT NULL)"
                )
            )
            connection.execute(
                text(
                    "CREATE TABLE professor_summaries_new(id serial PRIMARY KEY, professor_id text NOT NULL)"
                )
            )
        cls.apply_migration()

    @classmethod
    def apply_migration(cls):
        migration = (
            Path(__file__).parents[1]
            / "src/aggiermp/database/migrations/004_admin_jobs.sql"
        )
        with cls.engine.connect() as connection:
            connection.exec_driver_sql(migration.read_text())
            connection.commit()

    @classmethod
    def tearDownClass(cls):
        cls.engine.dispose()
        with cls.root.begin() as connection:
            connection.execute(text(f"DROP SCHEMA {cls.schema} CASCADE"))
        cls.root.dispose()

    def setUp(self):
        self.db = self.sessions()
        self.db.execute(
            text(
                "TRUNCATE admin_summary_jobs,admin_summary_rebuilds,professor_aliases,professors,reviews,professor_summaries_new CASCADE"
            )
        )
        self.db.execute(
            text(
                "INSERT INTO professors(id,first_name) VALUES('a','Alias'),('c','Canonical'),('p','Other')"
            )
        )
        self.db.commit()

    def tearDown(self):
        self.db.close()

    def test_merge_undo_and_rollback_stale_markers(self):
        self.db.execute(
            text("INSERT INTO professor_aliases VALUES('a','c','proposed')")
        )
        self.db.commit()
        self.assertEqual(admin_jobs.pending(self.db), [])
        self.db.execute(text("UPDATE professor_aliases SET status='confirmed'"))
        self.db.rollback()
        self.assertEqual(admin_jobs.pending(self.db), [])
        self.db.execute(text("UPDATE professor_aliases SET status='confirmed'"))
        self.db.commit()
        self.assertEqual([p["professorId"] for p in admin_jobs.pending(self.db)], ["c"])
        self.db.execute(text("UPDATE professor_aliases SET status='rejected'"))
        self.db.commit()
        self.assertEqual(
            {p["professorId"] for p in admin_jobs.pending(self.db)}, {"a", "c"}
        )
        revision = self.db.execute(
            text("SELECT revision FROM admin_summary_rebuilds WHERE professor_id='c'")
        ).scalar()
        self.assertEqual(revision, 2)

    def test_deleting_confirmed_alias_marks_both_professors(self):
        self.db.execute(
            text("INSERT INTO professor_aliases VALUES('a','c','confirmed')")
        )
        self.db.execute(text("DELETE FROM admin_summary_rebuilds"))
        self.db.commit()
        self.db.execute(text("DELETE FROM professor_aliases"))
        self.db.commit()
        self.assertEqual(
            {p["professorId"] for p in admin_jobs.pending(self.db)}, {"a", "c"}
        )
        # Deleting an unreviewed proposal changes nothing.
        self.db.execute(text("DELETE FROM admin_summary_rebuilds"))
        self.db.execute(
            text("INSERT INTO professor_aliases VALUES('a','c','proposed')")
        )
        self.db.execute(text("DELETE FROM professor_aliases"))
        self.db.commit()
        self.assertEqual(admin_jobs.pending(self.db), [])

    def test_reapplying_migration_does_not_remark_cleared_merges(self):
        self.db.execute(
            text("INSERT INTO professor_aliases VALUES('a','c','confirmed')")
        )
        self.db.execute(text("DELETE FROM admin_summary_rebuilds"))
        self.db.commit()
        type(self).apply_migration()
        type(self).apply_migration()
        self.assertEqual(admin_jobs.pending(self.db), [])

    def test_reassigning_confirmed_alias_marks_old_and_new_canonical(self):
        self.db.execute(
            text("INSERT INTO professor_aliases VALUES('a','c','confirmed')")
        )
        self.db.execute(text("DELETE FROM admin_summary_rebuilds"))
        self.db.commit()
        self.db.execute(text("UPDATE professor_aliases SET canonical_id='p'"))
        self.db.commit()
        self.assertEqual(
            {p["professorId"] for p in admin_jobs.pending(self.db)}, {"c", "p"}
        )

    def test_dedup_alias_normalization_overlap_and_invalid_ids(self):
        self.db.execute(
            text("INSERT INTO professor_aliases VALUES('a','c','confirmed')")
        )
        self.db.commit()
        first = admin_jobs.enqueue(self.db, ["a"])
        self.assertEqual(first["job"]["professorIds"], ["c"])
        second = admin_jobs.enqueue(self.db, ["c"])
        self.assertTrue(second["deduplicated"])
        self.assertEqual(first["job"]["id"], second["job"]["id"])
        with self.assertRaises(ValueError):
            admin_jobs.enqueue(self.db, ["c", "p"])
        self.db.rollback()
        with self.assertRaises(ValueError):
            admin_jobs.enqueue(self.db, ["missing"])
        self.db.rollback()

    def test_lock_excludes_second_connection_and_releases(self):
        from aggiermp.core.summary_lock import _held

        with summary_execution_lock(self.engine):
            token = _held.set(False)  # emulate another process/context
            try:
                with self.assertRaises(SummaryBusyError):
                    with summary_execution_lock(self.engine):
                        pass
            finally:
                _held.reset(token)
        with summary_execution_lock(self.engine):
            pass

    def test_completed_professor_can_be_requeued_while_batch_runs(self):
        first = admin_jobs.enqueue(self.db, ["a", "c"])["job"]
        self.db.execute(
            text(
                "UPDATE admin_summary_jobs SET status='running',completed_ids='[\"a\"]'::jsonb"
            )
        )
        self.db.commit()
        second = admin_jobs.enqueue(self.db, ["a"])
        self.assertFalse(second["deduplicated"])
        self.assertNotEqual(first["id"], second["job"]["id"])

    def prepare_rebuild(self):
        self.db.execute(
            text("INSERT INTO professor_aliases VALUES('a','c','confirmed')")
        )
        self.db.execute(text("INSERT INTO reviews VALUES('r','a')"))
        self.db.commit()

    def test_rebuild_quota_preserves_pending_and_does_not_clear_cache(self):
        from pipelines.professors import upsert_reviews_and_summaries as runner

        self.prepare_rebuild()
        with patch.object(
            runner,
            "upsert_reviews_and_summaries",
            return_value={"quota_exhausted": True},
        ), patch.object(admin_worker, "clear_summary_cache") as cache:
            self.assertEqual(admin_worker.rebuild_one(self.db, "c"), "quota_exhausted")
        cache.assert_not_called()
        self.assertEqual(len(admin_jobs.pending(self.db)), 1)

    def test_cache_failure_preserves_pending(self):
        from pipelines.professors import upsert_reviews_and_summaries as runner

        self.prepare_rebuild()
        with patch.object(
            runner,
            "upsert_reviews_and_summaries",
            return_value={"summaries_generated": 1},
        ), patch.object(
            admin_worker, "clear_summary_cache", side_effect=RuntimeError("unavailable")
        ):
            with self.assertRaises(RuntimeError):
                admin_worker.rebuild_one(self.db, "c")
        self.assertEqual(len(admin_jobs.pending(self.db)), 1)

    def test_merge_changed_during_rebuild_stays_pending(self):
        from pipelines.professors import upsert_reviews_and_summaries as runner

        self.prepare_rebuild()

        def generate(**kwargs):
            with self.sessions() as other:
                other.execute(text("UPDATE professor_aliases SET status='rejected'"))
                other.commit()
            return {"summaries_generated": 1}

        with patch.object(
            runner, "upsert_reviews_and_summaries", side_effect=generate
        ), patch.object(admin_worker, "clear_summary_cache"):
            self.assertEqual(admin_worker.rebuild_one(self.db, "c"), "stale")
        self.assertEqual(
            {p["professorId"] for p in admin_jobs.pending(self.db)}, {"a", "c"}
        )

    def test_no_reviews_removes_obsolete_summary(self):
        from pipelines.professors import upsert_reviews_and_summaries as runner

        self.db.execute(
            text("INSERT INTO admin_summary_rebuilds(professor_id) VALUES('c')")
        )
        self.db.execute(
            text("INSERT INTO professor_summaries_new(professor_id) VALUES('c')")
        )
        self.db.commit()
        with patch.object(
            runner, "upsert_reviews_and_summaries"
        ) as generate, patch.object(admin_worker, "clear_summary_cache"):
            self.assertEqual(admin_worker.rebuild_one(self.db, "c"), "succeeded")
        generate.assert_not_called()
        self.assertEqual(
            self.db.execute(
                text("SELECT count(*) FROM professor_summaries_new")
            ).scalar(),
            0,
        )
        self.assertEqual(admin_jobs.pending(self.db), [])

    def run_worker(self, result):
        with patch.object(
            admin_worker, "create_db_engine", return_value=self.engine
        ), patch.object(
            admin_worker, "get_session", side_effect=self.sessions
        ), patch.object(
            admin_worker, "ensure_cache_available"
        ), patch.object(
            admin_worker, "rebuild_one", side_effect=result
        ) as rebuild:
            admin_worker.run_once()
            return rebuild

    def test_cache_failure_stops_remaining_professors(self):
        job = admin_jobs.enqueue(self.db, ["a", "c"])["job"]
        rebuild = self.run_worker([admin_worker.CacheUnavailableError("unavailable")])
        self.assertEqual(rebuild.call_count, 1)
        job = admin_jobs.get_job(self.db, job["id"])
        self.assertEqual(job["status"], "failed")
        self.assertIn("Cache invalidation failed", job["error"])

    def test_cache_preflight_failure_does_not_generate(self):
        job = admin_jobs.enqueue(self.db, ["a"])["job"]
        with patch.object(
            admin_worker, "create_db_engine", return_value=self.engine
        ), patch.object(
            admin_worker, "get_session", side_effect=self.sessions
        ), patch.object(
            admin_worker,
            "ensure_cache_available",
            side_effect=admin_worker.CacheUnavailableError("unavailable"),
        ), patch.object(
            admin_worker, "rebuild_one"
        ) as rebuild:
            admin_worker.run_once()
        rebuild.assert_not_called()
        self.assertEqual(admin_jobs.get_job(self.db, job["id"])["status"], "failed")

    def test_quota_stops_and_retry_only_unfinished(self):
        job = admin_jobs.enqueue(self.db, ["a", "c", "p"])["job"]
        rebuild = self.run_worker(["succeeded", "quota_exhausted"])
        self.assertEqual(rebuild.call_count, 2)
        job = admin_jobs.get_job(self.db, job["id"])
        self.assertEqual(job["status"], "quota_exhausted")
        self.assertEqual(job["completedIds"], ["a"])
        from aggiermp.api.routers.admin_jobs import retry

        next_job = retry(uuid.UUID(job["id"]), self.db)["job"]
        self.assertEqual(next_job["professorIds"], ["c", "p"])

    def test_interrupted_job_resumes_without_completed_professors(self):
        job = admin_jobs.enqueue(self.db, ["a", "c"])["job"]
        self.db.execute(
            text(
                "UPDATE admin_summary_jobs SET status='running', completed_ids='[\"a\"]'::jsonb"
            )
        )
        self.db.commit()
        rebuild = self.run_worker(["succeeded"])
        self.assertEqual(rebuild.call_args.args[1], "c")
        job = admin_jobs.get_job(self.db, job["id"])
        self.assertEqual(job["status"], "succeeded")
        self.assertEqual(job["completedIds"], ["a", "c"])

    def test_new_revision_cannot_be_cleared_by_old_completion(self):
        self.db.execute(
            text("INSERT INTO professor_aliases VALUES('a','c','confirmed')")
        )
        self.db.commit()
        old = (
            self.db.execute(
                text("SELECT * FROM admin_summary_rebuilds WHERE professor_id='c'")
            )
            .mappings()
            .one()
        )
        self.db.execute(text("UPDATE professor_aliases SET status='rejected'"))
        self.db.commit()
        result = self.db.execute(
            text(
                "DELETE FROM admin_summary_rebuilds WHERE professor_id=:professor_id AND revision=:revision AND changed_at=:changed_at"
            ),
            old,
        )
        self.assertEqual(result.rowcount, 0)


if __name__ == "__main__":
    unittest.main()
