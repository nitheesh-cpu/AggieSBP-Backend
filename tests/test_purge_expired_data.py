"""Retention purge. SQL tests need an isolated Postgres: set PURGE_TEST_DATABASE_URL
(tables are dropped/created in it). Run with --noconftest."""

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipelines.maintenance import purge_expired_data as purge  # noqa: E402

URL = os.environ.get("PURGE_TEST_DATABASE_URL")

SCHEMA = """
DROP TABLE IF EXISTS push_notifications, user_tracked_sections, terms, admin_summary_jobs;
CREATE TABLE terms (term_code text PRIMARY KEY, end_date timestamp);
CREATE TABLE user_tracked_sections (id text PRIMARY KEY, user_id text, section_id text, term_code text,
  status text, created_at timestamptz DEFAULT now());
CREATE TABLE push_notifications (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), user_id text,
  sent_at timestamptz NOT NULL DEFAULT now(), status text DEFAULT 'sent');
CREATE TABLE admin_summary_jobs (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), status text,
  created_at timestamptz NOT NULL DEFAULT now());
"""


@unittest.skipUnless(URL, "Requires isolated PURGE_TEST_DATABASE_URL")
class PurgeSqlTests(unittest.TestCase):
    def setUp(self):
        engine = create_engine(URL)
        self.addCleanup(engine.dispose)
        self.db = sessionmaker(bind=engine)()
        self.addCleanup(self.db.close)
        self.db.execute(text(SCHEMA))
        self.db.commit()

    def q(self, sql):
        return self.db.execute(text(sql)).scalar()

    def seed(self):
        d = self.db
        d.execute(text("""
        INSERT INTO terms VALUES ('OLD', now() - interval '40 days'), ('RECENT', now() - interval '10 days'),
          ('FUTURE', now() + interval '60 days'), ('NODATE', NULL);
        INSERT INTO user_tracked_sections (id,user_id,section_id,term_code,created_at) VALUES
          ('a','u1','OLD-111-CSCE-121-501','OLD', now()),
          ('b','u1','RECENT-222-CSCE-121-502','RECENT', now()),
          ('c','u2','FUTURE-333-X-1-1','FUTURE', now()),
          ('d','u2','NODATE-444-X-1-1','NODATE', now() - interval '400 days'),
          ('e','u2','NODATE-555-X-1-1','NODATE', now() - interval '10 days'),
          ('f','u2','GONE-666-X-1-1','GONE', now() - interval '10 days');
        INSERT INTO push_notifications (user_id, sent_at) VALUES
          ('u1', now() - interval '181 days'), ('u1', now() - interval '179 days'), ('u1', now());
        INSERT INTO admin_summary_jobs (status, created_at)
          SELECT 'succeeded', now() - interval '200 days' - n * interval '1 day' FROM generate_series(1,130) n;
        INSERT INTO admin_summary_jobs (status, created_at) VALUES ('queued', now() - interval '300 days');
        """))
        d.commit()

    def test_dry_run_counts_and_deletes_nothing(self):
        self.seed()
        s = purge.run(self.db, dry_run=True)
        self.assertEqual((s["push_notifications"], s["user_tracked_sections"]), (1, 2))
        self.assertEqual(self.q("SELECT count(*) FROM push_notifications"), 3)
        self.assertEqual(self.q("SELECT count(*) FROM user_tracked_sections"), 6)
        self.assertEqual(self.q("SELECT count(*) FROM admin_summary_jobs"), 131)

    def test_live_run_deletes_only_expired(self):
        self.seed()
        with patch.object(purge, "_release") as rel:
            s = purge.run(self.db)
        self.assertEqual(s["errors"], [])
        self.assertEqual(self.q("SELECT count(*) FROM push_notifications"), 2)
        self.assertEqual(
            sorted(r[0] for r in self.db.execute(text("SELECT id FROM user_tracked_sections"))),
            ["b", "c", "e", "f"],
        )
        # 130 old finished jobs, newest 100 (by created_at) kept, queued job never touched
        self.assertEqual(s["admin_summary_jobs"], 30)
        self.assertEqual(self.q("SELECT count(*) FROM admin_summary_jobs WHERE status='queued'"), 1)
        self.assertEqual(self.q("SELECT count(*) FROM admin_summary_jobs"), 101)
        rel.assert_called_once()

    def test_batches(self):
        self.seed()
        with patch.object(purge, "BATCH_SIZE", 1), patch.object(purge, "BATCH_PAUSE_SECONDS", 0), patch.object(purge, "_release"):
            purge.run(self.db)
        self.assertEqual(self.q("SELECT count(*) FROM push_notifications"), 2)
        self.assertEqual(self.q("SELECT count(*) FROM admin_summary_jobs"), 101)

    def test_missing_tables_are_skipped(self):
        self.db.execute(text("DROP TABLE push_notifications, admin_summary_jobs"))
        self.db.commit()
        s = purge.run(self.db)
        self.assertEqual(s["errors"], [])


class PurgeExitCodeTests(unittest.TestCase):
    def test_failure_returns_nonzero_and_other_rules_still_run(self):
        calls = []

        class Db:
            def execute(self, *a, **k):
                return None

            def rollback(self):
                calls.append("rollback")

            def close(self):
                pass

        with patch.object(purge, "purge_push_log", side_effect=RuntimeError("db down")), \
             patch.object(purge, "purge_expired_watches", return_value=(0, [])), \
             patch.object(purge, "purge_admin_jobs", return_value=0), \
             patch.object(purge, "get_session", return_value=Db()), \
             patch.object(sys, "argv", ["purge"]):
            self.assertEqual(purge.main(), 1)
        self.assertEqual(calls, ["rollback"])

    def test_success_returns_zero(self):
        class Db:
            def execute(self, *a, **k):
                return None

            def close(self):
                pass

        with patch.object(purge, "purge_push_log", return_value=3), \
             patch.object(purge, "purge_expired_watches", return_value=(0, [])), \
             patch.object(purge, "purge_admin_jobs", return_value=0), \
             patch.object(purge, "get_session", return_value=Db()), \
             patch.object(sys, "argv", ["purge", "--dry-run"]):
            self.assertEqual(purge.main(), 0)


if __name__ == "__main__":
    unittest.main()
