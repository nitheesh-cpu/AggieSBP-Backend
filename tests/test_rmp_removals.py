"""RMP removal watch: profile misses, hiding, recovery, review removals.

No database, no network: a fake session answers the SQL, RMP lookups are mocked.
"""

import unittest
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pipelines.professors import review_sync
from pipelines.professors import rmp_removals as rr
from pipelines.professors import upsert_reviews_and_summaries as runner

from test_fetch_only import FakeSession, reviews_for

TODAY = date(2026, 11, 25)
YESTERDAY = TODAY - timedelta(days=1)


class Result:
    def __init__(self, rows=(), scalar=None):
        self.rows = list(rows)
        self._scalar = scalar
        self.rowcount = 1

    def fetchall(self):
        return self.rows

    def scalar(self):
        return self._scalar


class FakeDB:
    def __init__(self, stored=None, rows=None, hidden=None, tables=None):
        self.stored = stored or {}  # school -> set(ids)
        self.rows = rows or {}  # id -> dict(misses, last, action)
        self.hidden = hidden or {}  # id -> reason
        self.tables = (
            {"rmp_missing_profiles", "hidden_professors", "professor_aliases"}
            if tables is None
            else tables
        )
        self.writes = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, clause, params=None):
        sql = str(clause)
        params = params or {}
        if "to_regclass" in sql:
            return Result(scalar=params["name"] in self.tables)
        if sql.lstrip().startswith("SELECT") and "FROM rmp_missing_profiles" in sql:
            return Result(
                [
                    (pid, r["misses"], datetime.combine(r["last"], datetime.min.time()), r["action"])
                    for pid, r in self.rows.items()
                ]
            )
        if "FROM hidden_professors" in sql:
            return Result(list(self.hidden.items()))
        if "FROM professors WHERE university_id" in sql:
            return Result([(i,) for i in self.stored.get(params["school"], ())])
        if "first_name" in sql:
            return Result([])
        if "FROM professor_aliases" in sql:
            return Result(scalar=None)
        self.writes.append((sql, params))
        if sql.startswith("INSERT INTO rmp_missing_profiles"):
            self.rows[params["id"]] = {"misses": 1, "last": params["ts"].date(), "action": None}
        elif "SET misses" in sql:
            self.rows[params["id"]].update(misses=params["misses"], last=params["ts"].date())
        elif "SET action" in sql:
            self.rows[params["id"]]["action"] = params["action"]
        elif sql.startswith("DELETE FROM rmp_missing_profiles"):
            self.rows.pop(params["id"], None)
        return Result()

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def collector_factory():
    return SimpleNamespace(session=Mock())


class Env:
    """Patches the RMP lookups and the hide helpers for one test."""

    def __init__(self, exists=None, groups=None, hidden_ids=None):
        self.exists = exists or {}  # id -> True/False/None; default False
        self.groups = groups or {}
        self.hide = Mock(return_value={"groupIds": [], "deleted": {}})
        self.unhide = Mock(return_value=None)
        self.delete = Mock(return_value={})
        self.queue = Mock(return_value=1)
        self.hidden_ids = hidden_ids or set()

    def __enter__(self):
        self.patches = [
            patch.object(
                review_sync,
                "teacher_exists",
                side_effect=lambda c, pid: self.exists.get(pid, False),
            ),
            patch.object(rr, "hidden_professor_ids", side_effect=lambda db: set(self.hidden_ids)),
            patch.object(
                rr, "merge_group", side_effect=lambda db, pid: self.groups.get(pid, {pid})
            ),
            patch.object(rr, "hide_professor", self.hide),
            patch.object(rr, "unhide_professor", self.unhide),
            patch.object(rr, "delete_stored_content", self.delete),
            patch.object(runner, "rebuild_queue_available", return_value=True),
            patch.object(runner, "queue_summary_rebuilds", self.queue),
        ]
        for p in self.patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self.patches:
            p.stop()


def listing(*ids):
    return {i: 5 for i in ids}


def check(db, listings, env, apply=True, dry_run=False, today=TODAY):
    return rr.check_removed_profiles(
        db,
        listings,
        apply=apply,
        dry_run=dry_run,
        today=today,
        collector_factory=collector_factory,
    )


def school_db(missing=("gone",), live=("a", "b"), **kw):
    return FakeDB(stored={"S": set(live) | set(missing) | set(kw.pop("extra", ()))}, **kw)


class RemovalWatchTests(unittest.TestCase):
    def test_first_miss_records_row_only(self):
        db = school_db()
        with Env(exists={"a": True}) as env:
            res = check(db, {"S": listing("a", "b")}, env)
        self.assertEqual(db.rows["gone"]["misses"], 1)
        self.assertEqual(res["profiles_missing"], 1)
        env.hide.assert_not_called()

    def test_miss_counts_once_per_day(self):
        db = school_db(rows={"gone": {"misses": 1, "last": TODAY, "action": None}})
        with Env(exists={"a": True}) as env:
            check(db, {"S": listing("a", "b")}, env)
        self.assertEqual(db.rows["gone"]["misses"], 1)
        env.hide.assert_not_called()

    def test_second_day_miss_hides_with_auto_reason(self):
        db = school_db(rows={"gone": {"misses": 1, "last": YESTERDAY, "action": None}})
        env = Env(exists={"a": True})
        env.hide.return_value = {"groupIds": ["gone"], "deleted": {}}
        with env:
            res = check(db, {"S": listing("a", "b")}, env)
        env.hide.assert_called_once_with(db, "gone", rr.AUTO_REASON)
        self.assertEqual(db.rows["gone"]["misses"], 2)
        self.assertEqual(db.rows["gone"]["action"], "hidden")
        self.assertEqual(res["profiles_hidden"], 1)
        self.assertIn("gone", res["affected_ids"])
        self.assertIn("gone", res["hidden_ids"])

    def test_no_action_without_apply(self):
        db = school_db(rows={"gone": {"misses": 1, "last": YESTERDAY, "action": None}})
        with Env(exists={"a": True}) as env:
            res = check(db, {"S": listing("a", "b")}, env, apply=False)
        env.hide.assert_not_called()
        self.assertEqual(db.rows["gone"]["misses"], 2)  # still recorded
        self.assertIsNone(db.rows["gone"]["action"])
        self.assertEqual(res["profiles_hidden"], 0)

    def test_dry_run_writes_nothing(self):
        db = school_db(rows={"gone": {"misses": 1, "last": YESTERDAY, "action": None}})
        with Env(exists={"a": True}) as env:
            check(db, {"S": listing("a", "b")}, env, apply=True, dry_run=True)
        env.hide.assert_not_called()
        self.assertEqual(db.writes, [])
        self.assertEqual(db.commits, 0)
        self.assertEqual(db.rows["gone"]["misses"], 1)

    def test_dry_run_does_not_create_rows(self):
        db = school_db()
        with Env(exists={"a": True}) as env:
            check(db, {"S": listing("a", "b")}, env, dry_run=True)
        self.assertEqual(db.rows, {})
        self.assertEqual(db.writes, [])

    def test_teacher_exists_true_clears_row(self):
        db = school_db(rows={"gone": {"misses": 1, "last": YESTERDAY, "action": None}})
        with Env(exists={"a": True, "gone": True}) as env:
            res = check(db, {"S": listing("a", "b")}, env)
        self.assertNotIn("gone", db.rows)
        self.assertEqual(res["profiles_missing"], 0)
        env.hide.assert_not_called()

    def test_lookup_error_changes_nothing(self):
        db = school_db(rows={"gone": {"misses": 1, "last": YESTERDAY, "action": None}})
        with Env(exists={"a": True, "gone": None}) as env:
            res = check(db, {"S": listing("a", "b")}, env)
        self.assertEqual(db.rows["gone"]["misses"], 1)
        self.assertEqual(res["profiles_missing"], 0)
        env.hide.assert_not_called()

    def test_cap_skips_school_with_error(self):
        missing = [f"m{i}" for i in range(26)]
        db = school_db(missing=missing)
        with Env(exists={"a": True}) as env:
            res = check(db, {"S": listing("a", "b")}, env)
        self.assertEqual(res["schools_skipped"], 1)
        self.assertEqual(len(res["errors"]), 1)
        self.assertEqual(db.rows, {})
        env.hide.assert_not_called()

    def test_cap_scales_with_school_size(self):
        self.assertEqual(rr.candidate_cap(100), 25)
        self.assertEqual(rr.candidate_cap(5000), 100)

    def test_control_failure_skips_school(self):
        db = school_db()
        with Env(exists={"a": False}) as env:  # control "a" fails
            res = check(db, {"S": listing("a", "b")}, env)
        self.assertEqual(res["schools_skipped"], 1)
        self.assertEqual(len(res["errors"]), 1)
        self.assertEqual(db.rows, {})

    def test_empty_listing_skips_school(self):
        db = school_db()
        with Env() as env:
            res = check(db, {"S": {}}, env)
        self.assertEqual(res["schools_skipped"], 1)
        self.assertEqual(db.rows, {})

    def test_merged_group_with_live_member_deletes_content_only(self):
        db = school_db(
            live=("a", "b", "canon"),
            rows={"gone": {"misses": 1, "last": YESTERDAY, "action": None}},
        )
        with Env(exists={"a": True}, groups={"gone": {"gone", "canon"}}) as env:
            res = check(db, {"S": listing("a", "b", "canon")}, env)
        env.hide.assert_not_called()
        env.delete.assert_called_once_with(db, ["gone"])
        env.queue.assert_called_once()
        self.assertEqual(env.queue.call_args.args[1], [("gone", rr.MERGED_REASON, True)])
        self.assertEqual(db.rows["gone"]["action"], "content_deleted")
        self.assertEqual(res["profiles_content_deleted"], 1)
        self.assertEqual(res["hidden_ids"], set())

    def test_merged_group_all_gone_is_hidden(self):
        db = school_db(rows={"gone": {"misses": 1, "last": YESTERDAY, "action": None}})
        with Env(exists={"a": True}, groups={"gone": {"gone", "other"}}) as env:
            check(db, {"S": listing("a", "b")}, env)
        env.hide.assert_called_once()
        env.delete.assert_not_called()

    def test_manual_hide_is_untouched(self):
        db = school_db(
            hidden={"gone": "taken down by request"},
            rows={"gone": {"misses": 1, "last": YESTERDAY, "action": None}},
        )
        with Env(exists={"a": True}, hidden_ids={"gone"}) as env:
            res = check(db, {"S": listing("a", "b")}, env)
        env.hide.assert_not_called()
        self.assertEqual(res["profiles_missing"], 0)

    def test_recovery_unhides_only_auto_hides(self):
        db = school_db(
            missing=(),
            hidden={"auto": rr.AUTO_REASON, "manual": "by request"},
            rows={
                "auto": {"misses": 2, "last": YESTERDAY, "action": "hidden"},
                "manual": {"misses": 2, "last": YESTERDAY, "action": None},
            },
            extra=("auto", "manual"),
        )
        env = Env(exists={"a": True})
        env.unhide.return_value = {"auto"}
        with env:
            res = check(db, {"S": listing("a", "b", "auto", "manual")}, env)
        env.unhide.assert_called_once_with(db, "auto")
        self.assertEqual(res["profiles_restored"], 1)
        self.assertNotIn("auto", db.rows)
        self.assertNotIn("manual", db.rows)  # row cleared, hide left alone
        self.assertIn("auto", res["affected_ids"])

    def test_recovery_without_apply_does_not_unhide(self):
        db = school_db(
            missing=(),
            hidden={"auto": rr.AUTO_REASON},
            extra=("auto",),
        )
        with Env(exists={"a": True}) as env:
            res = check(db, {"S": listing("a", "b", "auto")}, env, apply=False)
        env.unhide.assert_not_called()
        self.assertEqual(res["profiles_restored"], 0)

    def test_missing_tables_warn_and_return(self):
        db = school_db(tables=set())
        with Env() as env:
            res = check(db, {"S": listing("a", "b")}, env)
        self.assertEqual(res["errors"], [])
        self.assertEqual(db.writes, [])

    def test_failed_action_rolls_back_and_continues(self):
        db = school_db(
            missing=("g1", "g2"),
            rows={
                "g1": {"misses": 1, "last": YESTERDAY, "action": None},
                "g2": {"misses": 1, "last": YESTERDAY, "action": None},
            },
        )
        env = Env(exists={"a": True})
        env.hide.side_effect = [RuntimeError("db"), {"groupIds": ["g2"], "deleted": {}}]
        with env:
            res = check(db, {"S": listing("a", "b")}, env)
        self.assertEqual(db.rollbacks, 1)
        self.assertEqual(len(res["errors"]), 1)
        self.assertEqual(res["profiles_hidden"], 1)


class WiringTests(unittest.TestCase):
    def run_full(self, stored, listings, apply=False, dry_run=False, professor_ids=None,
                 gone=None, check_result=None):
        session = FakeSession(["p", "q"], stored, {})
        session.execute = Mock()
        session.commit = Mock()
        session.rollback = Mock()
        fetched = []

        def fetch(pid, _e):
            fetched.append(pid)
            return pid, []

        collector = SimpleNamespace(session=Mock())
        base = {
            "profiles_missing": 0,
            "profiles_hidden": 0,
            "profiles_content_deleted": 0,
            "profiles_restored": 0,
            "errors": [],
            "affected_ids": set(),
            "hidden_ids": set(),
        }
        base.update(check_result or {})
        with patch.object(runner, "load_aliases", return_value={}), patch.object(
            runner, "hidden_professor_ids", return_value=set()
        ), patch.object(
            runner.review_sync, "fetch_school_listings", return_value=listings
        ) as fl, patch.object(
            runner, "fetch_rating_counts", return_value={}
        ) as frc, patch(
            "pipelines.professors.rmp_removals.check_removed_profiles", return_value=base
        ) as chk, patch.object(
            runner, "get_new_reviews_for_professor", side_effect=fetch
        ), patch.object(
            runner, "batch_upsert_reviews"
        ), patch.object(
            runner, "rebuild_queue_available", return_value=True
        ), patch.object(
            runner, "queue_summary_rebuilds", side_effect=lambda s, plan: len(plan)
        ) as queue, patch.object(
            runner.review_sync, "deleted_review_ids", return_value=gone
        ) as dri, patch.object(
            runner, "review_sync_collector", return_value=collector
        ), patch(
            "aggiermp.core.cache_invalidation.invalidate_professors",
            return_value=SimpleNamespace(keys_deleted=0, revalidation="ok"),
        ) as inv, patch(
            "pipelines.professors.upsert_professors.upsert_all_professors",
            side_effect=lambda uid, s: {"new": 0},
        ):
            res = runner.fetch_reviews_and_queue_rebuilds(
                session=session,
                professor_ids=professor_ids,
                apply_removals=apply,
                dry_run=dry_run,
                discover_professors=False,
            )
        return SimpleNamespace(
            res=res, session=session, fetched=fetched, fl=fl, frc=frc, chk=chk,
            dri=dri, queue=queue, inv=inv, collector=collector,
        )

    def test_removal_check_runs_on_full_runs_with_flags(self):
        r = self.run_full([], {"S": {"p": 0, "q": 0}}, apply=True)
        r.chk.assert_called_once()
        self.assertTrue(r.chk.call_args.kwargs["apply"])
        self.assertFalse(r.chk.call_args.kwargs["dry_run"])
        r.frc.assert_not_called()

    def test_default_is_report_mode(self):
        r = self.run_full([], {"S": {"p": 0, "q": 0}})
        self.assertFalse(r.chk.call_args.kwargs["apply"])

    def test_targeted_run_skips_removal_watch(self):
        r = self.run_full([], {"S": {"p": 0}}, professor_ids=["p"], apply=True)
        r.chk.assert_not_called()
        r.fl.assert_not_called()
        r.frc.assert_called_once()
        r.dri.assert_not_called()

    def test_newly_hidden_not_fetched_and_ids_invalidated(self):
        r = self.run_full(
            [],
            {"S": {"p": 3, "q": 3}},
            apply=True,
            check_result={
                "profiles_hidden": 1,
                "hidden_ids": {"q"},
                "affected_ids": {"q"},
                "errors": ["boom"],
            },
        )
        self.assertEqual(r.fetched, ["p"])
        self.assertEqual(r.res["profiles_hidden"], 1)
        self.assertEqual(r.res["errors"], ["boom"])
        self.assertIn("q", r.inv.call_args.args[0])

    def test_review_deletion_with_apply(self):
        stored = reviews_for("p", 4)
        r = self.run_full(stored, {"S": {"p": 3, "q": 0}}, apply=True, gone={"p-0"})
        r.dri.assert_called_once()
        self.assertEqual(r.dri.call_args.args[1], "p")
        sql, params = r.session.execute.call_args.args
        self.assertIn("DELETE FROM reviews", str(sql))
        self.assertEqual(params, {"pid": "p", "ids": ["p-0"]})
        self.assertEqual(r.res["reviews_removed"], 1)
        queued = [row for c in r.queue.call_args_list for row in c.args[1]]
        self.assertIn(("p", "reviews removed on RMP", True), queued)
        self.assertIn("p", r.inv.call_args.args[0])
        r.collector.session.close.assert_called_once()

    def test_review_removal_report_only_without_apply(self):
        stored = reviews_for("p", 4)
        r = self.run_full(stored, {"S": {"p": 3, "q": 0}}, gone={"p-0"})
        r.dri.assert_called_once()
        r.session.execute.assert_not_called()
        self.assertEqual(r.res["reviews_removed"], 0)

    def test_review_removal_dry_run_writes_nothing(self):
        stored = reviews_for("p", 4)
        r = self.run_full(stored, {"S": {"p": 3, "q": 0}}, apply=True, dry_run=True, gone={"p-0"})
        r.session.execute.assert_not_called()
        self.assertEqual(r.res["reviews_removed"], 0)

    def test_review_cap_skips_big_loss(self):
        stored = reviews_for("p", 10)
        gone = {f"p-{i}" for i in range(6)}
        r = self.run_full(stored, {"S": {"p": 4, "q": 0}}, apply=True, gone=gone)
        r.session.execute.assert_not_called()
        self.assertEqual(r.res["reviews_removed"], 0)
        self.assertEqual(len(r.res["errors"]), 1)

    def test_small_professor_may_lose_most_reviews(self):
        stored = reviews_for("p", 4)
        gone = {"p-0", "p-1", "p-2"}
        r = self.run_full(stored, {"S": {"p": 1, "q": 0}}, apply=True, gone=gone)
        self.assertEqual(r.res["reviews_removed"], 3)

    def test_unreadable_review_list_changes_nothing(self):
        stored = reviews_for("p", 4)
        r = self.run_full(stored, {"S": {"p": 3, "q": 0}}, apply=True, gone=None)
        r.session.execute.assert_not_called()
        self.assertEqual(r.res["errors"], [])

    def test_equal_counts_never_open_a_collector(self):
        stored = reviews_for("p", 3)
        r = self.run_full(stored, {"S": {"p": 3, "q": 0}}, apply=True)
        r.dri.assert_not_called()


class CliTests(unittest.TestCase):
    def run_main(self, argv):
        with patch("sys.argv", ["prog", *argv]), patch.object(
            runner, "fetch_reviews_and_queue_rebuilds", return_value={"errors": []}
        ) as fetch, patch.object(runner, "configure_console_logging"):
            runner.main()
        return fetch

    def test_apply_removals_requires_fetch_only(self):
        with patch("sys.argv", ["prog", "--apply-removals"]), patch.object(
            runner, "configure_console_logging"
        ):
            with self.assertRaises(SystemExit) as ctx:
                runner.main()
        self.assertEqual(ctx.exception.code, 2)

    def test_flag_is_passed_through(self):
        fetch = self.run_main(["--fetch-only", "--apply-removals"])
        self.assertTrue(fetch.call_args.kwargs["apply_removals"])

    def test_default_is_off(self):
        fetch = self.run_main(["--fetch-only"])
        self.assertFalse(fetch.call_args.kwargs["apply_removals"])


if __name__ == "__main__":
    unittest.main()
