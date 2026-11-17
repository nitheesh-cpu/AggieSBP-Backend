"""DELETE /users/me and the user-data coverage guard (run with --noconftest)."""

import asyncio
import re
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from aggiermp.api.routers import users
from aggiermp.core.user_data import USER_DATA_TABLES, release_trackers
from aggiermp.database.base import Base

SRC = Path(__file__).resolve().parent.parent / "src" / "aggiermp"
COVERED = {t for t, _ in USER_DATA_TABLES}


class FakeDB:
    def __init__(self, fail_on=None, missing=()):
        self.statements = []
        self.commits = 0
        self.rollbacks = 0
        self.fail_on = fail_on
        self.missing = set(missing)

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        self.statements.append((sql, params))
        if sql.startswith("SELECT to_regclass"):
            absent = params["t"] in self.missing
            return SimpleNamespace(scalar=lambda: None if absent else params["t"])
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("boom")
        if "FROM user_tracked_sections" in sql:
            rows = [("202631-12345-CSCE-121-501", "202631"), ("202631_777", "202631")]
            return SimpleNamespace(fetchall=lambda: rows, rowcount=2)
        return SimpleNamespace(rowcount=1)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class FakeRedis:
    def __init__(self):
        self.sets = {
            "trackers:202631:12345": {"u1", "u2"},
            "trackers:202631:777": {"u1"},
            "tracked_sections": {"202631:12345", "202631:777"},
        }

    async def srem(self, key, member):
        self.sets.get(key, set()).discard(member)

    async def scard(self, key):
        return len(self.sets.get(key, set()))


def call(db, **patches):
    sess = SimpleNamespace(get_user_id=lambda: "u1")
    return asyncio.run(users.delete_my_account(session=sess, db=db))


class DeleteAccountTests(unittest.TestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.st_delete = AsyncMock()
        self.revoke = AsyncMock()
        for p in (
            patch.object(users, "st_delete_user", self.st_delete),
            patch.object(users, "revoke_all_sessions_for_user", self.revoke),
            patch.object(users, "get_redis_or_none", AsyncMock(return_value=self.redis)),
        ):
            p.start()
            self.addCleanup(p.stop)

    def test_deletes_every_table_in_one_transaction_then_supertokens(self):
        db = FakeDB()
        out = call(db)
        deleted = [s for s, _ in db.statements if s.startswith("DELETE FROM")]
        self.assertEqual(len(deleted), len(USER_DATA_TABLES))
        for table in COVERED:
            self.assertTrue(any(f"DELETE FROM {table} " in s for s in deleted), table)
        self.assertTrue(all(p == {"u": "u1"} for s, p in db.statements if s.startswith("DELETE")))
        self.assertEqual(db.commits, 1)
        self.st_delete.assert_awaited_once_with("u1")
        self.revoke.assert_awaited_once_with("u1")
        self.assertEqual(out["status"], "deleted")

    def test_redis_trackers_removed_and_shared_watch_kept(self):
        call(FakeDB())
        self.assertEqual(self.redis.sets["trackers:202631:12345"], {"u2"})
        self.assertIn("202631:12345", self.redis.sets["tracked_sections"])  # u2 still watching
        self.assertNotIn("202631:777", self.redis.sets["tracked_sections"])  # last tracker gone

    def test_db_failure_rolls_back_and_touches_nothing_else(self):
        db = FakeDB(fail_on="DELETE FROM user_planner_state")
        with self.assertRaises(HTTPException) as cm:
            call(db)
        self.assertEqual(cm.exception.status_code, 500)
        self.assertEqual((db.commits, db.rollbacks), (0, 1))
        self.st_delete.assert_not_awaited()
        self.assertIn("u1", self.redis.sets["trackers:202631:12345"])

    def test_supertokens_failure_reports_and_retry_is_safe(self):
        self.st_delete.side_effect = RuntimeError("core down")
        with self.assertRaises(HTTPException) as cm:
            call(FakeDB())
        self.assertEqual(cm.exception.status_code, 502)
        self.assertIn("saved data was deleted", cm.exception.detail)
        self.revoke.assert_not_awaited()
        # Retry: rows already gone, SuperTokens healthy again.
        self.st_delete.side_effect = None
        self.assertEqual(call(FakeDB())["status"], "deleted")

    def test_redis_failure_does_not_block_deletion(self):
        with patch.object(users, "release_trackers", AsyncMock(side_effect=RuntimeError("x"))):
            self.assertEqual(call(FakeDB())["status"], "deleted")
        self.st_delete.assert_awaited_once()

    def test_missing_table_is_skipped(self):
        db = FakeDB(missing={"user_planner_state"})
        call(db)
        self.assertFalse(any("DELETE FROM user_planner_state" in s for s, _ in db.statements))

    def test_release_trackers_swallows_errors(self):
        class Broken:
            async def srem(self, *a):
                raise RuntimeError("down")

        asyncio.run(release_trackers(Broken(), [("u1", "202631", "1")]))
        asyncio.run(release_trackers(None, [("u1", "202631", "1")]))


class UserDataCoverageTests(unittest.TestCase):
    """Fails when a table holding user ids is added without being deleted on account deletion."""

    def test_orm_tables_with_user_id_are_covered(self):
        orm = {t.name for t in Base.metadata.tables.values() if "user_id" in t.columns}
        self.assertTrue(orm)
        self.assertEqual(orm - COVERED, set(), "add to USER_DATA_TABLES in core/user_data.py")

    def test_migration_tables_with_user_id_are_covered(self):
        found = set()
        for f in (SRC / "database" / "migrations").glob("*.sql"):
            for m in re.finditer(
                r"CREATE TABLE(?: IF NOT EXISTS)?\s+(\w+)\s*\((.*?)\n\);", f.read_text(encoding="utf-8"), re.S
            ):
                if re.search(r"^\s*user_id\b", m.group(2), re.M):
                    found.add(m.group(1))
        self.assertTrue(found)
        self.assertEqual(found - COVERED, set(), "add to USER_DATA_TABLES in core/user_data.py")

    def test_tables_written_with_user_id_in_code_are_covered(self):
        # Catches tables created outside base.py/migrations (user_schedules, user_tracked_sections).
        found = set()
        for f in SRC.rglob("*.py"):
            for m in re.finditer(r"INSERT INTO\s+(\w+)\s*\(([^)]*)\)", f.read_text(encoding="utf-8")):
                if re.search(r"\buser_id\b", m.group(2)):
                    found.add(m.group(1))
        self.assertTrue({"user_schedules", "user_tracked_sections"} <= found)
        self.assertEqual(found - COVERED, set(), "add to USER_DATA_TABLES in core/user_data.py")


if __name__ == "__main__":
    unittest.main()
