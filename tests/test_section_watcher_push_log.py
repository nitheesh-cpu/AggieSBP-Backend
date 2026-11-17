"""Section watcher delivery logging (push_notifications) with a fake DB and push sender."""

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import requests
from pywebpush import WebPushException

from aggiermp.collectors import section_watcher as sw

SENT_EP = "https://fcm.googleapis.com/fcm/send/sent"
GONE_EP = "https://fcm.googleapis.com/fcm/send/gone"
NOT_FOUND_EP = "https://fcm.googleapis.com/fcm/send/notfound"
ERROR_EP = "https://fcm.googleapis.com/fcm/send/error"
TIMEOUT_EP = "https://fcm.googleapis.com/fcm/send/timeout"


def _response(status: int) -> requests.Response:
    # A real Response: it is falsy for 4xx/5xx, which the watcher must handle.
    res = requests.Response()
    res.status_code = status
    return res


def fake_webpush(subscription_info, data, **_kwargs):
    endpoint = subscription_info["endpoint"]
    if endpoint == GONE_EP:
        raise WebPushException("Push failed: 410 Gone", response=_response(410))
    if endpoint == NOT_FOUND_EP:
        raise WebPushException("Push failed: 404 Not Found", response=_response(404))
    if endpoint == ERROR_EP:
        raise WebPushException("Push failed: 500 Internal Server Error", response=_response(500))
    if endpoint == TIMEOUT_EP:
        raise requests.exceptions.ReadTimeout("read timed out")
    return _response(201)


def watch_row(track_id, user_id, *, is_open=True, notified=None, section_number="501"):
    return SimpleNamespace(
        track_id=track_id,
        user_id=user_id,
        section_id=f"202631-12345-CSCE-121-{section_number}",
        last_notified_open_at=notified,
        is_open=is_open,
        dept="CSCE",
        course_number="121",
        section_number=section_number,
        course_title="Intro to Program Design",
        crn="12345",
        term_code="202631",
        term_desc="Fall 2026 - College Station",
    )


def sub_row(user_id, endpoint):
    return SimpleNamespace(user_id=user_id, endpoint=endpoint, p256dh="p", auth="a")


class FakeResult:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []
        self._scalar = scalar

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar


class FakeDB:
    def __init__(self, watches, subs, *, table_exists=True, insert_error=None):
        self.watches = watches
        self.subs = subs
        self.table_exists = table_exists
        self.insert_error = insert_error
        self.calls = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        self.calls.append((sql, params))
        if "information_schema.columns" in sql:
            return FakeResult([(1,)])
        if sql.startswith("SELECT id, user_id, section_id, term_code, status FROM user_tracked_sections"):
            return FakeResult([])  # diagnostic dump
        if "FROM user_tracked_sections uts" in sql:
            return FakeResult(self.watches)
        if "FROM user_subscriptions" in sql:
            return FakeResult(self.subs)
        if "to_regclass" in sql:
            return FakeResult(scalar="push_notifications" if self.table_exists else None)
        if sql.startswith("INSERT INTO push_notifications") and self.insert_error:
            raise self.insert_error
        return FakeResult([])

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True

    def statements(self, prefix):
        return [(sql, params) for sql, params in self.calls if sql.startswith(prefix)]


class WatcherPushLogTests(unittest.TestCase):
    def run_watcher(self, db, sender=fake_webpush):
        with patch.object(sw, "VAPID_PRIVATE_KEY", "test-key"), \
             patch.object(sw, "_get_session", return_value=db), \
             patch.object(sw, "webpush", side_effect=sender) as push:
            summary = sw.run_watcher()
        return summary, push

    def test_logs_sent_expired_and_failed_in_one_batch(self):
        watches = [
            watch_row("t1", "u1"),
            watch_row("t2", "u2", section_number="502"),
            watch_row("t3", "u3", is_open=False),  # closed: not notified
        ]
        subs = [
            sub_row("u1", SENT_EP),
            sub_row("u1", GONE_EP),
            sub_row("u2", ERROR_EP),
            sub_row("u2", NOT_FOUND_EP),
        ]
        db = FakeDB(watches, subs)

        summary, push = self.run_watcher(db)

        self.assertNotIn("error", summary)
        self.assertEqual(summary["checked"], 3)
        self.assertEqual(summary["newly_open"], 2)
        self.assertEqual(summary["sent"], 1)
        self.assertEqual(summary["notified"], 1)
        self.assertEqual(summary["expired"], 2)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["no_subscription"], 0)
        self.assertEqual(summary["logged"], 4)

        inserts = db.statements("INSERT INTO push_notifications")
        self.assertEqual(len(inserts), 1, "one batched insert per run")
        records = inserts[0][1]
        by_status = {}
        for record in records:
            by_status.setdefault(record["status"], []).append(record)
        self.assertEqual(len(by_status["sent"]), 1)
        self.assertEqual(len(by_status["expired"]), 2)
        self.assertEqual(len(by_status["failed"]), 1)
        self.assertIsNone(by_status["sent"][0]["error"])
        self.assertEqual({r["error"] for r in by_status["expired"]}, {"HTTP 410", "HTTP 404"})
        self.assertTrue(by_status["failed"][0]["error"].startswith("HTTP 500"))

        sent = by_status["sent"][0]
        self.assertEqual(sent["user_id"], "u1")
        self.assertEqual(sent["track_id"], "t1")
        self.assertEqual(sent["course"], "CSCE 121.501")
        self.assertEqual(sent["term_code"], "202631")
        self.assertEqual(sent["crn"], "12345")
        self.assertIsNotNone(sent["sent_at"].tzinfo)
        self.assertEqual(len({r["id"] for r in records}), 4, "unique id per attempt")

        # The payload carries what the service worker needs, and the id matches the log row.
        payloads = {
            call.kwargs["subscription_info"]["endpoint"]: json.loads(call.kwargs["data"])
            for call in push.call_args_list
        }
        sent_payload = payloads[SENT_EP]
        self.assertEqual(sent_payload["course"], "CSCE 121.501")
        self.assertEqual(sent_payload["term"], "202631")
        self.assertEqual(sent_payload["notificationId"], sent["id"])
        self.assertNotIn("u1", json.dumps(sent_payload))

        # Only gone subscriptions are deleted; the 500 keeps its subscription.
        deletes = db.statements("DELETE FROM user_subscriptions")
        self.assertEqual(len(deletes), 1)
        self.assertCountEqual(deletes[0][1]["eps"], [GONE_EP, NOT_FOUND_EP])

        # The last_notified_open_at guard still stamps every newly-open watch.
        stamps = [p for s, p in db.statements("UPDATE user_tracked_sections") if "SET last_notified_open_at = :now" in s]
        self.assertEqual(len(stamps), 1)
        self.assertCountEqual(stamps[0]["ids"], ["t1", "t2"])
        self.assertTrue(db.closed)

    def test_network_error_is_failed_and_keeps_subscription(self):
        db = FakeDB([watch_row("t1", "u1")], [sub_row("u1", TIMEOUT_EP)])

        summary, _ = self.run_watcher(db)

        self.assertEqual((summary["sent"], summary["expired"], summary["failed"]), (0, 0, 1))
        record = db.statements("INSERT INTO push_notifications")[0][1][0]
        self.assertEqual(record["status"], "failed")
        self.assertIn("ReadTimeout", record["error"])
        self.assertEqual(db.statements("DELETE FROM user_subscriptions"), [])

    def test_missing_table_skips_logging_with_one_warning(self):
        db = FakeDB(
            [watch_row("t1", "u1"), watch_row("t2", "u2")],
            [sub_row("u1", SENT_EP), sub_row("u2", GONE_EP)],
            table_exists=False,
        )

        with self.assertLogs("section_watcher", level="WARNING") as logs:
            summary, push = self.run_watcher(db)

        self.assertNotIn("error", summary)
        self.assertEqual(push.call_count, 2, "notifications still go out")
        self.assertEqual((summary["sent"], summary["expired"], summary["logged"]), (1, 1, 0))
        self.assertEqual(db.statements("INSERT INTO push_notifications"), [])
        table_warnings = [m for m in logs.output if "push_notifications table missing" in m]
        self.assertEqual(len(table_warnings), 1)
        self.assertEqual(len(db.statements("DELETE FROM user_subscriptions")), 1)

    def test_insert_failure_is_swallowed(self):
        db = FakeDB(
            [watch_row("t1", "u1")],
            [sub_row("u1", SENT_EP)],
            insert_error=RuntimeError("permission denied for table push_notifications"),
        )

        summary, _ = self.run_watcher(db)

        self.assertNotIn("error", summary)
        self.assertEqual((summary["sent"], summary["logged"]), (1, 0))
        self.assertGreaterEqual(db.rollbacks, 1)
        # Stamping happened (and was committed) before the log write.
        self.assertTrue(any("SET last_notified_open_at = :now" in s for s, _ in db.calls))

    def test_user_without_subscription_is_counted_not_logged(self):
        db = FakeDB([watch_row("t1", "u1")], [])

        summary, push = self.run_watcher(db)

        self.assertEqual(push.call_count, 0)
        self.assertEqual(summary["no_subscription"], 1)
        self.assertEqual(summary["logged"], 0)
        self.assertFalse(any("to_regclass" in s for s, _ in db.calls))

    def test_no_new_openings_summary_has_zero_counts(self):
        db = FakeDB([watch_row("t1", "u1", is_open=False)], [sub_row("u1", SENT_EP)])

        summary, push = self.run_watcher(db)

        push.assert_not_called()
        for key in ("sent", "expired", "failed", "logged", "newly_open"):
            self.assertEqual(summary[key], 0, key)


class SendPushClassificationTests(unittest.TestCase):
    def send(self, endpoint):
        with patch.object(sw, "webpush", side_effect=fake_webpush):
            return sw._send_push(sub_row("u", endpoint), "{}")

    def test_classification(self):
        self.assertEqual(self.send(SENT_EP).status, "sent")
        gone = self.send(GONE_EP)
        self.assertEqual((gone.status, gone.error, gone.remove_subscription), ("expired", "HTTP 410", True))
        error = self.send(ERROR_EP)
        self.assertEqual((error.status, error.remove_subscription), ("failed", False))
        self.assertTrue(error.error.startswith("HTTP 500: "))

    def test_disallowed_endpoint_is_failed_and_removed(self):
        with patch.object(sw, "webpush") as push:
            result = sw._send_push(sub_row("u", "https://127.0.0.1/push"), "{}")
        push.assert_not_called()
        self.assertEqual((result.status, result.remove_subscription), ("failed", True))

    def test_error_text_is_short(self):
        with patch.object(sw, "webpush", side_effect=RuntimeError("x" * 5000)):
            result = sw._send_push(sub_row("u", SENT_EP), "{}")
        self.assertLessEqual(len(result.error), 300)


if __name__ == "__main__":
    unittest.main()
