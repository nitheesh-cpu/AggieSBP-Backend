"""Push endpoint allowlist and send-timeout tests without API startup or database writes."""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from aggiermp.core import notifications
from aggiermp.core.push_endpoints import PUSH_TIMEOUT_SECONDS, is_allowed_push_endpoint


class PushEndpointAllowlistTests(unittest.TestCase):
    def test_accepts_browser_push_services(self):
        for endpoint in [
            "https://fcm.googleapis.com/fcm/send/abc:def",
            "https://updates.push.services.mozilla.com/wpush/v2/gAAAA",
            "https://wns2-bl2p.notify.windows.com/w/?token=abc",
            "https://web.push.apple.com/QOabc",
            "https://FCM.googleapis.com:443/fcm/send/abc",
        ]:
            self.assertTrue(is_allowed_push_endpoint(endpoint), endpoint)

    def test_rejects_internal_and_lookalike_targets(self):
        for endpoint in [
            "http://fcm.googleapis.com/fcm/send/abc",
            "https://127.0.0.1:3567/recipe",
            "http://169.254.169.254/latest/meta-data/",
            "https://localhost/push",
            "https://fcm.googleapis.com.attacker.example/x",
            "https://evilfcm.googleapis.com.example/x",
            "https://attackerfcm.googleapis.com/x",
            "https://fcm.googleapis.com:8443/fcm/send/abc",
            "https://user:pass@fcm.googleapis.com/fcm/send/abc",
            "https://[::1/",
            "fcm.googleapis.com/fcm/send/abc",
            "",
        ]:
            self.assertFalse(is_allowed_push_endpoint(endpoint), endpoint)


class SendPushToUserTests(unittest.TestCase):
    def test_skips_disallowed_endpoints_and_sets_timeout(self):
        good = SimpleNamespace(endpoint="https://fcm.googleapis.com/fcm/send/abc", p256dh="k", auth="a")
        bad = SimpleNamespace(endpoint="http://127.0.0.1:9999/ok", p256dh="k", auth="a")
        db = Mock()
        db.query.return_value.filter.return_value.all.return_value = [bad, good]

        with patch.object(notifications, "webpush") as webpush, \
                patch.object(notifications.settings, "vapid_private_key", "dummy-key"):
            sent = notifications.NotificationService.send_push_to_user("u1", {"title": "t"}, db)

        self.assertTrue(sent)
        webpush.assert_called_once()
        kwargs = webpush.call_args.kwargs
        self.assertEqual(kwargs["subscription_info"]["endpoint"], good.endpoint)
        self.assertEqual(kwargs["timeout"], PUSH_TIMEOUT_SECONDS)


if __name__ == "__main__":
    unittest.main()
