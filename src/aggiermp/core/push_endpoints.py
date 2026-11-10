"""Web Push endpoint policy shared by the API and the section watcher.

Kept dependency-free so the watcher can import it without loading settings.
"""

from urllib.parse import urlsplit

# Seconds before an outbound push request gives up. pywebpush's webpush()
# defaults to no timeout, which lets an unresponsive endpoint block forever.
PUSH_TIMEOUT_SECONDS = 10

# Browser push services. Endpoints come from PushManager.subscribe(), so a
# legitimate subscription always points at one of these hosts.
_ALLOWED_PUSH_HOST_SUFFIXES = (
    "fcm.googleapis.com",  # Chrome, Edge (Chromium), Opera, Brave
    "updates.push.services.mozilla.com",  # Firefox
    "notify.windows.com",  # Legacy Edge / Windows (wns2-*.notify.windows.com)
    "push.apple.com",  # Safari (web.push.apple.com)
)


def is_allowed_push_endpoint(endpoint: str) -> bool:
    """True when endpoint is an https URL on a known browser push service."""
    try:
        parts = urlsplit(endpoint)
        port = parts.port
    except ValueError:
        return False
    if parts.scheme != "https" or parts.username or parts.password:
        return False
    if port not in (None, 443):
        return False
    host = (parts.hostname or "").lower().rstrip(".")
    return any(
        host == suffix or host.endswith("." + suffix)
        for suffix in _ALLOWED_PUSH_HOST_SUFFIXES
    )
