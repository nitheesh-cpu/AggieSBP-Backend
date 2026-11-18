"""Aggregate the section watcher's retained push delivery log."""

from typing import Literal

from sqlalchemy import text

from ..database.base import get_session
from .admin_monitoring import iso

SeatAlertPeriod = Literal["day", "week", "month"]
CENTRAL_TIME = "America/Chicago"

_BUCKET_COUNTS = {"day": 24, "week": 7, "month": 30}

_SENT_ALERTS_BY_BUCKET = text("""
    WITH chart AS (
        SELECT
            statement_timestamp() AS as_of,
            date_trunc(:unit, statement_timestamp() AT TIME ZONE :zone) AS last_start,
            CAST(:step AS interval) AS step
    )
    SELECT bucket.start_at AT TIME ZONE :zone AS starts_at, count(log.sent_at) AS sent
    FROM chart
    CROSS JOIN LATERAL generate_series(
        last_start - (CAST(:size AS integer) - 1) * step, last_start, step
    ) AS bucket(start_at)
    LEFT JOIN push_notifications AS log
        ON log.sent_at >= (bucket.start_at AT TIME ZONE :zone)
        AND log.sent_at < ((bucket.start_at + step) AT TIME ZONE :zone)
        AND log.sent_at < as_of
        AND log.status = 'sent'
    GROUP BY bucket.start_at
    ORDER BY bucket.start_at
""")


def read_seat_alerts(period: SeatAlertPeriod) -> dict:
    hourly = period == "day"
    with get_session() as session:
        session.execute(text("SET LOCAL statement_timeout = '3000ms'"))
        rows = session.execute(
            _SENT_ALERTS_BY_BUCKET,
            {
                "unit": "hour" if hourly else "day",
                "zone": "UTC" if hourly else CENTRAL_TIME,
                "step": "1 hour" if hourly else "1 day",
                "size": _BUCKET_COUNTS[period],
            },
        ).all()

    # Hourly boundaries use UTC so both instances of a repeated local hour survive.
    return {
        "timeZone": CENTRAL_TIME,
        "buckets": [
            {"startAt": iso(starts_at), "sent": sent} for starts_at, sent in rows
        ],
    }
