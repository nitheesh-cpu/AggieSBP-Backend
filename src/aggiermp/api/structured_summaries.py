"""
Structured professor-summary fields (tldr, tips, review_stats, citations).

The columns are added by
pipelines/professors/migrations/001_structured_professor_summaries.sql. The
API checks for them before reading so it keeps working when the migration has
not been applied yet.
"""

import json
import logging
import time
from typing import Any, Dict, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

STRUCTURED_SUMMARY_COLUMNS = ("tldr", "tips", "review_stats", "citations")
RECHECK_SECONDS = 300

_columns_present = False
_checked_at: Optional[float] = None


def structured_summary_columns_present(db: Any) -> bool:
    """
    Whether professor_summaries_new has the structured-summary columns.

    A positive answer is cached for the process lifetime; a negative one is
    rechecked every few minutes so applying the migration needs no restart.
    """
    global _columns_present, _checked_at
    if _columns_present:
        return True
    now = time.monotonic()
    if _checked_at is not None and now - _checked_at < RECHECK_SECONDS:
        return False
    try:
        rows = db.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'professor_summaries_new' "
                "AND column_name = ANY(:names)"
            ),
            {"names": list(STRUCTURED_SUMMARY_COLUMNS)},
        ).fetchall()
        present = len(rows) == len(STRUCTURED_SUMMARY_COLUMNS)
    except Exception as e:
        logger.warning(f"Could not check structured summary columns: {e}")
        db.rollback()
        return False
    _checked_at = now
    _columns_present = present
    if not present:
        logger.warning(
            "professor_summaries_new lacks tldr/tips/review_stats/citations; "
            "apply pipelines/professors/migrations/"
            "001_structured_professor_summaries.sql to serve them"
        )
    return present


def _json_object(value: Any) -> Optional[Dict[str, Any]]:
    """JSON columns arrive parsed from psycopg2, but tolerate raw strings."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    return value if isinstance(value, dict) and value else None


def structured_overall_fields(row: Any, present: bool) -> Dict[str, Any]:
    """
    tldr/reviewStats/citations for an overall summary row.

    The row is only read when ``present``: on an ORM row the columns are
    deferred, and loading them before the migration is applied would fail.
    """
    if not present or row is None:
        return {"tldr": None, "reviewStats": None, "citations": None}
    return {
        "tldr": getattr(row, "tldr", None) or None,
        "reviewStats": _json_object(getattr(row, "review_stats", None)),
        "citations": _json_object(getattr(row, "citations", None)),
    }


def structured_course_fields(row: Any, present: bool) -> Dict[str, Any]:
    """tips/reviewStats/citations for a course summary row."""
    if not present or row is None:
        return {"tips": [], "reviewStats": None, "citations": None}
    return {
        "tips": [tip for tip in (getattr(row, "tips", None) or []) if tip],
        "reviewStats": _json_object(getattr(row, "review_stats", None)),
        "citations": _json_object(getattr(row, "citations", None)),
    }
