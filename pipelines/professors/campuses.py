"""Campuses included in the professor pool by default."""

import base64
import os

from aggiermp.models.schema import University
from aggiermp.core.campuses import TEXAS_AM_ID, TEXAS_AM_GALVESTON_ID

CAMPUSES = {
    TEXAS_AM_ID: University(
        id=TEXAS_AM_ID,
        name="Texas A&M University at College Station",
        legacy_school_id=1003,
        city="College Station",
        state="TX",
    ),
    TEXAS_AM_GALVESTON_ID: University(
        id=TEXAS_AM_GALVESTON_ID,
        name="Texas A&M University at Galveston",
        legacy_school_id=1006,
        city="Galveston",
        state="TX",
    ),
}


def get_university_ids(override: str | None = None) -> list[str]:
    selected = override or os.getenv("UNIVERSITY_ID")
    if not selected:
        return list(CAMPUSES)
    selected = selected.strip()
    if selected.isdigit():
        selected = base64.b64encode(f"School-{selected}".encode()).decode()
    return [selected]
