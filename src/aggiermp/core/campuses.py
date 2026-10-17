"""Resolve known registration campuses to their RMP school identities."""

TEXAS_AM_ID = "U2Nob29sLTEwMDM="
TEXAS_AM_GALVESTON_ID = "U2Nob29sLTEwMDY="


def university_id_for_campus(campus: str | None) -> str | None:
    return {
        "college station": TEXAS_AM_ID,
        "galveston": TEXAS_AM_GALVESTON_ID,
    }.get((campus or "").strip().casefold())


def campus_from_term_description(description: str | None) -> str | None:
    campus = (description or "").rsplit(" - ", 1)[-1].strip()
    return campus if university_id_for_campus(campus) else None


def campus_university_sql(column: str) -> str:
    """SQL for internal section-campus columns; never accepts user input."""
    if column not in {"s.campus", "up.campus"}:
        raise ValueError("Unsupported campus column")
    return f"""CASE
        WHEN LOWER(TRIM({column})) = 'galveston' THEN '{TEXAS_AM_GALVESTON_ID}'
        WHEN LOWER(TRIM({column})) = 'college station' THEN '{TEXAS_AM_ID}'
        WHEN NULLIF(TRIM({column}), '') IS NULL THEN (
            SELECT CASE
                WHEN LOWER(TRIM(t.term_desc)) LIKE '% - galveston' THEN '{TEXAS_AM_GALVESTON_ID}'
                WHEN LOWER(TRIM(t.term_desc)) LIKE '% - college station' THEN '{TEXAS_AM_ID}'
                ELSE NULL END
            FROM terms t WHERE t.term_code = :term_code
        )
        ELSE NULL END"""
