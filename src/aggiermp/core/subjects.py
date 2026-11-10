"""Registrar subject codes and the student shorthand RMP uses for them."""

from __future__ import annotations

import re
from typing import AbstractSet, Optional

# RMP course codes are free text typed by students. Map the common
# shorthand, retired codes, and spelled-out names to registrar subjects.
SUBJECT_ALIASES: dict[str, str] = {
    # Computer Science department changes
    "CPCS": "CSCE",
    "CPSC": "CSCE",
    "COSC": "CSCE",
    "CS": "CSCE",
    # Electrical Engineering variations
    "ELEN": "ECEN",
    "EE": "ECEN",
    "ELEC": "ECEN",
    # Mathematics variations
    "MATH": "MATH",
    "MATHS": "MATH",
    # Engineering variations
    "ENGR": "ENGR",
    "ENGI": "ENGR",
    # Physics variations
    "PHYS": "PHYS",
    "PHY": "PHYS",
    # Chemistry variations
    "CHEM": "CHEM",
    "CHM": "CHEM",
    # Business variations
    "MGMT": "MGMT",
    "MKTG": "MKTG",
    "FINC": "FINC",
    "ACCT": "ACCT",
    "ID": "IDIS",
    # Student shorthand and full subject names seen in RMP course codes
    "ASTRO": "ASTR",
    "ASTRONOMY": "ASTR",
    "PSYCH": "PSYC",
    "PYSCH": "PSYC",
    "PSYCHOLOGY": "PSYC",
    "CHEMISTRY": "CHEM",
    "OCHEM": "CHEM",
    "HISTORY": "HIST",
    "STATS": "STAT",
    "STATISTICS": "STAT",
    "SOCIO": "SOCI",
    "SOCIOLOGY": "SOCI",
    "PHYSICS": "PHYS",
    "PHSYS": "PHYS",
    "BIO": "BIOL",
    "BIOLOGY": "BIOL",
    "KINES": "KINE",
    "ANTHRO": "ANTH",
    "ANTHROPOLOGY": "ANTH",
    "ENGLISH": "ENGL",
    "POLISCI": "POLS",
    "GOVT": "POLS",
    "GOVERNMENT": "POLS",
    "COMMS": "COMM",
    "COMMUNICATIONS": "COMM",
    "CALC": "MATH",
    "CALCULUS": "MATH",
    "ACCTT": "ACCT",
    "BUSINESS": "BUSN",
}

_COURSE_CODE = re.compile(r"^([A-Z]+?)(\d{3})[A-Z]?$")


def normalize_course_code(raw: Optional[str]) -> str:
    """Uppercase and strip separators: ``"csce-470 "`` -> ``"CSCE470"``."""
    return re.sub(r"[^A-Z0-9]", "", (raw or "").upper())


def parse_course_code(
    raw: Optional[str], valid_subjects: AbstractSet[str]
) -> Optional[tuple[str, str]]:
    """Resolve a free-text RMP course code to ``(subject, number)``.

    ``"Honors AGEC-105H"`` -> ``("AGEC", "105")``. Codes that do not resolve
    to a real subject (bare numbers, typos, unknown abbreviations) return
    ``None`` so they never count as evidence.
    """
    code = normalize_course_code(raw)
    if code.startswith("HONORS"):
        code = code[len("HONORS") :]
    match = _COURSE_CODE.match(code)
    if not match:
        return None
    subject = SUBJECT_ALIASES.get(match.group(1), match.group(1))
    return (subject, match.group(2)) if subject in valid_subjects else None
