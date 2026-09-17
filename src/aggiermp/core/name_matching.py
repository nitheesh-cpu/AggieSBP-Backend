"""Utilities for matching professor names across registrar, GPA, and RMP data."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher


_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "phd", "md"}


def _normalize_token(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", ascii_value.lower())


def _tokens(value: str) -> tuple[str, ...]:
    result = []
    for raw_token in re.split(r"\s+", value.strip()):
        token = _normalize_token(raw_token)
        if token and token not in _SUFFIXES:
            result.append(token)
    return tuple(result)


@dataclass(frozen=True)
class ParsedName:
    given: tuple[str, ...]
    surname: tuple[str, ...]

    @property
    def given_initial(self) -> str:
        return self.given[0][0] if self.given and self.given[0] else ""


def parse_person_name(full_name: str) -> ParsedName:
    """Parse common professor-name formats without discarding compound surnames.

    Supported examples include ``Last, First Middle``, ``First Middle Last``, and
    the GPA export convention ``LAST COMPOUND I`` (surname followed by an initial).
    """
    if not full_name or not full_name.strip():
        return ParsedName((), ())

    if "," in full_name:
        surname_text, given_text = full_name.split(",", 1)
        return ParsedName(_tokens(given_text), _tokens(surname_text))

    parts = _tokens(full_name)
    if not parts:
        return ParsedName((), ())
    if len(parts) == 1:
        return ParsedName((), parts)

    # ANEX/GPA exports use "SURNAME [SURNAME ...] INITIAL".
    if len(parts[-1]) == 1:
        return ParsedName((parts[-1],), parts[:-1])

    return ParsedName(parts[:-1], (parts[-1],))


def _token_similarity(left: str, right: str) -> float:
    if left == right:
        return 1.0
    if len(left) == 1 or len(right) == 1:
        return 1.0 if left[:1] == right[:1] else 0.0
    return SequenceMatcher(None, left, right).ratio()


def professor_name_score(left: str, right: str) -> float:
    """Return a conservative 0..1 identity score for two professor names."""
    a = parse_person_name(left)
    b = parse_person_name(right)
    if not a.surname or not b.surname:
        return 0.0

    surname_scores = [
        _token_similarity(a_token, b_token)
        for a_token in a.surname
        for b_token in b.surname
    ]
    surname_score = max(surname_scores, default=0.0)
    if surname_score < 0.72:
        return 0.0
    primary_surname_matches = a.surname[0] == b.surname[0]
    if not primary_surname_matches:
        # Keep compound-surname variants eligible, but prefer the primary
        # surname so "Arroyo Relion" ranks above an unrelated "Relion".
        surname_score = min(surname_score, 0.78)

    given_score = max(
        (
            _token_similarity(a_token, b_token)
            for a_token in a.given
            for b_token in b.given
        ),
        default=0.0,
    )

    # A matching given-name initial plus a matching surname is a strong signal.
    has_initial_only = any(len(token) == 1 for token in (*a.given, *b.given))
    if (
        has_initial_only
        and a.given_initial
        and b.given_initial
        and a.given_initial == b.given_initial
    ):
        given_score = max(given_score, 0.9)
    elif a.given and b.given and given_score < 0.72:
        return 0.0

    overlap_bonus = 0.08 if primary_surname_matches else 0.0
    return min(1.0, surname_score * 0.72 + given_score * 0.28 + overlap_bonus)


def professor_names_match(left: str, right: str, threshold: float = 0.82) -> bool:
    return professor_name_score(left, right) >= threshold
