"""SQL matching for professor name searches, however students type the name."""

from __future__ import annotations

import re
from typing import Any, Dict, Tuple

_FULL_NAME = "(p.first_name || ' ' || p.last_name)"


def professor_name_condition(raw: str, key: str = "name") -> Tuple[str, Dict[str, Any]]:
    """WHERE clause (on ``professors p``) and params for a name search.

    Matches, in addition to a plain substring of the first, last or full name:

    * ``"Hanik, Bruce Walter"`` (Howdy / registrar format): the last name
      plus the first initial. Middle names are ignored, since review
      profiles rarely have them, and only the initial is used because the
      first name is often spelled or shortened differently.
    * ``"Furth, Brett Howard"`` against a profile with the names swapped
      ("FURTH HOWARD"): the last name is the profile's first name and a
      first or middle name is its last name.
    * ``"kim bryan"``: every word appears in the full name, in any order.
    """
    text_ = raw.strip()
    params: Dict[str, Any] = {key: f"%{text_}%"}
    conditions = [
        f"p.first_name ILIKE :{key} OR p.last_name ILIKE :{key} OR {_FULL_NAME} ILIKE :{key}"
    ]

    if "," in text_:
        last, _, first = text_.partition(",")
        last_words = [w for w in re.split(r"[\s\-]+", last) if len(w) >= 2]
        first_words = first.split()
        if last_words:
            last_parts = []
            for i, word in enumerate(last_words):
                params[f"{key}_last{i}"] = f"%{word}%"
                last_parts.append(f"p.last_name ILIKE :{key}_last{i}")
            condition = "(" + " OR ".join(last_parts) + ")"
            if first_words:
                params[f"{key}_initial"] = f"{first_words[0][0]}%"
                condition += f" AND p.first_name ILIKE :{key}_initial"
            conditions.append(condition)
            given_words = [w for w in re.split(r"[\s\-]+", first) if len(w) >= 2]
            if given_words:
                params[f"{key}_swapfirst"] = " ".join(last_words)
                given_parts = []
                for i, word in enumerate(given_words):
                    params[f"{key}_swaplast{i}"] = word
                    given_parts.append(f"TRIM(p.last_name) ILIKE :{key}_swaplast{i}")
                conditions.append(
                    f"TRIM(p.first_name) ILIKE :{key}_swapfirst AND ("
                    + " OR ".join(given_parts) + ")"
                )
    else:
        words = [w for w in text_.split() if len(w) >= 2]
        if len(words) >= 2:
            word_parts = []
            for i, word in enumerate(words):
                params[f"{key}_word{i}"] = f"%{word}%"
                word_parts.append(f"{_FULL_NAME} ILIKE :{key}_word{i}")
            conditions.append(" AND ".join(word_parts))

    return "(" + " OR ".join(f"({c})" for c in conditions) + ")", params
