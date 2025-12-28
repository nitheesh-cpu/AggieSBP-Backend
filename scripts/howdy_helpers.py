"""
Helpers for the Howdy sandbox.

This file intentionally has **no** AggieRMP dependencies so you can iterate fast.
"""

from __future__ import annotations

import json
from typing import Any


def recursive_parse_json(value: Any) -> Any:
    """
    Recursively parse JSON-ish values.

    Howdy endpoints sometimes return:
    - real JSON objects
    - JSON strings (stringified JSON)
    - nested JSON strings inside JSON
    - empty strings / nulls
    """
    if value is None:
        return None

    # If bytes, decode first
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8", errors="ignore")
        except Exception:
            return value

    # If string, try JSON decode; if it fails, return original string
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return value
        try:
            parsed = json.loads(s)
        except Exception:
            return value
        return recursive_parse_json(parsed)

    # If list/tuple, recurse per item
    if isinstance(value, list):
        return [recursive_parse_json(v) for v in value]

    # If dict, recurse per key
    if isinstance(value, dict):
        return {k: recursive_parse_json(v) for k, v in value.items()}

    return value









