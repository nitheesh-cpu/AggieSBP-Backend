"""The X-Admin-Key check shared by every admin endpoint."""

import secrets
from typing import Optional

from fastapi import Header, HTTPException

from .config import settings


def require_admin_key(x_admin_key: Optional[str] = Header(None)) -> None:
    """Reject the request unless X-Admin-Key matches ADMIN_API_KEY."""
    expected = settings.admin_api_key
    if not expected:
        raise HTTPException(status_code=503, detail="Admin endpoints are disabled.")
    if not x_admin_key or not secrets.compare_digest(
        x_admin_key.encode(), expected.encode()
    ):
        raise HTTPException(status_code=401, detail="Invalid admin key.")
