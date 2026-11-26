"""Admin endpoints to hide professors from the site (takedown requests).

Every route needs the X-Admin-Key header. Hiding is done here and never in a
committed file: the repo is public and who asked to be removed is private.

  GET    /admin/hidden-professors
  POST   /admin/hidden-professors            {"professor_id", "reason"}
  DELETE /admin/hidden-professors/{professor_id}

Needs migration 006_hidden_professors.sql; without it these routes return 503.
"""

import logging
from typing import Any, Dict, List, Set

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from ...core.admin_auth import require_admin_key
from ...core.cache_invalidation import ainvalidate_professors
from ...core.hidden_professors import hide_professor as hide_group, unhide_professor as unhide_group
from ...database.base import request_session

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/admin",
    tags=["Admin"],
    dependencies=[Depends(require_admin_key)],
)

MISSING_TABLE = (
    "The hidden_professors table does not exist. Run migration "
    "006_hidden_professors.sql first."
)


def _db_error(e: Exception, context: str = "") -> HTTPException:
    logger.error("DB error in %s: %s", context or "admin_hidden", e, exc_info=True)
    return HTTPException(status_code=500, detail="An internal server error occurred.")


class HideBody(BaseModel):
    professor_id: str = Field(..., min_length=1, max_length=200)
    reason: str = Field(..., min_length=1, max_length=500)

    @field_validator("professor_id", "reason")
    @classmethod
    def _not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value


def _exists(db: Session, name: str) -> bool:
    return bool(
        db.execute(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": name}).scalar()
    )


def _require_table(db: Session) -> None:
    if not _exists(db, "hidden_professors"):
        raise HTTPException(status_code=503, detail=MISSING_TABLE)


async def _clear_caches(ids: Set[str]) -> bool:
    """Best effort: the database change stands even if Redis is down."""
    try:
        await ainvalidate_professors(sorted(ids), strict=True)
        return True
    except Exception as exc:
        logger.warning("Cache clear after hide/unhide failed: %s", exc)
        return False


@router.get("/hidden-professors", summary="List hidden professors")
def list_hidden(db: Session = Depends(request_session)) -> Dict[str, Any]:
    try:
        _require_table(db)
        rows = db.execute(
            text(
                "SELECT h.professor_id, h.reason, h.created_at, p.name "
                "FROM hidden_professors h "
                "LEFT JOIN professors p ON p.id = h.professor_id "
                "ORDER BY h.created_at DESC"
            )
        ).fetchall()
        items: List[Dict[str, Any]] = [
            {
                "professorId": str(r[0]),
                "reason": r[1],
                "createdAt": r[2].isoformat() if r[2] else None,
                "name": r[3],
            }
            for r in rows
        ]
        return {"items": items}
    except HTTPException:
        raise
    except Exception as e:
        raise _db_error(e, "list_hidden")


@router.post("/hidden-professors", summary="Hide a professor and delete stored reviews")
async def hide_professor(
    body: HideBody, db: Session = Depends(request_session)
) -> Dict[str, Any]:
    """Hide the professor's whole merge group.

    In one transaction: records the entry (re-posting updates the reason),
    deletes our copy of the reviews and summaries, and drops pending summary
    rebuild rows for the group. The professors row and professor_aliases stay,
    so the nightly import does not recreate the person as new. Then clears
    caches; if that fails the hide still stands (cacheCleared is false).
    """
    try:
        _require_table(db)
        known = bool(
            db.execute(
                text("SELECT 1 FROM professors WHERE id = :id"), {"id": body.professor_id}
            ).first()
        )
        hidden = hide_group(db, body.professor_id, body.reason)
        group = set(hidden["groupIds"])
        ids = hidden["groupIds"]
        deleted = hidden["deleted"]
        db.commit()
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise _db_error(e, "hide_professor")

    cleared = await _clear_caches(group)
    return {
        "professorId": body.professor_id,
        "known": known,
        "groupIds": ids,
        "deleted": deleted,
        "cacheCleared": cleared,
    }


@router.delete("/hidden-professors/{professor_id}", summary="Unhide a professor")
async def unhide_professor(
    professor_id: str, db: Session = Depends(request_session)
) -> Dict[str, Any]:
    """Remove the hidden entry and clear caches.

    Deleted reviews are not restored here: they return on the next nightly
    review fetch. Summaries need a rebuild from /admin.
    """
    try:
        _require_table(db)
        group = unhide_group(db, professor_id)
        if group is None:
            raise HTTPException(status_code=404, detail="Professor is not hidden.")
        db.commit()
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise _db_error(e, "unhide_professor")

    cleared = await _clear_caches(group)
    return {"professorId": professor_id, "removed": True, "cacheCleared": cleared}
