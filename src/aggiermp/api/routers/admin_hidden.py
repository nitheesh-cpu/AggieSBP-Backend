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


def _merge_group(db: Session, professor_id: str) -> Set[str]:
    """The id, its confirmed canonical, and every confirmed alias of that canonical."""
    group = {professor_id}
    if not _exists(db, "professor_aliases"):
        return group
    canonical = db.execute(
        text(
            "SELECT canonical_id FROM professor_aliases "
            "WHERE alias_id = :id AND status = 'confirmed'"
        ),
        {"id": professor_id},
    ).scalar()
    canonical = str(canonical) if canonical else professor_id
    group.add(canonical)
    rows = db.execute(
        text(
            "SELECT alias_id FROM professor_aliases "
            "WHERE canonical_id = :canonical AND status = 'confirmed'"
        ),
        {"canonical": canonical},
    ).fetchall()
    group.update(str(r[0]) for r in rows)
    return group


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
        group = _merge_group(db, body.professor_id)
        ids = sorted(group)
        db.execute(
            text(
                "INSERT INTO hidden_professors (professor_id, reason) "
                "VALUES (:id, :reason) "
                "ON CONFLICT (professor_id) DO UPDATE SET reason = EXCLUDED.reason"
            ),
            {"id": body.professor_id, "reason": body.reason},
        )
        reviews = db.execute(
            text("DELETE FROM reviews WHERE professor_id = ANY(:ids)"), {"ids": ids}
        ).rowcount
        summaries = db.execute(
            text("DELETE FROM professor_summaries_new WHERE professor_id = ANY(:ids)"),
            {"ids": ids},
        ).rowcount
        rebuilds = 0
        if _exists(db, "admin_summary_rebuilds"):
            rebuilds = db.execute(
                text("DELETE FROM admin_summary_rebuilds WHERE professor_id = ANY(:ids)"),
                {"ids": ids},
            ).rowcount
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
        "deleted": {
            "reviews": reviews or 0,
            "summaries": summaries or 0,
            "pendingRebuilds": rebuilds or 0,
        },
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
        removed = db.execute(
            text("DELETE FROM hidden_professors WHERE professor_id = :id"),
            {"id": professor_id},
        ).rowcount
        if not removed:
            raise HTTPException(status_code=404, detail="Professor is not hidden.")
        group = _merge_group(db, professor_id)
        db.commit()
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise _db_error(e, "unhide_professor")

    cleared = await _clear_caches(group)
    return {"professorId": professor_id, "removed": True, "cacheCleared": cleared}
