"""
Admin endpoints for reviewing duplicate-professor merges.

Every route needs the X-Admin-Key header. The cron job
(pipelines/professors/merge_duplicates.py --propose) stores new candidates;
these endpoints list them and confirm or reject them, which is what the
site's /admin/merges page calls.

  GET  /admin/check                       verify the admin key
  GET  /admin/professor-merges?status=    list stored merges
  POST /admin/professor-merges/confirm    {"alias_ids": [...]}
  POST /admin/professor-merges/reject     {"alias_ids": [...]}  (also undoes)
"""

import logging
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ...core.admin_auth import require_admin_key
from ...core.cache import clear_all_cache
from ...core.professor_merges import list_merges, rebuild_command, set_status
from ...database.base import request_session

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/admin",
    tags=["Admin"],
    dependencies=[Depends(require_admin_key)],
)


class AliasIds(BaseModel):
    alias_ids: List[str] = Field(..., min_length=1, max_length=500)


@router.get("/check", summary="Verify the admin key")
async def check() -> Dict[str, bool]:
    return {"ok": True}


@router.get("/professor-merges", summary="List duplicate-professor merges")
async def get_merges(
    status: Optional[Literal["proposed", "confirmed", "rejected"]] = Query(None),
    db: Session = Depends(request_session),
) -> Dict[str, Any]:
    if not _aliases_ready(db):
        raise HTTPException(
            status_code=409,
            detail="professor_aliases does not exist yet: run migration 002_professor_aliases.sql",
        )
    return {"merges": list_merges(db, status)}


async def _change(db: Session, alias_ids: List[str], status: str) -> Dict[str, Any]:
    if not _aliases_ready(db):
        raise HTTPException(
            status_code=409,
            detail="professor_aliases does not exist yet: run migration 002_professor_aliases.sql",
        )
    changes = set_status(db, alias_ids, status)
    changed = [c for c in changes if c.status == status]
    if changed:
        # Professor pages, lists and search are cached for a week.
        await clear_all_cache()
        logger.info("Admin set %d professor merges to %s", len(changed), status)
    return {
        "changes": [
            {
                "aliasId": c.alias_id,
                "canonicalId": c.canonical_id,
                "status": c.status,
                "problem": c.problem,
            }
            for c in changes
        ],
        # Summaries are built per shown professor, so both confirming and
        # undoing a merge leave the canonical professor's summary stale.
        "rebuildCommand": rebuild_command(c.canonical_id for c in changed),
    }


@router.post("/professor-merges/confirm", summary="Make merges live")
async def confirm(body: AliasIds, db: Session = Depends(request_session)) -> Dict[str, Any]:
    return await _change(db, body.alias_ids, "confirmed")


@router.post("/professor-merges/reject", summary="Turn merges down or undo them")
async def reject(body: AliasIds, db: Session = Depends(request_session)) -> Dict[str, Any]:
    return await _change(db, body.alias_ids, "rejected")


def _aliases_ready(db: Session) -> bool:
    from sqlalchemy import text

    return db.execute(text("SELECT to_regclass('professor_aliases')")).scalar() is not None
