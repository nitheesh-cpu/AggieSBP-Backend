"""POST /admin/cache/clear: clear what changed instead of the whole cache."""

from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ...core.admin_auth import require_admin_key
from ...core.cache_invalidation import (
    CacheInvalidationError,
    ainvalidate_all,
    ainvalidate_courses,
    ainvalidate_professors,
)

router = APIRouter(
    prefix="/admin",
    tags=["Admin"],
    dependencies=[Depends(require_admin_key)],
)


class CacheClearRequest(BaseModel):
    professor_ids: Optional[List[str]] = Field(None, max_length=500)
    course_codes: Optional[List[str]] = Field(None, max_length=500)
    all: Optional[bool] = None


class CacheClearResponse(BaseModel):
    keysDeleted: int
    pagesRevalidated: int
    revalidation: Literal["ok", "skipped", "failed"]
    message: Optional[str] = None


@router.post(
    "/cache/clear",
    response_model=CacheClearResponse,
    summary="Clear the API cache and refresh website pages for what changed",
)
async def clear_cache(body: CacheClearRequest) -> CacheClearResponse:
    if not (body.all or body.professor_ids or body.course_codes):
        raise HTTPException(
            status_code=422,
            detail="Provide professor_ids, course_codes or all=true.",
        )
    # strict: a Redis failure must show as an error, not as "skipped" (which
    # reads like "nothing to clear").
    try:
        if body.all:
            results = [await ainvalidate_all(strict=True)]
        else:
            results = []
            if body.professor_ids:
                results.append(
                    await ainvalidate_professors(body.professor_ids, strict=True)
                )
            if body.course_codes:
                results.append(
                    await ainvalidate_courses(body.course_codes, strict=True)
                )
    except CacheInvalidationError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"{exc}. Nothing may have been cleared; check Redis and retry.",
        ) from exc

    # Worst outcome wins: failed > ok > skipped.
    states = {r.revalidation for r in results}
    state = "failed" if "failed" in states else "ok" if "ok" in states else "skipped"
    messages = [r.message for r in results if r.message]
    return CacheClearResponse(
        keysDeleted=sum(r.keys_deleted for r in results),
        pagesRevalidated=sum(r.pages_revalidated for r in results),
        revalidation=state,
        message="; ".join(messages) or None,
    )
