"""Targeted invalidation of the API's Redis cache and the website's page cache.

Every job that changes data calls one of these instead of clearing everything:

    invalidate_professors(ids)   a professor's reviews/summaries/aliases changed
    invalidate_courses(codes)    course rows or grade (GPA) data changed
    invalidate_all()             everything (the admin "clear everything" button)

Each has an async twin (``ainvalidate_*``) for the API; they run the same
synchronous code in a worker thread, so there is one implementation.

Redis keys look like ``api:{path}:{hash}`` (see core/cache.py), so a family of
endpoints is addressed by path. We make ONE SCAN pass over ``api:*`` and test
each key against every pattern client side, instead of one full keyspace walk
per pattern (a professor teaching 30 courses would otherwise need ~120 walks).
KEYS is never used; deletes are batched.

Invalidation map
================
invalidate_professors(ids). The ids are first widened with confirmed merges
(professor_aliases): an alias also covers its canonical id and every sibling
alias, and a canonical covers all its aliases, because the API serves a merged
professor under the canonical id but old links use the alias id.
  Redis, per professor id P:
    /professor/P, /professor/P/*         profile, reviews and every sub-path
    /course/*/reviews/P                  per-course review page for P
  Redis, per course C that P (or an alias) has a summary or review for:
    /course/C, /course/C/*               course page + its professors list
    /sections/*/course/C, /sections/*/course/C/*
                                          section professor lists/details
  Redis, always (these embed professor ratings or summaries):
    /professor/find, /professors, /professors/*   (list, search, compare)
    /courses, /courses/*                 list shows professor ratings
    /discover/*                          fit/ucc/gpa-booster rows list professors
    /data_stats                          professor/review counts
  Website: /professor/P, /professor/P/reviews, /course/C
  Left alone: /sections, /sections/{term} (cached only 15 minutes),
  /terms, /departments*, /users/*, /health.

invalidate_courses(codes). Normalised (upper case, no spaces).
  Redis, per course C: /course/C, /course/C/*, /sections/*/course/C,
    /sections/*/course/C/*
  Redis, always: /courses, /courses/* (list, compare, prereqs),
    /departments, /departments_info (course/professor/GPA counts),
    /discover/* (aggregate course rows), /data_stats
  With include_professor_pages=True (grade data changed, which professor
    pages show per course): also /professor/*, /professors, /professors/*
    and the website revalidates every professor page.
  Website: /course/C (or every course page when ``revalidate_all``).

invalidate_all(): every ``api:*`` key; website revalidates all dynamic pages.

Failures: Redis/DB errors are logged and never raised unless ``strict=True``
(the summary worker uses that so a stale cache stops a batch). Website
revalidation never raises; its outcome is in the result. Revalidation is
skipped, with a debug log, unless ``site_revalidate_url`` and
``revalidate_secret`` are both configured, and is not attempted after a Redis
failure (the site would just re-render from the stale API cache).
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Sequence, Set

from .config import settings

logger = logging.getLogger(__name__)

SCAN_COUNT = 1000
DELETE_BATCH = 500
REVALIDATE_BATCH = 200
REVALIDATE_TIMEOUT = 10  # seconds per request
# Above this many pages one "all" request is cheaper than many path batches.
REVALIDATE_ALL_THRESHOLD = 1000
# Past this many courses, clear every course endpoint instead of listing each.
MANY_COURSES = 200


class CacheInvalidationError(RuntimeError):
    """Raised only when ``strict=True`` and Redis or the database failed."""


@dataclass
class InvalidationResult:
    keys_deleted: int = 0
    pages_revalidated: int = 0
    revalidation: str = "skipped"  # "ok" | "skipped" | "failed"
    message: Optional[str] = None


# --------------------------------------------------------------------------
# Redis
# --------------------------------------------------------------------------


def _client() -> Any:
    import redis

    return redis.Redis.from_url(
        settings.redis_url or "redis://localhost:6379",
        socket_connect_timeout=5,
        socket_timeout=10,
    )


def _compile(patterns: Iterable[str], **values: str) -> "re.Pattern[str]":
    """Glob-ish patterns ('*' = anything) to one regex. Literal parts are escaped."""
    alternatives = []
    for pattern in patterns:
        for name, value in values.items():
            pattern = pattern.replace("{" + name + "}", value)
        pieces = [re.escape(part) for part in pattern.split("*")]
        alternatives.append(".*".join(pieces))
    return re.compile("^(?:" + "|".join(alternatives) + ")$", re.DOTALL)


def _delete_matching(client: Any, regex: "re.Pattern[str]", scan_match: str) -> int:
    deleted = 0
    batch: List[Any] = []
    for key in client.scan_iter(match=scan_match, count=SCAN_COUNT):
        text = key.decode() if isinstance(key, bytes) else key
        if regex.match(text):
            batch.append(key)
            if len(batch) >= DELETE_BATCH:
                deleted += client.delete(*batch)
                batch = []
    if batch:
        deleted += client.delete(*batch)
    return deleted


def _delete_keys(
    regexes: Sequence["re.Pattern[str]"], scan_match: str = "api:*"
) -> int:
    client = _client()
    try:
        client.ping()
        combined = re.compile("|".join(r.pattern for r in regexes), re.DOTALL)
        return _delete_matching(client, combined, scan_match)
    finally:
        try:
            client.close()
        except Exception:
            pass


# --------------------------------------------------------------------------
# Website page cache
# --------------------------------------------------------------------------


def revalidate_site(
    paths: Optional[Sequence[str]] = None, *, all_pages: bool = False
) -> InvalidationResult:
    """Ask the website to revalidate pages. Never raises."""
    url = settings.site_revalidate_url
    secret = settings.revalidate_secret
    if not url or not secret:
        logger.debug("Page revalidation skipped: site_revalidate_url/secret unset")
        return InvalidationResult(revalidation="skipped")
    paths = list(dict.fromkeys(paths or []))
    if not all_pages and not paths:
        return InvalidationResult(revalidation="skipped")
    if not all_pages and len(paths) > REVALIDATE_ALL_THRESHOLD:
        all_pages = True

    import requests

    headers = {"X-Revalidate-Secret": secret}
    try:
        if all_pages:
            body_list: List[dict] = [{"all": True}]
        else:
            body_list = [
                {"paths": paths[i : i + REVALIDATE_BATCH]}
                for i in range(0, len(paths), REVALIDATE_BATCH)
            ]
        count = 0
        for body in body_list:
            response = requests.post(
                url, json=body, headers=headers, timeout=REVALIDATE_TIMEOUT
            )
            if response.status_code != 200:
                message = f"Website returned HTTP {response.status_code}"
                logger.warning("Page revalidation failed: %s", message)
                return InvalidationResult(
                    pages_revalidated=count, revalidation="failed", message=message
                )
            count += len(body["paths"]) if "paths" in body else 1
        return InvalidationResult(pages_revalidated=count, revalidation="ok")
    except Exception as exc:
        logger.warning("Page revalidation failed: %s", exc)
        return InvalidationResult(
            revalidation="failed", message=f"Website unreachable: {type(exc).__name__}"
        )


# --------------------------------------------------------------------------
# Id expansion
# --------------------------------------------------------------------------


def normalize_course_code(code: str) -> str:
    return re.sub(r"\s+", "", str(code or "")).upper()


def _clean(values: Iterable[Any]) -> List[str]:
    return list(dict.fromkeys(str(v).strip() for v in values if v and str(v).strip()))


def expand_professor_ids(ids: Iterable[str], db: Any) -> Set[str]:
    """ids plus their canonical ids and every confirmed alias of those."""
    from .professor_identity import load_aliases

    group = set(_clean(ids))
    aliases = load_aliases(db)  # alias -> canonical
    for pid in list(group):
        if pid in aliases:
            group.add(aliases[pid])
    for alias, canonical in aliases.items():
        if canonical in group:
            group.add(alias)
    return group


def course_codes_for_professors(ids: Iterable[str], db: Any) -> Set[str]:
    from sqlalchemy import text

    id_list = list(ids)
    if not id_list:
        return set()
    codes: Set[str] = set()
    for table in ("professor_summaries_new", "reviews"):
        rows = db.execute(
            text(
                f"SELECT DISTINCT course_code FROM {table} "
                "WHERE professor_id = ANY(:ids) AND course_code IS NOT NULL"
            ),
            {"ids": id_list},
        ).fetchall()
        codes.update(normalize_course_code(r[0]) for r in rows if r[0])
    codes.discard("")
    return codes


def _lookup(given: Sequence[str], db: Any) -> Any:
    group = expand_professor_ids(given, db)
    return group, course_codes_for_professors(group, db)


def _with_session(db: Any, work: Any) -> Any:
    if db is not None:
        return work(db)
    from ..database.base import get_session

    session = get_session()
    try:
        return work(session)
    finally:
        session.close()


# --------------------------------------------------------------------------
# Public sync API
# --------------------------------------------------------------------------

_PROFESSOR_ALWAYS = [
    "api:/professor/find:*",
    "api:/professors:*",
    "api:/professors/*",
    "api:/courses:*",
    "api:/courses/*",
    "api:/discover/*",
    "api:/data_stats:*",
]
_PROFESSOR_EACH = [
    "api:/professor/{pid}:*",
    "api:/professor/{pid}/*",
    "api:/course/*/reviews/{pid}:*",
]
_COURSE_EACH = [
    "api:/course/{code}:*",
    "api:/course/{code}/*",
    "api:/sections/*/course/{code}:*",
    "api:/sections/*/course/{code}/*",
]
_COURSE_ALWAYS = [
    "api:/courses:*",
    "api:/courses/*",
    "api:/departments:*",
    "api:/departments_info:*",
    "api:/discover/*",
    "api:/data_stats:*",
]
_PROFESSOR_PAGES = ["api:/professor/*", "api:/professors:*", "api:/professors/*"]


def _course_regexes(codes: Iterable[str]) -> List["re.Pattern[str]"]:
    """Per-course patterns; for very many codes one generic pattern is cheaper
    than thousands of alternations tested against every key."""
    codes = sorted(codes)
    if len(codes) > MANY_COURSES:
        return [_compile(_COURSE_EACH, code="*")]
    return [_compile(_COURSE_EACH, code=c) for c in codes]


def _finish(
    result: InvalidationResult, strict: bool, error: Optional[Exception]
) -> InvalidationResult:
    if error is not None:
        if strict:
            raise CacheInvalidationError(
                f"Cache invalidation failed: {type(error).__name__}"
            ) from error
        result.revalidation = "skipped"
        result.message = f"Cache invalidation failed: {type(error).__name__}"
    return result


def invalidate_professors(
    ids: Iterable[str],
    *,
    db: Any = None,
    strict: bool = False,
    revalidate: bool = True,
) -> InvalidationResult:
    """Clear everything that shows these professors (and their merge aliases)."""
    result = InvalidationResult()
    given = _clean(ids)
    if not given:
        return result
    try:
        try:
            group, codes = _with_session(db, lambda s: _lookup(given, s))
        except Exception as exc:
            # Without the database we still clear the ids we were given.
            logger.warning("Alias/course lookup failed: %s", exc)
            if strict:
                raise
            group, codes = set(given), set()

        regexes = [_compile(_PROFESSOR_ALWAYS)]
        regexes += [_compile(_PROFESSOR_EACH, pid=p) for p in sorted(group)]
        regexes += _course_regexes(codes)
        result.keys_deleted = _delete_keys(regexes)
    except Exception as exc:
        logger.warning("Professor cache invalidation failed: %s", exc)
        return _finish(result, strict, exc)

    if revalidate:
        paths = []
        for pid in sorted(group):
            paths += [f"/professor/{pid}", f"/professor/{pid}/reviews"]
        paths += [f"/course/{c}" for c in sorted(codes)]
        _merge(result, revalidate_site(paths))
    logger.info(
        "Invalidated professors %s: %d keys", sorted(group)[:5], result.keys_deleted
    )
    return result


def invalidate_courses(
    codes: Iterable[str],
    *,
    strict: bool = False,
    revalidate: bool = True,
    revalidate_all: bool = False,
    include_professor_pages: bool = False,
) -> InvalidationResult:
    """Clear everything that shows these courses."""
    result = InvalidationResult()
    normalized = sorted({normalize_course_code(c) for c in codes} - {""})
    if not normalized and not revalidate_all:
        return result
    try:
        regexes = [_compile(_COURSE_ALWAYS)]
        regexes += _course_regexes(normalized)
        if include_professor_pages:
            regexes.append(_compile(_PROFESSOR_PAGES))
        result.keys_deleted = _delete_keys(regexes)
    except Exception as exc:
        logger.warning("Course cache invalidation failed: %s", exc)
        return _finish(result, strict, exc)

    if revalidate:
        if revalidate_all or include_professor_pages:
            _merge(result, revalidate_site(all_pages=True))
        else:
            _merge(result, revalidate_site([f"/course/{c}" for c in normalized]))
    return result


def invalidate_all(*, strict: bool = False, revalidate: bool = True) -> InvalidationResult:
    """Delete every cached API response and refresh all website pages."""
    result = InvalidationResult()
    try:
        result.keys_deleted = _delete_keys([_compile(["api:*"])])
    except Exception as exc:
        logger.warning("Full cache invalidation failed: %s", exc)
        return _finish(result, strict, exc)
    if revalidate:
        _merge(result, revalidate_site(all_pages=True))
    return result


def _merge(result: InvalidationResult, site: InvalidationResult) -> None:
    result.pages_revalidated = site.pages_revalidated
    result.revalidation = site.revalidation
    result.message = site.message


# --------------------------------------------------------------------------
# Async twins for the API
# --------------------------------------------------------------------------


async def ainvalidate_professors(ids: Iterable[str], **kwargs: Any) -> InvalidationResult:
    return await asyncio.to_thread(invalidate_professors, list(ids), **kwargs)


async def ainvalidate_courses(codes: Iterable[str], **kwargs: Any) -> InvalidationResult:
    return await asyncio.to_thread(invalidate_courses, list(codes), **kwargs)


async def ainvalidate_all(**kwargs: Any) -> InvalidationResult:
    return await asyncio.to_thread(invalidate_all, **kwargs)
