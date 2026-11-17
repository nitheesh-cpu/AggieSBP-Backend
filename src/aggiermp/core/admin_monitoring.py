"""Read-only operational probes. External failures never masquerade as health."""

import asyncio
import json
import math
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import aiohttp
from sqlalchemy import text

from .cache import get_redis
from .config import settings
from ..database.base import get_session


def iso(value: Any) -> str | None:
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            value = datetime.fromtimestamp(value, timezone.utc)
        elif isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            return None
        return (
            value.replace(tzinfo=value.tzinfo or timezone.utc)
            .astimezone(timezone.utc)
            .isoformat()
        )
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def number(value: Any) -> float | None:
    if (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    ):
        return float(value)
    return None


def schedule_text(raw: dict) -> str | None:
    """`schedule` in API version 2020-10-01, `schedules` (a list) from 2025-11-28."""
    value = raw.get("schedule")
    if isinstance(value, str):
        return value[:200]
    schedules = raw.get("schedules")
    if isinstance(schedules, list):
        parts = []
        for item in schedules[:5]:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                found = item.get("schedule") or item.get("expression")
                if isinstance(found, str):
                    parts.append(found)
        if parts:
            return ", ".join(parts)[:200]
    return None


def normalize_monitor(raw: dict, key: str) -> dict:
    event = raw.get("latest_event") or {}
    if not isinstance(event, dict):
        event = {}
    metrics = event.get("metrics") or {}
    if not isinstance(metrics, dict):
        metrics = {}
    # A currently running job may still have an open failure. Keep it visible.
    status = "unknown"
    if raw.get("disabled") or raw.get("paused"):
        status = "paused"
    elif raw.get("initialized") is False:
        status = "unknown"
    elif raw.get("passing") is False:
        status = "failing"
    elif raw.get("running") is True:
        status = "running"
    elif raw.get("passing") is True:
        status = "healthy"
    runs = raw.get("latest_invocations") or []
    if not isinstance(runs, list):
        runs = []
    recent = []
    for run in runs[:10]:
        if not isinstance(run, dict):
            continue
        duration = number(run.get("duration"))
        recent.append(
            {
                "startedAt": iso(run.get("started_at")),
                "endedAt": iso(run.get("ended_at")),
                "durationSeconds": duration / 1000 if duration is not None else None,
            }
        )
    return {
        "key": key,
        "name": str(raw.get("name") or key)[:200],
        "status": status,
        "schedule": schedule_text(raw),
        "lastActivityAt": iso(event.get("stamp")),
        "lastDurationSeconds": number(metrics.get("duration")),
        "nextExpectedAt": iso(raw.get("next_expected_at")),
        "url": "https://cronitor.io/app/monitors/" + quote(key, safe=""),
        "recentRuns": recent,
    }


class MonitorReader:
    """One bounded refresh per API process every 30s, including on failure."""

    def __init__(self):
        self.lock = asyncio.Lock()
        self.result: dict | None = None
        self.expires = 0.0
        self.config: tuple | None = None

    async def fetch(
        self, keys: list[str], api_key: str, environment: str
    ) -> list[dict]:
        semaphore = asyncio.Semaphore(4)
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(api_key, ""),
            timeout=aiohttp.ClientTimeout(total=5),
        ) as client:

            async def one(key):
                async with semaphore:
                    async with client.get(
                        "https://cronitor.io/api/monitors/" + quote(key, safe=""),
                        params={"env": environment, "withInvocations": "true"},
                        headers={"Cronitor-Version": "2025-11-28"},
                        allow_redirects=False,
                    ) as response:
                        response.raise_for_status()
                        if response.status != 200:
                            raise ValueError("Unexpected monitor response")
                        payload = bytearray()
                        async for chunk in response.content.iter_chunked(16384):
                            payload.extend(chunk)
                            if len(payload) > 1_000_000:
                                raise ValueError("Monitor response too large")
                        raw = json.loads(payload)
                        if not isinstance(raw, dict) or raw.get("key") != key:
                            raise ValueError("Invalid monitor response")
                        return normalize_monitor(raw, key)

            outcomes = await asyncio.wait_for(
                asyncio.gather(*(one(k) for k in keys), return_exceptions=True),
                timeout=8,
            )
            if all(isinstance(o, BaseException) for o in outcomes):
                raise next(iter(outcomes))
            # One bad key (e.g. 404) must not hide the other monitors, and it must
            # never read as healthy.
            return [
                (
                    {
                        "key": key,
                        "name": key,
                        "status": "unavailable",
                        "schedule": None,
                        "lastActivityAt": None,
                        "lastDurationSeconds": None,
                        "nextExpectedAt": None,
                        "url": "https://cronitor.io/app/monitors/"
                        + quote(key, safe=""),
                        "recentRuns": [],
                    }
                    if isinstance(outcome, BaseException)
                    else outcome
                )
                for key, outcome in zip(keys, outcomes)
            ]

    async def read(self) -> dict:
        keys = list(
            dict.fromkeys(
                k.strip()
                for k in settings.cronitor_monitor_keys.split(",")
                if k.strip()
            )
        )
        config = (settings.cronitor_api_key, tuple(keys), settings.cronitor_environment)
        async with self.lock:
            if config != self.config:
                self.result, self.expires, self.config = None, 0, config
            if self.result is not None and time.monotonic() < self.expires:
                return self.result
            if not config[0] or not keys:
                result = {
                    "status": "not_configured",
                    "fetchedAt": None,
                    "monitors": [],
                    "message": "Configure CRONITOR_API_KEY and CRONITOR_MONITOR_KEYS on the API server.",
                }
            elif len(keys) > 20 or any(len(key) > 200 for key in keys):
                result = {
                    "status": "unavailable",
                    "fetchedAt": None,
                    "monitors": [],
                    "message": "Configure at most 20 Cronitor monitor keys, each under 201 characters.",
                }
            else:
                try:
                    monitors = await self.fetch(keys, config[0], config[2])
                    result = {
                        "status": "ok",
                        "fetchedAt": iso(datetime.now(timezone.utc)),
                        "monitors": monitors,
                        "message": None,
                    }
                except Exception:
                    # Do not expose response bodies, authenticated URLs, or exception text.
                    previous = self.result or {}
                    result = {
                        "status": (
                            "stale" if previous.get("fetchedAt") else "unavailable"
                        ),
                        "fetchedAt": previous.get("fetchedAt"),
                        "monitors": previous.get("monitors", []),
                        "message": "Cronitor could not be refreshed. Check server credentials and connectivity.",
                    }
            self.result, self.expires = result, time.monotonic() + 30
            return result


monitor_reader = MonitorReader()


def database_and_worker() -> tuple[dict, dict]:
    """Use one short read-only transaction, returning no connection details."""
    session = None
    database = {"status": "unavailable"}
    worker = {"status": "unavailable", "lastHeartbeatAt": None}
    try:
        session = get_session()
        session.execute(text("SET LOCAL statement_timeout = '3000ms'"))
        session.execute(text("SELECT 1"))
        database = {"status": "healthy"}
        if (
            session.execute(
                text("SELECT to_regclass('admin_worker_heartbeats')")
            ).scalar()
            is None
        ):
            worker["status"] = "not_configured"
        else:
            heartbeat = session.execute(
                text("SELECT max(heartbeat_at) FROM admin_worker_heartbeats")
            ).scalar()
            worker["lastHeartbeatAt"] = iso(heartbeat)
            if heartbeat is None:
                worker["status"] = "not_started"
            else:
                stamp = datetime.fromisoformat(worker["lastHeartbeatAt"])
                age = (datetime.now(timezone.utc) - stamp).total_seconds()
                worker["status"] = "healthy" if -30 <= age <= 90 else "stale"
    except Exception:
        pass
    finally:
        if session is not None:
            session.close()
    return database, worker


async def redis_health() -> dict:
    try:
        client = await get_redis()
        if client is not None and await client.ping():
            return {"status": "healthy"}
    except Exception:
        pass
    return {"status": "unavailable"}


class HealthReader:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.result: dict | None = None
        self.expires = 0.0
        self.database_task: asyncio.Task | None = None

    async def read(self) -> dict:
        async with self.lock:
            if self.result is not None and time.monotonic() < self.expires:
                return self.result
            # Timeout cannot stop a blocking DB connection in a thread. Reuse
            # that task until it finishes instead of accumulating more probes.
            if self.database_task is None or self.database_task.done():
                if (
                    self.database_task is not None
                    and not self.database_task.cancelled()
                ):
                    self.database_task.exception()
                self.database_task = asyncio.create_task(
                    asyncio.to_thread(database_and_worker)
                )
            outcomes = await asyncio.gather(
                asyncio.wait_for(asyncio.shield(self.database_task), timeout=5),
                asyncio.wait_for(redis_health(), timeout=3),
                return_exceptions=True,
            )
            database, worker = (
                {"status": "unavailable"},
                {"status": "unavailable", "lastHeartbeatAt": None},
            )
            if not isinstance(outcomes[0], BaseException):
                database, worker = outcomes[0]
            redis = (
                outcomes[1]
                if not isinstance(outcomes[1], BaseException)
                else {"status": "unavailable"}
            )
            self.result = {
                "checkedAt": iso(datetime.now(timezone.utc)),
                "api": {"status": "healthy", "version": settings.version},
                "database": database,
                "redis": redis,
                "worker": worker,
            }
            self.expires = time.monotonic() + 15
            return self.result


health_reader = HealthReader()
