"""
Scraping functions for fetching GPA data from anex.us API.

This module contains functions to:
1. Get the newest semester available from anex.us
2. Fetch GPA data for courses
3. Extract and parse class records from API responses
"""

import asyncio
import json
import re
import ssl
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
import certifi

# Configuration
DEFAULT_REQUEST_INTERVAL = 1.0
INITIAL_RETRY_DELAY = 30
REQUEST_TIMEOUT = 30
MAX_RETRIES = 2  # Retries on timeouts, connection errors, 429 and 5xx
ANEX_BASE_URL = "https://anex.us/grades/"

# Verify TLS against certifi's CA bundle instead of the OS store. Some Windows
# trust stores pick an expired chain for anex.us's Let's Encrypt cert and every
# request fails with CERTIFICATE_VERIFY_FAILED.
SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


def make_http_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=SSL_CONTEXT, limit=1))


class RequestPacer:
    """Space out request starts, including retries and requests between batches."""

    def __init__(self, interval: float):
        self.interval = interval
        self.next_request = 0.0

    async def wait(self) -> None:
        loop = asyncio.get_running_loop()
        delay = self.next_request - loop.time()
        if delay > 0:
            await asyncio.sleep(delay)
        self.next_request = loop.time() + self.interval


def retry_delay(attempt: int, retry_after: Optional[str]) -> float:
    delay = INITIAL_RETRY_DELAY * 2**attempt
    if not retry_after:
        return delay
    try:
        if retry_after.isascii() and retry_after.isdecimal():
            return max(delay, float(retry_after))
        retry_at = parsedate_to_datetime(retry_after)
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(delay, (retry_at - datetime.now(timezone.utc)).total_seconds())
    except (ValueError, OverflowError):
        return delay


# anex.us uses calendar years, so within a year the order is
# SPRING (Jan) -> SUMMER (May) -> FALL (Aug). FALL 2025 is older than SPRING 2026.
SEMESTER_ORDER = {"SPRING": 1, "SUMMER": 2, "FALL": 3}


def term_key(year: Any, semester: Any) -> Tuple[int, int]:
    """Sortable key for a (year, semester) term. Unparseable values sort first."""
    match = re.search(r"(\d{4})", str(year or ""))
    year_int = int(match.group(1)) if match else 0
    return year_int, SEMESTER_ORDER.get(str(semester or "").upper().strip(), 0)


async def get_newest_semester(
    session: aiohttp.ClientSession,
    pacer: RequestPacer,
) -> Optional[Tuple[str, str]]:
    """
    Query anex.us to find the newest (year, semester) available, using MATH 151
    as a sample course since it is offered every term. Returns None on failure.
    """
    response = await fetch_course_data(session, "MATH", "151", pacer)
    if not response.get("success"):
        print(f"[WARNING] Could not fetch MATH 151 from anex.us: {response.get('error')}")
        return None

    newest = max(
        response["data"]["classes"],
        key=lambda c: term_key(c.get("year"), c.get("semester")),
        default=None,
    )
    if newest is None:
        return None
    return str(newest["year"]), str(newest["semester"]).upper().strip()


async def fetch_course_data(
    session: aiohttp.ClientSession,
    dept: str,
    number: str,
    pacer: RequestPacer,
) -> Dict[str, Any]:
    """Fetch one course, backing off on HTTP 429, 5xx and connection failures."""
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": "https://anex.us",
        "referer": ANEX_BASE_URL,
        "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    }
    data = {"dept": dept, "number": number}
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    last_error = "unknown error"

    for attempt in range(MAX_RETRIES + 1):
        retry_after = None
        await pacer.wait()
        try:
            async with session.post(
                ANEX_BASE_URL + "getData/", headers=headers, data=data, timeout=timeout
            ) as response:
                if response.status == 200:
                    return parse_course_response(await response.text())
                last_error = f"HTTP {response.status}"
                if response.status != 429 and response.status < 500:
                    return {"success": False, "error": last_error}
                retry_after = response.headers.get("Retry-After")
        except asyncio.TimeoutError:
            last_error = "Request timeout"
        except aiohttp.ClientSSLError as exc:
            return {"success": False, "error": str(exc)}
        except aiohttp.ClientError as exc:
            last_error = str(exc) or type(exc).__name__
        except (LookupError, ValueError) as exc:
            return {"success": False, "error": f"Invalid response: {exc}"}

        if attempt < MAX_RETRIES:
            delay = retry_delay(attempt, retry_after)
            print(
                f"[WARNING] {dept} {number}: {last_error}. Retrying in {delay:.0f}s.",
                flush=True,
            )
            await asyncio.sleep(delay)

    return {"success": False, "error": last_error, "retries_exhausted": True}


def parse_course_response(body: str) -> Dict[str, Any]:
    # anex.us returns this malformed JSON when a course has no grade data.
    if "".join(body.split()) == '{"classes":]}':
        return {"success": False, "no_data": True}
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return {"success": False, "error": "Invalid JSON response"}
    classes = payload.get("classes") if isinstance(payload, dict) else None
    if not isinstance(classes, list):
        return {"success": False, "error": "Expected a classes list"}
    if not classes:
        return {"success": False, "no_data": True}
    return {"success": True, "data": payload}


def extract_class_records(
    course_data: Dict[str, Any],
    skip_terms: Optional[Set[Tuple[str, str]]] = None,
    *,
    strict: bool = False,
) -> List[Dict[str, Any]]:
    """
    Extract individual class records from a course's data.

    skip_terms: (year, semester) pairs already stored for this course; records
    from those terms are dropped. None keeps every record.
    """
    if not course_data.get("success") or "data" not in course_data:
        return []

    records = []
    classes = course_data["data"].get("classes", [])

    for class_info in classes:
        try:
            if skip_terms and (
                str(class_info.get("year")),
                str(class_info.get("semester")),
            ) in skip_terms:
                continue

            # Calculate total students
            total_students = sum(
                [
                    int(class_info.get("A", 0)),
                    int(class_info.get("B", 0)),
                    int(class_info.get("C", 0)),
                    int(class_info.get("D", 0)),
                    int(class_info.get("F", 0)),
                    int(class_info.get("I", 0)),
                    int(class_info.get("S", 0)),
                    int(class_info.get("U", 0)),
                    int(class_info.get("Q", 0)),
                    int(class_info.get("X", 0)),
                ]
            )

            # Create unique ID
            unique_id = f"{class_info['dept']}_{class_info['number']}_{class_info['section']}_{class_info['year']}_{class_info['semester']}_{class_info['prof']}"

            gpa_record = {
                "id": unique_id,
                "dept": class_info["dept"],
                "course_number": class_info["number"],
                "section": class_info["section"],
                "professor": class_info["prof"],
                "year": class_info["year"],
                "semester": class_info["semester"],
                "gpa": float(class_info["gpa"]) if class_info["gpa"] != "" else None,
                "grade_a": int(class_info.get("A", 0)),
                "grade_b": int(class_info.get("B", 0)),
                "grade_c": int(class_info.get("C", 0)),
                "grade_d": int(class_info.get("D", 0)),
                "grade_f": int(class_info.get("F", 0)),
                "grade_i": int(class_info.get("I", 0)),
                "grade_s": int(class_info.get("S", 0)),
                "grade_u": int(class_info.get("U", 0)),
                "grade_q": int(class_info.get("Q", 0)),
                "grade_x": int(class_info.get("X", 0)),
                "total_students": total_students,
            }

            records.append(gpa_record)

        except (AttributeError, KeyError, TypeError, ValueError) as e:
            if strict:
                raise ValueError(f"Invalid class record: {e}") from e
            print(f"[WARNING] Error processing class record: {e}")
            continue

    return records
