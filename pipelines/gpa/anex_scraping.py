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
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
import certifi

# Configuration
MAX_CONCURRENT_REQUESTS = 10  # Concurrent API requests
REQUEST_TIMEOUT = 30
MAX_RETRIES = 2  # Retries on timeouts, connection errors, 429 and 5xx
ANEX_BASE_URL = "https://anex.us/grades/"

# Verify TLS against certifi's CA bundle instead of the OS store. Some Windows
# trust stores pick an expired chain for anex.us's Let's Encrypt cert and every
# request fails with CERTIFICATE_VERIFY_FAILED.
SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


def make_http_session(concurrency: int = MAX_CONCURRENT_REQUESTS) -> aiohttp.ClientSession:
    connector = aiohttp.TCPConnector(
        ssl=SSL_CONTEXT, limit=concurrency * 2, limit_per_host=concurrency
    )
    return aiohttp.ClientSession(connector=connector)


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
) -> Optional[Tuple[str, str]]:
    """
    Query anex.us to find the newest (year, semester) available, using MATH 151
    as a sample course since it is offered every term. Returns None on failure.
    """
    _, response = await fetch_course_data(
        session, "MATH", "151", asyncio.Semaphore(1)
    )
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
    semaphore: asyncio.Semaphore,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch GPA data for a single course from anex.us.
    """
    course_key = f"{dept}_{number}"

    async with semaphore:
        url = "https://anex.us/grades/getData/"

        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://anex.us",
            "referer": "https://anex.us/grades/",
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
            if attempt:
                await asyncio.sleep(2**attempt)
            try:
                async with session.post(
                    url, headers=headers, data=data, timeout=timeout
                ) as response:
                    if response.status == 429 or response.status >= 500:
                        last_error = f"HTTP {response.status}"
                        continue
                    if response.status != 200:
                        return course_key, {
                            "success": False,
                            "error": f"HTTP {response.status}",
                        }
                    text_response = await response.text()
            except asyncio.TimeoutError:
                last_error = "Request timeout"
                continue
            except aiohttp.ClientSSLError as e:
                # Certificate problems won't fix themselves on retry
                return course_key, {"success": False, "error": str(e)}
            except aiohttp.ClientError as e:
                last_error = str(e) or type(e).__name__
                continue
            except Exception as e:
                # Anything else (e.g. a body that won't decode) fails this
                # course only; the caller's as_completed loop would otherwise
                # re-raise it and stop the whole run.
                return course_key, {
                    "success": False,
                    "error": f"{type(e).__name__}: {e}",
                }

            # anex.us answers courses with no grade data with this invalid JSON
            if "".join(text_response.split()) == '{"classes":]}':
                return course_key, {
                    "success": False,
                    "no_data": True,
                    "error": "No data available",
                }
            try:
                json_data = json.loads(text_response)
            except json.JSONDecodeError:
                return course_key, {"success": False, "error": "Invalid JSON response"}

            if json_data.get("classes"):
                return course_key, {
                    "success": True,
                    "data": json_data,
                    "course": (dept, number),
                }
            return course_key, {
                "success": False,
                "no_data": True,
                "error": "No data available",
            }

        return course_key, {"success": False, "error": last_error}


def extract_class_records(
    course_data: Dict[str, Any],
    skip_terms: Optional[Set[Tuple[str, str]]] = None,
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

        except Exception as e:
            print(f"[WARNING] Error processing class record: {e}")
            continue

    return records


async def fetch_all_courses_concurrent(
    courses: List[Tuple[str, str]],
) -> List[Dict[str, Any]]:
    """
    Fetch data for all (dept, number) courses concurrently, printing progress.
    """
    print(f"[INFO] Fetching {len(courses)} courses with {MAX_CONCURRENT_REQUESTS} concurrent connections...")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    successful_responses: List[Dict[str, Any]] = []
    no_data = 0
    errors: List[str] = []
    start_time = time.time()
    progress_every = max(1, len(courses) // 20)

    async with make_http_session() as session:
        tasks = [
            fetch_course_data(session, dept, number, semaphore)
            for dept, number in courses
        ]
        for done, future in enumerate(asyncio.as_completed(tasks), start=1):
            course_key, response = await future
            if response.get("success"):
                successful_responses.append(response)
            elif response.get("no_data"):
                no_data += 1
            else:
                errors.append(f"{course_key}: {response.get('error')}")

            if done % progress_every == 0 or done == len(courses):
                elapsed = time.time() - start_time
                rate = done / elapsed if elapsed else 0
                eta = (len(courses) - done) / rate if rate else 0
                print(
                    f"  > {done}/{len(courses)} fetched "
                    f"({rate:.1f}/s, ~{eta:.0f}s left, {len(errors)} errors)",
                    flush=True,
                )

    print(f"[OK] With data: {len(successful_responses)}")
    print(f"[INFO] No data on anex.us: {no_data}")
    if errors:
        print(f"[WARNING] Errors after retries: {len(errors)}")
        for err in errors[:10]:
            print(f"    {err}")
        if len(errors) > 10:
            print(f"    ... and {len(errors) - 10} more")

    return successful_responses
