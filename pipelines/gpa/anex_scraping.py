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
from typing import Any, Dict, List, Tuple

import aiohttp

# Configuration
MAX_CONCURRENT_REQUESTS = 10  # Concurrent API requests
REQUEST_TIMEOUT = 30
ANEX_BASE_URL = "https://anex.us/grades/"


async def get_newest_semester(session: aiohttp.ClientSession) -> Tuple[str, str]:
    """
    Query anex.us API to find the newest semester and year available.
    Uses a sample course (MATH 151) to get semester data.
    Returns (year, semester) tuple.
    """
    try:
        # Use a common course (MATH 151) to get semester data
        url = "https://anex.us/grades/getData/"
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-requested-with": "XMLHttpRequest",
            "referer": "https://anex.us/grades/?dept=MATH&number=151",
        }
        data = {"dept": "MATH", "number": "151"}

        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        async with session.post(url, headers=headers, data=data, timeout=timeout) as response:
            if response.status != 200:
                print(
                    f"[WARNING] Could not fetch semester data from anex.us API (status {response.status})"
                )
                return None, None

            try:
                text_response = await response.text()
                json_data = json.loads(text_response)

                if "classes" not in json_data or not json_data["classes"]:
                    print("[WARNING] No class data in API response")
                    return None, None

                # Find the newest semester from the classes data
                # Within same year: SUMMER (newest, priority=1) > SPRING (priority=2) > FALL (oldest, priority=3)
                # This is because Summer comes after Spring in the same calendar year
                newest_year = None
                newest_semester = None
                newest_year_int = 0
                newest_sem_priority = 999  # Lower priority number = newer semester

                semester_priority = {"SUMMER": 1, "SPRING": 2, "FALL": 3}

                # Debug: print first few classes to see what we're getting
                print(
                    f"[DEBUG] Processing {len(json_data['classes'])} classes from API"
                )
                sample_classes = json_data["classes"][:5]
                for sample in sample_classes:
                    print(
                        f"[DEBUG] Sample: year={sample.get('year')}, semester={sample.get('semester')}"
                    )

                for class_info in json_data["classes"]:
                    year_str = str(class_info.get("year", ""))
                    semester_str = str(class_info.get("semester", "")).upper().strip()

                    # Extract numeric year (handle "2025b" format)
                    year_match = re.search(r"(\d{4})", year_str)
                    if not year_match:
                        print(f"[DEBUG] Skipping class with invalid year: {year_str}")
                        continue
                    year_int = int(year_match.group(1))
                    semester_priority_val = semester_priority.get(semester_str, 999)

                    # Debug for potential newest semesters
                    if year_int >= 2024:
                        print(
                            f"[DEBUG] Checking: year={year_int} ({year_str}), semester={semester_str} (priority={semester_priority_val})"
                        )

                    # Check if this is newer
                    # Newer = higher year, or same year with lower priority (SUMMER=1 is newest in same year)
                    is_newer = False
                    if year_int > newest_year_int:
                        is_newer = True
                    elif year_int == newest_year_int:
                        if semester_priority_val < newest_sem_priority:
                            is_newer = True

                    if is_newer:
                        print(
                            f"[DEBUG] Found newer: {semester_str} {year_int} (was: {newest_semester} {newest_year_int})"
                        )
                        newest_year_int = year_int
                        newest_sem_priority = semester_priority_val
                        newest_year = year_match.group(1)  # Use just the numeric part
                        newest_semester = semester_str

                if newest_year and newest_semester:
                    print(
                        f"[INFO] Found newest semester from API: {newest_semester} {newest_year}"
                    )
                    return newest_year, newest_semester
                else:
                    print("[WARNING] Could not determine newest semester from API data")

            except json.JSONDecodeError as e:
                print(f"[WARNING] Invalid JSON response from API: {e}")
            except Exception as e:
                print(f"[WARNING] Error parsing API response: {e}")

    except Exception as e:
        print(f"[WARNING] Error fetching newest semester: {e}")
        import traceback

        traceback.print_exc()

    return None, None


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

        try:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            async with session.post(url, headers=headers, data=data, timeout=timeout) as response:
                if response.status == 200:
                    try:
                        text_response = await response.text()
                        json_data = json.loads(text_response)

                        if "classes" in json_data and json_data["classes"]:
                            return course_key, {
                                "success": True,
                                "data": json_data,
                                "course": (dept, number),
                            }
                        else:
                            return course_key, {
                                "success": False,
                                "error": "No data available",
                            }
                    except json.JSONDecodeError:
                        return course_key, {
                            "success": False,
                            "error": "Invalid JSON response",
                        }
                else:
                    return course_key, {
                        "success": False,
                        "error": f"HTTP {response.status}",
                    }

        except asyncio.TimeoutError:
            return course_key, {"success": False, "error": "Request timeout"}
        except Exception as e:
            return course_key, {"success": False, "error": str(e)}


def extract_class_records(
    course_data: Dict[str, Any], min_year: str = None, min_semester: str = None
) -> List[Dict[str, Any]]:
    """
    Extract individual class records from a course's data.
    Optionally filter to only include records newer than min_year/min_semester.

    Semester priority: Within same year: SUMMER=1 (newest) > SPRING=2 > FALL=3 (oldest)
    """
    if not course_data.get("success") or "data" not in course_data:
        return []

    records = []
    classes = course_data["data"].get("classes", [])

    # Semester priority for comparison (lower number = newer in same year)
    # Within same year: SUMMER=1 (newest), SPRING=2, FALL=3 (oldest)
    semester_priority = {"SUMMER": 1, "SPRING": 2, "FALL": 3}

    for class_info in classes:
        try:
            # Filter by year/semester if specified
            if min_year and min_semester:
                try:
                    # Extract numeric year from class_info (handle formats like "2025", "2025b", etc.)
                    class_year_str = str(class_info.get("year", "0"))
                    class_year_match = re.search(r"(\d{4})", class_year_str)
                    if class_year_match:
                        class_year = int(class_year_match.group(1))
                    else:
                        class_year = (
                            int(class_year_str) if class_year_str.isdigit() else 0
                        )

                    class_semester = class_info.get("semester", "").upper()

                    # Extract numeric year from min_year (handle formats like "2025", "2025b", etc.)
                    min_year_match = re.search(r"(\d{4})", str(min_year))
                    if min_year_match:
                        min_year_int = int(min_year_match.group(1))
                    else:
                        min_year_int = int(min_year) if str(min_year).isdigit() else 0

                    min_sem_priority = semester_priority.get(min_semester.upper(), 0)
                    class_sem_priority = semester_priority.get(class_semester, 0)

                    # Skip if older year
                    if class_year < min_year_int:
                        continue
                    # Skip if same year but older or equal semester
                    # (e.g., if we have Fall 2024, skip Spring 2024 and Fall 2024, but keep Spring 2025)
                    if (
                        class_year == min_year_int
                        and class_sem_priority >= min_sem_priority
                    ):
                        continue
                except (ValueError, TypeError) as e:
                    # If we can't parse the year, include the record to be safe
                    print(
                        f"[WARNING] Error parsing year/semester for filtering: {e}, including record"
                    )
                    pass

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
    courses: List[Tuple[str, str, str, str]],
) -> List[Dict[str, Any]]:
    """
    Fetch data for all courses concurrently.
    """
    print(f"\n[INFO] Starting concurrent fetch of {len(courses)} courses...")
    print(f"[INFO] Using {MAX_CONCURRENT_REQUESTS} concurrent connections")

    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    # Create connector with appropriate limits
    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS * 2, limit_per_host=MAX_CONCURRENT_REQUESTS
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        # Create tasks for all courses
        tasks = [
            fetch_course_data(session, dept, number, semaphore)
            for dept, number, _, _ in courses
        ]

        print(f"[INFO] Created {len(tasks)} concurrent fetch tasks")

        # Execute all tasks concurrently
        import time

        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        duration = time.time() - start_time

        print(f"[INFO] Fetch completed in {duration:.1f} seconds")
        print(f"[INFO] Average: {len(courses) / duration:.1f} requests/second")

        # Process results
        successful_responses = []
        failed_count = 0

        for result in results:
            if isinstance(result, Exception):
                failed_count += 1
                print(f"[ERROR] Task failed with exception: {result}")
            else:
                course_key, response = result
                if response.get("success"):
                    successful_responses.append(response)
                else:
                    failed_count += 1

        print(f"[OK] Successful: {len(successful_responses)}")
        print(f"[ERROR] Failed: {failed_count}")

        return successful_responses

