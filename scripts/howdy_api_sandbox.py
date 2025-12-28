"""
Howdy Portal API sandbox

This is a standalone script so you can play around with Howdy endpoints before
you integrate anything into the FastAPI app.

Usage examples:
  python scripts/howdy_api_sandbox.py terms --semester "Spring 2026"
  python scripts/howdy_api_sandbox.py term-info --term 202511
  python scripts/howdy_api_sandbox.py instructors --term 202511
  python scripts/howdy_api_sandbox.py course --term 202511 --course "CSCE 221"
  python scripts/howdy_api_sandbox.py instructor --term 202511 --name "DOE, JOHN"
  python scripts/howdy_api_sandbox.py section --term 202511 --crn 30835

Notes:
  - These endpoints may require TAMU auth/cookies. If you get 401/403, that's expected.
  - Keep this in scripts/ until you're happy with the behavior.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiohttp
import requests

# Allow running as a script: `python scripts/howdy_api_sandbox.py ...`
# by ensuring project root is on sys.path so we can import from scripts/.
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.howdy_helpers import recursive_parse_json


ALL_TERMS_URL = "https://howdyportal.tamu.edu/api/all-terms"
CLASS_LIST_URL = "https://howdyportal.tamu.edu/api/course-sections"


DEFAULT_SEMESTERS = [
    "Spring 2026",
]


def _semester_sort_key(semester: str) -> int:
    # Matches the style you were using elsewhere: fall > spring > summer
    s = (semester or "").strip().lower()
    if s == "fall":
        return 1
    if s == "spring":
        return 2
    if s == "summer":
        return 3
    return 999


@dataclass
class HowdyConfig:
    semesters_filter: List[str]
    timeout_s: float = 10.0


class HowdyAPI:
    def __init__(self, config: Optional[HowdyConfig] = None):
        self.config = config or HowdyConfig(semesters_filter=list(DEFAULT_SEMESTERS))
        self.terms: List[Dict[str, Any]] = []
        self.term_codes_to_desc: Dict[str, str] = {}
        self.classes_by_term: Dict[str, List[Dict[str, Any]]] = {}

    # -------------------------
    # Basic endpoints (sync)
    # -------------------------
    def get_all_terms(self, current: bool = True) -> List[Dict[str, Any]]:
        res = requests.get(ALL_TERMS_URL, timeout=self.config.timeout_s)
        if res.status_code != 200:
            raise RuntimeError(f"Failed to fetch term data from {ALL_TERMS_URL}: {res.status_code}")
        data = res.json()
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected terms payload shape: {type(data)}")

        if not current:
            return data

        semesters = self.config.semesters_filter
        return [
            term
            for term in data
            if any(sem in str(term.get("STVTERM_DESC", "")) for sem in semesters)
        ]

    def get_classes(self, term_code: str) -> List[Dict[str, Any]]:
        res = requests.post(
            CLASS_LIST_URL,
            json={"termCode": term_code},
            timeout=self.config.timeout_s,
        )
        if res.status_code != 200:
            raise RuntimeError(
                f"Failed to fetch class data from {CLASS_LIST_URL}: {res.status_code}"
            )
        data = res.json()
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected classes payload shape: {type(data)}")
        return data

    def load(self, current: bool = True) -> None:
        """
        Load terms and classes into memory (like your original __init__).
        """
        self.terms = self.get_all_terms(current=current)
        self.term_codes_to_desc = {
            str(term.get("STVTERM_CODE")): str(term.get("STVTERM_DESC"))
            for term in self.terms
            if term.get("STVTERM_CODE") is not None
        }
        self.classes_by_term = {
            str(term.get("STVTERM_CODE")): self.get_classes(str(term.get("STVTERM_CODE")))
            for term in self.terms
            if term.get("STVTERM_CODE") is not None
        }

    # -------------------------
    # Convenience transforms
    # -------------------------
    def get_term_general_info(self, term_code: str) -> List[Tuple[str, str, str, str]]:
        class_list: set[Tuple[str, str, str, str]] = set()
        classes = self.classes_by_term.get(term_code) or []
        for c in classes:
            class_list.add(
                (
                    str(c.get("SWV_CLASS_SEARCH_SUBJECT", "")),
                    str(c.get("SWV_CLASS_SEARCH_COURSE", "")),
                    str(c.get("SWV_CLASS_SEARCH_SECTION", "")),
                    str(c.get("SWV_CLASS_SEARCH_CRN", "")),
                )
            )
        return sorted(class_list, key=lambda x: (x[0], x[1], x[2], x[3]))

    def filter_by_course(self, term_code: str, course: str) -> List[Dict[str, Any]]:
        major, number = course.split(" ", 1)
        out: List[Dict[str, Any]] = []
        for c in self.classes_by_term.get(term_code, []):
            if str(c.get("SWV_CLASS_SEARCH_SUBJECT", "")).lower() == major.lower() and str(
                c.get("SWV_CLASS_SEARCH_COURSE", "")
            ) == str(number):
                out.append(c)
        # open sections first
        return sorted(out, key=lambda x: x.get("STUSEAT_OPEN") != "Y")

    def get_all_instructors(self, term_code: str) -> List[str]:
        out: set[str] = set()
        for c in self.classes_by_term.get(term_code, []):
            raw = c.get("SWV_CLASS_SEARCH_INSTRCTR_JSON")
            if raw is None:
                continue
            instructors = recursive_parse_json(raw)
            if isinstance(instructors, list):
                for i in instructors:
                    if isinstance(i, dict) and "NAME" in i:
                        out.add(str(i["NAME"]))
        return sorted(out)

    def filter_by_instructor(self, term_code: str, instructor_name: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        cv_url: Optional[str] = None
        out: List[Dict[str, Any]] = []

        for c in self.classes_by_term.get(term_code, []):
            raw = c.get("SWV_CLASS_SEARCH_INSTRCTR_JSON")
            if raw is None:
                continue
            instructors = recursive_parse_json(raw)
            if not isinstance(instructors, list):
                continue

            for i in instructors:
                if not isinstance(i, dict):
                    continue
                if instructor_name == i.get("NAME"):
                    out.append(c)
                    if i.get("HAS_CV") == "Y" and i.get("MORE"):
                        # NOTE: your original code had nested quotes that would syntax-error
                        cv_url = (
                            "https://compass-ssb.tamu.edu/pls/PROD/bwykfupd.p_showdoc"
                            f"?doctype_in=CV&pidm_in={i.get('MORE')}"
                        )
                    break

        return out, cv_url

    # -------------------------
    # Section details (async)
    # -------------------------
    async def get_section_details(self, term_code: str, crn: str) -> Dict[str, Any]:
        errors: List[str] = []

        links = {
            "Section attributes": "https://howdyportal.tamu.edu/api/section-attributes",
            "Section prereqs": "https://howdyportal.tamu.edu/api/section-prereqs",
            "Bookstore links": "https://howdyportal.tamu.edu/api/section-bookstore-links",
            "Meeting times with profs": "https://howdyportal.tamu.edu/api/section-meeting-times-with-profs",
            "Section program restrictions": "https://howdyportal.tamu.edu/api/section-program-restrictions",
            "Section college restrictions": "https://howdyportal.tamu.edu/api/section-college-restrictions",
            "Level restrictions": "https://howdyportal.tamu.edu/api/section-level-restrictions",
            "Degree restrictions": "https://howdyportal.tamu.edu/api/section-degree-restrictions",
            "Major restrictions": "https://howdyportal.tamu.edu/api/section-major-restrictions",
            "Minor restrictions": "https://howdyportal.tamu.edu/api/section-minor-restrictions",
            "Concentrations restrictions": "https://howdyportal.tamu.edu/api/section-concentrations-restrictions",
            "Field of study restrictions": "https://howdyportal.tamu.edu/api/section-field-of-study-restrictions",
            "Department restrictions": "https://howdyportal.tamu.edu/api/section-department-restrictions",
            "Cohort restrictions": "https://howdyportal.tamu.edu/api/section-cohort-restrictions",
            "Student attribute restrictions": "https://howdyportal.tamu.edu/api/section-student-attribute-restrictions",
            "Classification restrictions": "https://howdyportal.tamu.edu/api/section-classifications-restrictions",
            "Campus restrictions": "https://howdyportal.tamu.edu/api/section-campus-restrictions",
        }

        general_info_link = (
            "https://howdyportal.tamu.edu/api/course-section-details"
            f"?term={term_code}&subject=&course=&crn={crn}"
        )

        out: Dict[str, Any] = {}

        timeout = aiohttp.ClientTimeout(total=self.config.timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.get(general_info_link) as response:
                    if response.status != 200:
                        errors.append(f"Failed to fetch general info: {response.status}")
                    else:
                        general_info = await response.json()
                        if not general_info:
                            errors.append(f"Empty general info payload for {term_code=} {crn=}")
                        else:
                            general_info["COURSE_NAME"] = f"{general_info.get('DEPT','')} {general_info.get('COURSE_NUMBER','')}".strip()
                            out.update(general_info)
            except Exception as e:
                errors.append(f"Exception when fetching general info: {e}")

            if not out:
                out["ERRORS"] = errors
                return out

            out["OTHER_ATTRIBUTES"] = {}

            async def fetch_data(key: str, link: str) -> None:
                try:
                    async with session.post(
                        link,
                        json={"term": term_code, "subject": None, "course": None, "crn": crn},
                    ) as res:
                        if res.status != 200:
                            errors.append(f"Failed to fetch {key}: {res.status}")
                            out["OTHER_ATTRIBUTES"][key] = {}
                            return
                        text_body = await res.text()
                        out["OTHER_ATTRIBUTES"][key] = recursive_parse_json(text_body)
                except Exception as exc:
                    errors.append(f"{key} generated an exception: {exc}")
                    out["OTHER_ATTRIBUTES"][key] = {}

            await asyncio.gather(*[fetch_data(k, v) for k, v in links.items()])

        out["SYLLABUS"] = (
            "https://compass-ssb.tamu.edu/pls/PROD/bwykfupd.p_showdoc"
            f"?doctype_in=SY&crn_in={crn}&termcode_in={term_code}"
        )

        # Try to set instructor/CV from meeting-times payload
        mt = out.get("OTHER_ATTRIBUTES", {}).get("Meeting times with profs") or {}
        try:
            instr_json = mt.get("SWV_CLASS_SEARCH_INSTRCTR_JSON")
            if isinstance(instr_json, list) and instr_json and isinstance(instr_json[0], dict):
                instructor_info = instr_json[0]
                name = str(instructor_info.get("NAME", "Not assigned")).rstrip(" (P)")
                out["INSTRUCTOR"] = name
                if instructor_info.get("MORE"):
                    instructor_info["CV"] = (
                        "https://compass-ssb.tamu.edu/pls/PROD/bwykfupd.p_showdoc"
                        f"?doctype_in=CV&pidm_in={instructor_info.get('MORE')}"
                    )
            else:
                out["INSTRUCTOR"] = "Not assigned"
        except Exception:
            out["INSTRUCTOR"] = "Not assigned"

        # Flatten OTHER_ATTRIBUTES to top-level keys (like your original)
        for k, v in (out.get("OTHER_ATTRIBUTES") or {}).items():
            out[k] = v
        out.pop("OTHER_ATTRIBUTES", None)

        # Also flatten meeting-times payload into top-level (like your original)
        if isinstance(out.get("Meeting times with profs"), dict):
            out.update(out["Meeting times with profs"])
            out.pop("Meeting times with profs", None)

        # Parse SWV_CLASS_SEARCH_JSON_CLOB into a readable message
        clob = out.get("SWV_CLASS_SEARCH_JSON_CLOB")
        if isinstance(clob, list):
            meeting_parts: List[str] = []
            for meeting in clob:
                if not isinstance(meeting, dict):
                    continue
                days = [
                    meeting[day]
                    for day in [
                        "SSRMEET_SUN_DAY",
                        "SSRMEET_MON_DAY",
                        "SSRMEET_TUE_DAY",
                        "SSRMEET_WED_DAY",
                        "SSRMEET_THU_DAY",
                        "SSRMEET_FRI_DAY",
                        "SSRMEET_SAT_DAY",
                    ]
                    if meeting.get(day)
                ]
                day_str = "".join(days) if days else "N/A"
                time_str = f"{meeting.get('SSRMEET_BEGIN_TIME', 'N/A')} - {meeting.get('SSRMEET_END_TIME', 'N/A')}"
                location_str = f"{meeting.get('SSRMEET_BLDG_CODE', 'N/A')} {meeting.get('SSRMEET_ROOM_CODE', 'N/A')}"
                mtyp = meeting.get("SSRMEET_MTYP_CODE", "N/A")
                meeting_parts.append(f"{mtyp}: {day_str} {time_str} at {location_str}")
            out["MEETING_MESSAGE"] = "\n".join(meeting_parts)

        out["ERRORS"] = errors
        return out


def _print_json(data: Any) -> None:
    print(json.dumps(data, indent=2, ensure_ascii=False))


def main() -> int:
    parser = argparse.ArgumentParser(description="Howdy Portal API sandbox")
    # Make semester filtering available for all commands (not just `terms`)
    parser.add_argument(
        "--semester",
        action="append",
        default=[],
        help="Semester description filter (repeatable). Used by some commands to filter terms.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_terms = sub.add_parser("terms", help="List terms (optionally filtered by semester descriptions)")
    p_terms.add_argument("--all", action="store_true", help="Do not filter by semesters")

    p_term_info = sub.add_parser("term-info", help="List basic section tuples for a term")
    p_term_info.add_argument("--term", required=True, help="Term code (e.g., 202511)")

    p_instructors = sub.add_parser("instructors", help="List instructors for a term")
    p_instructors.add_argument("--term", required=True)

    p_course = sub.add_parser("course", help="List sections for a course in a term")
    p_course.add_argument("--term", required=True)
    p_course.add_argument("--course", required=True, help='Course like "CSCE 221"')

    p_instr = sub.add_parser("instructor", help="Filter sections by instructor name for a term")
    p_instr.add_argument("--term", required=True)
    p_instr.add_argument("--name", required=True, help='Exact instructor NAME as Howdy returns it')

    p_section = sub.add_parser("section", help="Fetch section details (async fan-out)")
    p_section.add_argument("--term", required=True)
    p_section.add_argument("--crn", required=True)

    p_dump = sub.add_parser("dump-classes", help="Dump raw classes JSON for a term to a file")
    p_dump.add_argument("--term", required=True)
    p_dump.add_argument("--out", default="howdy_classes.json")

    args = parser.parse_args()

    semesters = (getattr(args, "semester", None) or []) or list(DEFAULT_SEMESTERS)
    api = HowdyAPI(HowdyConfig(semesters_filter=semesters))

    if args.cmd == "terms":
        terms = api.get_all_terms(current=not args.all)
        _print_json(terms)
        return 0

    # For all other commands, we want loaded classes for at least one term.
    # If you pass a term that isn't in the filtered terms list, we still try to fetch it.
    if args.cmd in {"term-info", "instructors", "course", "instructor", "dump-classes"}:
        term_code = args.term
        api.classes_by_term[term_code] = api.get_classes(term_code)

    if args.cmd == "term-info":
        _print_json(api.get_term_general_info(args.term))
        return 0

    if args.cmd == "instructors":
        _print_json(api.get_all_instructors(args.term))
        return 0

    if args.cmd == "course":
        _print_json(api.filter_by_course(args.term, args.course))
        return 0

    if args.cmd == "instructor":
        classes, cv = api.filter_by_instructor(args.term, args.name)
        _print_json({"count": len(classes), "cv": cv, "classes": classes})
        return 0

    if args.cmd == "dump-classes":
        data = api.get_classes(args.term)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Wrote {len(data)} classes to {args.out}")
        return 0

    if args.cmd == "section":
        out = asyncio.run(api.get_section_details(args.term, args.crn))
        _print_json(out)
        return 0

    raise RuntimeError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())


