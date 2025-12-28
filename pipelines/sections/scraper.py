"""
Scraper for Howdy API to fetch section data.

Fetches:
- All available terms
- All sections for each term
- Section details including instructors and meeting times
- Detailed section information (attributes, prereqs, restrictions, bookstore links)
"""

import asyncio
import aiohttp
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime

from .schemas import (
    SectionSchema, TermSchema,
    SectionAttributeDetailedSchema, SectionPrereqSchema,
    SectionRestrictionSchema, SectionBookstoreLinkSchema,
    SectionDetailsSchema
)

# API endpoints
ALL_TERMS_URL = "https://howdyportal.tamu.edu/api/all-terms"
CLASS_LIST_URL = "https://howdyportal.tamu.edu/api/course-sections"

# Detail endpoints (require term and crn)
SECTION_DETAIL_ENDPOINTS = {
    "attributes": "https://howdyportal.tamu.edu/api/section-attributes",
    "prereqs": "https://howdyportal.tamu.edu/api/section-prereqs",
    "bookstore_links": "https://howdyportal.tamu.edu/api/section-bookstore-links",
    "program_restrictions": "https://howdyportal.tamu.edu/api/section-program-restrictions",
    "college_restrictions": "https://howdyportal.tamu.edu/api/section-college-restrictions",
    "level_restrictions": "https://howdyportal.tamu.edu/api/section-level-restrictions",
    "degree_restrictions": "https://howdyportal.tamu.edu/api/section-degree-restrictions",
    "major_restrictions": "https://howdyportal.tamu.edu/api/section-major-restrictions",
    "minor_restrictions": "https://howdyportal.tamu.edu/api/section-minor-restrictions",
    "concentration_restrictions": "https://howdyportal.tamu.edu/api/section-concentrations-restrictions",
    "field_of_study_restrictions": "https://howdyportal.tamu.edu/api/section-field-of-study-restrictions",
    "department_restrictions": "https://howdyportal.tamu.edu/api/section-department-restrictions",
    "cohort_restrictions": "https://howdyportal.tamu.edu/api/section-cohort-restrictions",
    "student_attribute_restrictions": "https://howdyportal.tamu.edu/api/section-student-attribute-restrictions",
    "classification_restrictions": "https://howdyportal.tamu.edu/api/section-classifications-restrictions",
    "campus_restrictions": "https://howdyportal.tamu.edu/api/section-campus-restrictions",
}

# Mapping from endpoint name to restriction type
RESTRICTION_TYPE_MAP = {
    "program_restrictions": "program",
    "college_restrictions": "college",
    "level_restrictions": "level",
    "degree_restrictions": "degree",
    "major_restrictions": "major",
    "minor_restrictions": "minor",
    "concentration_restrictions": "concentration",
    "field_of_study_restrictions": "field_of_study",
    "department_restrictions": "department",
    "cohort_restrictions": "cohort",
    "student_attribute_restrictions": "student_attribute",
    "classification_restrictions": "classification",
    "campus_restrictions": "campus",
}

# Request timeout
REQUEST_TIMEOUT = 30


def get_all_terms(current_only: bool = True, semester_filter: Optional[List[str]] = None) -> List[TermSchema]:
    """
    Fetch all available terms from Howdy API.
    
    Args:
        current_only: If True, only return current/upcoming semesters
        semester_filter: Optional list of semester descriptions to filter by
                        (e.g., ["Spring 2026", "Fall 2025"])
    
    Returns:
        List of TermSchema objects
    """
    try:
        response = requests.get(ALL_TERMS_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        terms_data = response.json()
        
        terms = []
        for term_data in terms_data:
            term = TermSchema.from_api(term_data)
            
            # Apply filter if specified
            if semester_filter:
                if any(sem in term.term_desc for sem in semester_filter):
                    terms.append(term)
            elif current_only:
                # Default: get current year and next year terms
                current_year = datetime.now().year
                try:
                    # Term codes are like "202611" where 2026 is the year
                    term_year = int(term.term_code[:4])
                    if term_year >= current_year:
                        terms.append(term)
                except (ValueError, IndexError):
                    pass
            else:
                terms.append(term)
        
        return terms
        
    except requests.RequestException as e:
        print(f"Error fetching terms: {e}")
        return []


def get_sections_for_term(term_code: str) -> List[SectionSchema]:
    """
    Fetch all sections for a specific term.
    
    Args:
        term_code: Term code (e.g., "202611" for Spring 2026)
    
    Returns:
        List of SectionSchema objects
    """
    try:
        response = requests.post(
            CLASS_LIST_URL,
            json={"termCode": term_code},
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        sections_data = response.json()
        
        sections = []
        for section_data in sections_data:
            try:
                section = SectionSchema.from_api(section_data, term_code)
                sections.append(section)
            except Exception as e:
                crn = section_data.get("SWV_CLASS_SEARCH_CRN", "unknown")
                print(f"Error parsing section CRN {crn}: {e}")
                continue
        
        return sections
        
    except requests.RequestException as e:
        print(f"Error fetching sections for term {term_code}: {e}")
        return []


def get_all_sections(
    term_codes: Optional[List[str]] = None,
    semester_filter: Optional[List[str]] = None,
    max_workers: int = 4
) -> Dict[str, List[SectionSchema]]:
    """
    Fetch all sections for multiple terms in parallel.
    
    Args:
        term_codes: Optional list of term codes to fetch. If None, fetches current terms.
        semester_filter: Optional filter for semester descriptions
        max_workers: Number of parallel workers
    
    Returns:
        Dict mapping term_code to list of SectionSchema objects
    """
    # Get terms if not specified
    if term_codes is None:
        terms = get_all_terms(current_only=True, semester_filter=semester_filter)
        term_codes = [term.term_code for term in terms]
    
    if not term_codes:
        print("No terms to fetch")
        return {}
    
    print(f"Fetching sections for {len(term_codes)} term(s): {term_codes}")
    
    results = {}
    
    # Fetch sections in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_term = {
            executor.submit(get_sections_for_term, term_code): term_code
            for term_code in term_codes
        }
        
        for future in as_completed(future_to_term):
            term_code = future_to_term[future]
            try:
                sections = future.result()
                results[term_code] = sections
                print(f"  Term {term_code}: {len(sections)} sections")
            except Exception as e:
                print(f"  Term {term_code}: Error - {e}")
                results[term_code] = []
    
    total = sum(len(s) for s in results.values())
    print(f"Total: {total} sections across {len(results)} terms")
    
    return results


def get_sections_by_department(
    term_code: str,
    dept: str
) -> List[SectionSchema]:
    """
    Get sections for a specific department in a term.
    
    Args:
        term_code: Term code
        dept: Department code (e.g., "CSCE")
    
    Returns:
        List of SectionSchema objects for the department
    """
    all_sections = get_sections_for_term(term_code)
    return [s for s in all_sections if s.dept.upper() == dept.upper()]


def get_sections_by_course(
    term_code: str,
    dept: str,
    course_number: str
) -> List[SectionSchema]:
    """
    Get sections for a specific course in a term.
    
    Args:
        term_code: Term code
        dept: Department code (e.g., "CSCE")
        course_number: Course number (e.g., "121")
    
    Returns:
        List of SectionSchema objects for the course
    """
    all_sections = get_sections_for_term(term_code)
    return [
        s for s in all_sections
        if s.dept.upper() == dept.upper() and s.course_number == course_number
    ]


def get_section_statistics(sections: List[SectionSchema]) -> Dict[str, Any]:
    """
    Calculate statistics for a list of sections.
    
    Args:
        sections: List of SectionSchema objects
    
    Returns:
        Dictionary with statistics
    """
    if not sections:
        return {
            "total_sections": 0,
            "open_sections": 0,
            "closed_sections": 0,
            "total_seats": 0,
            "enrolled": 0,
            "available_seats": 0,
            "departments": set(),
            "courses": set(),
            "schedule_types": set(),
            "instruction_types": set(),
        }
    
    open_sections = [s for s in sections if s.is_open]
    closed_sections = [s for s in sections if not s.is_open]
    
    total_seats = sum(s.max_enrollment or 0 for s in sections)
    enrolled = sum(s.current_enrollment or 0 for s in sections)
    available = sum(s.seats_available or 0 for s in sections)
    
    departments = set(s.dept for s in sections)
    courses = set(f"{s.dept} {s.course_number}" for s in sections)
    schedule_types = set(s.schedule_type for s in sections if s.schedule_type)
    instruction_types = set(s.instruction_type for s in sections if s.instruction_type)
    
    return {
        "total_sections": len(sections),
        "open_sections": len(open_sections),
        "closed_sections": len(closed_sections),
        "total_seats": total_seats,
        "enrolled": enrolled,
        "available_seats": available,
        "fill_rate": enrolled / total_seats if total_seats > 0 else 0,
        "departments": len(departments),
        "unique_courses": len(courses),
        "schedule_types": list(schedule_types),
        "instruction_types": list(instruction_types),
    }


# ============================================================================
# Async Section Details Fetching
# ============================================================================

def _recursive_parse_json(data):
    """Recursively parse JSON strings within data."""
    if isinstance(data, str):
        try:
            parsed = json.loads(data)
            return _recursive_parse_json(parsed)
        except (json.JSONDecodeError, TypeError):
            return data
    elif isinstance(data, dict):
        return {k: _recursive_parse_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_recursive_parse_json(item) for item in data]
    return data


async def _fetch_section_detail_endpoint(
    session: aiohttp.ClientSession,
    endpoint_name: str,
    endpoint_url: str,
    term_code: str,
    crn: str,
    semaphore: asyncio.Semaphore
) -> Tuple[str, Any]:
    """Fetch a single detail endpoint for a section."""
    async with semaphore:
        try:
            async with session.post(
                endpoint_url,
                json={
                    "term": term_code,
                    "subject": None,
                    "course": None,
                    "crn": crn,
                },
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status != 200:
                    return endpoint_name, None
                text = await response.text()
                data = _recursive_parse_json(text)
                return endpoint_name, data
        except Exception as e:
            return endpoint_name, None


async def _fetch_all_details_for_section(
    session: aiohttp.ClientSession,
    section_id: str,
    term_code: str,
    crn: str,
    semaphore: asyncio.Semaphore
) -> SectionDetailsSchema:
    """Fetch all detail endpoints for a single section and parse into schemas."""
    tasks = [
        _fetch_section_detail_endpoint(session, name, url, term_code, crn, semaphore)
        for name, url in SECTION_DETAIL_ENDPOINTS.items()
    ]
    
    results = await asyncio.gather(*tasks)
    
    # Parse results into schemas
    attributes = []
    restrictions = []
    prereqs = None
    bookstore_link = None
    
    for name, data in results:
        if data is None:
            continue
        
        if name == "attributes":
            # Parse attributes
            if isinstance(data, list):
                for attr_data in data:
                    attr = SectionAttributeDetailedSchema.from_api(
                        attr_data, section_id, term_code, crn
                    )
                    attributes.append(attr)
        
        elif name == "prereqs":
            # Parse prereqs
            prereqs = SectionPrereqSchema.from_api(data, section_id, term_code, crn)
        
        elif name == "bookstore_links":
            # Parse bookstore link
            bookstore_link = SectionBookstoreLinkSchema.from_api(
                data, section_id, term_code, crn
            )
        
        elif name in RESTRICTION_TYPE_MAP:
            # Parse restrictions
            restriction_type = RESTRICTION_TYPE_MAP[name]
            if isinstance(data, list):
                for idx, restr_data in enumerate(data):
                    restr = SectionRestrictionSchema.from_api(
                        restr_data, restriction_type, idx, section_id, term_code, crn
                    )
                    restrictions.append(restr)
    
    return SectionDetailsSchema(
        section_id=section_id,
        term_code=term_code,
        crn=crn,
        attributes=attributes,
        prereqs=prereqs,
        restrictions=restrictions,
        bookstore_link=bookstore_link
    )


async def fetch_section_details_batch(
    sections: List[SectionSchema],
    max_concurrent: int = 50,
    progress_callback: Optional[callable] = None
) -> List[SectionDetailsSchema]:
    """
    Fetch detailed information for a batch of sections.
    
    Args:
        sections: List of SectionSchema objects to fetch details for
        max_concurrent: Maximum concurrent requests
        progress_callback: Optional callback(completed, total) for progress updates
    
    Returns:
        List of SectionDetailsSchema objects
    """
    if not sections:
        return []
    
    semaphore = asyncio.Semaphore(max_concurrent)
    results = []
    
    async with aiohttp.ClientSession() as session:
        batch_size = 100
        total = len(sections)
        
        for i in range(0, total, batch_size):
            batch = sections[i:i + batch_size]
            
            tasks = [
                _fetch_all_details_for_section(
                    session,
                    section.id,
                    section.term_code,
                    section.crn,
                    semaphore
                )
                for section in batch
            ]
            
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)
            
            # Progress callback
            completed = min(i + batch_size, total)
            if progress_callback:
                progress_callback(completed, total)
    
    return results


def fetch_section_details_sync(
    sections: List[SectionSchema],
    max_concurrent: int = 50,
    progress_callback: Optional[callable] = None
) -> List[SectionDetailsSchema]:
    """
    Synchronous wrapper for fetch_section_details_batch.
    
    Args:
        sections: List of SectionSchema objects to fetch details for
        max_concurrent: Maximum concurrent requests
        progress_callback: Optional callback(completed, total) for progress updates
    
    Returns:
        List of SectionDetailsSchema objects
    """
    return asyncio.run(
        fetch_section_details_batch(sections, max_concurrent, progress_callback)
    )


if __name__ == "__main__":
    # Example usage
    print("Fetching available terms...")
    terms = get_all_terms(current_only=True)
    print(f"Found {len(terms)} current/upcoming terms:")
    for term in terms:
        print(f"  {term.term_desc} ({term.term_code})")
    
    if terms:
        # Fetch sections for the first term
        term = terms[0]
        print(f"\nFetching sections for {term.term_desc}...")
        sections = get_sections_for_term(term.term_code)
        
        stats = get_section_statistics(sections)
        print(f"\nStatistics:")
        print(f"  Total sections: {stats['total_sections']}")
        print(f"  Open: {stats['open_sections']}, Closed: {stats['closed_sections']}")
        print(f"  Enrolled: {stats['enrolled']} / {stats['total_seats']} ({stats['fill_rate']:.1%})")
        print(f"  Departments: {stats['departments']}")
        print(f"  Unique courses: {stats['unique_courses']}")
        print(f"  Schedule types: {stats['schedule_types']}")
        print(f"  Instruction types: {stats['instruction_types']}")

