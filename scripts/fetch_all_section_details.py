"""
Fetch all section details from Howdy Portal API endpoints.
This script fetches comprehensive data for all sections in a term including:
- Section attributes
- Prerequisites
- Bookstore links
- Meeting times with professors
- Various restrictions (program, college, level, degree, major, minor, etc.)
"""

import asyncio
import aiohttp
import json
import os
from datetime import datetime
from typing import Optional

# API Endpoints
ALL_TERMS_URL = "https://howdyportal.tamu.edu/api/all-terms"
CLASS_LIST_URL = "https://howdyportal.tamu.edu/api/course-sections"

# All detail endpoints that require term/crn
SECTION_DETAIL_ENDPOINTS = {
    "attributes": "https://howdyportal.tamu.edu/api/section-attributes",
    "prereqs": "https://howdyportal.tamu.edu/api/section-prereqs",
    "bookstore_links": "https://howdyportal.tamu.edu/api/section-bookstore-links",
    "meeting_times": "https://howdyportal.tamu.edu/api/section-meeting-times-with-profs",
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


def recursive_parse_json(data):
    """Recursively parse JSON strings within data."""
    if isinstance(data, str):
        try:
            parsed = json.loads(data)
            return recursive_parse_json(parsed)
        except (json.JSONDecodeError, TypeError):
            return data
    elif isinstance(data, dict):
        return {k: recursive_parse_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [recursive_parse_json(item) for item in data]
    return data


async def fetch_terms(session: aiohttp.ClientSession) -> list:
    """Fetch all available terms."""
    async with session.get(ALL_TERMS_URL, timeout=aiohttp.ClientTimeout(total=10)) as response:
        if response.status != 200:
            raise Exception(f"Failed to fetch terms: {response.status}")
        return await response.json()


async def fetch_sections_for_term(session: aiohttp.ClientSession, term_code: str) -> list:
    """Fetch all sections for a given term."""
    async with session.post(
        CLASS_LIST_URL,
        json={"termCode": term_code},
        timeout=aiohttp.ClientTimeout(total=30)
    ) as response:
        if response.status != 200:
            raise Exception(f"Failed to fetch sections for term {term_code}: {response.status}")
        return await response.json()


async def fetch_section_detail(
    session: aiohttp.ClientSession,
    endpoint_name: str,
    endpoint_url: str,
    term_code: str,
    crn: str,
    semaphore: asyncio.Semaphore
) -> tuple:
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
                data = recursive_parse_json(text)
                return endpoint_name, data
        except Exception as e:
            return endpoint_name, {"error": str(e)}


async def fetch_all_details_for_section(
    session: aiohttp.ClientSession,
    term_code: str,
    crn: str,
    semaphore: asyncio.Semaphore
) -> dict:
    """Fetch all detail endpoints for a single section."""
    tasks = [
        fetch_section_detail(session, name, url, term_code, crn, semaphore)
        for name, url in SECTION_DETAIL_ENDPOINTS.items()
    ]
    
    results = await asyncio.gather(*tasks)
    
    details = {}
    for name, data in results:
        if data is not None:
            details[name] = data
    
    return details


async def fetch_all_sections_with_details(
    term_code: str,
    limit: Optional[int] = None,
    max_concurrent: int = 50,
    output_file: Optional[str] = None
) -> dict:
    """
    Fetch all sections for a term with their complete details.
    
    Args:
        term_code: The term code (e.g., '202611')
        limit: Optional limit on number of sections to process (for testing)
        max_concurrent: Maximum concurrent requests
        output_file: Optional file path to save results
    
    Returns:
        Dictionary with all section data
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async with aiohttp.ClientSession() as session:
        # Get all sections for the term
        print(f"Fetching sections for term {term_code}...")
        sections = await fetch_sections_for_term(session, term_code)
        print(f"Found {len(sections)} sections")
        
        if limit:
            sections = sections[:limit]
            print(f"Processing first {limit} sections")
        
        # Process sections in batches
        results = []
        batch_size = 100
        
        for i in range(0, len(sections), batch_size):
            batch = sections[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(sections) + batch_size - 1) // batch_size
            print(f"Processing batch {batch_num}/{total_batches} ({len(batch)} sections)...")
            
            tasks = []
            for section in batch:
                crn = section.get("SWV_CLASS_SEARCH_CRN")
                if crn:
                    tasks.append(
                        process_section(session, section, term_code, crn, semaphore)
                    )
            
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)
            
            # Progress update
            processed = min(i + batch_size, len(sections))
            print(f"  Completed {processed}/{len(sections)} sections")
        
        output = {
            "term_code": term_code,
            "fetched_at": datetime.now().isoformat(),
            "total_sections": len(results),
            "sections": results
        }
        
        if output_file:
            print(f"Saving results to {output_file}...")
            with open(output_file, "w") as f:
                json.dump(output, f, indent=2, default=str)
            print(f"Saved {len(results)} sections to {output_file}")
        
        return output


async def process_section(
    session: aiohttp.ClientSession,
    section: dict,
    term_code: str,
    crn: str,
    semaphore: asyncio.Semaphore
) -> dict:
    """Process a single section and fetch all its details."""
    # Get basic section info
    result = {
        "crn": crn,
        "subject": section.get("SWV_CLASS_SEARCH_SUBJECT"),
        "course_number": section.get("SWV_CLASS_SEARCH_COURSE"),
        "section_number": section.get("SWV_CLASS_SEARCH_SECTION"),
        "title": section.get("SWV_CLASS_SEARCH_TITLE"),
        "credit_hours": section.get("HRS_COLUMN_FIELD"),
        "campus": section.get("SWV_CLASS_SEARCH_CAMPUS"),
        "schedule_type": section.get("SWV_CLASS_SEARCH_SCHD"),
        "instruction_type": section.get("SWV_CLASS_SEARCH_INSTRUCT_TYPE_DESC"),
        "seats_open": section.get("STUSEAT_OPEN"),
        "instructor_json": section.get("SWV_CLASS_SEARCH_INSTRCTR_JSON"),
    }
    
    # Fetch all detail endpoints
    details = await fetch_all_details_for_section(session, term_code, crn, semaphore)
    result["details"] = details
    
    return result


async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch all section details from Howdy Portal API")
    parser.add_argument("--term", type=str, help="Term code (e.g., 202611)")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of sections (for testing)")
    parser.add_argument("--output", type=str, default=None, help="Output file path")
    parser.add_argument("--concurrent", type=int, default=50, help="Max concurrent requests")
    parser.add_argument("--list-terms", action="store_true", help="List available terms and exit")
    
    args = parser.parse_args()
    
    async with aiohttp.ClientSession() as session:
        terms = await fetch_terms(session)
    
    if args.list_terms:
        print("\nAvailable terms:")
        for term in sorted(terms, key=lambda x: x["STVTERM_CODE"], reverse=True)[:20]:
            print(f"  {term['STVTERM_CODE']}: {term['STVTERM_DESC']}")
        return
    
    if not args.term:
        # Default to the most recent term
        recent_terms = sorted(terms, key=lambda x: x["STVTERM_CODE"], reverse=True)
        args.term = recent_terms[0]["STVTERM_CODE"]
        term_desc = recent_terms[0]["STVTERM_DESC"]
        print(f"No term specified, using most recent: {args.term} ({term_desc})")
    
    if not args.output:
        args.output = f"section_details_{args.term}.json"
    
    await fetch_all_sections_with_details(
        term_code=args.term,
        limit=args.limit,
        max_concurrent=args.concurrent,
        output_file=args.output
    )


if __name__ == "__main__":
    asyncio.run(main())

