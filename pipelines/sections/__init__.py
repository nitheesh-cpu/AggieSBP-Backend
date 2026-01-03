"""
Sections pipeline for Howdy API data.

This pipeline fetches and stores section data including:
- Course sections with enrollment data
- Instructor assignments
- Meeting times and locations
"""

from .schemas import SectionSchema, InstructorSchema, MeetingSchema, TermSchema
from .scraper import (
    get_all_terms,
    get_sections_for_term,
    get_all_sections,
    get_sections_by_department,
    get_sections_by_course,
    get_section_statistics,
)
from .upsert import (
    upsert_terms,
    upsert_sections,
    upsert_all_sections,
    delete_old_sections,
)

__all__ = [
    # Schemas
    "SectionSchema",
    "InstructorSchema",
    "MeetingSchema",
    "TermSchema",
    # Scraper functions
    "get_all_terms",
    "get_sections_for_term",
    "get_all_sections",
    "get_sections_by_department",
    "get_sections_by_course",
    "get_section_statistics",
    # Upsert functions
    "upsert_terms",
    "upsert_sections",
    "upsert_all_sections",
    "delete_old_sections",
]
