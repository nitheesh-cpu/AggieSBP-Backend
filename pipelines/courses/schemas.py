"""
Pydantic models for course catalog data.

These models represent the structure of data scraped from the TAMU course catalog
and will be used to insert/update data in the database.
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class DepartmentSchema(BaseModel):
    """
    Department schema for course catalog data.

    Represents a department from the TAMU course catalog.

    Note: Optimized to remove redundant fields. The `id` field serves as the department
    code (replacing the redundant `short_name`), and `long_name` contains the full
    department name (replacing the redundant `title` field).
    """

    id: str = Field(
        ...,
        description="Department code (e.g., 'CSCE', 'MATH') - serves as both ID and short name",
    )
    title: str = Field(
        ..., description="Department title (e.g., 'Computer Sci & Engr')"
    )
    long_name: str = Field(
        ..., description="Department full name (e.g., 'CSCE - Computer Sci & Engr')"
    )
    created_at: datetime = Field(
        default_factory=datetime.now, description="Creation timestamp"
    )
    updated_at: datetime = Field(
        default_factory=datetime.now, description="Last update timestamp"
    )

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat() if v else None}


class CourseSchema(BaseModel):
    """
    Course schema for course catalog data.

    Represents a course from the TAMU course catalog with all scraped information.
    """

    # Basic course identification
    id: Optional[str] = Field(
        None, description="Course ID (e.g., 'csce221') - can be generated from code"
    )
    code: str = Field(..., description="Course code (e.g., 'CSCE 221')")
    name: str = Field(
        ..., description="Course name/title (e.g., 'Data Structures and Algorithms')"
    )

    # Department information
    subject_short_name: str = Field(..., description="Department code (e.g., 'CSCE')")
    subject_long_name: str = Field(
        ..., description="Department full name (e.g., 'Computer Sci & Engr')"
    )
    subject_id: str = Field(
        ..., description="Department ID (foreign key to departments.id)"
    )

    # Course number
    course_number: str = Field(..., description="Course number (e.g., '221')")

    # Credit hours and scheduling
    credits: Optional[int] = Field(None, description="Number of credit hours")
    lecture_hours: Optional[int] = Field(None, description="Lecture hours per week")
    lab_hours: Optional[int] = Field(None, description="Lab hours per week")
    other_hours: Optional[int] = Field(None, description="Other hours per week")

    # Course description
    description: Optional[str] = Field(None, description="Course description text")

    # Prerequisites
    prerequisites: Optional[str] = Field(
        None, description="Prerequisites text description"
    )
    prerequisite_courses: List[str] = Field(
        default_factory=list, description="Flat list of prerequisite course codes"
    )
    prerequisite_groups: List[List[str]] = Field(
        default_factory=list,
        description="Grouped prerequisites: [[group1], [group2]] where group1 is OR, groups are AND",
    )

    # Corequisites
    corequisites: Optional[str] = Field(
        None, description="Corequisites text description"
    )
    corequisite_courses: List[str] = Field(
        default_factory=list, description="Flat list of corequisite course codes"
    )
    corequisite_groups: List[List[str]] = Field(
        default_factory=list,
        description="Grouped corequisites: [[group1], [group2]] where group1 is OR, groups are AND",
    )

    # Cross-listings
    cross_listings: List[str] = Field(
        default_factory=list,
        description="List of cross-listed course codes (e.g., ['ECEN 222'] for CSCE 222)",
    )

    # Timestamps
    created_at: datetime = Field(
        default_factory=datetime.now, description="Creation timestamp"
    )
    updated_at: datetime = Field(
        default_factory=datetime.now, description="Last update timestamp"
    )

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat() if v else None}
