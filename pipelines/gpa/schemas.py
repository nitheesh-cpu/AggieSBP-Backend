"""
Pydantic models for GPA data from anex.us.

These models represent the structure of GPA data scraped from anex.us
and will be used to insert/update data in the database.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class GpaDataSchema(BaseModel):
    """
    GPA data schema for anex.us data.

    Represents a single class section's GPA and grade distribution data.
    """

    id: str = Field(
        ...,
        description="Unique identifier: dept_courseNumber_section_year_semester_professor",
    )
    dept: str = Field(..., description="Department code (e.g., 'MATH', 'CSCE')")
    course_number: str = Field(..., description="Course number (e.g., '151', '121')")
    section: str = Field(..., description="Section number")
    professor: str = Field(..., description="Professor name")
    year: str = Field(..., description="Year (e.g., '2024', '2025b')")
    semester: str = Field(..., description="Semester (FALL, SPRING, SUMMER)")
    gpa: Optional[float] = Field(None, description="Average GPA for the section")
    grade_a: int = Field(0, description="Number of A grades")
    grade_b: int = Field(0, description="Number of B grades")
    grade_c: int = Field(0, description="Number of C grades")
    grade_d: int = Field(0, description="Number of D grades")
    grade_f: int = Field(0, description="Number of F grades")
    grade_i: int = Field(0, description="Number of I (Incomplete) grades")
    grade_s: int = Field(0, description="Number of S (Satisfactory) grades")
    grade_u: int = Field(0, description="Number of U (Unsatisfactory) grades")
    grade_q: int = Field(0, description="Number of Q grades")
    grade_x: int = Field(0, description="Number of X grades")
    total_students: int = Field(0, description="Total number of students")
    created_at: datetime = Field(
        default_factory=datetime.now, description="Creation timestamp"
    )
    updated_at: datetime = Field(
        default_factory=datetime.now, description="Last update timestamp"
    )

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat() if v else None}
