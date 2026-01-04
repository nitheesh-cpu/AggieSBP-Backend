"""
Pydantic schemas for section data from Howdy API.
"""

from typing import List, Optional
from pydantic import BaseModel
from typing import Any


class InstructorSchema(BaseModel):
    """Schema for instructor data from Howdy API"""

    name: str  # NAME field (includes "(P)" suffix for primary)
    pidm: Optional[int] = None  # MORE field
    has_cv: bool = False  # HAS_CV == "Y"
    is_primary: bool = False  # First in list
    cv_url: Optional[str] = None  # Constructed URL

    @classmethod
    def from_api(cls, data: dict, is_primary: bool = False) -> "InstructorSchema":
        """Create from Howdy API instructor JSON"""
        name = data.get("NAME", "").rstrip(" (P)")
        pidm = data.get("MORE")
        has_cv = data.get("HAS_CV") == "Y"
        cv_url = None
        if has_cv and pidm:
            cv_url = f"https://compass-ssb.tamu.edu/pls/PROD/bwykfupd.p_showdoc?doctype_in=CV&pidm_in={pidm}"

        return cls(
            name=name,
            pidm=int(pidm) if pidm else None,
            has_cv=has_cv,
            is_primary=is_primary,
            cv_url=cv_url,
        )


class MeetingSchema(BaseModel):
    """Schema for meeting time data from Howdy API"""

    meeting_index: int
    credit_hours_session: Optional[int] = None  # SSRMEET_CREDIT_HR_SESS
    days_of_week: List[str] = []  # Array of day codes
    begin_time: Optional[str] = None  # SSRMEET_BEGIN_TIME
    end_time: Optional[str] = None  # SSRMEET_END_TIME
    start_date: Optional[str] = None  # SSRMEET_START_DATE
    end_date: Optional[str] = None  # SSRMEET_END_DATE
    building_code: Optional[str] = None  # SSRMEET_BLDG_CODE
    room_code: Optional[str] = None  # SSRMEET_ROOM_CODE
    meeting_type: Optional[str] = None  # SSRMEET_MTYP_CODE

    @classmethod
    def from_api(cls, data: dict, index: int) -> "MeetingSchema":
        """Create from Howdy API meeting JSON"""
        # Parse days of week
        day_mapping = {
            "SSRMEET_SUN_DAY": "U",
            "SSRMEET_MON_DAY": "M",
            "SSRMEET_TUE_DAY": "T",
            "SSRMEET_WED_DAY": "W",
            "SSRMEET_THU_DAY": "R",
            "SSRMEET_FRI_DAY": "F",
            "SSRMEET_SAT_DAY": "S",
        }
        days = []
        for api_key, day_code in day_mapping.items():
            if data.get(api_key):
                days.append(day_code)

        # Parse credit hours
        credit_hrs = data.get("SSRMEET_CREDIT_HR_SESS")
        credit_hours_session = None
        if credit_hrs is not None:
            try:
                credit_hours_session = int(credit_hrs)
            except (ValueError, TypeError):
                pass

        return cls(
            meeting_index=index,
            credit_hours_session=credit_hours_session,
            days_of_week=days,
            begin_time=data.get("SSRMEET_BEGIN_TIME"),
            end_time=data.get("SSRMEET_END_TIME"),
            start_date=data.get("SSRMEET_START_DATE"),
            end_date=data.get("SSRMEET_END_DATE"),
            building_code=data.get("SSRMEET_BLDG_CODE"),
            room_code=data.get("SSRMEET_ROOM_CODE"),
            meeting_type=data.get("SSRMEET_MTYP_CODE"),
        )


class SectionSchema(BaseModel):
    """Schema for section data from Howdy API"""

    id: str  # term_code + "_" + crn
    term_code: str
    crn: str
    dept: str  # SWV_CLASS_SEARCH_SUBJECT (e.g., "CSCE")
    dept_desc: Optional[str] = (
        None  # SWV_CLASS_SEARCH_SUBJECT_DESC (e.g., "CSCE - Computer Sci & Engr")
    )
    course_number: str  # SWV_CLASS_SEARCH_COURSE (e.g., "221")
    section_number: str  # SWV_CLASS_SEARCH_SECTION (e.g., "501")
    course_title: Optional[str] = None  # SWV_CLASS_SEARCH_TITLE

    # Credit hours - multiple fields available
    credit_hours: Optional[str] = (
        None  # HRS_COLUMN_FIELD - displayed credit hours (always populated)
    )
    hours_low: Optional[int] = None  # SWV_CLASS_SEARCH_HOURS_LOW (always populated)
    hours_high: Optional[int] = None  # SWV_CLASS_SEARCH_HOURS_HIGH (rarely populated)

    # Section info
    campus: Optional[str] = None  # SWV_CLASS_SEARCH_SITE (93% populated)
    part_of_term: Optional[str] = None  # SWV_CLASS_SEARCH_PTRM (always populated)
    session_type: Optional[str] = (
        None  # SWV_CLASS_SEARCH_SESSION (always populated, e.g., "Semester")
    )
    schedule_type: Optional[str] = (
        None  # SWV_CLASS_SEARCH_SCHD (always populated, e.g., "LEC", "LAB")
    )
    instruction_type: Optional[str] = (
        None  # SWV_CLASS_SEARCH_INST_TYPE (always populated, e.g., "Web Based")
    )

    # Availability - STUSEAT_OPEN is always populated
    is_open: bool = False  # STUSEAT_OPEN == "Y"

    # Syllabus
    has_syllabus: bool = False  # SWV_CLASS_SEARCH_HAS_SYL_IND == "Y"
    syllabus_url: Optional[str] = None  # Constructed URL

    # Attributes (pipe-delimited string, always populated)
    attributes_text: Optional[str] = None  # SWV_CLASS_SEARCH_ATTRIBUTES

    # Enrollment info
    max_enrollment: Optional[int] = None
    current_enrollment: Optional[int] = None
    seats_available: Optional[int] = None

    # Related data
    instructors: List[InstructorSchema] = []  # 98% populated
    meetings: List[MeetingSchema] = []  # Always populated

    @staticmethod
    def _parse_int(value: Any) -> Optional[int]:
        """Parse integer from API value, handling 'NA' and None"""
        if value is None or value == "NA" or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    @classmethod
    def from_api(cls, data: dict, term_code: str) -> "SectionSchema":
        """Create from Howdy API class list JSON"""
        crn = data.get("SWV_CLASS_SEARCH_CRN", "")
        section_id = f"{term_code}_{crn}"

        # Parse instructors from JSON
        instructors = []
        instructor_json = data.get("SWV_CLASS_SEARCH_INSTRCTR_JSON")
        if instructor_json:
            try:
                import json

                if isinstance(instructor_json, str):
                    instructor_list = json.loads(instructor_json)
                else:
                    instructor_list = instructor_json

                for i, inst in enumerate(instructor_list):
                    instructors.append(
                        InstructorSchema.from_api(inst, is_primary=(i == 0))
                    )
            except (json.JSONDecodeError, TypeError):
                pass

        # Parse meetings from JSON
        meetings = []
        meeting_json = data.get("SWV_CLASS_SEARCH_JSON_CLOB")
        if meeting_json:
            try:
                import json

                if isinstance(meeting_json, str):
                    meeting_list = json.loads(meeting_json)
                else:
                    meeting_list = meeting_json

                for i, meet in enumerate(meeting_list):
                    meetings.append(MeetingSchema.from_api(meet, i))
            except (json.JSONDecodeError, TypeError):
                pass

        # Construct syllabus URL
        syllabus_url = None
        if data.get("SWV_CLASS_SEARCH_HAS_SYL_IND") == "Y":
            syllabus_url = f"https://compass-ssb.tamu.edu/pls/PROD/bwykfupd.p_showdoc?doctype_in=SY&crn_in={crn}&termcode_in={term_code}"

        # Convert credit_hours to string (can be int or string from API)
        credit_hours_raw = data.get("HRS_COLUMN_FIELD")
        credit_hours = str(credit_hours_raw) if credit_hours_raw is not None else None

        return cls(
            id=section_id,
            term_code=term_code,
            crn=crn,
            dept=data.get("SWV_CLASS_SEARCH_SUBJECT", ""),
            dept_desc=data.get("SWV_CLASS_SEARCH_SUBJECT_DESC"),
            course_number=data.get("SWV_CLASS_SEARCH_COURSE", ""),
            section_number=data.get("SWV_CLASS_SEARCH_SECTION", ""),
            course_title=data.get("SWV_CLASS_SEARCH_TITLE"),
            credit_hours=credit_hours,  # String field, always populated
            hours_low=cls._parse_int(data.get("SWV_CLASS_SEARCH_HOURS_LOW")),
            hours_high=cls._parse_int(data.get("SWV_CLASS_SEARCH_HOURS_HIGH")),
            campus=data.get("SWV_CLASS_SEARCH_SITE"),
            part_of_term=data.get("SWV_CLASS_SEARCH_PTRM"),
            session_type=data.get("SWV_CLASS_SEARCH_SESSION"),
            schedule_type=data.get("SWV_CLASS_SEARCH_SCHD"),
            instruction_type=data.get("SWV_CLASS_SEARCH_INST_TYPE"),
            is_open=data.get("STUSEAT_OPEN") == "Y",
            has_syllabus=data.get("SWV_CLASS_SEARCH_HAS_SYL_IND") == "Y",
            syllabus_url=syllabus_url,
            attributes_text=data.get("SWV_CLASS_SEARCH_ATTRIBUTES"),
            max_enrollment=cls._parse_int(data.get("SWV_CLASS_SEARCH_MAX_ENRL")),
            current_enrollment=cls._parse_int(data.get("SWV_CLASS_SEARCH_ENRL")),
            seats_available=cls._parse_int(data.get("SWV_CLASS_SEARCH_SEATS_AVAIL")),
            instructors=instructors,
            meetings=meetings,
        )


class TermSchema(BaseModel):
    """Schema for term data from Howdy API"""

    term_code: str  # STVTERM_CODE
    term_desc: str  # STVTERM_DESC
    start_date: Optional[str] = None  # STVTERM_START_DATE
    end_date: Optional[str] = None  # STVTERM_END_DATE
    academic_year: Optional[str] = None  # STVTERM_ACYR_CODE

    @classmethod
    def from_api(cls, data: dict) -> "TermSchema":
        """Create from Howdy API term JSON"""
        return cls(
            term_code=str(data.get("STVTERM_CODE", "")),
            term_desc=str(data.get("STVTERM_DESC", "")),
            start_date=data.get("STVTERM_START_DATE"),
            end_date=data.get("STVTERM_END_DATE"),
            academic_year=data.get("STVTERM_ACYR_CODE"),
        )

    @property
    def semester(self) -> str:
        """Extract semester name (Spring, Summer, Fall) from description"""
        desc = self.term_desc.lower()
        if "spring" in desc:
            return "Spring"
        elif "summer" in desc:
            return "Summer"
        elif "fall" in desc:
            return "Fall"
        return "Other"

    @property
    def year(self) -> Optional[int]:
        """Extract year from term code (first 4 digits)"""
        try:
            return int(self.term_code[:4])
        except (ValueError, IndexError):
            return None

    @property
    def campus(self) -> str:
        """Extract campus from description"""
        desc = self.term_desc
        if "College Station" in desc:
            return "College Station"
        elif "Galveston" in desc:
            return "Galveston"
        elif "Qatar" in desc:
            return "Qatar"
        elif "Half Year" in desc:
            return "Half Year"
        return "Unknown"


# ============================================================================
# Section Detail Schemas (from additional API endpoints)
# ============================================================================


class SectionAttributeDetailedSchema(BaseModel):
    """Schema for detailed section attribute with description"""

    section_id: str
    term_code: str
    crn: str
    attribute_code: str  # SSRATTR_ATTR_CODE (e.g., "DIST")
    attribute_desc: Optional[str] = None  # STVATTR_DESC (e.g., "Distance Education")

    @classmethod
    def from_api(
        cls, data: dict, section_id: str, term_code: str, crn: str
    ) -> "SectionAttributeDetailedSchema":
        """Create from Howdy API attributes endpoint"""
        return cls(
            section_id=section_id,
            term_code=term_code,
            crn=crn,
            attribute_code=data.get("SSRATTR_ATTR_CODE", ""),
            attribute_desc=data.get("STVATTR_DESC"),
        )

    @property
    def id(self) -> str:
        return f"{self.section_id}_{self.attribute_code}"


class SectionPrereqSchema(BaseModel):
    """Schema for section prerequisites"""

    section_id: str
    term_code: str
    crn: str
    prereqs_text: Optional[str] = None  # P_PRE_REQS_OUT
    prereqs_json: Optional[dict] = None  # Full data

    @classmethod
    def from_api(
        cls, data: dict, section_id: str, term_code: str, crn: str
    ) -> "SectionPrereqSchema":
        """Create from Howdy API prereqs endpoint"""
        prereqs_text = data.get("P_PRE_REQS_OUT") if data else None
        return cls(
            section_id=section_id,
            term_code=term_code,
            crn=crn,
            prereqs_text=prereqs_text,
            prereqs_json=data if data and prereqs_text else None,
        )

    @property
    def id(self) -> str:
        return self.section_id


class SectionRestrictionSchema(BaseModel):
    """Schema for section restrictions"""

    section_id: str
    term_code: str
    crn: str
    restriction_type: str  # 'program', 'college', 'level', 'degree', etc.
    restriction_index: int  # Position in the list
    restriction_code: Optional[str] = None
    restriction_desc: Optional[str] = None
    include_exclude: Optional[str] = None  # 'I' for include, 'E' for exclude

    @classmethod
    def from_api(
        cls,
        data: dict,
        restriction_type: str,
        index: int,
        section_id: str,
        term_code: str,
        crn: str,
    ) -> "SectionRestrictionSchema":
        """Create from Howdy API restriction endpoint"""
        # Common field patterns for different restriction types
        code_fields = [
            "SSRRESV_MAJR_CODE",
            "SSRRESV_COLL_CODE",
            "SSRRESV_LEVL_CODE",
            "SSRRESV_DEGC_CODE",
            "SSRRESV_MINR_CODE",
            "SSRRESV_CONC_CODE",
            "SSRRESV_PROGRAM",
            "SSRRESV_DEPT_CODE",
            "SSRRESV_CAMP_CODE",
            "SSRRESV_ATTR_CODE",
            "SSRRESV_CLASS_CODE",
            "SSRRESV_COHORT",
            "SSRRESV_STYP_CODE",
            "SSRRESV_FOSI_CODE",
        ]
        desc_fields = [
            "STVMAJR_DESC",
            "STVCOLL_DESC",
            "STVLEVL_DESC",
            "STVDEGC_DESC",
            "STVMINR_DESC",
            "STVCONC_DESC",
            "SPRPROG_DESC",
            "STVDEPT_DESC",
            "STVCAMP_DESC",
            "STVATTR_DESC",
            "STVCLAS_DESC",
            "STVCHRT_DESC",
            "STVSTYP_DESC",
            "STVASTY_DESC",
        ]

        # Find the code and description
        restriction_code = None
        restriction_desc = None
        include_exclude = data.get("SSRRESV_INCL_EXCL")

        for field in code_fields:
            if data.get(field):
                restriction_code = data[field]
                break

        for field in desc_fields:
            if data.get(field):
                restriction_desc = data[field]
                break

        return cls(
            section_id=section_id,
            term_code=term_code,
            crn=crn,
            restriction_type=restriction_type,
            restriction_index=index,
            restriction_code=restriction_code,
            restriction_desc=restriction_desc,
            include_exclude=include_exclude,
        )

    @property
    def id(self) -> str:
        return f"{self.section_id}_{self.restriction_type}_{self.restriction_index}"


class SectionBookstoreLinkSchema(BaseModel):
    """Schema for section bookstore links"""

    section_id: str
    term_code: str
    crn: str
    bookstore_url: Optional[str] = None
    link_data: Optional[dict] = None

    @classmethod
    def from_api(
        cls, data: dict, section_id: str, term_code: str, crn: str
    ) -> "SectionBookstoreLinkSchema":
        """Create from Howdy API bookstore endpoint"""
        # Extract URL if available
        bookstore_url = None
        if data:
            bookstore_url = data.get("BOOKSTORE_URL") or data.get("url")

        return cls(
            section_id=section_id,
            term_code=term_code,
            crn=crn,
            bookstore_url=bookstore_url,
            link_data=data if data else None,
        )

    @property
    def id(self) -> str:
        return self.section_id


class SectionDetailsSchema(BaseModel):
    """Combined schema for all section details"""

    section_id: str
    term_code: str
    crn: str
    attributes: List[SectionAttributeDetailedSchema] = []
    prereqs: Optional[SectionPrereqSchema] = None
    restrictions: List[SectionRestrictionSchema] = []
    bookstore_link: Optional[SectionBookstoreLinkSchema] = None
