"""
Course code normalization module.

Handles normalization of course codes to group variations, aliases, and cross-listings.
"""

import re
from typing import Optional, Dict, List, Tuple
from sqlalchemy import text
from sqlalchemy.orm import Session

from aggiermp.core.subjects import SUBJECT_ALIASES


_NAME_STOPWORDS = {"AND", "OF", "THE", "FOR", "IN", "ENG"}


def _name_tokens(name: str) -> set[str]:
    """Words of a department name: "Computer Sci & Engr" -> {COMPUTER, SCI, ENGR}."""
    return {
        token
        for token in re.findall(r"[A-Z]+", name.upper())
        if len(token) > 1 and token not in _NAME_STOPWORDS
    }


def _tokens_overlap(left: set[str], right: set[str]) -> int:
    """Count words in left that match a word in right, allowing abbreviations."""
    return sum(
        1
        for a in left
        if any(
            a == b
            or (min(len(a), len(b)) >= 3 and (a.startswith(b) or b.startswith(a)))
            for b in right
        )
    )


class CourseNormalizer:
    """Normalizes course codes to handle aliases, cross-listings, and variations"""

    def __init__(self, session: Optional[Session] = None):
        """
        Initialize course normalizer.

        Args:
            session: Optional database session to load cross-listings from courses table
        """
        self.session = session
        # Department aliases mapping, shared with API identity matching
        self.dept_aliases = dict(SUBJECT_ALIASES)
        # Real department codes and "ASTRONOMY"-style full names, from the DB
        self.department_codes: set[str] = set()
        self.department_names: Dict[str, str] = {}
        # Department code -> words of its names, for fuzzy "Computer Science" lookups
        self.department_tokens: Dict[str, set[str]] = {}
        # Course number -> subjects that offer it, from the catalog
        self.subjects_by_number: Dict[str, set[str]] = {}
        # Professor ID -> course number -> subjects they taught (grade/section data)
        self._taught_cache: Dict[str, Dict[str, set[str]]] = {}

        # Known cross-listings (fallback if database doesn't have them)
        self.fallback_cross_listings = {
            # Computer Science/Electrical Engineering cross-listings
            ("CSCE", "222"): "CSCE222",
            ("ECEN", "222"): "CSCE222",
            ("CSCE", "314"): "CSCE314",
            ("ECEN", "314"): "CSCE314",
        }

        # Load cross-listings from database if session provided
        self.cross_listings: Dict[str, str] = {}  # Maps "DEPT123" -> canonical code
        if session:
            self._load_departments_from_db(session)
            self._load_cross_listings_from_db(session)
            self._load_catalog_from_db(session)

    def _rollback(self) -> None:
        """Clear a failed transaction so later queries in this session still run."""
        if self.session is None:
            return
        try:
            self.session.rollback()
        except Exception:
            pass

    def _load_departments_from_db(self, session: Session) -> None:
        """Map department names (e.g. ASTRONOMY, "Computer Sci & Engr") to codes."""
        try:
            rows = session.execute(text("SELECT id, title, long_name FROM departments"))
            for row in rows:
                code = str(row.id).upper().strip()
                self.department_codes.add(code)
                # long_name looks like "CHEM - Chemistry"; title is "Chemistry"
                long_name = re.sub(
                    rf"^{re.escape(code)}\s*-\s*",
                    "",
                    str(row.long_name or ""),
                    flags=re.IGNORECASE,
                )
                tokens: set[str] = set()
                for raw_name in (row.title, long_name):
                    name = re.sub(r"[^A-Z]", "", str(raw_name or "").upper())
                    if name and name != code:
                        self.department_names.setdefault(name, code)
                    tokens |= _name_tokens(str(raw_name or ""))
                self.department_tokens[code] = tokens
        except Exception:
            try:
                session.rollback()
            except Exception:
                pass

    def _load_catalog_from_db(self, session: Session) -> None:
        """Index which subjects offer each course number."""
        try:
            rows = session.execute(
                text("SELECT DISTINCT subject_id, course_number FROM courses")
            )
            for row in rows:
                number = str(row.course_number).upper().strip()
                self.subjects_by_number.setdefault(number, set()).add(
                    str(row.subject_id).upper().strip()
                )
        except Exception:
            try:
                session.rollback()
            except Exception:
                pass

    def normalize_department(self, dept: str) -> str:
        """Resolve aliases, full names, and truncated full names to a code."""
        dept = dept.upper()
        # Real codes first: COSC is Construction Science now, not old CS
        if dept in self.department_codes:
            return dept
        if dept in self.dept_aliases:
            return self.dept_aliases[dept]
        if dept in self.department_names:
            return self.department_names[dept]
        if len(dept) >= 6:
            # Truncated names such as ANTHROPOLOG; only an unambiguous prefix.
            matches = {
                code
                for name, code in self.department_names.items()
                if name.startswith(dept)
            }
            if len(matches) == 1:
                return matches.pop()
        return dept

    def _load_cross_listings_from_db(self, session: Session) -> None:
        """Load cross-listings from the courses table"""
        try:
            # Get all courses with their canonical codes
            query = text("""
                SELECT 
                    subject_id || course_number as canonical_code,
                    cross_listings
                FROM courses
                WHERE cross_listings IS NOT NULL 
                  AND array_length(cross_listings, 1) > 0
            """)

            result = session.execute(query)
            cross_listing_groups = {}  # Group courses that cross-list with each other

            for row in result:
                canonical = row.canonical_code
                if canonical not in cross_listing_groups:
                    cross_listing_groups[canonical] = set([canonical])

                # Normalize and add all cross-listed codes to the same group
                if row.cross_listings:
                    for cross_code in row.cross_listings:
                        normalized_cross = self._normalize_code_string(cross_code)
                        if normalized_cross:
                            cross_listing_groups[canonical].add(normalized_cross)

            # For each group, use the first alphabetically as canonical
            for group_codes in cross_listing_groups.values():
                canonical = sorted(group_codes)[
                    0
                ]  # Use first alphabetically as canonical
                for code in group_codes:
                    self.cross_listings[code] = canonical

            # Also map all courses to themselves (in case they're not in cross-listings)
            all_courses_query = text("""
                SELECT DISTINCT subject_id || course_number as canonical_code
                FROM courses
            """)
            all_courses = session.execute(all_courses_query)
            for row in all_courses:
                code = row.canonical_code
                if code not in self.cross_listings:
                    self.cross_listings[code] = code  # Map to itself

            pass  # Cross-listings loaded silently
        except Exception:
            # Fall back to hardcoded cross-listings
            try:
                session.rollback()
            except Exception:
                pass

    def _extract_course_numbers(self, number_str: str) -> List[str]:
        """
        Extract all possible course numbers from a potentially combined number.

        Handles cases like:
        - "221222" -> ["221", "222"] (split into two 3-digit numbers)
        - "221" -> ["221"]
        - "211311" -> ["211", "311"]
        - "120121" -> ["120", "121"]
        """
        # Remove non-numeric characters
        number_str = re.sub(r"[^\d]", "", number_str)

        if not number_str:
            return []

        # If number is 6+ digits, try to split into pairs of 3-digit numbers
        if len(number_str) >= 6:
            numbers = []
            # Try splitting every 3 digits
            for i in range(0, len(number_str), 3):
                if i + 3 <= len(number_str):
                    num = number_str[i : i + 3]
                    # Only add if it's a reasonable course number (100-999)
                    if 100 <= int(num) <= 999:
                        numbers.append(num)
            # If we found multiple valid numbers, return them
            if len(numbers) > 1:
                return numbers
            # Otherwise fall through to single number handling

        # If number is 4-5 digits, try to split intelligently
        if len(number_str) == 4 or len(number_str) == 5:
            # Try first 3 digits
            first_three = number_str[:3]
            if 100 <= int(first_three) <= 999:
                # Check if remaining digits form a valid course number
                remaining = number_str[3:]
                if len(remaining) >= 3 and 100 <= int(remaining) <= 999:
                    return [first_three, remaining]
                return [first_three]

        # Single valid course number
        if len(number_str) >= 3 and 100 <= int(number_str[:3]) <= 999:
            return [number_str[:3]]

        # Fallback: return as-is
        return [number_str] if number_str else []

    def _normalize_code_string(self, course_code: str) -> Optional[str]:
        """
        Normalize a course code string to canonical form.

        Args:
            course_code: Raw course code (e.g., "CSCE 221", "CPSC221", "CSCE221222")

        Returns:
            Normalized course code (e.g., "CSCE221") or None if invalid
            Returns the FIRST course code if multiple numbers are detected
        """
        codes = self.extract_all_course_codes(course_code)
        return codes[0] if codes else None

    def extract_all_course_codes(self, course_code: str) -> List[str]:
        """
        Extract all possible normalized course codes from a course code string.

        Handles cases like:
        - "CSCE221222" -> ["CSCE221", "CSCE222"]
        - "CSCE 221" -> ["CSCE221"]
        - "CPSC221" -> ["CSCE221"]

        Args:
            course_code: Raw course code

        Returns:
            List of normalized course codes (can be multiple if code contains multiple numbers)
        """
        if not course_code:
            return []

        # Clean the input
        original = course_code.strip().upper()

        # Remove common noise
        cleaned = re.sub(r"[^\w\s]", "", original)  # Remove punctuation
        cleaned = re.sub(r"\s+", " ", cleaned).strip()  # Normalize whitespace

        # Try to extract department and number
        # Common patterns: "CSCE 222", "CSCE222", "CS222", "CSCE221222", etc.
        match = re.match(r"([A-Z]+)\s*(\d+)", cleaned)

        if not match:
            return []

        dept, number = match.groups()

        # Normalize department name
        normalized_dept = self.normalize_department(dept)

        # Extract all possible course numbers (handles cases like 221222 -> [221, 222])
        course_numbers = self._extract_course_numbers(number)

        if not course_numbers:
            return []

        # Generate normalized codes for each number
        normalized_codes = [f"{normalized_dept}{num}" for num in course_numbers]

        return normalized_codes

    def normalize_course_code(
        self,
        course_code: Optional[str],
        professor_id: Optional[str] = None,
        professor_dept: Optional[str] = None,
    ) -> str:
        """
        Normalize course code, handling aliases, cross-listings, and number extraction.
        Returns the FIRST normalized code (for backward compatibility).

        Args:
            course_code: Raw course code from review
            professor_id: Optional professor ID for department inference
            professor_dept: Optional professor department for smart guessing

        Returns:
            Normalized course code (first one if multiple detected)
        """
        codes = self.normalize_course_codes(course_code, professor_id, professor_dept)
        return (
            codes[0]
            if codes
            else (course_code.upper().strip() if course_code else "UNKNOWN")
        )

    def normalize_course_codes(
        self,
        course_code: Optional[str],
        professor_id: Optional[str] = None,
        professor_dept: Optional[str] = None,
    ) -> List[str]:
        """
        Normalize course code, handling aliases, cross-listings, and number extraction.
        Returns ALL normalized codes (including multiple if code contains multiple numbers).

        If course_code is just a number (e.g., "202"), tries to infer department from:
        1. professor_dept parameter (if provided)
        2. Professor's department from database (if professor_id and session available)
        3. Most common department from professor's other reviews

        Args:
            course_code: Raw course code from review
            professor_id: Optional professor ID for department inference
            professor_dept: Optional professor department for smart guessing

        Returns:
            List of normalized course codes (can be multiple)
        """
        if not course_code:
            return ["UNKNOWN"]

        # Check if course_code is just a number (no department prefix)
        course_code_stripped = course_code.strip().upper()
        number_match = re.match(r"^(\d+[A-Z]*)$", course_code_stripped)

        if number_match and (professor_id or professor_dept):
            # Try to infer department
            dept = self._infer_department_for_course_number(
                course_code_stripped, professor_id, professor_dept
            )

            if dept:
                # Construct full course code with inferred department
                course_code = f"{dept}{course_code_stripped}"

        # Extract all possible course codes
        normalized_codes = self.extract_all_course_codes(course_code)

        if not normalized_codes:
            return [course_code.upper().strip()]

        # Apply cross-listings to each code
        canonical_codes = []
        for normalized in normalized_codes:
            # Check cross-listings (database or fallback)
            if normalized in self.cross_listings:
                canonical = self.cross_listings[normalized]
                if canonical not in canonical_codes:
                    canonical_codes.append(canonical)
                continue

            # Check fallback cross-listings using (dept, number) tuple
            dept_match = re.match(r"([A-Z]+)(\d+)", normalized)
            if dept_match:
                dept, number = dept_match.groups()
                cross_key = (dept, number)
                if cross_key in self.fallback_cross_listings:
                    canonical = self.fallback_cross_listings[cross_key]
                    if canonical not in canonical_codes:
                        canonical_codes.append(canonical)
                    continue

            # No cross-listing found, use normalized code
            if normalized not in canonical_codes:
                canonical_codes.append(normalized)

        return canonical_codes if canonical_codes else [course_code.upper().strip()]

    def _infer_department_for_course_number(
        self,
        course_number: str,
        professor_id: Optional[str] = None,
        professor_dept: Optional[str] = None,
    ) -> Optional[str]:
        """Department code for a bare course number such as "202", or None."""
        dept, _source = self.infer_department_with_source(
            course_number, professor_id, professor_dept
        )
        return dept

    def infer_department_with_source(
        self,
        course_number: str,
        professor_id: Optional[str] = None,
        professor_dept: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Infer the department for a course number given without one.

        Priority:
        1. "taught": the registrar's grade/section data shows this professor
           teaching that number in exactly one subject (or one cross-listing)
        1b. "taught_subject": of the subjects offering this number, only one
           is a subject they teach under other numbers
        2. "department": the professor's department name, mapped to a code
           (fuzzy names only match subjects that offer this number)
        3. "reviews": the most common department in the professor's other reviews

        When the grade/section data lists several subjects for the number, the
        later steps may only pick one of those.

        The catalog alone is never enough: it lacks retired courses, so "218"
        for a physics professor would otherwise become SPAN218, not PHYS218.

        Returns:
            (department code, source) or (None, None) if it can't be inferred
        """
        # Only 3-digit numbers ("119", "119H") or two run together ("221222");
        # "1", "4063" and "32089" are typos or CRNs, not course numbers.
        digits = re.match(r"^\d+", course_number.strip())
        if not digits or len(digits.group()) not in (3, 6):
            return None, None
        numbers = self._extract_course_numbers(course_number)
        if not numbers:
            return None, None
        number = numbers[0]
        offered = self.subjects_by_number.get(number, set())

        taught: set[str] = set()
        if professor_id:
            taught = self._subjects_taught_by(professor_id).get(number, set())
            canonical = {
                self.cross_listings.get(f"{dept}{number}", f"{dept}{number}")
                for dept in taught
            }
            if taught and len(canonical) == 1:
                return min(taught), "taught"

        if professor_dept is None and professor_id:
            professor_dept = self.professor_department(professor_id)
        exact = self._exact_department(professor_dept) if professor_dept else None
        if exact and (not taught or exact in taught):
            return exact, "department"

        # They never taught this number, but teach in exactly one subject that
        # offers it (e.g. a VIST professor's "105" is VIST105). Weaker than an
        # exact department: grade data names instructors like "WANG P".
        if professor_id and not taught:
            subjects = {
                dept
                for depts in self._subjects_taught_by(professor_id).values()
                for dept in depts
            }
            if len(subjects & offered) == 1:
                return min(subjects & offered), "taught_subject"

        if professor_dept:
            dept = self._fuzzy_department(professor_dept, taught or offered)
            if dept:
                return dept, "department"

        if professor_id:
            dept = self._most_common_review_department(professor_id)
            known = not self.department_codes or dept in self.department_codes
            if dept and known and (not taught or dept in taught):
                return dept, "reviews"

        return None, None

    def _exact_department(self, name: str) -> Optional[str]:
        """Code for a department name or code ("Chemistry", "ECON"), if exact."""
        cleaned = name.upper().strip()
        if not self.department_codes:
            # No department table (e.g. no DB): trust single-word names as given
            if re.match(r"^[A-Z]+$", cleaned):
                return self.normalize_department(cleaned)
            return None
        code = self.normalize_department(re.sub(r"[^A-Z]", "", cleaned))
        return code if code in self.department_codes else None

    def _fuzzy_department(self, name: str, candidates: set[str]) -> Optional[str]:
        """
        The one candidate whose name best shares words with name, e.g.
        "Computer Science" ~ "Computer Sci & Engr".
        """
        words = _name_tokens(name)
        if not words or not candidates:
            return None
        scores = {
            dept: _tokens_overlap(words, self.department_tokens.get(dept, set()))
            for dept in candidates
        }
        best = max(scores.values(), default=0)
        winners = [dept for dept, score in scores.items() if score == best]
        if best > 0 and len(winners) == 1:
            return winners[0]
        return None

    def professor_department(self, professor_id: str) -> Optional[str]:
        if not self.session:
            return None
        try:
            from aggiermp.database.base import ProfessorDB

            professor = (
                self.session.query(ProfessorDB).filter_by(id=professor_id).first()
            )
            return professor.department if professor else None
        except Exception:
            self._rollback()
            return None

    def _subjects_taught_by(self, professor_id: str) -> Dict[str, set[str]]:
        """Course number -> subjects this professor taught, per grade/section data."""
        if professor_id in self._taught_cache:
            return self._taught_cache[professor_id]
        taught: Dict[str, set[str]] = {}
        if self.session:
            try:
                from aggiermp.core.name_matching import professor_names_match
                from aggiermp.database.base import ProfessorDB

                professor = (
                    self.session.query(ProfessorDB).filter_by(id=professor_id).first()
                )
                surname_tokens = (
                    re.findall(r"[A-Za-z]+", professor.last_name or "")
                    if professor
                    else []
                )
                if surname_tokens:
                    full_name = f"{professor.first_name} {professor.last_name}"
                    rows = self.session.execute(
                        text("""
                            SELECT DISTINCT dept, course_number, professor AS name
                            FROM gpa_data
                            WHERE professor ILIKE :pattern
                            UNION
                            SELECT DISTINCT s.dept, s.course_number, si.instructor_name
                            FROM section_instructors si
                            JOIN sections s ON s.id = si.section_id
                            WHERE si.instructor_name ILIKE :pattern
                        """),
                        {"pattern": f"%{max(surname_tokens, key=len)}%"},
                    ).fetchall()
                    for row in rows:
                        if professor_names_match(full_name, row.name):
                            number = str(row.course_number).upper().strip()
                            taught.setdefault(number, set()).add(
                                str(row.dept).upper().strip()
                            )
            except Exception:
                self._rollback()
        self._taught_cache[professor_id] = taught
        return taught

    def _most_common_review_department(self, professor_id: str) -> Optional[str]:
        """Most common department prefix among the professor's other reviews."""
        if not self.session:
            return None
        try:
            from aggiermp.database.base import ReviewDB

            reviews = (
                self.session.query(ReviewDB.course_code)
                .filter_by(professor_id=professor_id)
                .filter(ReviewDB.course_code.isnot(None), ReviewDB.course_code != "")
                .all()
            )
        except Exception:
            self._rollback()
            return None

        dept_counts: Dict[str, int] = {}
        for (course_code,) in reviews:
            match = re.match(r"^([A-Z]+)", course_code.upper())
            if match:
                dept = self.normalize_department(match.group(1))
                dept_counts[dept] = dept_counts.get(dept, 0) + 1
        if not dept_counts:
            return None
        return max(dept_counts.items(), key=lambda x: x[1])[0]

    def group_reviews_by_normalized_course(
        self, reviews: list, course_code_key: str = "course_code"
    ) -> Dict[str, list]:
        """
        Group reviews by normalized course codes.

        Args:
            reviews: List of review objects (dicts or objects with course_code attribute)
            course_code_key: Key to access course_code if reviews are dicts

        Returns:
            Dictionary mapping normalized course codes to lists of reviews
        """
        grouped: Dict[str, list] = {}

        for review in reviews:
            # Extract course code
            if isinstance(review, dict):
                course_code = review.get(course_code_key)
            else:
                course_code = getattr(review, course_code_key, None)

            # Normalize
            normalized_code = self.normalize_course_code(course_code)

            # Group
            if normalized_code not in grouped:
                grouped[normalized_code] = []
            grouped[normalized_code].append(review)

        return grouped
