"""
Course code normalization module.

Handles normalization of course codes to group variations, aliases, and cross-listings.
"""

import re
from typing import Optional, Dict, Set, List
from sqlalchemy import text
from sqlalchemy.orm import Session


class CourseNormalizer:
    """Normalizes course codes to handle aliases, cross-listings, and variations"""
    
    def __init__(self, session: Optional[Session] = None):
        """
        Initialize course normalizer.
        
        Args:
            session: Optional database session to load cross-listings from courses table
        """
        self.session = session
        # Department aliases mapping
        self.dept_aliases = {
            # Computer Science department changes
            "CPCS": "CSCE",
            "CPSC": "CSCE",
            "COSC": "CSCE",
            "CS": "CSCE",
            # Electrical Engineering variations
            "ELEN": "ECEN",
            "EE": "ECEN",
            "ELEC": "ECEN",
            # Mathematics variations
            "MATH": "MATH",
            "MATHS": "MATH",
            # Engineering variations
            "ENGR": "ENGR",
            "ENGI": "ENGR",
            # Physics variations
            "PHYS": "PHYS",
            "PHY": "PHYS",
            # Chemistry variations
            "CHEM": "CHEM",
            "CHM": "CHEM",
            # Business variations
            "MGMT": "MGMT",
            "MKTG": "MKTG",
            "FINC": "FINC",
            "ACCT": "ACCT",
            "ID": "IDIS",
        }
        
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
            self._load_cross_listings_from_db(session)
    
    def _load_cross_listings_from_db(self, session: Session):
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
                canonical = sorted(group_codes)[0]  # Use first alphabetically as canonical
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
        except Exception as e:
            pass  # Silently fall back to hardcoded cross-listings
    
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
        number_str = re.sub(r'[^\d]', '', number_str)
        
        if not number_str:
            return []
        
        # If number is 6+ digits, try to split into pairs of 3-digit numbers
        if len(number_str) >= 6:
            numbers = []
            # Try splitting every 3 digits
            for i in range(0, len(number_str), 3):
                if i + 3 <= len(number_str):
                    num = number_str[i:i+3]
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
        normalized_dept = self.dept_aliases.get(dept, dept)
        
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
        professor_dept: Optional[str] = None
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
        return codes[0] if codes else (course_code.upper().strip() if course_code else "UNKNOWN")
    
    def normalize_course_codes(
        self, 
        course_code: Optional[str],
        professor_id: Optional[str] = None,
        professor_dept: Optional[str] = None
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
        number_match = re.match(r'^(\d+[A-Z]*)$', course_code_stripped)
        
        if number_match and (professor_id or professor_dept):
            # Try to infer department
            dept = self._infer_department_for_course_number(
                course_code_stripped,
                professor_id,
                professor_dept
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
        professor_dept: Optional[str] = None
    ) -> Optional[str]:
        """
        Infer department for a course number when only number is provided.
        
        Priority:
        1. professor_dept parameter
        2. Professor's department from database
        3. Most common department from professor's other reviews
        
        Args:
            course_number: Just the course number (e.g., "202")
            professor_id: Professor ID
            professor_dept: Professor's department (if known)
        
        Returns:
            Department code (e.g., "ECON") or None if can't infer
        """
        # First priority: use provided department
        if professor_dept:
            dept = professor_dept.upper().strip()
            # If it looks like a department code (just uppercase letters), use it
            if re.match(r'^[A-Z]+$', dept):
                return dept
            # Otherwise, try to find department code in database
            if self.session:
                try:
                    from aggiermp.database.base import DepartmentDB
                    dept_db = self.session.query(DepartmentDB).filter(
                        (DepartmentDB.title.ilike(f"%{dept}%")) |
                        (DepartmentDB.id == dept)
                    ).first()
                    if dept_db:
                        return dept_db.id.upper()
                except Exception:
                    pass
        
        # Second priority: get from database if we have session and professor_id
        if professor_id and self.session:
            try:
                from aggiermp.database.base import ProfessorDB
                professor = self.session.query(ProfessorDB).filter_by(
                    id=professor_id
                ).first()
                
                if professor and professor.department:
                    # Extract department code (might be full name, so try to extract code)
                    dept = professor.department.upper().strip()
                    # If it looks like just letters, use it
                    if re.match(r'^[A-Z]+$', dept):
                        return dept
                    # Otherwise, try to find department code in database
                    from aggiermp.database.base import DepartmentDB
                    dept_db = self.session.query(DepartmentDB).filter(
                        (DepartmentDB.title.ilike(f"%{dept}%")) |
                        (DepartmentDB.id == dept)
                    ).first()
                    if dept_db:
                        return dept_db.id.upper()
            except Exception:
                pass
        
        # Third priority: look at professor's other reviews to find most common department
        if professor_id and self.session:
            try:
                from aggiermp.database.base import ReviewDB
                from sqlalchemy import func
                
                # Get all reviews for this professor with course codes
                reviews = self.session.query(ReviewDB.course_code).filter_by(
                    professor_id=professor_id
                ).filter(
                    ReviewDB.course_code.isnot(None),
                    ReviewDB.course_code != ''
                ).all()
                
                # Extract departments from course codes
                dept_counts = {}
                for (course_code,) in reviews:
                    if course_code:
                        # Try to extract department (letters before number)
                        match = re.match(r'^([A-Z]+)', course_code.upper())
                        if match:
                            dept = match.group(1)
                            dept_counts[dept] = dept_counts.get(dept, 0) + 1
                
                # Return most common department
                if dept_counts:
                    return max(dept_counts.items(), key=lambda x: x[1])[0]
            except Exception:
                pass
        
        return None
    
    def group_reviews_by_normalized_course(
        self, 
        reviews: list,
        course_code_key: str = "course_code"
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

