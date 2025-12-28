"""
Script to check for issues in professor_summaries_new table.

Checks for:
- Course name inconsistencies (e.g., MATH140 vs MTH140)
- Invalid course codes
- Empty or malformed data
- Duplicate course codes per professor
- Missing course data
"""

import sys
from pathlib import Path
from collections import defaultdict
import re

# Add project root to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from aggiermp.database.base import get_session, ProfessorSummaryNewDB, CourseDB
from sqlalchemy import text


def normalize_course_code(code: str) -> str:
    """Normalize course code for comparison"""
    if not code:
        return ""
    # Remove spaces, convert to uppercase
    code = code.upper().strip()
    # Extract department and number
    match = re.match(r'([A-Z]+)(\d+)', code)
    if match:
        dept, num = match.groups()
        return f"{dept}{num}"
    return code


def get_all_valid_course_codes(session):
    """Get all valid course codes from courses table"""
    courses = session.query(CourseDB.subject_id, CourseDB.course_number).all()
    valid_codes = set()
    for subject_id, course_number in courses:
        if subject_id and course_number:
            valid_codes.add(f"{subject_id}{course_number}")
    return valid_codes


def check_summaries():
    """Check all summaries for issues"""
    session = get_session()
    
    try:
        print("Checking professor_summaries_new table...")
        print("=" * 80)
        
        # Get all summaries
        summaries = session.query(ProfessorSummaryNewDB).all()
        print(f"Total summaries: {len(summaries)}\n")
        
        # Get valid course codes
        print("Loading valid course codes from courses table...")
        valid_codes = get_all_valid_course_codes(session)
        print(f"Found {len(valid_codes)} valid course codes\n")
        
        # Track issues
        issues = {
            "course_inconsistencies": defaultdict(list),  # prof_id -> [(code1, code2), ...]
            "invalid_course_codes": defaultdict(list),    # prof_id -> [codes]
            "duplicate_courses": defaultdict(list),       # prof_id -> [codes]
            "empty_course_summaries": [],                 # prof_ids
            "missing_strengths": [],                      # prof_ids
            "missing_complaints": [],                     # prof_ids
            "low_confidence": [],                         # prof_ids with confidence < 0.5
        }
        
        # Process each summary
        for summary in summaries:
            prof_id = summary.professor_id
            
            # Check strengths/complaints
            if not summary.strengths or len(summary.strengths) == 0:
                issues["missing_strengths"].append(prof_id)
            if not summary.complaints or len(summary.complaints) == 0:
                issues["missing_complaints"].append(prof_id)
            
            # Check confidence
            if summary.confidence < 0.5:
                issues["low_confidence"].append((prof_id, summary.confidence))
            
            # Check course summaries
            if not summary.course_summaries:
                issues["empty_course_summaries"].append(prof_id)
                continue
            
            # Group courses by normalized code
            courses_by_normalized = defaultdict(list)
            all_codes = []
            
            for course_summary in summary.course_summaries:
                course_code = course_summary.get("course", "")
                if not course_code:
                    continue
                
                all_codes.append(course_code)
                normalized = normalize_course_code(course_code)
                courses_by_normalized[normalized].append(course_code)
                
                # Check if valid course code
                if normalized not in valid_codes:
                    issues["invalid_course_codes"][prof_id].append(course_code)
            
            # Check for duplicates (same normalized code but different original codes)
            for normalized, codes in courses_by_normalized.items():
                if len(codes) > 1:
                    # Multiple different representations of same course
                    issues["course_inconsistencies"][prof_id].append(codes)
            
            # Check for duplicate course codes (exact matches)
            seen_codes = set()
            for code in all_codes:
                if code in seen_codes:
                    issues["duplicate_courses"][prof_id].append(code)
                seen_codes.add(code)
        
        # Print results
        print("\n" + "=" * 80)
        print("ISSUES FOUND")
        print("=" * 80)
        
        # Course inconsistencies
        if issues["course_inconsistencies"]:
            print(f"\n1. Course Name Inconsistencies ({len(issues['course_inconsistencies'])} professors):")
            print("-" * 80)
            for prof_id, inconsistencies in list(issues["course_inconsistencies"].items())[:10]:
                print(f"  Professor {prof_id[:20]}...")
                for codes in inconsistencies:
                    print(f"    {codes}")
            if len(issues["course_inconsistencies"]) > 10:
                print(f"  ... and {len(issues['course_inconsistencies']) - 10} more")
        else:
            print("\n1. Course Name Inconsistencies: None")
        
        # Invalid course codes
        if issues["invalid_course_codes"]:
            print(f"\n2. Invalid Course Codes ({len(issues['invalid_course_codes'])} professors):")
            print("-" * 80)
            for prof_id, codes in list(issues["invalid_course_codes"].items())[:10]:
                print(f"  Professor {prof_id[:20]}...: {codes}")
            if len(issues["invalid_course_codes"]) > 10:
                print(f"  ... and {len(issues['invalid_course_codes']) - 10} more")
        else:
            print("\n2. Invalid Course Codes: None")
        
        # Duplicate courses
        if issues["duplicate_courses"]:
            print(f"\n3. Duplicate Course Codes ({len(issues['duplicate_courses'])} professors):")
            print("-" * 80)
            for prof_id, codes in list(issues["duplicate_courses"].items())[:10]:
                print(f"  Professor {prof_id[:20]}...: {codes}")
            if len(issues["duplicate_courses"]) > 10:
                print(f"  ... and {len(issues['duplicate_courses']) - 10} more")
        else:
            print("\n3. Duplicate Course Codes: None")
        
        # Empty course summaries
        if issues["empty_course_summaries"]:
            print(f"\n4. Empty Course Summaries ({len(issues['empty_course_summaries'])} professors)")
        else:
            print("\n4. Empty Course Summaries: None")
        
        # Missing strengths/complaints
        if issues["missing_strengths"]:
            print(f"\n5. Missing Strengths: {len(issues['missing_strengths'])} professors")
        else:
            print("\n5. Missing Strengths: None")
        
        if issues["missing_complaints"]:
            print(f"\n6. Missing Complaints: {len(issues['missing_complaints'])} professors")
        else:
            print("\n6. Missing Complaints: None")
        
        # Low confidence
        if issues["low_confidence"]:
            print(f"\n7. Low Confidence (< 0.5): {len(issues['low_confidence'])} professors")
            print("-" * 80)
            for prof_id, conf in issues["low_confidence"][:10]:
                print(f"  Professor {prof_id[:20]}...: {conf:.3f}")
            if len(issues["low_confidence"]) > 10:
                print(f"  ... and {len(issues['low_confidence']) - 10} more")
        else:
            print("\n7. Low Confidence: None")
        
        # Summary statistics
        print("\n" + "=" * 80)
        print("SUMMARY STATISTICS")
        print("=" * 80)
        print(f"Total summaries checked: {len(summaries)}")
        print(f"Professors with course inconsistencies: {len(issues['course_inconsistencies'])}")
        print(f"Professors with invalid course codes: {len(issues['invalid_course_codes'])}")
        print(f"Professors with duplicate courses: {len(issues['duplicate_courses'])}")
        print(f"Professors with empty course summaries: {len(issues['empty_course_summaries'])}")
        print(f"Professors with missing strengths: {len(issues['missing_strengths'])}")
        print(f"Professors with missing complaints: {len(issues['missing_complaints'])}")
        print(f"Professors with low confidence: {len(issues['low_confidence'])}")
        
        # Schema efficiency analysis
        print("\n" + "=" * 80)
        print("SCHEMA EFFICIENCY ANALYSIS")
        print("=" * 80)
        print("\nCurrent Schema (JSON array in single row):")
        print("  - All course summaries stored in JSON column")
        print("  - Querying by course requires JSON operations:")
        print("    SELECT * FROM professor_summaries_new")
        print("    WHERE course_summaries::jsonb @> '[{\"course\": \"MATH151\"}]'")
        print("  - Pros: Single row per professor, easier updates")
        print("  - Cons: JSON queries are slower, harder to index")
        
        print("\nAlternative Schema (separate rows per course):")
        print("  - Each course summary in separate row")
        print("  - Querying by course is simple:")
        print("    SELECT * FROM professor_summaries_new")
        print("    WHERE course_code = 'MATH151'")
        print("  - Pros: Fast queries, easy indexing, better for filtering")
        print("  - Cons: Multiple rows per professor, more complex updates")
        
        print("\nRecommendation:")
        print("  For query performance (filtering by course), separate rows are better.")
        print("  The original schema design (one row per course) is more query-efficient.")
        print("  Consider denormalizing: store both overall summary AND course-specific rows.")
        
    finally:
        session.close()


if __name__ == "__main__":
    check_summaries()

