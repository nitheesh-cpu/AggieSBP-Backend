import sys
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

# Ensure src is in python path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

# Import the FastAPI app
# Adjust the import path based on your findings
# It looks like it's in src/aggiermp/api/main.py
from aggiermp.api.main import app


from typing import Generator
from sqlalchemy import text
from sqlalchemy.orm import Session
from aggiermp.database.base import get_session, Base, create_db_engine


@pytest.fixture(scope="module")
def db_session() -> Generator[Session, None, None]:
    # Setup
    engine = create_db_engine()
    # Create tables
    Base.metadata.create_all(engine)
    session = get_session()
    yield session
    # Teardown
    session.close()
    # Optional: drop tables if you want a clean slate (engine.dispose() might be enough for throwaway container)


@pytest.fixture(scope="module", autouse=True)
def seed_data(db_session: Session) -> None:
    """Seed the database with minimal data for tests."""
    try:
        # Check if data already exists to avoid duplicates if re-running
        count = db_session.execute(text("SELECT COUNT(*) FROM terms")).scalar()
        if count is not None and count > 0:
            return

        # Insert dummy Term
        db_session.execute(
            text("""
            INSERT INTO terms (term_code, term_desc, start_date, end_date, academic_year, created_at, updated_at)
            VALUES ('202611', 'Fall 2026', NOW(), NOW() + INTERVAL '4 months', '2026', NOW(), NOW())
        """)
        )

        # Insert dummy University
        db_session.execute(
            text("""
            INSERT INTO universities (id, name)
            VALUES ('1', 'Texas A&M University')
        """)
        )

        # Insert dummy Professor
        db_session.execute(
            text("""
            INSERT INTO professors (id, first_name, last_name, avg_rating, num_ratings, university_id, created_at, updated_at)
            VALUES ('P1', 'Test', 'Professor', 4.5, 10, '1', NOW(), NOW())
        """)
        )

        # Insert dummy Section
        db_session.execute(
            text("""
            INSERT INTO sections (id, term_code, crn, dept, course_number, section_number, course_title, is_open, has_syllabus, created_at, updated_at)
            VALUES ('202611_12345', '202611', '12345', 'CSCE', '121', '500', 'INTRO TO COMP SCI', FALSE, FALSE, NOW(), NOW())
        """)
        )

        # Insert dummy Section Instructor link
        db_session.execute(
            text("""
            INSERT INTO section_instructors (id, section_id, term_code, crn, instructor_name, created_at, updated_at)
            VALUES ('202611_12345_P1', '202611_12345', '202611', '12345', 'Test Professor', NOW(), NOW())
        """)
        )

        # Insert dummy Department (Required for Course FK)
        db_session.execute(
            text("""
            INSERT INTO departments (id, short_name, long_name, title, created_at, updated_at)
            VALUES ('CSCE', 'CSCE', 'Computer Science & Engineering', 'Department of Computer Science & Engineering', NOW(), NOW())
        """)
        )

        # Insert dummy Course
        db_session.execute(
            text("""
            INSERT INTO courses (id, subject_long_name, subject_short_name, subject_id, course_number, course_display_title, course_title, course_title_long, has_topics, has_restrictions, created_at, updated_at)
            VALUES ('CSCE121', 'Computer Science & Engineering', 'CSCE', 'CSCE', '121', 'CSCE 121', 'INTRO TO COMP SCI', 'Introduction to Computer Science', FALSE, FALSE, NOW(), NOW())
        """)
        )

        # Insert dummy Review
        db_session.execute(
            text("""
            INSERT INTO reviews (id, professor_id, course_code, is_online_class, is_for_credit, created_by_user)
            VALUES ('R1', 'P1', 'CSCE121', FALSE, TRUE, FALSE)
        """)
        )

        # Insert dummy Section Attribute
        db_session.execute(
            text("""
            INSERT INTO section_attributes (id, dept, course_number, section, year, semester, attribute_id, attribute_title, attribute_value, created_at, updated_at)
            VALUES ('CSCE_121_500_2026_Fall_KCOM', 'CSCE', '121', '500', '2026', 'Fall', 'KCOM', 'Communication', 'Y', NOW(), NOW())
        """)
        )

        # Insert dummy GPA Data
        db_session.execute(
            text("""
            INSERT INTO gpa_data (id, dept, course_number, section, professor, year, semester, gpa, total_students, grade_a, grade_b, grade_c, grade_d, grade_f, grade_q, grade_x, grade_i, grade_s, grade_u, created_at, updated_at)
            VALUES ('GPA_1', 'CSCE', '121', '500', 'PROFESSOR', '2026', 'FALL', 3.5, 100, 50, 40, 10, 0, 0, 0, 0, 0, 0, 0, NOW(), NOW())
        """)
        )

        db_session.commit()
    except Exception as e:
        print(f"Seeding failed (might be already seeded or other error): {e}")
        db_session.rollback()


@pytest.fixture(scope="module")
def client() -> Generator[TestClient, None, None]:
    with TestClient(app) as c:
        yield c
