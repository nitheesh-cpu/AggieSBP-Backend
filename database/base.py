from datetime import datetime
import os
from sqlalchemy import ARRAY, DateTime, create_engine, Column, String, Integer, Float, Boolean, select, text, ForeignKey, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from pydantic import BaseModel, Field
from typing import Optional, List
import uuid
from sqlalchemy import URL

from models.schema import Review, University, Professor
from dotenv import load_dotenv
load_dotenv()

Base = declarative_base()

class UniversityDB(Base):
    __tablename__ = 'universities'
    
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    legacy_school_id = Column(Integer, nullable=True)
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)

    def __repr__(self):
        return f"<University(id='{self.id}', name='{self.name}')>"

class ProfessorDB(Base):
    __tablename__ = 'professors'
    
    id = Column(String, primary_key=True)
    university_id = Column(String, ForeignKey('universities.id'), nullable=False)
    legacy_id = Column(Integer, nullable=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    department = Column(String, nullable=True)
    avg_rating = Column(Float, nullable=True)
    avg_difficulty = Column(Float, nullable=True)
    num_ratings = Column(Integer, nullable=False, default=0)
    would_take_again_percent = Column(Float, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self):
        return f"<Professor(id='{self.id}', name='{self.first_name} {self.last_name}')>"
    
class ReviewDB(Base):
    __tablename__ = 'reviews'
    
    id = Column(String, primary_key=True)
    legacy_id = Column(Integer, nullable=True)
    professor_id = Column(String, ForeignKey('professors.id'), nullable=False)
    course_code = Column(String, nullable=True)
    clarity_rating = Column(Float, nullable=True)
    difficulty_rating = Column(Float, nullable=True)
    helpful_rating = Column(Float, nullable=True)
    would_take_again = Column(Integer, nullable=True)
    attendance_mandatory = Column(String, nullable=True)
    is_online_class = Column(Boolean, nullable=False)
    is_for_credit = Column(Boolean, nullable=False)
    review_text = Column(String, nullable=True)
    grade = Column(String, nullable=True)
    review_date = Column(DateTime, nullable=True)
    textbook_use = Column(Integer, nullable=True)
    thumbs_up_total = Column(Integer, nullable=True)
    thumbs_down_total = Column(Integer, nullable=True)
    rating_tags = Column(ARRAY(String), nullable=True)
    admin_reviewed_at = Column(DateTime, nullable=True)
    flag_status = Column(String, nullable=True)
    created_by_user = Column(Boolean, nullable=False)
    teacher_note = Column(String, nullable=True)
    
    def __repr__(self):
        return f"<Review(id='{self.id}', professor_id='{self.professor_id}', course_code='{self.course_code}')>"

# Database connection setup
def create_db_engine():
    """Create database engine and tables"""

    url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            os.getenv("POSTGRES_USER"),
            os.getenv("POSTGRES_PASSWORD"),
            os.getenv("POSTGRES_HOST"),
            os.getenv("POSTGRES_PORT"),
            os.getenv("POSTGRES_DATABASE")
        )
    print(url)
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    return engine

def get_session():
    """Get database session"""
    engine = create_db_engine()
    Session = sessionmaker(bind=engine)
    return Session()

def upsert_universities(session, universities: List[University]):
    """
    Upsert universities into the database.
    Insert new records and update existing ones based on ID.
    """
    query_all = session.execute(select(UniversityDB))
    all_db_universities = query_all.scalars().all()
    insert_universities: list[dict] = []
    update_universities: list[dict] = []
    
    for university in universities:
        db_university_obj = next(
                (
                    x
                    for x in all_db_universities
                    if x.id == university.id
                ),
                None,
            )
        
        if db_university_obj:
            if db_university_obj.name != university.name or \
                db_university_obj.legacy_school_id != university.legacy_school_id or \
                db_university_obj.city != university.city or \
                db_university_obj.state != university.state:
                    
                update_universities.append({
                    'id': university.id,
                    'name': university.name,
                    'legacy_school_id': university.legacy_school_id,
                    'city': university.city,
                    'state': university.state,
                    'updated_at': datetime.now()
                })
        else:
            insert_universities.append({
                'id': university.id,
                'name': university.name,
                'legacy_school_id': university.legacy_school_id,
                'city': university.city,
                'state': university.state
            })
    
    print("Number of universities to insert: ", len(insert_universities))
    print("Number of universities to update: ", len(update_universities))
    
    if insert_universities:
        session.execute(insert(UniversityDB), insert_universities)
    if update_universities:
        session.execute(update(UniversityDB), update_universities)
    
    session.commit()
    return insert_universities + update_universities

def upsert_professors(session, professors: List[Professor]):
    """
    Upsert professors into the database.
    Insert new records and update existing ones based on ID.
    """
    query_all = session.execute(select(ProfessorDB))
    all_db_profs = query_all.scalars().all()
    insert_professors: list[dict] = []
    update_professors: list[dict] = []
    
    for professor in professors:
        # Generate UUID if no ID provided
        if not professor.id:
            raise ValueError("Professor ID is required")
            
        db_prof_obj = next(
                (
                    x
                    for x in all_db_profs
                    if x.id == professor.id
                ),
                None,
            )
        
        if db_prof_obj:
            # if the new professor has more ratings, update the existing professor
            if professor.num_ratings > db_prof_obj.num_ratings:
                update_professors.append({
                    'id': professor.id,
                    'avg_rating': professor.avg_rating,
                    'avg_difficulty': professor.avg_difficulty,
                    'num_ratings': professor.num_ratings,
                    'would_take_again_percent': professor.would_take_again_percent,
                    'updated_at': datetime.now()
                })
        else:
            insert_professors.append({
                'id': professor.id,
                'university_id': professor.university_id,
                'legacy_id': professor.legacy_id,
                'first_name': professor.first_name,
                'last_name': professor.last_name,
                'department': professor.department,
                'avg_rating': professor.avg_rating,
                'avg_difficulty': professor.avg_difficulty,
                'num_ratings': professor.num_ratings,
                'would_take_again_percent': professor.would_take_again_percent,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            })
    
    print("Number of professors to insert: ", len(insert_professors))
    print("Number of professors to update: ", len(update_professors))
    
    if insert_professors:
        session.execute(insert(ProfessorDB), insert_professors)
    if update_professors:
        session.execute(update(ProfessorDB), update_professors)
    
    session.commit()
    return insert_professors + update_professors

def upsert_reviews(session, reviews: List[Review]):
    """
    Upsert reviews into the database.
    Insert new records and update existing ones based on ID.
    """
    insert_reviews: list[dict] = []

    for review in reviews:
        insert_reviews.append({
            **review.model_dump(),
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })
    
    if insert_reviews:
        session.execute(insert(ReviewDB), insert_reviews)
        session.commit()
    return insert_reviews
