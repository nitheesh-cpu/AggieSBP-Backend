"""
Upsert operations for professors pipeline.

Contains database models and operations for upserting professors, reviews, and summaries.
"""

import re
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy import ARRAY, Column, DateTime, Float, Integer, String, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from aggiermp.database.base import ProfessorDB, ReviewDB, upsert_professors, upsert_reviews
from pipelines.professors.schemas import ReviewData
from pipelines.professors.summarizer import ReviewSummarizer

Base = declarative_base()


class SummaryDB(Base):
    """Database model for professor summaries"""
    __tablename__ = "professor_summaries"

    id = Column(String, primary_key=True)
    professor_id = Column(String, nullable=False)
    course_code = Column(String, nullable=True)  # None for overall summary
    summary_type = Column(String, nullable=False)  # 'overall', 'course_specific', or 'course_number'
    summary_text = Column(Text, nullable=False)
    total_reviews = Column(Integer, nullable=False)
    avg_rating = Column(Float, nullable=True)
    avg_difficulty = Column(Float, nullable=True)
    common_tags = Column(ARRAY(String), nullable=True)
    tag_frequencies = Column(Text, nullable=True)  # JSON string of tag counts
    created_at = Column(DateTime, nullable=False, default=datetime.now)
    updated_at = Column(DateTime, nullable=False, default=datetime.now)


async def get_new_reviews_for_professor(session, professor_id):
    """Get new reviews that have been added since the last review date"""
    professor = (
        session.query(ProfessorDB).filter(ProfessorDB.id == professor_id).first()
    )
    if not professor:
        return []
    
    last_review_date = professor.updated_at
    from pipelines.professors.scrapers import RMPReviewCollector
    
    collector = RMPReviewCollector()
    reviews = collector.get_reviews_since_date(professor_id, last_review_date)
    if reviews:
        upsert_reviews(session, reviews)
    return reviews


async def update_professors(session, university_id):
    """Get all professors with new reviews"""
    from pipelines.professors.scrapers import RMPReviewCollector
    
    collector = RMPReviewCollector()
    professors = collector.get_all_professors(university_id, 1000)
    updated_professors = upsert_professors(session, professors)
    
    summarizer = ReviewSummarizer()
    for professor in updated_professors:
        await get_new_reviews_for_professor(session, professor.id)
        process_professor(summarizer, professor.id, include_course_numbers=True)
    
    summarizer.close()
    return len(updated_professors)


def create_overall_summary(summarizer: ReviewSummarizer, professor_id: str) -> Optional[str]:
    """Create an overall summary for a professor across all courses"""
    reviews = summarizer.fetch_reviews_for_professor(professor_id)

    if not reviews:
        return None

    # Prepare text and generate summary
    combined_text = summarizer.prepare_text_for_summarization(reviews, overall=True)
    if type(combined_text) == list:
        course_result = summarizer.generate_hybrid_summary(
            combined_text, is_course_specific=False
        )
        summary_text = course_result["summary"]
        # remove random punctuation from the beginning
        summary_text = re.sub(r"^[^\w\s]", "", summary_text)
    else:
        summary_text = summarizer.generate_summary(combined_text)

    # Aggregate tags and calculate averages
    common_tags, tag_frequencies = summarizer.aggregate_tags(reviews)
    averages = summarizer.calculate_averages(reviews)

    # Save to database
    summary_id = f"{professor_id}_overall"

    summary_record = SummaryDB(
        id=summary_id,
        professor_id=professor_id,
        course_code=None,
        summary_type="overall",
        summary_text=summary_text,
        total_reviews=len(reviews),
        avg_rating=averages["avg_clarity"],
        avg_difficulty=averages["avg_difficulty"],
        common_tags=common_tags,
        tag_frequencies=str(tag_frequencies),
        updated_at=datetime.now(),
    )

    # Upsert logic
    existing = summarizer.session.query(SummaryDB).filter_by(id=summary_id).first()
    if existing:
        existing.summary_text = summary_text
        existing.total_reviews = len(reviews)
        existing.avg_rating = averages["avg_clarity"]
        existing.avg_difficulty = averages["avg_difficulty"]
        existing.common_tags = common_tags
        existing.tag_frequencies = str(tag_frequencies)
        existing.updated_at = datetime.now()
    else:
        summarizer.session.add(summary_record)

    summarizer.session.commit()
    return summary_id


def create_course_specific_summaries(summarizer: ReviewSummarizer, professor_id: str) -> List[str]:
    """Create course-specific summaries for a professor"""
    reviews = summarizer.fetch_reviews_for_professor(professor_id)

    if not reviews:
        return []

    # Group reviews by normalized course
    course_reviews = summarizer.group_reviews_by_normalized_course(reviews)

    summary_ids = []

    for course_code, course_review_list in course_reviews.items():
        if len(course_review_list) < 2:  # Skip if too few reviews
            continue

        # Generate summary for this course
        combined_text = summarizer.prepare_text_for_summarization(
            course_review_list, overall=False
        )
        if type(combined_text) == list:
            course_result = summarizer.generate_hybrid_summary(
                combined_text, is_course_specific=True
            )
            summary_text = course_result["summary"]
        else:
            summary_text = summarizer.generate_summary(combined_text)

        # Aggregate tags and calculate averages for this course
        common_tags, tag_frequencies = summarizer.aggregate_tags(course_review_list)
        averages = summarizer.calculate_averages(course_review_list)

        # Save to database using normalized course code
        summary_id = f"{professor_id}_{course_code}"

        summary_record = SummaryDB(
            id=summary_id,
            professor_id=professor_id,
            course_code=course_code,  # Use normalized course code
            summary_type="course_specific",
            summary_text=summary_text,
            total_reviews=len(course_review_list),
            avg_rating=averages["avg_clarity"],
            avg_difficulty=averages["avg_difficulty"],
            common_tags=common_tags,
            tag_frequencies=str(tag_frequencies),
            updated_at=datetime.now(),
        )

        # Upsert logic
        existing = summarizer.session.query(SummaryDB).filter_by(id=summary_id).first()
        if existing:
            existing.summary_text = summary_text
            existing.total_reviews = len(course_review_list)
            existing.avg_rating = averages["avg_clarity"]
            existing.avg_difficulty = averages["avg_difficulty"]
            existing.common_tags = common_tags
            existing.tag_frequencies = str(tag_frequencies)
            existing.updated_at = datetime.now()
        else:
            summarizer.session.add(summary_record)

        summary_ids.append(summary_id)

    summarizer.session.commit()
    return summary_ids


def create_course_number_summaries(summarizer: ReviewSummarizer, professor_id: str) -> List[str]:
    """Create course number summaries using professor's department context for proper formatting"""
    reviews = summarizer.fetch_reviews_for_professor(professor_id)

    if not reviews:
        return []

    # Determine professor's primary department
    professor_dept = summarizer.get_professor_primary_department(reviews)

    # Group reviews by course number with department context
    course_reviews = summarizer.group_reviews_by_course_number(reviews, professor_dept)

    summary_ids = []

    for formatted_course_code, course_review_list in course_reviews.items():
        if len(course_review_list) < 2:  # Skip if too few reviews
            continue

        # Generate summary for this course number
        combined_text = summarizer.prepare_text_for_summarization(
            course_review_list, overall=False
        )
        if type(combined_text) == list:
            course_result = summarizer.generate_hybrid_summary(
                combined_text, is_course_specific=True
            )
            summary_text = course_result["summary"]
        else:
            summary_text = summarizer.generate_summary(combined_text)

        # Aggregate tags and calculate averages for this course number
        common_tags, tag_frequencies = summarizer.aggregate_tags(course_review_list)
        averages = summarizer.calculate_averages(course_review_list)

        # Save to database using formatted course code with NUM prefix to distinguish from course-specific
        summary_id = f"{professor_id}_NUM{formatted_course_code}"

        summary_record = SummaryDB(
            id=summary_id,
            professor_id=professor_id,
            course_code=formatted_course_code,  # Use properly formatted course code
            summary_type="course_number",
            summary_text=summary_text,
            total_reviews=len(course_review_list),
            avg_rating=averages["avg_clarity"],
            avg_difficulty=averages["avg_difficulty"],
            common_tags=common_tags,
            tag_frequencies=str(tag_frequencies),
            updated_at=datetime.now(),
        )

        # Upsert logic
        existing = summarizer.session.query(SummaryDB).filter_by(id=summary_id).first()
        if existing:
            existing.summary_text = summary_text
            existing.total_reviews = len(course_review_list)
            existing.avg_rating = averages["avg_clarity"]
            existing.avg_difficulty = averages["avg_difficulty"]
            existing.common_tags = common_tags
            existing.tag_frequencies = str(tag_frequencies)
            existing.updated_at = datetime.now()
        else:
            summarizer.session.add(summary_record)

        summary_ids.append(summary_id)

    summarizer.session.commit()
    return summary_ids


def process_professor(
    summarizer: ReviewSummarizer,
    professor_id: str,
    include_course_numbers: bool = True
) -> Dict[str, any]:
    """Process both overall and course-specific summaries for a professor"""
    results = {
        "professor_id": professor_id,
        "overall_summary_id": None,
        "course_summary_ids": [],
        "course_number_summary_ids": [],
        "error": None,
    }

    try:
        # Create overall summary
        overall_id = create_overall_summary(summarizer, professor_id)
        results["overall_summary_id"] = overall_id

        # Create course number summaries (handles typos and incomplete codes)
        if include_course_numbers:
            number_ids = create_course_number_summaries(summarizer, professor_id)
            results["course_number_summary_ids"] = number_ids

        print(f"\nPROFESSOR {professor_id} PROCESSING COMPLETE!")
        print(f"   Overall summary: {'OK' if overall_id else 'FAILED'}")
        if include_course_numbers:
            print(
                f"   Course number summaries: {len(results['course_number_summary_ids'])}"
            )

    except Exception as e:
        results["error"] = str(e)
        print(f"ERROR processing professor {professor_id}: {e}")
        import traceback
        traceback.print_exc()

    return results


def process_all_professors(
    summarizer: ReviewSummarizer,
    include_course_numbers: bool = True
) -> List[Dict[str, any]]:
    """Process summaries for all professors with reviews"""
    print("\nSTARTING BATCH PROCESSING OF ALL PROFESSORS")
    print("=" * 80)

    # Get all professor IDs that have reviews
    professors = summarizer.session.query(ProfessorDB).all()
    summaries = summarizer.session.query(SummaryDB.professor_id).all()
    # remove duplicates
    summaries = set([s[0] for s in summaries])
    print(f"Found {len(summaries)} professors with existing summaries")
    professor_ids = [professor.id for professor in professors][:4000]

    professor_ids = set(professor_ids) - summaries

    print(f"Found {len(professor_ids)} professors to process")

    results = []
    successful = 0
    failed = 0

    for i, professor_id in enumerate(professor_ids, 1):
        print(f"\nPROFESSOR {i}/{len(professor_ids)}")
        print(f"ID: {professor_id}")

        result = process_professor(summarizer, professor_id, include_course_numbers)
        results.append(result)

        if result["error"]:
            failed += 1
            print(f"Professor {i} failed")
        else:
            successful += 1
            print(f"Professor {i} completed successfully")

        print(
            f"Progress: {successful} successful, {failed} failed, {len(professor_ids) - i} remaining"
        )

    print("\nBATCH PROCESSING COMPLETE!")
    print("Final Stats:")
    print(f"   Successful: {successful}/{len(professor_ids)}")
    print(f"   Failed: {failed}/{len(professor_ids)}")
    print(f"   Success Rate: {successful/len(professor_ids)*100:.1f}%")

    return results


def get_summary(
    summarizer: ReviewSummarizer,
    professor_id: str,
    course_code: Optional[str] = None,
    summary_type: str = "overall",
) -> Optional[SummaryDB]:
    """
    Retrieve a summary from the database

    Args:
        summarizer: ReviewSummarizer instance
        professor_id: Professor ID
        course_code: Course code (optional, for course-specific or number summaries)
        summary_type: 'overall', 'course_specific', or 'course_number'

    Returns:
        SummaryDB record or None
    """
    if summary_type == "overall":
        summary_id = f"{professor_id}_overall"
    elif summary_type == "course_number":
        if not course_code:
            raise ValueError("course_code required for course_number summaries")
        summary_id = f"{professor_id}_NUM{course_code}"
    else:  # course_specific
        if not course_code:
            raise ValueError("course_code required for course_specific summaries")
        summary_id = f"{professor_id}_{course_code}"

    return summarizer.session.query(SummaryDB).filter_by(id=summary_id).first()


def get_all_summaries_for_professor(
    summarizer: ReviewSummarizer, professor_id: str
) -> Dict[str, List[SummaryDB]]:
    """
    Get all summaries for a professor organized by type

    Args:
        summarizer: ReviewSummarizer instance
        professor_id: Professor ID

    Returns:
        Dictionary with 'overall', 'course_specific', and 'course_number' keys
    """
    all_summaries = (
        summarizer.session.query(SummaryDB).filter_by(professor_id=professor_id).all()
    )

    organized = {"overall": [], "course_specific": [], "course_number": []}

    for summary in all_summaries:
        organized[summary.summary_type].append(summary)

    return organized


if __name__ == "__main__":
    from aggiermp.database.base import get_session
    import asyncio
    
    session = get_session()
    try:
        count = asyncio.run(update_professors(session, "U2Nob29zLTEwMDM="))
        print(f"Updated professors count: {count}")
    finally:
        session.close()

