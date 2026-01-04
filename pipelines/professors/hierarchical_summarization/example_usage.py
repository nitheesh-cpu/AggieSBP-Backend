"""
Example usage of the hierarchical summarization pipeline.
"""

import os
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from aggiermp.database.base import get_session, ReviewDB
from pipelines.professors.hierarchical_summarization import (
    HierarchicalSummarizationPipeline,
)


def main() -> None:
    """Example: Process reviews for a professor"""

    # Get reviews from database
    session = get_session()
    try:
        # Initialize pipeline with database session for cross-listings
        pipeline = HierarchicalSummarizationPipeline(session=session)
        # Example: Get reviews for a specific professor
        professor_id = "VGVhY2hlci02MDkxMDE="  # Replace with actual professor ID

        reviews = (
            session.query(ReviewDB).filter(ReviewDB.professor_id == professor_id).all()
        )

        # Convert to dict format
        raw_reviews = [
            {
                "id": review.id,
                "professor_id": review.professor_id,
                "course_code": review.course_code,
                "review_text": review.review_text or "",
            }
            for review in reviews
        ]

        print(f"Found {len(raw_reviews)} reviews for professor {professor_id}")

        # Process through pipeline
        professor_summary = pipeline.process_professor_reviews(
            raw_reviews, professor_id
        )

        # Print results
        print("\n" + "=" * 60)
        print("PROFESSOR SUMMARY")
        print("=" * 60)
        print(f"Professor ID: {professor_summary.professor_id}")
        print(f"Overall Sentiment: {professor_summary.overall_sentiment}")
        print(f"Confidence: {professor_summary.confidence:.2f}")
        print("\nStrengths:")
        for strength in professor_summary.strengths:
            print(f"  - {strength}")
        print("\nComplaints:")
        for complaint in professor_summary.complaints:
            print(f"  - {complaint}")
        print(f"\nConsistency: {professor_summary.consistency}")

        print(f"\nCourse Summaries ({len(professor_summary.course_summaries)}):")
        for course_summary in professor_summary.course_summaries:
            print(f"\n  Course: {course_summary.course}")
            print(f"  Total Reviews: {course_summary.total_reviews}")
            print(f"  Confidence: {course_summary.confidence:.2f}")
            if course_summary.teaching:
                print(f"  Teaching: {course_summary.teaching[:100]}...")
            if course_summary.exams:
                print(f"  Exams: {course_summary.exams[:100]}...")
            if course_summary.grading:
                print(f"  Grading: {course_summary.grading[:100]}...")
            if course_summary.workload:
                print(f"  Workload: {course_summary.workload[:100]}...")

    finally:
        session.close()


if __name__ == "__main__":
    # Set environment variables for performance
    os.environ["OMP_NUM_THREADS"] = "4"
    os.environ["TOKENIZERS_PARALLELISM"] = "true"

    main()
