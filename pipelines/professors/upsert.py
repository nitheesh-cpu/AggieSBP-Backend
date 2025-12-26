"""
Upsert operations for hierarchical summarization pipeline.

Handles saving ProfessorSummary and CourseSummary data to the database.
Uses separate rows per course for better query performance.
"""

import hashlib
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from aggiermp.database.base import ProfessorSummaryNewDB, get_session
from pipelines.professors.schemas import ProfessorSummary, CourseSummary
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text


def generate_summary_id(professor_id: str, course_code: Optional[str] = None) -> str:
    """Generate a unique ID for a summary row"""
    if course_code:
        key = f"{professor_id}:{course_code}"
    else:
        key = f"{professor_id}:overall"
    return hashlib.md5(key.encode()).hexdigest()


def upsert_professor_summary(
    professor_summary: ProfessorSummary,
    session=None
) -> bool:
    """
    Upsert a professor summary to the database.
    Creates separate rows for overall summary and each course summary.
    
    Args:
        professor_summary: ProfessorSummary object to save
        session: Optional database session (creates new if not provided)
    
    Returns:
        True if successful, False otherwise
    """
    close_session = False
    if session is None:
        session = get_session()
        close_session = True
    
    # CRITICAL: Check transaction state FIRST, before any operations
    # If a previous operation failed, the transaction will be in an aborted state
    try:
        session.execute(text("SELECT 1"))
    except Exception:
        # Transaction is aborted, rollback to reset state
        try:
            session.rollback()
        except Exception:
            pass  # If rollback fails, we'll handle it in the main try block
    
    try:
        now = datetime.now()
        records_to_upsert = []
        
        # 1. Upsert overall summary (course_code = NULL)
        overall_id = generate_summary_id(professor_summary.professor_id, None)
        # CRITICAL: Convert None to [] for array columns to prevent PostgreSQL errors
        # None + ::TEXT[] in bulk insert = transaction abort
        strengths = professor_summary.strengths if professor_summary.strengths is not None else []
        complaints = professor_summary.complaints if professor_summary.complaints is not None else []
        
        records_to_upsert.append({
            "id": overall_id,
            "professor_id": professor_summary.professor_id,
            "course_code": None,
            "overall_sentiment": professor_summary.overall_sentiment,
            "strengths": strengths,  # Always a list, never None
            "complaints": complaints,  # Always a list, never None
            "consistency": professor_summary.consistency,
            "teaching": None,
            "exams": None,
            "grading": None,
            "workload": None,
            "personality": None,
            "policies": None,
            "other": None,
            "confidence": professor_summary.confidence,
            "total_reviews": sum(cs.total_reviews for cs in professor_summary.course_summaries),
            "created_at": now,
            "updated_at": now,
        })
        
        # 2. Upsert each course summary
        # For course summaries, strengths/complaints should be empty lists (not None)
        # since these fields are only populated for overall summaries
        for course_summary in professor_summary.course_summaries:
            course_id = generate_summary_id(professor_summary.professor_id, course_summary.course)
            records_to_upsert.append({
                "id": course_id,
                "professor_id": professor_summary.professor_id,
                "course_code": course_summary.course,
                "overall_sentiment": None,
                "strengths": [],  # Empty list, not None - prevents ::TEXT[] cast errors
                "complaints": [],  # Empty list, not None - prevents ::TEXT[] cast errors
                "consistency": None,
                "teaching": course_summary.teaching,
                "exams": course_summary.exams,
                "grading": course_summary.grading,
                "workload": course_summary.workload,
                "personality": course_summary.personality,
                "policies": course_summary.policies,
                "other": course_summary.other,
                "confidence": course_summary.confidence,
                "total_reviews": course_summary.total_reviews,
                "created_at": now,
                "updated_at": now,
            })
        
        # Use bulk upsert for better performance
        try:
            stmt = insert(ProfessorSummaryNewDB).values(records_to_upsert)
            stmt = stmt.on_conflict_do_update(
                index_elements=['id'],
                set_={
                    'overall_sentiment': stmt.excluded.overall_sentiment,
                    'strengths': stmt.excluded.strengths,
                    'complaints': stmt.excluded.complaints,
                    'consistency': stmt.excluded.consistency,
                    'teaching': stmt.excluded.teaching,
                    'exams': stmt.excluded.exams,
                    'grading': stmt.excluded.grading,
                    'workload': stmt.excluded.workload,
                    'personality': stmt.excluded.personality,
                    'policies': stmt.excluded.policies,
                    'other': stmt.excluded.other,
                    'confidence': stmt.excluded.confidence,
                    'total_reviews': stmt.excluded.total_reviews,
                    'updated_at': stmt.excluded.updated_at,
                }
            )
            session.execute(stmt)
            session.commit()
        except Exception as db_error:
            # CRITICAL: Immediately rollback on database error to prevent transaction abortion
            session.rollback()
            # Re-raise to be caught by outer exception handler
            raise
        
        # Clean up old course summaries that are no longer in the new data
        current_course_codes = {cs.course for cs in professor_summary.course_summaries}
        if current_course_codes:
            try:
                session.query(ProfessorSummaryNewDB).filter(
                    ProfessorSummaryNewDB.professor_id == professor_summary.professor_id,
                    ProfessorSummaryNewDB.course_code.isnot(None),
                    ~ProfessorSummaryNewDB.course_code.in_(current_course_codes)
                ).delete(synchronize_session=False)
                session.commit()
            except Exception as delete_error:
                # If delete fails, rollback immediately
                session.rollback()
                raise
        
        return True
        
    except Exception as e:
        print(f"Error upserting professor summary for {professor_summary.professor_id}: {e}")
        import traceback
        traceback.print_exc()
        # CRITICAL: Always rollback on error to reset transaction state
        # Without this, the transaction stays aborted and all subsequent queries fail
        try:
            session.rollback()
        except Exception as rollback_error:
            # If rollback also fails, log it but continue
            print(f"Warning: Rollback also failed: {rollback_error}")
        return False
        
    finally:
        if close_session:
            session.close()


def get_professor_summary(professor_id: str, session=None) -> Optional[ProfessorSummary]:
    """
    Retrieve a professor summary from the database.
    
    Args:
        professor_id: Professor ID to retrieve
        session: Optional database session (creates new if not provided)
    
    Returns:
        ProfessorSummary object or None if not found
    """
    close_session = False
    if session is None:
        session = get_session()
        close_session = True
    
    try:
        # Get overall summary
        overall_record = session.query(ProfessorSummaryNewDB).filter(
            ProfessorSummaryNewDB.professor_id == professor_id,
            ProfessorSummaryNewDB.course_code.is_(None)
        ).first()
        
        if not overall_record:
            return None
        
        # Get all course summaries
        course_records = session.query(ProfessorSummaryNewDB).filter(
            ProfessorSummaryNewDB.professor_id == professor_id,
            ProfessorSummaryNewDB.course_code.isnot(None)
        ).all()
        
        # Convert course records to CourseSummary objects
        course_summaries = []
        for record in course_records:
            course_summaries.append(CourseSummary(
                course=record.course_code,
                teaching=record.teaching,
                exams=record.exams,
                grading=record.grading,
                workload=record.workload,
                personality=record.personality,
                policies=record.policies,
                other=record.other,
                confidence=record.confidence,
                total_reviews=record.total_reviews,
            ))
        
        return ProfessorSummary(
            professor_id=overall_record.professor_id,
            overall_sentiment=overall_record.overall_sentiment,
            strengths=overall_record.strengths or [],
            complaints=overall_record.complaints or [],
            consistency=overall_record.consistency,
            confidence=overall_record.confidence,
            course_summaries=course_summaries,
        )
        
    except Exception as e:
        print(f"Error retrieving professor summary for {professor_id}: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        if close_session:
            session.close()


def process_all_professors(
    professor_ids: List[str],
    pipeline,
    session=None
) -> dict:
    """
    Process summaries for multiple professors.
    
    Args:
        professor_ids: List of professor IDs to process
        pipeline: HierarchicalSummarizationPipeline instance
        session: Optional database session
    
    Returns:
        Dictionary with processing results
    """
    from aggiermp.database.base import ReviewDB
    
    close_session = False
    if session is None:
        session = get_session()
        close_session = True
    
    results = {
        "processed": 0,
        "failed": 0,
        "errors": []
    }
    
    try:
        for professor_id in professor_ids:
            try:
                # Fetch reviews for this professor
                reviews_query = session.query(ReviewDB).filter_by(
                    professor_id=professor_id
                )
                
                raw_reviews = []
                for review in reviews_query.all():
                    raw_reviews.append({
                        "id": review.id,
                        "professor_id": review.professor_id,
                        "course_code": review.course_code,
                        "review_text": review.review_text,
                    })
                
                if not raw_reviews:
                    print(f"No reviews found for professor {professor_id}")
                    results["failed"] += 1
                    continue
                
                # Process reviews through pipeline
                print(f"\nProcessing professor {professor_id} ({len(raw_reviews)} reviews)...")
                professor_summary = pipeline.process_professor_reviews(
                    raw_reviews,
                    professor_id
                )
                
                # Upsert to database
                success = upsert_professor_summary(professor_summary, session)
                
                if success:
                    results["processed"] += 1
                    print(f"Successfully processed and saved summary for professor {professor_id}")
                else:
                    results["failed"] += 1
                    results["errors"].append(f"Failed to save summary for {professor_id}")
                    
            except Exception as e:
                error_msg = f"Error processing professor {professor_id}: {e}"
                print(error_msg)
                results["failed"] += 1
                results["errors"].append(error_msg)
        
        return results
        
    finally:
        if close_session:
            session.close()

