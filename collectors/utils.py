from collectors.rmp_review_collector import RMPReviewCollector
from database.base import ProfessorDB, upsert_professors, upsert_reviews


async def get_new_reviews_for_professor(session, professor_id):
    """Get new reviews that have been added since the last review date"""
    professor = session.query(ProfessorDB).filter(ProfessorDB.id == professor_id).first()
    last_review_date = professor.updated_at
    collector = RMPReviewCollector()
    reviews = collector.get_reviews_since_date(professor_id, last_review_date)
    if reviews:
        upsert_reviews(session, reviews)
    return reviews

async def update_professors(session, university_id):
    """Get all professors with new reviews"""
    collector = RMPReviewCollector()
    professors = collector.get_all_professors(university_id, 1000) 
    updated_professors = upsert_professors(session, professors)
    for professor in updated_professors:
        await get_new_reviews_for_professor(session, professor.id)
    return len(updated_professors)
