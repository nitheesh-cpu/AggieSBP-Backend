import asyncio
import os
import sys
from datetime import date, datetime, time

# Ensure project src is importable
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_PATH = os.path.join(ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

# imports after sys.path modification
from pipelines.professors.upsert_professors import get_new_reviews_for_professor  # noqa: E402
from aggiermp.database.base import ProfessorDB, get_session  # noqa: E402
from pipelines.professors.summarizer import ReviewSummarizer  # noqa: E402
from pipelines.professors.upsert_professors import process_professor  # noqa: E402


async def main():
    session = get_session()
    try:
        start = datetime.combine(date.today(), time.min)
        end = datetime.combine(date.today(), time.max)

        print(f"Querying professors updated between {start} and {end}...")
        professors = (
            session.query(ProfessorDB)
            .filter(ProfessorDB.updated_at >= start, ProfessorDB.updated_at <= end)
            .all()
        )
        print(f"Found {len(professors)} professors updated today.")

        # Try to initialize summarizer; if it fails (missing heavy deps), skip summarization
        summarizer = None
        try:
            try:
                summarizer = ReviewSummarizer()
                print("Initialized ReviewSummarizer")
            except Exception as e:
                print(
                    "Failed to initialize ReviewSummarizer (will skip summarization):",
                    e,
                )
                summarizer = None
        except Exception:
            print("ReviewSummarizer import failed; skipping summarization")
            summarizer = None

        for prof in professors:
            print(
                f"\nProcessing professor {prof.id} - {getattr(prof, 'first_name', '')} {getattr(prof, 'last_name', '')}"
            )
            try:
                reviews = await get_new_reviews_for_professor(session, prof.id)
                print(f"  New reviews inserted: {len(reviews) if reviews else 0}")
            except Exception as e:
                print(f"  Error fetching/inserting reviews for {prof.id}: {e}")

            if summarizer:
                try:
                    process_professor(summarizer, prof.id, include_course_numbers=True)
                    print("  process_professor completed")
                except Exception as e:
                    print(f"  Error processing professor {prof.id}: {e}")

        if summarizer:
            try:
                summarizer.close()
            except Exception:
                pass

    finally:
        session.close()


if __name__ == "__main__":
    asyncio.run(main())
