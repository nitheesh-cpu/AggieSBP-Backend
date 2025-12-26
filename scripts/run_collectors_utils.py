import asyncio
import os
import sys

# Make sure the project's `src` package is importable when running this script
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_PATH = os.path.join(ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

# imports after sys.path modification; ignore E402 (module import not at top)
from pipelines.professors.upsert_professors import (  # noqa: E402
    get_new_reviews_for_professor,
    update_professors,
)
from aggiermp.database.base import get_session  # noqa: E402


async def _run_update_professors(university_id: str):
    session = get_session()
    try:
        count = await update_professors(session, university_id)
        print(f"Updated professors count: {count}")
    finally:
        session.close()


async def _run_get_new_reviews(professor_id: str):
    session = get_session()
    try:
        reviews = await get_new_reviews_for_professor(session, professor_id)
        print(f"Found {len(reviews) if reviews else 0} new reviews")
    finally:
        session.close()


def usage():
    print("Usage:")
    print("  python scripts\\run_collectors_utils.py update_professors <UNIVERSITY_ID>")
    print("  python scripts\\run_collectors_utils.py get_new_reviews <PROFESSOR_ID>")


def main(argv):
    if len(argv) < 2:
        usage()
        return 1

    cmd = argv[0]
    arg = argv[1]

    if cmd == "update_professors":
        asyncio.run(_run_update_professors(arg))
    elif cmd == "get_new_reviews":
        asyncio.run(_run_get_new_reviews(arg))
    else:
        usage()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
