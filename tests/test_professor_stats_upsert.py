"""upsert_professors refreshes rating stats whenever RMP reports different ones.

An edited or removed RMP review changes the average without raising the
count, so the old "only when num_ratings grows" guard left stale averages
(D-048). Runs for real only when FETCH_ONLY_TEST_DATABASE_URL points at a
disposable PostgreSQL instance (each run uses its own schema).
"""

import os
import unittest
import uuid

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from aggiermp.database.base import upsert_professors
from aggiermp.models.schema import Professor

URL = os.environ.get("FETCH_ONLY_TEST_DATABASE_URL")


def professor(**stats):
    values = {
        "id": "VGVhY2hlci0x",
        "university_id": "U2Nob29sLTEwMDM=",
        "legacy_id": 1,
        "first_name": "Test",
        "last_name": "Person",
        "department": "Testing",
        "avg_rating": 1.0,
        "avg_difficulty": 3.0,
        "num_ratings": 2,
        "would_take_again_percent": -1.0,
    }
    values.update(stats)
    return Professor(**values)


@unittest.skipUnless(URL, "Requires explicit isolated FETCH_ONLY_TEST_DATABASE_URL")
class ProfessorStatsUpsertTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = "professor_stats_test_" + uuid.uuid4().hex
        root = create_engine(URL)
        with root.begin() as connection:
            connection.execute(text(f"CREATE SCHEMA {cls.schema}"))
        root.dispose()
        cls.engine = create_engine(
            URL, connect_args={"options": f"-csearch_path={cls.schema}"}
        )
        cls.sessions = sessionmaker(bind=cls.engine)
        with cls.engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TABLE professors(id varchar PRIMARY KEY, university_id varchar, "
                    "legacy_id integer, first_name varchar, last_name varchar, department varchar, "
                    "avg_rating float, avg_difficulty float, num_ratings integer, "
                    "would_take_again_percent float, created_at timestamp, updated_at timestamp)"
                )
            )

    @classmethod
    def tearDownClass(cls):
        cls.engine.dispose()
        root = create_engine(URL)
        with root.begin() as connection:
            connection.execute(text(f"DROP SCHEMA {cls.schema} CASCADE"))
        root.dispose()

    def setUp(self):
        with self.engine.begin() as connection:
            connection.execute(text("DELETE FROM professors"))

    def stored(self):
        with self.engine.connect() as connection:
            return connection.execute(
                text("SELECT avg_rating, avg_difficulty, num_ratings FROM professors")
            ).one()

    def upsert(self, row):
        session = self.sessions()
        try:
            upsert_professors(session, [row])
        finally:
            session.close()

    def test_edited_review_updates_average_with_same_count(self):
        self.upsert(professor(avg_rating=1.0, num_ratings=2))
        self.upsert(professor(avg_rating=2.0, num_ratings=2))
        self.assertEqual(tuple(self.stored()), (2.0, 3.0, 2))

    def test_removed_review_updates_count_and_average(self):
        self.upsert(professor(avg_rating=2.0, num_ratings=3))
        self.upsert(professor(avg_rating=3.0, avg_difficulty=2.0, num_ratings=2))
        self.assertEqual(tuple(self.stored()), (3.0, 2.0, 2))

    def test_new_review_still_updates(self):
        self.upsert(professor(avg_rating=1.0, num_ratings=1))
        self.upsert(professor(avg_rating=2.0, num_ratings=2))
        self.assertEqual(tuple(self.stored()), (2.0, 3.0, 2))

    def test_unchanged_stats_leave_updated_at_alone(self):
        self.upsert(professor())
        with self.engine.connect() as connection:
            before = connection.execute(text("SELECT updated_at FROM professors")).scalar()
        self.upsert(professor())
        with self.engine.connect() as connection:
            after = connection.execute(text("SELECT updated_at FROM professors")).scalar()
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
