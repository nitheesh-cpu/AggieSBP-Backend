"""Campus regression tests without API startup or database writes."""

import ast
import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from fastapi import HTTPException
from aggiermp.core.campuses import (
    TEXAS_AM_ID, TEXAS_AM_GALVESTON_ID, university_id_for_campus,
    campus_from_term_description, campus_university_sql,
)


def details_endpoint():
    source = Path(__file__).parents[1] / "src/aggiermp/api/main.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    function = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef)
                    and n.name == "get_course_professors_details")
    function.decorator_list = []
    function.args.defaults = []
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[
        ast.alias(name="annotations")], level=0), function], type_ignores=[])
    namespace = {"HTTPException": HTTPException, "logger": logging.getLogger(__name__)}
    exec(compile(ast.fix_missing_locations(module), str(source), "exec"), namespace)
    return namespace[function.name]


def query(all_rows=None, first=None):
    result = Mock()
    result.options.return_value = result
    result.filter.return_value = result
    result.join.return_value = result
    result.distinct.return_value = result
    result.limit.return_value = result
    result.all.return_value = all_rows or []
    result.first.return_value = first
    return result


class CampusMatchingTests(unittest.TestCase):
    def test_de_ruiter_lookup_includes_compound_surname_candidate(self):
        profile = SimpleNamespace(
            id="VGVhY2hlci0xMTU3NzQ1", first_name="Darryl", last_name="De Ruiter",
            university_id=TEXAS_AM_ID, num_ratings=12, avg_rating=4.5, legacy_id=1157745,
        )
        candidates = query([profile])
        db = Mock()
        db.query.side_effect = [
            query(first=SimpleNamespace(term_desc="Spring 2027 - College Station")),
            query([SimpleNamespace(instructor_name="Darryl De Ruiter", section_number="500",
                                   campus="College Station")]),
            candidates, query(), query(), query(),
        ]
        result = asyncio.run(details_endpoint()(None, "202711", "ANTH409", db, "College Station"))
        professor = result["professors"][0]
        self.assertEqual(professor["id"], profile.id)
        self.assertEqual(professor["overallRating"], 4.5)
        self.assertEqual(professor["rmpId"], 1157745)
        # Check the actual SQL prefilter: the old parser only searched R%,
        # excluding a record whose last_name starts with De.
        filters = [arg for call in candidates.filter.call_args_list for arg in call.args]
        surname_filter = filters[1]
        prefixes = [clause.right.value for clause in surname_filter.clauses]
        self.assertIn("d%", prefixes)

    def test_campus_mapping_is_explicit_and_unknown_is_not_college_station(self):
        self.assertEqual(university_id_for_campus(" GALVESTON "), TEXAS_AM_GALVESTON_ID)
        self.assertEqual(university_id_for_campus("College Station"), TEXAS_AM_ID)
        for campus in [None, "", "Houston - Medicine North", "Other"]:
            self.assertIsNone(university_id_for_campus(campus))
        self.assertEqual(campus_from_term_description("Fall 2026 - Galveston"), "Galveston")

    def test_same_name_instructors_are_not_merged_across_campuses(self):
        instructors = [SimpleNamespace(instructor_name="Amanda Wood", section_number="1", campus=c)
                       for c in ["College Station", "Galveston"]]
        profiles = [SimpleNamespace(id=pid, first_name="Amanda", last_name="Wood",
                    university_id=uid, num_ratings=count, avg_rating=rating, legacy_id=legacy)
                    for pid, uid, count, rating, legacy in [
                        ("cs", TEXAS_AM_ID, 100, 4.5, 10),
                        ("gv", TEXAS_AM_GALVESTON_ID, 2, 2.5, 20)]]
        cs_candidates, gv_candidates = query([profiles[0]]), query([profiles[1]])
        db = Mock()
        db.query.side_effect = [
            query(first=SimpleNamespace(term_desc="Fall 2026 - College Station")),
            query(instructors), cs_candidates, query(), query(), query(),
            gv_candidates, query(), query(),
        ]
        result = asyncio.run(details_endpoint()(None, "202631", "MATH151", db, None))
        self.assertEqual([p["id"] for p in result["professors"]], ["cs", "gv"])
        self.assertEqual([p["overallRating"] for p in result["professors"]], [4.5, 2.5])
        self.assertIsNone(result["professors"][1]["grades"])
        for candidate_query, school in [(cs_candidates, TEXAS_AM_ID), (gv_candidates, TEXAS_AM_GALVESTON_ID)]:
            filters = [arg for call in candidate_query.filter.call_args_list for arg in call.args]
            self.assertTrue(any(getattr(getattr(f, "right", None), "value", None) == school for f in filters))

    def test_sql_rejects_untrusted_columns(self):
        with self.assertRaises(ValueError):
            campus_university_sql("user input")


if __name__ == "__main__":
    unittest.main()
