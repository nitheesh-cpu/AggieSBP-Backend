"""Instructor search finds people with no review profile (see instructor_search)."""

import re
import unittest
from types import SimpleNamespace

from aggiermp.core.instructor_search import (
    attach_grades, group_instructors, instructor_name_condition, to_json,
)


def matches(search, name):
    """Evaluate the generated condition against one instructor name in Python."""
    condition, params = instructor_name_condition(search, "n", "q")

    def ilike(key):
        pattern = params[key].replace("\\%", "\0").replace("\\_", "\1")
        parts = [re.escape(p).replace("\0", "%").replace("\1", "_") for p in pattern.split("%")]
        return str(bool(re.match("^" + ".*".join(parts) + "$", name, re.IGNORECASE)))

    expr = re.sub(r"n ILIKE :(\w+)", lambda m: ilike(m.group(1)), condition)
    return eval(expr.replace(" OR ", " or ").replace(" AND ", " and "))


def section(name, code, campus="College Station", pidm=1, term="202711", title=""):
    return SimpleNamespace(instructor_name=name, instructor_pidm=pidm, term_code=term,
                           dept=code[:4], course_number=code[4:], course_title=title,
                           campus=campus)


def gpa(professor, code, students, avg, a=0, b=0):
    return SimpleNamespace(professor=professor, dept=code[:4], course_number=code[4:],
                           gpa=avg, total_students=students, grade_a=a, grade_b=b,
                           grade_c=0, grade_d=0, grade_f=0)


class NameConditionTests(unittest.TestCase):
    def test_formats_students_type(self):
        self.assertTrue(matches("hee tak", "Hee Yeon Tak"))
        self.assertTrue(matches("Tak, Hee Yeon", "Hee Yeon Tak"))
        self.assertTrue(matches("oziomachukwu", "Oziomachukwu E. Ejechi"))
        self.assertTrue(matches("maria rou", "Maria Rouziou"))
        self.assertTrue(matches("chiang-yu", "Chiang-Yu Wang"))
        self.assertFalse(matches("hee kim", "Hee Yeon Tak"))

    def test_like_wildcards_in_input_are_literal(self):
        self.assertFalse(matches("t%k", "Hee Yeon Tak"))


class GroupingTests(unittest.TestCase):
    def test_one_entry_per_person_with_their_courses(self):
        rows = [section("Hee Yeon Tak", "ENGL104", title="Composition & Rhetoric"),
                section("Hee Yeon Tak", "ENGL104"),
                section("Hee Yeon Tak", "ENGL203", term="202631"),
                section("Hee Tak", "MATH151", pidm=2)]
        people = group_instructors(rows, "hee yeon tak", 8)
        self.assertEqual([p.name for p in people], ["Hee Yeon Tak", "Hee Tak"])
        self.assertEqual(people[0].courses, {"ENGL104": "Composition & Rhetoric", "ENGL203": ""})
        self.assertEqual(people[0].term_code, "202711")

    def test_word_start_matches_rank_first(self):
        rows = [section("Julia Spierce", "ENGL104", pidm=1),
                section("Pi-Ju Liu", "STAT211", pidm=2)]
        self.assertEqual(group_instructors(rows, "pi-ju", 8)[0].name, "Pi-Ju Liu")


class GradeTests(unittest.TestCase):
    def test_grades_only_from_their_courses_and_name(self):
        people = group_instructors([section("Hee Yeon Tak", "ENGL104")], "tak", 8)
        attach_grades(people, [gpa("TAK H", "ENGL104", 24, 4.0, a=24),
                               gpa("TAK H", "ENGL104", 20, 3.5, a=10, b=10),
                               gpa("TAK H", "MATH151", 99, 1.0),   # not their course
                               gpa("TAN H", "ENGL104", 30, 2.0)])  # someone else
        grades = people[0].grades
        self.assertEqual(grades["totalStudents"], 44)
        self.assertEqual(grades["avgGpa"], 3.77)
        self.assertEqual(grades["distribution"]["A"], 34)

    def test_no_grades_outside_college_station(self):
        people = group_instructors([section("Brett H. Furth", "ANTH210", campus="Galveston")],
                                   "furth", 8)
        attach_grades(people, [gpa("FURTH B", "ANTH210", 30, 3.0)])
        self.assertIsNone(people[0].grades)
        payload = to_json(people[0], {"202711": "Spring 2027 - College Station"})
        self.assertEqual(payload["campus"], "Galveston")
        self.assertEqual(payload["courses"], [{"code": "ANTH210", "title": None}])


if __name__ == "__main__":
    unittest.main()
