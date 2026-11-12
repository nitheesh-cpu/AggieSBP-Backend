"""Course search should match codes however students type them."""

import unittest

from aggiermp.core.subjects import course_search_key


class CourseSearchKeyTest(unittest.TestCase):
    def test_code_variants_compact_to_the_same_key(self):
        for raw in ["acct 327", "ACCT327", "acct-327", " Acct  327 ", "327acct", "327 ACCT"]:
            with self.subTest(raw=raw):
                self.assertEqual(course_search_key(raw), "ACCT327")

    def test_partial_codes(self):
        self.assertEqual(course_search_key("csce 31"), "CSCE31")
        self.assertEqual(course_search_key("geos105"), "GEOS105")

    def test_shorthand_subject_with_number(self):
        self.assertEqual(course_search_key("cs 121"), "CSCE121")

    def test_shorthand_without_number_is_left_alone(self):
        # "calc" alone is a title search, not MATH.
        self.assertEqual(course_search_key("calc"), "CALC")

    def test_non_codes(self):
        self.assertEqual(course_search_key("financial reporting"), "FINANCIALREPORTING")
        self.assertEqual(course_search_key("26580"), "26580")
        self.assertEqual(course_search_key(""), "")
        self.assertEqual(course_search_key(None), "")


if __name__ == "__main__":
    unittest.main()
