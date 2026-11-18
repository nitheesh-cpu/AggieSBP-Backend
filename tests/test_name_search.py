"""Professor name search should accept the formats students paste."""

import re
import unittest

from aggiermp.core.name_search import professor_name_condition


def matches(search: str, first: str, last: str) -> bool:
    """Evaluate the generated SQL condition against one professor in Python."""
    condition, params = professor_name_condition(search, "q")
    values = {
        "p.first_name": first,
        "p.last_name": last,
        "(p.first_name || ' ' || p.last_name)": f"{first} {last}",
        "TRIM(p.first_name)": first.strip(),
        "TRIM(p.last_name)": last.strip(),
    }

    def ilike(column: str, key: str) -> str:
        pattern = params[key]
        regex = "^" + ".*".join(re.escape(part) for part in pattern.split("%")) + "$"
        return str(bool(re.match(regex, values[column], re.IGNORECASE)))

    expr = re.sub(
        r"(\(p\.first_name \|\| ' ' \|\| p\.last_name\)|TRIM\(p\.first_name\)|TRIM\(p\.last_name\)"
        r"|p\.first_name|p\.last_name) ILIKE :(\w+)",
        lambda m: ilike(m.group(1), m.group(2)),
        condition,
    )
    return eval(expr.replace(" OR ", " or ").replace(" AND ", " and "))


class ProfessorNameConditionTest(unittest.TestCase):
    def test_plain_searches_still_work(self):
        self.assertTrue(matches("hanik", "Bruce", "Hanik"))
        self.assertTrue(matches("bruce hanik", "Bruce", "Hanik"))
        self.assertTrue(matches("Bru", "Bruce", "Hanik"))

    def test_registrar_format_with_middle_name(self):
        self.assertTrue(matches("Hanik, Bruce Walter", "Bruce", "Hanik"))
        self.assertTrue(matches("houngninou, david kebo", "David", "Houngninou"))

    def test_registrar_format_uses_first_initial_only(self):
        # Spelling differs between Howdy and the review profile.
        self.assertTrue(matches("Songok, Abigael", "Abigail", "Songok"))

    def test_registrar_format_compound_last_name(self):
        self.assertTrue(matches("Hamidi Sepehr, Fatemeh", "Fatemeh", "Hamidi"))

    def test_registrar_format_rejects_other_first_initial(self):
        self.assertFalse(matches("Kim, Bryan", "Daniel", "Kim"))

    def test_registrar_format_finds_swapped_profile(self):
        # The review site lists Brett Howard Furth as first "FURTH", last "HOWARD".
        self.assertTrue(matches("furth, brett howard", "FURTH ", "HOWARD"))
        self.assertFalse(matches("furth, brett", "FURTH", "HOWARD"))
        self.assertFalse(matches("furth, brett howard", "FURTHER", "HOWARD"))

    def test_words_in_any_order(self):
        self.assertTrue(matches("kim bryan", "Bryan", "Kim"))
        self.assertFalse(matches("kim bryan", "Daniel", "Kim"))


if __name__ == "__main__":
    unittest.main()
