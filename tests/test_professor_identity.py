"""Same-name professors must not share each other's RMP profiles."""

import unittest

from dataclasses import replace

from unittest.mock import MagicMock

from aggiermp.core.professor_identity import (
    Person, Profile, _same_name, _similar_person, choose_profile, explain_choice, given_initials,
    load_people, profile_match_name, rmp_name, surname_tokens, swapped_needs_course,
)
from aggiermp.core.subjects import course_search_key, parse_course_code

VALID = frozenset({"AGEC", "ECON", "CSCE", "ENGR", "BIOL", "MATH", "CHEM", "MEEN"})


def profile(pid, name, *codes):
    parsed = [parse_course_code(code, VALID) for code in codes]
    parsed = [p for p in parsed if p]
    return Profile(
        pid,
        name,
        frozenset(subject for subject, _ in parsed),
        frozenset(subject + number for subject, number in parsed),
    )


# Real data, Nov 2026: two Texas A&M instructors (two PIDMs) named Yu Zhang.
CS_YU = Person("Yu Zhang", frozenset({"CSCE", "ENGR"}))
AG_YU = Person("Yu Zhang", frozenset({"AGEC"}))
AG_PROFILE = profile("ag", "Yu Zhang", "AGEC105", "HONORSAGEC105", "ECON410", "105200")
CS_PROFILE = profile("cs", "Yu Zhang", "CSCE670")


class ChooseProfileTests(unittest.TestCase):
    def test_each_yu_zhang_gets_their_own_profile(self):
        profiles = [AG_PROFILE, CS_PROFILE]
        self.assertEqual(choose_profile(CS_YU, "CSCE470", profiles, [AG_YU]).id, "cs")
        self.assertEqual(choose_profile(AG_YU, "AGEC105", profiles, [CS_YU]).id, "ag")

    def test_namesakes_profile_is_not_borrowed_when_own_profile_is_missing(self):
        self.assertIsNone(choose_profile(CS_YU, "CSCE470", [AG_PROFILE], [AG_YU]))

    def test_profile_reviewed_only_elsewhere_is_rejected_when_name_is_shared(self):
        # Two profiles share the name and neither fits this instructor.
        other = profile("other", "Yu Zhang", "BIOL111")
        self.assertIsNone(choose_profile(CS_YU, "CSCE470", [AG_PROFILE, other]))

    def test_profile_without_evidence_is_not_split_between_namesakes(self):
        empty = profile("empty", "Yu Zhang")
        self.assertIsNone(choose_profile(CS_YU, "CSCE470", [empty], [AG_YU]))

    def test_unique_name_without_evidence_still_matches(self):
        # A new or moved professor with an unshared name keeps their profile.
        person = Person("Darryl De Ruiter", frozenset({"ANTH"}))
        moved = profile("dr", "Darryl De Ruiter", "BIOL111")
        self.assertEqual(choose_profile(person, "ANTH409", [moved]).id, "dr")
        new = profile("dr", "Darryl De Ruiter")
        self.assertEqual(choose_profile(person, "ANTH409", [new]).id, "dr")

    def test_other_taught_subject_counts_as_evidence(self):
        # CSCE Yu Zhang also taught ENGR; an ENGR-only profile is theirs.
        engr = profile("engr", "Yu Zhang", "ENGR102")
        self.assertEqual(
            choose_profile(CS_YU, "CSCE470", [AG_PROFILE, engr], [AG_YU]).id, "engr"
        )

    def test_exact_course_beats_subject_match(self):
        course = profile("course", "Yu Zhang", "CSCE470")
        subject = profile("subject", "Yu Zhang", "CSCE670")
        self.assertEqual(
            choose_profile(CS_YU, "CSCE470", [subject, course]).id, "course"
        )

    def test_equal_evidence_is_ambiguous(self):
        twins = [
            profile("a", "Yu Zhang", "CSCE670"),
            profile("b", "Yu Zhang", "CSCE121"),
        ]
        self.assertIsNone(choose_profile(CS_YU, "CSCE470", twins))

    def test_exact_name_beats_fuzzy_name_with_same_evidence(self):
        yue = profile("yue", "Yue Zhang", "CSCE670")
        self.assertEqual(choose_profile(CS_YU, "CSCE470", [yue, CS_PROFILE]).id, "cs")

    def test_colleague_with_closer_name_keeps_their_profile(self):
        yue_person = Person("Yue Zhang", frozenset({"CSCE"}))
        yue = profile("yue", "Yue Zhang", "CSCE121")
        self.assertIsNone(choose_profile(CS_YU, "CSCE470", [yue], [yue_person]))
        self.assertEqual(
            choose_profile(yue_person, "CSCE121", [yue, CS_PROFILE], [CS_YU]).id, "yue"
        )

    def test_unique_fuzzy_name_needs_evidence(self):
        violet = Person("Violet M. Johnson", frozenset({"HIST"}))
        self.assertIsNone(
            choose_profile(violet, "HIST685", [profile("v", "Valen Johnson")])
        )
        joshua = Person("Joshua D. Edwards", frozenset({"CHEM"}))
        josh = profile("j", "Josh Edwards", "CHEM107")
        self.assertEqual(choose_profile(joshua, "CHEM697", [josh]).id, "j")

    def test_middle_initial_does_not_match_a_different_first_name(self):
        monique = Person("Monique S. Cooper", frozenset({"SPMT"}))
        marcia = profile("m", "Marcia Cooper", "MEEN221")
        self.assertIsNone(choose_profile(monique, "SPMT610", [marcia]))

    def test_dissimilar_names_never_match(self):
        self.assertIsNone(
            choose_profile(CS_YU, "CSCE470", [profile("x", "Ying Zhao", "CSCE470")])
        )


class SwappedNameTests(unittest.TestCase):
    """RMP sometimes swaps first and last name: "FURTH HOWARD" (Brett H. Furth)."""

    def test_swapped_profile_name_is_a_candidate(self):
        self.assertEqual(profile_match_name("Brett H. Furth", "FURTH ", "HOWARD"),
                         ("HOWARD FURTH", True))
        # Normal order keeps RMP's own split, as "Last, First".
        self.assertEqual(profile_match_name("Brett H. Furth", "Brett", "Furth"),
                         ("Furth, Brett", False))
        # The first name must be exactly the surname, and the rest must fit.
        self.assertIsNone(profile_match_name("Brett H. Furth", "Furth", "Smith"))
        self.assertIsNone(profile_match_name("Brett H. Furth", "Furthe", "Howard"))

    def test_swapped_profile_needs_review_evidence(self):
        furth = Person("Brett H. Furth", frozenset({"ANTH", "MAST"}))
        base = Profile("f", "HOWARD FURTH", frozenset({"ANTH"}), frozenset({"ANTH210"}),
                       swapped=True)
        self.assertEqual(choose_profile(furth, "ANTH210", [base]).id, "f")
        unrelated = replace(base, subjects=frozenset({"CHEM"}), course_codes=frozenset())
        self.assertIsNone(choose_profile(furth, "ANTH210", [unrelated]))

    def test_exactly_reversed_name_without_evidence_is_not_trusted(self):
        # "Thomas James" may be a different person from "James Thomas".
        person = Person("James Thomas", frozenset({"MATH"}))
        reversed_profile = Profile("tj", "James Thomas", frozenset(), frozenset(), swapped=True)
        self.assertIsNone(choose_profile(person, "MATH151", [reversed_profile]))
        same = replace(reversed_profile, swapped=False)
        self.assertEqual(choose_profile(person, "MATH151", [same]).id, "tj")


class RivalTests(unittest.TestCase):
    """A namesake blocks a profile only when their names could really be one."""

    def test_middle_initials_and_fuzzy_names_are_not_rivals(self):
        # Real report cases (Nov 2026): exact-name profiles rejected only
        # because of these "similar" names.
        for name, exact_name, other in [
            ("Brian A. Anderson", "Brian Anderson", "Arden Anderson"),
            ("Christine M. Griffith", "Christine Griffith", "Jennifer M. Griffith"),
            ("Han Xu", "Han Xu", "Chang Xu"),
            ("Arum Han", "Arum Han", "Irfan A. Khan"),
            ("Sumi Lee", "Sumi Lee", "Sumin Lee"),
            ("Mariano A. Castillo", "Mariano Castillo", "Victor A. Castillo"),
        ]:
            self.assertFalse(_similar_person(name, other), (name, other))
            person = Person(name, frozenset({"ECON"}))
            namesake = Person(other, frozenset({"ECON"}))
            exact = profile("p", exact_name)
            self.assertEqual(choose_profile(person, "ECON202", [exact], [namesake]).id, "p")

    def test_real_namesakes_still_block(self):
        for name, other in [
            ("Yu Zhang", "Yu Zhang"),
            ("J. Smith", "John Smith"),
            ("M. E. Rister", "Edward Rister"),
            ("William Jones", "Bill Jones"),
            ("Abigail Songok", "Abigael Songok"),
            ("Stephen K. Bender", "Steven D. Bender"),
            ("Juan Garcia-Lopez", "Juan Garcia"),
            ("John Taylor Smith", "John Taylor"),
            ("Hee Yeon Tak", "Heeyeon Tak"),
        ]:
            self.assertTrue(_similar_person(name, other), (name, other))
        person = Person("John Taylor Smith", frozenset({"HIST"}))
        taylor = profile("t", "Taylor, John", "HIST101")
        namesake = Person("John Taylor", frozenset({"HIST"}))
        self.assertIsNone(choose_profile(person, "HIST101", [taylor], [namesake]))

    def test_rivals_never_create_a_match(self):
        person = Person("Brian A. Anderson", frozenset({"ECON"}))
        other = profile("o", "Brian Andrews", "ECON202")
        namesake = Person("Brian Anderson", frozenset())
        self.assertIsNone(choose_profile(person, "ECON202", [other], [namesake]))

    def test_load_people_searches_every_surname_word(self):
        db = MagicMock()
        rows = db.query.return_value.join.return_value.filter.return_value
        rows.distinct.return_value.all.return_value = [
            (1, "John Taylor Smith", "HIST"),
            (2, "John Taylor", "HIST"),
            (3, "Jane Smith", "HIST"),
            (4, "Arden Smith", "HIST"),
        ]
        person, namesakes = load_people(db, "John Taylor Smith", 1, "HIST")
        self.assertEqual(person.subjects, frozenset({"HIST"}))
        self.assertEqual([p.name for p in namesakes], ["John Taylor"])
        clause = db.query.return_value.join.return_value.filter.call_args.args[0]
        sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
        self.assertIn("'%taylor%'", sql)
        self.assertIn("'%smith%'", sql)
        self.assertIn("translate", sql)  # cheap letters-only form
        self.assertEqual(surname_tokens("Douglas M. O'Brien"), ["obrien"])
        self.assertEqual(surname_tokens("Juan De La Cruz"), ["cruz"])


class CampusAndRenameTests(unittest.TestCase):
    def test_other_campus_school_needs_review_evidence(self):
        person = Person("Negin Mirhosseini", frozenset({"ENGR"}))
        empty = Profile("nm", "Mirhosseini, Negin", frozenset(), frozenset())
        self.assertEqual(choose_profile(person, "ENGR102", [empty]).id, "nm")
        self.assertIsNone(explain_choice(person, "ENGR102", [empty], name_alone=False)[0])
        reviewed = Profile("nm", "Mirhosseini, Negin", frozenset({"ENGR"}), frozenset({"ENGR102"}))
        self.assertEqual(explain_choice(person, "ENGR102", [reviewed], name_alone=False)[0].id, "nm")

    def test_renamed_subject_reviews_count(self):
        # Brian A. Anderson's reviews say PSYC; he now teaches PBSI.
        self.assertEqual(parse_course_code("PSYC 107", frozenset({"PBSI"})), ("PBSI", "107"))
        person = Person("Brian A. Anderson", frozenset({"PBSI", "NRSC"}))
        reviewed = Profile("ba", "Anderson, Brian", frozenset({"PBSI"}), frozenset({"PBSI107"}))
        self.assertEqual(choose_profile(person, "PSYC107", [reviewed]).id, "ba")


class ExactNameTests(unittest.TestCase):
    def test_exact_names_across_splits(self):
        self.assertTrue(_same_name("Hee Yeon Tak", "Tak, Heeyeon"))
        self.assertTrue(_same_name("Heidi K. Vanden Brink", "Vanden Brink, Heidi"))
        self.assertTrue(_same_name("Alejandro Castillo Ayala", "Castillo Ayala, Alejandro"))
        self.assertFalse(_same_name("John Taylor Smith", "Taylor, John"))
        self.assertFalse(_same_name("Alejandro Castillo Ayala", "Castillo, Alejandro"))
        # Nicknames still need review evidence.
        self.assertFalse(_same_name("Michael J. McShane", "McShane, Mike"))

    def test_rmp_names_keep_their_own_split(self):
        self.assertEqual(rmp_name("Alejandro", "Castillo Ayala"), "Castillo Ayala, Alejandro")
        self.assertEqual(rmp_name("Delbert", "Gatlin, Jr."), "Gatlin, Delbert")
        self.assertEqual(rmp_name("", "Briers"), "Briers")
        self.assertEqual(
            profile_match_name("Alejandro Castillo Ayala", "Alejandro", "Castillo"),
            ("Castillo, Alejandro", False),
        )
        self.assertEqual(profile_match_name("Delbert Gatlin", "Delbert", "Gatlin, Jr."),
                         ("Gatlin, Delbert", False))


class PrefilterTests(unittest.TestCase):
    def test_first_name_initials(self):
        self.assertEqual(given_initials("J. Michael Smith"), ["j", "m"])
        self.assertIn("b", given_initials("William R. Jones"))
        self.assertEqual(given_initials("M. E. Rister"), ["m", "e"])
        self.assertEqual(given_initials("Hee Yeon Tak"), ["h", "y"])
        # A middle initial is not a name the person goes by.
        self.assertEqual(given_initials("Brian A. Anderson"), ["b"])
        self.assertEqual(sorted(surname_tokens("Alejandro Castillo Ayala")), ["ayala", "castillo"])


class SwappedInitialTests(unittest.TestCase):
    def test_swapped_name_on_a_middle_initial_needs_course_reviews(self):
        self.assertEqual(profile_match_name("Andee S. Scott", "Scott", "Socolofsky"),
                         ("Socolofsky Scott", True))
        self.assertTrue(swapped_needs_course("Andee S. Scott", "Socolofsky Scott"))
        andee = Person("Andee S. Scott", frozenset({"OCEN"}))
        base = Profile("s", "Socolofsky Scott", frozenset({"OCEN"}), frozenset({"OCEN300"}),
                       swapped=True, needs_course=True)
        # Never matched automatically, even with reviews of the course.
        self.assertIsNone(choose_profile(andee, "OCEN201", [base]))
        self.assertIsNone(choose_profile(andee, "OCEN300", [base]))
        # A nickname of the surname is not the surname.
        self.assertIsNone(profile_match_name("Michael S. Alexander", "Alex", "Sprintson"))

    def test_furth_is_never_matched_automatically(self):
        # "FURTH HOWARD" fits Brett H. Furth only through the "H.": the
        # unmatched-instructors report proposes it for a person to confirm.
        self.assertTrue(swapped_needs_course("Brett H. Furth", "HOWARD FURTH"))
        furth = Person("Brett H. Furth", frozenset({"ANTH", "MAST"}))
        base = Profile("f", "HOWARD FURTH", frozenset({"ANTH"}), frozenset({"ANTH210"}),
                       swapped=True, needs_course=True)
        for course in ("ANTH210", "ANTH301"):
            chosen, reasons = explain_choice(furth, course, [base])
            self.assertIsNone(chosen)
            self.assertIn("middle initial", reasons["f"])
            self.assertIn("proposed for review", reasons["f"])
        # It does not block a profile that has evidence either.
        plain = Profile("p", "Brett Furth", frozenset({"ANTH"}), frozenset({"ANTH210"}))
        self.assertEqual(choose_profile(furth, "ANTH210", [base, plain]).id, "p")

    def test_swapped_name_on_a_full_middle_name_keeps_subject_evidence(self):
        self.assertFalse(swapped_needs_course("Mary Howard Furth", "Howard Furth"))
        mary = Person("Mary Howard Furth", frozenset({"ANTH"}))
        base = Profile("f", "Howard Furth", frozenset({"ANTH"}), frozenset({"ANTH210"}),
                       swapped=True)
        self.assertEqual(choose_profile(mary, "ANTH301", [base]).id, "f")


class CourseCodeTests(unittest.TestCase):
    def test_parses_free_text_codes(self):
        self.assertEqual(parse_course_code("Honors AGEC-105H", VALID), ("AGEC", "105"))
        self.assertEqual(parse_course_code("bio 111", VALID), ("BIOL", "111"))
        self.assertEqual(parse_course_code("CPSC 121", VALID), ("CSCE", "121"))

    def test_real_subject_is_never_aliased(self):
        # COSC is Construction Science, not old computer science.
        self.assertEqual(parse_course_code("COSC 222", VALID | {"COSC"}), ("COSC", "222"))
        self.assertIsNone(parse_course_code("COSC 222", VALID))
        self.assertEqual(course_search_key("cosc 222"), "COSC222")

    def test_unresolvable_codes_are_not_evidence(self):
        for raw in ["105200", "AGRICULTURE105", "XYZW101", "", None]:
            self.assertIsNone(parse_course_code(raw, VALID))


if __name__ == "__main__":
    unittest.main()
