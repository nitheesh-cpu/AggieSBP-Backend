"""Same-name professors must not share each other's RMP profiles."""

import unittest

from aggiermp.core.professor_identity import Person, Profile, choose_profile
from aggiermp.core.subjects import parse_course_code

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


class CourseCodeTests(unittest.TestCase):
    def test_parses_free_text_codes(self):
        self.assertEqual(parse_course_code("Honors AGEC-105H", VALID), ("AGEC", "105"))
        self.assertEqual(parse_course_code("bio 111", VALID), ("BIOL", "111"))
        self.assertEqual(parse_course_code("CPSC 121", VALID), ("CSCE", "121"))

    def test_unresolvable_codes_are_not_evidence(self):
        for raw in ["105200", "AGRICULTURE105", "XYZW101", "", None]:
            self.assertIsNone(parse_course_code(raw, VALID))


if __name__ == "__main__":
    unittest.main()
