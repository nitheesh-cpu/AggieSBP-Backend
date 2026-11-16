"""Duplicate RMP profiles of one person merge; different people never do.

A false merge shows one person's reviews on another's page, so most of these
tests are cases that must NOT merge (several came from an adversarial review
of the rules).
"""

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from aggiermp.core import professor_identity as identity
from aggiermp.core.professor_identity import (
    Person,
    Profile,
    duplicate_evidence,
    name_variant,
    plan_profile_merges,
    resolve_professor_id,
)

TAMU = "U2Nob29sLTEwMDM="
GALVESTON = "U2Nob29sLTEwMDY="


def profile(pid, name, codes=(), ratings=0, department="", university=TAMU, legacy=None):
    codes = frozenset(codes)
    return Profile(
        id=pid,
        name=name,
        subjects=frozenset(c.rstrip("0123456789") for c in codes),
        course_codes=codes,
        university_id=university,
        department=department,
        num_ratings=ratings,
        legacy_id=legacy,
    )


def person(name, *subjects, pidm=True):
    return Person(name, frozenset(subjects), has_pidm=pidm)


# Real data, Nov 2026: two RMP entries for one CHEM instructor.
ABIGAIL = profile("abigail", "Abigail Songok", {"CHEM119", "CHEM120"}, 124, "Chemistry", legacy=2736672)
ABIGAEL = profile("abigael", "Abigael Songok", {"CHEM119"}, 85, "Chemistry", legacy=2772801)
SONGOK = person("Songok, Abigael", "CHEM")


class NameVariantTests(unittest.TestCase):
    def test_same_person_spellings(self):
        self.assertEqual(name_variant("Sara Fehr", "Sarah Fehr"), "spelling")
        self.assertEqual(name_variant("Jonathan Meer", "Johnathan Meer"), "spelling")
        self.assertEqual(name_variant("Colin Young", "Collin Young"), "spelling")
        self.assertEqual(name_variant("Abigail Songok", "Abigael Songok"), "vowel")
        self.assertEqual(name_variant("Kristen Moore", "Kristin Moore"), "vowel")
        self.assertEqual(name_variant("Matthew Del Ciampo", "Matthew DelCiampo"), "same")
        self.assertEqual(name_variant("Mary Vickers", "Mary Jane Vickers"), "same")
        self.assertEqual(name_variant("Dr. Lin Jiang", "Lin Jiang"), "same")

    def test_names_of_different_people_are_rejected(self):
        for left, right in [
            ("Mark Johnson", "Mary Johnson"),  # one changed letter, short name
            ("Andrew Smith", "Andrea Smith"),  # masculine/feminine forms
            ("Paul Klein", "Paula Klein"),
            ("Louis Martin", "Louise Martin"),
            ("Francis Smith", "Frances Smith"),
            ("Jesse Smith", "Jessie Smith"),
            ("Jose Smith", "Josie Smith"),
            ("Rene Smith", "Renee Smith"),
            ("Andre Smith", "Andrea Smith"),
            ("Terry Smith", "Terri Smith"),
            ("Randy Smith", "Randi Smith"),
            ("Marian Smith", "Marion Smith"),
            ("Nicola Smith", "Nicole Smith"),
            ("Gabriele Smith", "Gabriela Smith"),
            ("Jaime Smith", "Jamie Smith"),
            ("Daniel Smith", "Danielle Smith"),
            ("Maria Smith", "Marie Smith"),  # different names
            ("Julia Smith", "Julie Smith"),
            ("Diana Smith", "Diane Smith"),
            ("Louis Garcia", "Luis Garcia"),
            ("Ahmad Ali", "Ahmed Ali"),
            ("Stephan Smith", "Stephen Smith"),
            ("Elena Smith", "Elina Smith"),
            ("Joung Kim", "Young Kim"),  # different first letter
            ("Shelby Blair", "Shelly Blair"),  # changed consonant
            ("Xiaoning Qian", "Xiaofeng Qian"),  # changed syllable
            ("Darrell Lynch", "Darryl Lynch"),  # two edits
            ("Dana Smith", "Dina Smith"),  # changed vowel in a short name
            ("Mary Jane Vickers", "Mary Ann Vickers"),  # middle names disagree
            ("Yu Zhang", "Yu Zhao"),  # different surname
            ("Sean Smith", "Sean"),  # no given name on one side
            ("John Smith Jr.", "John Smith Sr."),  # father and son
            ("John Smith", "John Smith III"),
            ("Dr. Lin Jiang", "Dr. Jiang"),  # a title is not a first name
            ("Wei Li", "Li Wei"),  # swapped order
        ]:
            with self.subTest(left=left, right=right):
                self.assertNotIn(name_variant(left, right), {"same", "spelling", "vowel"})

    def test_loose_variants_are_labelled_but_never_mergeable(self):
        self.assertEqual(name_variant("Larry Brown", "Lawrence Brown"), "nickname")
        self.assertEqual(name_variant("Christine Shimek", "Christina Shimek"), "nickname")
        self.assertEqual(name_variant("A Songok", "Abigael Songok"), "initial")


class DuplicateEvidenceTests(unittest.TestCase):
    def test_vowel_spelling_merges_with_exact_registrar_name_and_shared_course(self):
        reason = duplicate_evidence(ABIGAIL, ABIGAEL, [SONGOK])
        self.assertIn("CHEM119", reason)
        self.assertIn("Songok, Abigael", reason)
        self.assertIn("CHECK", reason)
        # Without the registrar a vowel difference is not enough.
        self.assertIsNone(duplicate_evidence(ABIGAIL, ABIGAEL))

    def test_identical_names_with_a_shared_course_merge(self):
        a = profile("a", "Jeffrey Hart", {"STAT212"}, 40)
        b = profile("b", "Jeffrey Hart", {"STAT212", "STAT211"}, 10)
        self.assertIn("no registrar record", duplicate_evidence(a, b))
        self.assertIn("only registrar", duplicate_evidence(a, b, [person("Hart, Jeffrey", "STAT")]))

    def test_shared_subject_without_a_shared_course_is_not_enough(self):
        a = profile("a", "Wei Li", {"MATH151"}, 40)
        b = profile("b", "Wei Li", {"MATH308"}, 10)
        self.assertIsNone(duplicate_evidence(a, b))
        self.assertIsNone(duplicate_evidence(a, b, [person("Li, Wei", "MATH")]))

    def test_cross_department_profiles_never_merge(self):
        # Same name under STAT and MATH, one registrar instructor teaching
        # both: without a shared course they stay apart (missed duplicates
        # are acceptable, false merges are not).
        stat = profile("stat", "Jane Doe", {"STAT211"}, 30)
        math = profile("math", "Jane Doe", {"MATH151"}, 12)
        self.assertIsNone(duplicate_evidence(stat, math, [person("Doe, Jane", "STAT", "MATH")]))

    def test_two_registrar_namesakes_block_the_merge(self):
        a = profile("a", "Wei Li", {"MATH151"}, 40)
        b = profile("b", "Wei Li", {"MATH151"}, 10)
        people = [person("Li, Wei", "MATH"), person("Li, Wei", "MATH", "STAT")]
        self.assertIsNone(duplicate_evidence(a, b, people))

    def test_any_compatible_namesake_blocks_even_with_another_first_name(self):
        # Registrar Christina Lee could be the "Chris" behind either profile.
        a = profile("a", "Christopher Lee", {"MATH151"}, 40)
        b = profile("b", "Christopher Lee", {"MATH151"}, 10)
        people = [person("Lee, Christopher", "MATH"), person("Lee, Christina", "MATH")]
        self.assertIsNone(duplicate_evidence(a, b, people))

    def test_registrar_name_must_match_exactly(self):
        # The only registrar namesake is John Smith; profiles are J/Jane.
        a = profile("a", "Jane Smith", {"MATH151"}, 40)
        b = profile("b", "Jane Smith", {"MATH151"}, 10)
        self.assertIsNone(duplicate_evidence(a, b, [person("Smith, John", "MATH")]))
        # Vowel variant: the registrar name must be one of the two exactly.
        young = profile("young", "Young Kim", {"MATH151"}, 40)
        yong = profile("yong", "Yong Kim", {"MATH151"}, 10)
        self.assertIsNone(duplicate_evidence(young, yong, [person("Kim, Yung", "MATH")]))

    def test_sections_without_pidm_can_block_but_never_prove(self):
        a = profile("a", "Yu Zhang", {"AGEC105"}, 30)
        b = profile("b", "Yu Zhang", {"AGEC105"}, 2)
        self.assertIsNone(duplicate_evidence(a, b, [person("Zhang, Yu", "AGEC", pidm=False)]))
        self.assertIsNone(
            duplicate_evidence(a, b, [person("Zhang, Yu", "AGEC"), person("Zhang, Yu", "AGEC", pidm=False)])
        )

    def test_the_two_yu_zhangs_stay_apart(self):
        ag = profile("ag", "Yu Zhang", {"AGEC105", "ECON202"}, 30)
        cs = profile("cs", "Yu Zhang", {"CSCE121"}, 2)
        people = [person("Zhang, Yu", "AGEC"), person("Zhang, Yu", "CSCE", "ENGR")]
        self.assertIsNone(duplicate_evidence(ag, cs, people))
        self.assertIsNone(duplicate_evidence(ag, cs))

    def test_nicknames_initials_and_empty_profiles_never_merge(self):
        larry = profile("larry", "Larry Brown", {"CHEM107"}, 214)
        lawrence = profile("lawrence", "Lawrence Brown", {"CHEM107"}, 156)
        self.assertIsNone(duplicate_evidence(larry, lawrence, [person("Brown, Lawrence", "CHEM")]))
        initial = profile("initial", "L Brown", {"CHEM107"}, 3)
        self.assertIsNone(duplicate_evidence(lawrence, initial, [person("Brown, Lawrence", "CHEM")]))
        full = profile("full", "Robert Lightfoot", {"CSCE121"}, 128, "Computer Science")
        empty = profile("empty", "Robert Lightfoot", (), 0, "Computer Science")
        self.assertIsNone(duplicate_evidence(full, empty, [person("Lightfoot, Robert", "CSCE")]))

    def test_father_and_son_never_merge(self):
        sr = profile("sr", "John Smith Sr.", {"HIST105"}, 30)
        jr = profile("jr", "John Smith Jr.", {"HIST105"}, 10)
        self.assertIsNone(duplicate_evidence(sr, jr))

    def test_profiles_at_different_or_unknown_campuses_never_merge(self):
        galveston = profile("g", "Abigael Songok", {"CHEM119"}, 3, university=GALVESTON)
        self.assertIsNone(duplicate_evidence(ABIGAIL, galveston, [SONGOK]))
        a = profile("a", "Jeffrey Hart", {"STAT212"}, 40, university="")
        b = profile("b", "Jeffrey Hart", {"STAT212"}, 10, university="")
        self.assertIsNone(duplicate_evidence(a, b))


class PlanTests(unittest.TestCase):
    def test_most_reviewed_profile_is_canonical(self):
        merges, ambiguous = plan_profile_merges([ABIGAEL, ABIGAIL], [SONGOK])
        self.assertEqual(ambiguous, [])
        self.assertEqual(len(merges), 1)
        self.assertEqual(merges[0].canonical_id, "abigail")
        self.assertEqual(merges[0].alias_id, "abigael")

    def test_three_entries_fold_into_one(self):
        a = profile("a", "Linda Katehi-Tseregounis", {"ECEN325"}, 32)
        b = profile("b", "Linda Katehi-Tseregounis", {"ECEN325"}, 10)
        c = profile("c", "Linda Pisti Basile Katehi-Tseregounis", {"ECEN325"}, 2)
        merges, _ = plan_profile_merges([c, b, a])
        self.assertEqual({(m.canonical_id, m.alias_id) for m in merges}, {("a", "b"), ("a", "c")})

    def test_hub_profile_does_not_pull_two_people_together(self):
        # A was reviewed for both courses; B and C share nothing, so B and C
        # are not shown to be one person and nothing in the group merges.
        hub = profile("hub", "Yu Zhang", {"AGEC105", "CSCE121"}, 40)
        ag = profile("ag", "Yu Zhang", {"AGEC105"}, 10)
        cs = profile("cs", "Yu Zhang", {"CSCE121"}, 5)
        merges, ambiguous = plan_profile_merges([hub, ag, cs])
        self.assertEqual(merges, [])
        self.assertEqual(ambiguous, [["ag", "cs", "hub"]])

    def test_unrelated_professors_are_untouched(self):
        profiles = [
            profile("ag", "Yu Zhang", {"AGEC105"}, 30),
            profile("cs", "Yu Zhang", {"CSCE121"}, 2),
            profile("mark", "Mark Johnson", {"MATH151"}, 9),
            profile("mary", "Mary Johnson", {"MATH151"}, 9),
        ]
        people = [person("Zhang, Yu", "AGEC"), person("Zhang, Yu", "CSCE")]
        self.assertEqual(plan_profile_merges(profiles, people), ([], []))


class AliasSyncTests(unittest.TestCase):
    def test_confirming_never_chains_merges(self):
        from pipelines.professors.merge_duplicates import chain_problem

        confirmed = {"abigael": "abigail"}
        self.assertIsNone(chain_problem("abby", "abigail", confirmed))
        # The canonical id is itself merged away.
        self.assertIsNotNone(chain_problem("abby", "abigael", confirmed))
        # The alias is what other entries are shown under.
        self.assertIsNotNone(chain_problem("abigail", "songok", confirmed))

    def test_only_confirmed_merges_are_loaded(self):
        db = MagicMock()
        db.execute.return_value.scalar.return_value = "professor_aliases"
        db.execute.return_value.fetchall.return_value = [("abigael", "abigail")]
        self.assertEqual(identity.load_aliases(db), {"abigael": "abigail"})
        sql = str(db.execute.call_args_list[-1].args[0])
        self.assertIn("status = 'confirmed'", sql)

    def test_merged_entries_load_as_one_profile(self):
        candidates = [
            SimpleNamespace(id="abigael", first_name="Abigael", last_name="Songok",
                            university_id=TAMU, department="Chemistry", num_ratings=10, legacy_id=2),
        ]
        canonical = SimpleNamespace(id="abigail", first_name="Abigail", last_name="Songok",
                                    university_id=TAMU, department="Chemistry", num_ratings=30, legacy_id=1)
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = [canonical]
        codes = [("abigael", "CHEM 119"), ("abigail", "CHEM 120")]
        db.query.return_value.filter.return_value.distinct.return_value.all.side_effect = [codes, []]
        with patch.object(identity, "_valid_subjects", return_value=frozenset({"CHEM"})):
            profiles = identity.load_profiles(db, candidates, {"abigael": "abigail"})
        self.assertEqual([p.id for p in profiles], ["abigail"])
        self.assertEqual(profiles[0].course_codes, frozenset({"CHEM119", "CHEM120"}))

    def test_sections_without_pidm_stay_separate_people(self):
        db = MagicMock()
        db.query.return_value.join.return_value.distinct.return_value.all.return_value = [
            (101, "Songok, Abigael", "CHEM"),
            (None, "Songok, Abigael", "STAT"),
            (201, "Zhang, Yu", "AGEC"),
            (202, "Zhang, Yu", "CSCE"),
        ]
        people = sorted(identity.load_all_people(db), key=lambda p: (p.name, sorted(p.subjects)))
        self.assertEqual(
            [(p.name, sorted(p.subjects), p.has_pidm) for p in people],
            [
                ("Songok, Abigael", ["CHEM"], True),
                # May be the same person or not: kept apart, and only blocks.
                ("Songok, Abigael", ["STAT"], False),
                ("Zhang, Yu", ["AGEC"], True),
                ("Zhang, Yu", ["CSCE"], True),
            ],
        )

    def test_old_id_resolves_to_canonical(self):
        db = MagicMock()
        db.execute.return_value.scalar.return_value = "professor_aliases"
        db.execute.return_value.fetchone.return_value = ("abigail",)
        self.assertEqual(resolve_professor_id(db, "abigael"), "abigail")
        db.execute.return_value.fetchone.return_value = None
        self.assertEqual(resolve_professor_id(db, "abigail"), "abigail")
        # Table not created yet: no lookup, and nothing rolled back.
        db.execute.return_value.scalar.return_value = None
        self.assertEqual(resolve_professor_id(db, "abigael"), "abigael")
        db.rollback.assert_not_called()


if __name__ == "__main__":
    unittest.main()
