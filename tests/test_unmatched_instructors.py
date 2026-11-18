"""Unmatched-instructors report: pure parts and the shared matcher, no database."""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parents[1]))

from aggiermp.core.campuses import TEXAS_AM_GALVESTON_ID, TEXAS_AM_ID
from aggiermp.core.professor_identity import (
    InstructorMatch, Person, Profile, choose_profile, explain_choice, match_instructor,
)
from pipelines.professors import unmatched_instructors as report


def query(all_rows=None):
    result = Mock()
    result.filter.return_value = result
    result.all.return_value = all_rows or []
    return result


def professor(pid, first, last, university_id=TEXAS_AM_ID, legacy_id=None):
    return SimpleNamespace(id=pid, first_name=first, last_name=last,
                           university_id=university_id, legacy_id=legacy_id)


def section(name, pidm, term="202711", campus="College Station", dept="ENGL",
            number="104", sid="202711_1", title="Composition"):
    return SimpleNamespace(instructor_name=name, instructor_pidm=pidm, term_code=term,
                           section_id=sid, dept=dept, course_number=number,
                           course_title=title, campus=campus)


def evidence(subjects_by_id, own=frozenset(), namesakes=(), courses_by_id=None):
    calls = {"people": 0, "profiles": 0}

    def load_people(db, name, pidm, dept):
        calls["people"] += 1
        return Person(name, frozenset(own) | {dept}), list(namesakes)

    def load_profiles(db, candidates, aliases=None):
        calls["profiles"] += 1
        return [Profile(c.id, f"{c.first_name} {c.last_name}",
                        frozenset(subjects_by_id.get(c.id, ())),
                        frozenset((courses_by_id or {}).get(c.id, ())))
                for c in candidates]

    patchers = [
        patch("aggiermp.core.professor_identity.load_people", load_people),
        patch("aggiermp.core.professor_identity.load_profiles", load_profiles),
    ]
    return patchers, calls


class TermSelectionTests(unittest.TestCase):
    def test_latest_term_per_campus_digit(self):
        codes = ["202631", "202711", "202712", "202632", "202611", "bad", None, "2027111"]
        self.assertEqual(report.pick_current_terms(codes), ["202711", "202712"])

    def test_override_wins(self):
        self.assertEqual(report.pick_current_terms(["202711"], ["202631", " 202631 "]), ["202631"])

    def test_grouping_uses_term_campus_and_skips_unknown_campuses(self):
        rows = [
            section("Hee Yeon Tak", 9, sid="a", campus=None),
            section("Hee Yeon Tak", 9, sid="b", number="210", title="Technical Writing", campus=""),
            section("Hee Yeon Tak", 9, sid="b", number="210", campus=""),
            section("Brett H. Furth", 3, term="202712", sid="c", campus="Galveston", dept="ANTH"),
            section("Someone Else", 4, sid="d", campus="Qatar"),
            section("", 5, sid="e"),
        ]
        campuses = {"202711": "College Station", "202712": "Galveston"}
        instructors, skipped = report.group_instructors(rows, campuses)
        self.assertEqual(skipped, 1)
        by_name = {i.name: i for i in instructors}
        tak = by_name["Hee Yeon Tak"]
        self.assertEqual(tak.campus, "College Station")
        self.assertEqual(sorted(tak.courses), ["ENGL104", "ENGL210"])
        self.assertEqual(len(tak.sections), 2)
        self.assertIsNone(tak.students)
        self.assertEqual(by_name["Brett H. Furth"].university_id, TEXAS_AM_GALVESTON_ID)
        only, _ = report.group_instructors(rows, campuses, "galveston")
        self.assertEqual([i.name for i in only], ["Brett H. Furth"])


class HitFilteringTests(unittest.TestCase):
    def test_swapped_rmp_name_is_plausible(self):
        hits = [
            {"legacyId": 2924307, "firstName": "FURTH", "lastName": "HOWARD", "numRatings": 12},
            {"legacyId": 1, "firstName": "Brett", "lastName": "Smith", "numRatings": 3},
            {"legacyId": 2, "firstName": "Anna", "lastName": "Furth", "numRatings": 3},
            # Real Galveston hit (Nov 2026). Close spellings of short
            # surnames are other families too often (Li/Liu), so no match.
            {"legacyId": 2961743, "firstName": "Brett", "lastName": "Ferth", "numRatings": 1},
        ]
        plausible, near = report.classify_hits("Brett H. Furth", hits)
        self.assertEqual([h["legacyId"] for h in plausible], [2924307])
        self.assertTrue(plausible[0]["swapped"])
        self.assertEqual([h["legacyId"] for h in near], [2])

    def test_fuzzy_search_noise_is_not_plausible(self):
        # Real RMP hits for "Hee Yeon Tak" (Nov 2026): none is her.
        hits = [
            {"legacyId": 2689953, "firstName": "Inn Hee", "lastName": "Gee"},
            {"legacyId": 3085383, "firstName": "Jesuino", "lastName": "Takachi Tomita"},
            {"legacyId": 9, "firstName": "Hee Yeon", "lastName": "Tak"},
        ]
        plausible, near = report.classify_hits("Hee Yeon Tak", hits)
        self.assertEqual([h["legacyId"] for h in plausible], [9])
        self.assertEqual(near, [])

    def test_classification(self):
        self.assertEqual(report.classify(False, []), report.UNCHECKED)
        self.assertEqual(report.classify(True, []), report.NOT_ON_RMP)
        self.assertEqual(report.classify(True, [{"db_id": "x"}]), report.RMP_IN_DB)
        self.assertEqual(report.classify(True, [{"db_id": "x"}, {"db_id": None}]), report.RMP_MISSING)


class UrlTests(unittest.TestCase):
    def test_school_ids(self):
        self.assertEqual(report.legacy_school_id(TEXAS_AM_ID), 1003)
        self.assertEqual(report.legacy_school_id(TEXAS_AM_GALVESTON_ID), 1006)
        self.assertIsNone(report.legacy_school_id("not base64!"))
        self.assertIsNone(report.legacy_school_id(None))

    def test_urls_are_encoded(self):
        self.assertEqual(report.rmp_profile_url(2924307),
                         "https://www.ratemyprofessors.com/professor/2924307")
        self.assertEqual(
            report.rmp_search_url(1003, "O'Brien, Mary A. R&D"),
            "https://www.ratemyprofessors.com/search/professors/1003?q=Mary%20R%26D%20O%27Brien",
        )
        self.assertEqual(report.rmp_search_url(1006, "Brett H. Furth"),
                         "https://www.ratemyprofessors.com/search/professors/1006?q=Brett%20Furth")
        self.assertEqual(report.site_professor_url("VGVh/Y2hl+ci0=?x"),
                         "https://aggieschedulebuilderplus.vercel.app/professor/VGVh%2FY2hl%2Bci0=%3Fx")


def unmatched_instructor(name='<script>alert("x")</script> Tak'):
    instructor = report.Instructor(name, 9, "College Station", TEXAS_AM_ID)
    instructor.courses["ENGL104"] = ("ENGL", "Comp & <Rhet>")
    instructor.sections.update({"a", "b"})
    instructor.matches["ENGL104"] = InstructorMatch(reason="no profile at this school has a matching name")
    instructor.category = report.RMP_IN_DB
    instructor.hits = [{"legacyId": 5, "name": 'Hee "Q" Tak', "score": 1.0, "numRatings": 2,
                        "department": "English", "db_id": "VGVh=", "db_reason": "tied <b>"}]
    instructor.rejected = [{"id": "p/1", "name": "H & Tak", "reason": "name <score>"}]
    return instructor


class OutputTests(unittest.TestCase):
    def test_html_escapes_data_and_is_self_contained(self):
        instructor = unmatched_instructor()
        summary = report.build_summary(3, [instructor], ["202711"], 0)
        page = report.render_html([instructor], summary)
        self.assertNotIn('<script>alert', page)
        self.assertIn("&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;", page)
        self.assertIn("Comp &amp; &lt;Rhet&gt;", page)
        self.assertIn("tied &lt;b&gt;", page)
        self.assertIn("Hee &quot;Q&quot; Tak", page)
        self.assertIn('href="https://www.ratemyprofessors.com/professor/5"', page)
        self.assertIn('href="https://aggieschedulebuilderplus.vercel.app/professor/p%2F1"', page)
        self.assertIn("search/professors/1003?q=", page)
        self.assertEqual(page.count("<script>"), 1)
        self.assertNotIn("src=", page)
        self.assertNotIn("<link", page)
        self.assertIn('data-filter="rmp_in_db_not_matched"', page)
        self.assertIn("2 matched", page)

    def test_sorting_and_csv(self):
        small, big = unmatched_instructor("A Small"), unmatched_instructor("B Big")
        big.sections.add("c")
        self.assertEqual([i.name for i in sorted([small, big], key=report.sort_key)],
                         ["B Big", "A Small"])
        small.section_students = {"a": 10}
        self.assertEqual(sorted([small, big], key=report.sort_key)[0].name, "A Small")
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "out" / "report.csv"
            report.write_csv(path, [big])
            text = path.read_text(encoding="utf-8")
        self.assertIn("rmp_in_db_not_matched", text)
        self.assertIn("in our table as VGVh=: tied <b>", text)


class FakeClient:
    def __init__(self, results=None, error=None):
        self.calls = []
        self.results = results or []
        self.error = error

    def search_professors(self, text, university_id):
        self.calls.append(text)
        if self.error:
            raise self.error
        return self.results


class RmpSearcherTests(unittest.TestCase):
    def make(self, path, client, delay=1.5):
        now = [100.0]
        sleeps = []

        def sleep(seconds):
            sleeps.append(seconds)
            now[0] += seconds

        searcher = report.RmpSearcher(path, delay, client_factory=lambda: client,
                                      sleep=sleep, clock=lambda: now[0])
        return searcher, sleeps

    def test_throttles_and_caches(self):
        hit = {"id": "x", "legacyId": 7, "firstName": "Hee", "lastName": "Tak", "school": {}}
        client = FakeClient([hit])
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "cache.json"
            searcher, sleeps = self.make(path, client, delay=0.2)
            self.assertEqual(searcher.delay, 1.0)  # never below one second
            first = searcher.search("Hee Tak", TEXAS_AM_ID)
            searcher.search("Brett Furth", TEXAS_AM_GALVESTON_ID)
            self.assertEqual(first[0]["legacyId"], 7)
            self.assertNotIn("school", first[0])
            # Cookie fetch, then a wait before each search.
            self.assertEqual(sleeps, [1.0, 1.0])
            self.assertEqual(client.calls, ["Hee Tak", "Brett Furth"])
            again, sleeps2 = self.make(path, FakeClient(error=AssertionError("no requests")))
            self.assertEqual(again.search("hee tak", TEXAS_AM_ID)[0]["legacyId"], 7)
            self.assertEqual(sleeps2, [])
            self.assertEqual(len(json.loads(path.read_text())["searches"]), 2)

    def test_stops_after_repeated_failures(self):
        client = FakeClient(error=RuntimeError("403"))
        searcher, _ = self.make(None, client)
        for name in ["a b", "c d", "e f", "g h"]:
            self.assertIsNone(searcher.search(name, TEXAS_AM_ID))
        self.assertTrue(searcher.disabled)
        self.assertEqual(len(client.calls), 3)


class SharedMatcherTests(unittest.TestCase):
    def run_match(self, rows, subjects, course="ANTH210", cache=None, db=None, **kwargs):
        patchers, calls = evidence(subjects, **kwargs)
        for patcher in patchers:
            patcher.start()
        try:
            db = db or Mock()
            if not cache:
                db.query.side_effect = [query(rows)]
            result = match_instructor(db, "Brett H. Furth", 3, TEXAS_AM_GALVESTON_ID,
                                      course[:4], course, {}, cache)
        finally:
            for patcher in patchers:
                patcher.stop()
        return result, calls, db

    def test_swapped_profile_needs_evidence_and_reports_why(self):
        furth = professor("furth", "FURTH ", "HOWARD", TEXAS_AM_GALVESTON_ID, 2924307)
        other = professor("bf", "Bob", "Fuller", TEXAS_AM_GALVESTON_ID)
        matched, _, _ = self.run_match([furth, other], {"furth": {"ANTH"}},
                                       courses_by_id={"furth": {"ANTH210"}})
        self.assertEqual(matched.professor.id, "furth")
        self.assertEqual(matched.swapped_names, {"furth": "HOWARD FURTH"})
        self.assertEqual([c.id for c in matched.pool], ["furth", "bf"])

        missed, _, _ = self.run_match([furth, other], {"furth": {"CHEM"}})
        self.assertIsNone(missed.professor)
        self.assertIn("swapped RMP name needs review evidence", missed.rejected["furth"])
        self.assertIn("reviews are for CHEM", missed.rejected["furth"])
        self.assertTrue(missed.reason)

        instructor = report.Instructor("Brett H. Furth", 3, "Galveston", TEXAS_AM_GALVESTON_ID)
        instructor.matches["ANTH210"] = missed
        self.assertIn("swapped", report.rejection_reason(furth, instructor, {}))
        self.assertIn("name check failed", report.rejection_reason(other, instructor, {}))
        outsider = professor("zz", "Brett", "Furth", TEXAS_AM_ID)
        self.assertIn("another school", report.rejection_reason(outsider, instructor, {}))
        stranger = professor("yy", "Brett", "Furth", TEXAS_AM_GALVESTON_ID)
        self.assertIn("prefilter", report.rejection_reason(stranger, instructor, {}))
        rejected = report.rejected_candidates(instructor)
        self.assertEqual(rejected[0]["id"], "furth")

    def test_no_surname_is_not_shown(self):
        db = Mock()
        result = match_instructor(db, "  ", None, TEXAS_AM_ID, "ENGL", "ENGL104")
        self.assertFalse(result.shown)
        db.query.assert_not_called()

    def test_cache_reuses_queries_across_courses(self):
        furth = professor("furth", "Brett", "Furth", TEXAS_AM_GALVESTON_ID)
        cache, db = {}, Mock()
        db.query.side_effect = [query([furth])]
        patchers, calls = evidence({"furth": {"ANTH"}})
        for patcher in patchers:
            patcher.start()
        try:
            for code in ["ANTH210", "ANTH301", "MARB101"]:
                result = match_instructor(db, "Brett H. Furth", 3, TEXAS_AM_GALVESTON_ID,
                                          code[:4], code, {}, cache)
                self.assertEqual(result.professor.id, "furth")
        finally:
            for patcher in patchers:
                patcher.stop()
        self.assertEqual(db.query.call_count, 1)
        self.assertEqual(calls, {"people": 2, "profiles": 1})  # people per dept

    def test_match_courses_stops_at_first_match(self):
        instructor = report.Instructor("Brett H. Furth", 3, "Galveston", TEXAS_AM_GALVESTON_ID)
        instructor.courses = {"ANTH210": ("ANTH", ""), "MARB101": ("MARB", "")}
        furth = professor("furth", "Brett", "Furth", TEXAS_AM_GALVESTON_ID)
        db = Mock()
        db.query.side_effect = [query([furth])]
        patchers, _ = evidence({"furth": {"ANTH"}})
        for patcher in patchers:
            patcher.start()
        try:
            report.match_courses(db, instructor, {}, {})
        finally:
            for patcher in patchers:
                patcher.stop()
        self.assertEqual(instructor.matched_id, "furth")
        self.assertEqual(list(instructor.matches), ["ANTH210"])


class ExplainChoiceTests(unittest.TestCase):
    def test_explanations_agree_with_choose_profile(self):
        cs_yu = Person("Yu Zhang", frozenset({"CSCE"}))
        ag_yu = Person("Yu Zhang", frozenset({"AGEC"}))
        ag = Profile("ag", "Yu Zhang", frozenset({"AGEC"}), frozenset({"AGEC105"}))
        twin_a = Profile("a", "Yu Zhang", frozenset({"CSCE"}), frozenset())
        twin_b = Profile("b", "Yu Zhang", frozenset({"CSCE"}), frozenset())
        far = Profile("far", "Ying Zhao", frozenset({"CSCE"}), frozenset())
        cases = [
            (cs_yu, [ag], [ag_yu]),
            (cs_yu, [twin_a, twin_b, far], []),
            (ag_yu, [ag, twin_a], [cs_yu]),
        ]
        for person, profiles, namesakes in cases:
            chosen, reasons = explain_choice(person, "CSCE470", profiles, namesakes)
            self.assertEqual(chosen, choose_profile(person, "CSCE470", profiles, namesakes))
            for profile in profiles:
                if profile is not chosen:
                    self.assertIn(profile.id, reasons)
        _, reasons = explain_choice(cs_yu, "CSCE470", [ag], [ag_yu])
        self.assertIn("another instructor", reasons["ag"])
        _, reasons = explain_choice(cs_yu, "CSCE470", [twin_a, twin_b, far])
        self.assertIn("tied", reasons["a"])
        self.assertIn("name score", reasons["far"])


if __name__ == "__main__":
    unittest.main()
