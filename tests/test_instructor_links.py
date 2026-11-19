"""Reviewed instructor <-> RMP profile links: matcher, report proposals,
admin decisions and the admin API. No database: rows live in memory.

Run with: PYTHONPATH="src;." pytest --noconftest tests/test_instructor_links.py
"""

import asyncio
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

sys.path.insert(0, str(Path(__file__).parents[1]))

from fastapi import FastAPI
from fastapi.testclient import TestClient

from aggiermp.api.routers import admin_links
from aggiermp.core import instructor_links as links_module
from aggiermp.core.admin_auth import require_admin_key
from aggiermp.core.campuses import TEXAS_AM_GALVESTON_ID, TEXAS_AM_ID
from aggiermp.core.instructor_links import (
    LinkError, LinkRow, LoadedLinks, Proposal, build_instructors, instructor_key,
    load_instructor_links, load_spring_section_counts, plan_links, plan_no_profile,
    plan_proposals, plan_reject,
    plan_undo, rmp_search_url, select_tab,
)
from aggiermp.core.professor_identity import Person, Profile, match_instructor_any_campus
from aggiermp.database.base import request_session
from pipelines.professors import unmatched_instructors as report

sys.path.insert(0, str(Path(__file__).parent))
from test_campus_matching import details_endpoint, query as endpoint_query  # noqa: E402

NOW = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)
EARLIER = NOW - timedelta(days=30)


def professor(pid, first, last, university_id=TEXAS_AM_ID, legacy_id=None, **extra):
    return SimpleNamespace(id=pid, first_name=first, last_name=last,
                           university_id=university_id, legacy_id=legacy_id,
                           department=extra.get("department"),
                           num_ratings=extra.get("num_ratings", 0),
                           avg_rating=extra.get("avg_rating"))


def query(all_rows=None):
    result = Mock()
    result.filter.return_value = result
    result.all.return_value = all_rows or []
    return result


def evidence(subjects_by_id, courses_by_id=None):
    def load_people(db, name, pidm, dept):
        return Person(name, frozenset({dept})), []

    def load_profiles(db, candidates, aliases=None):
        return [Profile(c.id, f"{c.first_name} {c.last_name}",
                        frozenset(subjects_by_id.get(c.id, ())),
                        frozenset((courses_by_id or {}).get(c.id, ())),
                        num_ratings=getattr(c, "num_ratings", 0))
                for c in candidates]

    return [
        patch("aggiermp.core.professor_identity.load_people", load_people),
        patch("aggiermp.core.professor_identity.load_profiles", load_profiles),
    ]


def started(test, patchers):
    for patcher in patchers:
        patcher.start()
        test.addCleanup(patcher.stop)


def row(rid, key, pid, status="proposed", kind="plausible", reviewed=None, name="Seong G. Choi",
        courses=("ECEN214",), sections=3, evidence=None, updated=EARLIER):
    return LinkRow(
        id=rid, instructor_key=key, instructor_name=name, instructor_pidm=None,
        campus="College Station", courses=list(courses), sections=sections,
        professor_id=pid, kind=kind, status=status, reviewed_at=reviewed,
        evidence=dict(evidence or {}), created_at=EARLIER, updated_at=updated,
    )


# ---------------------------------------------------------------------------
# Matcher
# ---------------------------------------------------------------------------


class LoadLinksTests(unittest.TestCase):
    def test_mock_session_and_missing_table_give_no_links(self):
        self.assertFalse(load_instructor_links(Mock()))
        db = Mock()
        db.execute.return_value.scalar.return_value = None
        self.assertFalse(load_instructor_links(db))
        db.rollback.assert_not_called()

    def test_reads_decisions_only(self):
        db = Mock()
        exists, rows = Mock(), Mock()
        exists.scalar.return_value = "instructor_profile_links"
        rows.fetchall.return_value = [
            ("pidm:1", "a", "rejected"), ("pidm:1", "b", "confirmed"),
            ("pidm:2", None, "confirmed"), ("pidm:3", "d", "confirmed"),
            ("pidm:3", "c", "confirmed"),
        ]
        db.execute.side_effect = [exists, rows]
        links = load_instructor_links(db)
        self.assertEqual(links.confirmed, {"pidm:1": ["b"], "pidm:3": ["d", "c"]})
        self.assertEqual(links.no_profile, {"pidm:2"})
        self.assertEqual(links.rejected_ids("pidm:1"), {"a"})
        self.assertEqual(links.rejected_only().confirmed, {})

    def test_keys(self):
        self.assertEqual(instructor_key("Tanya V. Wickliff", 556672), "pidm:556672")
        self.assertEqual(instructor_key(" Staff Person ", None), "name:Staff Person")
        self.assertEqual(rmp_search_url("Brett H. Furth", "Galveston"),
                         "https://www.ratemyprofessors.com/search/professors/1006?q=Brett%20Furth")
        self.assertIn("/1003?q=Seong%20Choi", rmp_search_url("Seong G. Choi", "McAllen"))


class MatcherLinkTests(unittest.TestCase):
    def setUp(self):
        started(self, evidence({"yu": {"CSCE"}, "other": {"CSCE"}}))

    def test_confirmed_link_wins_without_running_the_matcher(self):
        gwan = professor("gwan", "Gwan", "Choi", legacy_id=2229614)
        db = Mock()
        db.get.return_value = gwan
        links = LoadedLinks(confirmed={"pidm:7": ["gwan"]})
        result = match_instructor_any_campus(db, "Seong G. Choi", 7, TEXAS_AM_ID, "ECEN",
                                             "ECEN214", {}, links=links)
        self.assertIs(result.professor, gwan)
        db.query.assert_not_called()
        self.assertEqual(db.get.call_args.args[1], "gwan")

    def test_multiple_confirmed_links_prefer_course_evidence_then_rating_count(self):
        started(self, evidence({}, {"course": {"ECEN214"}}))
        more_ratings = professor("popular", "Gwan", "Choi", num_ratings=40)
        course_profile = professor("course", "Gwan", "Choi", num_ratings=2)
        db = Mock()
        rows = {p.id: p for p in (more_ratings, course_profile)}
        db.get.side_effect = lambda model, pid: rows.get(pid)
        links = LoadedLinks(confirmed={"pidm:7": ["course", "popular", "gone", "alias"]})
        cache = {}

        def match(course):
            return match_instructor_any_campus(
                db, "Seong G. Choi", 7, TEXAS_AM_ID, "ECEN", course,
                {"alias": "course"}, cache, links,
            ).professor

        self.assertIs(match("ECEN 214"), course_profile)
        self.assertIs(match("ECEN248"), more_ratings)
        self.assertEqual(db.get.call_count, 3)

    def test_confirmed_link_to_a_merged_alias_shows_the_canonical_row(self):
        db = Mock()
        db.get.return_value = professor("canon", "Gwan", "Choi")
        links = LoadedLinks(confirmed={"pidm:7": ["alias"]})
        result = match_instructor_any_campus(db, "Seong G. Choi", 7, TEXAS_AM_ID, "ECEN",
                                             "ECEN214", {"alias": "canon"}, links=links)
        self.assertEqual(result.professor.id, "canon")
        self.assertEqual(db.get.call_args.args[1], "canon")

    def test_missing_linked_profile_falls_back_to_the_matcher(self):
        db = Mock()
        db.get.return_value = None
        db.query.side_effect = [query([professor("yu", "Yu", "Zhang")])]
        links = LoadedLinks(confirmed={"pidm:7": ["gone"]})
        result = match_instructor_any_campus(db, "Yu Zhang", 7, TEXAS_AM_ID, "CSCE",
                                             "CSCE411", {}, links=links)
        self.assertEqual(result.professor.id, "yu")

    def test_no_profile_decision_shows_none(self):
        db = Mock()
        links = LoadedLinks(no_profile={"pidm:7"})
        result = match_instructor_any_campus(db, "Yu Zhang", 7, TEXAS_AM_ID, "CSCE",
                                             "CSCE411", {}, links=links)
        self.assertIsNone(result.professor)
        self.assertTrue(result.shown)
        self.assertIn("no RMP profile", result.reason)
        db.query.assert_not_called()

    def test_rejected_pair_is_removed_before_choosing(self):
        rows = [professor("yu", "Yu", "Zhang"), professor("other", "Yu", "Zhang")]

        def run(links, pidm=7):
            db = Mock()
            db.query.side_effect = [query(rows)]
            return match_instructor_any_campus(db, "Yu Zhang", pidm, TEXAS_AM_ID, "CSCE",
                                               "CSCE411", {}, links=links)

        # Two equally good profiles: a tie, nothing shown.
        self.assertIsNone(run(None).professor)
        # Turning one down for this person leaves the other.
        links = LoadedLinks(rejected={"pidm:7": {"other"}})
        self.assertEqual(run(links).professor.id, "yu")
        # Another person with the same name is not affected.
        self.assertIsNone(run(links, pidm=8).professor)
        # Everything rejected: nothing shown, and the reason says why.
        everything = run(LoadedLinks(rejected={"pidm:7": {"yu", "other"}}))
        self.assertIsNone(everything.professor)
        self.assertIn("rejected in the admin console", everything.reason)

    def test_rejection_does_not_leak_through_a_shared_cache(self):
        rows = [professor("yu", "Yu", "Zhang"), professor("other", "Yu", "Zhang")]
        db, cache = Mock(), {}
        db.query.side_effect = [query(rows)]
        links = LoadedLinks(rejected={"pidm:7": {"other"}})
        first = match_instructor_any_campus(db, "Yu Zhang", 7, TEXAS_AM_ID, "CSCE",
                                            "CSCE411", {}, cache, links=links)
        second = match_instructor_any_campus(db, "Yu Zhang", 8, TEXAS_AM_ID, "CSCE",
                                             "CSCE411", {}, cache, links=links)
        self.assertEqual(first.professor.id, "yu")
        self.assertIsNone(second.professor)
        self.assertEqual({p.id for p in second.profiles}, {"yu", "other"})


class EndpointLinkTests(unittest.TestCase):
    def test_endpoint_shows_the_confirmed_profile(self):
        gwan = SimpleNamespace(id="gwan", first_name="Gwan", last_name="Choi",
                               university_id=TEXAS_AM_ID, num_ratings=40, avg_rating=4.1,
                               legacy_id=2229614)
        db = Mock()
        db.get.return_value = gwan
        db.query.side_effect = [
            endpoint_query(first=SimpleNamespace(term_desc="Spring 2027 - College Station")),
            endpoint_query([SimpleNamespace(instructor_name="Seong G. Choi", instructor_pidm=7,
                                            section_number="501", campus="College Station")]),
            endpoint_query(), endpoint_query(), endpoint_query(), endpoint_query(),
        ]
        links = LoadedLinks(confirmed={"pidm:7": ["gwan"]})
        with patch.object(links_module, "load_instructor_links", return_value=links):
            result = asyncio.run(details_endpoint()(None, "202711", "ECEN214", db, None))
        shown = result["professors"][0]
        self.assertEqual(shown["id"], "gwan")
        self.assertEqual(shown["name"], "Seong G. Choi")
        self.assertEqual(shown["rmpId"], 2229614)


# ---------------------------------------------------------------------------
# Report: candidates and proposals
# ---------------------------------------------------------------------------


def instructor(name, pidm=7, courses=("ECEN214", "ECEN248"), campus="College Station",
               campus_uid=TEXAS_AM_ID):
    person = report.Instructor(name, pidm, campus, campus_uid or TEXAS_AM_ID, campus_uid)
    for code in courses:
        person.courses[code] = (code[:4], "")
    person.sections.update({f"{name}-1", f"{name}-2"})
    return person


class InitialCandidateTests(unittest.TestCase):
    def test_strict_surname(self):
        self.assertTrue(report.strict_surname_match("Seong G. Choi", "Choi"))
        self.assertTrue(report.strict_surname_match("Tanya V. Wickliff", "Dugat-Wickliff"))
        self.assertTrue(report.strict_surname_match("Juan A. Garcia-Lopez", "Garcia"))
        self.assertFalse(report.strict_surname_match("Seong G. Choi", "Choiniere"))
        self.assertFalse(report.strict_surname_match("Ana B. Ruiter", "De Ruiterman"))

    def test_kinds(self):
        fit = report.initial_candidate("Seong G. Choi", "Gwan", "Choi")
        self.assertEqual(fit[0], "middle_initial")
        self.assertEqual(report.initial_candidate("Marjorie A. Clendenin", "Angela",
                                                  "Clendenin")[0], "middle_initial")
        swapped = report.initial_candidate("Brett H. Furth", "FURTH ", "HOWARD")
        self.assertEqual(swapped[:2], ("swapped", "HOWARD FURTH"))
        # Not an assumption on the middle initial:
        self.assertIsNone(report.initial_candidate("Seong G. Choi", "Seong", "Choi"))
        self.assertIsNone(report.initial_candidate("Seong G. Choi", "Gwan", "Choiniere"))
        self.assertIsNone(report.initial_candidate("Seong Choi", "Gwan", "Choi"))
        self.assertIsNone(report.initial_candidate("S. G. Choi", "Gwan", "Choi"))
        # A swapped name on the full given name keeps the matcher's own rule.
        self.assertIsNone(report.initial_candidate("Mary Howard Furth", "Furth", "Howard"))

    def test_middle_initial_candidates_with_course_overlap(self):
        person = instructor("Seong G. Choi", courses=("ECEN214", "ECEN248", "PSYC107"))
        gwan = professor("gwan", "Gwan", "Choi", legacy_id=2229614, department="Engineering",
                         num_ratings=40, avg_rating=4.1)
        gwen_other = professor("gc", "Gwen", "Choiniere")
        sam = professor("sam", "Sam", "Choi")
        db = Mock()
        db.query.side_effect = [query([gwan, gwen_other, sam]), query([gwan])]
        report.find_initial_candidates(db, [person])
        self.assertEqual([c["id"] for c in person.initial_candidates], ["gwan"])
        self.assertEqual(person.initial_candidates[0]["kind"], "middle_initial")

        person.candidates = report.collect_candidates(person)

        def load_profiles(db, rows, aliases=None):
            return [Profile("gwan", "Gwan Choi", frozenset({"ECEN", "PBSI"}),
                            frozenset({"ECEN214", "PBSI107", "ECEN601"}))]

        with patch("aggiermp.core.professor_identity.load_profiles", load_profiles):
            report.attach_profile_evidence(db, [person])
        candidate = person.candidates["gwan"]
        # PSYC107 is PBSI107 now.
        self.assertEqual(candidate["courseOverlap"], ["ECEN214", "PSYC107"])
        self.assertEqual(candidate["department"], "Engineering")
        self.assertTrue(candidate["inDatabase"])

        proposals = report.build_proposals([person])
        self.assertEqual(len(proposals), 1)
        proposal = proposals[0]
        self.assertEqual((proposal.key, proposal.professor_id, proposal.kind),
                         ("pidm:7", "gwan", "middle_initial"))
        self.assertEqual(proposal.rmp_legacy_id, 2229614)
        self.assertEqual(proposal.evidence["courseOverlap"], ["ECEN214", "PSYC107"])
        self.assertEqual(proposal.evidence["rmpName"], "Gwan Choi")
        self.assertIn("middle initial G.", proposal.reason)
        # The HTML and CSV show the overlap.
        page = report.render_html([person], report.build_summary(1, [person], ["202711"], 0))
        self.assertIn("reviews mention ECEN214, PSYC107", page)
        self.assertIn("reviews mention ECEN214, PSYC107", report.csv_row(person)["initial_candidates"])

    def test_furth_becomes_a_swapped_proposal(self):
        person = instructor("Brett H. Furth", pidm=3, courses=("ANTH210",), campus="Galveston",
                            campus_uid=TEXAS_AM_GALVESTON_ID)
        furth = professor("furth", "FURTH ", "HOWARD", TEXAS_AM_GALVESTON_ID, 2924307)
        person.rejected = [{"id": "furth", "name": "HOWARD FURTH",
                            "reason": "its swapped RMP name fits only through a middle initial"}]
        db = Mock()
        db.query.side_effect = [query([furth])]
        report.find_initial_candidates(db, [person])
        person.candidates = report.collect_candidates(person)
        candidate = person.candidates["furth"]
        self.assertEqual(candidate["kind"], "swapped")
        self.assertEqual(sorted(candidate["kinds"]), ["rejected", "swapped"])
        self.assertIn("matcher:", candidate["reason"])
        self.assertEqual(report.candidate_schools(person), [TEXAS_AM_GALVESTON_ID, TEXAS_AM_ID])
        proposal = report.build_proposals([person])[0]
        self.assertEqual((proposal.kind, proposal.key), ("swapped", "pidm:3"))


class ReportRowTests(unittest.TestCase):
    def test_drops_instructors_without_candidates_and_decided_ones(self):
        empty = instructor("No Body", pidm=1)
        hit = instructor("Has Hit", pidm=2)
        hit.near_hits = [{"name": "Has Hits", "score": 0.5}]
        rejected = instructor("Has Rejected", pidm=3)
        rejected.rejected = [{"id": "x", "name": "Has Rejectedd", "reason": "r"}]
        initial = instructor("Has I. Initial", pidm=4)
        initial.initial_candidates = [{"id": "y", "kind": "middle_initial", "name": "Ian Initial",
                                       "reason": "r", "score": 0.9}]
        decided = instructor("Decided Person", pidm=5)
        decided.rejected = [{"id": "z", "name": "Decided Person", "reason": "r"}]
        links = LoadedLinks(confirmed={"pidm:5": ["z"]})
        listed, dropped, decided_count = report.report_rows(
            [empty, hit, rejected, initial, decided], links)
        self.assertEqual({i.name for i in listed}, {"Has Hit", "Has Rejected", "Has I. Initial"})
        self.assertEqual((dropped, decided_count), (1, 1))
        summary = report.build_summary(10, listed, ["202711"], 0, unmatched=4,
                                       dropped=dropped, decided=decided_count)
        self.assertEqual((summary["matched"], summary["listed"]), (6, 3))
        self.assertIn("1 unmatched with no candidate left out", report.render_html(listed, summary))
        self.assertEqual(report.build_proposals([empty]), [])


def proposal(key, pid, kind="plausible", name="Seong G. Choi", evidence=None):
    return Proposal(key=key, name=name, pidm=None, campus="College Station",
                    courses=["ECEN214"], sections=3, professor_id=pid, rmp_legacy_id=None,
                    kind=kind, score=0.9, reason="why", evidence=evidence or {})


class ProposalPlanTests(unittest.TestCase):
    def test_upsert_rules(self):
        existing = [
            row(1, "k1", "confirmed-pair", status="confirmed", reviewed=EARLIER),
            row(2, "k2", "rejected-pair", status="rejected", reviewed=EARLIER),
            row(3, "k2", "open", kind="near"),
            row(4, "k3", "stale"),
            row(5, "k4", "kept", status="confirmed", reviewed=EARLIER),
            row(6, "k4", "old-proposal"),
            row(7, "k5", "same", evidence={"courseOverlap": []}),
        ]
        same = proposal("k5", "same", evidence={"courseOverlap": []})
        same.kind, same.reason, same.score = "plausible", None, None
        existing[6].reason, existing[6].score, existing[6].rmp_legacy_id = None, None, None
        existing[6].instructor_pidm = None
        ops, counts = plan_proposals(existing, [
            proposal("k1", "confirmed-pair"),        # decided pair: untouched
            proposal("k1", "brand-new"),             # decided instructor: new -> conflict
            proposal("k2", "rejected-pair"),         # never re-proposed
            proposal("k2", "open", kind="middle_initial"),  # refreshed
            proposal("k2", "open"),                  # duplicate in one run: ignored
            proposal("k4", "old-proposal"),          # decided instructor: untouched
            proposal("k6", "fresh"),                 # new instructor
            same,                                    # nothing changed
        ], auto_matched=["k3", "k4"])
        inserts = [(op.row.instructor_key, op.row.professor_id, op.row.status)
                   for op in ops if op.action == "insert"]
        self.assertEqual(sorted(inserts), [("k1", "brand-new", "proposed"),
                                           ("k6", "fresh", "proposed")])
        updates = {op.row_id: op.fields for op in ops if op.action == "update"}
        self.assertEqual(list(updates), [3])
        self.assertEqual(updates[3]["kind"], "middle_initial")
        # k3 is matched automatically now; k4 has a decision, so it keeps its row.
        self.assertEqual([op.row_id for op in ops if op.action == "delete"], [4])
        self.assertEqual(counts, {"inserted": 2, "updated": 1, "deleted": 1,
                                  "unchanged": 4, "conflicts": 1})

    def test_new_candidate_after_a_decision_is_a_conflict(self):
        existing = [row(1, "k1", "linked", status="confirmed", reviewed=EARLIER)]
        ops, _ = plan_proposals(existing, [proposal("k1", "newcomer")])
        rows = existing + [LinkRow(**{**ops[0].row.__dict__, "id": 2})]
        professors = {"linked": {"name": "Gwan Choi"}, "newcomer": {"name": "G Choi"}}
        [found] = build_instructors(rows, professors)
        self.assertEqual(found["tab"], "conflict")
        self.assertIn("1 new candidate since the decision", found["conflict"])
        self.assertEqual(found["linkedProfessorId"], "linked")


# ---------------------------------------------------------------------------
# Admin decisions and listing
# ---------------------------------------------------------------------------


class ConflictTests(unittest.TestCase):
    def test_tabs_and_conflicts(self):
        rows = [
            row(1, "pidm:1", "a", name="Open Person", sections=2),
            row(2, "pidm:1", "b", status="rejected", reviewed=EARLIER, name="Open Person", sections=2),
            row(3, "pidm:2", "c", status="rejected", reviewed=EARLIER, name="Rejected Person"),
            row(4, "pidm:3", "d", status="confirmed", reviewed=EARLIER, name="Linked Person", sections=9),
            row(5, "pidm:4", None, status="confirmed", kind="manual", reviewed=EARLIER,
                name="No Profile Person"),
            row(6, "pidm:5", "gone", status="confirmed", reviewed=EARLIER, name="Broken Link"),
            row(7, "pidm:6", "shared", status="confirmed", reviewed=EARLIER, name="Twin A"),
            row(8, "pidm:7", "shared", status="confirmed", reviewed=EARLIER, name="Twin B"),
        ]
        professors = {pid: {"name": pid.upper(), "department": "Eng", "numRatings": 3,
                            "avgRating": 4.0, "legacyId": 1}
                      for pid in ("a", "b", "c", "d", "shared")}
        found = {i["key"]: i for i in build_instructors(rows, professors)}
        self.assertEqual(found["pidm:1"]["tab"], "proposed")
        self.assertEqual(found["pidm:2"]["tab"], "rejected")
        self.assertEqual(found["pidm:3"]["tab"], "confirmed")
        self.assertEqual(found["pidm:4"]["tab"], "confirmed")
        self.assertTrue(found["pidm:4"]["noProfile"])
        self.assertEqual(found["pidm:5"]["tab"], "conflict")
        self.assertIn("not in our professors table", found["pidm:5"]["conflict"])
        self.assertEqual(found["pidm:6"]["tab"], "conflict")
        self.assertIn("also confirmed for Twin B", found["pidm:6"]["conflict"])
        candidate = found["pidm:1"]["candidates"][0]
        self.assertEqual(candidate["professorId"], "a")
        self.assertTrue(candidate["inDatabase"])
        self.assertEqual(candidate["department"], "Eng")
        self.assertFalse(found["pidm:5"]["candidates"][0]["inDatabase"])
        self.assertIn("search/professors/1003", found["pidm:1"]["rmpSearchUrl"])

        page = select_tab(list(found.values()), "conflict", limit=1)
        self.assertEqual(page["counts"], {"proposed": 1, "confirmed": 2, "rejected": 1,
                                          "conflict": 3})
        self.assertEqual(page["total"], 3)
        self.assertEqual(len(page["instructors"]), 1)
        filtered = select_tab(list(found.values()), "confirmed", q="linked")
        self.assertEqual([i["name"] for i in filtered["instructors"]], ["Linked Person"])
        self.assertEqual(filtered["counts"]["confirmed"], 1)


class SpringSectionOrderTests(unittest.TestCase):
    def test_latest_loaded_spring_ranks_all_campuses_before_pagination(self):
        from sqlalchemy import create_engine

        with create_engine("sqlite://").connect() as db:
            db.exec_driver_sql("CREATE TABLE terms (term_code text, term_desc text)")
            db.exec_driver_sql("CREATE TABLE sections (term_code text)")
            db.exec_driver_sql("""CREATE TABLE section_instructors (
                section_id text, term_code text, instructor_pidm integer, instructor_name text
            )""")
            db.exec_driver_sql("INSERT INTO terms VALUES (?, ?)", [
                ("202611", "Spring 2026 - College Station"),
                ("202711", "Spring 2027 - College Station"),
                ("202712", "Spring 2027 - Galveston"),
                ("202731", "Fall 2027 - College Station"),
                ("202811", "Spring 2028 - College Station"),
            ])
            db.exec_driver_sql("INSERT INTO sections VALUES (?)", [
                (term,) for term in ("202611", "202711", "202712", "202731")
            ])
            db.exec_driver_sql("INSERT INTO section_instructors VALUES (?, ?, ?, ?)", [
                ("old", "202611", 3, "Earlier Instructor"),
                ("a", "202711", 1, "Name A"),
                ("a", "202711", 1, "Name Variant"),
                ("b", "202712", 1, "Name Variant"),
                ("c", "202711", None, " Name B "),
                ("fall", "202731", 3, "Earlier Instructor"),
            ])
            counts = load_spring_section_counts(db)

        self.assertEqual(counts, {"pidm:1": 2, "name:Name B": 1})
        instructors = build_instructors([
            row(1, "pidm:1", "a", name="Name A", sections=2),
            row(2, "name:Name B", "b", name="Name B", sections=4),
            row(3, "pidm:3", "c", name="Earlier Instructor", sections=30),
        ], {})
        first = select_tab(instructors, limit=2, spring_sections=counts)
        self.assertEqual([i["key"] for i in first["instructors"]], ["pidm:1", "name:Name B"])
        second = select_tab(instructors, limit=2, offset=2, spring_sections=counts)
        self.assertEqual([i["key"] for i in second["instructors"]], ["pidm:3"])


def statuses(items, ops):
    """Apply ops to copies of rows, the way apply_ops would."""
    rows = {r.id: LinkRow(**r.__dict__) for r in items}
    next_id = max(rows, default=0) + 1
    for op in ops:
        if op.action == "insert":
            rows[next_id] = LinkRow(**{**op.row.__dict__, "id": next_id})
            next_id += 1
        elif op.action == "update":
            for name, value in op.fields.items():
                setattr(rows[op.row_id], name, value)
        else:
            del rows[op.row_id]
    return rows


class DecisionTests(unittest.TestCase):
    def setUp(self):
        self.items = [
            row(1, "pidm:7", "gwan"),
            row(2, "pidm:7", "sam"),
            row(3, "pidm:7", "old", status="rejected", reviewed=EARLIER),
        ]

    def test_link_confirms_and_rejects_the_rest_and_undo_restores(self):
        after = statuses(self.items, plan_links(self.items, ["gwan"], NOW, {}))
        self.assertEqual({r.professor_id: r.status for r in after.values()},
                         {"gwan": "confirmed", "sam": "rejected", "old": "rejected"})
        undone = statuses(list(after.values()), plan_undo(list(after.values())))
        # The explicit, earlier rejection stays.
        self.assertEqual({r.professor_id: r.status for r in undone.values()},
                         {"gwan": "proposed", "sam": "proposed", "old": "rejected"})

    def test_manual_link_needs_a_known_professor(self):
        with self.assertRaises(LinkError) as caught:
            plan_links(self.items, ["nobody"], NOW, {})
        self.assertEqual(caught.exception.status_code, 404)
        after = statuses(self.items, plan_links(
            self.items, ["new"], NOW, {"new": {"name": "G Choi", "legacyId": 5}},
        ))
        manual = next(r for r in after.values() if r.professor_id == "new")
        self.assertEqual((manual.kind, manual.status, manual.source, manual.rmp_legacy_id),
                         ("manual", "confirmed", "admin", 5))
        with self.assertRaises(LinkError):
            plan_links([], ["gwan"], NOW, {})

    def test_no_profile_and_undo(self):
        after = statuses(self.items, plan_no_profile(self.items, NOW))
        decision = [r for r in after.values() if r.professor_id is None]
        self.assertEqual([(r.status, r.kind) for r in decision], [("confirmed", "manual")])
        self.assertEqual({r.status for r in after.values() if r.professor_id}, {"rejected"})
        [found] = build_instructors(list(after.values()), {"gwan": {}, "sam": {}, "old": {}})
        self.assertTrue(found["noProfile"])
        self.assertEqual(found["tab"], "confirmed")
        undone = statuses(list(after.values()), plan_undo(list(after.values())))
        self.assertEqual({r.professor_id: r.status for r in undone.values()},
                         {"gwan": "proposed", "sam": "proposed", "old": "rejected"})

    def test_linking_replaces_a_no_profile_decision(self):
        after = statuses(self.items, plan_no_profile(self.items, EARLIER))
        relinked = statuses(list(after.values()), plan_links(list(after.values()), ["sam"], NOW, {}))
        self.assertEqual({r.professor_id: r.status for r in relinked.values()},
                         {"gwan": "rejected", "sam": "confirmed", "old": "rejected"})

    def test_reject_and_undo_rejections(self):
        with self.assertRaises(LinkError):
            plan_reject(self.items, ["unknown"], NOW)
        after = statuses(self.items, plan_reject(self.items, ["gwan", "sam"], NOW))
        [found] = build_instructors(list(after.values()), {})
        self.assertEqual(found["tab"], "rejected")
        undone = statuses(list(after.values()), plan_undo(list(after.values())))
        self.assertEqual({r.status for r in undone.values()}, {"proposed"})


class ApplyOpsTests(unittest.TestCase):
    def test_sql_casts_and_commit(self):
        db = MagicMock()
        ops = plan_links(
            [row(1, "pidm:7", "gwan")], ["new"], NOW, {"new": {"name": "G", "legacyId": 2}},
        )
        links_module.apply_ops(db, ops)
        statements = [str(call.args[0]) for call in db.execute.call_args_list]
        self.assertTrue(any("CAST(:courses AS text[])" in s for s in statements))
        self.assertTrue(any("ON CONFLICT (instructor_key, (COALESCE(professor_id, '')))" in s
                            for s in statements))
        insert = next(c for c in db.execute.call_args_list if "INSERT" in str(c.args[0]))
        self.assertIsInstance(insert.args[1]["evidence"], str)
        db.commit.assert_called_once()
        with self.assertRaises(ValueError):
            links_module.apply_ops(db, [links_module.Op("update", 1, {"professor_id": "x"})])


# ---------------------------------------------------------------------------
# Admin API
# ---------------------------------------------------------------------------


class MemoryStore:
    """fetch_rows / fetch_professors / apply_ops over in-memory rows."""

    def __init__(self, rows, professors):
        self.rows = {r.id: r for r in rows}
        self.professors = professors

    def fetch_rows(self, db, keys=None):
        rows = [LinkRow(**r.__dict__) for r in self.rows.values()]
        return rows if keys is None else [r for r in rows if r.instructor_key in set(keys)]

    def fetch_professors(self, db, ids):
        return {i: self.professors[i] for i in ids if i in self.professors}

    def apply_ops(self, db, ops, commit=True):
        self.rows = statuses(list(self.rows.values()), ops)


class AdminApiTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(admin_links.router)
        app.dependency_overrides[require_admin_key] = lambda: None
        self.db = MagicMock()
        app.dependency_overrides[request_session] = lambda: self.db
        self.client = TestClient(app)
        self.store = MemoryStore(
            [row(1, "pidm:7", "gwan", courses=["ECEN214", "ECEN 248"]), row(2, "pidm:7", "sam")],
            {"gwan": {"name": "Gwan Choi", "department": "Engineering", "numRatings": 40,
                      "avgRating": 4.1, "legacyId": 2229614},
             "sam": {"name": "Sam Choi", "department": "Math", "numRatings": 2,
                     "avgRating": 3.0, "legacyId": 9},
             "other": {"name": "Other", "department": None, "numRatings": 0,
                       "avgRating": None, "legacyId": 1}},
        )
        self.invalidate = AsyncMock()
        for target, value in [
            ("fetch_rows", self.store.fetch_rows),
            ("fetch_professors", self.store.fetch_professors),
            ("apply_ops", self.store.apply_ops),
            ("links_table_exists", lambda db: True),
        ]:
            patcher = patch.object(links_module, target, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        for target, value in [("ainvalidate_courses", self.invalidate),
                              ("links_table_exists", lambda db: True)]:
            patcher = patch.object(admin_links, target, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_list(self):
        response = self.client.get("/admin/instructor-links", params={"tab": "proposed"})
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["counts"], {"proposed": 1, "confirmed": 0, "rejected": 0,
                                          "conflict": 0})
        self.assertEqual(body["total"], 1)
        instructor = body["instructors"][0]
        self.assertEqual(instructor["key"], "pidm:7")
        self.assertEqual({c["professorId"] for c in instructor["candidates"]}, {"gwan", "sam"})
        self.assertEqual(self.client.get("/admin/instructor-links",
                                         params={"tab": "bogus"}).status_code, 422)

    def test_link_clears_course_pages(self):
        response = self.client.post("/admin/instructor-links/link",
                                    json={"key": "pidm:7", "professorId": "gwan"})
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["instructor"]["tab"], "confirmed")
        self.assertEqual(body["instructor"]["linkedProfessorId"], "gwan")
        self.assertEqual(body["invalidatedCourses"], ["ECEN214", "ECEN248"])
        self.invalidate.assert_awaited_once_with(["ECEN214", "ECEN248"])
        undo = self.client.post("/admin/instructor-links/undo", json={"key": "pidm:7"}).json()
        self.assertEqual(undo["instructor"]["tab"], "proposed")
        no_profile = self.client.post("/admin/instructor-links/no-profile",
                                      json={"key": "pidm:7"}).json()
        self.assertTrue(no_profile["instructor"]["noProfile"])
        self.assertEqual(self.invalidate.await_count, 3)

    def test_manual_link_and_reject(self):
        manual = self.client.post("/admin/instructor-links/link",
                                  json={"key": "pidm:7", "professorId": "other"}).json()
        kinds = {c["professorId"]: (c["kind"], c["status"]) for c in manual["instructor"]["candidates"]}
        self.assertEqual(kinds["other"], ("manual", "confirmed"))
        rejected = self.client.post("/admin/instructor-links/reject",
                                    json={"key": "pidm:7", "professorIds": ["other"]})
        self.assertEqual(rejected.status_code, 200)
        self.assertEqual(rejected.json()["instructor"]["tab"], "rejected")

    def test_link_selection_is_atomic_and_preserves_existing_links(self):
        path = "/admin/instructor-links/link"
        bad = self.client.post(path, json={"key": "pidm:7", "professorIds": ["gwan", "missing"]})
        self.assertEqual(bad.status_code, 404)
        self.assertTrue(all(r.status == "proposed" for r in self.store.rows.values()))
        self.invalidate.assert_not_awaited()

        self.store.rows[3] = row(3, "pidm:7", "unused")
        for selected, expected in [
            (["gwan", "sam", "gwan"], {"gwan", "sam"}),
            (["other"], {"gwan", "sam", "other"}),
        ]:
            response = self.client.post(path, json={"key": "pidm:7", "professorIds": selected})
            self.assertEqual(response.status_code, 200)
            instructor = response.json()["instructor"]
            self.assertEqual(instructor["tab"], "confirmed")
            self.assertEqual(set(instructor["linkedProfessorIds"]), expected)
            self.assertEqual(next(c["status"] for c in instructor["candidates"]
                                  if c["professorId"] == "unused"), "rejected")
        self.assertEqual(self.invalidate.await_count, 2)
        undo = self.client.post("/admin/instructor-links/undo", json={"key": "pidm:7"}).json()
        self.assertEqual(undo["instructor"]["linkedProfessorIds"], [])
        self.assertTrue(all(c["status"] == "proposed" for c in undo["instructor"]["candidates"]))

    def test_reject_all_only_rejects_pending_candidates_for_the_given_instructor(self):
        path = "/admin/instructor-links/reject-all"
        self.store.rows[3] = row(3, "pidm:8", "other")
        response = self.client.post(path, json={"key": "pidm:7"})
        self.assertEqual(response.status_code, 200)
        instructor = response.json()["instructor"]
        self.assertEqual(instructor["tab"], "rejected")
        self.assertFalse(instructor["noProfile"])
        self.assertTrue(all(c["status"] == "rejected" for c in instructor["candidates"]))
        self.assertEqual(self.store.rows[3].status, "proposed")
        self.invalidate.assert_awaited_once()

        self.store.rows[1].status = "confirmed"
        self.store.rows[2].status = "proposed"
        response = self.client.post(path, json={"key": "pidm:7"}).json()
        self.assertEqual(response["instructor"]["tab"], "confirmed")
        self.assertEqual(response["instructor"]["linkedProfessorIds"], ["gwan"])
        self.assertEqual(self.store.rows[2].status, "rejected")

    def test_errors(self):
        unknown_key = self.client.post("/admin/instructor-links/no-profile", json={"key": "pidm:1"})
        self.assertEqual(unknown_key.status_code, 404)
        unknown_professor = self.client.post("/admin/instructor-links/link",
                                             json={"key": "pidm:7", "professorId": "nobody"})
        self.assertEqual(unknown_professor.status_code, 404)
        self.assertIn("detail", unknown_professor.json())
        self.invalidate.assert_not_awaited()
        with patch.object(admin_links, "links_table_exists", lambda db: False):
            missing = self.client.get("/admin/instructor-links")
        self.assertEqual(missing.status_code, 409)
        self.assertIn("005_instructor_links.sql", missing.json()["detail"])


if __name__ == "__main__":
    unittest.main()
