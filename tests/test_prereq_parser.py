"""Prerequisite text -> AND/OR tree: rules, validation, lint, LLM orchestration.

Pure tests (no DB). Run with: pytest --noconftest tests/test_prereq_parser.py
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipelines.courses import parse_prereqs as runner  # noqa: E402
from pipelines.courses.parse_prereqs import CourseRow, format_tree  # noqa: E402
from pipelines.courses.prereq_parser import (  # noqa: E402
    RuleParseFailed,
    codes_in_text,
    finalize,
    legacy_tree,
    lint_tree,
    normalize_code,
    normalize_text,
    parse_with_rules,
    simplify,
    text_hash,
    validate_tree,
)
from pipelines.professors.hierarchical_summarization.ai_clients import (  # noqa: E402
    TamuAIError,
)


def rules(prereq, coreq=None, code="TEST 100"):
    p, c = parse_with_rules(prereq, coreq, code)
    return format_tree(p), format_tree(c)


def C(code, grade=None, concurrent=False):
    return {"type": "course", "code": code, "min_grade": grade, "concurrent": concurrent}


class NormalizationTests(unittest.TestCase):
    def test_fixes_double_encoded_nbsp_and_spacing(self):
        raw = "Grade of C or better in CSCE\u00c2\u00a0221 , or concurrent enrollment"
        self.assertEqual(
            normalize_text(raw), "Grade of C or better in CSCE 221, or concurrent enrollment"
        )
        self.assertEqual(normalize_text("MATH\u00a0151"), "MATH 151")
        self.assertEqual(normalize_text("MATH\ufffd151"), "MATH 151")

    def test_semicolon_or_continues_clause(self):
        self.assertEqual(
            normalize_text("CHEM 228 or CHEM 258 ; or approval of instructor"),
            "CHEM 228 or CHEM 258, or approval of instructor",
        )

    def test_collapses_mirrored_crosslisting_keeping_list_connector(self):
        self.assertEqual(
            normalize_text("CSCE 314, CSCE 350/ECEN 350, or ECEN 350/CSCE 350"),
            "CSCE 314, or CSCE 350/ECEN 350",
        )
        self.assertEqual(
            normalize_text("PHYS 206 and ENGR 216/PHYS 216 or PHYS 216/ENGR 216"),
            "PHYS 206 and ENGR 216/PHYS 216",
        )

    def test_expands_subjectless_course_numbers(self):
        self.assertEqual(
            normalize_text("GEOL 101 and 102, or GEOL 104"), "GEOL 101 and GEOL 102, or GEOL 104"
        )
        self.assertEqual(
            normalize_text("STAT 301, 302, 303, or ANSC 309"),
            "STAT 301, STAT 302, STAT 303, or ANSC 309",
        )
        for untouched in ["SPAN 202, 300-level courses", "CSCE 221 or 300 or above",
                          "ENGL 104 and 6 credits", "SPAN 202 or 300 level Spanish"]:
            with self.subTest(text=untouched):
                self.assertNotIn("SPAN 300", normalize_text(untouched))
                self.assertNotIn("CSCE 300", normalize_text(untouched))

    def test_codes(self):
        self.assertEqual(normalize_code("csce121"), "CSCE 121")
        self.assertEqual(normalize_code("MATH 151H"), "MATH 151H")
        self.assertIsNone(normalize_code("300-level"))
        self.assertEqual(
            codes_in_text("CYBR 201/CSCE 201, CSCE 201/CYBR 201, or CSCE 221"),
            ["CYBR 201", "CSCE 201", "CSCE 221"],
        )

    def test_hash_ignores_mojibake_but_not_content(self):
        self.assertEqual(text_hash("CSCE\u00a0221", None), text_hash("CSCE 221", None))
        self.assertNotEqual(text_hash("CSCE 221", None), text_hash("CSCE 222", None))


class RuleParserTests(unittest.TestCase):
    def test_and_list_is_and(self):
        # The legacy parser stored this as a single OR group.
        self.assertEqual(
            rules(
                "Grade of C or better in CSCE 221 and CSCE 222 ; junior or senior "
                "classification or approval of instructor",
                code="CSCE 411",
            )[0],
            "(CSCE 221[C] AND CSCE 222[C] AND (junior/senior OR approval:instructor))",
        )

    def test_or_list_with_commas(self):
        self.assertEqual(
            rules("Grade of C or better in MATH 148 , MATH 152 , or MATH 172")[0],
            "(MATH 148[C] OR MATH 152[C] OR MATH 172[C])",
        )

    def test_comma_and_list(self):
        self.assertEqual(
            rules("Grade of C or better in CVEN 302, CVEN 305 and MATH 308")[0],
            "(CVEN 302[C] AND CVEN 305[C] AND MATH 308[C])",
        )

    def test_grade_suffix_and_approval(self):
        self.assertEqual(
            rules("SPAN 407 with a grade of B or better or approval of instructor")[0],
            "(SPAN 407[B] OR approval:instructor)",
        )

    def test_or_concurrent_marks_course_and_drops_scraper_coreq(self):
        self.assertEqual(
            rules("FINC 381 or concurrent enrollment", "Concurrent enrollment in FINC 381"),
            ("FINC 381~", "-"),
        )
        self.assertEqual(rules("CHEM 107 or registration therein")[0], "CHEM 107~")

    def test_required_concurrent_clause(self):
        self.assertEqual(
            rules("MATH 151 or MATH 171; concurrent enrollment in PHYS 206",
                  "Concurrent enrollment in PHYS 206"),
            ("((MATH 151 OR MATH 171) AND PHYS 206~)", "-"),
        )

    def test_crosslisted_pair_is_or(self):
        self.assertEqual(
            rules("Grade of C or better in MEEN 360 or MSEN 222/MEEN 222")[0],
            "(MEEN 360[C] OR MSEN 222[C] OR MEEN 222[C])",
        )

    def test_equivalent_and_placement(self):
        self.assertEqual(
            rules("MEEN 344 or equivalent")[0], "(MEEN 344 OR <equivalent>)"
        )
        self.assertEqual(
            rules("SPAN 202, SPAN 203, SPAN 218, or SPAN 222; or placement by exam")[0],
            "(SPAN 202 OR SPAN 203 OR SPAN 218 OR SPAN 222 OR <placement>)",
        )

    def test_course_free_clauses_become_conditions(self):
        self.assertEqual(
            rules("Mechanical engineering major; grade of C or better in ENGR 102")[0],
            "(<major> AND ENGR 102[C])",
        )
        self.assertEqual(rules("Admission to Professional Program")[0], "<admission>")

    def test_course_free_grammar(self):
        cases = {
            "Junior or senior classification, or approval of instructor":
                "(junior/senior OR approval:instructor)",
            "Freshman or sophomore classification and approval of instructor":
                "(freshman/sophomore AND approval:instructor)",
            "Approval of instructor and department head":
                "(approval:instructor AND approval:department)",
            "junior or senior classification or instructor approval":
                "(junior/senior OR approval:instructor)",
            "Junior or senior classification in environmental design":
                "(junior/senior AND <major>)",
            "Junior classification and admission to the BSN nursing program":
                "(junior AND <admission>)",
            "6 credits of 300-level Spanish courses or approval of instructor":
                "(<credits> OR approval:instructor)",
            "Dance science majors and dance minors, or approval of instructor":
                "(<major> OR approval:instructor)",
        }
        for text, want in cases.items():
            with self.subTest(text=text):
                self.assertEqual(rules(text)[0], want)

    def test_suggestion_clauses_are_skipped(self):
        self.assertEqual(
            rules("CHEM 102 or CHEM 120; concurrent enrollment in CHEM 237 is suggested",
                  "Concurrent enrollment in CHEM 237"),
            ("(CHEM 102 OR CHEM 120)", "-"),
        )
        self.assertEqual(
            rules("PBSI 107; PBSI 301 and PBSI 302 recommended")[0], "PBSI 107"
        )

    def test_finalize_fixes_half_crosslisting_and_prior_to(self):
        p, _, issues = finalize(
            {"type": "and", "children": [C("MKTG 321"), C("IBUS 402", None, True), C("MKTG 402", None, True)]},
            None,
            prereq_text="MKTG 321; concurrent registration in IBUS 402/MKTG 402",
            coreq_text="", self_code="MKTG 403",
        )
        self.assertEqual(format_tree(p), "(MKTG 321 AND (IBUS 402~ OR MKTG 402~))")
        self.assertEqual(issues, [])
        p, _, issues = finalize(
            C("AGSM 310"), None,
            prereq_text="AGSM 310; must be taken prior to AGSM 440", coreq_text="", self_code="AGSM 439",
        )
        self.assertEqual(issues, [])

    def test_self_reference_dropped(self):
        self.assertEqual(rules("CSCE 314 or CSCE 221", code="CSCE 314")[0], "CSCE 221")

    def test_hard_phrasings_go_to_llm(self):
        for text in [
            "BIOL 213 and PSYC 235, or approval of instructor",  # alternative after AND
            "CSCE 312 and CSCE 314, or CSCE 350",  # mixed and/or
            "CSCE 221 and preferably CSCE 222 recommended or approval",  # suggestion mid-clause
            "MATH 308, MATH 311",  # comma-only list
            "Senior undergraduate majors in biology",  # classification inside a condition
        ]:
            with self.subTest(text=text), self.assertRaises(RuleParseFailed):
                parse_with_rules(text, None, "TEST 100")


class TreeHelperTests(unittest.TestCase):
    def test_simplify_flattens_and_dedupes_order_insensitively(self):
        tree = {
            "type": "and",
            "children": [
                {"type": "and", "children": [C("PHYS 206")]},
                {"type": "or", "children": [C("ENGR 216"), C("PHYS 216")]},
                {"type": "or", "children": [C("PHYS 216"), C("ENGR 216")]},
            ],
        }
        self.assertEqual(format_tree(simplify(tree)), "(PHYS 206 AND (ENGR 216 OR PHYS 216))")

    def test_simplify_merges_course_or_concurrent_copy(self):
        tree = {"type": "or", "children": [C("MATH 308", "C"), C("MATH 308", None, True)]}
        self.assertEqual(format_tree(simplify(tree)), "MATH 308[C]~")

    def test_validate_rejects_invented_codes(self):
        bad = {"type": "or", "children": [C("SPAN 300"), {"type": "approval", "by": "instructor"}]}
        with self.assertRaises(ValueError):
            validate_tree(bad, source_text="three credits of 300-level Spanish", self_code="SPAN 305")

    def test_validate_coerces_harmless_slips(self):
        raw = {"type": "and", "children": [
            {"type": "course", "code": "ECON 323", "min_grade": "C-", "concurrent": None},
            {"type": "condition", "kind": "approval", "text": "pre-approval of the director"},
            {"type": "condition", "kind": "residency", "text": "Texas resident"},
            {"type": "approval", "by": "Department head"},
        ]}
        tree, _ = validate_tree(raw, source_text="ECON 323", self_code="ECON 484")
        self.assertEqual(
            format_tree(tree), "(ECON 323[C] AND approval:other AND <other> AND approval:department)"
        )

    def test_validate_rejects_bad_shape(self):
        with self.assertRaises(ValueError):
            validate_tree({"type": "xor", "children": []}, source_text="", self_code=None)
        with self.assertRaises(ValueError):
            validate_tree({"type": "and", "children": []}, source_text="", self_code=None)

    def test_finalize_moves_optional_coreq_onto_prereq_node(self):
        p, c, issues = finalize(
            {"type": "and", "children": [C("BICH 410"), C("BIOL 413")]},
            {"type": "and", "children": [C("BICH 410", None, True), C("BIOL 413", None, True)]},
            prereq_text="BICH 410 and BIOL 413, or concurrent enrollment",
            coreq_text="Concurrent enrollment in BICH 410, BIOL 413",
            self_code="BIOL 423",
        )
        self.assertEqual(format_tree(p), "(BICH 410~ AND BIOL 413~)")
        self.assertIsNone(c)
        self.assertEqual(issues, [])

    def test_finalize_drops_suggested_coreq_and_reports_missing(self):
        p, c, issues = finalize(
            C("CHEM 102"),
            C("CHEM 237", None, True),
            prereq_text="CHEM 102 or CHEM 120; concurrent enrollment in CHEM 237 is suggested",
            coreq_text="Concurrent enrollment in CHEM 237",
            self_code="CHEM 227",
        )
        self.assertIsNone(c)
        self.assertEqual(issues, ["missing_codes:CHEM 120"])

    def test_lint_flags_structure_mismatches(self):
        text = "Grade of C or better in MATH 251 or MATH 253"
        self.assertEqual(
            lint_tree({"type": "and", "children": [C("MATH 251"), C("MATH 253")]}, text),
            ["and_in_or_clause"],
        )
        text = "STAT 404; STAT 408 and senior classification"
        tree = {
            "type": "and",
            "children": [
                C("STAT 404"),
                {"type": "or", "children": [C("STAT 408"), {"type": "classification", "levels": ["senior"]}]},
            ],
        }
        self.assertEqual(lint_tree(tree, text), ["or_in_and_clause"])
        self.assertEqual(
            lint_tree({"type": "and", "children": [C("ENGR 216"), C("PHYS 216")]}, "ENGR 216/PHYS 216"),
            ["and_of_crosslisted"],
        )

    def test_lint_accepts_correct_trees(self):
        cases = [
            ("BIOL 213 and PSYC 235, or approval of instructor",
             {"type": "or", "children": [
                 {"type": "and", "children": [C("BIOL 213"), C("PSYC 235")]},
                 {"type": "approval", "by": "instructor"}]}),
            ("Grade of C or better in ECEN 248 and CSCE 350/ECEN 350",
             {"type": "and", "children": [
                 C("ECEN 248"), {"type": "or", "children": [C("CSCE 350"), C("ECEN 350")]}]}),
            ("CSCE 312 and CSCE 314, or CSCE 350/ECEN 350",
             {"type": "or", "children": [
                 {"type": "and", "children": [C("CSCE 312"), C("CSCE 314")]},
                 C("CSCE 350"), C("ECEN 350")]}),
        ]
        for text, tree in cases:
            with self.subTest(text=text):
                self.assertEqual(lint_tree(tree, text), [])

    def test_legacy_tree_drops_codes_not_in_text(self):
        tree = legacy_tree(
            json.dumps([["MATH 221", "MATH 148", "MATH 152"]]),
            "Grade of C or better in MATH 148 or MATH 152",
            "MATH 251",
        )
        self.assertEqual(format_tree(tree), "(MATH 148 OR MATH 152)")


class FakeClient:
    """Returns queued responses for _post and records the messages sent."""

    model = "fake-model"

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def _post(self, messages, max_tokens=None, response_format=None):
        self.calls.append({"messages": messages, "response_format": response_format})
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def result(code, prereq=None, coreq=None):
    return {"code": code, "prereq": prereq, "coreq": coreq}


class LlmOrchestrationTests(unittest.TestCase):
    ROW = CourseRow("STAT482", "STAT 482", "STAT 404; STAT 408 and senior classification", None)
    SENIOR = {"type": "classification", "levels": ["senior"]}

    def test_good_result_is_high_confidence_and_uses_json_object(self):
        tree = {"type": "and", "children": [C("STAT 404"), C("STAT 408"), self.SENIOR]}
        client = FakeClient([{"results": [result("STAT 482", tree)]}])
        [r] = runner.parse_batch_llm(client, [self.ROW])
        self.assertEqual((r.source, r.confidence), ("llm", "high"))
        self.assertEqual(client.calls[0]["response_format"], {"type": "json_object"})
        self.assertEqual(len(client.calls), 1)

    def test_misread_structure_is_retried_with_hint(self):
        wrong = {"type": "and", "children": [
            C("STAT 404"), {"type": "or", "children": [C("STAT 408"), self.SENIOR]}]}
        right = {"type": "and", "children": [C("STAT 404"), C("STAT 408"), self.SENIOR]}
        client = FakeClient([
            {"results": [result("STAT 482", wrong)]},
            {"results": [result("STAT 482", right)]},
        ])
        [r] = runner.parse_batch_llm(client, [self.ROW])
        self.assertEqual(format_tree(r.prereq_tree), "(STAT 404 AND STAT 408 AND senior)")
        self.assertEqual(r.confidence, "high")
        retry_input = json.loads(client.calls[1]["messages"][1]["content"])
        self.assertIn("connector is 'and'", retry_input[0]["note"])

    def test_retry_that_is_no_better_keeps_first_as_low(self):
        wrong = {"type": "and", "children": [
            C("STAT 404"), {"type": "or", "children": [C("STAT 408"), self.SENIOR]}]}
        client = FakeClient([{"results": [result("STAT 482", wrong)]}] * 2)
        [r] = runner.parse_batch_llm(client, [self.ROW])
        self.assertEqual(r.confidence, "low")
        self.assertIn("or_in_and_clause", r.issues)

    def test_hallucinated_codes_fall_back_to_condition(self):
        row = CourseRow("SPAN305", "SPAN 305", "Three credits of 300 or 400-level Spanish courses", None)
        bad = {"type": "or", "children": [C("SPAN 300"), C("SPAN 400")]}
        client = FakeClient([{"results": [result("SPAN 305", bad)]}] * 2)
        [r] = runner.parse_batch_llm(client, [row])
        self.assertEqual((r.source, r.confidence), ("legacy", "low"))
        self.assertEqual(r.prereq_tree["type"], "condition")

    def test_request_error_falls_back_without_retry(self):
        client = FakeClient([TamuAIError("boom")])
        [r] = runner.parse_batch_llm(client, [self.ROW])
        self.assertEqual((r.source, r.issues), ("legacy", ["llm_error"]))
        self.assertEqual(len(client.calls), 1)

    def test_parse_rows_prefers_rules_and_skips_llm_when_unavailable(self):
        rows = [
            CourseRow("A", "CSCE 231", "Grade of C or better in CSCE 221", None),
            CourseRow("B", "BIOL 428", "BIOL 213 and PSYC 235, or approval of instructor", None),
            CourseRow("C", "ENGL 104", None, None),
        ]
        out = {r.row.code: r for r in runner.parse_rows(rows, use_llm=False, workers=1)}
        self.assertEqual((out["CSCE 231"].source, format_tree(out["CSCE 231"].prereq_tree)),
                         ("rules", "CSCE 221[C]"))
        self.assertEqual(out["BIOL 428"].issues, ["llm_unavailable"])
        self.assertIsNone(out["ENGL 104"].prereq_tree)

    def test_apply_writes_only_unchanged_rows(self):
        import tempfile
        from types import SimpleNamespace

        fresh = runner.ParseResult(
            CourseRow("CSCE231", "CSCE 231", "Grade of C or better in CSCE 221", None),
            C("CSCE 221", "C"), None, "rules", "high",
        )
        stale = runner.ParseResult(
            CourseRow("CSCE312", "CSCE 312", "CSCE 221", None), C("CSCE 221"), None, "rules", "high",
        )
        entries = [
            {"code": r.row.code, "prereq_tree": r.prereq_tree, "coreq_tree": None, **r.meta()}
            for r in (fresh, stale)
        ]

        class Session:
            def __init__(self):
                self.writes = []

            def execute(self, _stmt, params):
                if isinstance(params, list):
                    self.writes.extend(params)
                    return None
                rows = [
                    SimpleNamespace(id="CSCE231", code="CSCE 231",
                                    prerequisites="Grade of C or better in CSCE 221", corequisites=None),
                    # Catalog text changed since the dry run.
                    SimpleNamespace(id="CSCE312", code="CSCE 312",
                                    prerequisites="CSCE 221 and CSCE 222", corequisites=None),
                ]
                return SimpleNamespace(fetchall=lambda: rows)

            def commit(self):
                pass

        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "results.json"
            path.write_text(json.dumps(entries), encoding="utf-8")
            session = Session()
            stats = runner.apply_results_file(session, str(path))
        self.assertEqual(stats, {"written": 1, "stale": 1, "not_in_db": 0})
        self.assertEqual([w["id"] for w in session.writes], ["CSCE231"])
        self.assertEqual(json.loads(session.writes[0]["prereq_tree"])["code"], "CSCE 221")

    def test_meta_records_hash_and_version(self):
        r = runner.ParseResult(self.ROW, None, None, "rules", "high")
        meta = r.meta()
        self.assertEqual(meta["hash"], text_hash(self.ROW.prereq, self.ROW.coreq))
        self.assertEqual(meta["source"], "rules")


if __name__ == "__main__":
    unittest.main()
