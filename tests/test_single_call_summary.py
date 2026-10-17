"""Single TAMU request per professor: grouping, ID mapping, assembly, stats."""

import json
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np

from test_ai_summarization_clients import FakeResponse, FakeSession
from pipelines.professors import upsert_reviews_and_summaries as runner
from pipelines.professors.hierarchical_summarization import ai_clients
from pipelines.professors.hierarchical_summarization.ai_clients import (
    TamuAIChatClient,
    TamuAIQuotaExceeded,
    TypeSafeReviewJudge,
)
from pipelines.professors.hierarchical_summarization.course_normalizer import (
    CourseNormalizer,
)
from pipelines.professors.hierarchical_summarization.pipeline import (
    HierarchicalSummarizationPipeline,
    headline_sentiment,
)
from pipelines.professors.hierarchical_summarization.preprocess import (
    ReviewPreprocessor,
)
from pipelines.professors.hierarchical_summarization.professor_request import (
    OTHER_GROUP,
    RequestReview,
    build_request_parts,
    map_part_response,
    self_contained,
)
from pipelines.professors.hierarchical_summarization.response_schema import (
    ProfessorSummaryOutput,
)
from pipelines.professors.hierarchical_summarization.review_stats import (
    compute_review_stats,
)


def empty_course(code, **fields):
    entry = {
        "course": code,
        "exams": [],
        "grading": [],
        "workload": [],
        "attendance_policies": [],
        "teaching": [],
        "tips": [],
    }
    entry.update(fields)
    return entry


def output(courses, strengths=(), watch_outs=(), tldr_ids=("r1",)):
    return {
        "courses": list(courses),
        "strengths": list(strengths),
        "watch_outs": list(watch_outs),
        "tldr": {"text": "Hard exams, but well supported.", "ids": list(tldr_ids)},
    }


def tamu_response(body):
    return FakeResponse(200, {"choices": [{"message": {"content": json.dumps(body)}}]})


def raw_review(review_id, course, date, rating, text=None, **extra):
    row = {
        "id": review_id,
        "professor_id": "p1",
        "course_code": course,
        "review_text": text
        or f"Review {review_id} says the exams were hard but the study guide helped.",
        "clarity_rating": rating,
        "helpful_rating": rating,
        "difficulty_rating": 4,
        "rating_tags": ["Tough grader"],
        "review_date": date,
    }
    row.update(extra)
    return row


RAW_REVIEWS = [
    raw_review("a3", "CPSC121", datetime(2019, 3, 1), 3),
    raw_review("a1", "CSCE 121", datetime(2025, 9, 1), 5),
    raw_review("b1", "ASTRONOMY111", datetime(2023, 2, 1), 4),
    raw_review("a2", "CSCE121", datetime(2024, 1, 1), 2),
]

# With newest-first grouping: CSCE121 -> r1=a1, r2=a2, r3=a3; OTHER -> r4=b1.
CANNED = output(
    [
        empty_course(
            "CSCE121",
            exams=[
                {
                    "text": "Exams are hard, but the study guide mirrors them.",
                    "ids": ["r1", "r3"],
                    "sentiment": "mixed",
                }
            ],
            attendance_policies=[
                {
                    "text": "Missed quizzes can be made up with a doctor's note.",
                    "ids": ["r2"],
                    "sentiment": "neutral",
                }
            ],
            teaching=[
                {
                    "text": "Slides are posted only after class.",
                    "ids": ["r9"],
                    "sentiment": "negative",
                }
            ],
            tips=[{"text": "Work every study guide problem.", "ids": ["r1"]}],
        ),
        empty_course(
            OTHER_GROUP,
            teaching=[
                {
                    "text": "Other students found the lectures engaging.",
                    "ids": ["r4"],
                    "sentiment": "positive",
                }
            ],
        ),
        empty_course(
            "MATH999",
            exams=[{"text": "Invented course.", "ids": ["r1"], "sentiment": "neutral"}],
        ),
    ],
    strengths=[
        {
            "text": "Patient in office hours.",
            "ids": ["r2"],
            "topic": "personality",
        }
    ],
    watch_outs=[
        {"text": "Grading feels harsh on projects.", "ids": ["r1"], "topic": "grading"}
    ],
    tldr_ids=["r1", "r2"],
)


class DistinctEmbeddings:
    """Orthogonal vectors so the 0.95 deduplication keeps every review."""

    def encode(self, texts, **kwargs):
        return np.eye(len(texts), max(len(texts), 2))


def make_client(responses, **kwargs):
    return TamuAIChatClient(
        api_key="test",
        base_url="https://example.test",
        model="test",
        session=FakeSession(responses),
        **kwargs,
    )


def make_pipeline(client, judge=None):
    pipeline = object.__new__(HierarchicalSummarizationPipeline)
    pipeline.summarizer = SimpleNamespace(tamu_client=client, fallback_clusters=0)
    pipeline.preprocessor = ReviewPreprocessor(embedding_model=DistinctEmbeddings())
    pipeline.course_normalizer = CourseNormalizer()
    pipeline.typesafe_judge = judge
    return pipeline


def request_review(review_id, course, year, text="Useful synthetic review text."):
    return RequestReview(
        local_id="",
        review_id=review_id,
        text=text,
        courses=[course],
        date=datetime(year, 1, 1),
        quality=4.0,
    )


class RequestBuildingTests(unittest.TestCase):
    def test_groups_by_course_newest_first_with_small_courses_in_other(self):
        reviews = [
            request_review("old", "CSCE121", 2015),
            request_review("new", "CSCE121", 2025),
            request_review("mid", "CSCE121", 2020),
            request_review("solo", "ASTR111", 2022),
            request_review("pair1", "MATH151", 2021),
            request_review("pair2", "MATH151", 2023),
        ]
        parts = build_request_parts("p", reviews, max_input_chars=45_000)

        self.assertEqual(len(parts), 1)
        groups = parts[0].groups
        self.assertEqual([g.code for g in groups], ["CSCE121", OTHER_GROUP])
        self.assertEqual(
            [r.review_id for r in groups[0].reviews], ["new", "mid", "old"]
        )
        # Courses under 3 reviews never get a group (or request) of their own.
        self.assertEqual(
            [r.review_id for r in groups[1].reviews], ["pair2", "solo", "pair1"]
        )
        self.assertEqual(
            [r.local_id for r in parts[0].reviews], ["r1", "r2", "r3", "r4", "r5", "r6"]
        )
        text = parts[0].render()
        self.assertIn("## CSCE121 (3 reviews)", text)
        self.assertIn("r4 MATH151 2023 q4: ", text)
        self.assertNotIn("pair2", text)  # real RMP IDs never reach the prompt

    def test_splits_by_whole_course_groups_when_over_budget(self):
        reviews = [
            request_review(f"{course}-{i}", course, 2020 + i, "x" * 400)
            for course in ("AAAA101", "BBBB202", "CCCC303")
            for i in range(4)
        ]
        prompt_overhead = len(ai_clients.PROFESSOR_SYSTEM_PROMPT) + 200
        parts = build_request_parts(
            "p", reviews, max_input_chars=prompt_overhead + 3_800
        )

        self.assertGreater(len(parts), 1)
        for part in parts:
            for group in part.groups:
                # A course is never split across requests.
                self.assertEqual(len(group.reviews), 4)
        self.assertEqual(
            sorted(g.code for p in parts for g in p.groups),
            ["AAAA101", "BBBB202", "CCCC303"],
        )
        local_ids = [r.local_id for p in parts for r in p.reviews]
        self.assertEqual(len(local_ids), len(set(local_ids)))

    def test_oversized_single_course_keeps_newest_reviews(self):
        reviews = [
            request_review(f"r-{year}", "CSCE121", year, "y" * 500)
            for year in range(2010, 2026)
        ]
        prompt_overhead = len(ai_clients.PROFESSOR_SYSTEM_PROMPT) + 200
        parts = build_request_parts(
            "p", reviews, max_input_chars=prompt_overhead + 2_000, clip_chars=600
        )

        self.assertEqual(len(parts), 1)
        kept = [r.review_id for r in parts[0].reviews]
        self.assertEqual(kept[0], "r-2025")
        self.assertLess(len(kept), len(reviews))
        self.assertEqual(parts[0].dropped_reviews, len(reviews) - len(kept))


class IdMappingTests(unittest.TestCase):
    def setUp(self):
        reviews = [
            request_review("rmp-new", "CSCE121", 2025),
            request_review("rmp-mid", "CSCE121", 2024),
            request_review("rmp-old", "CSCE121", 2019),
            request_review("rmp-astr", "ASTR111", 2023),
        ]
        self.part = build_request_parts("p", reviews, max_input_chars=45_000)[0]

    def test_maps_local_ids_and_rejects_any_unrequested_citation(self):
        body = output(
            [
                empty_course(
                    "CSCE121",
                    exams=[
                        {"text": "Valid.", "ids": ["r1", "r3"], "sentiment": "mixed"},
                        # One invented ID discards the whole claim.
                        {
                            "text": "Mixed.",
                            "ids": ["r2", "r77"],
                            "sentiment": "negative",
                        },
                    ],
                ),
                empty_course(
                    "MATH999",
                    exams=[{"text": "No.", "ids": ["r1"], "sentiment": "neutral"}],
                ),
            ]
        )
        result = map_part_response(
            self.part, ProfessorSummaryOutput.model_validate(body)
        )

        claims = result.courses["CSCE121"]["exams"]
        self.assertEqual([c.text for c in claims], ["Valid."])
        self.assertEqual(claims[0].evidence_review_ids, ["rmp-new", "rmp-old"])
        self.assertEqual(claims[0].sentiment, "mixed")
        self.assertNotIn("MATH999", result.courses)
        self.assertEqual(result.rejected_citations, 2)
        self.assertEqual(result.tldr.evidence_review_ids, ["rmp-new"])

    def test_other_group_claims_are_routed_to_the_cited_reviews_course(self):
        body = output(
            [
                empty_course(
                    OTHER_GROUP,
                    teaching=[
                        {"text": "Engaging.", "ids": ["r4"], "sentiment": "positive"}
                    ],
                )
            ]
        )
        result = map_part_response(
            self.part, ProfessorSummaryOutput.model_validate(body)
        )
        self.assertEqual(result.courses["ASTR111"]["teaching"][0].text, "Engaging.")
        self.assertNotIn(OTHER_GROUP, result.courses)


class ClientTests(unittest.TestCase):
    def test_one_strict_schema_request_per_professor(self):
        session_response = tamu_response(output([empty_course("CSCE121")]))
        client = make_client(session_response)

        result = client.generate_professor_summary("## CSCE121 (1 reviews)")

        self.assertEqual(result.tldr.text, "Hard exams, but well supported.")
        self.assertEqual(client.requests_made, 1)
        request = client._injected_session.calls[0][1]["json"]
        self.assertEqual(request["response_format"]["type"], "json_schema")
        self.assertEqual(
            request["response_format"]["json_schema"]["name"], "professor_summary"
        )
        self.assertTrue(request["response_format"]["json_schema"]["strict"])
        self.assertEqual(request["messages"][1]["content"], "## CSCE121 (1 reviews)")

    def test_schema_violation_gets_one_strict_retry(self):
        bad = output([empty_course("CSCE121", extra=True)])
        good = output([empty_course("CSCE121")])
        client = make_client([tamu_response(bad), tamu_response(good)])

        client.generate_professor_summary("text")

        self.assertEqual(client.requests_made, 2)
        self.assertEqual(client.json_retries, 1)
        retry = client._injected_session.calls[1][1]["json"]["messages"][1]["content"]
        self.assertIn("Return one complete JSON object only", retry)

    def test_truncation_is_not_retried_with_the_same_prompt(self):
        client = make_client(
            FakeResponse(
                200,
                {"choices": [{"message": {"content": "{"}, "finish_reason": "length"}]},
            )
        )
        with self.assertRaises(ai_clients.TamuAITruncated):
            client.generate_professor_summary("text")
        self.assertEqual(client.requests_made, 1)

    def test_quota_is_not_retried(self):
        client = make_client(FakeResponse(429, {}))
        with self.assertRaises(TamuAIQuotaExceeded):
            client.generate_professor_summary("text")
        self.assertEqual(client.requests_made, 1)


class SingleCallPipelineTests(unittest.TestCase):
    def test_professor_costs_one_request_and_maps_fields(self):
        client = make_client(tamu_response(CANNED))
        pipeline = make_pipeline(client)

        summary = pipeline.process_professor_reviews(list(RAW_REVIEWS), "p1")

        self.assertEqual(client.requests_made, 1)
        prompt = client._injected_session.calls[0][1]["json"]["messages"][1]["content"]
        self.assertIn("## CSCE121 (3 reviews)", prompt)
        self.assertIn("r4 ASTR111 2023", prompt)
        self.assertEqual(summary.generation_stats["requests"], 1)

        courses = {c.course: c for c in summary.course_summaries}
        self.assertEqual(set(courses), {"CSCE121", "ASTR111"})
        csce = courses["CSCE121"]
        self.assertEqual(
            csce.exams, "Exams are hard, but the study guide mirrors them."
        )
        self.assertEqual(
            csce.policies, "Missed quizzes can be made up with a doctor's note."
        )
        self.assertIsNone(csce.teaching)  # its only claim cited r9
        self.assertEqual(csce.tips, ["Work every study guide problem."])
        self.assertEqual(csce.citations["exams"], [["a1", "a3"]])
        self.assertEqual(csce.total_reviews, 3)
        self.assertEqual(csce.review_stats["review_count"], 3)
        # "Other students..." no longer refers to a sentence that is not shown.
        self.assertEqual(
            courses["ASTR111"].teaching, "Some students found the lectures engaging."
        )

        self.assertEqual(summary.tldr, "Hard exams, but well supported.")
        self.assertEqual(summary.citations["tldr"], ["a1", "a2"])
        self.assertEqual(summary.strengths, ["Personality: Patient in office hours."])
        self.assertEqual(
            summary.complaints, ["Grading: Grading feels harsh on projects."]
        )
        self.assertEqual(summary.total_reviews, 4)
        self.assertEqual(summary.generation_stats["claims_rejected_citation"], 2)
        self.assertEqual(summary.review_stats["review_count"], 4)

    def test_batch_runs_one_request_per_professor(self):
        client = make_client(tamu_response(CANNED))
        pipeline = make_pipeline(client)
        jobs = [
            (pid, [dict(row, professor_id=pid) for row in RAW_REVIEWS])
            for pid in ("p1", "p2", "p3")
        ]
        with patch.dict("os.environ", {"SUMMARY_API_WORKERS": "2"}):
            results = list(pipeline.process_professor_batch(jobs))

        self.assertEqual(client.requests_made, 3)
        self.assertEqual(sorted(pid for pid, _, _ in results), ["p1", "p2", "p3"])
        self.assertTrue(all(error is None for _, _, error in results))
        self.assertTrue(
            all(summary.generation_stats["requests"] == 1 for _, summary, _ in results)
        )

    def test_failed_request_fails_the_professor_instead_of_saving_fallback(self):
        bad = FakeResponse(200, {"choices": [{"message": {"content": "not json"}}]})
        client = make_client(bad)
        pipeline = make_pipeline(client)

        results = list(pipeline.process_professor_batch([("p1", list(RAW_REVIEWS))]))

        self.assertIsNone(results[0][1])
        self.assertIsInstance(results[0][2], ai_clients.TamuAIError)
        self.assertEqual(client.requests_made, 2)  # one strict retry, no more

    def test_verification_rejects_only_low_support(self):
        judge = Mock(spec=TypeSafeReviewJudge)
        judge.support_threshold = 0.72

        def support(claims, texts):
            self.assertTrue(all(c.evidence_review_ids for c in claims))
            scores = {}
            for index, claim in enumerate(claims):
                if "doctor" in claim.text:
                    scores[index] = 0.2
                elif "office hours" in claim.text:
                    continue  # unanswered: stays, because the check is optional
                else:
                    scores[index] = 0.9
            return scores

        judge.verify_claim_support.side_effect = support
        client = make_client(tamu_response(CANNED))
        summary = make_pipeline(client, judge).process_professor_reviews(
            list(RAW_REVIEWS), "p1"
        )

        csce = next(c for c in summary.course_summaries if c.course == "CSCE121")
        self.assertIsNone(csce.policies)
        self.assertIsNotNone(csce.exams)
        self.assertEqual(summary.strengths, ["Personality: Patient in office hours."])
        self.assertEqual(summary.generation_stats["claims_rejected_support"], 1)
        self.assertEqual(summary.generation_stats["claims_unverified"], 1)
        judge.classify_clusters.assert_not_called()
        judge.verify_claims.assert_not_called()

    def test_multi_course_failure_splits_by_course_group(self):
        good = tamu_response(output([empty_course("AAAA101")]))
        truncated = FakeResponse(
            200, {"choices": [{"message": {"content": "{"}, "finish_reason": "length"}]}
        )
        client = make_client([truncated, good, good])
        pipeline = make_pipeline(client)
        reviews = [
            raw_review(f"{course}-{i}", course, datetime(2020 + i, 1, 1), 4)
            for course in ("AAAA101", "BBBB202")
            for i in range(3)
        ]

        summary = pipeline.process_professor_reviews(reviews, "p1")

        self.assertEqual(client.requests_made, 3)
        self.assertEqual(summary.generation_stats["requests"], 3)
        prompts = [
            call[1]["json"]["messages"][1]["content"]
            for call in client._injected_session.calls[1:]
        ]
        self.assertTrue(all(p.count("## ") == 1 for p in prompts))


class ReviewStatsTests(unittest.TestCase):
    def test_computes_student_facing_stats_without_a_model(self):
        now = datetime(2026, 10, 16)
        rows = [
            dict(
                would_take_again=1,
                grade="A",
                attendance_mandatory="mandatory",
                textbook_use=3,
                review_date=datetime(2026, 1, 1),
                clarity_rating=5,
                helpful_rating=5,
            ),
            dict(
                would_take_again=0,
                grade="Not_Sure_Yet",
                attendance_mandatory="N",
                textbook_use=-1,
                review_date=datetime(2025, 5, 1),
                clarity_rating=3,
                helpful_rating=3,
            ),
            dict(
                would_take_again=None,
                grade="B+",
                attendance_mandatory="",
                textbook_use=2,
                review_date=datetime(2014, 5, 1),
                clarity_rating=2,
                helpful_rating=2,
            ),
            dict(
                would_take_again=1,
                grade="A",
                attendance_mandatory="non mandatory",
                textbook_use=0,
                review_date=datetime(2018, 5, 1),
                clarity_rating=4,
                helpful_rating=4,
            ),
        ]
        stats = compute_review_stats(rows, now=now)

        self.assertEqual(stats["would_take_again_pct"], 66.7)
        self.assertEqual(stats["would_take_again_n"], 3)
        self.assertEqual(stats["grade_distribution"], {"A": 2, "B+": 1})
        self.assertEqual(stats["attendance_mandatory_pct"], 33.3)
        # The 2014 1-5 usage scale is skipped; -1/0 mean no, 3/5 mean yes.
        self.assertEqual(stats["textbook_used_pct"], 33.3)
        self.assertEqual(stats["textbook_n"], 3)
        self.assertEqual(stats["avg_rating_recent"], 4.0)
        self.assertEqual(stats["recent_n"], 2)
        self.assertEqual(stats["avg_rating_older"], 3.0)
        self.assertEqual(stats["older_n"], 2)


class SmallBugTests(unittest.TestCase):
    def test_single_course_mixed_professor_is_not_called_varies_by_course(self):
        ratings = [5.0, 1.0, 3.0, 2.0, 4.0]
        self.assertEqual(headline_sentiment(ratings, {"A": ratings}), "Mixed reviews")
        self.assertEqual(
            headline_sentiment(
                [5, 5, 5, 1, 1, 1], {"A": [5.0, 5.0, 5.0], "B": [1.0, 1.0, 1.0]}
            ),
            "Mixed - varies by course",
        )

    def test_full_subject_names_normalize_to_department_codes(self):
        normalizer = CourseNormalizer()
        for raw, expected in [
            ("ASTRONOMY111", "ASTR111"),
            ("ASTRO 101", "ASTR101"),
            ("Chemistry-119", "CHEM119"),
            ("PSYCH107", "PSYC107"),
        ]:
            with self.subTest(raw=raw):
                self.assertEqual(normalizer.normalize_course_codes(raw), [expected])

    def test_dangling_openers_are_made_self_contained(self):
        self.assertEqual(
            self_contained("Other students say the curve is generous."),
            "Some students say the curve is generous.",
        )
        self.assertEqual(self_contained("However, exams are long."), "Exams are long.")
        self.assertEqual(self_contained("Exams are long."), "Exams are long.")


class RunOrderTests(unittest.TestCase):
    def test_current_term_instructors_run_first_in_stable_order(self):
        session = Mock()
        session.execute.side_effect = [
            [("Courtney Fitzpatrick",), ("Tipton, Rayburn",)],
            Mock(
                fetchall=lambda: [
                    ("knoop", "Jacqueline", "Knoop"),
                    ("tipton", "Rayburn", "Tipton"),
                    ("other", "Pat", "Example"),
                    ("fitz", "Courtney", "Fitzpatrick"),
                ]
            ),
        ]
        ordered, current = runner.prioritize_current_instructors(
            session, ["knoop", "tipton", "other", "fitz"]
        )
        self.assertEqual(ordered, ["tipton", "fitz", "knoop", "other"])
        self.assertEqual(current, 2)


if __name__ == "__main__":
    unittest.main()
