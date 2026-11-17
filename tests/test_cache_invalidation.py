"""Targeted cache invalidation: key patterns, aliases, batching, failures, endpoint."""

import fnmatch
import unittest
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from aggiermp.api.routers import admin_cache
from aggiermp.core import cache_invalidation as ci
from aggiermp.core.config import settings


class FakeRedis:
    def __init__(self, keys, fail=False):
        self.keys = set(keys)
        self.fail = fail
        self.delete_calls = []
        self.scan_calls = 0

    def ping(self):
        if self.fail:
            raise ConnectionError("down")

    def scan_iter(self, match=None, count=None):
        self.scan_calls += 1
        for key in sorted(self.keys):
            if match is None or fnmatch.fnmatchcase(key, match):
                yield key

    def delete(self, *keys):
        self.delete_calls.append(len(keys))
        n = len(set(keys) & self.keys)
        self.keys -= set(keys)
        return n

    def close(self):
        pass


class FakeDB:
    """Answers the two course-code queries; aliases come from load_aliases."""

    def __init__(self, summary_codes=(), review_codes=()):
        self.summary_codes = summary_codes
        self.review_codes = review_codes

    def execute(self, statement, params=None):
        sql = str(statement)
        rows = (
            self.summary_codes if "professor_summaries_new" in sql else self.review_codes
        )
        result = Mock()
        result.fetchall.return_value = [(c,) for c in rows]
        return result


def key(path):
    return f"api:{path}:0123456789abcdef"


KEYS = [
    "/professor/p1", "/professor/p1/reviews", "/professor/p10", "/professor/p2",
    "/professor/alias1", "/professor/find", "/professors", "/professors/search",
    "/professors/compare", "/course/CSCE121", "/course/CSCE121/professors",
    "/course/CSCE1210", "/course/MATH151", "/course/MATH151/reviews/p1",
    "/course/MATH151/reviews/p2", "/sections/202611/course/CSCE121",
    "/sections/202611/course/CSCE121/professors/details",
    "/sections/202611/course/MATH151", "/sections/202611", "/courses",
    "/courses/compare", "/departments", "/departments_info", "/discover/202611/ucc",
    "/data_stats", "/terms", "/users/me",
]


def make_redis(**kw):
    return FakeRedis([key(p) for p in KEYS], **kw)


def paths(fake):
    return {k.split(":")[1] for k in fake.keys}


class Base(unittest.TestCase):
    def setUp(self):
        self.redis = make_redis()
        for target in (
            patch.object(ci, "_client", lambda: self.redis),
            patch.object(settings, "site_revalidate_url", None),
            patch.object(settings, "revalidate_secret", None),
        ):
            target.start()
            self.addCleanup(target.stop)

    def enable_site(self):
        for name, value in (
            ("site_revalidate_url", "https://site.test/api/revalidate"),
            ("revalidate_secret", "s"),
        ):
            p = patch.object(settings, name, value)
            p.start()
            self.addCleanup(p.stop)


class ProfessorTests(Base):
    def run_prof(self, ids, aliases=None, summary=(), reviews=(), **kw):
        with patch(
            "aggiermp.core.professor_identity.load_aliases",
            return_value=aliases or {},
        ):
            return ci.invalidate_professors(ids, db=FakeDB(summary, reviews), **kw)

    def test_clears_professor_and_course_families_only(self):
        result = self.run_prof(["p1"], summary=["CSCE 121"], reviews=["MATH151"])
        left = paths(self.redis)
        for gone in (
            "/professor/p1", "/professor/p1/reviews", "/professor/find",
            "/professors", "/professors/search", "/professors/compare",
            "/course/CSCE121", "/course/CSCE121/professors",
            "/sections/202611/course/CSCE121",
            "/sections/202611/course/CSCE121/professors/details",
            "/course/MATH151", "/course/MATH151/reviews/p1",
            "/course/MATH151/reviews/p2",
            "/courses", "/courses/compare", "/discover/202611/ucc", "/data_stats",
        ):
            self.assertNotIn(gone, left, gone)
        # prefix-lookalikes and unrelated endpoints survive
        for kept in (
            "/professor/p10", "/professor/p2", "/course/CSCE1210",
            "/sections/202611", "/terms", "/users/me", "/departments",
        ):
            self.assertIn(kept, left, kept)
        self.assertEqual(result.keys_deleted, len(KEYS) - len(self.redis.keys))

    def test_alias_and_canonical_cover_each_other(self):
        self.run_prof(["p1"], aliases={"alias1": "p1"})
        self.assertNotIn("/professor/alias1", paths(self.redis))
        self.redis = make_redis()
        self.run_prof(["alias1"], aliases={"alias1": "p1"})
        self.assertNotIn("/professor/p1/reviews", paths(self.redis))
        self.assertIn("/professor/p2", paths(self.redis))

    def test_expand_includes_sibling_aliases(self):
        with patch(
            "aggiermp.core.professor_identity.load_aliases",
            return_value={"a": "c", "b": "c"},
        ):
            self.assertEqual(ci.expand_professor_ids(["a"], object()), {"a", "b", "c"})

    def test_paths_requested_from_website(self):
        self.enable_site()
        with patch("requests.post") as post:
            post.return_value.status_code = 200
            result = self.run_prof(["p1"], summary=["CSCE121"])
        body = post.call_args.kwargs["json"]
        self.assertEqual(
            set(body["paths"]),
            {"/professor/p1", "/professor/p1/reviews", "/course/CSCE121"},
        )
        self.assertEqual(post.call_args.kwargs["headers"], {"X-Revalidate-Secret": "s"})
        self.assertEqual((result.revalidation, result.pages_revalidated), ("ok", 3))

    def test_revalidation_skipped_when_unset(self):
        with patch("requests.post") as post:
            result = self.run_prof(["p1"])
        post.assert_not_called()
        self.assertEqual(result.revalidation, "skipped")

    def test_website_failure_is_reported_not_raised(self):
        self.enable_site()
        with patch("requests.post", side_effect=OSError("boom")):
            result = self.run_prof(["p1"])
        self.assertEqual(result.revalidation, "failed")
        self.assertGreater(result.keys_deleted, 0)

    def test_redis_failure_lenient_vs_strict(self):
        self.redis = make_redis(fail=True)
        self.enable_site()
        with patch("requests.post") as post:
            result = self.run_prof(["p1"])
            post.assert_not_called()  # would re-cache stale API data
        self.assertEqual(result.keys_deleted, 0)
        self.assertIn("failed", result.message)
        with self.assertRaises(ci.CacheInvalidationError):
            self.run_prof(["p1"], strict=True)

    def test_db_failure_still_clears_given_ids_unless_strict(self):
        db = Mock()
        db.execute.side_effect = RuntimeError("db down")
        with patch(
            "aggiermp.core.professor_identity.load_aliases", side_effect=RuntimeError
        ):
            result = ci.invalidate_professors(["p1"], db=db)
            self.assertNotIn("/professor/p1", paths(self.redis))
            self.assertGreater(result.keys_deleted, 0)
            with self.assertRaises(ci.CacheInvalidationError):
                ci.invalidate_professors(["p1"], db=db, strict=True)

    def test_empty_ids_do_nothing(self):
        self.assertEqual(ci.invalidate_professors([], db=object()).keys_deleted, 0)
        self.assertEqual(self.redis.scan_calls, 0)


class CourseTests(Base):
    def test_course_patterns(self):
        ci.invalidate_courses(["csce 121"])
        left = paths(self.redis)
        for gone in (
            "/course/CSCE121", "/course/CSCE121/professors",
            "/sections/202611/course/CSCE121", "/courses", "/courses/compare",
            "/departments", "/departments_info", "/discover/202611/ucc", "/data_stats",
        ):
            self.assertNotIn(gone, left, gone)
        for kept in (
            "/course/CSCE1210", "/course/MATH151", "/professor/p1", "/professors",
            "/sections/202611", "/terms",
        ):
            self.assertIn(kept, left, kept)

    def test_professor_pages_optional(self):
        ci.invalidate_courses(["MATH151"], include_professor_pages=True)
        self.assertNotIn("/professor/p1", paths(self.redis))
        self.assertNotIn("/professors/search", paths(self.redis))

    def test_many_courses_use_one_generic_pattern(self):
        codes = [f"AAAA{n}" for n in range(ci.MANY_COURSES + 1)]
        ci.invalidate_courses(codes)
        self.assertNotIn("/course/MATH151", paths(self.redis))
        self.assertEqual(self.redis.scan_calls, 1)

    def test_revalidates_course_paths_or_all(self):
        self.enable_site()
        with patch("requests.post") as post:
            post.return_value.status_code = 200
            ci.invalidate_courses(["MATH151"])
            self.assertEqual(post.call_args.kwargs["json"], {"paths": ["/course/MATH151"]})
            ci.invalidate_courses(["MATH151"], revalidate_all=True)
            self.assertEqual(post.call_args.kwargs["json"], {"all": True})

    def test_paths_are_batched(self):
        self.enable_site()
        pages = [f"/course/C{n}" for n in range(450)]
        with patch("requests.post") as post:
            post.return_value.status_code = 200
            result = ci.revalidate_site(pages)
        self.assertEqual(post.call_count, 3)
        self.assertEqual(result.pages_revalidated, 450)

    def test_non_200_marks_failed(self):
        self.enable_site()
        with patch("requests.post") as post:
            post.return_value.status_code = 401
            result = ci.revalidate_site(["/course/A"])
        self.assertEqual(result.revalidation, "failed")


class AllAndBatchingTests(Base):
    def test_all_deletes_every_api_key_in_batches(self):
        self.redis = FakeRedis([f"api:/x/{n}:h" for n in range(1200)] + ["other:1"])
        result = ci.invalidate_all()
        self.assertEqual(result.keys_deleted, 1200)
        self.assertEqual(self.redis.keys, {"other:1"})
        self.assertEqual(self.redis.delete_calls, [500, 500, 200])


class AdminEndpointTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(admin_cache.router)
        self.client = TestClient(app)
        patcher = patch.object(settings, "admin_api_key", "k")
        patcher.start()
        self.addCleanup(patcher.stop)
        self.h = {"X-Admin-Key": "k"}

    def test_requires_key(self):
        for headers in ({}, {"X-Admin-Key": "bad"}):
            r = self.client.post(
                "/admin/cache/clear", headers=headers, json={"all": True}
            )
            self.assertEqual(r.status_code, 401)

    def test_requires_something_to_clear(self):
        for body in ({}, {"professor_ids": []}, {"all": False}):
            r = self.client.post("/admin/cache/clear", headers=self.h, json=body)
            self.assertEqual(r.status_code, 422, body)

    def test_response_shape_and_dispatch(self):
        prof = ci.InvalidationResult(3, 2, "ok")
        course = ci.InvalidationResult(4, 1, "failed", "Website returned HTTP 500")
        with patch.object(
            admin_cache, "ainvalidate_professors", return_value=prof
        ) as p, patch.object(
            admin_cache, "ainvalidate_courses", return_value=course
        ) as c:
            r = self.client.post(
                "/admin/cache/clear",
                headers=self.h,
                json={"professor_ids": ["a"], "course_codes": ["MATH151"]},
            )
        self.assertEqual(r.status_code, 200)
        self.assertEqual(
            r.json(),
            {
                "keysDeleted": 7,
                "pagesRevalidated": 3,
                "revalidation": "failed",
                "message": "Website returned HTTP 500",
            },
        )
        p.assert_called_once_with(["a"], strict=True)
        c.assert_called_once_with(["MATH151"], strict=True)

    def test_redis_failure_is_an_error_not_skipped(self):
        failing = patch.object(
            admin_cache,
            "ainvalidate_courses",
            side_effect=ci.CacheInvalidationError("Cache invalidation failed: ConnectionError"),
        )
        with failing:
            r = self.client.post(
                "/admin/cache/clear", headers=self.h, json={"course_codes": ["MATH151"]}
            )
        self.assertEqual(r.status_code, 502)
        self.assertIn("ConnectionError", r.json()["detail"])

    def test_all(self):
        with patch.object(
            admin_cache, "ainvalidate_all", return_value=ci.InvalidationResult(9)
        ):
            r = self.client.post(
                "/admin/cache/clear", headers=self.h, json={"all": True}
            )
        self.assertEqual(
            r.json(),
            {
                "keysDeleted": 9,
                "pagesRevalidated": 0,
                "revalidation": "skipped",
                "message": None,
            },
        )


if __name__ == "__main__":
    unittest.main()
