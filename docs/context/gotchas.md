# Backend gotchas

Code that looks wrong but is intentional, and bugs that were already fixed. Read the entry before "cleaning up" anything listed here. Decisions (the why behind features) are in `decisions.md`; open problems are in `open-items.md`.

Format: **Looks like** / **Actually** / **If you change it** / **Where** / **Fixed in** (commit, when known). New entries take the next free number (G-041 next).

## Tests

### G-001: The test suite's `conftest.py` writes to the database in `.env`, which is production
**Looks like:** a normal pytest setup; `pytest tests` should be safe.
**Actually:** `tests/conftest.py` imports the whole app and has a module-scoped `autouse` fixture `seed_data` -> `db_session` -> `create_db_engine()` + `Base.metadata.create_all` + INSERT seed rows. The local `.env` points `POSTGRES_*` at a localhost port that is an SSH tunnel to the production database, so a plain `pytest` run creates tables and inserts fake rows in prod when the tunnel is up (and hangs when it is down).
**If you change it:** always pass `--noconftest`. Example that works: `PYTHONPATH="src;." .venv/Scripts/python -m pytest --noconftest -p no:cacheprovider -p no:warnings -q tests/test_course_search_key.py`. Tests that need real SQL use a throwaway Postgres via `ADMIN_JOBS_TEST_DATABASE_URL`, `FETCH_ONLY_TEST_DATABASE_URL` or `PURGE_TEST_DATABASE_URL` and skip otherwise. CI runs the full suite against a throwaway Postgres service (`.github/workflows/ci.yml`).
**Where:** `tests/conftest.py`, `README.md` (Tests section).

### G-002: `tests/test_easiness_score.py` defines an empty `seed_data` fixture
**Looks like:** an unused fixture.
**Actually:** it overrides conftest's autouse DB-seeding fixture so these pure-function tests never touch a database, even when someone forgets `--noconftest`.
**If you change it:** deleting it makes this file hit the DB in `.env` (G-001).
**Where:** `tests/test_easiness_score.py`.

### G-003: `get_course_professors_details` imports inside the function and keeps a fixed query order
**Looks like:** sloppy local imports in `api/main.py` that a linter wants at module level; helpers that could live inline in `main.py`.
**Actually:** `tests/test_campus_matching.py` pulls this endpoint's source out of `main.py` with `ast`, strips decorators and `exec`s it in a namespace that only has `HTTPException` and `logger`. Everything else must be imported inside the function. The tests mock `db.query` in call order, so adding or reordering a `db.query` call breaks them. `load_instructor_links` uses only `db.execute` and returns empty for Mock sessions (its `to_regclass` check is not a str). Structured-summary helpers live in `api/structured_summaries.py` for the same reason. Tests that mock SQL strings must use the `_merged` view names.
**If you change it:** update `tests/test_campus_matching.py` in the same change.
**Where:** `api/main.py::get_course_professors_details`, `core/instructor_links.py`, `tests/test_campus_matching.py`.

### G-004: The summary runner uses `filter_by` for one professor and `filter(in_)` for merged ones
**Looks like:** a pointless branch; `in_(member_ids)` works for one id too.
**Actually:** `tests/test_summary_runner.py` mocks `query.filter_by(...)`. The single-member branch keeps those mocks valid.
**If you change it:** update the mocks at the same time.
**Where:** `pipelines/professors/upsert_reviews_and_summaries.py` (`member_ids`).

## Caching

### G-005: `@cached` does not store empty lists or `None`
**Looks like:** a missed cache opportunity.
**Actually:** a week-long TTL once kept an empty `/ucc` result from before a query fix. Empty results are recomputed every time on purpose.
**If you change it:** a transient empty result sticks for the full TTL (up to a week).
**Where:** `core/cache.py::cached`.

### G-006: Production Redis is only reachable through the API
**Looks like:** you can clear the cache from your machine.
**Actually:** local `.env` `REDIS_URL` is a local Redis. Production cache is cleared with `DELETE /cache/clear` (not `/cache`) plus `X-Admin-Key`, or from the website's `/admin` page (targeted clears via `core/cache_invalidation.py`). Professor and course pages are cached up to a week, so DB data fixes are invisible until cleared. When running a local API against production data, set `REDIS_URL=redis://127.0.0.1:1` (a dead port) so nothing local is ever written into a shared cache.
**Where:** `core/cache.py`, `core/admin_auth.py`, `api/routers/admin_cache.py`.

## API (`api/main.py` and routers)

### G-007: `except HTTPException: raise` comes before the generic handler
**Looks like:** a redundant except clause.
**Actually:** handlers end with `except Exception as e: raise _db_error(e)`. Without the re-raise first, an intentional 404 "Course not found" (or the tracking-cap 422 in `routers/users.py`) becomes a generic 500. 5xx responses also land in Google Search Console's error bucket.
**If you change it:** check `/course/CSCE999` still returns 404.
**Where:** course endpoints in `api/main.py`, `routers/users.py`.
**Fixed in:** e5c9e6b.

### G-008: Doubled braces inside f-string SQL
**Looks like:** typos: `'^[0-9]{{4}}$'` in gpa-boosters, and `text(f"""...""")` around big queries.
**Actually:** `/course/{id}` and `/professor/{id}` SQL is an f-string with one placeholder, `{structured_select}` (empty when the structured-summary columns are missing). gpa-boosters is also an f-string. Any literal brace in those queries must be doubled.
**If you change it:** "fixing" `{{4}}` to `{4}` turns it into the Python value 4 and breaks the regex; adding a single `{` breaks the query.
**Where:** `api/main.py` (`structured_select`), `routers/discover.py::discover_gpa_boosters`.

### G-009: `:param::type` does not work in SQLAlchemy `text()`
**Looks like:** valid Postgres (`:depts::text[]`).
**Actually:** it fails with "syntax error at or near ':'". Use `CAST(:x AS text[])` or `.bindparams(bindparam("x", type_=ARRAY(String)))`. Inside `text()`, this codebase writes `LIKE` wildcards as `%%` by convention. The `courses` department column is `subject_id`, not `dept`.
**Where:** any raw SQL.

### G-010: CORS: the extension regex is dead code, and wildcard Vercel origins are safe only because tokens are headers
**Looks like:** `allow_origin_regex` allows `chrome-extension://` and `moz-extension://` origins.
**Actually:** a few lines later `_cors_kwargs["allow_origin_regex"]` is overwritten with the Vercel + localhost regex (or localhost only). The extension still works because MV3 background fetches to `host_permissions` URLs bypass CORS. `^https://.*\.vercel\.app$` with `allow_credentials=True` is tolerable only because SuperTokens uses `get_token_transfer_method -> "header"` and `anti_csrf="NONE"`. `https://howdy.tamu.edu` in the origin list is a leftover from a content-script fetch that no longer exists.
**If you change it:** switching sessions to cookies makes every `*.vercel.app` site a credentialed origin (production turned the wildcard off on 2026-11-24 with `cors_allow_vercel_previews=false` in the VM `.env`; the code default is still True). Cleanup is an open item (O-006).
**Where:** `api/main.py` (CORS block, SuperTokens init), `core/config.py` (`cors_allow_vercel_previews`).

### G-011: `Depends(get_session)` from `database.base` leaks connections
**Looks like:** the obvious FastAPI dependency.
**Actually:** `database.base.get_session()` returns a plain pooled session that nothing closes. Use `Depends(request_session)` (yield + close). `routers/users.py`, `planner.py` and `prereqs.py` define their own generator `get_session` that wraps the raw one; that local name is safe. `routers/discover.py` still uses the raw one (O-012).
**If you change it:** new routers must use `request_session`.
**Where:** `database/base.py::request_session`, `routers/professors.py` (the compare endpoint leaked connections until it switched to `request_session`).

### G-012: The `DepartmentDB` ORM model declares a column the table does not have
**Looks like:** a normal model with `short_name`.
**Actually:** the real `departments` table has `id, title, long_name`. Any ORM query on `DepartmentDB` fails, and inside a shared session it leaves the transaction aborted, so every later query in the batch fails too. That is how 452 summary rows got bare course numbers ("119"). `course_normalizer.py` uses raw `SELECT id, title, long_name FROM departments` and calls `_rollback()` after any failed lookup.
**If you change it:** don't query `DepartmentDB` through the ORM; never swallow a DB error without a rollback. Fixing the model is O-013.
**Where:** `database/base.py::DepartmentDB`, `pipelines/professors/hierarchical_summarization/course_normalizer.py`.

### G-013: `create_db_engine()` runs `Base.metadata.create_all`
**Looks like:** a harmless engine factory.
**Actually:** every call creates any missing ORM tables in whatever DB it points at. Don't use it for read-only peeks at production; use a plain psycopg2 connection or `SET TRANSACTION READ ONLY`. Schema changes go in SQL migrations (D-040), not new ORM models.
**Where:** `database/base.py::create_db_engine`.

## Professor identity and merges

### G-014: Editing `professor_identity.py`, `name_matching.py` or `subjects.py` changes a cache key
**Looks like:** `MATCHING_VERSION = sha1(...)` over source files is over-engineering.
**Actually:** it is passed as `@cached(..., version=PROFESSOR_MATCHING_VERSION)` on the course professors details endpoint, so a matching fix shows up at once instead of after a week. Any byte change in those three files, even a comment or whitespace, starts that cache cold; big courses then take 5-25 s on first visit.
**If you change it:** don't make drive-by comment or format edits to those three files. Don't replace the hash with a manual constant (D-017).
**Where:** `core/professor_identity.py`, `api/main.py`, `core/cache.py::_generate_cache_key`.

### G-015: `_aliases_table_exists()` checks `to_regclass` first instead of try/except around the SELECT
**Looks like:** could be a simple try/except.
**Actually:** catching a failed SELECT needs a rollback, which would discard the caller's uncommitted work (`upsert_professors` runs mid-transaction). It returns `isinstance(found, str)` so Mock sessions in tests count as "missing". The same pattern is used for other optional tables (`user_data.py`, `instructor_links.py`).
**Where:** `core/professor_identity.py::_aliases_table_exists`.

### G-016: The API reads `*_merged` views, pipelines write raw tables
**Looks like:** needless indirection; `FROM professors` would be simpler.
**Actually:** duplicate RMP profiles are merged through `professor_aliases` and the views in migration 002 (D-021). Switching API reads back to the raw tables brings duplicates back; making pipelines write merged data breaks undo. The views are built from column lists, so re-run migration 002 after adding a column to `professors`, `reviews` or `professor_summaries_new`.
**Where:** `database/migrations/002_professor_aliases.sql`, `api/main.py`, `routers/discover.py`, `routers/professors.py`.

## Discover (`api/routers/discover.py`)

### G-017: A new fit-sections field needs SQL, the model AND the constructor kwarg
**Looks like:** adding a field to the SQL and the Pydantic model is enough.
**Actually:** `DiscoverFitSection` objects are built by hand; `title` came back `None` until `title=s.get("title")` was added. The frontend splits special topics on that per-section title, and silently stops splitting without it.
**Where:** `routers/discover.py` (`json_build_object`, `DiscoverFitSection`, the constructor).

### G-018: Final-exam meetings are excluded from class times
**Looks like:** an odd filter, `COALESCE(sm.meeting_type, '') NOT ILIKE '%%exam%%'`, in two places.
**Actually:** exam meetings (729 in Spring 2027) were counted as class times, so Fit My Schedule reported false conflicts and Find electives showed exam slots. `%%` is the escaped `%` inside `text()`.
**If you change it:** false conflicts come back. Keep both the meeting_times CTE and the meetings `string_agg` filtered.
**Where:** `routers/discover.py` (fit-sections).

### G-019: Specific discover routes must stay above `GET /{term_code}/{dept_code}`
**Looks like:** route order does not matter.
**Actually:** FastAPI matches in declaration order. The catch-all `/{term_code}/{dept_code}` is the last route in the file; `ucc`, `ucc-filter-options`, `gpa-boosters` etc. above it would otherwise be read as department codes.
**If you change it:** add new GET routes above the catch-all.
**Where:** `routers/discover.py`.

### G-020: `/ucc` filters sections in a CTE before splitting attributes
**Looks like:** the `ucc_sections` CTE with `EXISTS(regexp_split_to_table(...))` could be folded into the main query; `attributes_text` looks like a worse source than `section_attributes_detailed`.
**Actually:** doing the LATERAL split before the professor ILIKE join multiplied rows until the request timed out. `attributes_text` is used because the detailed table was empty for the terms in use (D-027).
**Where:** `routers/discover.py::discover_ucc_courses`.

## Pipelines

### G-021: The catalog scraper strips zero-width spaces
**Looks like:** `.replace("​", "")` after `unescape` is noise.
**Actually:** graduate catalog department labels contain U+200B (`"AAMD-​ Aerosp &​ Austere Med"`). Without the strip only 2 of 372 graduate departments parsed.
**If you change it:** graduate courses (CSCE 614 etc.) disappear on the next catalog run.
**Where:** `pipelines/courses/course_catalog_scraping.py`.

### G-022: anex.us replies are odd; the GPA pipeline is deliberately slow
**Looks like:** a string compare against `'{"classes":]}'` instead of JSON parsing; an 8 s request interval.
**Actually:** anex returns that invalid JSON for courses with no grades; it is "no data", not an error. anex IP-blocks after about 10 quick requests, and the block lasts a long time. TLS is checked against certifi because the Windows trust store rejected anex's chain (every request failed and looked like slowness). Terms are ordered SPRING < SUMMER < FALL within a year; the old order meant fall grades never loaded.
**If you change it:** don't raise concurrency or cut the interval; don't replace the no-data check with JSON error handling. Use a fresh `--checkpoint` file per run (checkpoints skip courses already saved).
**Where:** `pipelines/gpa/anex_scraping.py`, `pipelines/gpa/upsert_gpa_data.py`.
**Fixed in:** c81c802, 51f1838, 2714fae.

### G-023: Cache invalidation wiring has a few deliberate quirks
**Looks like:** inconsistent naming and flags.
**Actually:** `admin_worker.clear_summary_cache(...)` kept its old name so `tests/test_admin_jobs.py` patches still resolve; it calls `invalidate_professors(..., strict=True)`. The worker passes `invalidate_cache=False` to `upsert_reviews_and_summaries` to avoid double invalidation (that function invalidates in a `finally`, also after errors). `upsert_courses.py` uses `revalidate_all=True` because `ON CONFLICT DO UPDATE` can't tell what changed. GPA `fetch_and_upsert` wraps `_fetch_and_upsert` and invalidates in `finally`. `/admin/cache/clear` uses strict mode so a Redis failure is an error, not "skipped".
**Where:** `core/cache_invalidation.py`, `core/admin_worker.py`, `api/routers/admin_cache.py`, `pipelines/courses/upsert_courses.py`, `pipelines/gpa/`.

### G-024: TAMU AI can report "budget exceeded" with HTTP 200
**Looks like:** the client inspects the error body before the status code, which seems backwards.
**Actually:** TAMU's gateway has returned `budget_exceeded` with status 200 and `error.code` 400. `_raise_provider_error` maps `budget_exceeded`, `insufficient_quota`, `quota_exceeded`, `rate_limit_exceeded`, 429 or matching message text to `TamuAIQuotaExceeded`. The run then stops cleanly, keeps its checkpoint and saves nothing partial. The budget is a token budget, not a request count.
**If you change it:** a status-code-only check treats an exhausted budget as success.
**Where:** `pipelines/professors/hierarchical_summarization/ai_clients.py`.

### G-025: Without TAMU credentials the summary pipeline silently switches to local BART
**Looks like:** the pipeline "works" on a host with no AI config.
**Actually:** without `TAMU_AI_API_KEY` and `TAMU_AI_MODEL` it uses HDBSCAN + BART (a ~1.6 GB model, much heavier and lower quality). On the TAMU path a failed request fails that professor; no fallback summary is saved (D-004).
**Where:** `pipelines/professors/hierarchical_summarization/pipeline.py`.

### G-026: `parse_prereqs.py --dry-run --out` writes the file before printing
**Looks like:** odd ordering.
**Actually:** the first full LLM run (~70 requests) crashed on a Windows console encoding error while printing catalog text and lost every result. Save first; run with `PYTHONIOENCODING=utf-8`. `--apply FILE` writes a reviewed dry run with no LLM calls and skips rows whose text hash changed. Bump `PARSER_VERSION` in `prereq_parser.py` when parser logic changes, or unchanged rows are skipped.
**Where:** `pipelines/courses/parse_prereqs.py`, `pipelines/courses/prereq_parser.py`.

### G-027: Prerequisite `normalize_text` quirks are load-bearing
**Looks like:** fiddly regexes.
**Actually:** it fixes double-encoded NBSP (`Â` + NBSP, the "CSCEÂ 221" mojibake) and U+FFFD between subject and number; turns `"; or X"` into `", or X"`; expands shorthand ("GEOL 101 and 102" -> "GEOL 101 and GEOL 102") but must not touch "300-level", "6 credits" or "300 or above" (`_BARE_NUM` lookahead); collapses mirrored cross-lists. Without shorthand expansion the validator rejected correct LLM trees for "inventing" GEOL 102. Keep non-ASCII characters as `\u` escapes in the source; a file write once turned them into invisible literal characters.
**Where:** `pipelines/courses/prereq_parser.py`, `tests/test_prereq_parser.py`.

### G-028: Course normalizer: real subject codes beat aliases; one alias is knowingly wrong
**Looks like:** `COSC` should map to CSCE; `PSYCH -> PSYC` looks right.
**Actually:** `COSC` is Construction Science today, so real department codes are checked before aliases (the old alias turned COSC 375 into CSCE 375). `PSYCHOLOGY/PSYCH/PYSCH -> PSYC` is wrong (TAMU uses PBSI) but kept because an existing test expects it (O-014). Only 3-digit (or 6-digit run-together) numbers get a department inferred; "1", "4063" and CRNs are typos, 4-digit numbers are Texas common course numbers.
**Where:** `pipelines/professors/hierarchical_summarization/course_normalizer.py`, `core/subjects.py`.

## Planner

### G-029: Planner `_save` inserts before it locks
**Looks like:** a redundant `INSERT ... ON CONFLICT DO NOTHING` before `SELECT ... FOR UPDATE`.
**Actually:** `FOR UPDATE` on a missing row locks nothing, so two simultaneous first saves both inserted and one hit a unique violation (500) instead of a clean 409.
**If you change it:** the race comes back.
**Where:** `api/routers/planner.py::_save`.

## Seat alerts and push

### G-030: `ex.response is not None`, not `if ex.response`
**Looks like:** a verbose truthiness check.
**Actually:** a `requests.Response` is falsy for any 4xx/5xx, so `if ex.response and ex.response.status_code in [404, 410]` never ran and dead subscriptions were never cleaned.
**If you change it:** cleanup silently stops again.
**Where:** `core/notifications.py`, `collectors/section_watcher.py`.
**Fixed in:** 71614b2.

### G-031: `send_test_notification` is a plain `def`
**Looks like:** should be `async def` like its neighbours.
**Actually:** pywebpush is blocking. A plain `def` makes FastAPI run it in the threadpool. As `async def`, a user pointing at a never-responding host stalls one of the few API workers. Sends also pass `timeout=PUSH_TIMEOUT_SECONDS` (pywebpush has no default timeout).
**Where:** `api/routers/users.py::send_test_notification`, `core/push_endpoints.py`.

### G-032: The push endpoint allowlist is checked in three places, and its module imports nothing
**Looks like:** duplicated validation; `push_endpoints.py` re-implements config.
**Actually:** the server POSTs to the stored endpoint, so an arbitrary URL is SSRF. The check runs on save (`PushSubscriptionRequest` validator, 422), on send in `core/notifications.py`, and on send in the cron watcher (which deletes non-allowlisted rows saved before validation existed). The module has no settings import so the watcher can import it standalone.
**If you change it:** removing any call site reopens SSRF for old rows. Test: `tests/test_push_endpoints.py` (includes lookalike hosts).
**Where:** `core/push_endpoints.py`, `api/routers/users.py`, `core/notifications.py`, `collectors/section_watcher.py`.
**Fixed in:** 3f9f57f.

### G-033: The `/users/tracking` Redis block never runs (wrong relative import)
**Looks like:** working code that adds `tracked_sections` / `trackers:{term}:{crn}` Redis sets.
**Actually:** `from ..core.cache import get_redis` inside `routers/users.py` resolves to `aggiermp.api.core.cache`, which does not exist; the exception is swallowed. Nothing reads those sets for alerts (the watcher reads Postgres), so it has no user-visible effect.
**If you change it:** "fixing" the import to `...core.cache` makes the sets grow with no reader and no expiry. Decide deliberately (O-010): fix it with a cleanup plan, or delete the block.
**Where:** `api/routers/users.py` (`POST /users/tracking`).

### G-034: Seat-alert section ids come in two formats
**Looks like:** `section_id` should be the DB id.
**Actually:** the extension sends `TERMCODE-CRN-DEPT-NUM-SEC`; the DB `sections.id` is `TERMCODE_CRN`. `core/user_data.py::extract_crn` accepts both. The website deliberately emits the extension format so alerts dedupe the same way from either client.
**Where:** `core/user_data.py`, `api/routers/users.py`.

## Data shapes

### G-035: Course codes are stored with a space
**Looks like:** `c.code = 'ACCT327'` should match.
**Actually:** codes are stored as "ACCT 327". Search compares `REPLACE(c.code, ' ', '')` with `course_search_key()`; course-id endpoints strip whitespace from the path (`re.sub(r"\s+", "", course_code)`), so `/course/ACCT 327` works. Grade data names instructors as "LAST I" (surname + first initial).
**Where:** `api/main.py`, `core/subjects.py`.

### G-036: Term codes
**Looks like:** a 6-digit number.
**Actually:** registrar `term_code` is year + season digit (1 spring, 2 summer, 3 fall) + campus digit (1 College Station, 2 Galveston): `202711` = Spring 2027 College Station, `202631` = Fall 2026. Planner term keys are the first 5 characters, compared as strings. Hardcoded years are a recurring bug class; grep for literal years when touching term logic.
**Where:** `core/campuses.py`, `api/routers/planner.py`.

### G-037: Review counts come from the overall summary row, not a SUM
**Looks like:** `SUM(ps.total_reviews)` over a professor's summary rows.
**Actually:** `professor_summaries_new` has one overall row (`course_code IS NULL`, already counts every review) plus per-course rows. Summing them doubled counts (170 in the list vs 85 on the profile). Same row distinction applies to ratings: per-course cards must not show the overall rating.
**Where:** `api/main.py` (`/professors` queries).
**Fixed in:** e5c9e6b.

### G-038: `InternalExemptLimiter` overrides a private slowapi method
**Looks like:** hacky subclassing.
**Actually:** slowapi's `request_filter` / `exempt_when` take no arguments and cannot see the request header, so `_check_request_limit` is overridden (and `request.state.view_rate_limit` set to None). It depends on slowapi internals; `slowapi==0.1.9` is pinned in `requirements.txt`.
**If you change it:** after any slowapi upgrade, run `tests/test_internal_rate_limit_exemption.py`. Without the exemption, server-rendered pages hit the per-IP limit and Googlebot gets error pages.
**Where:** `api/main.py`, `core/config.py` (`internal_api_key`).

## Repo hygiene

### G-039: Line endings are mixed
**Looks like:** whole-file diffs after a scripted edit.
**Actually:** git stores LF; with `core.autocrlf=true` many working-tree files are CRLF (`api/main.py`, `core/cache.py`, `routers/users.py`), others LF (`routers/discover.py`). A Python replace script once produced `\r\r\n` and a 12k-line diff. Use the Edit tool, or open files with `newline=""`. Check with `git diff --ignore-cr-at-eol --stat`. `database/migrations/004_admin_jobs.sql` can show as modified from CRLF noise alone; don't commit that.
**Where:** whole repo.

### G-040: Two user tables are not defined anywhere in this repo
**Looks like:** `user_tracked_sections` and `user_schedules` are missing models.
**Actually:** they were created outside the repo (no model, no migration). Account deletion lists them in `USER_DATA_TABLES`, and `tests/test_account_deletion.py` finds user_id tables from models, migrations and `INSERT INTO ... (user_id` in source, so a new user table that isn't listed fails the test.
**Where:** `core/user_data.py`, `tests/test_account_deletion.py`.

### G-041: Rate limits use `request.client.host`, which is right only because nginx fixes it
**Looks like:** slowapi keys on `get_remote_address` with no Cloudflare handling, so behind Cloudflare it would see Cloudflare's servers.
**Actually:** nginx on the VM turns `CF-Connecting-IP` into the real client address (only for Cloudflare's ranges) and sends `X-Forwarded-For: $remote_addr`; uvicorn trusts 127.0.0.1 and takes it. Before 2026-11-24 the limiter counted Cloudflare servers, so students behind one shared every limit (parent OI-019).
**If you change it:** do not read `CF-Connecting-IP` in Python; anyone who reaches the VM directly could fake it. If nginx or Cloudflare goes away, redo the client-address setup there. Requests with `X-Internal-Key` skip limits anyway.
**Where:** `api/main.py` (`InternalExemptLimiter`), VM `/etc/nginx/nginx.conf`, `/etc/nginx/cloudflare-realip.conf`, `/usr/local/sbin/update-cloudflare-realip`.

### G-042: RMP's numRatings can be lower than the number of ratings it lists
**Looks like:** a teacher's `numRatings` should equal the ratings its `ratings` connection returns.
**Actually:** live RMP returned `numRatings` 753 with 754 listed ratings (2026-11-25). `deleted_review_ids` therefore trusts a list that is at least as long as `numRatings` and returns None only for a shorter one (missing pages would look like deletions). The same lag makes `needs_review_fetch` refetch such professors each night (cheap: no new ids).
**If you change it:** never require equality, and never treat a short list as authoritative.
**Where:** `pipelines/professors/review_sync.py` (`deleted_review_ids`, `needs_review_fetch`).

### G-043: Course numbers are not always digits (LAW 7383S)
**Looks like:** `courses.course_number` is numeric, so `course_number::int` and a `\d+` route regex are safe.
**Actually:** 12 Law courses end in a letter (`7383S`, 2026-11-25). The related-courses query in `get_course_details` cast every course number in the subject to int, so Postgres failed on `7383S` and every LAW course page returned 500 (Search Console: 265 pages with server errors after 2026-11-23). It now compares leading digits (`substring(course_number from '^[0-9]+')::int`), and the course routes accept one trailing letter (`\d+[A-Z]?`), like the course-professors route already did.
**If you change it:** never cast `course_number` to int without a digits-only guard that Postgres must evaluate first (a `CASE`, or `substring` as above); `WHERE x ~ '^[0-9]+$' AND x::int ...` is not guaranteed to short-circuit.
**Where:** `api/main.py` (`get_course_details` and the other course routes' regex).
