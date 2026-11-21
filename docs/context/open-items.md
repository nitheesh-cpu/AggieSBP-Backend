# Backend open items

Known issues, deferred ideas and unverified claims. Each item has the date it was raised and a status. Close an item by changing its status to `done (<date>, <commit>)`; don't delete it. New items take the next free number (O-037 next).

"Unverified" means a past session reported it and it could not be confirmed from the code alone (it needs the live API, the production DB or the VM).

## Deploy and environment

### O-001: `develop` is ahead of `main` (2026-11-10, re-checked 2026-11-20)
The VM deploys from `develop`; GitHub's default branch is `main`. Local refs show `origin/main` 19 commits behind `origin/develop`, so the README and features on GitHub's front page are stale. Merge when the owner asks. **Status:** open.

### O-011: VM Python 3.13 vs the torch pin (2026-11-17)
`requirements.txt` pins `torch==2.5.1` for aarch64, which has no cp313 wheel, and the VM venv is Python 3.13. The owner said "ok fixed, ignore that" without saying how. Find out what the VM has and reconcile `requirements.txt`. **Status:** unverified.

### O-029: Default `cronitor_monitor_keys` lists a dropped monitor (2026-11-18)
Cronitor's free tier allows 5 monitors; the courses-upsert and merge-proposal monitors were dropped on the VM, but the default in `core/config.py` still includes the courses-upsert key. The VM `.env` overrides it; fix the default. **Status:** open.

### O-033: `pyproject.toml` dependency list is partial (2026-11-10)
`requirements.txt` is the real install path (D-042). Placeholder authors/URLs were fixed. Don't `uv run` casually: it re-synced the venv to `uv.lock` and reinstalled 33 packages. **Status:** open.

## API correctness

### O-002: Bare-number summary rows that collided with an existing code (2026-10-26)
`backfill_course_departments.py --apply` renamed 261 rows, skipped 163 "collisions" (bare `207` plus `POLS207` for the same professor, 135 professors) and left 28 unresolved. Those professors need re-summarizing and the old bare rows deleted; `upsert` does not delete stale course rows with different codes. **Status:** unverified (prod state unknown).

### O-003: Slow cold course professors details (2026-11-18)
`/sections/{term}/course/{code}/professors/details` takes 5-25 s cold for big courses; a 502 on MEEN 691 and timeouts on MGMT 311 and MLSC 485 were seen. Every edit to the matching files starts this cache cold (G-014). A "speed up" task was proposed, never started. **Status:** open.

### O-004: Two queries still show placeholders or the overall rating (2026-10-17, re-checked 2026-11-20)
`get_course_professors` ("BATCH 2" in `/course/{course_id}/professors`) still selects `COALESCE(p.avg_rating, NULL)` (the professor's overall rating) as the per-course `avg_rating`, and both it and `compare_professors` use `COALESCE(c.name, 'Course Title')`. Same bug class fixed in `/professor/{id}` (D-009). Check whether the frontend/extension display these fields before fixing. **Status:** open.

### O-005: Health endpoints return raw exception text (2026-10-30, re-checked 2026-11-20)
`/health` returns `"error": str(e)`, `/db-status` puts `str(e)` in its 500 detail, and `/cache/status` does the same (`core/cache.py`). Everything else uses `_db_error` (D-044). **Status:** open.

### O-006: CORS cleanup (2026-05-16, re-checked 2026-11-20)
Delete or fix the dead `chrome-extension|moz-extension` `allow_origin_regex` and decide whether the hardcoded `https://howdy.tamu.edu` origin is still needed (G-010). **Status:** open.

### O-007: Two sources for section attributes (2026-11-19)
`pipelines/sections/upsert.py` now writes `section_attributes_detailed`, but `routers/discover.py` reads only `attributes_text` (D-027). Consolidate on one source after checking every term is populated. **Status:** open.

### O-008: Ratings-only easiness can outrank GPA-backed scores (2026-11-09)
Renormalizing (D-028) lets a professor with only ratings score 78 next to a GPA-backed 74. The "no grade data" label is the only signal. Ranking them slightly lower was offered; the owner did not decide. **Status:** open (product decision).

### O-009: GPA boosters include some major-restricted courses (2026-11-09)
BMEN design and VTPP practicum courses slip through; the prereq filter hides most but not all. **Status:** open.

### O-010: `/users/tracking` Redis block is dead (2026-11-09, re-checked 2026-11-20)
Wrong relative import (G-033). Either delete the block or fix it with an expiry/cleanup plan. **Status:** open (decide deliberately).

### O-012: `routers/discover.py` never closes its DB sessions (2026-11-06, re-checked 2026-11-20)
It uses `Depends(get_session)` from `database.base` (G-011). Switch to `request_session`. **Status:** open.

### O-013: `DepartmentDB` declares a nonexistent `short_name` column (2026-10-26, re-checked 2026-11-20)
Any ORM query on departments fails (G-012). Fix the model to `id, title, long_name` after checking nothing writes `short_name`. **Status:** open.

### O-014: Course normalizer alias `PSYCH -> PSYC` should be PBSI (2026-10-26)
Kept because an existing test expects it (G-028). **Status:** open.

### O-015: Cache key drops repeated query values (2026-10-30, re-checked 2026-11-20)
`core/cache.py::_generate_cache_key` uses `str(sorted(request.query_params.items()))`; Starlette's `items()` keeps one value per key, so `/discover/{term}/ucc?section_type=A&section_type=B` shares a cache entry with `?section_type=B`. Use `multi_items()`. Changing the key format invalidates every cached entry once. **Status:** open.

### O-016: `/sections` accepts any positive `limit` (2026-10-30, re-checked 2026-11-20)
Only `-1` (mapped to 5000) and `<= 0` (500) are remapped; `limit=1000000` passes. Add an upper cap. **Status:** open.

### O-017: Dead SMTP settings (2026-05-13, re-checked 2026-11-20)
`smtp_*` fields in `core/config.py` are unused; email alerts and auto-untrack after N hours were removed. Delete them, or rebuild email fallback on purpose. Don't assume email alerts exist. **Status:** open.

### O-028: `POST /courses/compare` returned 500 in prod (2026-10-26)
The website's compare page no longer uses it. Its cache key may also ignore the request body. Fix or delete. **Status:** unverified.

### O-035: Full subject-name searches (2026-11-16)
"accounting 327" and "computer science 310" returned 0 results in a traffic review. `course_search_key` maps shorthand subjects, not full names. **Status:** unverified.

## Seat alerts

### O-018: Failed alerts are never retried (2026-11-17)
The watcher stamps `last_notified_open_at` even when every push for that watch failed. Pre-existing. **Status:** open.

## Professor data and matching

### O-019: PSYC-era grades and reviews are not merged into PBSI courses (2026-11-16)
Search and lookups map PSYC to PBSI (D-012), but older `gpa_data` rows and reviews filed under PSYC still sit under the old code. **Status:** open.

### O-020: Grade-row matching is strict (2026-11-18)
Rows are lost if `gpa_data` truncates long surnames (check max `length(professor)`). Rows now missed: "TAMAYO O", "DUGAT-WICKLIFF T", and initials that aren't the first given name ("AZAD A", "SMITH M"). Accepted cost of D-016. **Status:** unverified.

### O-021: Fetch-only deferral limits (2026-11-17)
Below-threshold new reviews are re-checked only when the professor gets another review (`--force-update` covers them). A new-review bump overwrites a pending row's reason (for example "Professor merge changed"). **Status:** open.

### O-022: Known duplicate RMP profiles to review in `/admin/merges` (2026-11-18)
Linda Katehi-Tseregounis (4 entries), Garth V. Crosby (2 plus "Garth Cosby"), Taylor Lathrop / Lathrop Taylor, Tanya Wickliff / "Tanay Wicliff" (score 0.79), Shelby/Shelly Blair. Tied evidence means neither entry shows on course pages. Middle-name and married-name cases (Seong G. Choi -> Gwan Choi, Marjorie A. Clendenin -> Angela Clendenin) are left to instructor-link proposals on purpose. **Status:** open (admin work, not code).

### O-023: Matching coverage was never measured end to end (2026-11-10, 2026-11-18)
The full-term old-vs-new identity audit died when the SSH tunnel dropped (write results incrementally next time). The RMP sweep searched only the 285 most-taught of 2,470 unmatched instructors. **Status:** open.

### O-024: Features tested only with mocks or a local Postgres (2026-11-17/18)
Instructor-links admin page and merges redesign (mock data), migration 005 (local Postgres only), the first monthly `unmatched_instructors.py --write-proposals` run, the summary worker end to end against real Redis and the AI provider, and the delete-account flow in a browser (SuperTokens delete only mocked). **Status:** unverified.

### O-027: Course normalizer keeps raw RMP class text as a course code (2026-11-09)
When it can't parse a code it keeps the raw text; the extension now escapes it. Rejecting non-`ABCD123` codes in the pipeline was proposed as a second layer. **Status:** open.

## AI summaries and prerequisites

### O-025: Summary quality follow-ups (2026-10-17)
Watch-outs are sometimes neutral facts ("multiple exams and quizzes in 2025"); he/she pronouns still appear. Prompt tightening was suggested, not done. There is no accuracy eval; hand-checking ~50 summaries for citation accuracy was suggested. **Status:** open.

### O-026: Prerequisite tree gaps (2026-11-06)
No N-of node ("select two from" stays low confidence); 73 low-confidence courses should show "verify in catalog"; CSCE 331 lost the C-or-better on the CSCE 350/ECEN 350 alternative. **Status:** open.

### O-030: `gpa_data` may need an index on `(dept, course_number)` (2026-11-16)
A SQL anti-join against sections timed out, so the GPA pipeline compares in Python. **Status:** unverified.

## Security audit leads (2026-10-30)

### O-032: Leads from the October security audit that were never validated
`/discover/.../fit-sections` accepts unbounded list sizes (large cross-join); password reset may not revoke existing sessions; no rate limit on login, sign-up or reset email; `/auth/dashboard` recipe exposed publicly; `/docs` loads `@scalar/api-reference@latest` with no SRI; slowapi keys on `get_remote_address` with in-memory storage, so behind a proxy all clients may share one bucket; the 30 s timeout middleware can't interrupt sync DB work. **Status:** unverified.

## Ideas (not scheduled)

### O-031: Deferred feature ideas
Conflict-aware seat alerts (skip the alert if the section conflicts with the user's saved schedule) (2026-05); "did you mean professor X?" when a course search matches a professor name (2026-11-12); a summarizer flag for professors with only 1-3 reviews (2026-05-21, requested then dropped); a read-only MCP server (`search_courses`, `get_sections`, `get_professor`, `compare_professors`, deterministic schedule generation) with per-IP limits and no raw RMP text (2026-10-20); infinite scroll for recent reviews (backend already supports skip/limit). **Status:** idea.

### O-034: Summary rebuilds are not scheduled (2026-11-16)
The nightly `--fetch-only` run queues rebuilds; they only run when someone starts them from `/admin` (by design, D-006). Listed so nobody assumes summaries refresh on their own. **Status:** by design.

### O-036: No admin "delete user" button (2026-11-17)
Email deletion requests are answered by pointing the user at the profile page. **Status:** idea.
