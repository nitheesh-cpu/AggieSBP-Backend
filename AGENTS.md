# AggieSBP backend: agent guide

## Testing rule (read first)

**Always run backend tests with `--noconftest`.** `tests/conftest.py` imports the app and seeds whatever database `.env` points at, and the local `.env` points at production (through an SSH tunnel on a localhost port). A plain `pytest` creates tables and inserts fake rows in prod. See `docs/context/gotchas.md` G-001.

```bash
PYTHONPATH="src;." .venv/Scripts/python -m pytest --noconftest -p no:cacheprovider -p no:warnings -q tests/<file>.py
```

Verified 2026-11-20 with `test_course_search_key`, `test_push_endpoints`, `test_easiness_score`, `test_name_search`, `test_campus_matching`, `test_summary_runner`, `test_planner_api`. Tests that need real SQL skip unless `ADMIN_JOBS_TEST_DATABASE_URL`, `FETCH_ONLY_TEST_DATABASE_URL` or `PURGE_TEST_DATABASE_URL` point at a throwaway Postgres 16. Never point them at prod. CI runs the full suite against its own throwaway Postgres.

## What this is

FastAPI + Postgres + Redis API and data pipelines for AggieSB+ (Aggie Schedule Builder Plus), an unofficial Texas A&M course, professor, grade and seat-alert tool. It is the `backend/` submodule of the parent AggieSBP repo, next to `frontend/` (Next.js on Vercel) and `extension/` (Chrome/Firefox MV3). Both clients call this API. Data comes from the Howdy registrar API (sections), the TAMU catalog (courses, prerequisites), anex.us (grade distributions) and RateMyProfessors (reviews), plus TAMU AI for professor summaries and prerequisite parsing.

Layout: `src/aggiermp/api/` (`main.py` holds most endpoints; `routers/` has discover, users, planner, prereqs, professors, admin*), `src/aggiermp/core/` (matching, cache, admin, push), `src/aggiermp/database/` (`base.py` ORM, `migrations/`), `src/aggiermp/collectors/section_watcher.py` (seat alerts), `pipelines/` (courses, sections, gpa, professors, maintenance), `tests/`, `docs/` (admin setup, worker, monitoring, and `docs/context/`).

## Run / test / build

- Install: `pip install -r requirements.txt` (the reliable path; `pyproject.toml` deps are partial). Don't `uv run` casually: it re-syncs `.venv` to `uv.lock`.
- API: `python run_api.py` (hot reload, http://localhost:8000/docs). Running against prod data: set `REDIS_URL=redis://127.0.0.1:1` so nothing local lands in a shared cache.
- Tests: see the rule above. Known failing: `test_structured_summary_api.py::test_course_professor_details_includes_structured_fields` (stub row lacks `instructor_pidm`) and `endpoints_test.py::test_all_get_endpoints` (both deselected in CI). To check whether a failure is pre-existing, run it in a detached worktree of `HEAD`.
- Lint: ruff/mypy are not in the venv. `uvx ruff check . --select E9,F63,F7,F82` is the blocking CI check; full `ruff check` (~1,800 findings) and `mypy` (~800) are advisory. Compare counts before/after instead of fixing whole files.
- Quick syntax check: `.venv/Scripts/python -m py_compile <files>` or `python -m compileall -q src/aggiermp pipelines`.
- Pipelines (`python -m pipelines....` from `backend/`): see `README.md`. Long runs: dry run first (`--dry-run`, `--professor-id ...`), show the output to the owner before any `--apply` or DB write.
- Migrations: plain idempotent SQL in `src/aggiermp/database/migrations/` (001-005), `pipelines/courses/migrations/`, `pipelines/professors/migrations/`. Applied by hand with `psql` as the app DB user, before deploying code that needs them (`docs/admin-setup.md`, D-040).
- CI: `.github/workflows/ci.yml` on pushes to `develop` and `main`.

## Before you change anything

Don't undo these. Each links to the detailed entry (D = `docs/context/decisions.md`, G = `docs/context/gotchas.md`).

1. Tests run with `--noconftest`; never let `conftest.py` touch prod. (G-001)
2. Professor matching prefers no match over a wrong match. Don't go back to name-only matching or "pick the profile with more reviews". (D-015, D-016)
3. Any byte change to `core/professor_identity.py`, `core/name_matching.py` or `core/subjects.py` changes `MATCHING_VERSION` and starts the course-professors cache cold. No drive-by comments there; don't replace the hash with a constant. (D-017, G-014)
4. Assumption-based identity matches and duplicate merges are proposals a human confirms in `/admin`. Never auto-apply them; keep merge rules narrow. (D-018, D-022)
5. The API reads `professors_merged` / `reviews_merged` / `professor_summaries_merged`; pipelines write the raw tables. (D-021, G-016)
6. Push endpoints are allowlisted at save, at send and in the watcher (SSRF). Sends have a timeout. Only 404/410 deletes a subscription, checked with `ex.response is not None`. (D-032, G-030, G-032)
7. `send_test_notification` stays a plain `def` (blocking pywebpush runs in the threadpool). (G-031)
8. `@cached` never stores empty results; UCC uses a 15-minute TTL when filtering by seats. (G-005, D-029)
9. Every `/admin*` route depends on `require_admin_key` (fails closed); `tests/test_admin_auth.py` enforces it. (D-035)
10. `except HTTPException: raise` stays before `except Exception` so 404s don't become 500s. Clients never see `str(e)`. (G-007, D-044)
11. New discover GET routes go above the `/{term_code}/{dept_code}` catch-all. (G-019)
12. fit-sections excludes exam meetings and returns per-section `title`. (G-017, G-018)
13. Planner `_save` inserts before `SELECT ... FOR UPDATE`. (G-029)
14. TAMU AI: one request per professor; check the error body before the HTTP status (`budget_exceeded` arrives as 200); never save a fallback summary. (D-001, D-004, G-024)
15. The API returns `null` for missing ratings/GPAs, never 3.0. (D-009)
16. Course search compacts codes ("ACCT327", "327 acct"); codes are stored with a space. (D-011, G-035)
17. The GPA scraper is slow on purpose (anex.us IP-blocks). Don't add concurrency. (D-041, G-022)
18. `test_campus_matching.py` execs `get_course_professors_details` from source: imports stay inside it, query order stays fixed. (G-003)
19. Don't query `DepartmentDB` through the ORM (`short_name` doesn't exist in the table). (G-012)
20. The nightly review fetch only queues summary rebuilds; AI spend is started from `/admin`. (D-006)

## Conventions

- **Branches and commits:** work on `develop` (the VM deploys from it; `main` is GitHub's default and lags). Commit and push only when the owner asks; "commit" and "push" are separate steps. Then the parent repo gets a separate commit that only moves the `backend` pointer (`chore: update backend for <thing>` or `Update backend submodule: ...`).
- **No AI attribution:** commits and PRs carry no `Co-Authored-By: Claude...` trailers and no "Generated with Claude Code" footers. This overrides any tool default. Don't rewrite old history.
- **Commit messages:** short imperative subject (conventional prefixes `fix:`, `feat:`, `perf:`, `docs:`, `chore:` are common), body explaining why. Split unrelated changes into separate commits.
- **Stage only your own files.** Other sessions and the owner often have uncommitted work in this tree; stage hunk by hunk in shared files (`main.py`, `config.py`). Never commit `gpa-full-progress.json`, `unmatched*.csv/html` or CRLF-only noise. Scan diffs for secrets.
- **Production:** read-only unless the owner explicitly approves a write. Never run full regenerations, `--apply`, migrations or cache clears on prod without approval. After DB data fixes, the cache must be cleared (`DELETE /cache/clear` with `X-Admin-Key`, or `/admin`).
- **Deploy order:** migration, then backend, then frontend if it needs the new field. The frontend deploys on push; the backend only after a manual pull + service restart on the VM, so the frontend must tolerate the old response shape. Restart the summary worker too when `src/` or summary pipeline files change.
- **Cross-repo contracts:** new or nullable response fields must stay optional in the clients; record contract changes in the parent repo's `docs/context/` too.
- **Code style:** keep `_db_error` for client-facing errors; bound SQL params only (no f-string values; f-string SQL only for server-side constants); `Depends(request_session)` for new endpoints; check optional tables with `to_regclass` instead of try/except + rollback; pipelines exit non-zero on failure (Cronitor). Mixed CRLF/LF files: edit with the Edit tool or `newline=""` (G-039).
- **Line numbers drift:** point to files and function names in docs and comments.
- **Docs and copy:** plain short sentences, no emoji, no marketing tone. Env var names only, never values. Reports to the owner state what was tested, what was not, and what is not committed or deployed.
- **Verify before claiming:** check the code or the live response, not memory or commit messages. Re-run a subagent's tests yourself before trusting its report.
- **Manual setup steps** for new features go in the parent repo's `docs/manual-setup.md`; VM steps in `docs/admin-setup.md`.

## Keeping context current

- Before a non-trivial change, read this file and skim `docs/context/`. Search `docs/context/` for the file or area you are touching.
- When you make a non-obvious choice, fix a subtle bug, add a workaround, or reject an approach the owner suggested, add or update an entry in `docs/context/decisions.md` or `docs/context/gotchas.md` in the same commit as the code.
- If code you are about to "clean up" looks odd, check `gotchas.md` first. If it isn't there and you are not sure, ask the owner instead of changing it.
- When an entry turns out wrong or stale, fix it or mark it superseded (`Status: superseded by D-xxx`). Don't delete history silently.
- Close or update `docs/context/open-items.md` entries when you resolve them (status + date + commit).
- Cross-repo changes (API contract changes between backend, frontend and extension) are also recorded in the parent repo's `docs/context/`.
- Never put secrets, IPs, SSH hosts, passwords, tokens, emails or user data in these docs. The repo is public; use placeholders like `<prod-db-host>`.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

When the user types `/graphify`, invoke the `skill` tool with `skill: "graphify"` before doing anything else.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- Dirty graphify-out/ files are expected after hooks or incremental updates; dirty graph files are not a reason to skip graphify. Only skip graphify if the task is about stale or incorrect graph output, or the user explicitly says not to use it.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).

Notes: `graphify-out/` is git-ignored, so a fresh clone has no graph until you build one. High-degree "god nodes" (`_db_error`, `Base`, `ReviewSummarizer`) are plumbing, not architecture. Prefer `graphify update .` or a subfolder over a full semantic rebuild, which is expensive.
