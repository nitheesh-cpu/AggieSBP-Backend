# Admin operations: deployment checklist

One ordered pass for the VM (`/home/opc/backend`, user `opc`, venv `.venv`, systemd, nginx in front of the API).
Run everything as `opc` from `/home/opc/backend` unless noted. The database connection comes from the
`POSTGRES_*` values in `.env` (the API builds its URL from them; there's no `DATABASE_URL` there).

## 1. Pull

```sh
cd /home/opc/backend && git pull
.venv/bin/pip install -r requirements.txt   # only if dependencies changed
```

## 2. Migrations (in order; each is safe to re-run)

```sh
set -a; . ./.env; set +a; export PGPASSWORD="$POSTGRES_PASSWORD"
M=src/aggiermp/database/migrations
DB=(-h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" -v ON_ERROR_STOP=1)
psql "${DB[@]}" -f $M/002_professor_aliases.sql     # skip if already applied; re-running is harmless
psql "${DB[@]}" -f $M/003_push_notifications.sql
psql "${DB[@]}" -f $M/004_admin_jobs.sql            # needs 002; first run marks existing reviewed merges for one summary rebuild
psql "${DB[@]}" -f $M/005_instructor_links.sql      # instructor <-> RMP profile links reviewed in /admin
psql "${DB[@]}" -f $M/006_hidden_professors.sql     # professors hidden after a takedown request (D-047)
```

Re-running 004 does not mark merges as pending again. No AI calls run during migration. Until 005 is applied the
course pages behave as before and `/admin/instructor-links` answers 409. Until 006 is applied
`/admin/hidden-professors` answers 503.

Hiding a professor (takedown request). Never put the id in a committed file; the repo is public.

```sh
curl -X POST "$API/admin/hidden-professors" -H "X-Admin-Key: $ADMIN_API_KEY" -H "Content-Type: application/json" \
  -d '{"professor_id": "<id from /professor/find>", "reason": "takedown request"}'
```

It deletes our copy of their reviews and summaries for the whole merge group and clears the API cache. The
professor row stays so the nightly import does not bring them back. `GET` lists hidden professors; `DELETE
/admin/hidden-professors/<id>` undoes it (reviews return on the next fetch, summaries need a rebuild).

## 3. `.env` additions (`/home/opc/backend/.env`)

```dotenv
ADMIN_API_KEY=<long random string>        # admin UI sends it as X-Admin-Key; unset = every /admin route answers 503
INTERNAL_API_KEY=<long random string>     # website server-side fetches send it as X-Internal-Key (skips rate limits)
CRONITOR_API_KEY=<Cronitor API key, monitor read scope>
CRONITOR_MONITOR_KEYS=CX7Ga9,YStGRp,PpJGqD,FnepfT,<merge-proposals key>
CRONITOR_ENVIRONMENT=production
SITE_REVALIDATE_URL=https://aggiesbp.com/api/revalidate   # see "Cache" below
REVALIDATE_SECRET=<long random string, same value as on Vercel>                 # see "Cache" below
```

Monitors are config only: to add one, append its key to `CRONITOR_MONITOR_KEYS` (max 20) and restart the API.
Generate secrets with `openssl rand -hex 32`. Keep `.env` mode 600.

## 4. Restart the API

```sh
sudo systemctl restart <api-service>      # your API unit name
sudo systemctl status <api-service> --no-pager
```

## 5. Install the summary worker

Copy the unit from `docs/admin-worker.md` to `/etc/systemd/system/aggiesbp-summary-worker.service`, then:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now aggiesbp-summary-worker
journalctl -u aggiesbp-summary-worker -n 20 --no-pager
```

Redis must be reachable by the worker. Redeploying code later: restart the worker too.

## 6. Cron: merge proposals

Create the "professor merge proposals" monitor in Cronitor, put its key in `CRONITOR_MONITOR_KEYS`, then `crontab -e`:

```cron
30 4 * * * cronitor exec <KEY> /home/opc/backend/.venv/bin/python /home/opc/backend/pipelines/professors/merge_duplicates.py --propose
```

### Nightly review fetch

Fetches new RateMyProfessors reviews (only for professors whose rating count changed), clears their caches, and queues professors who crossed the new-review threshold (`SUMMARY_MIN_NEW_REVIEW_PERCENT`, default 10) or have no summary yet in the "Awaiting rebuild" list of the admin UI. It never calls an AI provider; an admin rebuilds the queued professors from the UI. Needs migration 004 applied (without it the reviews are still stored and a warning is logged). It exits non-zero on any failure so Cronitor notices; add `--dry-run` to count without writing.

```cron
30 2 * * * cronitor exec <KEY> /home/opc/backend/.venv/bin/python /home/opc/backend/pipelines/professors/upsert_reviews_and_summaries.py --fetch-only
```

It does not take the summary lock, so it can overlap a worker rebuild safely: a review arriving mid-rebuild bumps the queue revision and the professor stays pending.

### Monthly instructor link proposals

Runs the unmatched-instructors report for the current terms, searches RateMyProfessors for instructors whose course
cards show no profile (throttled, about 1.5 s per new name, cached in `rmp_search_cache.json`), and stores every
candidate the matcher would only attach on an assumption (middle initial, swapped name, fuzzy RMP hit) as a proposal
in `instructor_profile_links`. Review them in the admin UI's instructor links page: confirm a profile, mark "no RMP
profile", or reject candidates; the course pages update right away. Confirmed and rejected pairs are never proposed
again; new candidates for an instructor you already decided show up under Conflicts. Needs migration 005 (without it
the report is written and the run exits 1). Without `--write-proposals` the report writes nothing to the database.

First run by hand (it fills the search cache, so it takes a while):

```sh
mkdir -p /home/opc/reports
.venv/bin/python pipelines/professors/unmatched_instructors.py --rmp \
    --rmp-cache /home/opc/reports/rmp_search_cache.json --write-proposals \
    --out-html /home/opc/reports/unmatched.html --out-csv /home/opc/reports/unmatched.csv
```

It prints how many proposals were added, refreshed and removed. Then create the "instructor link proposals" monitor
(monthly) in Cronitor, add its key to `CRONITOR_MONITOR_KEYS`, and `crontab -e`:

```cron
0 4 1 * * cronitor exec <KEY> /home/opc/backend/.venv/bin/python /home/opc/backend/pipelines/professors/unmatched_instructors.py --rmp --rmp-cache /home/opc/reports/rmp_search_cache.json --write-proposals --out-html /home/opc/reports/unmatched.html
```

Existing cron jobs keep their lines; the scheduled summary runner shares a PostgreSQL lock with the worker, so it needs the updated code from step 1 (nothing to change in crontab).

### Manual AI review of pending instructor links

From the backend root, use the existing `TAMU_AI_API_KEY` and `TAMU_AI_MODEL`
configuration and set `SERPER_API_KEY` in the environment or `.env` to include
Google search snippets. Pass `--no-web` to evaluate only the stored candidate
evidence. Start with a small batch before running the entire queue.

```sh
.venv/bin/python -m pipelines.professors.review_instructor_links --limit 20
```

Open **Admin → Instructors**. The API reads the existing
`scratch/instructor-ai-review.html` in the backend checkout. If the report was
saved elsewhere, set `INSTRUCTOR_AI_REPORT_PATH` to its absolute path in the
backend `.env` and restart the API. High-confidence matches are highlighted on
their candidate rows, with the AI's reasoning and sources beside the existing
link controls. In **To review** and **Conflicts**, high-confidence matches rank
first, followed by possible matches, with Spring section order within each group.
The script leaves the database unchanged. Omit `--limit`
to process the full queue. After a quota stop, use the reported `--offset` with
a different `--out` path to retain the first batch's report. Offsets assume the
pending queue has stayed in the same order.

Use **Bulk reject mismatches** to preview and confirm rejections across the
full queue. Pending profiles the AI rated **very unlikely** or **unlikely**
qualify, so you only review the matches it could not rule out. An unlikely
profile with reviews in a subject the instructor teaches is skipped. Clear
language-versus-quantitative department mismatches, such as French versus
Statistics, qualify with or without an AI report. An exact course in common
and high-confidence AI matches are always protected, as are confirmed links.
Rejections can be undone from the Rejected tab.

## 7. Vercel (website project, then redeploy)

| Variable | Value |
| --- | --- |
| `INTERNAL_API_KEY` | same as backend `INTERNAL_API_KEY` |
| `REVALIDATE_SECRET` | same as backend `REVALIDATE_SECRET` |
| `NEXT_PUBLIC_UMAMI_PROXY` | optional; `1` serves Umami through the site (build-time) |

Do not put `ADMIN_API_KEY` or any Cronitor key in Vercel; admins type the admin key into the admin UI.

Order matters: course and professor pages are server-rendered, and every render calls the API from Vercel's
servers. Finish steps 1–4 (new API code with `INTERNAL_API_KEY`) and set these variables **before** the frontend
deploy, or Google's crawling can hit the API's per-IP rate limits and get error pages.

## Cache

Nothing needs clearing by hand. Each job clears what it changed, in the API's Redis cache and in Vercel's page cache
(through `SITE_REVALIDATE_URL`, authenticated with `REVALIDATE_SECRET`):

| Trigger | Clears |
| --- | --- |
| Merge confirmed or undone (admin page or `merge_duplicates.py`) | both professors, the courses they teach, professor/course lists |
| Instructor link confirmed, rejected, "no profile" or undone (admin page) | the instructor's courses (course pages and section professor lists) |
| Summary rebuild (admin worker or the cron summary runner) | that professor and its merged entries, their courses, lists |
| GPA upsert | courses with new grade data, and professor pages (they show grades) |
| Course catalog upsert | every upserted course and all course pages |
| Sections upsert, section watcher | nothing (those responses are cached 15 minutes) |

The admin UI's Cache section (or `POST /admin/cache/clear`) clears specific professors, courses or everything.
If `SITE_REVALIDATE_URL`/`REVALIDATE_SECRET` are unset, Redis is still cleared and the page refresh is skipped
(pages then refresh within a day).

## 8. Verify

```sh
KEY=<ADMIN_API_KEY>; API=https://api-aggiesbp.nitheeshk.com
curl -s -o /dev/null -w '%{http_code}\n' $API/admin/check                       # 401
curl -s -H "X-Admin-Key: $KEY" $API/admin/check                                  # 200
curl -s -H "X-Admin-Key: $KEY" $API/admin/operations/health                      # database/redis/worker "healthy"
curl -s -H "X-Admin-Key: $KEY" $API/admin/operations/monitors | head -c 600      # status "ok", one entry per key
curl -s -X POST -H "X-Admin-Key: $KEY" -H 'Content-Type: application/json' -d '{"all":true}' $API/admin/cache/clear
                                                                                 # revalidation "ok" (or "skipped" if unset)
.venv/bin/python pipelines/professors/merge_duplicates.py --propose              # one manual run, then check the Cronitor monitor
curl -s -H "X-Admin-Key: $KEY" "$API/admin/instructor-links?tab=proposed&limit=1" | head -c 600
                                                                                 # counts per tab (409 = migration 005 missing)
```

Then open `/admin/summaries`, rebuild one professor, and confirm the job reaches `succeeded` in `/admin/jobs` and the
pending entry disappears. Worker heartbeat older than 90 s shows `stale`: check `journalctl -u aggiesbp-summary-worker`.
