# Admin operational monitoring

Set these values in the **backend** environment, then restart the API:

```dotenv
CRONITOR_API_KEY=<Cronitor API key with monitor read access>
CRONITOR_MONITOR_KEYS=CX7Ga9,PpJGqD,YStGRp,FnepfT
CRONITOR_ENVIRONMENT=production
```

The default keys are the four monitors documented in the backend README. Verify
these against your account and add or replace keys as needed (up to 20). This is
a read-only integration: existing schedules and Cronitor alerting stay in place.
No Cronitor credentials belong in frontend or `NEXT_PUBLIC_` variables.

`GET /admin/operations/monitors` and `GET /admin/operations/health` require the
existing `X-Admin-Key` header. Monitor details include up to ten recent executions.
Cronitor reports missed schedules through its failing state; the dashboard also
shows the next expected event. Monitoring does not prove that a successful import
changed records; data freshness should be interpreted alongside job results.

Monitor responses are cached per API process for 30 seconds, including errors.
Requests use four concurrent connections, a five-second per-request timeout, an
eight-second overall deadline, and a one-megabyte response limit. Failed refreshes
retain the previous snapshot with `status: stale` and its original `fetchedAt`.
An initial failure is `unavailable`; missing credentials are `not_configured`.
Raw upstream errors and response fields are not returned to the browser.

Health checks are cached for 15 seconds and probe the database and Redis separately.
Worker health reads `admin_worker_heartbeats` from the admin jobs migration. A
heartbeat older than 90 seconds is stale; an empty table means not started; a
missing table means not configured. Timestamps are UTC ISO strings for the UI to
localize. The API version is the configured application version.

`GET /admin/operations/freshness` also requires the admin key. It reports the
latest stored course and section update timestamps and the publication date of
the newest stored professor review. A dataset with no recorded date returns
`null`. Results are cached for five minutes and probe failures return HTTP 503
for up to 30 seconds before retrying.

## GPA upsert

On the backend VM, install `tmux` and replace the existing GPA entry in
`opc`'s crontab with this line. Keep the PATH that currently finds `cronitor`
and make sure it also finds `tmux`.

```cron
0 0 1,15 * * /bin/bash /home/opc/backend/pipelines/gpa/run_in_tmux.sh
```

The launcher starts a detached session named `gpa` and runs Cronitor monitor
`PpJGqD` around the Python command inside it. As `opc`, attach with
`tmux attach -t =gpa` and detach with Ctrl+B, then D. The session closes when
the command exits. tmux rejects a launch while that session already exists.
Wait for any run started by the old crontab command to finish before starting
one with the launcher.

For a manual run, use
`/bin/bash /home/opc/backend/pipelines/gpa/run_in_tmux.sh`. The launcher
forwards GPA options such as `--full` and `--checkpoint /absolute/path.json`.

After deploying the backend and frontend, Admin → Operations → GPA upsert
shows courses attempted, records upserted, failures and cooldown retry times.
The page refreshes every 30 seconds while visible. The authenticated
`GET /admin/operations/gpa` endpoint reads the latest snapshot from
`data/gpa-upsert.json` under the backend checkout (or the directory configured
by `DATA_DIR`). Run the API and importer as `opc` on the same VM so both can
access the private snapshot. If you set `DATA_DIR`, use the same absolute
directory for the API and the importer.
A run with no heartbeat for five minutes is shown as stale. The final
snapshot stays available after the tmux session closes. Direct Python runs
can opt in with `--report-progress`; use the tmux launcher for scheduled
and manual runs on the VM so their progress updates cannot overlap.

Local isolated verification (does not load the live-database pytest fixtures):

```sh
PYTHONPATH=src python -m unittest discover -s tests -p test_admin_monitoring.py
```

API field reference: https://cronitor.io/docs/monitors-api
