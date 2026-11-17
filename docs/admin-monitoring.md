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

Local isolated verification (does not load the live-database pytest fixtures):

```sh
PYTHONPATH=src python -m unittest discover -s tests -p test_admin_monitoring.py
```

API field reference: https://cronitor.io/docs/monitors-api
