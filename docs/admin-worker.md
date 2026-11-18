# Admin summary worker

The admin API queues targeted rebuilds from stored reviews. A separate worker runs
them, preserving progress across browser/API/worker restarts. Summary requests
are serialized with the scheduled summary runner using a PostgreSQL advisory
lock. Deploy the updated runner to cron as well as the API and worker.

1. Apply `src/aggiermp/database/migrations/004_admin_jobs.sql` to the same database
   used by the API (requires migration 002). This adds a transactional merge
   trigger, durable jobs, and heartbeat tables. Existing reviewed merges are
   conservatively marked for rebuilding once. No AI calls run during migration.
2. Install the backend's normal pipeline dependencies and credentials in its
   existing environment. The worker uses the same DB, Redis, and AI configuration.
3. Install the service below, adjusting paths/user if the VM differs, and run
   `sudo systemctl daemon-reload` and `sudo systemctl enable --now aggiesbp-summary-worker`.
4. Open `/admin/summaries`, select a small rebuild, and verify it finishes in
   `/admin/jobs`, clears the pending entry, and updates the professor page.

Example `/etc/systemd/system/aggiesbp-summary-worker.service`:

```ini
[Unit]
Description=AggieSBP admin summary worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=opc
WorkingDirectory=/home/opc/backend
# No EnvironmentFile: the worker loads /home/opc/backend/.env itself (settings and
# the summary runner both read it). On Oracle Linux, SELinux blocks systemd from
# reading a file in a home directory, so EnvironmentFile= fails with
# "Failed to load environment files: Permission denied".
Environment=PYTHONPATH=/home/opc/backend/src:/home/opc/backend
# Started through /bin/bash: SELinux won't let systemd execute a program in a
# home directory directly (status=203/EXEC "Permission denied").
ExecStart=/bin/bash -c "exec /home/opc/backend/.venv/bin/python -m aggiermp.core.admin_worker"
Restart=always
RestartSec=10
TimeoutStopSec=30
UMask=0077

[Install]
WantedBy=multi-user.target
```

The worker updates its heartbeat every 20 seconds. A crash releases the database
lock; the next worker resumes unfinished professors. A crash after saving a
summary but before recording progress can regenerate that professor (at-least-once
execution). Each batch is capped at 500 professors, history at 100 jobs, and the
visible event log at 100 entries. Events contain IDs and outcomes, not review text,
prompts or provider exception messages. Provider output may still be in the
restricted service journal. Unexpected worker failures log the job ID, exception
type and code location there, plus the missing module name or SQLSTATE when
available. Quota exhaustion requires an explicit retry after the
allowance resets; it never loops through paid requests automatically.

Merge revisions are checked when clearing stale markers, so a merge edited during
a rebuild remains pending and is reported as unfinished with a retry message.
Undoing a merge marks both professors. Professors with
no remaining reviews have obsolete summaries removed. Cron summary runs share the
execution lock but do not clear admin pending markers; an explicit admin rebuild
provides a conservative, auditable resolution.

Redis must be reachable: the worker checks it before starting expensive generation.
If cache invalidation fails later, the batch stops and pending markers stay intact.
Restore Redis before retrying. A retry may regenerate the last professor whose
summary was saved before cache invalidation failed.

To roll back the feature, stop the worker and revert the API/UI. Leave its tables
and merge trigger in place to preserve history and pending work. The service does
not modify cron schedules or expose shell execution.
