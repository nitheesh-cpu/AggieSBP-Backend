-- Degree planner: one row per signed-in user holding their academic record
-- (courses taken/in progress/planned) and their plan (requirement buckets,
-- watchlist). The frontend keeps the same documents in localStorage and
-- syncs them here; *_rev counters make concurrent saves from two devices
-- fail with 409 instead of overwriting each other.
-- Apply once before deploying the API that serves /users/me/planner:
--   psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" -v ON_ERROR_STOP=1 -f src/aggiermp/database/migrations/001_user_planner_state.sql

CREATE TABLE IF NOT EXISTS user_planner_state (
    user_id TEXT PRIMARY KEY,
    record JSONB NOT NULL DEFAULT '{}'::jsonb,
    record_rev INTEGER NOT NULL DEFAULT 0,
    plan JSONB NOT NULL DEFAULT '{}'::jsonb,
    plan_rev INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
