-- Seat-alert delivery log: one row per Web Push attempt made by the section
-- watcher (collectors/section_watcher.py), written in one batch per run.
--   status 'sent'    the push service accepted the message
--          'expired' the subscription is gone (push service answered 404/410);
--                    the watcher deletes that subscription
--          'failed'  any other error (timeout, 4xx/5xx, bad endpoint); `error`
--                    holds a short reason
-- `id` is also sent to the browser as the payload's notificationId.
-- No foreign keys: watches and subscriptions get deleted, the log stays.
-- Until this is applied the watcher still sends notifications and just logs
-- a warning that it skipped recording them.
--
-- Safe to re-run.
--   psql "$DATABASE_URL" -f src/aggiermp/database/migrations/003_push_notifications.sql

CREATE TABLE IF NOT EXISTS push_notifications (
    id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    track_id TEXT,          -- user_tracked_sections.id
    section_id TEXT,        -- user_tracked_sections.section_id
    term_code TEXT,
    crn TEXT,
    course TEXT,            -- e.g. 'CSCE 121.501'
    sent_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL CHECK (status IN ('sent', 'expired', 'failed')),
    error TEXT
);

CREATE INDEX IF NOT EXISTS ix_push_notifications_sent_at
    ON push_notifications (sent_at);

CREATE INDEX IF NOT EXISTS ix_push_notifications_user_id
    ON push_notifications (user_id);
