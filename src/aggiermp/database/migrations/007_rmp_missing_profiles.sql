-- RateMyProfessors profiles that seem to have been removed from RMP.
--
-- The nightly fetch (upsert_reviews_and_summaries.py --fetch-only) lists every
-- RMP profile at each campus. A stored profile missing from a complete listing
-- is looked up directly; if RMP returns nothing for it while a control lookup
-- works, it gets a row here. Each later night that confirms it again adds one
-- miss (at most one per day). Seen again on RMP: the row is deleted.
--
-- With --apply-removals, a profile with 2+ misses on different days is hidden
-- (hidden_professors, reason starting "Removed from RateMyProfessors"), or,
-- when it was merged with profiles RMP still has (RMP merging duplicates),
-- only its own stored reviews and summaries are deleted. Without the flag the
-- run only records misses and prints what it would do. See D-049.
--
-- professor_id has no foreign key on purpose, like hidden_professors.
--
-- Safe to re-run.
--   psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" -v ON_ERROR_STOP=1 -f src/aggiermp/database/migrations/007_rmp_missing_profiles.sql

CREATE TABLE IF NOT EXISTS rmp_missing_profiles (
    professor_id VARCHAR PRIMARY KEY,
    first_missing_at TIMESTAMP NOT NULL DEFAULT now(),
    last_missing_at TIMESTAMP NOT NULL DEFAULT now(),
    misses INTEGER NOT NULL DEFAULT 1 CHECK (misses >= 1),
    action VARCHAR CHECK (action IN ('hidden', 'content_deleted')),
    acted_at TIMESTAMP
);
