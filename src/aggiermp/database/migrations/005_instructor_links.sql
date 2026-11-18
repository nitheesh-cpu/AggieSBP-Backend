-- Reviewed links between registrar instructors and RateMyProfessors profiles.
--
-- The course page matcher (core/professor_identity.py) attaches a profile
-- only on evidence. Anything it could only attach on an assumption (a middle
-- initial standing in for the RMP first name, a swapped RMP name that fits
-- only through an initial, a fuzzy RMP search hit) is stored here as a
-- 'proposed' row by pipelines/professors/unmatched_instructors.py
-- --write-proposals, and a person decides in the admin console
-- (/admin/instructors):
--
--   confirmed  this profile is the instructor's; the course page shows it
--   rejected   this profile is not theirs; the matcher never picks it
--   a confirmed row with professor_id NULL means "this instructor has no RMP
--   profile": the course page shows none.
--
-- One row per (instructor_key, candidate). instructor_key is "pidm:<pidm>",
-- or "name:<registrar name>" for sections listed without a PIDM.
-- professor_id has no foreign key on purpose: a proposal may name an RMP
-- profile the nightly import has not added yet, and a confirmed link whose
-- profile disappears must stay visible (the admin Conflicts tab) instead of
-- being deleted with it.
--
-- Safe to re-run.
--   psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" -v ON_ERROR_STOP=1 -f src/aggiermp/database/migrations/005_instructor_links.sql

CREATE TABLE IF NOT EXISTS instructor_profile_links (
    id BIGSERIAL PRIMARY KEY,
    instructor_key TEXT NOT NULL,
    instructor_name TEXT NOT NULL,
    instructor_pidm BIGINT,
    campus TEXT,
    courses TEXT[] NOT NULL DEFAULT '{}',
    sections INTEGER,
    professor_id TEXT,
    rmp_legacy_id BIGINT,
    kind TEXT NOT NULL
        CHECK (kind IN ('plausible', 'near', 'rejected', 'middle_initial', 'swapped', 'manual')),
    score DOUBLE PRECISION,
    reason TEXT,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'proposed'
        CHECK (status IN ('proposed', 'confirmed', 'rejected')),
    source TEXT NOT NULL DEFAULT 'report'
        CHECK (source IN ('report', 'admin')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reviewed_at TIMESTAMPTZ,
    -- A "no RMP profile" decision is only ever a confirmed admin row.
    CHECK (professor_id IS NOT NULL OR (status = 'confirmed' AND kind = 'manual'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_instructor_profile_links_pair
    ON instructor_profile_links (instructor_key, (COALESCE(professor_id, '')));

-- The course page reads only decided rows.
CREATE INDEX IF NOT EXISTS ix_instructor_profile_links_decided
    ON instructor_profile_links (instructor_key)
    WHERE status IN ('confirmed', 'rejected');

CREATE INDEX IF NOT EXISTS ix_instructor_profile_links_professor
    ON instructor_profile_links (professor_id)
    WHERE professor_id IS NOT NULL;
