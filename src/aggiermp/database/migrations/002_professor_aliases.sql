-- Duplicate RateMyProfessors profiles, merged without touching the raw data.
--
-- `professors`, `reviews` and `professor_summaries_new` keep one row per RMP
-- teacher entry, exactly as the pipelines write them. `professor_aliases`
-- records which entries are the same person, and the *_merged views below
-- present one professor per person. The API reads the views; the pipelines
-- write the tables.
--
-- A merge only takes effect once its status is 'confirmed'
-- (pipelines/professors/merge_duplicates.py proposes, a person confirms).
-- Undo a merge by setting its status to 'rejected'; nothing else changes.
--
-- Safe to re-run. Re-run it after adding a column to professors, reviews or
-- professor_summaries_new so the views pick the column up.
--   psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" -v ON_ERROR_STOP=1 -f src/aggiermp/database/migrations/002_professor_aliases.sql

CREATE TABLE IF NOT EXISTS professor_aliases (
    alias_id VARCHAR PRIMARY KEY REFERENCES professors(id),
    canonical_id VARCHAR NOT NULL REFERENCES professors(id),
    evidence TEXT NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'proposed'
        CHECK (status IN ('proposed', 'confirmed', 'rejected')),
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    reviewed_at TIMESTAMP,
    CHECK (alias_id <> canonical_id)
);

CREATE INDEX IF NOT EXISTS ix_professor_aliases_canonical_id
    ON professor_aliases (canonical_id);

-- Hidden professors (see 006_hidden_professors.sql). Created here too so this
-- file can be re-run on its own without un-hiding anyone.
CREATE TABLE IF NOT EXISTS hidden_professors (
    professor_id VARCHAR PRIMARY KEY,
    reason TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- professor id -> the id it is shown under (itself unless merged). Hidden
-- merge groups have no rows, so every *_merged view below drops them.
CREATE OR REPLACE VIEW professor_canonical_ids AS
SELECT p.id AS professor_id, COALESCE(a.canonical_id, p.id) AS canonical_id
FROM professors p
LEFT JOIN professor_aliases a
    ON a.alias_id = p.id AND a.status = 'confirmed'
WHERE COALESCE(a.canonical_id, p.id) NOT IN (
    SELECT COALESCE(ha.canonical_id, h.professor_id)
    FROM hidden_professors h
    LEFT JOIN professor_aliases ha
        ON ha.alias_id = h.professor_id AND ha.status = 'confirmed'
);

DO $$
DECLARE
    cols TEXT;
BEGIN
    -- professors_merged: the canonical row's columns, with rating stats
    -- combined across every merged entry (weighted by rating count; RMP uses
    -- 0 for "no rating yet" and -1 for an unknown would-take-again share).
    SELECT string_agg(
        CASE column_name
            WHEN 'num_ratings' THEN 'g.num_ratings'
            WHEN 'avg_rating' THEN 'COALESCE(g.avg_rating, c.avg_rating) AS avg_rating'
            WHEN 'avg_difficulty' THEN 'COALESCE(g.avg_difficulty, c.avg_difficulty) AS avg_difficulty'
            WHEN 'would_take_again_percent' THEN
                'COALESCE(g.would_take_again_percent, c.would_take_again_percent) AS would_take_again_percent'
            ELSE 'c.' || quote_ident(column_name)
        END,
        ', ' ORDER BY ordinal_position
    )
    INTO cols
    FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'professors';

    EXECUTE 'DROP VIEW IF EXISTS professors_merged';
    EXECUTE $q$
        CREATE VIEW professors_merged AS
        WITH grouped AS (
            SELECT
                m.canonical_id,
                SUM(GREATEST(COALESCE(p.num_ratings, 0), 0)) AS num_ratings,
                SUM(p.avg_rating * p.num_ratings) FILTER (WHERE p.avg_rating > 0 AND p.num_ratings > 0)
                    / NULLIF(SUM(p.num_ratings) FILTER (WHERE p.avg_rating > 0 AND p.num_ratings > 0), 0)
                    AS avg_rating,
                SUM(p.avg_difficulty * p.num_ratings) FILTER (WHERE p.avg_difficulty > 0 AND p.num_ratings > 0)
                    / NULLIF(SUM(p.num_ratings) FILTER (WHERE p.avg_difficulty > 0 AND p.num_ratings > 0), 0)
                    AS avg_difficulty,
                SUM(p.would_take_again_percent * p.num_ratings) FILTER (WHERE p.would_take_again_percent >= 0 AND p.num_ratings > 0)
                    / NULLIF(SUM(p.num_ratings) FILTER (WHERE p.would_take_again_percent >= 0 AND p.num_ratings > 0), 0)
                    AS would_take_again_percent
            FROM professors p
            JOIN professor_canonical_ids m ON m.professor_id = p.id
            GROUP BY m.canonical_id
        )
        SELECT $q$ || cols || $q$
        FROM professors c
        JOIN grouped g ON g.canonical_id = c.id
    $q$;

    -- reviews_merged: every review, filed under the professor it is shown on.
    SELECT string_agg(
        CASE column_name
            WHEN 'professor_id' THEN 'm.canonical_id AS professor_id'
            ELSE 'r.' || quote_ident(column_name)
        END,
        ', ' ORDER BY ordinal_position
    )
    INTO cols
    FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'reviews';

    EXECUTE 'DROP VIEW IF EXISTS reviews_merged';
    EXECUTE 'CREATE VIEW reviews_merged AS SELECT ' || cols || '
        FROM reviews r
        JOIN professor_canonical_ids m ON m.professor_id = r.professor_id';

    -- professor_summaries_merged: one summary per shown professor and course.
    -- The canonical entry's own summary wins; an alias's summary fills in a
    -- course the canonical entry has none for, until the pipeline rebuilds
    -- the summaries from the merged reviews.
    SELECT string_agg(
        CASE column_name
            WHEN 'professor_id' THEN 'm.canonical_id AS professor_id'
            ELSE 's.' || quote_ident(column_name)
        END,
        ', ' ORDER BY ordinal_position
    )
    INTO cols
    FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'professor_summaries_new';

    EXECUTE 'DROP VIEW IF EXISTS professor_summaries_merged';
    EXECUTE 'CREATE VIEW professor_summaries_merged AS
        SELECT DISTINCT ON (m.canonical_id, s.course_code) ' || cols || '
        FROM professor_summaries_new s
        JOIN professor_canonical_ids m ON m.professor_id = s.professor_id
        ORDER BY m.canonical_id, s.course_code, (s.professor_id = m.canonical_id) DESC';
END $$;
