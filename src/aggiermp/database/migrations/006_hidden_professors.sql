-- Professors hidden from the site, for example after a takedown request.
--
-- A hidden professor disappears from every API read: professor_canonical_ids
-- drops the whole merge group, so professors_merged, reviews_merged and
-- professor_summaries_merged have no rows for it, and the course page matcher
-- treats it as "no profile" (core/hidden_professors.py). The raw professors
-- row stays, so the nightly RMP import does not bring the person back as a
-- new entry; the review and summary pipelines skip hidden ids.
--
-- Rows are added through POST /admin/hidden-professors (which also deletes our
-- copy of the reviews and summaries), never in a committed migration: the repo
-- is public and who asked to be removed is private. Undo with
-- DELETE /admin/hidden-professors/{id}; reviews come back on the next fetch.
--
-- professor_id has no foreign key on purpose: an entry can be hidden before
-- the import has added it.
--
-- Safe to re-run. Migration 002 defines professor_canonical_ids with the
-- same filter, so re-running 002 keeps professors hidden.
--   psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DATABASE" -v ON_ERROR_STOP=1 -f src/aggiermp/database/migrations/006_hidden_professors.sql

CREATE TABLE IF NOT EXISTS hidden_professors (
    professor_id VARCHAR PRIMARY KEY,
    reason TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Same definition as in 002. A group is hidden when any of its entries is.
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
