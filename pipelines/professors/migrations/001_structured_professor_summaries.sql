-- Structured professor summaries (one TAMU request per professor).
-- The repository has no migration framework; apply once with psql before
-- saving summaries from the new pipeline. Every statement is idempotent.
--   psql "$DATABASE_URL" -f pipelines/professors/migrations/001_structured_professor_summaries.sql

ALTER TABLE professor_summaries_new ADD COLUMN IF NOT EXISTS tldr TEXT;
ALTER TABLE professor_summaries_new ADD COLUMN IF NOT EXISTS tips TEXT[];
-- Python-computed stats: would-take-again %, grade distribution, attendance,
-- textbook use, and recent-vs-older average rating.
ALTER TABLE professor_summaries_new ADD COLUMN IF NOT EXISTS review_stats JSON;
-- Field name -> one list of cited RMP review IDs per claim.
ALTER TABLE professor_summaries_new ADD COLUMN IF NOT EXISTS citations JSON;
