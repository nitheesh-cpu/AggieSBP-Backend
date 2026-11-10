-- Structured prerequisite trees for the degree planner.
-- The repository has no migration framework; apply once with psql before
-- running pipelines/courses/parse_prereqs.py or deploying the API that reads
-- these columns. Every statement is idempotent.
--   psql "$DATABASE_URL" -f pipelines/courses/migrations/001_prereq_trees.sql

-- AND/OR tree parsed from `prerequisites` (see pipelines/courses/prereq_parser.py
-- for the node shapes). NULL means no prerequisites or not parsed yet.
ALTER TABLE courses ADD COLUMN IF NOT EXISTS prereq_tree JSONB;
-- Courses that must be taken in the same term (true corequisites only; an
-- optional "or concurrent enrollment" lives on the prereq course node).
ALTER TABLE courses ADD COLUMN IF NOT EXISTS coreq_tree JSONB;
-- {source: rules|llm|legacy, confidence: high|low, hash, parser_version,
--  model, issues[], parsed_at}. `hash` lets reruns skip unchanged text.
ALTER TABLE courses ADD COLUMN IF NOT EXISTS prereq_parse JSONB;

-- Lookups by code (the planner and /courses/prereqs fetch by code).
CREATE INDEX IF NOT EXISTS idx_courses_code ON courses (code);
