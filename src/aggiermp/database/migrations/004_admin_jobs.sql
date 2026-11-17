BEGIN;
-- Remember whether this is the first run: the historical backfill below must not
-- re-mark merges as pending every time the migration is re-applied.
CREATE TEMP TABLE _admin_jobs_first_run ON COMMIT DROP AS
 SELECT to_regclass('admin_summary_rebuilds') IS NULL AS fresh;
CREATE TABLE IF NOT EXISTS admin_summary_rebuilds (
 professor_id text PRIMARY KEY REFERENCES professors(id) ON DELETE CASCADE,
 revision bigint NOT NULL DEFAULT 1,
 reason text NOT NULL DEFAULT 'Professor merge changed',
 changed_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS admin_summary_jobs (
 id uuid PRIMARY KEY,
 status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','succeeded','failed','quota_exhausted')),
 professor_ids jsonb NOT NULL,
 completed_ids jsonb NOT NULL DEFAULT '[]',
 failed_ids jsonb NOT NULL DEFAULT '[]',
 logs jsonb NOT NULL DEFAULT '[]',
 error text,
 created_at timestamptz NOT NULL DEFAULT now(),
 started_at timestamptz,
 finished_at timestamptz
);
CREATE INDEX IF NOT EXISTS admin_summary_jobs_pending ON admin_summary_jobs(created_at) WHERE status IN ('queued','running');
CREATE TABLE IF NOT EXISTS admin_worker_heartbeats (
 worker_id text PRIMARY KEY,
 heartbeat_at timestamptz NOT NULL DEFAULT now()
);
CREATE OR REPLACE FUNCTION mark_merge_summaries_stale() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP = 'DELETE' THEN
   -- Removing a confirmed merge is an undo: both professors need a rebuild.
   IF OLD.status = 'confirmed' THEN
     INSERT INTO admin_summary_rebuilds(professor_id) VALUES (OLD.canonical_id)
     ON CONFLICT(professor_id) DO UPDATE SET revision=admin_summary_rebuilds.revision+1, changed_at=now();
     INSERT INTO admin_summary_rebuilds(professor_id) VALUES (OLD.alias_id)
     ON CONFLICT(professor_id) DO UPDATE SET revision=admin_summary_rebuilds.revision+1, changed_at=now();
   END IF;
   RETURN OLD;
 END IF;
 IF TG_OP = 'UPDATE' AND OLD.status = NEW.status AND OLD.canonical_id = NEW.canonical_id THEN RETURN NEW; END IF;
 IF NEW.status = 'confirmed' OR (TG_OP = 'UPDATE' AND OLD.status = 'confirmed') THEN
   INSERT INTO admin_summary_rebuilds(professor_id) VALUES (NEW.canonical_id)
   ON CONFLICT(professor_id) DO UPDATE SET revision=admin_summary_rebuilds.revision+1, changed_at=now();
   IF TG_OP = 'UPDATE' AND OLD.status = 'confirmed' THEN
     INSERT INTO admin_summary_rebuilds(professor_id) VALUES (OLD.alias_id)
     ON CONFLICT(professor_id) DO UPDATE SET revision=admin_summary_rebuilds.revision+1, changed_at=now();
     IF OLD.canonical_id <> NEW.canonical_id THEN
       INSERT INTO admin_summary_rebuilds(professor_id) VALUES (OLD.canonical_id)
       ON CONFLICT(professor_id) DO UPDATE SET revision=admin_summary_rebuilds.revision+1, changed_at=now();
     END IF;
   END IF;
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS admin_merge_summary_stale ON professor_aliases;
CREATE TRIGGER admin_merge_summary_stale AFTER INSERT OR UPDATE OR DELETE ON professor_aliases
 FOR EACH ROW EXECUTE FUNCTION mark_merge_summaries_stale();
-- Conservatively include historical reviewed merges; safe to rebuild once.
INSERT INTO admin_summary_rebuilds(professor_id)
 SELECT x.professor_id FROM (
  SELECT canonical_id AS professor_id FROM professor_aliases WHERE status IN ('confirmed','rejected')
  UNION SELECT alias_id FROM professor_aliases WHERE status='rejected'
 ) x WHERE (SELECT fresh FROM _admin_jobs_first_run)
 ON CONFLICT DO NOTHING;
COMMIT;
