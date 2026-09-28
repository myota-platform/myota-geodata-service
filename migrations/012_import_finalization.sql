-- Import-run finalization metadata.
-- A finalized run keeps its summary but discards staged candidate and queue data.
ALTER TABLE import_run
  ADD COLUMN IF NOT EXISTS processed_at timestamptz,
  ADD COLUMN IF NOT EXISTS processed_by text;

CREATE INDEX IF NOT EXISTS import_run_processed_idx
  ON import_run (processed_at);
