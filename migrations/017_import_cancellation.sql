-- Durable cancellation for queued uploads and active preprocessing runs.
ALTER TABLE import_run
  ADD COLUMN IF NOT EXISTS cancellation_requested_at timestamptz,
  ADD COLUMN IF NOT EXISTS cancellation_requested_by text;

CREATE INDEX IF NOT EXISTS import_run_cancellation_idx
  ON import_run (status, cancellation_requested_at)
  WHERE status IN ('CANCELLING', 'CANCELLED');

CREATE INDEX IF NOT EXISTS import_run_cancelled_retention_idx
  ON import_run (
    GREATEST(started_at,
             COALESCE(heartbeat_at, '-infinity'::timestamptz),
             COALESCE(completed_at, '-infinity'::timestamptz)),
    id
  )
  WHERE status IN ('CANCELLING', 'CANCELLED');

COMMENT ON COLUMN import_run.cancellation_requested_at IS
  'Time an administrator requested cancellation of upload/preprocessing work.';
COMMENT ON COLUMN import_run.cancellation_requested_by IS
  'Authenticated administrator subject that requested import cancellation.';
