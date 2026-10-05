-- Retention scans are restricted to explicitly finalized imports.
CREATE INDEX IF NOT EXISTS import_run_retention_due_idx
  ON import_run (processed_at, id)
  WHERE status = 'PROCESSED' AND processed_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS import_run_stale_retention_idx
  ON import_run (
    GREATEST(started_at,
             COALESCE(heartbeat_at, '-infinity'::timestamptz),
             COALESCE(completed_at, '-infinity'::timestamptz)),
    id
  )
  WHERE status IN ('UPLOAD_PENDING', 'QUEUED', 'PROCESSING', 'PREPROCESSED',
                   'PREPROCESSED_WITH_ERRORS', 'COMPLETED',
                   'COMPLETED_WITH_ERRORS', 'FAILED');
