-- Restart-safe import execution metadata.
--
-- Source documents remain in object storage and these fields allow a service
-- instance to distinguish an active lease from work abandoned by a restart.
ALTER TABLE import_run
  ADD COLUMN IF NOT EXISTS attempt_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz,
  ADD COLUMN IF NOT EXISTS lease_until timestamptz,
  ADD COLUMN IF NOT EXISTS last_error text;

CREATE INDEX IF NOT EXISTS import_run_recovery_idx
  ON import_run (status, lease_until, started_at);

COMMENT ON COLUMN import_run.lease_until IS
  'Execution lease expiry. A queued or processing run can be recovered after this time.';
