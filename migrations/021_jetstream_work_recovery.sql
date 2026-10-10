-- Track the last durable dispatch repair so lost/expired work is re-enqueued
-- at a bounded rate without making reconcilers a second job executor.
ALTER TABLE import_run
  ADD COLUMN IF NOT EXISTS work_dispatched_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE geodata_import_processing_queue
  ADD COLUMN IF NOT EXISTS work_dispatched_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE geodata_entity
  ADD COLUMN IF NOT EXISTS location_work_dispatched_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS import_run_work_recovery_idx
  ON import_run (status, work_dispatched_at, lease_until)
  WHERE status IN ('QUEUED', 'PROCESSING');

CREATE INDEX IF NOT EXISTS import_queue_work_recovery_idx
  ON geodata_import_processing_queue (status, work_dispatched_at, lease_until)
  WHERE status IN ('QUEUED', 'PROCESSING');

CREATE INDEX IF NOT EXISTS geodata_location_work_recovery_idx
  ON geodata_entity (location_work_dispatched_at, id)
  WHERE public_properties->>'locationEnrichmentStatus' = 'QUEUED';
