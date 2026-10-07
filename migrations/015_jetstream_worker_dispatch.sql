-- Lease promotion jobs and repair dispatch for work accepted before the
-- dedicated JetStream worker became authoritative.
ALTER TABLE geodata_import_processing_queue
  ADD COLUMN IF NOT EXISTS attempt_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz,
  ADD COLUMN IF NOT EXISTS lease_until timestamptz;

CREATE INDEX IF NOT EXISTS geodata_import_queue_recovery_idx
  ON geodata_import_processing_queue (status, lease_until, requested_at);

INSERT INTO outbox_event (
  event_id, event_type, producer, aggregate_type, aggregate_id, payload,
  occurred_at
)
SELECT gen_random_uuid(), 'geodata.import.recovered.v1', 'geodata-service',
       'import_run', run.id::text,
       jsonb_build_object(
         'importRunId', run.id::text,
         'natsSubject', 'myota.geodata.import.preprocess.v1'
       ), now()
FROM import_run AS run
WHERE run.status IN ('QUEUED', 'PROCESSING')
  AND NOT EXISTS (
    SELECT 1 FROM outbox_event AS prior
    WHERE prior.aggregate_type = 'import_run'
      AND prior.aggregate_id = run.id::text
      AND prior.event_type = 'geodata.import.recovered.v1'
  );

INSERT INTO outbox_event (
  event_id, event_type, producer, aggregate_type, aggregate_id, payload,
  occurred_at
)
SELECT gen_random_uuid(), 'geodata.import.processing.recovered.v1',
       'geodata-service', 'import_processing_queue', queue.id::text,
       jsonb_build_object(
         'queueId', queue.id::text,
         'natsSubject', 'myota.geodata.import.process.v1'
       ), now()
FROM geodata_import_processing_queue AS queue
WHERE queue.status IN ('QUEUED', 'PROCESSING')
  AND NOT EXISTS (
    SELECT 1 FROM outbox_event AS prior
    WHERE prior.aggregate_type = 'import_processing_queue'
      AND prior.aggregate_id = queue.id::text
      AND prior.event_type = 'geodata.import.processing.recovered.v1'
  );
