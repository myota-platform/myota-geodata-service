-- Keep request correlation with the transactional outbox without changing
-- registered domain payloads. Existing rows remain readable with NULL.
ALTER TABLE outbox_event
  ADD COLUMN IF NOT EXISTS correlation_id text;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'outbox_event_correlation_id_length_check'
      AND conrelid = 'outbox_event'::regclass
  ) THEN
    ALTER TABLE outbox_event
      ADD CONSTRAINT outbox_event_correlation_id_length_check
      CHECK (correlation_id IS NULL OR
        (length(correlation_id) BETWEEN 1 AND 128 AND
         correlation_id !~ '[^!-~]'));
  END IF;
END $$;
