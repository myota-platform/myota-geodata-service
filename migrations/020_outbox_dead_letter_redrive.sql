-- Preserve dead-letter evidence while recording operator-authorized redrives.
ALTER TABLE dead_letter_event
  ADD COLUMN IF NOT EXISTS resolved_at timestamptz;

CREATE INDEX IF NOT EXISTS outbox_dead_letter_unresolved_idx
  ON dead_letter_event (dead_lettered_at)
  WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS outbox_redrive_audit (
  id bigserial PRIMARY KEY,
  event_id uuid NOT NULL,
  actor text NOT NULL CHECK (length(actor) BETWEEN 1 AND 120),
  reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 500),
  previous_attempts integer NOT NULL,
  previous_error text,
  requested_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS outbox_redrive_audit_event_idx
  ON outbox_redrive_audit (event_id, requested_at DESC);
