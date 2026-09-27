-- Import safety boundary.
--
-- An import is first normalized into reviewable records. Those records are
-- deliberately separate from geodata_entity until an administrator confirms
-- them and queues promotion to CANDIDATE or APPROVED.
CREATE TABLE IF NOT EXISTS geodata_import_candidate (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    import_run_id uuid NOT NULL REFERENCES import_run(id) ON DELETE CASCADE,
    ordinal integer NOT NULL,
    planned_entity_id uuid,
    programme_slug text,
    entity_type_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
    name text NOT NULL,
    geom geometry(Geometry, 4326) NOT NULL,
    candidate_source jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_ref text,
    source_hash text,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    entity_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    validation_status text NOT NULL DEFAULT 'PENDING'
      CHECK (validation_status IN ('PENDING', 'CONFIRMED', 'REJECTED', 'PROCESSED')),
    validation_note text,
    validated_by text,
    validated_at timestamptz,
    target_status text
      CHECK (target_status IN ('CANDIDATE', 'APPROVED')),
    processed_entity_id uuid,
    processed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (import_run_id, ordinal)
);

CREATE INDEX IF NOT EXISTS geodata_import_candidate_run_idx
  ON geodata_import_candidate (import_run_id, ordinal);
CREATE INDEX IF NOT EXISTS geodata_import_candidate_validation_idx
  ON geodata_import_candidate (validation_status, import_run_id);
CREATE INDEX IF NOT EXISTS geodata_import_candidate_geom_idx
  ON geodata_import_candidate USING GIST (geom);

CREATE TABLE IF NOT EXISTS geodata_import_processing_queue (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    import_run_id uuid NOT NULL REFERENCES import_run(id) ON DELETE CASCADE,
    candidate_ids jsonb NOT NULL,
    target_status text NOT NULL CHECK (target_status IN ('CANDIDATE', 'APPROVED')),
    requested_by text NOT NULL,
    status text NOT NULL DEFAULT 'QUEUED'
      CHECK (status IN ('QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED')),
    result jsonb NOT NULL DEFAULT '{}'::jsonb,
    error text,
    requested_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz
);

CREATE INDEX IF NOT EXISTS geodata_import_processing_queue_status_idx
  ON geodata_import_processing_queue (status, requested_at);

COMMENT ON TABLE geodata_import_candidate IS
  'Normalized import records awaiting explicit administrator validation and promotion.';
COMMENT ON TABLE geodata_import_processing_queue IS
  'Durable projection of the NATS-backed import promotion queue.';
