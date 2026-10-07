-- Stop live geodata pods reading/writing service_state snapshots. This
-- migration preserves metadata-only legacy resources once, never merges
-- old catalogue/import snapshots over their authoritative relational rows.
SET myota.geodata_writer = 'row-v1';
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS revision bigint NOT NULL DEFAULT 1;
ALTER TABLE geodata_import_processing_queue ADD COLUMN IF NOT EXISTS job_metadata jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE geodata_import_candidate ADD COLUMN IF NOT EXISTS existing_entity_id uuid;
ALTER TABLE geodata_import_candidate ADD COLUMN IF NOT EXISTS processing_queue_id uuid;
CREATE OR REPLACE FUNCTION geodata_bump_revision() RETURNS trigger AS $$
BEGIN
  NEW.revision := OLD.revision + 1;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS geodata_entity_revision ON geodata_entity;
CREATE TRIGGER geodata_entity_revision BEFORE UPDATE ON geodata_entity
FOR EACH ROW EXECUTE FUNCTION geodata_bump_revision();

CREATE TABLE IF NOT EXISTS geodata_control_record (
  kind text NOT NULL CHECK (kind IN ('schedules','conflationCandidates','sourceManifests','entityDeletionJobs')),
  id text NOT NULL,
  payload jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (kind,id)
);
CREATE INDEX IF NOT EXISTS geodata_control_status_idx
  ON geodata_control_record(kind,(payload->>'status'));
CREATE TABLE IF NOT EXISTS geodata_audit_event (
  event_id uuid PRIMARY KEY,
  aggregate_type text NOT NULL,
  aggregate_id text NOT NULL,
  event jsonb NOT NULL,
  occurred_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS geodata_audit_aggregate_idx
  ON geodata_audit_event(aggregate_type,aggregate_id,occurred_at,event_id);
CREATE TABLE IF NOT EXISTS geodata_schema_feature (
  name text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);
DO $once$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('geodata:row-authority-migration',0));
  IF NOT EXISTS (SELECT 1 FROM geodata_schema_feature WHERE name='row_authority_v1') THEN
    UPDATE geodata_import_candidate AS candidate SET existing_entity_id=planned_entity_id
    WHERE existing_entity_id IS NULL AND EXISTS (
      SELECT 1 FROM geodata_entity WHERE id=candidate.planned_entity_id
    );
    UPDATE geodata_import_candidate AS candidate SET validation_status='CONFIRMED',
      processing_queue_id=queue.id
    FROM geodata_import_processing_queue AS queue
    WHERE queue.status IN ('QUEUED','PROCESSING')
      AND queue.candidate_ids ? candidate.id::text
      AND candidate.validation_status='PENDING';
    
    INSERT INTO geodata_control_record(kind,id,payload)
    SELECT namespace.key, record.key, record.value
    FROM service_state AS legacy
    CROSS JOIN LATERAL jsonb_each(coalesce(legacy.state->'data','{}'::jsonb)) AS namespace
    CROSS JOIN LATERAL jsonb_each(CASE WHEN jsonb_typeof(namespace.value)='object'
      THEN namespace.value ELSE '{}'::jsonb END) AS record
    WHERE legacy.service='geodata'
      AND namespace.key IN ('schedules','conflationCandidates','sourceManifests','entityDeletionJobs')
    ON CONFLICT DO NOTHING;
    
    -- Old executor jobs have no durable authorization context. Surface the need
    -- to recreate/reconfirm them, never silently grant permission on recovery.
    UPDATE geodata_control_record SET payload=payload || jsonb_build_object(
      'status','FAILED','error','Legacy deletion must be recreated and confirmed by an administrator')
    WHERE kind='entityDeletionJobs' AND payload->>'status' IN ('QUEUED','PROCESSING')
      AND payload->'executionClaims' IS NULL;
    
    INSERT INTO geodata_audit_event(event_id,aggregate_type,aggregate_id,event,occurred_at)
    SELECT (event->>'eventId')::uuid, event->'aggregate'->>'type',
           event->'aggregate'->>'id', event, (event->>'occurredAt')::timestamptz
    FROM service_state AS legacy
    CROSS JOIN LATERAL jsonb_array_elements(coalesce(legacy.state->'events','[]'::jsonb)) AS event
    WHERE legacy.service='geodata'
    ON CONFLICT DO NOTHING;
    INSERT INTO geodata_schema_feature(name) VALUES ('row_authority_v1');
  END IF;
END $once$;

CREATE INDEX IF NOT EXISTS geodata_entity_catalogue_sort_idx
  ON geodata_entity(lower(name),id);
CREATE INDEX IF NOT EXISTS geodata_entity_lifecycle_idx
  ON geodata_entity(lifecycle_status,lower(name),id);
CREATE INDEX IF NOT EXISTS geodata_category_lookup_idx
  ON geodata_entity_category(lower(category_code),entity_id);
CREATE INDEX IF NOT EXISTS geodata_import_history_idx ON import_run(started_at DESC,id);
CREATE INDEX IF NOT EXISTS geodata_candidate_review_page_idx
  ON geodata_import_candidate(import_run_id,validation_status,ordinal,id);
DO $$
DECLARE field_name text;
BEGIN
  FOREACH field_name IN ARRAY ARRAY['continent','country','region','province','city','municipality'] LOOP
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON geodata_entity '
      || '(lower(coalesce(nullif(public_properties->>%L, ''''), '
      || 'public_properties->''location''->>%L, '''')))',
      'geodata_location_' || field_name || '_idx', field_name, field_name);
  END LOOP;
END $$;

COMMENT ON TABLE service_state IS
  'Legacy geodata snapshot archive. New geodata runtime never reads or writes this table. Stop old writers before migration.';

-- During rollout, fence old snapshot-based API/worker images. They must not
-- overwrite rows while a new image is already serving writes. Migrations and
-- maintenance use the same explicit transaction flag as the new repository.
CREATE OR REPLACE FUNCTION geodata_require_row_writer() RETURNS trigger AS $$
BEGIN
  IF current_setting('myota.geodata_writer',true) IS DISTINCT FROM 'row-v1' THEN
    RAISE EXCEPTION 'Geodata writer is obsolete; deploy the row-authoritative service image'
      USING ERRCODE='55000';
  END IF;
  RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END;
$$ LANGUAGE plpgsql;
DO $$
DECLARE table_name text;
BEGIN
  FOREACH table_name IN ARRAY ARRAY['geodata_entity','import_run','geodata_import_candidate',
      'geodata_import_processing_queue','geodata_control_record','geodata_audit_event'] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS geodata_writer_fence ON %I', table_name);
    EXECUTE format('CREATE TRIGGER geodata_writer_fence BEFORE INSERT OR UPDATE OR DELETE ON %I '
      || 'FOR EACH ROW EXECUTE FUNCTION geodata_require_row_writer()', table_name);
  END LOOP;
END $$;
