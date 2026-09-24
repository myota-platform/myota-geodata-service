-- Production geodata pipeline metadata and least-privilege GIS staging.
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS source_state text NOT NULL DEFAULT 'CURRENT'
  CHECK (source_state IN ('CURRENT','STALE','REVIEW_REQUIRED','RETIRED'));
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS source_key text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS source_hash text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS jurisdiction text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS attachments jsonb NOT NULL DEFAULT '[]'::jsonb;
CREATE INDEX IF NOT EXISTS geodata_entity_source_idx ON geodata_entity (programme_id, source_key, source_state);
CREATE INDEX IF NOT EXISTS geodata_entity_geog_idx ON geodata_entity USING GIST ((geom::geography));

ALTER TABLE import_run ADD COLUMN IF NOT EXISTS programme_id uuid;
ALTER TABLE import_run ADD COLUMN IF NOT EXISTS source_key text;
ALTER TABLE import_run ADD COLUMN IF NOT EXISTS source_hash text;
ALTER TABLE import_run ADD COLUMN IF NOT EXISTS complete_snapshot boolean NOT NULL DEFAULT false;
ALTER TABLE import_run ADD COLUMN IF NOT EXISTS disappearance_policy text NOT NULL DEFAULT 'REVIEW_REQUIRED';
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'import_run_disappearance_policy_ck') THEN
    ALTER TABLE import_run ADD CONSTRAINT import_run_disappearance_policy_ck CHECK (disappearance_policy IN ('UNCHANGED','STALE','RETIRED','REVIEW_REQUIRED'));
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS source_snapshot_manifest (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  import_run_id uuid NOT NULL REFERENCES import_run(id),
  source_key text NOT NULL,
  adapter_code text NOT NULL,
  source_hash text NOT NULL,
  record_count integer NOT NULL DEFAULT 0,
  source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  retrieved_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS source_snapshot_manifest_lookup_idx ON source_snapshot_manifest (source_key, created_at DESC);

CREATE TABLE IF NOT EXISTS source_snapshot_record (
  manifest_id uuid NOT NULL REFERENCES source_snapshot_manifest(id) ON DELETE CASCADE,
  source_record_id text NOT NULL,
  record_hash text NOT NULL,
  PRIMARY KEY (manifest_id, source_record_id)
);

CREATE TABLE IF NOT EXISTS import_schedule (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  programme_id uuid NOT NULL,
  adapter_code text NOT NULL,
  source_key text NOT NULL,
  source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  interval_seconds integer NOT NULL CHECK (interval_seconds >= 300),
  disappearance_policy text NOT NULL DEFAULT 'REVIEW_REQUIRED' CHECK (disappearance_policy IN ('UNCHANGED','STALE','RETIRED','REVIEW_REQUIRED')),
  enabled boolean NOT NULL DEFAULT true,
  last_run_at timestamptz,
  next_run_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS import_schedule_due_idx ON import_schedule (enabled, next_run_at);

ALTER TABLE conflation_candidate ADD COLUMN IF NOT EXISTS survivor_entity_id uuid REFERENCES geodata_entity(id);
ALTER TABLE conflation_candidate ADD COLUMN IF NOT EXISTS resolution_history jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE conflation_candidate ADD COLUMN IF NOT EXISTS resolved_by uuid;
ALTER TABLE conflation_candidate ADD COLUMN IF NOT EXISTS resolved_at timestamptz;

CREATE TABLE IF NOT EXISTS geodata_entity_attachment (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id uuid NOT NULL REFERENCES geodata_entity(id) ON DELETE CASCADE,
  name text NOT NULL,
  media_type text NOT NULL,
  size_bytes bigint NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
  uri text,
  sha256 text,
  license text,
  caption text,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (uri IS NOT NULL OR sha256 IS NOT NULL)
);

-- QGIS may inspect and repair staging geometry, but lifecycle approval remains API-owned.
CREATE TABLE IF NOT EXISTS geodata_edit_staging (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id uuid REFERENCES geodata_entity(id),
  programme_id uuid NOT NULL,
  proposed_name text,
  geom geometry(Geometry, 4326) NOT NULL,
  editor_subject text NOT NULL,
  edit_note text,
  state text NOT NULL DEFAULT 'OPEN' CHECK (state IN ('OPEN','SUBMITTED','DISCARDED')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS geodata_edit_staging_geom_idx ON geodata_edit_staging USING GIST (geom);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'myota_geo_qgis_readonly') THEN
    CREATE ROLE myota_geo_qgis_readonly NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'myota_geo_qgis_editor') THEN
    CREATE ROLE myota_geo_qgis_editor NOLOGIN;
  END IF;
END $$;
GRANT USAGE ON SCHEMA public TO myota_geo_qgis_readonly, myota_geo_qgis_editor;
GRANT SELECT ON qgis_entity_review_queue, qgis_approved_entities TO myota_geo_qgis_readonly;
GRANT SELECT ON qgis_entity_review_queue, qgis_approved_entities TO myota_geo_qgis_editor;
GRANT SELECT, INSERT, UPDATE ON geodata_edit_staging TO myota_geo_qgis_editor;
REVOKE DELETE ON geodata_entity FROM myota_geo_qgis_editor;

DROP VIEW IF EXISTS qgis_entity_review_queue;
DROP VIEW IF EXISTS qgis_approved_entities;
CREATE VIEW qgis_entity_review_queue AS
SELECT e.id, e.programme_id, e.name, e.lifecycle_status, e.source_state, e.jurisdiction, e.geom, e.public_properties,
       s.adapter_code, s.source_uri, s.source_record_id, s.license, s.attribution, s.source_payload
FROM geodata_entity e
LEFT JOIN source_reference s ON s.entity_id = e.id
WHERE e.lifecycle_status IN ('CANDIDATE', 'PROPOSED', 'REJECTED') OR e.source_state IN ('STALE', 'REVIEW_REQUIRED');

CREATE VIEW qgis_approved_entities AS
SELECT id, programme_id, name, geom, public_properties, source_state, jurisdiction
FROM geodata_entity
WHERE lifecycle_status = 'APPROVED';
