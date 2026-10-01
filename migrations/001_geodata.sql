CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Geodata owns its local state, idempotency and outbox infrastructure. These
-- tables are intentionally repeated by service database, not shared through
-- cross-database foreign keys.
CREATE TABLE IF NOT EXISTS service_state (
  service text PRIMARY KEY,
  state jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS idempotency_record (
  service text NOT NULL,
  key text NOT NULL,
  response jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (service, key)
);
CREATE TABLE IF NOT EXISTS outbox_event (
  event_id uuid PRIMARY KEY,
  event_type text NOT NULL,
  producer text NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_id text NOT NULL,
  payload jsonb NOT NULL,
  occurred_at timestamptz NOT NULL,
  available_at timestamptz NOT NULL DEFAULT now(),
  attempts integer NOT NULL DEFAULT 0,
  published_at timestamptz,
  last_error text
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox_event (available_at, occurred_at) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS consumer_checkpoint (
  consumer text PRIMARY KEY,
  last_event_id uuid,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS consumer_processed_event (
  consumer text NOT NULL,
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
CREATE TABLE IF NOT EXISTS dead_letter_event (
  event_id uuid PRIMARY KEY,
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  attempts integer NOT NULL,
  error text,
  dead_lettered_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS entity_type (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  programme_id uuid NOT NULL,
  code text NOT NULL,
  label text NOT NULL,
  geometry_kind text NOT NULL,
  config jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (programme_id, code)
);
CREATE TABLE IF NOT EXISTS geodata_entity (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  programme_id uuid NOT NULL,
  entity_type_id uuid NOT NULL REFERENCES entity_type(id),
  name text NOT NULL,
  lifecycle_status text NOT NULL DEFAULT 'CANDIDATE' CHECK (lifecycle_status IN ('CANDIDATE','APPROVED','REJECTED','RETIRED')),
  geom geometry(Geometry, 4326) NOT NULL,
  centroid geography(Point, 4326),
  public_properties jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS geodata_entity_geom_idx ON geodata_entity USING GIST (geom);
CREATE INDEX IF NOT EXISTS geodata_entity_status_idx ON geodata_entity (programme_id, lifecycle_status);

CREATE TABLE IF NOT EXISTS source_reference (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id uuid NOT NULL REFERENCES geodata_entity(id),
  adapter_code text NOT NULL CHECK (adapter_code IN ('PARKSERVE_US','OSM','GOVERNMENT_GIS','MANUAL')),
  source_uri text,
  source_record_id text,
  license text,
  attribution text,
  retrieved_at timestamptz,
  source_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (adapter_code, source_record_id)
);

CREATE TABLE IF NOT EXISTS import_run (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  adapter_code text NOT NULL,
  source_metadata jsonb NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  stats jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'RUNNING'
);

CREATE TABLE IF NOT EXISTS entity_review (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id uuid NOT NULL REFERENCES geodata_entity(id),
  proposer_id uuid,
  reviewer_id uuid,
  proposal_note text,
  review_note text,
  decision text CHECK (decision IN ('APPROVED','REJECTED')),
  proposed_at timestamptz,
  reviewed_at timestamptz
);

CREATE TABLE IF NOT EXISTS conflation_candidate (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  left_entity_id uuid NOT NULL REFERENCES geodata_entity(id),
  right_entity_id uuid NOT NULL REFERENCES geodata_entity(id),
  similarity numeric NOT NULL,
  reason jsonb NOT NULL DEFAULT '{}'::jsonb,
  resolution text NOT NULL DEFAULT 'OPEN' CHECK (resolution IN ('OPEN','MERGED','KEPT_SEPARATE','IGNORED'))
);
