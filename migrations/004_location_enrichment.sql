-- Reverse-geocoded location metadata is owned by geodata and remains separate
-- from programme policy. Provider payload is retained for provenance/audit.
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS continent text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS continent_code text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS country text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS country_code text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS region text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS region_code text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS subdivision text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS subdivision_code text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS province text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS province_code text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS county text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS county_code text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS city text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS locality text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS geocode_provider text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS geocode_status text NOT NULL DEFAULT 'NOT_CONFIGURED';
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS geocode_lookup_source text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS geocoded_at timestamptz;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS geocode_payload jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS geodata_entity_location_idx
  ON geodata_entity (continent_code, country_code, region_code, province_code, county_code, city);

DROP VIEW IF EXISTS qgis_entity_review_queue;
DROP VIEW IF EXISTS qgis_approved_entities;
CREATE VIEW qgis_entity_review_queue AS
SELECT e.id, e.programme_id, e.name, e.lifecycle_status, e.source_state, e.jurisdiction, e.geom, e.public_properties,
       e.continent, e.continent_code, e.country, e.country_code, e.region, e.region_code,
       e.subdivision, e.subdivision_code,
       e.province, e.province_code, e.county, e.county_code, e.city, e.locality,
       s.adapter_code, s.source_uri, s.source_record_id, s.license, s.attribution, s.source_payload
FROM geodata_entity e
LEFT JOIN source_reference s ON s.entity_id = e.id
WHERE e.lifecycle_status IN ('CANDIDATE', 'PROPOSED', 'REJECTED') OR e.source_state IN ('STALE', 'REVIEW_REQUIRED');

CREATE VIEW qgis_approved_entities AS
SELECT id, programme_id, name, geom, public_properties, source_state, jurisdiction,
       continent, continent_code, country, country_code, region, region_code,
       subdivision, subdivision_code,
       province, province_code, county, county_code, city, locality
FROM geodata_entity
WHERE lifecycle_status = 'APPROVED';
