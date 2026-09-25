-- Extend reverse-geocoded location metadata with an explicit municipality
-- alias and durable manual-override markers. This migration is synchronized
-- byte-for-byte into the platform and deployment repositories.
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS municipality text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS manual_location_fields jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS manual_location_updated_at timestamptz;

CREATE INDEX IF NOT EXISTS geodata_entity_manual_location_fields_gin_idx
  ON geodata_entity USING gin (manual_location_fields);

CREATE OR REPLACE VIEW qgis_entity_review_queue AS
SELECT e.id, e.programme_id, e.name, e.lifecycle_status, e.source_state, e.jurisdiction, e.geom, e.public_properties,
       e.continent, e.continent_code, e.country, e.country_code, e.region, e.region_code,
       e.subdivision, e.subdivision_code,
       e.province, e.province_code, e.county, e.county_code, e.city, e.locality,
       s.adapter_code, s.source_uri, s.source_record_id, s.license, s.attribution, s.source_payload,
       e.municipality, e.manual_location_fields
FROM geodata_entity e
LEFT JOIN source_reference s ON s.entity_id = e.id
WHERE e.lifecycle_status IN ('CANDIDATE', 'PROPOSED', 'REJECTED') OR e.source_state IN ('STALE', 'REVIEW_REQUIRED');

CREATE OR REPLACE VIEW qgis_approved_entities AS
SELECT id, programme_id, name, geom, public_properties, source_state, jurisdiction,
       continent, continent_code, country, country_code, region, region_code,
       subdivision, subdivision_code,
       province, province_code, county, county_code, city, locality,
       municipality, manual_location_fields
FROM geodata_entity
WHERE lifecycle_status = 'APPROVED';
