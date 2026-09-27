-- Candidate is the single pre-review lifecycle state.
-- Community proposals and adapter/import runs are candidate sources; neither
-- source is a separate lifecycle status.
DO $$
DECLARE
  constraint_name text;
BEGIN
  SELECT con.conname INTO constraint_name
  FROM pg_constraint con
  JOIN pg_class rel ON rel.oid = con.conrelid
  WHERE rel.relname = 'geodata_entity'
    AND con.contype = 'c'
    AND pg_get_constraintdef(con.oid) LIKE '%PROPOSED%'
  LIMIT 1;
  IF constraint_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE geodata_entity DROP CONSTRAINT %I', constraint_name);
  END IF;
END $$;

UPDATE geodata_entity SET lifecycle_status = 'CANDIDATE'
WHERE lifecycle_status = 'PROPOSED';

ALTER TABLE geodata_entity DROP CONSTRAINT IF EXISTS geodata_entity_lifecycle_status_check;
ALTER TABLE geodata_entity
  ADD CONSTRAINT geodata_entity_lifecycle_status_check
  CHECK (lifecycle_status IN ('CANDIDATE','APPROVED','REJECTED','RETIRED'));

DROP VIEW IF EXISTS qgis_entity_review_queue;
CREATE VIEW qgis_entity_review_queue AS
SELECT e.id, e.programme_id, e.name, e.lifecycle_status, e.source_state,
       e.jurisdiction, e.geom, e.public_properties,
       e.continent, e.continent_code, e.country, e.country_code, e.region,
       e.region_code, e.subdivision, e.subdivision_code, e.province,
       e.province_code, e.county, e.county_code, e.city, e.locality,
       s.adapter_code, s.source_uri, s.source_record_id, s.license,
       s.attribution, s.source_payload, e.municipality,
       e.manual_location_fields
FROM geodata_entity e
LEFT JOIN source_reference s ON s.entity_id = e.id
WHERE e.lifecycle_status IN ('CANDIDATE', 'REJECTED')
   OR e.source_state IN ('STALE', 'REVIEW_REQUIRED');
