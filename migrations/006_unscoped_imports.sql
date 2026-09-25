-- Geodata intake is a platform-wide catalogue operation. Programme
-- assignment remains a separate eligibility concern and is optional while an
-- imported candidate is being reviewed.
ALTER TABLE entity_type ALTER COLUMN programme_id DROP NOT NULL;
ALTER TABLE geodata_entity ALTER COLUMN programme_id DROP NOT NULL;
ALTER TABLE import_run ALTER COLUMN programme_id DROP NOT NULL;
ALTER TABLE import_schedule ALTER COLUMN programme_id DROP NOT NULL;
ALTER TABLE import_schedule ADD COLUMN IF NOT EXISTS entity_type_code text;
ALTER TABLE geodata_edit_staging ALTER COLUMN programme_id DROP NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS entity_type_global_code_uidx
  ON entity_type (code)
  WHERE programme_id IS NULL;

COMMENT ON COLUMN geodata_entity.programme_id IS
  'Optional programme assignment. Imports create platform-wide candidates; programme eligibility is assigned separately.';
