-- Geodata service persistence boundary.
--
-- The service historically kept a compatibility JSON snapshot in service_state.
-- The catalogue itself is now also stored in these PostGIS tables. Programme
-- slugs and shared category codes are retained because these are cross-service
-- identifiers rather than local foreign keys.
ALTER TABLE entity_type ALTER COLUMN programme_id DROP NOT NULL;
ALTER TABLE geodata_entity ALTER COLUMN programme_id DROP NOT NULL;
ALTER TABLE geodata_entity ALTER COLUMN entity_type_id DROP NOT NULL;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS programme_slug text;
ALTER TABLE geodata_entity ADD COLUMN IF NOT EXISTS entity_type_code text;
CREATE INDEX IF NOT EXISTS geodata_entity_programme_slug_idx ON geodata_entity (programme_slug, lifecycle_status);
CREATE INDEX IF NOT EXISTS geodata_entity_type_code_idx ON geodata_entity (entity_type_code);
