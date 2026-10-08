-- Keep preprocessing lookups proportional to the current import rather than
-- repeatedly materializing the entire candidate/entity catalogues.
CREATE INDEX CONCURRENTLY IF NOT EXISTS geodata_entity_source_ref_lookup_idx
  ON geodata_entity (programme_slug, (public_properties->>'sourceRef'))
  WHERE public_properties ? 'sourceRef';

COMMENT ON INDEX geodata_entity_source_ref_lookup_idx IS
  'Supports source-reference conflation without scanning the full entity catalogue.';
