CREATE OR REPLACE VIEW qgis_entity_review_queue AS
SELECT e.id, e.programme_id, e.name, e.lifecycle_status, e.geom, e.public_properties,
       s.adapter_code, s.source_uri, s.source_record_id, s.license, s.attribution
FROM geodata_entity e
LEFT JOIN source_reference s ON s.entity_id = e.id
WHERE e.lifecycle_status IN ('CANDIDATE', 'PROPOSED', 'REJECTED');

CREATE OR REPLACE VIEW qgis_approved_entities AS
SELECT id, programme_id, name, geom, public_properties
FROM geodata_entity
WHERE lifecycle_status = 'APPROVED';

