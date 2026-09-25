# Geodata migration source

This directory is the canonical source for the geodata service schema. The
ordered SQL files define the `myota_geo` database:

1. `001_geodata.sql` creates the core PostGIS entity, provenance, review and
   conflation tables.
2. `002_qgis_views.sql` creates the read-only QGIS review views.
3. `003_production_pipeline.sql` adds refresh manifests, schedules,
   disappearance policy, attachments, conflation history and QGIS staging.
4. `004_location_enrichment.sql` adds normalized continent, country,
   subdivision, province, county and city fields plus reverse-geocoding
   provenance and indexes.
5. `005_location_manual_precedence.sql` adds the municipality alias and
   durable manual-location override fields, index, and QGIS view projections.
6. `006_unscoped_imports.sql` makes programme assignment optional for
   imported candidates and adds the category field used by global refresh
   schedules.
7. `007_relational_entity_persistence.sql` adds the nullable cross-service
   programme slug and shared category code columns used by the service's
   relational entity persistence adapter.

`myota-platform/db/migrations/geo/` and
`myota-deploy/db/migrations/geo/` are synchronized copies used by the
vertical-slice bootstrap and deployment migration runner. When this schema
changes, update this directory first, then copy the complete ordered set to
both repositories and verify the files are byte-for-byte identical.

Shared platform tables such as service state, idempotency, outbox and event
consumer bookkeeping belong to the core migration, not this geodata schema.
