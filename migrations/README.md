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
8. `008_entity_category_assignments.sql` adds relational entity-to-category
   assignments, enforces one primary category per entity, and backfills the
   legacy primary category.
9. `009_candidate_lifecycle.sql` removes the legacy `PROPOSED` lifecycle state,
   normalizes old rows to `CANDIDATE`, and records that adapter/import runs and
   community proposals are candidate sources rather than statuses.
10. `010_import_preprocessing.sql` adds the normalized pre-processing records
   and durable projection of the administrator-controlled NATS promotion queue.
11. `011_import_recovery.sql` adds execution attempts, heartbeat/lease
   timestamps, and the last recovery error used to resume abandoned imports
   safely after a service restart.
12. `012_import_finalization.sql` records who finalized an import and when,
   while allowing staged candidate and queue data to be removed after review.

`myota-platform/db/migrations/geo/` and
`myota-deploy/db/migrations/geo/` are synchronized copies used by the
vertical-slice bootstrap and deployment migration runner. When this schema
changes, update this directory first, then copy the complete ordered set to
both repositories and verify the files are byte-for-byte identical.

Shared platform tables such as service state, idempotency, outbox and event
consumer bookkeeping belong to the core migration, not this geodata schema.

Import recovery

Uploaded and pasted import sources are stored in SeaweedFS before a durable
background run is started. The geodata service claims a PostgreSQL lease,
refreshes its heartbeat while parsing and normalizing, and clears the lease
when the run reaches `PREPROCESSED` or `FAILED`. On startup, queued runs and
processing runs whose lease has expired are requeued from their stored source.
Runs without a recoverable source are marked `FAILED` with an explanatory
error instead of remaining indefinitely in `PROCESSING`. The lease duration
defaults to 15 minutes and can be tuned with
`MYOTA_IMPORT_LEASE_SECONDS`; the heartbeat interval is controlled by
`MYOTA_IMPORT_HEARTBEAT_SECONDS`.
