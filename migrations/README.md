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
13. `013_import_retention.sql` indexes finalized and inactive import runs
    eligible for age-based retention cleanup.
14. `014_resumable_uploads.sql` adds user-owned resumable S3 multipart upload
    sessions, per-part checksums, idempotency, and expiry metadata.
15. `015_jetstream_worker_dispatch.sql` adds recoverable promotion leases and
    re-dispatches in-flight imports through JetStream during the migration.
16. `016_relational_authority.sql` adds entity revisions, indexed audit and
    auxiliary control records, promotion-job links, and a one-time legacy
    metadata migration. A writer fence prevents obsolete snapshot writers
    from corrupting authoritative rows. New API/consumer processes wait for
    its feature marker before accepting work. Replaying migrations does not
    re-create deleted legacy resources.
17. `017_import_cancellation.sql` adds durable cancellation-request metadata
    for queued uploads and active preprocessing runs. Active workers observe
    `CANCELLING` at bounded checkpoints and finish as `CANCELLED`; queued work
    is cancelled immediately and staged rows/source artifacts are removed.

`myota-platform/db/migrations/geo/` and
`myota-deploy/db/migrations/geo/` are synchronized copies used by the
vertical-slice bootstrap and deployment migration runner. When this schema
changes, update this directory first, then copy the complete ordered set to
both repositories and verify the files are byte-for-byte identical.

Shared platform tables such as service state, idempotency, outbox and event
consumer bookkeeping are supplied by the shared platform migration runner
in each owning database, not created by this service's schema. Geodata's
legacy `service_state` row is retained only as an archive; the live service
does not hydrate or persist it. See the
[coordinated upgrade procedure](https://github.com/myota-platform/myota-docs/blob/main/docs/geodata-phase1-relational-authority.md).

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

Import retention

The geodata retention worker runs daily and permanently deletes source objects
and import-specific history/log records after 30 days. `PROCESSED` runs age
from `processed_at`; queued, upload-pending, processing, preprocessed,
preprocessed-with-errors, legacy completed, and failed runs age from the latest
of their start, completion, or heartbeat timestamps. Thus active processing is
retained while heartbeats continue, but stalled work and unreviewed pending
imports expire after 30 inactive days. Retention removes source objects,
staged/snapshot records, compatibility run summaries, and published import-run
outbox/consumer logs. It does not delete geodata entities or their provenance.
Object deletion is idempotent; a database cleanup failure leaves the run
eligible for retry.
