# Geodata migration source

This directory is the canonical source for the current `myota_geo` PostGIS
schema. A new database is provisioned from
[`001_geodata_baseline.sql`](001_geodata_baseline.sql), which captures the
verified deployed catalog: extensions, relational tables, indexes, constraints,
functions, triggers, views, QGIS roles/grants, relational authority, writer
fencing, import recovery, retention, outbox correlation, and current entity
fields. It contains schema only; it does not restore application rows or legacy
JSON state.

The baseline consolidates the prior numbered history (001–022), including
PostGIS/QGIS setup, import and review workflows, relational entity/category
ownership, spatial indexes, cancellation/recovery, outbox redrive, website and
description fields, and event correlation. Exact source files and exclusions
are recorded in the MyOTA
[database baseline inventory](https://github.com/myota-platform/myota-docs/blob/main/docs/operations/databases/baseline-migration-inventory.md).

`myota-deploy/db/migrations/geo/` and `myota-platform/db/migrations/geo/` are
synchronized deployment/bootstrap mirrors. This service repository is
authoritative. CI checks the mirrors byte-for-byte. Future schema changes use
numbered, checksummed migrations after the baseline; never edit an applied
baseline. The runner adopts an already-ready schema and refuses a checksum
mismatch or an unready non-empty database.

The legacy `service_state` row is retained only as an archive; the live service
does not hydrate or persist it. See the
[coordinated relational-authority procedure](https://github.com/myota-platform/myota-docs/blob/main/docs/geodata-phase1-relational-authority.md).

Import recovery

Uploaded and pasted import sources are stored in SeaweedFS before a durable
background run is started. The geodata service claims a PostgreSQL lease,
refreshes its heartbeat while parsing and normalizing, and clears the lease
when the run reaches `PREPROCESSED` or `FAILED`. On startup, queued runs and
processing runs whose lease has expired are requeued from their stored source.
Preprocessing loads existing candidates through the import-run index and
resolves source references through a dedicated entity index. The worker stops
and joins its heartbeat before refreshing the authoritative run row and writing
terminal status, preventing a stale heartbeat from conflicting with cleanup.
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
