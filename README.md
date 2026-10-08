# MyOTA geodata service

MyOTA is a programme-agnostic platform for outdoor activation programmes. MPOTA is represented as a configured programme, not as the platform itself. No rules or charter text are copied from POTA or any other programme: every programme supplies its own configuration, policy, eligibility, awards and public charter.

This repository owns the platform-wide geospatial catalogue and its
candidate-to-approved lifecycle. It owns PostGIS entities, geometries, source
provenance, import runs, adapter/conflation metadata, review and audit
records, location enrichment, and entity-category assignments. Imports are
programme-independent and first enter durable pre-processing; administrator
promotion chooses CANDIDATE or APPROVED while programme eligibility remains a
separate decision.

## What works now

- Phase 1 geodata state is database-authoritative: request-scoped row
  projections, transactional changes/audit/outbox, durable idempotency,
  indexed catalogue queries, and optimistic entity revisions. No live API or
  worker reads or rewrites the archived whole-service JSON snapshot.
- Entity deletion jobs are consumed by the durable
  `geodata-entity-deletion-v1` JetStream consumer, not an API-local executor.
  The worker also reconciles confirmed `QUEUED` and lease-expired
  `PROCESSING` jobs from PostgreSQL, so an acknowledged or unavailable broker
  event cannot strand a permanent deletion. A missing job is a processing
  failure, never a successful no-op.
- Migration 016 is a coordinated upgrade: old writers are fenced, and new API
  and consumer processes wait for its completion before accepting work.
  Follow the [Phase 1 upgrade and evidence record](https://github.com/myota-platform/myota-docs/blob/main/docs/geodata-phase1-relational-authority.md).
- The identity, programme, activity, public-web, admin-web, and deployment
  boundaries are documented in the
  [repository map](https://github.com/myota-platform/myota-docs/blob/main/docs/repository-map.md).
- Geodata lifecycle: adapter/import run or community proposal → CANDIDATE → approver review → APPROVED or REJECTED; approved entities may only be RETIRED.
- GeoJSON Point, Polygon/MultiPolygon, and LineString trail/way geometry; OSM-style `type: "way"` records are normalized to LineString.
- Provenance-aware imports with adapter metadata for ParkServe, OSM, government GIS and manual proposals.
- A dedicated two-stage intake supports pasted GeoJSON/KML/GPX/WFS/ArcGIS JSON and uploaded Shapefile, OSM PBF and ParkServe payloads. Uploads are scanned, stored in SeaweedFS through its S3-compatible API, and emit durable queue events. Parsing and normalization stop at `PREPROCESSED`; no entity is created until an administrator validates selected records.
- Reverse-geocoded entity location fields: continent/country, ISO codes, first
  country subdivision, optional province/county, and city/municipality.
- Automatically calculated Maidenhead coverage is returned as sorted,
  read-only `maidenheadGridSquares4` and `maidenheadLocators6` arrays. Points
  have one canonical cell; lines and areas include every cell they intersect.
  The database migration backfills existing entities and a geometry trigger
  keeps both arrays current. See the [Maidenhead field
  reference](https://github.com/myota-platform/myota-docs/blob/main/docs/geodata-maidenhead-locators.md).
- Imports retain source-provided administrative location fields. When a source
  supplies a country code and location metadata, the entity is marked
  `geocodeStatus=SOURCE_DATA`; normal enrichment does not make a redundant
  remote lookup. Explicitly refreshed BigDataCloud results and manually edited
  fields keep their existing precedence rules.
- Lifecycle transitions are API-owned and audited; QGIS is a controlled
  graphical editing tool, not an approval bypass.
- A daily retention worker deletes source objects and import history/logs
  after 30 days. Finalized runs age from finalization; pending, failed, and
  stalled runs age from their latest activity. Active heartbeat updates protect
  ongoing processing; promoted entities and provenance remain.
- Geodata source uploads and recovery snapshots use the dedicated
  `myota-geodata-imports` object-storage bucket. The 30-day cleanup is scoped
  only to that bucket and never deletes ADIF logs, award assets, signatures,
  or issued certificates; configure its name with
  `MYOTA_GEODATA_IMPORT_BUCKET`.
- Phase 2 resource APIs are available for consolidated imports, proposals,
  metadata, categories, geometry, reviews, bbox-filtered entity collections,
  and confirmed cross-service deletion jobs. Existing action routes remain
  aliases with deprecation headers; see the
  [Phase 2 resource model](https://github.com/myota-platform/myota-docs/blob/main/docs/api-phase2-geodata-resource-model.md).

Unit tests may use an in-memory adapter. Durable Compose/Kubernetes operation
uses PostgreSQL/PostGIS and SeaweedFS through myota-deploy. The remaining
large-scale source workers, public data publication, stewardship workflow, and
production operations are tracked in the
[charter gap analysis](https://github.com/myota-platform/myota-docs/blob/main/docs/charter-gap-analysis.md).

Database concurrency tests require `GEO_TEST_DATABASE_URL` pointing to an
isolated database whose name ends in `_tests`, with all migrations applied.
They refuse other database names. The image workflow also starts two API
processes and verifies a concurrent same-revision edit returns one success
and one conflict. This establishes Phase 1 correctness, not completion of
the later streaming, memory-capacity, and rollout qualification gates.

The `/metrics` endpoint exposes durable entity, geometry, category, import,
pre-processing, PostgreSQL pool/activity, lock-wait and outbox metrics, plus
timed PostGIS bounding-box and entity-upsert queries and broker-sourced
JetStream consumer backlog. With
`MYOTA_OTEL_ENABLED=1`, HTTP request rate/latency, active requests, request body
size, and per-process CPU/memory telemetry are exported to the OpenTelemetry
Collector.

### Read-only load baseline (Grafana k6)

`loadtests/geodata-baseline.js` is a small, read-only HTTP baseline for the
production API. It samples up to 10 public entity records during k6 `setup`,
then exercises health, paged catalogue, bounding-box catalogue, and entity
detail reads. The sample IDs and bounding boxes exist only in the k6 process
memory. If the catalogue is empty, it explicitly reports that map and detail
requests are skipped and still measures health and paged catalogue reads. The
test itself does not create entities/imports, upload files, or write any
application data. It does produce ordinary request telemetry in production,
which is intentionally retained for operations.

Install the external runner on macOS with Homebrew (`brew install k6`), as
documented by [Grafana k6](https://grafana.com/docs/k6/latest/set-up/install-k6/).
Run the conservative defaults (2 virtual users, 60 seconds):

```bash
MYOTA_ALLOW_PRODUCTION=YES k6 run loadtests/geodata-baseline.js
```

The target defaults to `https://api.myota.top`. The current project policy
designates the live K3s deployment on `spainip.es` as provisional production
and the required target for load/performance qualification. Non-production
targets remain available for development checks, not capacity qualification.
Production requires the explicit acknowledgement above. The script caps runs
at 50 virtual users and 5 minutes, and
paces requests to at most about 2 per second per user. Tune
`MYOTA_LOAD_TEST_VUS` (1–50) and `MYOTA_LOAD_TEST_DURATION` (1–300 seconds or
1–5 minutes) only in coordination with the operator. The workload is closed
loop and read-only; it is not a large-file upload test or a benchmark of
synthetic database cardinality. Production results and remaining evidence
gates are documented in the [Phase 0 evidence record](https://github.com/myota-platform/myota-docs/blob/main/docs/geodata-phase0-production-evidence-2026-10-08.md).

View service, request-size, process, PostgreSQL-pool, import, and outbox metrics
in the provisioned **MyOTA Geodata capacity baseline** Grafana dashboard.

### Permanent Sevilla scale fixtures

`loadtests/provision_scale_fixtures.py` sends 10,000 synthetic point records
through the authenticated public APIs in four 2,500-feature imports. For each
freshly processed import it promotes exactly 5%: 2.5% to `CANDIDATE` and 2.5%
to `APPROVED`; the remaining 95% are rejected from the preprocessing queue.
Because 2.5% of 2,500 is fractional, the four imports alternate 62/63 records
per status, yielding exactly 250 candidates and 250 approved entities overall.
It creates the dedicated `SCALE_TEST_FIXTURE` point category, keeps promoted
entities unassigned to every programme, uses deterministic source references,
and stores generated location metadata with source provenance. These are
explicitly synthetic coordinates, not parks or programme-eligible entities.
The script uses no load-test cleanup tag and intentionally has no delete
operation. It refuses the wrong API hostname and requires separate production
and permanent-data acknowledgements.

The first 2,500-record import in the current deployment had already been
queued for full approval before the 5% sampling change. Approved entities are
immutable except for retirement, so the provisioner recognizes that exact
legacy prefix and resumes at the next import rather than attempting a downgrade
or duplicate. The remaining three imports use the 5% split. The current
deployed fixture distribution therefore differs from a fresh run and is
recorded explicitly in the Phase 0 evidence.

Run it once from a trusted workstation or the K3s host only after confirming
that retaining the imported records and promoted scale fixtures is intended. Do not
put the password in shell history or command arguments; on the host, read it
from the already-provisioned protected file without printing it:

```bash
export MYOTA_API_BASE_URL=https://api.myota.top
export MYOTA_LOAD_TEST_EMAIL=demo@example.test
export MYOTA_SCALE_FIXTURES_ALLOW_PRODUCTION=YES
export MYOTA_SCALE_FIXTURES_PERMANENT=YES
export MYOTA_LOAD_TEST_PASSWORD="$(< /root/4test)"
python3 loadtests/provision_scale_fixtures.py
unset MYOTA_LOAD_TEST_PASSWORD
```

The importer sends four 2,500-feature batches, waits for preprocessing and
promotion, verifies the catalogue count after each batch, then finalizes each
import so rejected and processed staging records are discarded. Promoted
fixtures are not test-run rows and will not be removed by normal load-test
cleanup or import-object retention. Record their category and source prefix
when an administrator later removes them through the API.

### Write and worker profiles

`loadtests/geodata-workloads.js` separates five mutating profiles: `large-upload`
(one resumable upload session per VU), `simultaneous-edits` (concurrent edits to a
test-owned entity), `preprocessing` (imports left in the validation queue),
`promotion` (validation and candidate promotion), and `queue-backlog` (steady
accepted import submissions without waiting for workers). They can run in
development, test, staging, or production with the corresponding explicit
acknowledgement. Under current policy, `api.myota.top` is the required
capacity-qualification target; local/non-production runs are development
checks only. The runner caps duration at 10 minutes and VUs at 50 in both
production and non-production, features per
import at 100 (50 for queue backlog; 40 for promotion; 5,000 for uploads), and
submissions at five per VU (30 for the steady backlog profile; two for
promotion). Each 40-record promotion import promotes one record to Candidate
and one to Approved (2.5% each); the remaining 95% are rejected from staging.
Promotion feature overrides must remain divisible by 40 to preserve that exact
split. Large uploads run once per VU
and default to 2,500 features with 1 KiB
of synthetic payload padding per feature (roughly 3–4 MiB per file); padding is
capped at 4 KiB per feature so the largest generated file stays around 23 MiB.
The upload profile uses `POST /v1/geodata/import-uploads`, raw checksum-verified
parts, and session completion; it does not use the disabled single-request
`/imports/upload` endpoint. Parts are bounded at 16 MiB and completion reads
the nested `importRun.id`. Failed transfers abort their session, while a lost
completion response is reconciled with the session's durable status. There is
exactly one real upload iteration per VU; duration is its maximum time budget,
not a period filled with idle iterations. Each failed stage prints sanitized
HTTP problem details and request/correlation IDs.

Install k6 on macOS with `brew install k6`. Use a dedicated, least-use
global-admin account for each target environment; never reuse development
credentials in production. Non-production requires an exact allowlist for
remote hosts plus `MYOTA_LOAD_TEST_ALLOW_NONPROD=YES`. Production additionally
requires `MYOTA_ENV=production`, `MYOTA_LOAD_TEST_ALLOW_PRODUCTION=YES`, and an
exact `MYOTA_LOAD_TEST_PRODUCTION_HOSTS` hostname match. No production run is
started automatically by the script. The same hard per-run limits apply in
production; remove neither the limits nor cleanup safeguards.
For example, with the local stack running:

```bash
MYOTA_ENV=development \
MYOTA_LOAD_TEST_ALLOW_NONPROD=YES \
MYOTA_BASE_URL=http://localhost:8090 \
MYOTA_LOAD_TEST_EMAIL="$MYOTA_TEST_ADMIN_EMAIL" \
MYOTA_LOAD_TEST_PASSWORD="$MYOTA_TEST_ADMIN_PASSWORD" \
MYOTA_LOAD_TEST_PROFILE=preprocessing \
k6 run loadtests/geodata-workloads.js
```

To explicitly target production, use the production gateway hostname and a
dedicated production test administrator:

```bash
MYOTA_ENV=production \
MYOTA_LOAD_TEST_ALLOW_PRODUCTION=YES \
MYOTA_LOAD_TEST_PRODUCTION_HOSTS=api.myota.top \
MYOTA_BASE_URL=https://api.myota.top \
MYOTA_LOAD_TEST_EMAIL="$MYOTA_PRODUCTION_TEST_ADMIN_EMAIL" \
MYOTA_LOAD_TEST_PASSWORD="$MYOTA_PRODUCTION_TEST_ADMIN_PASSWORD" \
MYOTA_LOAD_TEST_PROFILE=simultaneous-edits \
k6 run loadtests/geodata-workloads.js
```

Before production writes, enable cleanup in Helm only for the planned test
window with `geodataLoadTestCleanup.enabled=true` and
`geodataLoadTestCleanup.allowProductionCleanup=true` while
`auth.environment=production`. Confirm rollout before the test. After teardown
confirms cleanup, disable both values and redeploy immediately. Cleanup
requires the identity service's global-administrator role (`GLOBAL_OPERATOR`;
legacy `GLOBAL_ADMIN` tokens are also accepted), exact per-run confirmation,
and refuses fixtures with activation, QSO, or award-progress records. If teardown fails, stop further
write tests and resolve cleanup first.

Set `MYOTA_LOAD_TEST_PROFILE` to `large-upload`, `simultaneous-edits`,
`preprocessing`, `promotion`, or `queue-backlog`. The `cleanup-only` recovery
mode removes a specified run without starting a workload. Optional controls are
`MYOTA_LOAD_TEST_VUS`, `MYOTA_LOAD_TEST_DURATION`, `MYOTA_LOAD_TEST_FEATURES`,
`MYOTA_LOAD_TEST_PADDING_BYTES`, and `MYOTA_LOAD_TEST_IMPORTS_PER_VU`; all are
bounded by the harness. Set `MYOTA_LOAD_TEST_ALLOWED_HOSTS` only for an
approved non-production staging hostname; production uses the separate exact
`MYOTA_LOAD_TEST_PRODUCTION_HOSTS` allowlist. Do not run profiles concurrently
against one environment.

After an accepted import, the status poll may briefly receive HTTP 404 while
the asynchronous upload handoff publishes its durable run record. The harness
counts only 200 and this specific polling 404 as expected; the 404 is retried
until the run appears, while other error statuses still count against
`http_req_failed`. If polling times out, the error reports the last HTTP status
and sanitized API details.

Production pass/fail thresholds are evaluated after the configured workload
finishes instead of aborting it on an early latency spike. This lets accepted
imports drain and gives teardown a chance to clean every fixture; threshold
failures are still reported in the final k6 result. Non-production runs retain
early abort behavior.

Every run receives a unique fixture tag and the k6 teardown calls
`DELETE /v1/geodata/load-test-runs/{testRunId}`. Local Compose explicitly enables
this endpoint; it additionally requires a global administrator and exact
confirmation. Cleanup refuses active import/promotion jobs and refuses to
delete any fixture entity with linked activations, QSOs, or award progress. It
removes the run's source objects, terminal resumable sessions and their part
records, staged candidates, queue rows, audit/outbox
events, imports, and created entities. If k6 is interrupted, run cleanup with
the same dedicated account and exact confirmation before repeating the test.
Helm does not enable cleanup by default; operators must deliberately enable it
only for the duration of a test.

To recover a failed teardown, do not start another workload. Run the harness
with `MYOTA_LOAD_TEST_PROFILE=cleanup-only` and the failed run's exact
`MYOTA_LOAD_TEST_RUN_ID`; this performs cleanup only. If cleanup still fails,
the harness reports the sanitized API problem detail and correlation ID.
While an import or promotion is still active, cleanup retries the specific
pending response without counting it as a failed workload request; other
client errors fail immediately. k6 allows up to six minutes for setup-only
recovery and teardown cleanup. If an import submission is not accepted, the
harness prints its HTTP status and a bounded, credential-redacted API problem
response (including request/correlation IDs when supplied). Cleanup retry waits
also report the safe API detail and attempt number; use those diagnostics to
identify backend or worker failures rather than relaxing the acceptance or
request-failure thresholds. Active upload sessions are also refused and retried;
after an interrupted process, abort only the upload IDs logged for that exact
test run through the user-bound upload DELETE API before cleanup. Terminal
session cleanup reports `uploadSessionsDeleted` and does not delete another
run's sessions or delete the same source object twice.

Upload-harness regressions need no production credentials or network access:

```bash
node --experimental-vm-modules --test loadtests/tests/geodata-workloads.test.mjs
```

With k6 installed, this transport smoke test starts an ephemeral localhost-only
protocol fixture, transfers an 8.46 MB synthetic document in two parts and
asserts teardown removed all fixture sessions. It is not a production or
SeaweedFS performance qualification:

```bash
node loadtests/tests/k6-smoke.mjs
```

CI gates image publishing on upload-harness regressions and the Python suite,
including terminal-session cleanup with foreign-key part cascades in an
isolated `*_tests` database. See the
[verification record and recovery guidance](https://github.com/myota-platform/myota-docs/blob/main/docs/geodata-load-test-upload-verification.md).

For example, reuse the production target and credentials from the production
command above, replacing its profile and adding the failed run ID:

```bash
MYOTA_LOAD_TEST_PROFILE=cleanup-only \
MYOTA_LOAD_TEST_RUN_ID=lt-20261006123955-simultaneous-edits \
k6 run loadtests/geodata-workloads.js
```

Do not launch multiple profiles at once against the same fixture environment:
cleanup is scoped by run ID but writes still contend on the shared database,
object store, and worker capacity. The workload profiles are bounded diagnostic
loads, not a production capacity guarantee.

### Query-plan and JetStream evidence

The PostGIS bounding-box API query and relational entity-upsert path emit
`myota_geodata_postgis_query_duration_seconds` observations labeled by the
low-cardinality `query` name, and increment
`myota_geodata_slow_queries_total` when execution exceeds
`MYOTA_SLOW_QUERY_THRESHOLD_MS` (250 ms by default). API request latency remains
a separate outer measurement.

For an inspectable query plan, run the read-only evidence tool against a
development, test, or staging database. It refuses production-like hostnames,
sets the PostgreSQL session read-only, limits the query window/result count, and
captures `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`, database version, row count,
and available GiST index definitions:

```bash
MYOTA_ENV=development \
MYOTA_ALLOW_EXPLAIN_ANALYZE=YES \
GEO_DATABASE_URL="$MYOTA_NONPROD_GEO_DATABASE_URL" \
python3 scripts/geodata-query-plan-evidence.py \
  --bbox=-5.99,37.37,-5.90,37.43 \
  --output /tmp/myota-postgis-query-plan.json
```

The evidence file may include schema/index information and should be reviewed
before sharing. Grafana's **MyOTA JetStream backlog and PostGIS query
performance** dashboard separates broker consumer pending and ack-pending
counts, redeliveries, oldest outstanding message age, poller health, PostGIS
query percentiles, and slow-query rate. JetStream values are polled directly
from NATS every 15 seconds; age is marked unavailable rather than guessed if a
message was purged or does not match the consumer's configured subject filter.
The detailed workflow and evidence handling are in the
[organization runbook](https://github.com/myota-platform/myota-docs/blob/main/docs/geodata-load-test-and-query-evidence.md).

## Run the vertical slice

```bash
python3 -m unittest discover -s tests -v
python3 services/dev_server.py
```

Open <http://127.0.0.1:8080>. The dev server starts the four services on ports 8001–8004 and proxies the browser API calls. It is intentionally dependency-free.

For a containerized PostGIS environment, use `docker compose up --build` after starting Colima. The image uses the same service code with `SERVICE=identity|programmes|geodata|activity`.

## Reverse geocoding

Server-side entity materialization and geometry changes use BigDataCloud's
Reverse Geocoding to City API. The service calls
`https://api-bdc.net/data/reverse-geocode` asynchronously from the durable
geodata worker with the persisted geometry centroid, keeps normalized values on
the entity, and preserves the provider response under
`provenance.reverseGeocoding`. Preprocessing does not call the provider.

Copy `.env.example` to `.env` for local development and set
`BIGDATACLOUD_API_KEY`. The real `.env` is ignored and must never be committed.
Deployments should inject the key through a secret. The client-side free
endpoint is intentionally not used because imports and stored entity
coordinates are server-side/batch operations.

The stable entity fields are `continent`, `continentCode`, `country`,
`countryCode`, `region`, `regionCode`, `subdivision`, `subdivisionCode`,
`province`, `provinceCode`, `county`, `countyCode`, `city`, `municipality`, and
`locality`. `regionCode` and `subdivisionCode` identify the provider's first
administrative subdivision after the country (`principalSubdivisionCode`).
Province and county are populated only when the provider supplies a matching
administrative unit; the full administrative chain remains in provenance.

Administrators can edit these fields through the entity metadata resource (the
legacy `POST /v1/geodata/entities/{entityId}/location` alias remains available)
with `location`, `manualFields`, `editorId`, and an optional note. The service
records the manual field set in `manualLocationFields`; provider responses
never replace those values or their corresponding codes. Removing a field from
`manualFields` returns it to provider-managed values and queues an `only
missing` lookup.

Enrichment runs after a candidate/approved entity is materialized when required
location values are missing, and after a geometry or geometry-type change. A
geometry change queues a refresh for the new coordinates; manual values remain
protected. The request ID and geometry hash are rechecked after the provider
call, so stale results cannot overwrite a later edit. Administrators can
manually queue missing metadata from Entity Management; the endpoint is
`POST /v1/geodata/entities/{entityId}/location-enrichment-requests`. It writes a
transactional outbox event consumed by the durable
`geodata-location-enrichment-v1` worker. A queued or failed lookup leaves the
entity persisted and visible; retry is available while required metadata is
still missing. Durable entity reads reconstruct the representative point from
the PostGIS centroid column (falling back to the geometry centroid), so the
worker retains its lookup coordinates after reloading an entity from the
database.
The hierarchy editor uses `GET /v1/geodata/location-options`, which aggregates
the stored BigDataCloud names and codes into continent → country → first
subdivision → province options. BigDataCloud documents these values as
`continent`/`continentCode`, `countryName`/`countryCode`,
`principalSubdivision`/`principalSubdivisionCode`, and administrative
`isoCode` values; it does not provide a separate global catalog endpoint.
Hierarchy codes are therefore read-only and derived from the selected names.

## Architecture

Read the [geodata architecture](https://github.com/myota-platform/myota-docs/blob/main/docs/architecture.md),
[project charter](https://github.com/myota-platform/myota-docs/blob/main/docs/project-charter.md),
and [repository map](https://github.com/myota-platform/myota-docs/blob/main/docs/repository-map.md).

## Source project

The original `ea7klk/mpota` repository remains untouched; see the
[migration strategy](https://github.com/myota-platform/myota-docs/blob/main/docs/migration-from-mpota.md).

## Dataset imports

### Coordinate reference systems

Imported geometries are stored and validated as WGS84 longitude/latitude
(EPSG:4326). The intake pipeline reads the declared CRS from GeoJSON or
ArcGIS-style `spatialReference` metadata and reprojects supported source CRSs
with `pyproj` before validation. Shapefile ZIP uploads use the matching `.prj`
sidecar when present. Common EPSG forms and OGC URNs are accepted, including
local projected systems such as ETRS89 / UTM 30N (EPSG:25830), which is useful
for Spanish municipal GIS data. The original declaration is retained as
`provenance.sourceCrs` for auditability. A source without CRS metadata is
treated as WGS84 for backwards compatibility.

Dataset intake is platform-wide and is not assigned to a programme. Each
import selects one or more categories from the shared Master data catalogue; programme
assignment is a later eligibility decision. The admin page reads all category
definitions from `GET /v1/entity-types`, including categories not currently
assigned to any programme. Imports stop at `PREPROCESSED` (or
`PREPROCESSED_WITH_ERRORS`) and require administrator confirmation before they
create or update an entity. Preprocessing is record-isolated: successfully
normalized features remain available as pending candidates when other features
fail. The run summary records failed feature indexes and messages, and only
those failed features are omitted from the validation queue.

Administrators can cancel `UPLOAD_PENDING` and `QUEUED` imports immediately,
or request cancellation of a `PROCESSING` import through the idempotent
`PUT /v1/geodata/imports/{runId}/cancellation` resource. Active workers poll
the durable cancellation state and stop at feature boundaries; the API returns
`202` with `CANCELLING` until that safe checkpoint is reached. Cancellation
then removes staged candidate rows and temporary source bytes while retaining
the cancelled run summary. Preprocessed runs cannot be cancelled because they
have entered administrator review. Cancelled/stalled summaries remain subject
to the configured import-retention policy.

The dedicated import worker also reconciles `CANCELLING` runs whose processing
lease has expired. This lease-aware sweep runs every 30 seconds (configurable
with `GEODATA_CANCELLATION_RECONCILE_SECONDS`) so a worker crash or lost event
cannot leave a cancelled upload in the active queue indefinitely. It does not
interfere with runs that still have a live worker lease.

Cancellation uses the row repository as the sole lifecycle/timestamp writer;
the worker finalizer reloads and locks the authoritative run through staged-row
cleanup and persistence. This avoids cancellation timestamp conflicts and
discards unfinished worker changes. Repeated requests retain the first actor
and request time. Staged cleanup uses an indexed `import_run_id` deletion;
it does not enumerate unrelated import records or load their geometries.
The isolated PostGIS relational suite covers pending uploads,
queued imports, idempotent retries and stale-worker finalization.

Use the admin web's **Geodata imports** page rather than the review page. Select
the shared feature categories before submitting; programme assignment is not
part of dataset intake. Every dataset import is pre-processed with its
source/license/attribution metadata and `ADAPTER_IMPORT` candidate source. The
validated processing queue can create a `CANDIDATE` or, for an authorized
administrator, an `APPROVED` entity. Community proposals use
`COMMUNITY_PROPOSAL` as their candidate source and follow the same review path.

Text can be pasted through `POST /v1/geodata/imports` with `format` and
`content`. The admin UI provides an explicit **OpenStreetMap GeoJSON** option;
it submits GeoJSON through the `OSM` adapter so OSM tags are filtered,
attribution is preserved, and source references remain available for review.
Browser file uploads use resumable sessions: create with
`POST /v1/geodata/import-uploads`, upload bounded binary parts to
`POST /v1/geodata/import-uploads/{uploadId}/parts/{partNumber}`, query progress
with `GET /v1/geodata/import-uploads/{uploadId}`, and finalize with
`POST /v1/geodata/import-uploads/{uploadId}/complete`. A client can abort with
`DELETE /v1/geodata/import-uploads/{uploadId}`. Sessions are user-bound and
idempotent; the API stores each part in SeaweedFS S3 multipart storage, checks
part and whole-object SHA-256, scans the completed object, and persists the
import run/outbox record before returning acceptance. Parts default to at most
16 MiB. Abandoned multipart sessions are aborted by retention maintenance.
There is no shared upload-spool PVC. The old single-request upload endpoint is
not used by the admin web; durable deployments reject this legacy upload path.

Durable deployments separate HTTP and queue execution. The API persists the
import run and outbox event, while the geodata-owned `geodata_import_worker.py`
process consumes durable NATS JetStream pull consumers with explicit ACKs,
bounded pending work, lease/heartbeat/attempt tracking, retry backoff, and a
terminal dead-letter path. Preprocessing (`geodata-preprocessing-v1`), promotion
(`geodata-import-processing-v2`), confirmed entity deletion
(`geodata-entity-deletion-v1`) and location enrichment
(`geodata-location-enrichment-v1`) use separate durable consumers and subjects.
The worker is deployed independently of the API; increasing replicas still
requires the qualification gates below. Stable identities make replay safe. A
worker refreshes relevant database queue rows before acting rather than
relying on the API process's copy. In local non-durable unit-test mode only,
the existing in-process fallback remains available; it is not the durable
Compose/Helm path.

Restart recovery replays queued work from the outbox/JetStream and reclaims
expired database leases. This removes API-local durable job execution, but it
does not yet make feature parsing memory-bounded: current parsing and some
broad candidate/spatial traversals can consume memory proportional to a large
source/run. Database-authoritative row repositories and multi-instance mutation
safety are implemented and verified; streaming parsers, bounded batches and
forced-failure/multi-worker qualification remain
[horizontal-scaling roadmap work](https://github.com/myota-platform/myota-docs/blob/main/docs/geodata-horizontal-scaling-roadmap.md).
The SeaweedFS multipart protocol also requires integration and restart
failure testing against the exact deployed SeaweedFS version before the upload
phase is considered verified.

Entity lifecycle, geometry, and category changes are persisted to the relational
PostGIS tables. Migration 016 retains JSON `service_state` only as an archive;
the durable runtime never reads or writes it. Request/job-scoped repositories
flush only changed rows, with revision conflicts and atomic audit/outbox writes.
See the [inventory, tests and fenced rollout](https://github.com/myota-platform/myota-docs/blob/main/docs/geodata-phase1-relational-authority.md).
The service does not
create built-in geodata entities at startup; local catalogues must be populated
through the import or community-proposal workflows.

Each candidate is checked against existing entities; identical geometry or a
centroid distance under 50 metres returns `dedupeWarning=POSSIBLE_DUPLICATE`
and a list of comparison geometries. This is a review warning, not an automatic
merge or rejection.

During normalization, preprocessing also derives the canonical candidate display
name from common GIS property aliases. Explicit `name` values take precedence,
followed by fields such as `SITE_NAME`, `official_name`, `NOMBRE`,
`DENOMINACION`, `title`, and `label`. Code, identifier, geometry, and
administrative fields are not used as names. The original source properties are
retained unchanged for provenance; when no usable alias exists the review UI
continues to show `Unnamed candidate`.

The deployment stores uploaded bytes in SeaweedFS and records
`myota.geodata.import.preprocess.v1` in the durable outbox for NATS consumers.
Shapefile uploads must be ZIP archives with their `.shp`, `.shx`, and `.dbf`
members.

Use `GET /v1/geodata/imports/{runId}` to retrieve the durable run summary.
It includes status/timestamps, source manifest information, candidate counts,
and counts for pre-processed, created, updated, skipped, disappeared, and
failed features. Use `GET /v1/geodata/imports/{runId}/candidates` for a compact
paged validation queue containing only pending records. `POST
.../candidates/validate` remains available for explicit confirmation or
removes records when `validationStatus=REJECTED`. The admin import detail uses
`POST /v1/geodata/imports/{runId}/process` directly: it accepts pending or
previously confirmed IDs and an explicit `CANDIDATE` or `APPROVED` target.
Successful promotion deletes the staged record; rejection also removes it from
the import detail. The promotion request emits
`geodata.import.processing.queued.v1` to the
`myota.geodata.import.process.v1` subject. The durable worker consumes the
database-backed queue idempotently. The default whole-object limit is 1 GiB
(`MYOTA_UPLOAD_MAX_BYTES`); deployments may set a lower limit. Each part is
temporarily staged as a bounded file (16 MiB by default), not the entire source
document.
The admin web treats the text area as optional when a file is
selected and links each recent run to this summary. Active `QUEUED`,
`PROCESSING`, and `PREPROCESSED` runs are exposed with their pending and
confirmed candidate counts so the admin web can keep a dedicated
pre-processing queue separate from Geodata Review. Only explicit promotion
creates or updates reviewable entities.

The import-run collection is ordered newest-first before pagination, so a newly
submitted file remains visible on the first page even when the history contains
more runs than the page size.

After review, `POST /v1/geodata/imports/{runId}/processed` explicitly finalizes
the run. The operation is idempotent, records the administrator and timestamp,
deletes all staged candidate and promotion-queue rows for that run, and keeps
the import summary in `PROCESSED` history. Already materialized entities are
not deleted. The admin UI also opens a Leaflet location preview when a
pre-processed record's name is clicked.

During review, `POST /v1/geodata/entities/{entityId}/entity-type` changes the
ordered shared Master data category list, regardless of whether
`programmeSlug` is set. The first category remains the compatibility
`entityType`; all selected categories are returned as `entityTypes` and
`entityTypeCodes`. Assignments are persisted in the relational
`geodata_entity_category` table; the JSON state is only a compatibility
projection. Category filters match any assigned category.
`POST /v1/geodata/entities/{entityId}/name` corrects the display name. Both
operations require review authorization, preserve the previous value, editor,
note, and timestamp in the entity audit history, and reject edits to retired
entities for category changes; name corrections remain available for historical
records.
