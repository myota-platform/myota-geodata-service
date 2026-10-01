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

- The identity, programme, activity, public-web, admin-web, and deployment
  boundaries are documented in the
  [repository map](https://github.com/myota-platform/myota-docs/blob/main/docs/repository-map.md).
- Geodata lifecycle: adapter/import run or community proposal → CANDIDATE → approver review → APPROVED or REJECTED; approved entities may only be RETIRED.
- GeoJSON Point, Polygon/MultiPolygon, and LineString trail/way geometry; OSM-style `type: "way"` records are normalized to LineString.
- Provenance-aware imports with adapter metadata for ParkServe, OSM, government GIS and manual proposals.
- A dedicated two-stage intake supports pasted GeoJSON/KML/GPX/WFS/ArcGIS JSON and uploaded Shapefile, OSM PBF and ParkServe payloads. Uploads are scanned, stored in SeaweedFS through its S3-compatible API, and emit durable queue events. Parsing and normalization stop at `PREPROCESSED`; no entity is created until an administrator validates selected records.
- Reverse-geocoded entity location fields: continent/country, ISO codes, first
  country subdivision, optional province/county, and city/municipality.
- Lifecycle transitions are API-owned and audited; QGIS is a controlled
  graphical editing tool, not an approval bypass.
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

The `/metrics` endpoint exposes durable entity, geometry, category, import-run
and preprocessing-candidate counts. With `MYOTA_OTEL_ENABLED=1`, HTTP request
metrics and traces are exported to the OpenTelemetry Collector.

## Run the vertical slice

```bash
python3 -m unittest discover -s tests -v
python3 services/dev_server.py
```

Open <http://127.0.0.1:8080>. The dev server starts the four services on ports 8001–8004 and proxies the browser API calls. It is intentionally dependency-free.

For a containerized PostGIS environment, use `docker compose up --build` after starting Colima. The image uses the same service code with `SERVICE=identity|programmes|geodata|activity`.

## Reverse geocoding

Server-side imports and geometry edits use BigDataCloud's Reverse Geocoding to
City API. The service calls `https://api-bdc.net/data/reverse-geocode` with the
entity centroid, keeps the normalized response on the entity, and preserves the
provider response under `provenance.reverseGeocoding`.

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

Administrators can edit these fields through
`POST /v1/geodata/entities/{entityId}/location` with `location`,
`manualFields`, `editorId`, and an optional note. The service records the
manual field set in `manualLocationFields` and never overwrites those fields
when an import, geometry edit, or reverse-geocoding refresh runs. Removing a
field from `manualFields` explicitly returns it to provider-managed values.
Successful provider data is reused by subsequent imports, geometry edits and
metadata saves; a new remote lookup is made only when location data is missing
or an administrator explicitly releases fields back to automatic management.
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

The original `ea7klk/mpota` repository remains untouched. Its charter and planned flows are treated as the migration source; see [`docs/migration-from-mpota.md`](docs/migration-from-mpota.md).

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

Use the admin web's **Geodata imports** page rather than the review page. Select
the shared feature categories before submitting; programme assignment is not
part of dataset intake. Every dataset import is pre-processed with its
source/license/attribution metadata and `ADAPTER_IMPORT` candidate source. The
validated processing queue can create a `CANDIDATE` or, for an authorized
administrator, an `APPROVED` entity. Community proposals use
`COMMUNITY_PROPOSAL` as their candidate source and follow the same review path.

Text can be pasted through `POST /v1/geodata/imports` with `format` and
`content`. The admin UI provides an explicit **OpenStreetMap GeoJSON** option;
it submits ordinary GeoJSON through the `OSM` adapter so OSM tags are filtered,
attribution is preserved, and source references remain available for review.
File uploads use `POST /v1/geodata/imports/upload` as a multipart request with
a `metadata` JSON part and a `file` part. The legacy JSON `contentBase64`
payload remains supported for non-browser clients. Both paths return
`202 QUEUED`; parsing, normalization, reverse-geocoding, deduplication, and
pre-processed candidate persistence run in a bounded background import worker.
Each run stores its source document in SeaweedFS, claims a PostgreSQL lease,
and refreshes a heartbeat while it is working. On service restart, queued runs
and all runs left in `PROCESSING` by the previous service instance are
immediately requeued and resumed from object storage; the startup requeue is
persisted before workers are dispatched. This avoids waiting for the normal
lease timeout after a restart. Pasted KML/GPX sources are replayed as their
normalized GeoJSON snapshot, while uploaded files retain their original
parser format. Durable binary uploads whose parser adapter is not available
remain visibly queued instead of being incorrectly marked failed.
Unrecoverable runs are marked `FAILED` with a visible reason instead of being
left indefinitely in `PROCESSING`. This makes the import history a durable
operational status view rather than a process-local queue snapshot.

Entity lifecycle, geometry, and category changes are persisted to the relational
PostGIS tables. The JSON `service_state` record is only a compatibility snapshot;
on restart, relational entity columns are authoritative. The service does not
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
`geodata.import.preprocessed.v1` in the durable outbox for NATS consumers.
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
`myota.geodata.import.process.v1` subject. The local service runs a bounded
fallback worker, while production NATS consumers process the same durable
queue idempotently. The default
request and upload limits default to 1 GiB (`MYOTA_MAX_BODY_BYTES` and
`MYOTA_UPLOAD_MAX_BYTES`); deployments may set lower bounded values. Multipart
uploads are spooled to a temporary file and streamed into object storage, so
the gateway does not need a second in-memory copy of a large source document.
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
