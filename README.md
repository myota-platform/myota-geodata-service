# MyOTA Outdoor Activation Platform

MyOTA is a programme-agnostic platform for outdoor activation programmes. MPOTA is represented as a configured programme, not as the platform itself. No rules or charter text are copied from POTA or any other programme: every programme supplies its own configuration, policy, eligibility, awards and public charter.

This repository is a runnable vertical-slice bootstrap for the service repositories described in [`docs/repository-map.md`](docs/repository-map.md). It contains four independently runnable Python services, an API-first contract, a universal browser UI, PostGIS migrations, and Kubernetes/Helm deployment assets.

## What works now

- Amateur-radio-aware identity: operator/SWL participation, multiple callsigns, one primary callsign, lifecycle and verification fields.
- Programme configuration: programme-owned entity types, rules, minimum QSOs, awards, theme and optional OIDC settings.
- Geodata lifecycle: imported candidate → community proposal → approver review → approved entity.
- GeoJSON Point, Polygon/MultiPolygon, and LineString trail/way geometry; OSM-style `type: "way"` records are normalized to LineString.
- Provenance-aware imports with adapter metadata for ParkServe, OSM, government GIS and manual proposals.
- A dedicated candidate-only intake supports pasted GeoJSON/KML/GPX/WFS/ArcGIS JSON and uploaded Shapefile, OSM PBF and ParkServe payloads. Uploads are scanned, stored in MinIO/S3-compatible storage, and emit a durable queue event.
- Reverse-geocoded entity location fields: continent/country, ISO codes, first
  country subdivision, optional province/county, and city/municipality.
- Activation and QSO primitives with idempotency keys and audit events.
- Universal themed frontend with verified/candidate map distinction.
- OpenAPI and event contracts, ADRs, migration notes, health endpoints and local deployment manifests.

The default test/runtime adapter is in-memory so the slice can be exercised without third-party Python packages. PostgreSQL/PostGIS is the production storage target and is defined in `db/migrations/`.

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

Read [`docs/architecture.md`](docs/architecture.md), [`docs/adr/0001-storage-topology.md`](docs/adr/0001-storage-topology.md), and [`docs/repository-map.md`](docs/repository-map.md). The current bootstrap is kept together to make the vertical slice easy to run; the repository map defines the justified GitHub split once the MyOTA organization is available.

## Source project

The original `ea7klk/mpota` repository remains untouched. Its charter and planned flows are treated as the migration source; see [`docs/migration-from-mpota.md`](docs/migration-from-mpota.md).

## Dataset imports

Dataset intake is platform-wide and is not assigned to a programme. Each
import selects a category from the shared Master data catalogue; programme
assignment is a later eligibility decision. The admin page reads all category
definitions from `GET /v1/entity-types`, including categories not currently
assigned to any programme, and imports always create `CANDIDATE` entities.

Use the admin web's **Geodata imports** page rather than the review page. Select
the shared feature category before submitting; programme assignment is not
part of dataset intake. Every
dataset import is written as `CANDIDATE`, retains source/license/attribution
metadata, and must pass the normal proposal and approval workflow.

Text can be pasted through `POST /v1/geodata/imports` with `format` and
`content`. File uploads use `POST /v1/geodata/imports/upload` with a base64
payload, filename, and the same category/source metadata. The deployment
stores uploaded bytes in MinIO and records a `geodata.import.queued.v1`
outbox event for NATS consumers. Shapefile uploads must be ZIP archives with
their `.shp`, `.shx`, and `.dbf` members.

During review, `POST /v1/geodata/entities/{entityId}/entity-type` changes the
shared Master data category, regardless of whether `programmeSlug` is set.
`POST /v1/geodata/entities/{entityId}/name` corrects the display name. Both
operations require review authorization, preserve the previous value, editor,
note, and timestamp in the entity audit history, and reject edits to retired
entities for category changes; name corrections remain available for historical
records.
