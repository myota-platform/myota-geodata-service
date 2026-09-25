"""Durable relational persistence for the geodata catalogue.

The compatibility JSON snapshot is retained for older service metadata and
tests. Entity writes are additionally stored in the PostGIS-owned tables so a
manual proposal is durable and visible to GIS tooling.
"""
from __future__ import annotations

import json
import uuid
from typing import Any

from common import Store


def _uuid(value: Any) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(value)) if value else None
    except (ValueError, TypeError, AttributeError):
        return None


class GeodataStore(Store):
    def _category_id(self, connection: Any, code: str, geometry_type: str) -> uuid.UUID:
        row = connection.execute(
            "SELECT id FROM entity_type WHERE programme_id IS NULL AND code = %s LIMIT 1", (code,)
        ).fetchone()
        if row:
            return row[0]
        category_id = uuid.uuid4()
        connection.execute(
            "INSERT INTO entity_type(id, programme_id, code, label, geometry_kind, config) "
            "VALUES (%s, NULL, %s, %s, %s, '{}'::jsonb)",
            (category_id, code, code.replace("_", " ").title(), geometry_type.upper()),
        )
        return category_id

    @staticmethod
    def _public_properties(entity: dict[str, Any]) -> dict[str, Any]:
        properties = dict(entity)
        properties.pop("geometry", None)
        properties.pop("centroid", None)
        return properties

    def _upsert_entity(self, connection: Any, entity: dict[str, Any]) -> None:
        entity_id = _uuid(entity.get("id"))
        if not entity_id:
            raise ValueError("geodata entity ids must be UUIDs")
        geometry = entity.get("geometry") or {}
        geometry_json = json.dumps(geometry, separators=(",", ":"))
        entity_type = str(entity.get("entityType") or "UNKNOWN")
        category_id = self._category_id(connection, entity_type, str(geometry.get("type") or "GEOMETRY"))
        provenance = entity.get("provenance") or {}
        source = provenance.get("source") or {}
        connection.execute(
            "INSERT INTO geodata_entity(id, programme_id, programme_slug, entity_type_id, entity_type_code, name, lifecycle_status, geom, centroid, public_properties, source_state, source_key, source_hash, jurisdiction, attachments) "
            "VALUES (%s, NULL, %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), ST_Centroid(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))::geography, %s::jsonb, %s, %s, %s, %s, %s::jsonb) "
            "ON CONFLICT (id) DO UPDATE SET programme_slug=EXCLUDED.programme_slug, entity_type_id=EXCLUDED.entity_type_id, entity_type_code=EXCLUDED.entity_type_code, name=EXCLUDED.name, lifecycle_status=EXCLUDED.lifecycle_status, geom=EXCLUDED.geom, centroid=EXCLUDED.centroid, public_properties=EXCLUDED.public_properties, source_state=EXCLUDED.source_state, source_key=EXCLUDED.source_key, source_hash=EXCLUDED.source_hash, jurisdiction=EXCLUDED.jurisdiction, attachments=EXCLUDED.attachments, updated_at=now()",
            (entity_id, entity.get("programmeSlug"), category_id, entity_type, entity.get("name") or "Unnamed entity",
             entity.get("status") or "CANDIDATE", geometry_json, geometry_json, json.dumps(self._public_properties(entity)),
             entity.get("sourceState") or "CURRENT", provenance.get("sourceKey"), provenance.get("sourceHash"),
             entity.get("jurisdiction"), json.dumps(entity.get("attachments") or [])),
        )
        connection.execute("DELETE FROM source_reference WHERE entity_id = %s", (entity_id,))
        connection.execute(
            "INSERT INTO source_reference(id, entity_id, adapter_code, source_uri, source_record_id, license, attribution, retrieved_at, source_payload) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)",
            (uuid.uuid4(), entity_id, provenance.get("adapter") or "MANUAL", source.get("url") or source.get("uri"),
             entity.get("sourceRef") or str(entity_id), source.get("license") or provenance.get("license"),
             source.get("attribution") or provenance.get("attribution"), source.get("retrievedAt"),
             json.dumps(provenance.get("sourceFeature") or source)),
        )

    def _sync_relational(self) -> None:
        if not self.durable:
            return
        with self.transaction() as connection:
            for entity in self.items.values():
                self._upsert_entity(connection, entity)

    def delete_relational(self, entity_id: str) -> None:
        if not self.durable:
            return
        entity_uuid = _uuid(entity_id)
        if not entity_uuid:
            return
        with self.transaction() as connection:
            connection.execute("DELETE FROM source_reference WHERE entity_id = %s", (entity_uuid,))
            connection.execute("DELETE FROM entity_review WHERE entity_id = %s", (entity_uuid,))
            connection.execute("DELETE FROM conflation_candidate WHERE left_entity_id = %s OR right_entity_id = %s", (entity_uuid, entity_uuid))
            connection.execute("DELETE FROM geodata_entity WHERE id = %s", (entity_uuid,))

    def hydrate(self) -> None:
        super().hydrate()
        if not self.durable:
            return
        with self.transaction() as connection:
            rows = connection.execute(
                "SELECT id::text, programme_slug, entity_type_code, name, lifecycle_status, ST_AsGeoJSON(geom)::jsonb, public_properties, source_state, jurisdiction, attachments FROM geodata_entity"
            ).fetchall()
            source_rows = connection.execute(
                "SELECT entity_id::text, adapter_code, source_uri, source_record_id, license, attribution, retrieved_at, source_payload FROM source_reference"
            ).fetchall()
        sources = {row[0]: row for row in source_rows}
        for row in rows:
            if row[0] in self.items:
                continue
            properties = row[6] if isinstance(row[6], dict) else json.loads(row[6] or "{}")
            entity = {**properties, "id": row[0], "programmeSlug": row[1], "entityType": row[2], "name": row[3],
                      "status": row[4], "geometry": row[5], "sourceState": row[7], "jurisdiction": row[8], "attachments": row[9] or []}
            source = sources.get(row[0])
            if source:
                payload = source[7] if isinstance(source[7], dict) else json.loads(source[7] or "{}")
                entity["sourceRef"] = source[3]
                entity.setdefault("provenance", {}).update({
                    "adapter": source[1], "source": {"url": source[2], "license": source[4], "attribution": source[5], "retrievedAt": source[6]},
                    "sourceFeature": payload,
                })
            self.items[row[0]] = entity

    def persist(self) -> None:
        self._sync_relational()
        super().persist()
