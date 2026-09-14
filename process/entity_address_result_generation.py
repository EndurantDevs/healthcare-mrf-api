# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Durable generation authority for the unified-address serving family."""

from __future__ import annotations

import datetime
import re
from dataclasses import dataclass
from typing import Any, Mapping
from uuid import UUID

from sqlalchemy import text

from db.models import (
    EntityAddressEvidence,
    EntityAddressMedicationBridge,
    EntityAddressNetworkBridge,
    EntityAddressPlanBridge,
    EntityAddressProcedureBridge,
    EntityAddressUnified,
    FacilityAnchorNPICandidate,
)


TABLE_NAME = "entity_address_result_generation"
ENTITY_ADDRESS_RESULT_MODELS = (
    EntityAddressUnified,
    EntityAddressEvidence,
    EntityAddressPlanBridge,
    EntityAddressNetworkBridge,
    EntityAddressProcedureBridge,
    EntityAddressMedicationBridge,
    FacilityAnchorNPICandidate,
)
RELATION_NAMES = tuple(model.__tablename__ for model in ENTITY_ADDRESS_RESULT_MODELS)
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_MAX_GENERATION = (1 << 63) - 1
_MAX_OID = (1 << 32) - 1


@dataclass(frozen=True)
class EntityAddressServingGeneration:
    """Portable origin identity for one successfully published result."""

    origin_lineage_id: str
    origin_generation: int
    published_at: datetime.datetime

    def as_dict(self) -> dict[str, Any]:
        """Return the canonical portable generation representation."""

        return {
            "origin_lineage_id": self.origin_lineage_id,
            "origin_generation": self.origin_generation,
            "published_at": _timestamp_text(self.published_at),
        }


@dataclass(frozen=True)
class EntityAddressResultGenerationAuthority:
    """Local counter plus the distinct origin currently served by local OIDs."""

    local_lineage_id: str
    local_generation: int
    serving_generation: EntityAddressServingGeneration | None
    relation_oids: tuple[int, ...] | None

    def as_dict(self) -> dict[str, Any]:
        """Return the durable authority without inventing a serving generation."""

        return {
            "local_lineage_id": self.local_lineage_id,
            "local_generation": self.local_generation,
            "serving_generation": (None if self.serving_generation is None else self.serving_generation.as_dict()),
            "relation_oids": None if self.relation_oids is None else list(self.relation_oids),
        }


def _schema_name(value: object) -> str:
    normalized = str(value or "").strip()
    if not _IDENTIFIER.fullmatch(normalized) or len(normalized.encode("utf-8")) > 63:
        raise ValueError("entity-address result generation schema is invalid")
    return normalized


def _quoted(value: str) -> str:
    return f'"{value}"'


def _uuid_text(value: object) -> str:
    try:
        normalized = str(UUID(str(value)))
    except AttributeError, TypeError, ValueError:
        raise ValueError("entity-address result generation lineage is invalid") from None
    return normalized


def _timestamp(value: object) -> datetime.datetime:
    if isinstance(value, str):
        try:
            value = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("entity-address result generation publication time is invalid") from None
    if not isinstance(value, datetime.datetime) or value.tzinfo is None:
        raise ValueError("entity-address result generation publication time is invalid")
    return value.astimezone(datetime.timezone.utc)


def _timestamp_text(value: datetime.datetime) -> str:
    return _timestamp(value).isoformat().replace("+00:00", "Z")


def validate_entity_address_serving_generation(
    value: object,
) -> EntityAddressServingGeneration:
    """Validate one complete portable origin generation."""

    if isinstance(value, EntityAddressServingGeneration):
        value = value.as_dict()
    if not isinstance(value, Mapping) or set(value) != {
        "origin_lineage_id",
        "origin_generation",
        "published_at",
    }:
        raise ValueError("entity-address serving generation is invalid")
    generation = value["origin_generation"]
    if type(generation) is not int or not 0 < generation <= _MAX_GENERATION:
        raise ValueError("entity-address serving generation is invalid")
    return EntityAddressServingGeneration(
        origin_lineage_id=_uuid_text(value["origin_lineage_id"]),
        origin_generation=generation,
        published_at=_timestamp(value["published_at"]),
    )


def _relation_oids(relation_oid_values: object) -> tuple[int, ...]:
    if not isinstance(relation_oid_values, (list, tuple)) or len(relation_oid_values) != len(RELATION_NAMES):
        raise ValueError("entity-address result generation relation identity is invalid")
    relation_oids = tuple(relation_oid_values)
    if any(type(relation_oid) is not int or not 0 < relation_oid <= _MAX_OID for relation_oid in relation_oids) or len(
        set(relation_oids)
    ) != len(relation_oids):
        raise ValueError("entity-address result generation relation identity is invalid")
    return relation_oids


def _row_mapping(row: object) -> Mapping[str, Any]:
    mapping = getattr(row, "_mapping", row)
    if not isinstance(mapping, Mapping):
        raise RuntimeError("entity-address result generation singleton is unavailable")
    return mapping


def validate_entity_address_result_generation_authority(
    authority_row: object,
) -> EntityAddressResultGenerationAuthority:
    """Validate one migration-installed singleton without inferring history."""

    authority_by_field = _row_mapping(authority_row)
    if authority_by_field.get("singleton") is not True:
        raise RuntimeError("entity-address result generation singleton is unavailable")
    local_generation = authority_by_field.get("local_generation")
    if type(local_generation) is not int or not 0 <= local_generation <= _MAX_GENERATION:
        raise RuntimeError("entity-address local result generation is invalid")
    local_lineage_id = _uuid_text(authority_by_field.get("local_lineage_id"))
    origin_values = (
        authority_by_field.get("origin_lineage_id"),
        authority_by_field.get("origin_generation"),
        authority_by_field.get("published_at"),
        authority_by_field.get("relation_oids"),
    )
    if all(origin_value is None for origin_value in origin_values):
        return EntityAddressResultGenerationAuthority(
            local_lineage_id,
            local_generation,
            None,
            None,
        )
    if any(origin_value is None for origin_value in origin_values):
        raise RuntimeError("entity-address serving generation is incomplete")
    try:
        serving_generation = validate_entity_address_serving_generation(
            {
                "origin_lineage_id": str(authority_by_field["origin_lineage_id"]),
                "origin_generation": authority_by_field["origin_generation"],
                "published_at": authority_by_field["published_at"],
            }
        )
        relation_oids = _relation_oids(authority_by_field["relation_oids"])
    except ValueError as error:
        raise RuntimeError("entity-address serving generation is invalid") from error
    return EntityAddressResultGenerationAuthority(
        local_lineage_id,
        local_generation,
        serving_generation,
        relation_oids,
    )


def _state_sql(schema_name: str, *, lock: bool) -> str:
    suffix = " FOR UPDATE" if lock else ""
    return (
        "SELECT singleton, local_lineage_id, local_generation, "
        "origin_lineage_id, origin_generation, published_at, relation_oids "
        f"FROM {_quoted(schema_name)}.{_quoted(TABLE_NAME)} "
        f"WHERE singleton IS TRUE{suffix}"
    )


async def _locked_authority(
    database: Any,
    schema_name: str,
) -> EntityAddressResultGenerationAuthority:
    row = await database.first(text(_state_sql(schema_name, lock=True)))
    if row is None:
        raise RuntimeError("entity-address result generation singleton is unavailable")
    return validate_entity_address_result_generation_authority(row)


async def read_entity_address_result_generation_authority(
    session: Any,
    *,
    schema_name: str,
) -> EntityAddressResultGenerationAuthority:
    """Read the required singleton in a caller-owned serving transaction."""

    schema = _schema_name(schema_name)
    row = (await session.execute(text(_state_sql(schema, lock=False)))).mappings().one_or_none()
    if row is None:
        raise RuntimeError("entity-address result generation singleton is unavailable")
    return validate_entity_address_result_generation_authority(row)


async def _current_relation_oids(database: Any, schema_name: str) -> tuple[int, ...]:
    rows = await database.all(
        text(
            "SELECT relation_name, "
            "to_regclass(format('%I.%I', CAST(:schema_name AS text), relation_name))::oid::bigint "
            "AS relation_oid "
            "FROM unnest(CAST(:relation_names AS text[])) WITH ORDINALITY "
            "AS relations(relation_name, ordinal) ORDER BY ordinal"
        ),
        schema_name=schema_name,
        relation_names=list(RELATION_NAMES),
    )
    try:
        names = tuple(str(row[0]) for row in rows)
        relation_oids = _relation_oids(tuple(int(row[1]) for row in rows))
    except IndexError, TypeError, ValueError:
        raise RuntimeError("entity-address serving relations are unavailable") from None
    if names != RELATION_NAMES:
        raise RuntimeError("entity-address serving relations are unavailable")
    return relation_oids


async def publish_local_entity_address_generation(
    database: Any,
    *,
    schema_name: str,
) -> EntityAddressResultGenerationAuthority:
    """Advance the local counter after an ordinary successful cutover."""

    schema = _schema_name(schema_name)
    current = await _locked_authority(database, schema)
    if current.local_generation >= _MAX_GENERATION:
        raise RuntimeError("entity-address local result generation is exhausted")
    relation_oids = await _current_relation_oids(database, schema)
    next_generation = current.local_generation + 1
    updated = await database.first(
        text(
            f"UPDATE {_quoted(schema)}.{_quoted(TABLE_NAME)} SET "
            "local_generation=:next_generation, "
            "origin_lineage_id=local_lineage_id, "
            "origin_generation=:next_generation, "
            "published_at=clock_timestamp(), "
            "relation_oids=CAST(:relation_oids AS bigint[]) "
            "WHERE singleton IS TRUE "
            "RETURNING singleton, local_lineage_id, local_generation, "
            "origin_lineage_id, origin_generation, published_at, relation_oids"
        ),
        next_generation=next_generation,
        relation_oids=list(relation_oids),
    )
    if updated is None:
        raise RuntimeError("entity-address result generation singleton is unavailable")
    return validate_entity_address_result_generation_authority(updated)


async def publish_adopted_entity_address_generation(
    database: Any,
    *,
    schema_name: str,
    source_generation: Mapping[str, Any] | EntityAddressServingGeneration | None,
) -> EntityAddressResultGenerationAuthority:
    """Preserve a source origin while replacing only destination-local OIDs."""

    schema = _schema_name(schema_name)
    current_authority = await _locked_authority(database, schema)
    if source_generation is None:
        update_values_by_column = {
            "origin_lineage_id": None,
            "origin_generation": None,
            "published_at": None,
            "relation_oids": None,
        }
    else:
        source_identity = validate_entity_address_serving_generation(source_generation)
        update_values_by_column = {
            "origin_lineage_id": UUID(source_identity.origin_lineage_id),
            "origin_generation": source_identity.origin_generation,
            "published_at": source_identity.published_at,
            "relation_oids": list(await _current_relation_oids(database, schema)),
        }
    updated = await database.first(
        text(
            f"UPDATE {_quoted(schema)}.{_quoted(TABLE_NAME)} SET "
            "origin_lineage_id=CAST(:origin_lineage_id AS uuid), "
            "origin_generation=:origin_generation, "
            "published_at=CAST(:published_at AS timestamptz), "
            "relation_oids=CAST(:relation_oids AS bigint[]) "
            "WHERE singleton IS TRUE "
            "RETURNING singleton, local_lineage_id, local_generation, "
            "origin_lineage_id, origin_generation, published_at, relation_oids"
        ),
        **update_values_by_column,
    )
    if updated is None:
        raise RuntimeError("entity-address result generation singleton is unavailable")
    updated_authority = validate_entity_address_result_generation_authority(updated)
    if (
        updated_authority.local_lineage_id != current_authority.local_lineage_id
        or updated_authority.local_generation != current_authority.local_generation
    ):
        raise RuntimeError("entity-address local result generation changed during adoption")
    return updated_authority


async def publish_cutover_entity_address_generation(
    database: Any,
    *,
    schema_name: str,
    context: Mapping[str, Any],
) -> EntityAddressResultGenerationAuthority | None:
    """Publish the configured generation identity inside the cutover transaction."""

    generation_mode = context.get("result_generation_mode")
    if generation_mode == "ordinary":
        return await publish_local_entity_address_generation(
            database,
            schema_name=schema_name,
        )
    if generation_mode == "adoption":
        return await publish_adopted_entity_address_generation(
            database,
            schema_name=schema_name,
            source_generation=context.get("source_serving_generation"),
        )
    if generation_mode is not None:
        raise RuntimeError("entity-address result generation mode is invalid")
    return None


def require_entity_address_automatic_generation_order(
    candidate: object,
    incumbent: object,
) -> None:
    """Require a strictly newer generation from the same known origin lineage."""

    if candidate is None or incumbent is None:
        raise ValueError("automatic entity-address generation order is unavailable")
    candidate_generation = validate_entity_address_serving_generation(candidate)
    incumbent_generation = validate_entity_address_serving_generation(incumbent)
    if (
        candidate_generation.origin_lineage_id != incumbent_generation.origin_lineage_id
        or candidate_generation.origin_generation <= incumbent_generation.origin_generation
    ):
        raise ValueError("automatic entity-address generation order is unsupported")


__all__ = [
    "ENTITY_ADDRESS_RESULT_MODELS",
    "EntityAddressResultGenerationAuthority",
    "EntityAddressServingGeneration",
    "RELATION_NAMES",
    "publish_adopted_entity_address_generation",
    "publish_cutover_entity_address_generation",
    "publish_local_entity_address_generation",
    "read_entity_address_result_generation_authority",
    "require_entity_address_automatic_generation_order",
    "validate_entity_address_result_generation_authority",
    "validate_entity_address_serving_generation",
]
