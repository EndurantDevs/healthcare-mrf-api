# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Durable generation authority for closed reference replacement families."""

from __future__ import annotations

import datetime
import os
import re
from dataclasses import dataclass
from typing import Any, Mapping
from uuid import UUID

import asyncpg
from sqlalchemy import text

TABLE_NAME = "reference_family_result_generation"
RELATION_NAMES_BY_IMPORTER = {
    "mrf": (
        "issuer",
        "plan",
        "plan_formulary",
        "plan_benefits_marketplace",
        "plan_transparency",
        "plan_drug_raw",
        "plan_drug_stats",
        "plan_drug_tier_stats",
        "log",
        "plan_npi_raw",
        "plan_networktier",
        "mrf_address",
        "mrf_address_evidence",
    ),
    "mrf-address": ("mrf_address", "mrf_address_evidence"),
    "plan-attributes": (
        "plan_attributes",
        "plan_prices",
        "plan_rating_areas",
        "plan_benefits",
    ),
    "places-zcta": ("pricing_places_zcta",),
    "geo": ("geo_zip_lookup",),
    "lodes": ("lodes_workplace_aggregate",),
    "cms-doctors": ("doctor_clinician_address", "cms_doctor_education"),
    "tiger": ("zip_state", "zcta5"),
    "medicare-enrollment": (
        "medicare_enrollment_county_stats",
        "medicare_enrollment_stats",
    ),
}
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_MAX_GENERATION = (1 << 63) - 1
_MAX_OID = (1 << 32) - 1


@dataclass(frozen=True)
class ReferenceFamilyServingGeneration:
    """Portable origin identity for one published family generation."""

    origin_lineage_id: str
    origin_generation: int
    published_at: datetime.datetime

    def as_dict(self) -> dict[str, Any]:
        """Return the portable generation identity as canonical fields."""

        return {
            "origin_lineage_id": self.origin_lineage_id,
            "origin_generation": self.origin_generation,
            "published_at": _timestamp_text(self.published_at),
        }


@dataclass(frozen=True)
class ReferenceFamilyResultGenerationAuthority:
    """Destination-local counter and the origin currently served by exact OIDs."""

    importer_id: str
    local_lineage_id: str
    local_generation: int
    serving_generation: ReferenceFamilyServingGeneration | None
    relation_oids: tuple[int, ...] | None

    def as_dict(self) -> dict[str, Any]:
        """Return local and portable authority fields for durable receipts."""

        return {
            "importer_id": self.importer_id,
            "local_lineage_id": self.local_lineage_id,
            "local_generation": self.local_generation,
            "serving_generation": (None if self.serving_generation is None else self.serving_generation.as_dict()),
            "relation_oids": None if self.relation_oids is None else list(self.relation_oids),
        }


def _importer_id(value: object) -> str:
    if not isinstance(value, str) or value not in RELATION_NAMES_BY_IMPORTER:
        raise ValueError("reference family result generation importer is invalid")
    return value


def _schema_name(value: object) -> str:
    normalized = str(value or "").strip()
    if not _IDENTIFIER.fullmatch(normalized) or len(normalized.encode("utf-8")) > 63:
        raise ValueError("reference family result generation schema is invalid")
    return normalized


def _quoted(value: str) -> str:
    return f'"{value}"'


def _uuid_text(value: object) -> str:
    try:
        return str(UUID(str(value)))
    except AttributeError, TypeError, ValueError:
        raise ValueError("reference family result generation lineage is invalid") from None


def _timestamp(value: object) -> datetime.datetime:
    if isinstance(value, str):
        try:
            value = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("reference family publication time is invalid") from None
    if not isinstance(value, datetime.datetime) or value.tzinfo is None:
        raise ValueError("reference family publication time is invalid")
    return value.astimezone(datetime.timezone.utc)


def _timestamp_text(value: datetime.datetime) -> str:
    return _timestamp(value).isoformat().replace("+00:00", "Z")


def validate_reference_family_serving_generation(value: object) -> ReferenceFamilyServingGeneration:
    """Validate one complete portable origin generation."""

    if isinstance(value, ReferenceFamilyServingGeneration):
        value = value.as_dict()
    if not isinstance(value, Mapping) or set(value) != {
        "origin_lineage_id",
        "origin_generation",
        "published_at",
    }:
        raise ValueError("reference family serving generation is invalid")
    generation = value["origin_generation"]
    if type(generation) is not int or not 0 < generation <= _MAX_GENERATION:
        raise ValueError("reference family serving generation is invalid")
    return ReferenceFamilyServingGeneration(
        _uuid_text(value["origin_lineage_id"]),
        generation,
        _timestamp(value["published_at"]),
    )


def _relation_oids(importer_id: str, value: object) -> tuple[int, ...]:
    expected = RELATION_NAMES_BY_IMPORTER[importer_id]
    if not isinstance(value, (list, tuple)) or len(value) != len(expected):
        raise ValueError("reference family relation identity is invalid")
    relation_oids = tuple(value)
    if any(type(oid) is not int or not 0 < oid <= _MAX_OID for oid in relation_oids) or len(set(relation_oids)) != len(
        relation_oids
    ):
        raise ValueError("reference family relation identity is invalid")
    return relation_oids


def _row_mapping(row: object) -> Mapping[str, Any]:
    mapping = getattr(row, "_mapping", row)
    if not isinstance(mapping, Mapping):
        if isinstance(row, asyncpg.Record):
            return dict(row)
        raise RuntimeError("reference family generation authority is unavailable")
    return mapping


def validate_reference_family_result_generation_authority(
    authority_row: object,
) -> ReferenceFamilyResultGenerationAuthority:
    """Validate one migration-installed family row without inferring legacy history."""

    authority_by_field = _row_mapping(authority_row)
    try:
        importer_id = _importer_id(authority_by_field.get("importer_id"))
        local_lineage_id = _uuid_text(authority_by_field.get("local_lineage_id"))
    except ValueError as error:
        raise RuntimeError("reference family generation authority is invalid") from error
    local_generation = authority_by_field.get("local_generation")
    if type(local_generation) is not int or not 0 <= local_generation <= _MAX_GENERATION:
        raise RuntimeError("reference family local generation is invalid")
    serving_fields = (
        authority_by_field.get("origin_lineage_id"),
        authority_by_field.get("origin_generation"),
        authority_by_field.get("published_at"),
        authority_by_field.get("relation_oids"),
    )
    if all(field is None for field in serving_fields):
        return ReferenceFamilyResultGenerationAuthority(importer_id, local_lineage_id, local_generation, None, None)
    if any(field is None for field in serving_fields):
        raise RuntimeError("reference family serving generation is incomplete")
    try:
        serving_generation = validate_reference_family_serving_generation(
            {
                "origin_lineage_id": authority_by_field["origin_lineage_id"],
                "origin_generation": authority_by_field["origin_generation"],
                "published_at": authority_by_field["published_at"],
            }
        )
        relation_oids = _relation_oids(importer_id, authority_by_field["relation_oids"])
    except ValueError as error:
        raise RuntimeError("reference family serving generation is invalid") from error
    return ReferenceFamilyResultGenerationAuthority(
        importer_id,
        local_lineage_id,
        local_generation,
        serving_generation,
        relation_oids,
    )


async def _first(database: Any, statement: Any, **params: Any) -> object | None:
    if hasattr(database, "first"):
        return await database.first(statement, **params)
    return (await database.execute(statement, params)).mappings().one_or_none()


async def _all(database: Any, statement: Any, **params: Any) -> list[object]:
    if hasattr(database, "all"):
        return list(await database.all(statement, **params))
    return list((await database.execute(statement, params)).all())


def _state_sql(schema_name: str, *, lock: bool) -> str:
    suffix = " FOR UPDATE" if lock else ""
    return (
        "SELECT importer_id, local_lineage_id, local_generation, origin_lineage_id, "
        "origin_generation, published_at, relation_oids "
        f"FROM {_quoted(schema_name)}.{_quoted(TABLE_NAME)} "
        "WHERE importer_id=:importer_id" + suffix
    )


def _authority_schema(importer_id: str, serving_schema: str) -> str:
    """Keep the static TIGER ledger in the application schema, without TIGER DDL grants."""

    if importer_id != "tiger":
        return serving_schema
    if serving_schema != "tiger":
        raise ValueError("TIGER serving schema must be tiger")
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise ValueError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema")
    authority_schema = _schema_name(runtime_schema or legacy_schema or "mrf")
    if authority_schema == "tiger":
        raise ValueError("TIGER authority requires a separate application schema")
    return authority_schema


async def read_reference_family_result_generation_authority(
    database: Any,
    *,
    importer_id: str,
    schema_name: str,
    lock: bool = False,
) -> ReferenceFamilyResultGenerationAuthority:
    """Read and optionally lock one closed family authority row."""

    importer = _importer_id(importer_id)
    schema = _schema_name(schema_name)
    row = await _first(database, text(_state_sql(_authority_schema(importer, schema), lock=lock)), importer_id=importer)
    if row is None:
        raise RuntimeError("reference family generation authority is unavailable")
    return validate_reference_family_result_generation_authority(row)


async def current_reference_family_relation_oids(
    database: Any,
    *,
    importer_id: str,
    schema_name: str,
) -> tuple[int, ...]:
    """Resolve the fixed canonical family in model order."""

    importer = _importer_id(importer_id)
    schema = _schema_name(schema_name)
    relation_names = RELATION_NAMES_BY_IMPORTER[importer]
    relation_rows = await _all(
        database,
        text(
            "SELECT relation_name, "
            "to_regclass(format('%I.%I', CAST(:schema_name AS text), relation_name))::oid::bigint "
            "AS relation_oid FROM unnest(CAST(:relation_names AS text[])) WITH ORDINALITY "
            "AS relations(relation_name, ordinal) ORDER BY ordinal"
        ),
        schema_name=schema,
        relation_names=list(relation_names),
    )
    try:
        names = tuple(str(relation_row[0]) for relation_row in relation_rows)
        relation_oids = _relation_oids(importer, tuple(int(relation_row[1]) for relation_row in relation_rows))
    except IndexError, TypeError, ValueError:
        raise RuntimeError("reference family serving relations are unavailable") from None
    if names != relation_names:
        raise RuntimeError("reference family serving relations are unavailable")
    return relation_oids


async def capture_reference_family_serving_generation(
    database: Any,
    *,
    importer_id: str,
    schema_name: str,
) -> ReferenceFamilyServingGeneration:
    """Bind the serving generation to the exact canonical OIDs in this transaction."""

    authority = await read_reference_family_result_generation_authority(
        database, importer_id=importer_id, schema_name=schema_name
    )
    current_oids = await current_reference_family_relation_oids(
        database, importer_id=importer_id, schema_name=schema_name
    )
    if authority.serving_generation is None or authority.relation_oids != current_oids:
        raise RuntimeError("reference family serving generation is unavailable or drifted")
    return authority.serving_generation


async def publish_local_reference_family_generation(
    database: Any,
    *,
    importer_id: str,
    schema_name: str,
) -> ReferenceFamilyResultGenerationAuthority:
    """Advance one local generation inside the ordinary publication transaction."""

    importer = _importer_id(importer_id)
    schema = _schema_name(schema_name)
    current = await read_reference_family_result_generation_authority(
        database, importer_id=importer, schema_name=schema, lock=True
    )
    if current.local_generation >= _MAX_GENERATION:
        raise RuntimeError("reference family local generation is exhausted")
    relation_oids = await current_reference_family_relation_oids(database, importer_id=importer, schema_name=schema)
    next_generation = current.local_generation + 1
    updated = await _first(
        database,
        text(
            f"UPDATE {_quoted(_authority_schema(importer, schema))}.{_quoted(TABLE_NAME)} SET "
            "local_generation=:next_generation, origin_lineage_id=local_lineage_id, "
            "origin_generation=:next_generation, published_at=clock_timestamp(), "
            "relation_oids=CAST(:relation_oids AS bigint[]) WHERE importer_id=:importer_id "
            "RETURNING importer_id, local_lineage_id, local_generation, origin_lineage_id, "
            "origin_generation, published_at, relation_oids"
        ),
        importer_id=importer,
        next_generation=next_generation,
        relation_oids=list(relation_oids),
    )
    if updated is None:
        raise RuntimeError("reference family generation authority is unavailable")
    return validate_reference_family_result_generation_authority(updated)


async def publish_adopted_reference_family_generation(
    database: Any,
    *,
    importer_id: str,
    schema_name: str,
    source_generation: Mapping[str, Any] | ReferenceFamilyServingGeneration | None,
) -> ReferenceFamilyResultGenerationAuthority:
    """Preserve a source origin or explicitly clear generation-less adoption."""

    importer = _importer_id(importer_id)
    schema = _schema_name(schema_name)
    current = await read_reference_family_result_generation_authority(
        database, importer_id=importer, schema_name=schema, lock=True
    )
    if source_generation is None:
        update_by_field = {
            "origin_lineage_id": None,
            "origin_generation": None,
            "published_at": None,
            "relation_oids": None,
        }
    else:
        source_serving_generation = validate_reference_family_serving_generation(source_generation)
        update_by_field = {
            "origin_lineage_id": source_serving_generation.origin_lineage_id,
            "origin_generation": source_serving_generation.origin_generation,
            "published_at": source_serving_generation.published_at,
            "relation_oids": list(
                await current_reference_family_relation_oids(database, importer_id=importer, schema_name=schema)
            ),
        }
    updated = await _first(
        database,
        text(
            f"UPDATE {_quoted(_authority_schema(importer, schema))}.{_quoted(TABLE_NAME)} SET "
            "origin_lineage_id=CAST(:origin_lineage_id AS uuid), "
            "origin_generation=:origin_generation, published_at=CAST(:published_at AS timestamptz), "
            "relation_oids=CAST(:relation_oids AS bigint[]) WHERE importer_id=:importer_id "
            "RETURNING importer_id, local_lineage_id, local_generation, origin_lineage_id, "
            "origin_generation, published_at, relation_oids"
        ),
        importer_id=importer,
        **update_by_field,
    )
    if updated is None:
        raise RuntimeError("reference family generation authority is unavailable")
    adopted_authority = validate_reference_family_result_generation_authority(updated)
    if (
        adopted_authority.local_lineage_id != current.local_lineage_id
        or adopted_authority.local_generation != current.local_generation
    ):
        raise RuntimeError("reference family local generation changed during adoption")
    return adopted_authority


def require_reference_family_automatic_generation_order(candidate: object, incumbent: object) -> None:
    """Require a strictly newer generation from the same known origin lineage."""

    if candidate is None or incumbent is None:
        raise ValueError("automatic reference family generation order is unavailable")
    candidate_generation = validate_reference_family_serving_generation(candidate)
    incumbent_generation = validate_reference_family_serving_generation(incumbent)
    if (
        candidate_generation.origin_lineage_id != incumbent_generation.origin_lineage_id
        or candidate_generation.origin_generation <= incumbent_generation.origin_generation
    ):
        raise ValueError("automatic reference family generation order is unsupported")


__all__ = [
    "RELATION_NAMES_BY_IMPORTER",
    "ReferenceFamilyResultGenerationAuthority",
    "ReferenceFamilyServingGeneration",
    "capture_reference_family_serving_generation",
    "current_reference_family_relation_oids",
    "publish_adopted_reference_family_generation",
    "publish_local_reference_family_generation",
    "read_reference_family_result_generation_authority",
    "require_reference_family_automatic_generation_order",
    "validate_reference_family_result_generation_authority",
    "validate_reference_family_serving_generation",
]
