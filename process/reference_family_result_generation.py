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

from process.procedure_taxonomy_signals import mark_procedure_taxonomy_inputs_changed

TABLE_NAME = "reference_family_result_generation"
RELATION_NAMES_BY_IMPORTER = {
    "nucc": ("nucc_taxonomy",),
    "label": ("label",),
    "mrf": (
        "issuer",
        "plan",
        "plan_formulary",
        "plan_benefits_marketplace",
        "plan_transparency",
        "plan_drug_raw",
        "plan_drug_stats",
        "plan_drug_tier_stats",
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
    "geo-census": ("geo_zip_census_profile",),
    "lodes": ("lodes_workplace_aggregate",),
    "cms-doctors": ("doctor_clinician_address", "cms_doctor_education", "cms_doctor_group_site"),
    "facility-anchors": ("facility_anchor", "facility_address_contribution"),
    "tiger": ("zip_state", "zcta5"),
    "medicare-enrollment": (
        "medicare_enrollment_county_stats",
        "medicare_enrollment_stats",
    ),
    "pharmacy-economics": ("pharmacy_economics_summary",),
    "terminology-synonyms": ("terminology_synonym",),
    "provider-quality": (
        "pricing_qpp_provider",
        "pricing_svi_zcta",
        "pricing_provider_quality_measure",
        "pricing_provider_quality_domain",
        "pricing_provider_quality_score",
        "pricing_provider_quality_feature",
        "pricing_provider_quality_procedure_lsh",
        "pricing_provider_quality_peer_target",
    ),
}
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_MAX_GENERATION = (1 << 63) - 1
_MAX_OID = (1 << 32) - 1
_FROZEN_FACILITY_PUBLICATION = "reference-family.facility-anchors.read-only.v1"


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
    relations: tuple[tuple[str, str], ...] | None

    def as_dict(self) -> dict[str, Any]:
        """Return local and portable authority fields for durable receipts."""

        return {
            "importer_id": self.importer_id,
            "local_lineage_id": self.local_lineage_id,
            "local_generation": self.local_generation,
            "serving_generation": (None if self.serving_generation is None else self.serving_generation.as_dict()),
            "relations": None
            if self.relations is None
            else [{"schema_name": schema, "relation_name": name} for schema, name in self.relations],
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


def declared_reference_family_relations(importer_id, schema_name):
    """Return each existing family's configured ordered serving names."""
    return tuple((_schema_name(schema_name), name) for name in RELATION_NAMES_BY_IMPORTER[_importer_id(importer_id)])


def _authority_relations(generation_row, importer, declared_schema):
    if "serving_relation_names" in generation_row:
        schema = _schema_name(generation_row["serving_schema"])
        names = tuple(generation_row["serving_relation_names"])
        relations = tuple((schema, name) for name in names)
    elif "relations" in generation_row:
        relations = tuple((entry["schema_name"], entry["relation_name"]) for entry in generation_row["relations"])
        schema = _schema_name(relations[0][0])
    else:
        if declared_schema is None:
            raise ValueError("recorded serving schema declaration is required")
        schema = _schema_name(declared_schema)
        relations = declared_reference_family_relations(importer, schema)
    if relations != declared_reference_family_relations(importer, schema):
        raise ValueError
    return relations


def validate_reference_family_result_generation_authority(
    authority_row: object, *, declared_schema=None
) -> ReferenceFamilyResultGenerationAuthority:
    """Validate publication revision and its configured logical serving location."""
    generation_row = _row_mapping(authority_row)
    try:
        if ("serving_schema" in generation_row) != ("serving_relation_names" in generation_row):
            raise ValueError("incomplete named serving generation")
        importer = _importer_id(generation_row.get("importer_id"))
        lineage = _uuid_text(generation_row.get("local_lineage_id"))
        local = generation_row.get("local_generation")
        if type(local) is not int or not 0 <= local <= _MAX_GENERATION:
            raise ValueError
        origin_values = tuple(
            generation_row.get(key) for key in ("origin_lineage_id", "origin_generation", "published_at")
        )
        if all(field is None for field in origin_values):
            if (
                generation_row.get("serving_schema") is not None
                or generation_row.get("serving_relation_names") is not None
                or generation_row.get("relations") is not None
            ):
                raise ValueError
            serving, relations = None, None
        elif any(field is None for field in origin_values):
            raise ValueError
        else:
            serving = validate_reference_family_serving_generation(
                dict(zip(("origin_lineage_id", "origin_generation", "published_at"), origin_values, strict=True))
            )
            relations = _authority_relations(generation_row, importer, declared_schema)
        return ReferenceFamilyResultGenerationAuthority(importer, lineage, local, serving, relations)
    except (AttributeError, IndexError, KeyError, TypeError, ValueError) as error:
        raise RuntimeError(
            "reference family serving generation is invalid or its schema declaration is missing"
        ) from error


async def _first(database: Any, statement: Any, **params: Any) -> object | None:
    if hasattr(database, "first"):
        return await database.first(statement, **params)
    return (await database.execute(statement, params)).mappings().one_or_none()


async def _all(database: Any, statement: Any, **params: Any) -> list[object]:
    if hasattr(database, "all"):
        return list(await database.all(statement, **params))
    return list((await database.execute(statement, params)).all())


def _state_sql(schema_name: str, *, lock: bool, named: bool = True) -> str:
    suffix = " FOR UPDATE" if lock else ""
    named_columns = ", serving_schema, serving_relation_names" if named else ""
    return (
        "SELECT importer_id, local_lineage_id, local_generation, origin_lineage_id, "
        f"origin_generation, published_at{named_columns} "
        f"FROM {_quoted(schema_name)}.{_quoted(TABLE_NAME)} "
        "WHERE importer_id=:importer_id" + suffix
    )


async def _has_named_generation_columns(database, schema):
    """Select the live metadata edition using only the reader's required columns."""
    relation = f"{_quoted(schema)}.{_quoted(TABLE_NAME)}"
    await _all(database, text(f"SELECT local_generation FROM {relation} WHERE FALSE"))
    columns = {
        row[0]
        for row in await _all(
            database,
            text(
                "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid=to_regclass(:relation) "
                "AND attnum>0 AND NOT attisdropped AND attname IN ('serving_schema','serving_relation_names')"
            ),
            relation=relation,
        )
    }
    if columns not in (set(), {"serving_schema", "serving_relation_names"}):
        raise RuntimeError("reference family serving generation is invalid")
    return bool(columns)


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
    if importer == "label":
        from drug_snapshot_runtime.publication import read_result_publication_authority

        return _label_authority(
            await read_result_publication_authority(database, importer_id=importer, schema=schema, lock=lock)
        )
    authority_schema = _authority_schema(importer, schema)
    named = await _has_named_generation_columns(database, authority_schema)
    row = await _first(database, text(_state_sql(authority_schema, lock=lock, named=named)), importer_id=importer)
    if row is None:
        raise RuntimeError("reference family generation authority is unavailable")
    return validate_reference_family_result_generation_authority(row, declared_schema=schema)


def _label_authority(authority):
    """Preserve the shared generation shape while using the Drug-owned ledger."""
    serving = authority.serving_generation
    return ReferenceFamilyResultGenerationAuthority(
        authority.importer_id,
        authority.local_lineage_id,
        authority.local_generation,
        None if serving is None else validate_reference_family_serving_generation(serving.as_dict()),
        None if serving is None else declared_reference_family_relations("label", "rx_data"),
    )


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
    """Read the generation for the declared qualified relations in the caller transaction."""

    authority = await read_reference_family_result_generation_authority(
        database, importer_id=importer_id, schema_name=schema_name
    )
    if importer_id == "nucc":
        storage = await require_nucc_native_predecessor_storage(database, schema_name=schema_name)
        relation = storage["child"] if storage is not None and "child" in storage else (schema_name, "nucc_taxonomy")
        if authority.serving_generation is None or authority.relations != (relation,):
            raise RuntimeError("NUCC serving generation is unavailable or its selected child changed")
        return authority.serving_generation
    await current_reference_family_relation_oids(database, importer_id=importer_id, schema_name=schema_name)
    if authority.serving_generation is None or authority.relations != declared_reference_family_relations(
        importer_id, schema_name
    ):
        raise RuntimeError("reference family serving generation is unavailable or drifted")
    if importer_id == "geo":
        from process.geo_source_storage import capture_geo_generation

        return await capture_geo_generation(database, schema_name)
    if importer_id == "cms-doctors":
        from process.cms_doctors_immutable import require_doctors_storage

        await require_doctors_storage(database, schema_name=schema_name, legacy=True)
    elif importer_id != "label":
        await require_reference_family_source_consistency(database, importer_id=importer_id, schema_name=schema_name)
    return authority.serving_generation


async def require_reference_family_source_consistency(database, *, importer_id, schema_name):
    """Require declared read-only Facility storage or the mutable revision guard."""
    if importer_id == "facility-anchors":
        schema = _quoted(_schema_name(schema_name))
        row = await _first(
            database,
            text(
                "SELECT NOT source_revision_tracked AND obj_description(to_regclass(:relation),'pg_class')=:kind AS frozen "
                f"FROM {schema}.{_quoted(TABLE_NAME)} WHERE importer_id='facility-anchors'"
            ),
            relation=f'{schema}."facility_anchor"',
            kind=_FROZEN_FACILITY_PUBLICATION,
        )
        if row is not None and _row_mapping(row)["frozen"] is True:
            return
    from process.reference_source_generation import require_reference_revision_tracking

    await require_reference_revision_tracking(database, importer_id=importer_id, schema_name=schema_name)


async def _declare_facility_publication_kind(session, schema_name, *, frozen):
    """Record the serving storage kind on the relation that moves during rollback."""
    relation = f'{_quoted(_schema_name(schema_name))}."facility_anchor"'
    if not frozen:
        existing_kind = await session.scalar(
            text("SELECT obj_description(to_regclass(:relation),'pg_class')"), {"relation": relation}
        )
        if existing_kind != _FROZEN_FACILITY_PUBLICATION:
            return
    comment = f"'{_FROZEN_FACILITY_PUBLICATION}'" if frozen else "NULL"
    await session.execute(text(f"COMMENT ON TABLE {relation} IS {comment}"))


async def publish_local_reference_family_generation(
    database: Any,
    *,
    importer_id: str,
    schema_name: str,
) -> ReferenceFamilyResultGenerationAuthority:
    """Advance one local generation inside the ordinary publication transaction."""

    importer = _importer_id(importer_id)
    schema = _schema_name(schema_name)
    if importer == "label":
        from drug_snapshot_runtime.publication import publish_local_result_generation

        return _label_authority(
            await publish_local_result_generation(
                database, importer_id="label", schema=schema, consumed_dependencies={}
            )
        )
    if importer == "cms-doctors":
        raise RuntimeError("cms_doctors_native_publication_required")
    if importer == "geo":
        raise RuntimeError("geo_source_native_publication_required")
    from process.reference_source_generation import install_reference_revision_guards

    await install_reference_revision_guards(database, importer_id=importer, schema_name=schema)
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
            "local_generation=:next_generation, origin_lineage_id=local_lineage_id, source_revision_tracked=TRUE, "
            "origin_generation=:next_generation, published_at=clock_timestamp(), "
            "serving_schema=:serving_schema, serving_relation_names=CAST(:serving_relation_names AS text[]) WHERE importer_id=:importer_id "
            "RETURNING importer_id, local_lineage_id, local_generation, origin_lineage_id, "
            "origin_generation, published_at, serving_schema, serving_relation_names"
        ),
        importer_id=importer,
        next_generation=next_generation,
        serving_schema=schema,
        serving_relation_names=list(RELATION_NAMES_BY_IMPORTER[importer]),
    )
    if updated is None:
        raise RuntimeError("reference family generation authority is unavailable")
    return validate_reference_family_result_generation_authority(updated)


async def require_immutable_nucc_storage(session, *, schema_name, expected_relation_oid=None):
    """Validate native read compatibility at the declared serving name."""
    from process import nucc_snapshot_partition as partition
    from process import reference_family_archive as native

    await _require_sealed_nucc_storage(session, schema_name)
    kind = await session.scalar(
        text("SELECT relkind::text FROM pg_class WHERE oid=to_regclass(:relation)"),
        {"relation": f'{_quoted(_schema_name(schema_name))}."nucc_taxonomy"'},
    )
    if kind == "p":
        return await partition.capture_nucc_parent(session)
    return await native._observe_nucc_native_stage(session, schema_name, "nucc_taxonomy", native_revision=True)


async def _require_sealed_nucc_storage(session, schema_name, expected_relation_oid=None):
    """Lock the declared name and retain native model/dependency compatibility."""
    from process import reference_family_archive as native

    schema = _schema_name(schema_name)
    native._require_transaction(session)
    await session.execute(text(f'LOCK TABLE ONLY {_quoted(schema)}."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT'))
    address = await session.scalar(
        text(
            "SELECT oid::bigint FROM pg_class c WHERE oid=to_regclass(:relation) AND relkind IN ('r','p') "
            "AND relpersistence='p' AND NOT relispartition AND NOT relrowsecurity AND NOT relforcerowsecurity "
            "AND NOT EXISTS(SELECT FROM pg_constraint WHERE conrelid=c.oid AND contype NOT IN ('p','n')) "
            "AND NOT EXISTS(SELECT FROM pg_index WHERE indrelid=c.oid AND (NOT indisvalid OR NOT indisready OR NOT indislive))"
        ),
        {"relation": f'{_quoted(schema)}."nucc_taxonomy"'},
    )
    if type(address) is not int or address <= 0:
        raise RuntimeError("NUCC named storage is unavailable or incompatible")
    await native._require_nucc_native_columns(session, address)
    await native.require_native_read_catalog(session, (address,))
    return (schema, "nucc_taxonomy")


async def require_nucc_native_predecessor_storage(session, *, schema_name, expected_relation_oid=None):
    """Read an immutable named generation or validate its existing legacy revision boundary."""
    from process import reference_family_archive as native
    from process.reference_source_generation import REVISION_TRIGGER, require_reference_revision_tracking

    await _require_sealed_nucc_storage(session, schema_name)
    hooks = await session.scalar(
        text(
            "SELECT ARRAY(SELECT tgname::text FROM pg_trigger WHERE tgrelid=to_regclass(:relation) AND NOT tgisinternal ORDER BY tgname)"
        ),
        {"relation": f'{_quoted(_schema_name(schema_name))}."nucc_taxonomy"'},
    )
    if hooks in ([], ["cms_native_input_mutation"]):
        return await require_immutable_nucc_storage(session, schema_name=schema_name)
    if hooks != [REVISION_TRIGGER]:
        raise RuntimeError("NUCC native predecessor hooks differ")
    await require_reference_revision_tracking(session, importer_id="nucc", schema_name=schema_name)
    return None


async def publish_immutable_nucc_generation(session, *, schema_name, expected_authority, expected_relation_oid=None):
    """CAS the sealed NUCC projection; the protected controller owns ordering authority."""
    schema = _schema_name(schema_name)
    from process import reference_family_archive as native

    native.model_family_roles()
    storage = await require_immutable_nucc_storage(session, schema_name=schema)
    serving_relation = storage["child"] if "child" in storage else (schema, "nucc_taxonomy")
    current = await read_reference_family_result_generation_authority(
        session, importer_id="nucc", schema_name=schema, lock=True
    )
    if (
        current != _nucc_result_authority(expected_authority, allow_untracked=True, declared_schema=schema)
        or current.local_generation >= _MAX_GENERATION
    ):
        raise RuntimeError("immutable NUCC generation predecessor differs")
    updated = await _first(
        session,
        text(
            f"UPDATE {_quoted(schema)}.{_quoted(TABLE_NAME)} SET local_generation=:next_generation, "
            "origin_lineage_id=local_lineage_id, origin_generation=:next_generation, published_at=clock_timestamp(), "
            "serving_schema=:serving_schema, serving_relation_names=CAST(:serving_names AS text[]), source_revision_tracked=FALSE "
            "WHERE importer_id='nucc' AND local_lineage_id=CAST(:lineage AS uuid) AND local_generation=:prior "
            "RETURNING importer_id,local_lineage_id,local_generation,origin_lineage_id,origin_generation,published_at,serving_schema,serving_relation_names"
        ),
        next_generation=current.local_generation + 1,
        prior=current.local_generation,
        lineage=current.local_lineage_id,
        serving_schema=serving_relation[0],
        serving_names=[serving_relation[1]],
    )
    if updated is None:
        raise RuntimeError("immutable NUCC generation changed")
    return validate_reference_family_result_generation_authority(updated)


async def adopt_immutable_nucc_generation(
    session, *, schema_name, source_generation, expected_authority, expected_relation_oid=None
):
    """Preserve portable NUCC provenance only on authenticated sealed destination storage."""
    schema = _schema_name(schema_name)
    from process import reference_family_archive as native

    native.model_family_roles()
    source_generation_value = validate_reference_family_serving_generation(source_generation)
    storage = await require_immutable_nucc_storage(session, schema_name=schema)
    serving_relation = storage["child"] if "child" in storage else (schema, "nucc_taxonomy")
    current = await read_reference_family_result_generation_authority(
        session, importer_id="nucc", schema_name=schema, lock=True
    )
    if current != _nucc_result_authority(expected_authority, allow_untracked=True, declared_schema=schema):
        raise RuntimeError("immutable NUCC adoption predecessor differs")
    updated = await _first(
        session,
        text(
            f"UPDATE {_quoted(schema)}.{_quoted(TABLE_NAME)} SET origin_lineage_id=CAST(:origin AS uuid), "
            "origin_generation=:generation,published_at=CAST(:published AS timestamptz), "
            "serving_schema=:serving_schema, serving_relation_names=CAST(:serving_names AS text[]),source_revision_tracked=FALSE "
            "WHERE importer_id='nucc' AND local_lineage_id=CAST(:lineage AS uuid) AND local_generation=:prior "
            "RETURNING importer_id,local_lineage_id,local_generation,origin_lineage_id,origin_generation,published_at,serving_schema,serving_relation_names"
        ),
        origin=source_generation_value.origin_lineage_id,
        generation=source_generation_value.origin_generation,
        published=source_generation_value.published_at,
        lineage=current.local_lineage_id,
        prior=current.local_generation,
        serving_schema=serving_relation[0],
        serving_names=[serving_relation[1]],
    )
    if updated is None:
        raise RuntimeError("immutable NUCC adoption changed")
    return validate_reference_family_result_generation_authority(updated)


async def _adopt_label_generation(
    database: Any,
    schema: str,
    source_generation: Mapping[str, Any] | ReferenceFamilyServingGeneration | None,
) -> ReferenceFamilyResultGenerationAuthority:
    """Bridge the shared generation contract to Label's native ledger."""

    from drug_snapshot_runtime.publication import adopt_label_generation

    source = (
        source_generation.as_dict()
        if isinstance(source_generation, ReferenceFamilyServingGeneration)
        else source_generation
    )
    return _label_authority(await adopt_label_generation(database, schema=schema, source_generation=source))


async def _adopted_generation_fields(database, importer, schema, source_generation) -> dict[str, Any]:
    """Bind adopted provenance and its exact local relation inventory."""

    if source_generation is None:
        return {
            "origin_lineage_id": None,
            "origin_generation": None,
            "published_at": None,
            "serving_schema": None,
            "serving_relation_names": None,
        }
    source = validate_reference_family_serving_generation(source_generation)
    await current_reference_family_relation_oids(database, importer_id=importer, schema_name=schema)
    return {
        "origin_lineage_id": source.origin_lineage_id,
        "origin_generation": source.origin_generation,
        "published_at": source.published_at,
        "serving_schema": schema,
        "serving_relation_names": list(RELATION_NAMES_BY_IMPORTER[importer]),
    }


async def _adopt_native_doctors_generation(database, importer, schema, source_generation, source_revision_tracked):
    """Adopt the immutable doctors generation without installing revision hooks."""
    from process.cms_doctors_immutable import adopt_doctors_generation

    if source_revision_tracked:
        raise ValueError("cms_doctors_native_publication_cannot_install_revision_hooks")
    current = await read_reference_family_result_generation_authority(
        database,
        importer_id=importer,
        schema_name=schema,
        lock=True,
    )
    return await adopt_doctors_generation(
        database,
        schema_name=schema,
        expected_authority=current,
        source_generation=source_generation,
    )


async def publish_adopted_reference_family_generation(
    database: Any,
    *,
    importer_id: str,
    schema_name: str,
    source_generation: Mapping[str, Any] | ReferenceFamilyServingGeneration | None,
    source_revision_tracked: bool = False,
) -> ReferenceFamilyResultGenerationAuthority:
    """Preserve provenance; trust revisions only with validated guarded capture."""
    importer = _importer_id(importer_id)
    schema = _schema_name(schema_name)
    if type(source_revision_tracked) is not bool or (source_revision_tracked and source_generation is None):
        raise ValueError("reference family adopted revision tracking is invalid")
    if importer == "label":
        return await _adopt_label_generation(database, schema, source_generation)
    if importer == "geo":
        from process.geo_source_storage import adopt_geo_generation

        return await adopt_geo_generation(database, schema, source_generation, source_revision_tracked)
    if importer == "cms-doctors":
        return await _adopt_native_doctors_generation(
            database, importer, schema, source_generation, source_revision_tracked
        )
    from process.reference_source_generation import install_reference_revision_guards

    if source_revision_tracked:
        await install_reference_revision_guards(database, importer_id=importer, schema_name=schema)
    current = await read_reference_family_result_generation_authority(
        database, importer_id=importer, schema_name=schema, lock=True
    )
    update_by_field = await _adopted_generation_fields(database, importer, schema, source_generation)
    updated = await _first(
        database,
        text(
            f"UPDATE {_quoted(_authority_schema(importer, schema))}.{_quoted(TABLE_NAME)} SET "
            "origin_lineage_id=CAST(:origin_lineage_id AS uuid), "
            "source_revision_tracked=:source_revision_tracked, "
            "origin_generation=:origin_generation, published_at=CAST(:published_at AS timestamptz), "
            "serving_schema=:serving_schema, serving_relation_names=CAST(:serving_relation_names AS text[]) WHERE importer_id=:importer_id "
            "RETURNING importer_id, local_lineage_id, local_generation, origin_lineage_id, "
            "origin_generation, published_at, serving_schema, serving_relation_names"
        ),
        importer_id=importer,
        source_revision_tracked=source_revision_tracked,
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


async def _precreate_nucc_attempt(
    session, schema_name, run_id, attempt_id, attempt_started_at, suffix, *, incumbent=None, incumbent_relation_oid=None
):
    """Reserve a real logical attempt, then create only its absent named heap."""
    from db import models
    from process import reference_family_archive as native
    from process.ext.utils import make_class

    stage_by_field = dict(
        contract="nucc-native-stage.v1",
        run_id=run_id,
        attempt_id=attempt_id,
        attempt_started_at=attempt_started_at,
        schema_name=native._schema_name(schema_name),
        import_date=suffix,
    )
    run = await native._nucc_locked_attempt(session, stage_by_field, ("running",))
    if run["finished_at"] is not None or (run["metrics"] or {}).get("nucc_handoff") is not None:
        raise native.ReferenceFamilyArchiveError("NUCC native stage attempt is already closed")
    if incumbent is not None:
        await _reserve_nucc_stage(session, stage_by_field, run, incumbent)
    model = make_class(models.NUCCTaxonomy, suffix)
    if await native._relation_oid(session, schema_name, model.__tablename__) is not None:
        raise native.ReferenceFamilyArchiveError("NUCC native attempt heap already exists")
    await native._create_model_heaps(
        session, native.ReferenceFamilySpec("nucc", (model,)), schema_name, create_indexes=False, ordinary_heaps=True
    )
    stage_by_field.update(
        node_id=run["node_id"],
        stage=await native._nucc_native_stage(session, schema_name, model.__tablename__),
        source_contract_sha256=native._nucc_source_contract(stage_by_field, run["params"]),
    )
    if stage_by_field["stage"]["indexes"]:
        raise native.ReferenceFamilyArchiveError("NUCC native attempt heap is not index-free")
    stage_by_field["stage_sha256"] = native.nucc_native_digest(stage_by_field)
    if incumbent is not None:
        await _persist_nucc_stage(session, stage_by_field, reservation=False)
    return stage_by_field


async def _reserve_nucc_stage(session, stage_by_field, run, incumbent, incumbent_relation_oid=None):
    """Persist the logical attempt and saved generation before candidate DDL."""
    from process import reference_family_archive as native

    await _require_nucc_native_builder(session)
    authority = native._nucc_result_authority(
        incumbent, allow_untracked=True, declared_schema=stage_by_field["schema_name"]
    )
    relation = (
        authority.relations[0] if authority.relations is not None else (stage_by_field["schema_name"], "nucc_taxonomy")
    )
    if (run["metrics"] or {}).get("nucc_native_stage") is not None:
        raise native.ReferenceFamilyArchiveError("NUCC native prior stage requires retirement")
    stage_by_field.update(
        contract="nucc-native-stage.v2",
        incumbent=incumbent,
        incumbent_relation=list(relation),
        node_id=run["node_id"],
        source_contract_sha256=native._nucc_source_contract(stage_by_field, run["params"]),
    )
    await _persist_nucc_stage(session, stage_by_field, reservation=True)


async def _persist_nucc_stage(session, stage, *, reservation):
    """Reserve the named candidate and exact attempt in the same owning transaction."""
    from process import reference_family_archive as native

    slot = "nucc_native_stage_reservation" if reservation else "nucc_native_stage"
    changed = await session.scalar(
        text(
            f"UPDATE {native._quoted(stage['schema_name'])}.import_run SET "
            "metrics=(COALESCE(metrics::jsonb,'{}'::jsonb)||jsonb_build_object(CAST(:slot AS text),CAST(:receipt AS jsonb)))::json "
            "WHERE run_id=:run_id AND node_id=:node_id AND importer='nucc' AND engine='healthcare-mrf-api' "
            "AND status='running' AND finished_at IS NULL AND error IS NULL "
            "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
            "AND metrics->'nucc_native_stage' IS NULL AND metrics->'nucc_handoff' IS NULL RETURNING run_id"
        ),
        {**stage, "slot": slot, "receipt": native._canonical_json(stage).decode("ascii")},
    )
    if changed != stage["run_id"]:
        raise native.ReferenceFamilyArchiveError("NUCC native stage reservation changed")


async def _require_nucc_native_builder(session):
    """Keep the configured namespace policy; PostgreSQL enforces actual candidate writes."""
    from process import reference_family_archive as native

    native._require_transaction(session)
    native.model_family_roles()
    await session.execute(text('SELECT run_id FROM "mrf".import_run WHERE false'))


_NUCC_NATIVE_STAGE_FIELDS = frozenset(
    {
        "attempt_id",
        "attempt_started_at",
        "contract",
        "import_date",
        "node_id",
        "run_id",
        "schema_name",
        "source_contract_sha256",
        "stage",
        "stage_sha256",
    }
)


def _nucc_precreated_stage_value(stage_by_field):
    """Decode named stages and retain exact historical receipt editions opaquely."""
    from process import reference_family_archive as native

    if len(native._canonical_json(stage_by_field)) > 131072:
        raise native.ReferenceFamilyArchiveError("NUCC native stage metadata exceeds its bound")
    if type(stage_by_field) is not dict or stage_by_field.get("contract") not in {
        "nucc-native-stage.v1",
        "nucc-native-stage.v2",
    }:
        raise native.ReferenceFamilyArchiveError("NUCC native stage fields differ")
    is_legacy = "database_oid" in stage_by_field
    fields = _NUCC_NATIVE_STAGE_FIELDS | ({"database_oid", "import_run_oid"} if is_legacy else set())
    if stage_by_field["contract"] == "nucc-native-stage.v2":
        fields |= {"incumbent", "incumbent_relation_oid" if is_legacy else "incumbent_relation"}
    if set(stage_by_field) != fields:
        raise native.ReferenceFamilyArchiveError("NUCC native stage fields differ")
    if (
        type(stage_by_field["node_id"]) is not str
        or not 0 < len(stage_by_field["node_id"]) <= 128
        or stage_by_field["node_id"] != stage_by_field["node_id"].strip()
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native stage node differs")
    if "incumbent" in stage_by_field:
        authority = native._nucc_result_authority(
            stage_by_field["incumbent"], allow_untracked=True, declared_schema=stage_by_field["schema_name"]
        )
        relation = (
            authority.relations[0]
            if authority.relations is not None
            else (stage_by_field["schema_name"], "nucc_taxonomy")
        )
        if not is_legacy and stage_by_field["incumbent_relation"] != list(relation):
            raise native.ReferenceFamilyArchiveError("NUCC native incumbent name differs")
    _validate_nucc_empty_stage_catalog(stage_by_field)
    if is_legacy != ("relation_oid" in stage_by_field["stage"]):
        raise native.ReferenceFamilyArchiveError("NUCC native stage edition differs")
    _run, _attempt, _started, suffix = native._nucc_attempt(
        {
            "control_run_id": stage_by_field["run_id"],
            "context": {
                "_control_attempt_id": stage_by_field["attempt_id"],
                "_control_attempt_started_at": stage_by_field["attempt_started_at"],
            },
        }
    )
    if stage_by_field["import_date"] != suffix or stage_by_field["stage"]["table_name"] != "nucc_taxonomy_" + suffix:
        raise native.ReferenceFamilyArchiveError("NUCC native stage attempt differs")
    for key in ("stage_sha256", "source_contract_sha256"):
        if type(stage_by_field[key]) is not str or re.fullmatch(r"[0-9a-f]{64}", stage_by_field[key]) is None:
            raise native.ReferenceFamilyArchiveError("NUCC native stage digest differs")
    if (
        not is_legacy
        and native.nucc_native_digest(
            {key: field_value for key, field_value in stage_by_field.items() if key != "stage_sha256"}
        )
        != stage_by_field["stage_sha256"]
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native stage digest differs")
    return stage_by_field


def _validate_nucc_empty_stage_catalog(stage_by_field):
    from process import reference_family_archive as native

    stage = stage_by_field["stage"]
    if (
        native._schema_name(stage_by_field["schema_name"]) != stage_by_field["schema_name"]
        or type(stage) is not dict
        or set(stage)
        not in ({"table_name", "indexes"}, {"table_name", "indexes", "relation_oid", "relfilenode", "owner_oid"})
        or stage["indexes"] != []
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native stage catalog is invalid")


async def _require_nucc_precreated_stage(session, stage_by_field):
    """Require the persisted logical attempt and its native empty-index model heap."""
    from process import reference_family_archive as native

    stage_by_field = _nucc_precreated_stage_value(stage_by_field)
    run = await native._nucc_locked_attempt(session, stage_by_field, ("running",))
    await native._require_nucc_native_location(session, stage_by_field, run)
    if run["finished_at"] is not None or (run["metrics"] or {}).get("nucc_handoff") is not None:
        raise native.ReferenceFamilyArchiveError("NUCC native stage attempt is closed")
    if (
        stage_by_field["contract"] == "nucc-native-stage.v2"
        and (run["metrics"] or {}).get("nucc_native_stage") != stage_by_field
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native persisted stage differs")
    observed = await native._nucc_native_stage(
        session, stage_by_field["schema_name"], stage_by_field["stage"]["table_name"]
    )
    if observed["table_name"] != stage_by_field["stage"]["table_name"] or observed["indexes"]:
        raise native.ReferenceFamilyArchiveError("NUCC native stage catalog differs")


async def cleanup_nucc_native_stage(session, stage_receipt, *, runtime_owner_oids=None, assert_unreferenced):
    """Retire only a named abandoned, unreferenced attempt in the original transaction."""
    from process import reference_family_archive as native

    native._require_transaction(session)
    stage = _nucc_precreated_stage_value(stage_receipt)
    if stage["contract"] != "nucc-native-stage.v2" or not callable(assert_unreferenced):
        raise native.ReferenceFamilyArchiveError("NUCC trusted stage cleanup is required")
    async with session.begin_nested():
        await session.connection()
        transaction = session.get_transaction()
        handoff = await _require_nucc_stage_retirement(session, stage)
        observed = await _require_nucc_stage_cleanup_storage(session, stage, handoff)
        if await assert_unreferenced(session, stage) is not True:
            raise native.ReferenceFamilyArchiveError("NUCC native stage remains referenced")
        if (
            await assert_unreferenced(session, stage) is not True
            or not session.in_transaction()
            or session.get_transaction() is not transaction
        ):
            raise native.ReferenceFamilyArchiveError("NUCC native stage cleanup transaction changed")
        await session.execute(
            text(f"DROP TABLE {native._quoted(stage['schema_name'])}.{native._quoted(observed['table_name'])} RESTRICT")
        )
        return await _record_nucc_stage_cleanup(session, stage, handoff, observed)


async def _require_nucc_stage_retirement(session, stage_by_field):
    """Lock and corroborate actual terminal/cancel/replacement facts, never a supplied reason."""
    from process import reference_family_archive as native

    run = (
        (
            await session.execute(
                text(
                    f"SELECT node_id,engine,importer,status,progress,metrics,params,finished_at,error,phase_detail FROM {native._quoted(stage_by_field['schema_name'])}.import_run "
                    "WHERE run_id=:run_id AND octet_length(params::text)<=131072 AND octet_length(metrics::text)<=393216 FOR UPDATE NOWAIT"
                ),
                {"run_id": stage_by_field["run_id"]},
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        run is None
        or run["engine"] != "healthcare-mrf-api"
        or run["importer"] != "nucc"
        or type(run["metrics"]) is not dict
        or type(run["progress"]) is not dict
        or type(run["params"]) is not dict
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native stage cleanup run differs")
    await native._require_nucc_native_location(session, stage_by_field, run)
    metrics_by_field = run["metrics"]
    if (
        metrics_by_field.get("nucc_native_stage") != stage_by_field
        or metrics_by_field.get("nucc_native_publication") is not None
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native stage cleanup custody differs")
    handoff_by_field = metrics_by_field.get("nucc_handoff")
    if handoff_by_field is not None:
        handoff_by_field = native.validate_nucc_native_handoff(handoff_by_field)
        if (
            handoff_by_field["contract"] != native.NUCC_IMMUTABLE_HANDOFF_CONTRACT
            or handoff_by_field["precreated_stage"] != stage_by_field
        ):
            raise native.ReferenceFamilyArchiveError("NUCC native cleanup handoff differs")
    _require_nucc_abandoned_attempt(stage_by_field, run)
    return handoff_by_field


def _require_nucc_abandoned_attempt(stage_by_field, run):
    """A replacement must itself be an authentic running controlled attempt."""
    from process import reference_family_archive as native

    is_original_attempt = (run["progress"].get("attempt_id"), run["progress"].get("attempt_started_at")) == (
        stage_by_field["attempt_id"],
        stage_by_field["attempt_started_at"],
    )
    is_terminal = run["status"] in {"failed", "canceled", "cancelled"} and run["finished_at"] is not None
    is_canceling = (
        run["status"] == "canceling"
        and run["finished_at"] is None
        and run["phase_detail"] == run["progress"].get("message") == "cancel requested"
    )
    is_replaced = (
        not is_original_attempt and run["status"] == "running" and run["finished_at"] is None and run["error"] is None
    )
    if is_replaced:
        _nucc_attempt(
            {
                "control_run_id": stage_by_field["run_id"],
                "context": {
                    "_control_attempt_id": run["progress"].get("attempt_id"),
                    "_control_attempt_started_at": run["progress"].get("attempt_started_at"),
                },
            }
        )
    if not ((is_original_attempt and (is_terminal or is_canceling)) or is_replaced):
        raise native.ReferenceFamilyArchiveError("NUCC native stage is not abandoned")


async def _require_nucc_stage_cleanup_storage(session, stage_by_field, handoff_by_field, owner_oid=None):
    """Lock the declared candidate and retain native model/index/serving exclusions."""
    from process import reference_family_archive as native

    stage = stage_by_field["stage"] if handoff_by_field is None else handoff_by_field["stage"]
    await session.execute(
        text(
            f"LOCK TABLE ONLY {native._quoted(stage_by_field['schema_name'])}.{native._quoted(stage['table_name'])} IN ACCESS SHARE MODE NOWAIT"
        )
    )
    if handoff_by_field is not None:
        await native._require_nucc_handoff_stage(session, handoff_by_field)
    else:
        observed = await native._nucc_native_stage(session, stage_by_field["schema_name"], stage["table_name"])
        if observed["table_name"] != stage["table_name"] or observed["indexes"]:
            raise native.ReferenceFamilyArchiveError("NUCC native stage cleanup layout differs")
    await native._require_nucc_cleanup_custody(session, {**stage_by_field, "stage": stage})
    return stage


async def _record_nucc_stage_cleanup(session, stage_by_field, handoff_by_field, physical_stage):
    """Retire slots only after the exact heap DROP in the same owning transaction."""
    from process import reference_family_archive as native

    receipt_by_field = {
        "contract": "nucc-native-stage-cleanup.v1",
        "stage_sha256": stage_by_field["stage_sha256"],
        "stage": physical_stage,
        "physical_cleanup_completed": True,
    }
    if handoff_by_field is not None:
        receipt_by_field["handoff_sha256"] = handoff_by_field["handoff_sha256"]
    changed = await session.scalar(
        text(
            f"UPDATE {native._quoted(stage_by_field['schema_name'])}.import_run SET metrics=((metrics::jsonb-'nucc_native_stage'-'nucc_native_stage_reservation'-'nucc_handoff')||"
            "jsonb_build_object('nucc_native_stage_cleanups',COALESCE(metrics::jsonb->'nucc_native_stage_cleanups','{}'::jsonb)||jsonb_build_object(CAST(:digest AS text),CAST(:receipt AS jsonb))))::json "
            "WHERE run_id=:run_id AND metrics::jsonb->'nucc_native_stage'=CAST(:stage AS jsonb) "
            "AND metrics::jsonb->'nucc_handoff' IS NOT DISTINCT FROM CAST(:handoff AS jsonb) RETURNING run_id"
        ),
        {
            "run_id": stage_by_field["run_id"],
            "digest": stage_by_field["stage_sha256"],
            "stage": native._canonical_json(stage_by_field).decode("ascii"),
            "handoff": None if handoff_by_field is None else native._canonical_json(handoff_by_field).decode("ascii"),
            "receipt": native._canonical_json(receipt_by_field).decode("ascii"),
        },
    )
    if changed != stage_by_field["run_id"]:
        raise native.ReferenceFamilyArchiveError("NUCC native stage cleanup fence changed")
    return receipt_by_field


def _nucc_attempt(ctx):
    """Require the actual controlled worker attempt, not a shared date suffix."""
    import base64

    from process import reference_family_archive as native

    context = ctx.get("context") or {}
    run_id = ctx.get("control_run_id") or context.get("control_run_id")
    attempt = context.get("_control_attempt_id")
    started = context.get("_control_attempt_started_at")
    if (
        type(run_id) is not str
        or not 0 < len(run_id) <= 128
        or type(attempt) is not str
        or re.fullmatch(re.escape(run_id) + r":[0-9a-f]{32}", attempt) is None
        or type(started) is not str
        or len(started) > 64
        or context.get("test_mode")
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native controlled attempt is invalid")
    try:
        if datetime.datetime.fromisoformat(started).tzinfo is None:
            raise ValueError
    except ValueError:
        raise native.ReferenceFamilyArchiveError("NUCC native controlled attempt time is invalid") from None
    suffix = base64.b32encode(UUID(attempt.rsplit(":", 1)[1]).bytes).decode("ascii").lower().rstrip("=")
    return run_id, attempt, started, suffix


def _nucc_result_authority(authority_by_field, *, allow_untracked=False, declared_schema=None, legacy_read=False):
    """Validate exact logical provenance; historical physical edition values remain opaque."""
    from process import reference_family_archive as native

    fields = {"importer_id", "local_lineage_id", "local_generation", "serving_generation"}
    if not isinstance(authority_by_field, Mapping) or set(authority_by_field) not in (
        fields | {"relations"},
        fields | {"relation_oids"},
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native predecessor differs")
    serving = authority_by_field["serving_generation"]
    if serving is None:
        if not allow_untracked or authority_by_field["local_generation"] != 0:
            raise native.ReferenceFamilyArchiveError("NUCC native generation is untracked")
        origin_by_field = dict(origin_lineage_id=None, origin_generation=None, published_at=None)
    else:
        try:
            validate_reference_family_serving_generation(serving)
        except (TypeError, ValueError) as error:
            raise native.ReferenceFamilyArchiveError("NUCC native predecessor differs") from error
        origin_by_field = serving
    if serving is not None and "relation_oids" in authority_by_field and declared_schema is None:
        if not legacy_read:
            raise native.ReferenceFamilyArchiveError(
                "transition-legacy-nucc-generation: recorded serving name is required"
            )
        local = authority_by_field["local_generation"]
        if type(local) is not int or not 0 <= local <= _MAX_GENERATION or local == 0 and not allow_untracked:
            raise native.ReferenceFamilyArchiveError("NUCC native predecessor differs")
        try:
            importer = _importer_id(authority_by_field["importer_id"])
            lineage = _uuid_text(authority_by_field["local_lineage_id"])
        except (TypeError, ValueError, RuntimeError) as error:
            raise native.ReferenceFamilyArchiveError("NUCC native predecessor differs") from error
        if importer != "nucc":
            raise native.ReferenceFamilyArchiveError("NUCC native predecessor differs")
        return ReferenceFamilyResultGenerationAuthority(
            importer, lineage, local, validate_reference_family_serving_generation(serving), None
        )
    try:
        authority = validate_reference_family_result_generation_authority(
            {**authority_by_field, **origin_by_field}, declared_schema=declared_schema
        )
    except (TypeError, ValueError, RuntimeError) as error:
        raise native.ReferenceFamilyArchiveError("NUCC native predecessor differs") from error
    if authority.importer_id != "nucc" or (authority.local_generation <= 0 and not allow_untracked):
        raise native.ReferenceFamilyArchiveError("NUCC native predecessor differs")
    return authority


async def _publish_nucc_handoff_generation(session, handoff):
    """Advance the logical generation of an already frozen named publication."""
    schema = handoff["schema_name"]
    published = await publish_immutable_nucc_generation(
        session, schema_name=schema, expected_authority=handoff["incumbent"]
    )
    await mark_procedure_taxonomy_inputs_changed(
        schema, lambda statement: session.execute(text(statement)), "nucc_taxonomy"
    )
    return published


async def _retire_nucc_native_counter(session, schema, roles):
    """Close the retired heap, then remove only its exact existing native mutation guard."""
    from process import provider_directory_cms_native_registration as registration
    from process import reference_family_archive as native
    from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot

    driver = await native._native_model_copy_driver(
        session, ("fetch", "fetchrow", "fetchval", "execute", "is_in_transaction")
    )
    qualified = native._quoted(schema) + '."nucc_taxonomy"'
    hook_query = (
        "SELECT tgname::text FROM pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal ORDER BY tgname"
    )
    hooks = await driver.fetch(hook_query, qualified)
    names = [hook["tgname"] for hook in hooks]
    if "cms_native_input_mutation" not in names:
        return
    if names != ["cms_native_input_mutation"]:
        raise native.ReferenceFamilyArchiveError("NUCC native predecessor hooks differ")
    owner, readers = native.model_family_roles(roles)
    async with session.begin_nested():
        await freeze_fhir_source_snapshot(
            driver,
            schema,
            namespace_owner=owner,
            reader_roles=readers,
            application_roles=(),
            relation_names=("nucc_taxonomy",),
            freeze_schema=False,
        )
        if [hook["tgname"] for hook in await driver.fetch(hook_query, qualified)] != names:
            raise native.ReferenceFamilyArchiveError("NUCC native predecessor hooks differ")
        evidence = await registration.capture_native_input_adoption(
            driver,
            ledger_schema=schema,
            candidate=registration.NativeInputCandidate(schema, "nucc_taxonomy", schema, "nucc_taxonomy"),
        )
        if evidence is None:
            raise native.ReferenceFamilyArchiveError("NUCC native predecessor guard changed")
        await driver.execute(f'DROP TRIGGER "cms_native_input_mutation" ON {qualified}')
        await registration.verify_retired_native_input_adoption(driver, evidence=evidence)


async def _rename_nucc_candidate_indexes(session, ownership, roles):
    """Temporarily permit private candidate index DDL, then restore its frozen namespace."""
    from process import reference_family_archive as native

    owner, _readers = native.model_family_roles(roles)
    schema = native._quoted(ownership.schema_name)
    async with session.begin_nested():
        await session.connection()
        await session.execute(text(f"GRANT CREATE ON SCHEMA {schema} TO {native._quoted(owner)}"))
        await native.rename_nucc_published_indexes(session, schema_name=ownership.schema_name)
        await session.execute(text(f"REVOKE CREATE ON SCHEMA {schema} FROM {native._quoted(owner)}"))


async def _freeze_nucc_predecessor(session, schema_name, roles):
    """Close the retained namespace and preserve only configured reader access after its move."""
    from process import reference_family_archive as native
    from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot

    owner, readers = native.model_family_roles(roles)
    driver = await native._native_model_copy_driver(session, ("execute", "fetch", "fetchval"))
    await driver.execute(f"ALTER SCHEMA {native._quoted(schema_name)} OWNER TO {native._quoted(owner)}")
    await freeze_fhir_source_snapshot(
        driver,
        schema_name,
        namespace_owner=owner,
        reader_roles=readers,
        application_roles=(),
        relation_names=("nucc_taxonomy",),
    )


async def _rotate_family_relations(session, spec, ownership, expected_incumbent, *, roles=None) -> str | None:
    """Preserve incumbent reads and exchange the already locked, verified family heaps."""
    from process import reference_family_archive as native

    if spec.importer_id == "mrf":
        ordinary_predecessors = ", ".join(
            f"{native._quoted(expected_incumbent.schema_name)}.{native._quoted(table_name + '_old')}"
            for table_name in spec.table_names
        )
        await session.execute(text(f"DROP TABLE IF EXISTS {ordinary_predecessors} RESTRICT"))
    incumbent_oids_by_name = dict(expected_incumbent.relation_oids)
    if spec.importer_id == "nucc" and any(oid is not None for oid in incumbent_oids_by_name.values()):
        await _retire_nucc_native_counter(session, expected_incumbent.schema_name, roles)
    if spec.importer_id == "nucc":
        await _rename_nucc_candidate_indexes(session, ownership, roles)
    predecessor_schema = None
    if any(oid is not None for oid in incumbent_oids_by_name.values()):
        predecessor_schema = native.reference_family_predecessor_schema(ownership.dataset_id)
        if spec.importer_id == "geo":
            await native.preserve_geo_cut_reads(session, ownership, expected_incumbent)
        elif spec.importer_id == "cms-doctors":
            from process.cms_doctors_immutable import _archive_writer
            from process.native_publication_read_access import preserve_publication_reads

            await _archive_writer(session, spec.importer_id, expected_incumbent.schema_name)
            driver = await native._native_model_copy_driver(session, ("execute", "fetch"))
            for name in spec.table_names:
                await preserve_publication_reads(
                    driver,
                    schema=expected_incumbent.schema_name,
                    live_table=name,
                    stage_schema=ownership.schema_name,
                    stage_table=name,
                )
        # Never infer ownership from a UUID-shaped name or replace content
        # already present there: CREATE is the collision/CAS fence.
        await native.create_model_family_predecessor_schema(session, spec.importer_id, predecessor_schema)
    await _move_family_heaps(session, spec, ownership, expected_incumbent, predecessor_schema)
    if spec.importer_id == "nucc" and predecessor_schema is not None:
        await _freeze_nucc_predecessor(session, predecessor_schema, roles)
    return predecessor_schema


async def _move_family_heaps(session, spec, ownership, expected_incumbent, predecessor_schema):
    """Exchange live and stage heaps; a frozen cutover shares one wait across its live-table locks."""
    from process import reference_family_archive as native

    incumbent_oids_by_name = dict(expected_incumbent.relation_oids)
    # A lock timeout aborts the transaction, so the caller rolls back the queued lock before retrying.
    deadline = native.cutover_wait_deadline()
    for table_name in spec.table_names:
        incumbent_oid = incumbent_oids_by_name[table_name]
        if incumbent_oid is not None:
            await native.bound_cutover_wait(session, deadline)
            await session.execute(
                text(
                    f"ALTER TABLE {native._quoted(expected_incumbent.schema_name)}.{native._quoted(table_name)} "
                    f"SET SCHEMA {native._quoted(predecessor_schema)}"
                )
            )
        await native.bound_cutover_wait(session, deadline)
        await session.execute(
            text(
                f"ALTER TABLE {native._quoted(ownership.schema_name)}.{native._quoted(table_name)} "
                f"SET SCHEMA {native._quoted(expected_incumbent.schema_name)}"
            )
        )
    if deadline is not None:
        await native._set_local_timeout(session, "lock_timeout", native._LOCK_TIMEOUT)


async def _activate_verified_reference_family(
    session, spec, ownership, expected_incumbent, validated_manifest, validation, cutover, contribution_effect_receipt
):
    """Keep immutable observation, heap exchange and generation CAS in one verified authority scope."""
    from process import reference_family_archive as native

    immutable_predecessor = await native._read_immutable_activation_predecessor(
        session, validated_manifest, expected_incumbent
    )
    incoming_generation = native._activation_source_generation(validated_manifest, cutover)
    if cutover.authority == "automatic":
        await native._require_automatic_cutover_generation(
            session,
            spec,
            expected_incumbent,
            incoming_generation,
            source_capture_contract=validated_manifest.source_capture_contract,
        )
    async with native._doctors_cut_savepoint(session, spec.importer_id):
        await native._apply_validated_contribution(session, ownership, expected_incumbent, contribution_effect_receipt)
        predecessor_schema_name, live_pairs, canonical_publication = await native._complete_validated_stage_activation(
            session,
            spec,
            ownership,
            expected_incumbent,
            validated_manifest,
            **({"roles": cutover.roles} if spec.importer_id == "nucc" else {}),
        )
        published_authority = await native._adopt_validated_family_generation(
            session,
            validated_manifest,
            expected_incumbent,
            incoming_generation,
            live_pairs,
            immutable_predecessor,
            frozen=cutover.stage_read_only,
        )
    return native.ReferenceFamilyActivationReceipt(
        spec.importer_id,
        validated_manifest.source_metadata_sha256,
        tuple((name, int(relation_oid)) for name, relation_oid in live_pairs),
        expected_incumbent.relation_oids,
        predecessor_schema_name,
        validation.tables,
        canonical_publication,
        published_authority.as_dict() if immutable_predecessor is not None else None,
    )


async def _read_immutable_activation_predecessor(session, manifest, incumbent):
    """Observe the protected predecessor before physical receive rotation."""
    from process import reference_family_archive as native

    if manifest.importer_id == "geo":
        from process.geo_source_storage import capture_geo_predecessor

        return await capture_geo_predecessor(session, incumbent.schema_name, incumbent.relation_oids)
    if manifest.importer_id == "cms-doctors":
        from process.cms_doctors_immutable import capture_doctors_predecessor

        presence_flags = [oid is not None for _, oid in incumbent.relation_oids]
        if any(presence_flags):
            if not all(presence_flags):
                raise native.ReferenceFamilyArchiveError("Doctors native predecessor is incomplete")
            return await capture_doctors_predecessor(session, incumbent.schema_name)
        authority = await read_reference_family_result_generation_authority(
            session, importer_id="cms-doctors", schema_name=incumbent.schema_name, lock=True
        )
        if authority.relations is not None:
            raise native.ReferenceFamilyArchiveError("Doctors native predecessor differs")
        return authority
    if (
        manifest.importer_id == "nucc"
        and manifest.source_capture_contract == native.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT
    ):
        return await read_reference_family_result_generation_authority(
            session, importer_id=manifest.importer_id, schema_name=incumbent.schema_name, lock=True
        )
    return None


async def _adopt_validated_family_generation(
    session, manifest, incumbent, incoming_generation, live_pairs, predecessor, *, geo_retained_cut=None, frozen=False
):
    """Preserve the validated source origin on the destination's declared serving names."""
    from process import reference_family_archive as native

    if geo_retained_cut is not None:
        from process.geo_source_storage import require, require_geo_retained_manifest

        require(predecessor is not None and manifest.importer_id == "geo", "retained_cut_scope_changed")
        require_geo_retained_manifest(geo_retained_cut, manifest, live_pairs)
    if manifest.importer_id in native.TERMINAL_CAPTURE_IMPORTERS:
        return None
    if predecessor is not None and manifest.importer_id == "geo":
        from process.geo_source_storage import publish_geo_generation

        published_authority = await publish_geo_generation(
            session,
            schema_name=incumbent.schema_name,
            source_generation=incoming_generation,
            expected_authority=predecessor,
            local=False,
            geo_retained_cut=geo_retained_cut,
        )
    elif predecessor is not None and manifest.importer_id == "cms-doctors":
        from process.cms_doctors_immutable import adopt_doctors_generation

        published_authority = await adopt_doctors_generation(
            session,
            schema_name=incumbent.schema_name,
            source_generation=incoming_generation,
            expected_authority=predecessor,
        )
    elif predecessor is not None:
        published_authority = await adopt_immutable_nucc_generation(
            session,
            schema_name=incumbent.schema_name,
            source_generation=incoming_generation,
            expected_authority=predecessor.as_dict(),
            expected_relation_oid=dict(live_pairs)["nucc_taxonomy"],
        )
    else:
        published_authority = await publish_adopted_reference_family_generation(
            session,
            importer_id=manifest.importer_id,
            schema_name=incumbent.schema_name,
            source_generation=incoming_generation,
            source_revision_tracked=(
                incoming_generation is not None
                and manifest.source_capture_contract == native.GUARDED_SOURCE_CAPTURE_CONTRACT
                and not (frozen and manifest.importer_id == "facility-anchors")
            ),
        )
    await _finish_adopted_family_publication(
        session, manifest, incumbent, live_pairs, incoming_generation, published_authority, frozen=frozen
    )
    return published_authority


async def _finish_adopted_family_publication(
    session, manifest, incumbent, live_pairs, incoming_generation, published_authority, *, frozen
):
    """Check the live binding before recording its family-specific publication state."""
    from process import reference_family_archive as native

    importer_id, schema = manifest.importer_id, incumbent.schema_name
    native._require_published_generation_binding(
        native.reference_family_spec(importer_id),
        live_pairs,
        incoming_generation,
        published_authority,
        schema_name=schema,
    )
    if importer_id == "nucc":
        await mark_procedure_taxonomy_inputs_changed(
            schema, lambda statement: session.execute(text(statement)), "nucc_taxonomy"
        )
    if importer_id == "facility-anchors":
        await _declare_facility_publication_kind(session, schema, frozen=frozen)


def _require_nucc_handoff_fields(handoff_by_field):
    from process import reference_family_archive as native

    fields = {
        "contract",
        "run_id",
        "node_id",
        "attempt_id",
        "attempt_started_at",
        "schema_name",
        "import_date",
        "stage",
        "incumbent",
        "row_count",
        "source_contract_sha256",
        "handoff_sha256",
    }
    if type(handoff_by_field) is not dict or handoff_by_field.get("contract") not in {
        native.NUCC_HANDOFF_CONTRACT,
        native.NUCC_IMMUTABLE_HANDOFF_CONTRACT,
    }:
        raise native.ReferenceFamilyArchiveError("NUCC native handoff differs")
    if "database_oid" in handoff_by_field:
        fields |= {"database_oid", "import_run_oid"}
    if handoff_by_field["contract"] == native.NUCC_IMMUTABLE_HANDOFF_CONTRACT:
        fields.add("precreated_stage")
    if set(handoff_by_field) != fields:
        raise native.ReferenceFamilyArchiveError("NUCC native handoff differs")


def _require_nucc_precreated_handoff_binding(handoff_by_field):
    """Bind the completed named candidate to its exact original logical attempt/source."""
    from process import reference_family_archive as native

    original = _nucc_precreated_stage_value(handoff_by_field["precreated_stage"])
    if (
        original["contract"] != "nucc-native-stage.v2"
        or any(
            original[key] != handoff_by_field[key]
            for key in (
                "run_id",
                "node_id",
                "attempt_id",
                "attempt_started_at",
                "schema_name",
                "import_date",
                "source_contract_sha256",
            )
        )
        or original["stage"]["table_name"] != handoff_by_field["stage"]["table_name"]
        or _nucc_result_authority(original["incumbent"], allow_untracked=True, declared_schema=original["schema_name"])
        != _nucc_result_authority(
            handoff_by_field["incumbent"], allow_untracked=True, declared_schema=original["schema_name"]
        )
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native original attempt differs")


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
