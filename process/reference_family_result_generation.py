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
    if importer == "label":
        from drug_snapshot_runtime.publication import read_result_publication_authority

        return _label_authority(
            await read_result_publication_authority(database, importer_id=importer, schema=schema, lock=lock)
        )
    row = await _first(database, text(_state_sql(_authority_schema(importer, schema), lock=lock)), importer_id=importer)
    if row is None:
        raise RuntimeError("reference family generation authority is unavailable")
    return validate_reference_family_result_generation_authority(row)


def _label_authority(authority):
    """Preserve the shared generation shape while using the Drug-owned ledger."""
    serving = authority.serving_generation
    return ReferenceFamilyResultGenerationAuthority(
        authority.importer_id,
        authority.local_lineage_id,
        authority.local_generation,
        None if serving is None else validate_reference_family_serving_generation(serving.as_dict()),
        authority.relation_oids,
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
    """Bind the serving generation to the exact canonical OIDs in this transaction."""

    authority = await read_reference_family_result_generation_authority(
        database, importer_id=importer_id, schema_name=schema_name
    )
    current_oids = await current_reference_family_relation_oids(
        database, importer_id=importer_id, schema_name=schema_name
    )
    if authority.serving_generation is None or authority.relation_oids != current_oids:
        raise RuntimeError("reference family serving generation is unavailable or drifted")
    if importer_id != "label":
        from process.reference_source_generation import require_reference_revision_tracking

        await require_reference_revision_tracking(database, importer_id=importer_id, schema_name=schema_name)
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
    if importer == "label":
        from drug_snapshot_runtime.publication import publish_local_result_generation

        return _label_authority(
            await publish_local_result_generation(
                database, importer_id="label", schema=schema, consumed_dependencies={}
            )
        )
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


async def require_immutable_nucc_storage(session, *, schema_name, expected_relation_oid):
    """Authenticate sealed native storage, not a caller's immutable marker."""
    from process import reference_family_archive as native

    owner_oid = await _require_sealed_nucc_storage(session, schema_name, expected_relation_oid)
    stage = await native._nucc_native_stage(session, schema_name, "nucc_taxonomy")
    if stage["relation_oid"] != expected_relation_oid or stage["owner_oid"] != owner_oid:
        raise RuntimeError("immutable NUCC storage identity differs")
    return stage


async def _require_sealed_nucc_storage(session, schema_name, expected_relation_oid):
    """Share native payload custody checks without relaxing trigger-free candidates."""
    from process import reference_family_archive as native
    from process.entity_address_snapshot_preparation import _require_no_untrusted_mutation
    from process.mrf_address_publication import require_native_read_catalog

    schema = _schema_name(schema_name)
    if not session.in_transaction() or type(expected_relation_oid) is not int or expected_relation_oid <= 0:
        raise RuntimeError("immutable NUCC transaction or identity is invalid")
    owner_oid = await session.scalar(
        text(
            "SELECT owner.oid::bigint FROM pg_namespace namespace JOIN pg_roles owner ON owner.oid=namespace.nspowner "
            "WHERE namespace.nspname='hp_snapshot_retention' AND NOT owner.rolcanlogin AND NOT owner.rolsuper "
            "AND NOT owner.rolcreaterole AND NOT owner.rolcreatedb AND NOT owner.rolreplication AND NOT owner.rolbypassrls"
        )
    )
    if type(owner_oid) is not int or owner_oid <= 0:
        raise RuntimeError("immutable NUCC protected owner is unavailable")
    await session.execute(text(f'LOCK TABLE ONLY {_quoted(schema)}."nucc_taxonomy" IN ACCESS SHARE MODE NOWAIT'))
    actual_oid = await session.scalar(
        text(
            "SELECT oid::bigint FROM pg_class candidate WHERE oid=to_regclass(:relation) AND relowner=:owner "
            "AND relkind='r' AND relpersistence='p' AND NOT relispartition "
            "AND NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=candidate.oid AND contype NOT IN ('p','n')) "
            "AND NOT EXISTS(SELECT 1 FROM pg_index WHERE indrelid=candidate.oid "
            "AND (NOT indisvalid OR NOT indisready OR NOT indislive))"
        ),
        {"relation": f'{_quoted(schema)}."nucc_taxonomy"', "owner": owner_oid},
    )
    if actual_oid != expected_relation_oid:
        raise RuntimeError("immutable NUCC storage identity differs")
    await native._require_nucc_native_columns(session, expected_relation_oid)
    await require_native_read_catalog(session, (expected_relation_oid,))
    await _require_no_untrusted_mutation(session, [expected_relation_oid], owner_oid)
    return owner_oid


async def require_nucc_native_predecessor_storage(session, *, schema_name, expected_relation_oid):
    """Retain an authentic old guard; never install a hook on its successor."""
    from process.reference_source_generation import require_reference_revision_tracking

    await _require_sealed_nucc_storage(session, schema_name, expected_relation_oid)
    trigger_count = await session.scalar(
        text("SELECT count(*) FROM pg_trigger WHERE tgrelid=:oid AND NOT tgisinternal"),
        {"oid": expected_relation_oid},
    )
    if trigger_count == 0:
        return await require_immutable_nucc_storage(
            session, schema_name=schema_name, expected_relation_oid=expected_relation_oid
        )
    if trigger_count != 1:
        raise RuntimeError("NUCC native predecessor hooks differ")
    await require_reference_revision_tracking(session, importer_id="nucc", schema_name=schema_name)
    return None


async def publish_immutable_nucc_generation(session, *, schema_name, expected_authority, expected_relation_oid):
    """CAS the sealed NUCC projection; the protected controller owns ordering authority."""
    schema = _schema_name(schema_name)
    from process import reference_family_archive as native

    await native.protected_publisher_owner(session)
    await require_immutable_nucc_storage(session, schema_name=schema, expected_relation_oid=expected_relation_oid)
    current = await read_reference_family_result_generation_authority(
        session, importer_id="nucc", schema_name=schema, lock=True
    )
    if current.as_dict() != expected_authority or current.local_generation >= _MAX_GENERATION:
        raise RuntimeError("immutable NUCC generation predecessor differs")
    updated = await _first(
        session,
        text(
            f"UPDATE {_quoted(schema)}.{_quoted(TABLE_NAME)} SET local_generation=:next_generation, "
            "origin_lineage_id=local_lineage_id, origin_generation=:next_generation, published_at=clock_timestamp(), "
            "relation_oids=CAST(:oids AS bigint[]), source_revision_tracked=FALSE "
            "WHERE importer_id='nucc' AND local_lineage_id=CAST(:lineage AS uuid) AND local_generation=:prior "
            "RETURNING importer_id,local_lineage_id,local_generation,origin_lineage_id,origin_generation,published_at,relation_oids"
        ),
        next_generation=current.local_generation + 1,
        prior=current.local_generation,
        lineage=current.local_lineage_id,
        oids=[expected_relation_oid],
    )
    if updated is None:
        raise RuntimeError("immutable NUCC generation changed")
    return validate_reference_family_result_generation_authority(updated)


async def adopt_immutable_nucc_generation(
    session, *, schema_name, source_generation, expected_authority, expected_relation_oid
):
    """Preserve portable NUCC provenance only on authenticated sealed destination storage."""
    schema = _schema_name(schema_name)
    from process import reference_family_archive as native

    await native.protected_publisher_owner(session)
    source_generation_value = validate_reference_family_serving_generation(source_generation)
    await require_immutable_nucc_storage(session, schema_name=schema, expected_relation_oid=expected_relation_oid)
    current = await read_reference_family_result_generation_authority(
        session, importer_id="nucc", schema_name=schema, lock=True
    )
    if current.as_dict() != expected_authority:
        raise RuntimeError("immutable NUCC adoption predecessor differs")
    updated = await _first(
        session,
        text(
            f"UPDATE {_quoted(schema)}.{_quoted(TABLE_NAME)} SET origin_lineage_id=CAST(:origin AS uuid), "
            "origin_generation=:generation,published_at=CAST(:published AS timestamptz), "
            "relation_oids=CAST(:oids AS bigint[]),source_revision_tracked=FALSE "
            "WHERE importer_id='nucc' AND local_lineage_id=CAST(:lineage AS uuid) AND local_generation=:prior "
            "RETURNING importer_id,local_lineage_id,local_generation,origin_lineage_id,origin_generation,published_at,relation_oids"
        ),
        origin=source_generation_value.origin_lineage_id,
        generation=source_generation_value.origin_generation,
        published=source_generation_value.published_at,
        lineage=current.local_lineage_id,
        prior=current.local_generation,
        oids=[expected_relation_oid],
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
            "relation_oids": None,
        }
    source = validate_reference_family_serving_generation(source_generation)
    return {
        "origin_lineage_id": source.origin_lineage_id,
        "origin_generation": source.origin_generation,
        "published_at": source.published_at,
        "relation_oids": list(
            await current_reference_family_relation_oids(database, importer_id=importer, schema_name=schema)
        ),
    }


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
    from process.reference_source_generation import install_reference_revision_guards

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
            "relation_oids=CAST(:relation_oids AS bigint[]) WHERE importer_id=:importer_id "
            "RETURNING importer_id, local_lineage_id, local_generation, origin_lineage_id, "
            "origin_generation, published_at, relation_oids"
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
    """Create only an absent attempt heap after locking the real running attempt."""
    from db import models
    from process import reference_family_archive as native
    from process.ext.utils import make_class

    stage_by_field = {
        "contract": "nucc-native-stage.v1",
        "run_id": run_id,
        "attempt_id": attempt_id,
        "attempt_started_at": attempt_started_at,
        "schema_name": native._schema_name(schema_name),
        "import_date": suffix,
    }
    run = await native._nucc_locked_attempt(session, stage_by_field, ("running",))
    if run["finished_at"] is not None or (run["metrics"] or {}).get("nucc_handoff") is not None:
        raise native.ReferenceFamilyArchiveError("NUCC native stage attempt is already closed")
    if incumbent is not None:
        await _reserve_nucc_stage(session, stage_by_field, run, incumbent, incumbent_relation_oid)
    stage_model = make_class(models.NUCCTaxonomy, suffix)
    if await native._relation_oid(session, schema_name, stage_model.__tablename__) is not None:
        raise native.ReferenceFamilyArchiveError("NUCC native attempt heap already exists")
    await native._create_model_heaps(
        session,
        native.ReferenceFamilySpec("nucc", (stage_model,)),
        schema_name,
        create_indexes=False,
        ordinary_heaps=True,
    )
    stage_by_field.update(
        node_id=run["node_id"],
        stage=await native._nucc_native_stage(session, schema_name, stage_model.__tablename__),
        database_oid=await session.scalar(text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()")),
        import_run_oid=await native._relation_oid(session, schema_name, "import_run"),
        source_contract_sha256=native._nucc_source_contract(stage_by_field, run["params"]),
    )
    if stage_by_field["stage"]["indexes"]:
        raise native.ReferenceFamilyArchiveError("NUCC native attempt heap is not index-free")
    stage_by_field["stage_sha256"] = native.nucc_native_digest(stage_by_field)
    connection = await session.connection()
    await connection.exec_driver_sql(
        f"COMMENT ON TABLE {native._quoted(schema_name)}.{native._quoted(stage_model.__tablename__)} IS "
        + "'"
        + native._canonical_json(stage_by_field).decode("ascii").replace("'", "''")
        + "'"
    )
    if incumbent is not None:
        await _persist_nucc_stage(session, stage_by_field, reservation=False)
    return stage_by_field


async def _reserve_nucc_stage(session, stage_by_field, run, incumbent, incumbent_relation_oid):
    """Persist the exact actual attempt/predecessor before any candidate DDL."""
    from process import reference_family_archive as native

    await _require_nucc_native_builder(session)
    native._nucc_result_authority(incumbent, allow_untracked=True)
    if type(incumbent_relation_oid) is not int or incumbent_relation_oid <= 0:
        raise native.ReferenceFamilyArchiveError("NUCC native incumbent identity differs")
    if (run["metrics"] or {}).get("nucc_native_stage") is not None:
        raise native.ReferenceFamilyArchiveError("NUCC native prior custody requires retirement")
    stage_by_field.update(
        contract="nucc-native-stage.v2",
        incumbent=incumbent,
        incumbent_relation_oid=incumbent_relation_oid,
        node_id=run["node_id"],
        database_oid=await session.scalar(text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()")),
        import_run_oid=await native._relation_oid(session, stage_by_field["schema_name"], "import_run"),
        source_contract_sha256=native._nucc_source_contract(stage_by_field, run["params"]),
    )
    await _persist_nucc_stage(session, stage_by_field, reservation=True)


async def _persist_nucc_stage(session, stage, *, reservation):
    """Reserve before DDL and durably bind its actual OID in the same owning transaction."""
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


async def _require_nucc_native_builder(session, *, expected_owner_oid=None):
    """Builder ownership is confined to its unpublished attempt, never the sealed owner."""
    from process import reference_family_archive as native

    safe = await session.scalar(
        text(
            "SELECT session_user=current_user AND builder.rolcanlogin AND NOT builder.rolsuper "
            "AND NOT builder.rolcreaterole AND NOT builder.rolcreatedb AND NOT builder.rolreplication AND NOT builder.rolbypassrls "
            "AND NOT pg_has_role(builder.oid,namespace.nspowner,'USAGE') "
            "AND (CAST(:expected AS bigint) IS NULL OR builder.oid=CAST(:expected AS bigint)) "
            "AND current_setting('session_replication_role')='origin' FROM pg_roles builder,pg_namespace namespace "
            "WHERE builder.rolname=session_user AND namespace.nspname='hp_snapshot_retention'"
        ),
        {"expected": expected_owner_oid},
    )
    if safe is not True:
        raise native.ReferenceFamilyArchiveError("NUCC native Builder authority differs")


def _nucc_precreated_stage_value(stage_by_field):
    """Decode bounded empty-heap metadata, without granting candidate authority."""
    from process import reference_family_archive as native

    if (
        type(stage_by_field) is not dict
        or set(stage_by_field)
        != {
            "contract",
            "run_id",
            "attempt_id",
            "attempt_started_at",
            "schema_name",
            "import_date",
            "node_id",
            "stage",
            "database_oid",
            "import_run_oid",
            "source_contract_sha256",
            "stage_sha256",
        }
        | (
            {"incumbent", "incumbent_relation_oid"}
            if stage_by_field.get("contract") == "nucc-native-stage.v2"
            else set()
        )
        or stage_by_field["contract"] not in {"nucc-native-stage.v1", "nucc-native-stage.v2"}
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native stage custody differs")
    stage = stage_by_field["stage"]
    if stage_by_field["contract"] == "nucc-native-stage.v2":
        native._nucc_result_authority(stage_by_field["incumbent"], allow_untracked=True)
        if type(stage_by_field["incumbent_relation_oid"]) is not int or stage_by_field["incumbent_relation_oid"] <= 0:
            raise native.ReferenceFamilyArchiveError("NUCC native incumbent identity differs")
    _validate_nucc_empty_stage_catalog(stage_by_field)
    _run_id, _attempt_id, _started_at, suffix = native._nucc_attempt(
        {
            "control_run_id": stage_by_field["run_id"],
            "context": {
                "_control_attempt_id": stage_by_field["attempt_id"],
                "_control_attempt_started_at": stage_by_field["attempt_started_at"],
            },
        }
    )
    if stage_by_field["import_date"] != suffix or stage_by_field["stage"]["table_name"] != "nucc_taxonomy_" + suffix:
        raise native.ReferenceFamilyArchiveError("NUCC native stage custody attempt differs")
    if (
        native.nucc_native_digest({key: field for key, field in stage_by_field.items() if key != "stage_sha256"})
        != stage_by_field["stage_sha256"]
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native stage custody digest differs")
    return stage_by_field


def _validate_nucc_empty_stage_catalog(stage_by_field):
    """Keep the original index-free heap receipt separate from completed index evidence."""
    from process import reference_family_archive as native

    stage = stage_by_field["stage"]
    if (
        native._schema_name(stage_by_field["schema_name"]) != stage_by_field["schema_name"]
        or type(stage) is not dict
        or set(stage) != {"table_name", "relation_oid", "relfilenode", "owner_oid", "indexes"}
        or stage["indexes"] != []
        or any(
            type(stage[key]) is not int or not 0 < stage[key] < 2**32
            for key in ("relation_oid", "relfilenode", "owner_oid")
        )
        or any(
            type(stage_by_field[key]) is not int or not 0 < stage_by_field[key] < 2**32
            for key in ("database_oid", "import_run_oid")
        )
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native stage custody catalog is invalid")


async def _require_nucc_precreated_stage(session, stage_by_field):
    """Authenticate deferred-index custody from the actual locked run and heap marker."""
    from process import reference_family_archive as native

    stage_by_field = _nucc_precreated_stage_value(stage_by_field)
    stage = stage_by_field["stage"]
    run = await native._nucc_locked_attempt(session, stage_by_field, ("running",))
    await native._require_nucc_native_location(session, stage_by_field, run)
    if run["finished_at"] is not None or (run["metrics"] or {}).get("nucc_handoff") is not None:
        raise native.ReferenceFamilyArchiveError("NUCC native stage attempt is closed")
    if (
        stage_by_field["contract"] == "nucc-native-stage.v2"
        and (run["metrics"] or {}).get("nucc_native_stage") != stage_by_field
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native persisted custody differs")
    if stage_by_field["contract"] == "nucc-native-stage.v2":
        await _require_nucc_native_builder(session, expected_owner_oid=stage["owner_oid"])
    if (
        stage["indexes"]
        or await native._nucc_native_stage(session, stage_by_field["schema_name"], stage["table_name"]) != stage
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native stage custody catalog differs")
    marker = await session.scalar(
        text("SELECT obj_description(CAST(:oid AS oid),'pg_class')"), {"oid": stage["relation_oid"]}
    )
    if marker != native._canonical_json(stage_by_field).decode("ascii"):
        raise native.ReferenceFamilyArchiveError("NUCC native stage custody marker differs")


async def cleanup_nucc_native_stage(session, stage_receipt, *, runtime_owner_oids, assert_unreferenced):
    """Retire exact abandoned custody, including a never-admitted completed handoff."""
    from process import reference_family_archive as native

    native._require_transaction(session)
    stage_by_field = _nucc_precreated_stage_value(stage_receipt)
    if (
        stage_by_field["contract"] != "nucc-native-stage.v2"
        or runtime_owner_oids != (stage_by_field["stage"]["owner_oid"],)
        or not callable(assert_unreferenced)
    ):
        raise native.ReferenceFamilyArchiveError("NUCC trusted stage cleanup is required")
    handoff_by_field = await _require_nucc_stage_retirement(session, stage_by_field)
    owner_oid = await native._nucc_native_publisher_owner(session)
    physical_stage = await _require_nucc_stage_cleanup_storage(session, stage_by_field, handoff_by_field, owner_oid)
    transaction_id = await session.scalar(text("SELECT pg_current_xact_id()::text"))
    if await assert_unreferenced(session, stage_by_field) is not True:
        raise native.ReferenceFamilyArchiveError("NUCC native stage remains referenced")
    if (
        await assert_unreferenced(session, stage_by_field) is not True
        or not session.in_transaction()
        or await session.scalar(text("SELECT pg_current_xact_id()::text")) != transaction_id
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native stage cleanup transaction changed")
    await session.execute(
        text(
            f"DROP TABLE {native._quoted(stage_by_field['schema_name'])}.{native._quoted(physical_stage['table_name'])} RESTRICT"
        )
    )
    return await _record_nucc_stage_cleanup(session, stage_by_field, handoff_by_field, physical_stage)


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


async def _require_nucc_stage_cleanup_storage(session, stage_by_field, handoff_by_field, owner_oid):
    """Lock the exact original heap/index custody before reference fences and restrictive DROP."""
    from process import reference_family_archive as native

    physical_stage = stage_by_field["stage"] if handoff_by_field is None else handoff_by_field["stage"]
    await session.execute(
        text(
            f"LOCK TABLE ONLY {native._quoted(stage_by_field['schema_name'])}.{native._quoted(physical_stage['table_name'])} IN ACCESS EXCLUSIVE MODE NOWAIT"
        )
    )
    if handoff_by_field is not None:
        await native._require_nucc_handoff_stage(session, handoff_by_field)
    else:
        if (
            await native._nucc_native_stage(session, stage_by_field["schema_name"], physical_stage["table_name"])
            != physical_stage
        ):
            raise native.ReferenceFamilyArchiveError("NUCC native stage cleanup identity differs")
        marker = await session.scalar(
            text("SELECT obj_description(CAST(:oid AS oid),'pg_class')"), {"oid": physical_stage["relation_oid"]}
        )
        if marker != native._canonical_json(stage_by_field).decode("ascii"):
            raise native.ReferenceFamilyArchiveError("NUCC native stage cleanup marker differs")
    await native._require_nucc_cleanup_custody(session, {**stage_by_field, "stage": physical_stage}, owner_oid)
    return physical_stage


async def _record_nucc_stage_cleanup(session, stage_by_field, handoff_by_field, physical_stage):
    """Retire slots only after the exact heap DROP in the same owning transaction."""
    from process import reference_family_archive as native

    receipt_by_field = {
        "contract": "nucc-native-stage-cleanup.v1",
        "stage_sha256": stage_by_field["stage_sha256"],
        "database_oid": stage_by_field["database_oid"],
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


def _nucc_result_authority(authority_by_field, *, allow_untracked=False):
    """Decode the closed one-model generation without manufacturing prior authority."""
    from process import reference_family_archive as native
    from process.reference_family_result_generation import validate_reference_family_result_generation_authority

    if type(authority_by_field) is not dict or set(authority_by_field) != {
        "importer_id",
        "local_lineage_id",
        "local_generation",
        "serving_generation",
        "relation_oids",
    }:
        raise native.ReferenceFamilyArchiveError("NUCC native predecessor differs")
    if (
        allow_untracked
        and authority_by_field["serving_generation"] is None
        and authority_by_field["relation_oids"] is None
        and authority_by_field["local_generation"] == 0
    ):
        authority = validate_reference_family_result_generation_authority(
            {**authority_by_field, "origin_lineage_id": None, "origin_generation": None, "published_at": None}
        )
        if authority.importer_id != "nucc" or authority.as_dict() != authority_by_field:
            raise native.ReferenceFamilyArchiveError("NUCC native predecessor differs")
        return authority
    if type(authority_by_field["serving_generation"]) is not dict:
        raise native.ReferenceFamilyArchiveError("NUCC native generation is untracked")
    validate_reference_family_serving_generation(authority_by_field["serving_generation"])
    complete_authority_by_field = {**authority_by_field, **authority_by_field["serving_generation"]}
    authority = validate_reference_family_result_generation_authority(complete_authority_by_field)
    if (
        authority.importer_id != "nucc"
        or authority.as_dict() != authority_by_field
        or authority.serving_generation is None
        or (authority.local_generation <= 0 and not allow_untracked)
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native predecessor differs")
    return authority


async def _publish_nucc_handoff_generation(session, handoff):
    """Preserve guarded v1 while immutable v2 never invokes the trigger installer."""
    from process import reference_family_archive as native

    if handoff["contract"] == native.NUCC_IMMUTABLE_HANDOFF_CONTRACT:
        return await publish_immutable_nucc_generation(
            session,
            schema_name=handoff["schema_name"],
            expected_authority=handoff["incumbent"],
            expected_relation_oid=handoff["stage"]["relation_oid"],
        )
    return await publish_local_reference_family_generation(
        session, importer_id="nucc", schema_name=handoff["schema_name"]
    )


async def _read_immutable_activation_predecessor(session, manifest, incumbent):
    """Observe the protected predecessor before physical receive rotation."""
    from process import reference_family_archive as native

    if (
        manifest.importer_id == "nucc"
        and manifest.source_capture_contract == native.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT
    ):
        return await read_reference_family_result_generation_authority(
            session, importer_id="nucc", schema_name=incumbent.schema_name, lock=True
        )
    return None


async def _adopt_validated_family_generation(
    session, manifest, incumbent, incoming_generation, live_pairs, predecessor
):
    """Select immutable NUCC only from the already validated exact capture contract."""
    from process import reference_family_archive as native

    if manifest.importer_id in native.TERMINAL_CAPTURE_IMPORTERS:
        return None
    if predecessor is not None:
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
            ),
        )
    native._require_published_generation_binding(
        native.reference_family_spec(manifest.importer_id), live_pairs, incoming_generation, published_authority
    )
    return published_authority


def _require_nucc_handoff_fields(handoff_by_field):
    """Keep guarded and immutable handoffs separately closed before decoding."""
    from process import reference_family_archive as native

    fields = {
        "contract",
        "run_id",
        "node_id",
        "attempt_id",
        "attempt_started_at",
        "schema_name",
        "import_date",
        "database_oid",
        "import_run_oid",
        "stage",
        "incumbent",
        "row_count",
        "source_contract_sha256",
        "handoff_sha256",
    }
    if (
        type(handoff_by_field) is not dict
        or set(handoff_by_field)
        != fields
        | (
            {"precreated_stage"}
            if handoff_by_field.get("contract") == native.NUCC_IMMUTABLE_HANDOFF_CONTRACT
            else set()
        )
        or handoff_by_field["contract"] not in {native.NUCC_HANDOFF_CONTRACT, native.NUCC_IMMUTABLE_HANDOFF_CONTRACT}
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native handoff differs")


def _require_nucc_precreated_handoff_binding(handoff_by_field):
    """Bind completed index custody to the original exact persisted empty heap."""
    from process import reference_family_archive as native

    stage = handoff_by_field["stage"]
    original_by_field = _nucc_precreated_stage_value(handoff_by_field["precreated_stage"])
    if (
        original_by_field["contract"] != "nucc-native-stage.v2"
        or any(
            original_by_field[key] != handoff_by_field[key]
            for key in (
                "run_id",
                "node_id",
                "attempt_id",
                "attempt_started_at",
                "schema_name",
                "import_date",
                "database_oid",
                "import_run_oid",
                "source_contract_sha256",
                "incumbent",
            )
        )
        or any(
            original_by_field["stage"][key] != stage[key]
            for key in ("table_name", "relation_oid", "relfilenode", "owner_oid")
        )
    ):
        raise native.ReferenceFamilyArchiveError("NUCC native original custody differs")
    if stage["relation_oid"] == original_by_field["incumbent_relation_oid"]:
        raise native.ReferenceFamilyArchiveError("NUCC native stage reuses its predecessor")


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
