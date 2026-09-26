# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed native archive mechanics for replacement-style reference families.

This module does not register importers or infer publication authority. It
only pins, clones, validates, and activates reviewed replacement families. The
caller owns native ``pg_dump``/``pg_restore`` execution and the transaction
that makes a validated stage live.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from collections.abc import Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from sqlalchemy import DefaultClause, Sequence, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateSequence, CreateTable, MetaData

from db import models
from db.tiger_models import Zip_zcta5, ZipState
from process import entity_address_snapshot_receipt as catalog_identity
from process.entity_address_snapshot_receipt import _projected_row_identity
from process.ext.address_canon import archive_table_name
from process.mrf_address_publication import STAGE_TABLE, referenced_address_filter
from process.mrf_publication_receipt import require_completed_publication
from process.provider_quality_parts.table_helpers import _index_name_for_table
from process.reference_family_result_generation import (
    RELATION_NAMES_BY_IMPORTER,
    ReferenceFamilyServingGeneration,
    current_reference_family_relation_oids,
    publish_adopted_reference_family_generation,
    read_reference_family_result_generation_authority,
    require_reference_family_automatic_generation_order,
    validate_reference_family_serving_generation,
)
from process.reference_family_result_generation import (
    TABLE_NAME as GENERATION_TABLE,
)

logger = logging.getLogger(__name__)

CONTRACT = "reference-replacement-family.postgres.v1"
VALIDATION_CONTRACT = "reference-replacement-family.validation.v1"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SNAPSHOT = re.compile(r"^[0-9A-Fa-f-]+$")
_STAGE_PREFIX = "reference_family_archive_"
_PREDECESSOR_PREFIX = "reference_family_predecessor_"
_LOCK_TIMEOUT = "500ms"
_CAPTURE_TIMEOUT = "5s"
_MAX_METADATA_BYTES = 16_384
_AUX_SCHEMA = "mrf-canonical-address.payload-jsonb.v1"


class ReferenceFamilyArchiveError(RuntimeError):
    """A closed family archive or its local ownership fence is invalid."""


@dataclass(frozen=True)
class ReferenceFamilySpec:
    """One reviewed replacement family; no names come from configuration."""

    importer_id: str
    model_types: tuple[type, ...]
    dependencies: tuple[str, ...] = ()

    @property
    def table_names(self) -> tuple[str, ...]:
        """Return the exact ordered relation names owned by this family."""

        return tuple(model_type.__tablename__ for model_type in self.model_types)

    @property
    def archive_names(self) -> tuple[str, ...]:
        """Return table names included in the portable archive."""

        return self.table_names + ((STAGE_TABLE,) if self.importer_id == "mrf" else ())


@dataclass(frozen=True)
class ReferenceTableReceipt:
    """Portable schema and exact row-count identity for one relation."""

    model_name: str
    table_name: str
    schema_sha256: str
    row_count: int

    def as_dict(self) -> dict[str, Any]:
        """Return the portable representation used by the family manifest."""

        return {
            "model_name": self.model_name,
            "table_name": self.table_name,
            "schema_sha256": self.schema_sha256,
            "row_count": self.row_count,
        }


@dataclass(frozen=True)
class ReferenceFamilyManifest:
    """Portable semantics with explicit provenance and optional generation authority."""

    importer_id: str
    tables: tuple[ReferenceTableReceipt, ...]
    source_metadata: Mapping[str, Any]
    source_metadata_sha256: str
    schema_sha256: str
    dependencies: Mapping[str, str] = field(default_factory=dict)
    auxiliary: Mapping[str, Any] | None = None
    publication_authority: str = "manual-only"
    source_serving_generation: ReferenceFamilyServingGeneration | None = None

    def as_dict(self) -> dict[str, Any]:
        """Return the strict portable archive manifest."""

        if (self.publication_authority == "tracked-generation") != (
            self.source_serving_generation is not None
        ) or self.publication_authority not in {"manual-only", "tracked-generation"}:
            raise ReferenceFamilyArchiveError("reference family manifest authority is invalid")

        manifest_by_field = {
            "contract": CONTRACT,
            "importer_id": self.importer_id,
            "publication_authority": self.publication_authority,
            "tables": [table.as_dict() for table in self.tables],
            "source_metadata": dict(self.source_metadata),
            "source_metadata_sha256": self.source_metadata_sha256,
            "schema_sha256": self.schema_sha256,
        }
        if self.publication_authority == "tracked-generation":
            manifest_by_field["source_serving_generation"] = self.source_serving_generation.as_dict()
        if self.dependencies:
            manifest_by_field["dependencies"] = dict(self.dependencies)
        if self.importer_id == "mrf":
            manifest_by_field["auxiliary"] = dict(self.auxiliary or {})
        return manifest_by_field


@dataclass(frozen=True)
class ReferenceFamilySourceCapture:
    """One pinned live source family for a consistent clone transaction."""

    manifest: ReferenceFamilyManifest
    schema_name: str
    postgres_snapshot: str


@dataclass(frozen=True)
class ReferenceFamilyStageOwnership:
    """Exact local catalog ownership for one UUID-derived stage schema."""

    importer_id: str
    dataset_id: UUID
    schema_name: str
    schema_oid: int
    relation_oids: tuple[tuple[str, int], ...]
    sequence_oids: tuple[tuple[str, int, str, str], ...] = ()
    auxiliary_oid: int | None = None


@dataclass(frozen=True)
class ReferenceFamilyStageCapture:
    """A validated committed clone pinned while native archive copy runs."""

    manifest: ReferenceFamilyManifest
    ownership: ReferenceFamilyStageOwnership
    postgres_snapshot: str


@dataclass(frozen=True)
class ReferenceFamilyPreparedSource:
    """A committed frozen clone whose exact ownership is durably recorded."""

    manifest: ReferenceFamilyManifest
    ownership: ReferenceFamilyStageOwnership


@dataclass(frozen=True)
class ReferenceFamilyIncumbent:
    """Compare-and-swap token for the complete destination family."""

    importer_id: str
    schema_name: str
    relation_oids: tuple[tuple[str, int | None], ...]


@dataclass(frozen=True)
class ReferenceFamilyActivationReceipt:
    """Destination-local identity captured before the activation commits."""

    importer_id: str
    source_metadata_sha256: str
    relation_oids: tuple[tuple[str, int], ...]
    predecessor_oids: tuple[tuple[str, int | None], ...]
    predecessor_schema_name: str | None
    tables: tuple[ReferenceTableReceipt, ...]


@dataclass(frozen=True)
class ReferenceFamilyValidationReceipt:
    """Publisher-minted evidence for one immutable protected stage."""

    importer_id: str
    package_id: str
    profile_contract: str
    stage_schema: str
    stage_schema_oid: int
    relation_oids: tuple[tuple[str, int], ...]
    sealed_owner_oid: int
    manifest_sha256: str
    tables: tuple[ReferenceTableReceipt, ...]
    validation_sha256: str

    def as_dict(self) -> dict[str, Any]:
        """Return the closed durable representation stored by the controller."""

        return {
            "contract": VALIDATION_CONTRACT,
            "importer_id": self.importer_id,
            "package_id": self.package_id,
            "profile_contract": self.profile_contract,
            "stage_schema": self.stage_schema,
            "stage_schema_oid": self.stage_schema_oid,
            "relation_oids": [list(pair) for pair in self.relation_oids],
            "sealed_owner_oid": self.sealed_owner_oid,
            "manifest_sha256": self.manifest_sha256,
            "tables": [table.as_dict() for table in self.tables],
            "validation_sha256": self.validation_sha256,
        }


@dataclass(frozen=True)
class ReferenceFamilyCutoverAuthority:
    """Trusted controller bindings rechecked during a short cutover."""

    package_id: str
    profile_contract: str
    sealed_owner_oid: int
    expected_stage_owner_oid: int
    authority: str
    source_serving_generation: Mapping[str, Any] | None = None


_SPECS = {
    spec.importer_id: spec
    for spec in (
        ReferenceFamilySpec(
            "plan-attributes",
            (models.PlanAttributes, models.PlanPrices, models.PlanRatingAreas, models.PlanBenefits),
        ),
        ReferenceFamilySpec(
            "mrf",
            (
                models.Issuer,
                models.Plan,
                models.PlanFormulary,
                models.PlanBenefitsMarketplace,
                models.PlanTransparency,
                models.PlanDrugRaw,
                models.PlanDrugStats,
                models.PlanDrugTierStats,
                models.ImportLog,
                models.PlanNPIRaw,
                models.PlanNetworkTierRaw,
                models.MRFAddress,
                models.MRFAddressEvidence,
                models.PlanSearchSummary,
            ),
            ("plan-attributes",),
        ),
        ReferenceFamilySpec("mrf-address", (models.MRFAddress, models.MRFAddressEvidence)),
        ReferenceFamilySpec("places-zcta", (models.PricingPlacesZcta,)),
        ReferenceFamilySpec("geo", (models.GeoZipLookup,)),
        ReferenceFamilySpec("geo-census", (models.GeoZipCensusProfile,), ("geo",)),
        ReferenceFamilySpec("lodes", (models.LODESWorkplaceAggregate,)),
        ReferenceFamilySpec("cms-doctors", (models.DoctorClinicianAddress, models.CMSDoctorEducation)),
        ReferenceFamilySpec("facility-anchors", (models.FacilityAnchor, models.FacilityAddressContribution)),
        ReferenceFamilySpec("tiger", (ZipState, Zip_zcta5)),
        ReferenceFamilySpec(
            "medicare-enrollment",
            (models.MedicareEnrollmentCountyStats, models.MedicareEnrollmentStats),
        ),
        ReferenceFamilySpec("pharmacy-economics", (models.PharmacyEconomicsSummary,)),
        ReferenceFamilySpec("terminology-synonyms", (models.TerminologySynonym,)),
        ReferenceFamilySpec(
            "provider-quality",
            (
                models.PricingQppProvider,
                models.PricingSviZcta,
                models.PricingProviderQualityMeasure,
                models.PricingProviderQualityDomain,
                models.PricingProviderQualityScore,
                models.PricingProviderQualityFeature,
                models.PricingProviderQualityProcedureLSH,
                models.PricingProviderQualityPeerTarget,
            ),
        ),
    )
}
_OWNED_SEQUENCES = {
    "tiger": (("zcta5_gid_seq", "zcta5", "gid"),),
    "mrf": (
        ("issuer_issuer_id_seq", "issuer", "issuer_id"),
        (
            "mrf_address_evidence_evidence_checksum_seq",
            "mrf_address_evidence",
            "evidence_checksum",
        ),
    ),
    "mrf-address": (
        (
            "mrf_address_evidence_evidence_checksum_seq",
            "mrf_address_evidence",
            "evidence_checksum",
        ),
    ),
}


def reference_family_spec(importer_id: str) -> ReferenceFamilySpec:
    """Resolve only a compiled-in reviewed importer family."""

    if importer_id == "label":
        from drug_snapshot_runtime.label import Label

        return ReferenceFamilySpec("label", (Label,))
    try:
        spec = _SPECS[importer_id]
    except (KeyError, TypeError) as error:
        raise ReferenceFamilyArchiveError("reference family importer is unsupported") from error
    if (
        not spec.model_types
        or len(set(spec.table_names)) != len(spec.table_names)
        or any(_IDENTIFIER.fullmatch(table_name) is None for table_name in spec.table_names)
    ):
        raise ReferenceFamilyArchiveError("reference family model declaration is invalid")
    return spec


def reference_family_stage_schema(dataset_id: UUID) -> str:
    """Derive the only admitted local stage namespace from a UUID owner."""

    if not isinstance(dataset_id, UUID):
        raise ReferenceFamilyArchiveError("reference family stage requires a UUID dataset_id")
    return _STAGE_PREFIX + dataset_id.hex


def _quoted(value: str) -> str:
    if _IDENTIFIER.fullmatch(value) is None:
        raise ReferenceFamilyArchiveError("reference family identifier is invalid")
    return f'"{value}"'


def _schema_name(value: object) -> str:
    if not isinstance(value, str):
        raise ReferenceFamilyArchiveError("reference family schema is invalid")
    normalized = value.strip()
    if _IDENTIFIER.fullmatch(normalized) is None or len(normalized.encode()) > 63:
        raise ReferenceFamilyArchiveError("reference family schema is invalid")
    return normalized


def _canonical_json(value: object) -> bytes:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as error:
        raise ReferenceFamilyArchiveError("reference family source metadata is invalid") from error
    return encoded


def _source_metadata(value: object) -> tuple[dict[str, Any], str]:
    if not isinstance(value, Mapping) or not value:
        raise ReferenceFamilyArchiveError("reference family source metadata is required")
    encoded = _canonical_json(dict(value))
    if len(encoded) > _MAX_METADATA_BYTES:
        raise ReferenceFamilyArchiveError("reference family source metadata is too large")
    metadata = json.loads(encoded.decode("ascii"))
    if not isinstance(metadata, dict) or not metadata:
        raise ReferenceFamilyArchiveError("reference family source metadata is invalid")
    return metadata, hashlib.sha256(b"reference-family-source-metadata/v1\0" + encoded).hexdigest()


def _schema_digest(receipts: list[ReferenceTableReceipt] | tuple[ReferenceTableReceipt, ...]) -> str:
    schema_receipts = [
        {
            "model_name": receipt.model_name,
            "table_name": receipt.table_name,
            "schema_sha256": receipt.schema_sha256,
        }
        for receipt in receipts
    ]
    return hashlib.sha256(b"reference-family-schema/v1\0" + _canonical_json(schema_receipts)).hexdigest()


async def _restored_mrf_auxiliary_identity(session, schema_name, publication):
    archive_name = publication["archive_name"]
    relation_oid = await _relation_oid(session, schema_name, STAGE_TABLE)
    if relation_oid is None:
        raise ReferenceFamilyArchiveError("MRF canonical auxiliary relation is missing")
    columns = await catalog_identity._catalog_columns(session, relation_oid)
    if [
        (column["attname"], column["type"], column["attnotnull"], column["default_expression"]) for column in columns
    ] != [
        ("address_key", "uuid", True, None),
        ("payload", "jsonb", True, None),
    ]:
        raise ReferenceFamilyArchiveError("MRF canonical auxiliary schema differs")
    primary_keys = await session.scalar(
        text("SELECT count(*) FROM pg_catalog.pg_constraint WHERE conrelid=:oid AND contype='p'"),
        {"oid": relation_oid},
    )
    if primary_keys != 1:
        raise ReferenceFamilyArchiveError("MRF canonical auxiliary key constraint differs")
    malformed = await session.scalar(
        text(
            f"SELECT count(*) FROM {_quoted(schema_name)}.{_quoted(STAGE_TABLE)} "
            "WHERE payload->>'address_key' IS DISTINCT FROM address_key::text"
        )
    )
    if malformed:
        raise ReferenceFamilyArchiveError("MRF canonical auxiliary key differs")
    count, digest = await _projected_row_identity(
        session,
        schema_name,
        STAGE_TABLE,
        row_json_sql="row_value.payload",
    )
    return archive_name, count, digest, publication["publication_sha256"]


async def _mrf_auxiliary_receipt(session, schema_name, *, is_source=False, publication=None):
    """Build the portable canonical-address receipt for source or restored data."""

    if is_source:
        archive_name = archive_table_name()
        if await _relation_oid(session, schema_name, archive_name) is None or publication is None:
            raise ReferenceFamilyArchiveError("MRF canonical source or publication receipt is unavailable")
        where_sql = referenced_address_filter(schema_name, lambda schema, name: f"{_quoted(schema)}.{_quoted(name)}")
        count, digest = await _projected_row_identity(
            session,
            schema_name,
            archive_name,
            row_json_sql="to_jsonb(row_value)",
            where_sql=where_sql,
        )
        publication_sha256 = hashlib.sha256(
            _canonical_json(
                {
                    "attempt_id": str(publication["attempt_id"]),
                    "generation": publication["generation"],
                    "address_content": publication["address_content"],
                }
            )
        ).hexdigest()
    else:
        archive_name, count, digest, publication_sha256 = await _restored_mrf_auxiliary_identity(
            session, schema_name, publication
        )
    return {
        "table_name": STAGE_TABLE,
        "archive_name": archive_name,
        "schema_sha256": hashlib.sha256(_AUX_SCHEMA.encode()).hexdigest(),
        "row_count": count,
        "content_sha256": digest,
        "publication_sha256": publication_sha256,
    }


def _require_transaction(session: Any) -> None:
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise ReferenceFamilyArchiveError("reference family operation requires a caller transaction")


async def _relation_oid(session: Any, schema_name: str, table_name: str) -> int | None:
    value = await session.scalar(
        text(
            "SELECT relation.oid FROM pg_catalog.pg_class AS relation "
            "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
            "WHERE namespace.nspname=:schema_name AND relation.relname=:table_name "
            "AND relation.relkind='r' AND relation.relpersistence='p' "
            "AND NOT relation.relrowsecurity AND NOT relation.relforcerowsecurity"
        ),
        {"schema_name": schema_name, "table_name": table_name},
    )
    if value is None:
        return None
    if type(value) is not int or value <= 0:
        raise ReferenceFamilyArchiveError("reference family relation is unavailable")
    return value


async def _timeout_value(session: Any, setting: str) -> str:
    value = await session.scalar(text(f"SHOW {setting}"))
    if not isinstance(value, str) or not value:
        raise ReferenceFamilyArchiveError("reference family timeout state is unavailable")
    return value


async def _set_local_timeout(session: Any, setting: str, value: str) -> None:
    await session.execute(
        text("SELECT pg_catalog.set_config(:setting, :value, true)"),
        {"setting": setting, "value": value},
    )


@asynccontextmanager
async def _bounded_capture(session: Any):
    """Bound lock/capture work, then restore caller settings before cloning."""

    previous_lock = await _timeout_value(session, "lock_timeout")
    previous_statement = await _timeout_value(session, "statement_timeout")
    await _set_local_timeout(session, "lock_timeout", _LOCK_TIMEOUT)
    await _set_local_timeout(session, "statement_timeout", _CAPTURE_TIMEOUT)
    try:
        yield
    except BaseException:
        # A failed caller transaction expires SET LOCAL; more SQL would mask the original error.
        raise
    else:
        await _set_local_timeout(session, "lock_timeout", previous_lock)
        await _set_local_timeout(session, "statement_timeout", previous_statement)


async def _lock_family(
    session: Any,
    schema_name: str,
    table_names: tuple[str, ...],
    mode: str,
    *,
    nowait: bool = False,
) -> None:
    relations = ", ".join(f"{_quoted(schema_name)}.{_quoted(name)}" for name in table_names)
    await session.execute(text(f"LOCK TABLE {relations} IN {mode} MODE{' NOWAIT' if nowait else ''}"))


async def _lock_source_family(session: Any, spec: ReferenceFamilySpec, schema_name: str) -> None:
    """Pin source relations before summaries; repeatable read pins their row versions."""

    names = RELATION_NAMES_BY_IMPORTER["mrf"] if spec.importer_id == "mrf" else spec.table_names
    await _lock_family(session, schema_name, names, "ACCESS SHARE")


async def _has_source_generation_authority(session: Any, spec: ReferenceFamilySpec, schema_name: str) -> bool:
    """Treat pre-ledger/manual source schemas as generation-less."""

    if spec.importer_id == "tiger":
        return schema_name == "tiger"
    if spec.importer_id == "label":
        return True
    return bool(
        await session.scalar(
            text(
                "SELECT to_regclass(format('%I.%I',CAST(:schema_name AS text),CAST(:table_name AS text))) IS NOT NULL"
            ),
            {"schema_name": schema_name, "table_name": GENERATION_TABLE},
        )
    )


async def _table_receipt(
    session: Any,
    *,
    importer_id: str,
    schema_name: str,
    model_type: type,
) -> ReferenceTableReceipt:
    table_name = model_type.__tablename__
    relation_oid = await _relation_oid(session, schema_name, table_name)
    if relation_oid is None:
        raise ReferenceFamilyArchiveError("reference family relation is missing")
    try:
        schema_sha256 = await _family_schema_identity(session, importer_id, relation_oid, schema_name, table_name)
    except Exception as error:
        raise ReferenceFamilyArchiveError("reference family schema identity is unavailable") from error
    row_count = await session.scalar(text(f"SELECT count(*)::bigint FROM {_quoted(schema_name)}.{_quoted(table_name)}"))
    if type(row_count) is not int or row_count < 0:
        raise ReferenceFamilyArchiveError("reference family row count is invalid")
    return ReferenceTableReceipt(model_type.__name__, table_name, schema_sha256, row_count)


async def _family_schema_identity(
    session: Any,
    importer_id: str,
    relation_oid: int,
    schema_name: str,
    table_name: str,
) -> str:
    sequence_by_table = {
        owner_table: (sequence_name, owner_column)
        for sequence_name, owner_table, owner_column in _OWNED_SEQUENCES.get(importer_id, ())
    }
    if table_name not in sequence_by_table:
        return await catalog_identity._schema_identity(session, relation_oid, schema_name, table_name)
    columns = await catalog_identity._catalog_columns(session, relation_oid)
    constraints = await catalog_identity._catalog_constraints(session, relation_oid, schema_name)
    indexes = await catalog_identity._catalog_indexes(session, relation_oid)
    expected_sequence, expected_column = sequence_by_table[table_name]
    source_sequence = await _source_owned_sequence(session, relation_oid, expected_column)
    default_pattern = re.compile(
        rf"nextval\('(?:\"?{re.escape(schema_name)}\"?\.)?"
        rf"\"?{re.escape(source_sequence)}\"?\'::regclass\)"
    )
    has_owned_sequence_default = False
    for column in columns:
        default_expression = column.get("default_expression")
        if isinstance(default_expression, str) and "nextval(" in default_expression:
            if (
                has_owned_sequence_default
                or column.get("attname") != expected_column
                or default_pattern.fullmatch(default_expression) is None
            ):
                raise ReferenceFamilyArchiveError("reference family owned sequence default is unsupported")
            has_owned_sequence_default = True
            column["default_expression"] = f"reference-family-owned-sequence:{expected_sequence}"
    if not has_owned_sequence_default:
        raise ReferenceFamilyArchiveError("reference family owned sequence default is unavailable")
    catalog_identity._reject_schema_qualified_expressions(schema_name, columns, constraints, indexes)
    return catalog_identity._canonical_digest(
        {
            "table_name": table_name,
            "columns": columns,
            "constraints": constraints,
            "indexes": indexes,
        }
    )


async def _source_owned_sequence(session: Any, relation_oid: int, column_name: str) -> str:
    """Identify the exact SERIAL sequence owned and referenced by a source column."""

    sequence_records = list(
        (
            await session.execute(
                text(
                    "SELECT sequence.relname AS sequence_name FROM pg_catalog.pg_class AS owner_table "
                    "JOIN pg_catalog.pg_attribute AS owner_column "
                    "ON owner_column.attrelid=owner_table.oid AND owner_column.attname=:column_name "
                    "JOIN pg_catalog.pg_depend AS ownership "
                    "ON ownership.classid='pg_class'::regclass AND ownership.objid<>owner_table.oid "
                    "AND ownership.objsubid=0 AND ownership.refclassid='pg_class'::regclass "
                    "AND ownership.refobjid=owner_table.oid AND ownership.refobjsubid=owner_column.attnum "
                    "AND ownership.deptype='a' "
                    "JOIN pg_catalog.pg_class AS sequence ON sequence.oid=ownership.objid "
                    "AND sequence.relkind='S' AND sequence.relnamespace=owner_table.relnamespace "
                    "JOIN pg_catalog.pg_attrdef AS default_value "
                    "ON default_value.adrelid=owner_table.oid AND default_value.adnum=owner_column.attnum "
                    "JOIN pg_catalog.pg_depend AS default_reference "
                    "ON default_reference.classid='pg_attrdef'::regclass "
                    "AND default_reference.objid=default_value.oid "
                    "AND default_reference.refclassid='pg_class'::regclass "
                    "AND default_reference.refobjid=sequence.oid AND default_reference.deptype='n' "
                    "WHERE owner_table.oid=:relation_oid"
                ),
                {"relation_oid": relation_oid, "column_name": column_name},
            )
        ).mappings()
    )
    if len(sequence_records) != 1 or _IDENTIFIER.fullmatch(sequence_records[0]["sequence_name"]) is None:
        raise ReferenceFamilyArchiveError("reference family owned sequence is unavailable")
    return str(sequence_records[0]["sequence_name"])


async def _family_manifest(
    session: Any,
    *,
    spec: ReferenceFamilySpec,
    schema_name: str,
    source_metadata: Mapping[str, Any],
    dependencies: Mapping[str, str] | None = None,
    auxiliary: Mapping[str, Any] | None = None,
    publication: Mapping[str, Any] | None = None,
    source_serving_generation: ReferenceFamilyServingGeneration | None = None,
) -> ReferenceFamilyManifest:
    metadata, metadata_sha256 = _source_metadata(source_metadata)
    receipts = tuple(
        [
            await _table_receipt(
                session,
                importer_id=spec.importer_id,
                schema_name=schema_name,
                model_type=model_type,
            )
            for model_type in spec.model_types
        ]
    )
    schema_sha256 = _schema_digest(receipts)
    if spec.importer_id == "mrf":
        auxiliary = await _mrf_auxiliary_receipt(
            session,
            schema_name,
            is_source=auxiliary is None,
            publication=publication if auxiliary is None else auxiliary,
        )
    return ReferenceFamilyManifest(
        spec.importer_id,
        receipts,
        metadata,
        metadata_sha256,
        schema_sha256,
        _dependency_packages(spec.importer_id, {} if dependencies is None else dependencies),
        auxiliary,
        "tracked-generation" if source_serving_generation is not None else "manual-only",
        source_serving_generation,
    )


def _dependency_packages(importer_id: str, value: object) -> dict[str, str]:
    """Keep dependency identity portable: exact package hashes, never local OIDs."""
    if (
        not isinstance(value, Mapping)
        or len(value) > 32
        or set(value) != set(reference_family_spec(importer_id).dependencies)
    ):
        raise ReferenceFamilyArchiveError("reference family dependencies are invalid")
    if any(
        not isinstance(name, str)
        or re.fullmatch(r"[a-z][a-z0-9_-]{0,127}", name) is None
        or name == importer_id
        or not isinstance(package_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", package_id) is None
        for name, package_id in value.items()
    ):
        raise ReferenceFamilyArchiveError("reference family dependencies are invalid")
    return dict(sorted(value.items()))


def _validate_mrf_auxiliary_receipt(auxiliary: object) -> Mapping[str, Any]:
    """Validate the required portable canonical-address receipt."""

    expected_fields = {
        "table_name",
        "archive_name",
        "schema_sha256",
        "row_count",
        "content_sha256",
        "publication_sha256",
    }
    if (
        not isinstance(auxiliary, Mapping)
        or set(auxiliary) != expected_fields
        or auxiliary["table_name"] != STAGE_TABLE
        or _IDENTIFIER.fullmatch(str(auxiliary["archive_name"])) is None
        or auxiliary["schema_sha256"] != hashlib.sha256(_AUX_SCHEMA.encode()).hexdigest()
        or type(auxiliary["row_count"]) is not int
        or auxiliary["row_count"] < 0
        or any(
            re.fullmatch(r"[0-9a-f]{64}", str(auxiliary[key])) is None
            for key in ("content_sha256", "publication_sha256")
        )
    ):
        raise ReferenceFamilyArchiveError("MRF canonical auxiliary receipt is invalid")
    return auxiliary


def _manifest_table_receipts(raw_tables: object, spec: ReferenceFamilySpec) -> tuple[ReferenceTableReceipt, ...]:
    if not isinstance(raw_tables, list) or len(raw_tables) != len(spec.model_types):
        raise ReferenceFamilyArchiveError("reference family manifest table set is invalid")
    receipts = []
    for raw_table, model_type in zip(raw_tables, spec.model_types, strict=True):
        if not isinstance(raw_table, Mapping) or set(raw_table) != {
            "model_name",
            "table_name",
            "schema_sha256",
            "row_count",
        }:
            raise ReferenceFamilyArchiveError("reference family table receipt is invalid")
        if (
            raw_table["model_name"] != model_type.__name__
            or raw_table["table_name"] != model_type.__tablename__
            or not re.fullmatch(r"[0-9a-f]{64}", str(raw_table["schema_sha256"]))
            or type(raw_table["row_count"]) is not int
            or raw_table["row_count"] < 0
        ):
            raise ReferenceFamilyArchiveError("reference family table receipt is invalid")
        receipts.append(ReferenceTableReceipt(**dict(raw_table)))
    return tuple(receipts)


def validate_reference_family_manifest(manifest_value: object) -> ReferenceFamilyManifest:
    """Validate the portable closed-family receipt without granting authority."""

    if isinstance(manifest_value, ReferenceFamilyManifest):
        manifest_value = manifest_value.as_dict()
    if not isinstance(manifest_value, Mapping):
        raise ReferenceFamilyArchiveError("reference family manifest is invalid")
    authority = manifest_value.get("publication_authority")
    expected_fields = {
        "contract",
        "importer_id",
        "publication_authority",
        "tables",
        "source_metadata",
        "source_metadata_sha256",
        "schema_sha256",
    }
    if manifest_value.get("importer_id") == "mrf":
        expected_fields.add("auxiliary")
    if authority == "tracked-generation":
        expected_fields.add("source_serving_generation")
    if set(manifest_value) - {"dependencies"} != expected_fields:
        raise ReferenceFamilyArchiveError("reference family manifest is invalid")
    if manifest_value["contract"] != CONTRACT or authority not in {"manual-only", "tracked-generation"}:
        raise ReferenceFamilyArchiveError("reference family manifest authority is invalid")
    spec = reference_family_spec(manifest_value["importer_id"])
    if spec.importer_id == "label" and authority != "tracked-generation":
        raise ReferenceFamilyArchiveError("label source generation is required")
    auxiliary = _validate_mrf_auxiliary_receipt(manifest_value.get("auxiliary")) if spec.importer_id == "mrf" else None
    metadata, metadata_sha256 = _source_metadata(manifest_value["source_metadata"])
    receipts = _manifest_table_receipts(manifest_value["tables"], spec)
    schema_sha256 = _schema_digest(receipts)
    if manifest_value["source_metadata_sha256"] != metadata_sha256 or manifest_value["schema_sha256"] != schema_sha256:
        raise ReferenceFamilyArchiveError("reference family manifest digest differs")
    dependencies = _dependency_packages(spec.importer_id, manifest_value.get("dependencies", {}))
    try:
        source_serving_generation = (
            None
            if authority == "manual-only"
            else validate_reference_family_serving_generation(manifest_value["source_serving_generation"])
        )
    except (KeyError, ValueError) as error:
        raise ReferenceFamilyArchiveError("reference family source generation is invalid") from error
    return ReferenceFamilyManifest(
        spec.importer_id,
        tuple(receipts),
        metadata,
        metadata_sha256,
        schema_sha256,
        dependencies,
        auxiliary,
        authority,
        source_serving_generation,
    )


def _validation_digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(b"reference-family-validation/v1\0" + _canonical_json(dict(payload))).hexdigest()


def _validation_inventory(value: Mapping[str, Any], spec: ReferenceFamilySpec) -> tuple[tuple[str, int], ...]:
    relation_values = value["relation_oids"]
    if not isinstance(relation_values, list) or len(relation_values) != len(spec.table_names):
        raise ReferenceFamilyArchiveError("reference family validation inventory is invalid")
    relation_oids: list[tuple[str, int]] = []
    for relation_value, expected_name in zip(relation_values, sorted(spec.table_names), strict=True):
        if (
            not isinstance(relation_value, list)
            or len(relation_value) != 2
            or relation_value[0] != expected_name
            or type(relation_value[1]) is not int
            or relation_value[1] <= 0
        ):
            raise ReferenceFamilyArchiveError("reference family validation inventory is invalid")
        relation_oids.append((relation_value[0], relation_value[1]))
    return tuple(relation_oids)


def _validation_tables(value: Mapping[str, Any], spec: ReferenceFamilySpec) -> tuple[ReferenceTableReceipt, ...]:
    raw_tables = value["tables"]
    if not isinstance(raw_tables, list) or len(raw_tables) != len(spec.model_types):
        raise ReferenceFamilyArchiveError("reference family validation tables are invalid")
    table_receipts: list[ReferenceTableReceipt] = []
    for raw_table, model_type in zip(raw_tables, spec.model_types, strict=True):
        if not isinstance(raw_table, Mapping) or set(raw_table) != {
            "model_name",
            "table_name",
            "schema_sha256",
            "row_count",
        }:
            raise ReferenceFamilyArchiveError("reference family validation tables are invalid")
        if (
            raw_table["model_name"] != model_type.__name__
            or raw_table["table_name"] != model_type.__tablename__
            or re.fullmatch(r"[0-9a-f]{64}", str(raw_table["schema_sha256"])) is None
            or type(raw_table["row_count"]) is not int
            or raw_table["row_count"] < 0
        ):
            raise ReferenceFamilyArchiveError("reference family validation tables are invalid")
        table_receipts.append(ReferenceTableReceipt(**dict(raw_table)))
    return tuple(table_receipts)


def validate_reference_family_validation_receipt(receipt_value: object) -> ReferenceFamilyValidationReceipt:
    """Validate a durable receipt without treating it as publication authority."""

    if isinstance(receipt_value, ReferenceFamilyValidationReceipt):
        receipt_value = receipt_value.as_dict()
    expected_fields = {
        "contract",
        "importer_id",
        "package_id",
        "profile_contract",
        "stage_schema",
        "stage_schema_oid",
        "relation_oids",
        "sealed_owner_oid",
        "manifest_sha256",
        "tables",
        "validation_sha256",
    }
    if not isinstance(receipt_value, Mapping) or set(receipt_value) != expected_fields:
        raise ReferenceFamilyArchiveError("reference family validation receipt is invalid")
    spec = reference_family_spec(receipt_value["importer_id"])
    if (
        receipt_value["contract"] != VALIDATION_CONTRACT
        or receipt_value["profile_contract"] != CONTRACT
        or re.fullmatch(r"[0-9a-f]{64}", str(receipt_value["package_id"])) is None
        or re.fullmatch(r"[0-9a-f]{64}", str(receipt_value["manifest_sha256"])) is None
        or type(receipt_value["stage_schema_oid"]) is not int
        or receipt_value["stage_schema_oid"] <= 0
        or type(receipt_value["sealed_owner_oid"]) is not int
        or receipt_value["sealed_owner_oid"] <= 0
    ):
        raise ReferenceFamilyArchiveError("reference family validation receipt is invalid")
    stage_schema = _schema_name(receipt_value["stage_schema"])
    relation_oids = _validation_inventory(receipt_value, spec)
    table_receipts = _validation_tables(receipt_value, spec)
    digest_by_field = {key: receipt_value[key] for key in expected_fields - {"validation_sha256"}}
    if receipt_value["validation_sha256"] != _validation_digest(digest_by_field):
        raise ReferenceFamilyArchiveError("reference family validation digest differs")
    return ReferenceFamilyValidationReceipt(
        spec.importer_id,
        receipt_value["package_id"],
        receipt_value["profile_contract"],
        stage_schema,
        receipt_value["stage_schema_oid"],
        relation_oids,
        receipt_value["sealed_owner_oid"],
        receipt_value["manifest_sha256"],
        table_receipts,
        receipt_value["validation_sha256"],
    )


async def capture_reference_family_source(
    session: Any,
    *,
    importer_id: str,
    schema_name: str,
    source_metadata: Mapping[str, Any],
    dependencies: Mapping[str, str] | None = None,
) -> ReferenceFamilySourceCapture:
    """Pin and describe one exact live family under the caller transaction."""

    return await _capture_reference_family_source(
        session,
        importer_id=importer_id,
        schema_name=schema_name,
        source_metadata=source_metadata,
        configure_isolation=True,
        dependencies=dependencies,
    )


async def _capture_reference_family_source(
    session: Any,
    *,
    importer_id: str,
    schema_name: str,
    source_metadata: Mapping[str, Any],
    configure_isolation: bool,
    dependencies: Mapping[str, str] | None = None,
) -> ReferenceFamilySourceCapture:
    """Capture after either this function or its caller establishes isolation."""

    _require_transaction(session)
    spec = reference_family_spec(importer_id)
    schema = _schema_name(schema_name)
    _source_metadata(source_metadata)
    if configure_isolation:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    async with _bounded_capture(session):
        await _lock_source_family(session, spec, schema)
        publication = None
        if importer_id == "mrf":
            publication = await require_completed_publication(session, schema)
        source_serving_generation = None
        if await _has_source_generation_authority(session, spec, schema):
            generation_authority = await read_reference_family_result_generation_authority(
                session,
                importer_id=importer_id,
                schema_name=schema,
            )
            current_generation_oids = await current_reference_family_relation_oids(
                session,
                importer_id=importer_id,
                schema_name=schema,
            )
            if generation_authority.serving_generation is None:
                if importer_id == "label" or generation_authority.relation_oids is not None:
                    raise ReferenceFamilyArchiveError("reference family source generation is incomplete")
            elif generation_authority.relation_oids != current_generation_oids:
                raise ReferenceFamilyArchiveError("reference family source generation is drifted")
            else:
                source_serving_generation = generation_authority.serving_generation
        if importer_id == "facility-anchors":
            from process.facility_address_contribution_merge import validate_observations

            if source_serving_generation is None:
                raise ReferenceFamilyArchiveError("facility source generation is unavailable")
            await validate_observations(session, stage_schema=schema, schema=schema, bind_alias=False)
        manifest = await _family_manifest(
            session,
            spec=spec,
            schema_name=schema,
            source_metadata=source_metadata,
            dependencies=dependencies,
            publication=publication,
            source_serving_generation=source_serving_generation,
        )
        snapshot = (await session.execute(text("SELECT pg_export_snapshot()"))).scalar_one()
        if not isinstance(snapshot, str) or _SNAPSHOT.fullmatch(snapshot) is None:
            raise ReferenceFamilyArchiveError("reference family source snapshot is invalid")
    return ReferenceFamilySourceCapture(manifest, schema, snapshot)


async def _clone_source(session: Any, capture: ReferenceFamilySourceCapture, stage_schema: str) -> None:
    if _SNAPSHOT.fullmatch(capture.postgres_snapshot) is None:
        raise ReferenceFamilyArchiveError("reference family source snapshot is invalid")
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    await session.execute(text(f"SET TRANSACTION SNAPSHOT '{capture.postgres_snapshot}'"))
    spec = reference_family_spec(capture.manifest.importer_id)
    if spec.importer_id in _OWNED_SEQUENCES:
        await _create_model_family(session, spec, stage_schema)
    else:
        await session.execute(text(f"CREATE SCHEMA {_quoted(stage_schema)}"))
    models_by_table = {model.__tablename__: model for model in spec.model_types}
    for table in capture.manifest.tables:
        source_ref = f"{_quoted(capture.schema_name)}.{_quoted(table.table_name)}"
        stage_ref = f"{_quoted(stage_schema)}.{_quoted(table.table_name)}"
        if spec.importer_id not in _OWNED_SEQUENCES:
            await session.execute(text(f"CREATE TABLE {stage_ref} (LIKE {source_ref} INCLUDING ALL)"))
        columns = ", ".join(_quoted(column.name) for column in models_by_table[table.table_name].__table__.columns)
        await session.execute(text(f"INSERT INTO {stage_ref} ({columns}) SELECT {columns} FROM {source_ref}"))
    if spec.importer_id == "mrf":
        source_archive = f"{_quoted(capture.schema_name)}.{_quoted(archive_table_name())}"
        stage_aux = f"{_quoted(stage_schema)}.{_quoted(STAGE_TABLE)}"
        address_filter = referenced_address_filter(
            capture.schema_name,
            lambda schema, name: f"{_quoted(schema)}.{_quoted(name)}",
            key="canonical.address_key",
        )
        await session.execute(
            text(
                f"INSERT INTO {stage_aux} (address_key, payload) "
                f"SELECT canonical.address_key, to_jsonb(canonical) FROM {source_archive} AS canonical {address_filter}"
            )
        )
    await _rebase_owned_sequences(session, stage_schema, spec.importer_id)


async def _rebase_mrf_sequences(
    session: Any,
    stage_schema: str,
    *,
    ownership: ReferenceFamilyStageOwnership | None = None,
) -> None:
    """Rebase restored MRF sequences only after checking their frozen OIDs."""

    await _rebase_owned_sequences(session, stage_schema, "mrf", ownership=ownership)


async def _rebase_owned_sequences(
    session: Any,
    stage_schema: str,
    importer_id: str,
    *,
    ownership: ReferenceFamilyStageOwnership | None = None,
) -> None:
    """Set cloned sequence state from frozen rows, without consulting mutable sequences."""

    sequence_oid_by_name = {}
    if ownership is not None:
        _require_transaction(session)
        if ownership.importer_id != importer_id or ownership.schema_name != stage_schema:
            raise ReferenceFamilyArchiveError("reference family sequence ownership scope differs")
        await _lock_family(session, stage_schema, reference_family_spec(importer_id).archive_names, "ACCESS EXCLUSIVE")
        # Reading each sequence retains a relation lock; verify the locked OIDs before nontransactional setval.
        for name, oid, _table, _column in ownership.sequence_oids:
            await session.execute(text(f"SELECT last_value FROM {_quoted(stage_schema)}.{_quoted(name)}"))
            sequence_oid_by_name[name] = str(oid)
        await verify_reference_family_stage_ownership(session, ownership)
    for sequence_name, table_name, column_name in _OWNED_SEQUENCES.get(importer_id, ()):
        maximum_value = await session.scalar(
            text(f"SELECT max({_quoted(column_name)})::bigint FROM {_quoted(stage_schema)}.{_quoted(table_name)}")
        )
        sequence_value = max(int(maximum_value), 1) if maximum_value is not None else 1
        await session.execute(
            text("SELECT pg_catalog.setval(CAST(:sequence AS regclass), :value, :called)"),
            {
                "sequence": sequence_oid_by_name[sequence_name] if ownership else f"{stage_schema}.{sequence_name}",
                "value": sequence_value,
                "called": maximum_value is not None and maximum_value >= 1,
            },
        )


async def _schema_oid(session: Any, schema_name: str) -> int:
    value = await session.scalar(
        text("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=:schema_name"),
        {"schema_name": schema_name},
    )
    if type(value) is not int or value <= 0:
        raise ReferenceFamilyArchiveError("reference family owned schema is unavailable")
    return value


async def _namespace_relations(session: Any, schema_oid: int) -> list[Mapping[str, Any]]:
    return list(
        (
            await session.execute(
                text(
                    "SELECT relation.oid, relation.relname, relation.relkind::text AS relkind, "
                    "indexed.indrelid AS index_table_oid FROM pg_catalog.pg_class AS relation "
                    "LEFT JOIN pg_catalog.pg_index AS indexed ON indexed.indexrelid=relation.oid "
                    "WHERE relation.relnamespace=:schema_oid ORDER BY relation.oid"
                ),
                {"schema_oid": schema_oid},
            )
        ).mappings()
    )


async def _owned_sequences(
    session: Any,
    schema_oid: int,
) -> tuple[tuple[str, int, str, str], ...]:
    sequence_rows = list(
        (
            await session.execute(
                text(
                    "SELECT sequence.relname AS sequence_name, sequence.oid AS sequence_oid, "
                    "owner_table.relname AS table_name, owner_column.attname AS column_name "
                    "FROM pg_catalog.pg_class AS sequence "
                    "JOIN pg_catalog.pg_depend AS dependency "
                    "ON dependency.classid='pg_class'::regclass "
                    "AND dependency.objid=sequence.oid AND dependency.objsubid=0 "
                    "AND dependency.refclassid='pg_class'::regclass "
                    "AND dependency.deptype='a' "
                    "JOIN pg_catalog.pg_class AS owner_table "
                    "ON owner_table.oid=dependency.refobjid "
                    "JOIN pg_catalog.pg_attribute AS owner_column "
                    "ON owner_column.attrelid=owner_table.oid "
                    "AND owner_column.attnum=dependency.refobjsubid "
                    "WHERE sequence.relnamespace=:schema_oid AND sequence.relkind='S' "
                    "ORDER BY sequence.relname"
                ),
                {"schema_oid": schema_oid},
            )
        ).mappings()
    )
    return tuple(
        (
            str(sequence_record["sequence_name"]),
            int(sequence_record["sequence_oid"]),
            str(sequence_record["table_name"]),
            str(sequence_record["column_name"]),
        )
        for sequence_record in sequence_rows
    )


async def capture_reference_family_stage_ownership(
    session: Any,
    *,
    importer_id: str,
    dataset_id: UUID,
) -> ReferenceFamilyStageOwnership:
    """Capture exact OIDs only for a complete, otherwise empty owned schema."""

    _require_transaction(session)
    spec = reference_family_spec(importer_id)
    schema_name = reference_family_stage_schema(dataset_id)
    schema_oid = await _schema_oid(session, schema_name)
    relation_oids = []
    for table_name in sorted(spec.table_names):
        relation_oid = await _relation_oid(session, schema_name, table_name)
        if relation_oid is None:
            raise ReferenceFamilyArchiveError("reference family owned relation is missing")
        relation_oids.append((table_name, relation_oid))
    owned_oids = {oid for _, oid in relation_oids}
    auxiliary_oid = None
    if spec.importer_id == "mrf":
        auxiliary_oid = await _relation_oid(session, schema_name, STAGE_TABLE)
        if auxiliary_oid is None:
            raise ReferenceFamilyArchiveError("MRF canonical auxiliary relation is missing")
        owned_oids.add(auxiliary_oid)
    sequence_oids = await _owned_sequences(session, schema_oid)
    expected_sequences = _OWNED_SEQUENCES.get(spec.importer_id, ())
    if (
        tuple((name, table_name, column_name) for name, _, table_name, column_name in sequence_oids)
        != expected_sequences
    ):
        raise ReferenceFamilyArchiveError("reference family owned sequence set is invalid")
    owned_sequence_oids = {sequence_oid for _, sequence_oid, _, _ in sequence_oids}
    for relation_row in await _namespace_relations(session, schema_oid):
        kind = relation_row["relkind"]
        if isinstance(kind, bytes):
            kind = kind.decode("ascii")
        relation_oid = int(relation_row["oid"])
        if kind == "r" and relation_oid in owned_oids:
            continue
        if kind == "i" and int(relation_row["index_table_oid"] or 0) in owned_oids:
            continue
        if kind == "S" and relation_oid in owned_sequence_oids:
            continue
        raise ReferenceFamilyArchiveError("reference family owned schema contains an unexpected relation")
    return ReferenceFamilyStageOwnership(
        spec.importer_id,
        dataset_id,
        schema_name,
        schema_oid,
        tuple(relation_oids),
        sequence_oids,
        auxiliary_oid,
    )


async def verify_reference_family_stage_ownership(
    session: Any,
    ownership: ReferenceFamilyStageOwnership,
) -> ReferenceFamilyStageOwnership:
    """Recheck a local owner token against current catalog identities."""

    _require_transaction(session)
    if not isinstance(ownership, ReferenceFamilyStageOwnership):
        raise ReferenceFamilyArchiveError("reference family stage ownership is invalid")
    observed = await capture_reference_family_stage_ownership(
        session,
        importer_id=ownership.importer_id,
        dataset_id=ownership.dataset_id,
    )
    if observed != ownership:
        raise ReferenceFamilyArchiveError("reference family stage ownership differs")
    return observed


async def _validate_stage_manifest(
    session: Any,
    *,
    ownership: ReferenceFamilyStageOwnership,
    manifest: ReferenceFamilyManifest,
) -> tuple[ReferenceTableReceipt, ...]:
    validated = validate_reference_family_manifest(manifest)
    if validated.importer_id != ownership.importer_id:
        raise ReferenceFamilyArchiveError("reference family stage scope differs")
    spec = reference_family_spec(validated.importer_id)
    observed = await _family_manifest(
        session,
        spec=spec,
        schema_name=ownership.schema_name,
        source_metadata=validated.source_metadata,
        dependencies=validated.dependencies,
        auxiliary=validated.auxiliary,
        source_serving_generation=validated.source_serving_generation,
    )
    if observed.as_dict() != validated.as_dict():
        raise ReferenceFamilyArchiveError("reference family restored stage differs")
    return observed.tables


async def cleanup_reference_family_stage(
    session: Any,
    ownership: ReferenceFamilyStageOwnership,
) -> None:
    """Drop only an unchanged UUID-owned stage using restrictive DDL."""

    _require_transaction(session)
    current_schema_oid = await session.scalar(
        text("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=:schema_name"),
        {"schema_name": ownership.schema_name},
    )
    if current_schema_oid is None:
        return
    await _lock_family(
        session,
        ownership.schema_name,
        reference_family_spec(ownership.importer_id).archive_names,
        "ACCESS EXCLUSIVE",
        nowait=True,
    )
    await verify_reference_family_stage_ownership(session, ownership)
    relations = ", ".join(
        f"{_quoted(ownership.schema_name)}.{_quoted(name)}"
        for name in reference_family_spec(ownership.importer_id).archive_names
    )
    await session.execute(text(f"DROP TABLE {relations} RESTRICT"))
    if int(
        await session.scalar(
            text("SELECT count(*) FROM pg_catalog.pg_class WHERE relnamespace=:schema_oid"),
            {"schema_oid": ownership.schema_oid},
        )
        or 0
    ):
        raise ReferenceFamilyArchiveError("reference family owned schema is not empty")
    await session.execute(text(f"DROP SCHEMA {_quoted(ownership.schema_name)}"))


async def _shielded_cleanup(session_factory: Any, ownership: ReferenceFamilyStageOwnership) -> None:
    async def cleanup() -> None:
        """Clean the exact stage in an independent transaction."""

        async with session_factory() as session, session.begin():
            await cleanup_reference_family_stage(session, ownership)

    task = asyncio.create_task(cleanup())
    is_cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            is_cancelled = True
    task.result()
    if is_cancelled:
        raise asyncio.CancelledError


async def export_reference_family_archive(
    session_factory: Any,
    *,
    importer_id: str,
    schema_name: str,
    source_metadata: Mapping[str, Any],
    dataset_id: UUID,
    archive_copy: Callable[[ReferenceFamilyStageCapture], Awaitable[None]],
) -> ReferenceFamilyManifest:
    """Clone, validate, dump, and exactly clean one closed family stage."""

    ownership = None
    try:

        async def retain_prepared_source(_session, _prepared_source):
            """The convenience wrapper owns cleanup rather than durable retention."""

            return None

        prepared_source = await prepare_reference_family_archive_source(
            session_factory,
            importer_id=importer_id,
            schema_name=schema_name,
            source_metadata=source_metadata,
            dataset_id=dataset_id,
            on_prepared=retain_prepared_source,
        )
        ownership = prepared_source.ownership
        await export_prepared_reference_family_archive(
            session_factory,
            prepared=prepared_source,
            archive_copy=archive_copy,
        )
    except BaseException:
        if ownership is not None:
            try:
                await _shielded_cleanup(session_factory, ownership)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("reference family stage %s requires manual cleanup", ownership.schema_name)
        raise
    else:
        if ownership is not None:
            await _shielded_cleanup(session_factory, ownership)
        return prepared_source.manifest


async def prepare_reference_family_archive_source(
    session_factory: Any,
    *,
    importer_id: str,
    schema_name: str,
    source_metadata: Mapping[str, Any] | None,
    dataset_id: UUID,
    on_prepared: Callable[[Any, ReferenceFamilyPreparedSource], Awaitable[None]],
    source_metadata_factory: Callable[[Any], Awaitable[Mapping[str, Any]]] | None = None,
    dependency_factory: Callable[[Any], Awaitable[Mapping[str, str]]] | None = None,
) -> ReferenceFamilyPreparedSource:
    """Clone once and persist its exact owner before the clone transaction commits."""

    stage_schema = reference_family_stage_schema(dataset_id)
    async with session_factory() as source_session, source_session.begin():
        await source_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        effective_source_metadata = source_metadata
        package_by_dataset = {}
        if source_metadata_factory is not None or dependency_factory is not None:
            spec = reference_family_spec(importer_id)
            async with _bounded_capture(source_session):
                await _lock_source_family(source_session, spec, _schema_name(schema_name))
                if source_metadata_factory is not None:
                    effective_source_metadata = await source_metadata_factory(source_session)
                if dependency_factory is not None:
                    package_by_dataset = await dependency_factory(source_session)
        if effective_source_metadata is None:
            raise ReferenceFamilyArchiveError("reference family source metadata is required")
        capture = await _capture_reference_family_source(
            source_session,
            importer_id=importer_id,
            schema_name=schema_name,
            source_metadata=effective_source_metadata,
            configure_isolation=False,
            dependencies=package_by_dataset,
        )
        async with session_factory() as clone_session, clone_session.begin():
            await _clone_source(clone_session, capture, stage_schema)
            ownership = await capture_reference_family_stage_ownership(
                clone_session,
                importer_id=importer_id,
                dataset_id=dataset_id,
            )
            prepared = ReferenceFamilyPreparedSource(capture.manifest, ownership)
            await on_prepared(clone_session, prepared)
    return prepared


async def export_prepared_reference_family_archive(
    session_factory: Any,
    *,
    prepared: ReferenceFamilyPreparedSource,
    archive_copy: Callable[[ReferenceFamilyStageCapture], Awaitable[None]],
) -> ReferenceFamilyManifest:
    """Dump only one previously committed frozen clone; never recapture live data."""

    if not isinstance(prepared, ReferenceFamilyPreparedSource):
        raise ReferenceFamilyArchiveError("reference family prepared source is invalid")
    ownership, manifest = prepared.ownership, prepared.manifest
    stage_schema = ownership.schema_name
    importer_id = ownership.importer_id
    if manifest.importer_id != importer_id:
        raise ReferenceFamilyArchiveError("reference family prepared source scope differs")
    async with session_factory() as stage_session, stage_session.begin():
        await stage_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        async with _bounded_capture(stage_session):
            await _lock_family(
                stage_session,
                stage_schema,
                reference_family_spec(importer_id).archive_names,
                "SHARE",
            )
            await verify_reference_family_stage_ownership(stage_session, ownership)
            await _validate_stage_manifest(stage_session, ownership=ownership, manifest=manifest)
            snapshot = (await stage_session.execute(text("SELECT pg_export_snapshot()"))).scalar_one()
            if not isinstance(snapshot, str) or _SNAPSHOT.fullmatch(snapshot) is None:
                raise ReferenceFamilyArchiveError("reference family stage snapshot is invalid")
        await archive_copy(ReferenceFamilyStageCapture(manifest, ownership, snapshot))
    return manifest


def _additional_index_sql(schema_name: str, model_type: type, index_spec: Mapping[str, Any]) -> str:
    allowed_keys = {"index_elements", "name", "using", "unique", "include", "where"}
    if set(index_spec) - allowed_keys:
        raise ReferenceFamilyArchiveError("reference family model index is unsupported")
    elements = index_spec.get("index_elements")
    if (
        not isinstance(elements, (tuple, list))
        or not elements
        or not all(_is_reviewed_index_element(index_element) for index_element in elements)
    ):
        raise ReferenceFamilyArchiveError("reference family model index is invalid")
    suffix = index_spec.get("name", "_".join(elements))
    if not isinstance(suffix, str) or _IDENTIFIER.fullmatch(suffix) is None:
        raise ReferenceFamilyArchiveError("reference family model index is invalid")
    index_name = _index_name_for_table(
        model_type.__tablename__,
        f"{schema_name}_{model_type.__tablename__}_idx_{suffix}",
    )
    method = index_spec.get("using")
    if method is not None and method not in {"btree", "gin", "gist", "hash", "brin", "spgist"}:
        raise ReferenceFamilyArchiveError("reference family model index method is invalid")
    is_unique = index_spec.get("unique", False)
    if type(is_unique) is not bool:
        raise ReferenceFamilyArchiveError("reference family model index uniqueness is invalid")
    included_columns = index_spec.get("include", ())
    if not isinstance(included_columns, (tuple, list)) or not all(
        isinstance(column_name, str) and _IDENTIFIER.fullmatch(column_name) is not None
        for column_name in included_columns
    ):
        raise ReferenceFamilyArchiveError("reference family model index include is invalid")
    using = f" USING {method}" if method else ""
    unique = "UNIQUE " if is_unique else ""
    include = f" INCLUDE ({', '.join(included_columns)})" if included_columns else ""
    where_clause = index_spec.get("where")
    if where_clause not in {
        None,
        "estimated_gross_margin IS NOT NULL",
        "type='practice'",
        "type='practice' AND phone_number IS NOT NULL AND phone_number <> ''",
    }:
        raise ReferenceFamilyArchiveError("reference family model index predicate is unsupported")
    where = "" if where_clause is None else f" WHERE {where_clause}"
    return (
        f"CREATE {unique}INDEX {_quoted(index_name)} ON {_quoted(schema_name)}."
        f"{_quoted(model_type.__tablename__)}{using} ({', '.join(elements)}){include}{where}"
    )


def _is_reviewed_index_element(value: object) -> bool:
    if not isinstance(value, str):
        return False
    tokens = value.split()
    if 0 < len(tokens) <= 2 and all(_IDENTIFIER.fullmatch(token) is not None for token in tokens):
        return True
    return value in {
        "lower(synonym)",
        "LEFT(postal_code, 5)",
        "regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g')",
    }


async def precreate_reference_family_restore(
    session: Any,
    *,
    importer_id: str,
    dataset_id: UUID,
) -> ReferenceFamilyStageOwnership:
    """Create model tables, constraints and sequences for a native data-only restore."""

    _require_transaction(session)
    spec = reference_family_spec(importer_id)
    schema_name = reference_family_stage_schema(dataset_id)
    await _create_model_family(session, spec, schema_name, create_indexes=False)
    return await capture_reference_family_stage_ownership(
        session,
        importer_id=importer_id,
        dataset_id=dataset_id,
    )


async def _create_model_family(
    session: Any,
    spec: ReferenceFamilySpec,
    schema_name: str,
    *,
    create_indexes: bool = True,
) -> None:
    await session.execute(text(f"CREATE SCHEMA {_quoted(schema_name)}"))
    if spec.importer_id == "mrf":
        await session.execute(
            text(
                f"CREATE TABLE {_quoted(schema_name)}.{_quoted(STAGE_TABLE)} "
                "(address_key uuid PRIMARY KEY, payload jsonb NOT NULL)"
            )
        )
    metadata = MetaData(schema=schema_name)
    for model_type in spec.model_types:
        table = model_type.__table__.to_metadata(metadata, schema=schema_name)
        if spec.importer_id == "provider-quality" and table.primary_key.columns:
            table.primary_key.name = _index_name_for_table(
                table.name,
                f"{schema_name}_{table.name}_pkey",
            )
        explicit_sequences = []
        for sequence_name, owner_table, column_name in _OWNED_SEQUENCES.get(spec.importer_id, ()):
            if owner_table == table.name and isinstance(table.c[column_name].default, Sequence):
                sequence = Sequence(sequence_name, schema=schema_name, data_type=table.c[column_name].type)
                await session.execute(CreateSequence(sequence))
                table.c[column_name].server_default = DefaultClause(sequence.next_value())
                explicit_sequences.append((sequence_name, column_name))
        if (
            spec.importer_id in {"mrf", "mrf-address"}
            and "address_key" in table.c
            and list(table.c.keys())[-1] != "address_key"
        ):
            # Ordinary MRF stages move this column before their table swap.
            address_key_column = table.c.address_key
            table._columns.remove(address_key_column)
            table.append_column(address_key_column)
        statement = str(CreateTable(table).compile(dialect=postgresql.dialect()))
        await session.execute(text(statement))
        for sequence_name, column_name in explicit_sequences:
            await session.execute(
                text(
                    f"ALTER SEQUENCE {_quoted(schema_name)}.{_quoted(sequence_name)} OWNED BY "
                    f"{_quoted(schema_name)}.{_quoted(table.name)}.{_quoted(column_name)}"
                )
            )
    if create_indexes:
        await _create_model_indexes(session, spec, schema_name)


async def _create_model_indexes(session: Any, spec: ReferenceFamilySpec, schema_name: str) -> None:
    for model_type in spec.model_types:
        if spec.importer_id == "provider-quality":
            primary_elements = tuple(getattr(model_type, "__my_index_elements__", ()) or ())
            if primary_elements:
                primary_name = _index_name_for_table(
                    model_type.__tablename__,
                    f"{schema_name}_{model_type.__tablename__}_idx_primary",
                )
                await session.execute(
                    text(
                        f"CREATE UNIQUE INDEX {_quoted(primary_name)} ON {_quoted(schema_name)}."
                        f"{_quoted(model_type.__tablename__)} ({', '.join(primary_elements)})"
                    )
                )
        indexes = tuple(getattr(model_type, "__my_initial_indexes__", ()) or ()) + tuple(
            getattr(model_type, "__my_additional_indexes__", ()) or ()
        )
        if spec.importer_id in {"mrf", "mrf-address"}:
            # The importer only creates copied additional indexes for these stages.
            indexes = (
                tuple(getattr(model_type, "__my_additional_indexes__", ()) or ())
                if model_type.__tablename__
                in {"plan_benefits_marketplace", "mrf_address", "mrf_address_evidence", "plan_search_summary"}
                else ()
            )
        for index in indexes:
            await session.execute(text(_additional_index_sql(schema_name, model_type, index)))


async def complete_reference_family_restore(session: Any, ownership: ReferenceFamilyStageOwnership) -> None:
    """Build reviewed model indexes after data restore, before freezing the owned stage."""

    _require_transaction(session)
    await verify_reference_family_stage_ownership(session, ownership)
    await _create_model_indexes(session, reference_family_spec(ownership.importer_id), ownership.schema_name)


async def validate_reference_family_stage(
    session: Any,
    *,
    ownership: ReferenceFamilyStageOwnership,
    manifest: Mapping[str, Any] | ReferenceFamilyManifest,
) -> tuple[ReferenceTableReceipt, ...]:
    """Validate restored schema/count semantics under an exact ownership lock."""

    _require_transaction(session)
    validated_manifest = validate_reference_family_manifest(manifest)
    async with _bounded_capture(session):
        await _lock_family(
            session,
            ownership.schema_name,
            reference_family_spec(ownership.importer_id).archive_names,
            "SHARE",
        )
        await verify_reference_family_stage_ownership(session, ownership)
    return await _validate_stage_manifest(
        session,
        ownership=ownership,
        manifest=validated_manifest,
    )


async def _verify_stage_owner(
    session: Any,
    ownership: ReferenceFamilyStageOwnership,
    expected_owner_oid: int,
) -> None:
    if type(expected_owner_oid) is not int or expected_owner_oid <= 0:
        raise ReferenceFamilyArchiveError("reference family stage owner is invalid")
    schema_owner = await session.scalar(
        text("SELECT nspowner FROM pg_catalog.pg_namespace WHERE oid=:schema_oid AND nspname=:schema_name"),
        {"schema_oid": ownership.schema_oid, "schema_name": ownership.schema_name},
    )
    if schema_owner != expected_owner_oid:
        raise ReferenceFamilyArchiveError("reference family stage owner differs")
    relation_oids = tuple(
        sorted(
            ownership.relation_oids
            + (((STAGE_TABLE, ownership.auxiliary_oid),) if ownership.auxiliary_oid is not None else ())
        )
    )
    relation_rows = list(
        (
            await session.execute(
                text(
                    "SELECT relation.relname, relation.oid, relation.relowner "
                    "FROM pg_catalog.pg_class AS relation "
                    "WHERE relation.oid=ANY(CAST(:relation_oids AS oid[])) ORDER BY relation.relname"
                ),
                {"relation_oids": [relation_oid for _, relation_oid in relation_oids]},
            )
        ).mappings()
    )
    if [
        (relation_record["relname"], int(relation_record["oid"]), int(relation_record["relowner"]))
        for relation_record in relation_rows
    ] != [(table_name, relation_oid, expected_owner_oid) for table_name, relation_oid in relation_oids]:
        raise ReferenceFamilyArchiveError("reference family stage owner differs")
    if ownership.sequence_oids:
        sequence_rows = list(
            (
                await session.execute(
                    text(
                        "SELECT relname, oid, relowner FROM pg_catalog.pg_class "
                        "WHERE oid=ANY(CAST(:sequence_oids AS oid[])) ORDER BY relname"
                    ),
                    {"sequence_oids": [sequence_oid for _, sequence_oid, _, _ in ownership.sequence_oids]},
                )
            ).mappings()
        )
        if [
            (sequence_record["relname"], int(sequence_record["oid"]), int(sequence_record["relowner"]))
            for sequence_record in sequence_rows
        ] != [
            (sequence_name, sequence_oid, expected_owner_oid)
            for sequence_name, sequence_oid, _, _ in ownership.sequence_oids
        ]:
            raise ReferenceFamilyArchiveError("reference family stage sequence owner differs")


async def prepare_reference_family_activation(
    session: Any,
    *,
    ownership: ReferenceFamilyStageOwnership,
    manifest: Mapping[str, Any] | ReferenceFamilyManifest,
    package_id: str,
    profile_contract: str,
    sealed_owner_oid: int,
) -> ReferenceFamilyValidationReceipt:
    """Perform long validation over an already publisher-frozen stage."""

    _require_transaction(session)
    validated_manifest = validate_reference_family_manifest(manifest)
    if (
        profile_contract != CONTRACT
        or re.fullmatch(r"[0-9a-f]{64}", str(package_id)) is None
        or validated_manifest.importer_id != ownership.importer_id
    ):
        raise ReferenceFamilyArchiveError("reference family validation scope differs")
    await verify_reference_family_stage_ownership(session, ownership)
    await _verify_stage_owner(session, ownership, sealed_owner_oid)
    table_receipts = await _validate_stage_manifest(session, ownership=ownership, manifest=validated_manifest)
    validation_by_field = {
        "contract": VALIDATION_CONTRACT,
        "importer_id": ownership.importer_id,
        "package_id": package_id,
        "profile_contract": profile_contract,
        "stage_schema": ownership.schema_name,
        "stage_schema_oid": ownership.schema_oid,
        "relation_oids": [list(pair) for pair in ownership.relation_oids],
        "sealed_owner_oid": sealed_owner_oid,
        "manifest_sha256": hashlib.sha256(_canonical_json(validated_manifest.as_dict())).hexdigest(),
        "tables": [table.as_dict() for table in table_receipts],
    }
    return validate_reference_family_validation_receipt(
        {**validation_by_field, "validation_sha256": _validation_digest(validation_by_field)}
    )


async def capture_reference_family_incumbent(
    session: Any,
    *,
    importer_id: str,
    schema_name: str,
) -> ReferenceFamilyIncumbent:
    """Capture all-present or all-absent live relation OIDs for later CAS."""

    _require_transaction(session)
    spec = reference_family_spec(importer_id)
    schema = _schema_name(schema_name)
    async with _bounded_capture(session):
        pairs = await _incumbent_pairs(session, spec, schema)
        present_flags = [oid is not None for _, oid in pairs]
        if any(present_flags) and not all(present_flags):
            raise ReferenceFamilyArchiveError("reference family incumbent is incomplete")
        if all(present_flags):
            await _lock_family(session, schema, spec.table_names, "SHARE")
            if await _incumbent_pairs(session, spec, schema) != pairs:
                raise ReferenceFamilyArchiveError("reference family incumbent changed during capture")
    return ReferenceFamilyIncumbent(spec.importer_id, schema, pairs)


async def _incumbent_pairs(
    session: Any,
    spec: ReferenceFamilySpec,
    schema_name: str,
) -> tuple[tuple[str, int | None], ...]:
    relation_oids = []
    for table_name in spec.table_names:
        relation_oids.append((table_name, await _relation_oid(session, schema_name, table_name)))
    return tuple(relation_oids)


async def _verify_incumbent(session: Any, expected: ReferenceFamilyIncumbent) -> None:
    spec = reference_family_spec(expected.importer_id)
    observed_oids = await _incumbent_pairs(session, spec, expected.schema_name)
    if observed_oids != expected.relation_oids:
        raise ReferenceFamilyArchiveError("reference family incumbent changed")


def reference_family_predecessor_schema(dataset_id: UUID) -> str:
    """Derive a collision-free retained namespace from the new UUID owner."""

    if not isinstance(dataset_id, UUID):
        raise ReferenceFamilyArchiveError("reference family predecessor requires a UUID dataset_id")
    return _PREDECESSOR_PREFIX + dataset_id.hex


async def _lock_and_verify_activation(
    session: Any,
    spec: ReferenceFamilySpec,
    ownership: ReferenceFamilyStageOwnership,
    expected_incumbent: ReferenceFamilyIncumbent,
) -> None:
    async with _bounded_capture(session):
        await _lock_family(session, ownership.schema_name, spec.archive_names, "ACCESS EXCLUSIVE")
        incumbent_names = tuple(name for name, oid in expected_incumbent.relation_oids if oid is not None)
        if incumbent_names:
            await _lock_family(session, expected_incumbent.schema_name, incumbent_names, "ACCESS EXCLUSIVE")
        await verify_reference_family_stage_ownership(session, ownership)
        await _verify_incumbent(session, expected_incumbent)


async def _rotate_family_relations(
    session: Any,
    spec: ReferenceFamilySpec,
    ownership: ReferenceFamilyStageOwnership,
    expected_incumbent: ReferenceFamilyIncumbent,
) -> str | None:
    if spec.importer_id == "mrf":
        ordinary_predecessors = ", ".join(
            f"{_quoted(expected_incumbent.schema_name)}.{_quoted(table_name + '_old')}"
            for table_name in spec.table_names
        )
        await session.execute(text(f"DROP TABLE IF EXISTS {ordinary_predecessors} RESTRICT"))
    incumbent_oids_by_name = dict(expected_incumbent.relation_oids)
    predecessor_schema = None
    if any(oid is not None for oid in incumbent_oids_by_name.values()):
        predecessor_schema = reference_family_predecessor_schema(ownership.dataset_id)
        # Never infer ownership from a UUID-shaped name or replace content
        # already present there: CREATE is the collision/CAS fence.
        await session.execute(text(f"CREATE SCHEMA {_quoted(predecessor_schema)}"))
    for table_name in spec.table_names:
        incumbent_oid = incumbent_oids_by_name[table_name]
        if incumbent_oid is not None:
            await session.execute(
                text(
                    f"ALTER TABLE {_quoted(expected_incumbent.schema_name)}.{_quoted(table_name)} "
                    f"SET SCHEMA {_quoted(predecessor_schema)}"
                )
            )
        await session.execute(
            text(
                f"ALTER TABLE {_quoted(ownership.schema_name)}.{_quoted(table_name)} "
                f"SET SCHEMA {_quoted(expected_incumbent.schema_name)}"
            )
        )
    return predecessor_schema


async def _validate_mrf_canonical_address_merge(session, stage, archive) -> None:
    """Reject mutable or mismatched canonical-address contributions before merge."""

    conflict = await session.scalar(
        text(f"""
        SELECT count(*) FROM {stage} AS source JOIN {archive} AS target USING (address_key)
        WHERE target.merged_into IS NOT NULL
           OR source.payload->>'merged_into' IS NOT NULL
           OR jsonb_build_array(
                source.payload->'identity_key', source.payload->'identity_version',
                source.payload->'precision', source.payload->'premise_key',
                source.payload->'line1_norm', source.payload->'unit_norm',
                source.payload->'city_norm', source.payload->'state_code',
                source.payload->'zip5', source.payload->'zip4', source.payload->'country_code'
              ) IS DISTINCT FROM jsonb_build_array(
                to_jsonb(target)->'identity_key', to_jsonb(target)->'identity_version',
                to_jsonb(target)->'precision', to_jsonb(target)->'premise_key',
                to_jsonb(target)->'line1_norm', to_jsonb(target)->'unit_norm',
                to_jsonb(target)->'city_norm', to_jsonb(target)->'state_code',
                to_jsonb(target)->'zip5', to_jsonb(target)->'zip4', to_jsonb(target)->'country_code'
              )
    """)
    )
    if conflict:
        raise ReferenceFamilyArchiveError("MRF canonical destination key conflicts")
    source_invalid = await session.scalar(
        text(f"""
        SELECT count(*) FROM {stage}
        WHERE ((payload->>'source_bits')::integer & 16) IS DISTINCT FROM 16
           OR payload->>'merged_into' IS NOT NULL
    """)
    )
    if source_invalid:
        raise ReferenceFamilyArchiveError("MRF canonical source contribution is invalid")


async def _merge_mrf_canonical_address(session, ownership, destination_schema, auxiliary):
    """Merge only source-owned canonical contributions inside the cutover transaction."""

    archive_name = archive_table_name()
    if archive_name != auxiliary["archive_name"]:
        raise ReferenceFamilyArchiveError("MRF canonical archive name differs")
    if await _relation_oid(session, destination_schema, archive_name) is None:
        raise ReferenceFamilyArchiveError("MRF destination canonical archive is unavailable")
    stage = f"{_quoted(ownership.schema_name)}.{_quoted(STAGE_TABLE)}"
    archive = f"{_quoted(destination_schema)}.{_quoted(archive_name)}"
    await _lock_family(session, destination_schema, (archive_name,), "SHARE ROW EXCLUSIVE")
    await _validate_mrf_canonical_address_merge(session, stage, archive)
    await session.execute(
        text(f"""
        INSERT INTO {archive}
        SELECT (jsonb_populate_record(NULL::{archive},
            jsonb_set(jsonb_set(source.payload, '{{source_bits}}', '16'::jsonb),
                '{{strict_source_bits}}', to_jsonb(coalesce((source.payload->>'strict_source_bits')::integer, 0) & 16)))).*
        FROM {stage} AS source
        WHERE NOT EXISTS (SELECT 1 FROM {archive} AS target WHERE target.address_key=source.address_key)
    """)
    )
    await session.execute(
        text(f"""
        UPDATE {archive} AS target SET source_bits=target.source_bits | 16
        FROM {stage} AS source WHERE target.address_key=source.address_key
          AND (target.source_bits & 16) <> 16
    """)
    )
    has_strict_bits = await session.scalar(
        text("""
        SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_attribute
            WHERE attrelid=to_regclass(:archive) AND attname='strict_source_bits'
              AND attnum>0 AND NOT attisdropped
        )
    """),
        {"archive": archive},
    )
    if has_strict_bits:
        await session.execute(
            text(f"""
            UPDATE {archive} AS target
               SET strict_source_bits=target.strict_source_bits |
                   (coalesce((source.payload->>'strict_source_bits')::integer, 0) & 16)
              FROM {stage} AS source WHERE target.address_key=source.address_key
        """)
        )
    await session.execute(text(f"DROP TABLE {stage} RESTRICT"))


async def _drop_empty_stage_schema(session: Any, ownership: ReferenceFamilyStageOwnership) -> None:
    remaining_relations = int(
        await session.scalar(
            text("SELECT count(*) FROM pg_catalog.pg_class WHERE relnamespace=:schema_oid"),
            {"schema_oid": ownership.schema_oid},
        )
        or 0
    )
    if remaining_relations:
        raise ReferenceFamilyArchiveError("reference family stage schema is not empty after activation")
    await session.execute(text(f"DROP SCHEMA {_quoted(ownership.schema_name)}"))


async def _activation_receipt(
    session: Any,
    spec: ReferenceFamilySpec,
    ownership: ReferenceFamilyStageOwnership,
    expected_incumbent: ReferenceFamilyIncumbent,
    manifest: ReferenceFamilyManifest,
    tables: tuple[ReferenceTableReceipt, ...],
    predecessor_schema_name: str | None,
) -> ReferenceFamilyActivationReceipt:
    live_pairs = await _incumbent_pairs(session, spec, expected_incumbent.schema_name)
    if any(type(oid) is not int or oid <= 0 for _, oid in live_pairs):
        raise ReferenceFamilyArchiveError("reference family activated relation is unavailable")
    if tuple(sorted(live_pairs)) != ownership.relation_oids:
        raise ReferenceFamilyArchiveError("reference family activated relation OID differs")
    if spec.importer_id == "mrf":
        observed_tables = tuple(
            [
                await _table_receipt(
                    session,
                    importer_id=spec.importer_id,
                    schema_name=expected_incumbent.schema_name,
                    model_type=model,
                )
                for model in spec.model_types
            ]
        )
        if observed_tables != manifest.tables:
            raise ReferenceFamilyArchiveError("reference family activated receipt differs")
    else:
        local_manifest = await _family_manifest(
            session,
            spec=spec,
            schema_name=expected_incumbent.schema_name,
            source_metadata=manifest.source_metadata,
            dependencies=manifest.dependencies,
            source_serving_generation=manifest.source_serving_generation,
        )
        if local_manifest.as_dict() != manifest.as_dict():
            raise ReferenceFamilyArchiveError("reference family activated receipt differs")
    return ReferenceFamilyActivationReceipt(
        spec.importer_id,
        manifest.source_metadata_sha256,
        tuple((name, int(oid)) for name, oid in live_pairs),
        expected_incumbent.relation_oids,
        predecessor_schema_name,
        tables,
    )


async def activate_reference_family_stage(
    session: Any,
    *,
    ownership: ReferenceFamilyStageOwnership,
    manifest: Mapping[str, Any] | ReferenceFamilyManifest,
    expected_incumbent: ReferenceFamilyIncumbent,
    authority: str,
) -> ReferenceFamilyActivationReceipt:
    """Manually rotate one complete family inside the caller-owned transaction."""

    _require_transaction(session)
    if isinstance(ownership, ReferenceFamilyStageOwnership) and ownership.importer_id == "facility-anchors":
        raise ReferenceFamilyArchiveError("facility activation requires protected contribution preparation")
    if authority != "manual":
        raise ReferenceFamilyArchiveError("reference family automatic activation is unsupported")
    if not isinstance(ownership, ReferenceFamilyStageOwnership) or not isinstance(
        expected_incumbent,
        ReferenceFamilyIncumbent,
    ):
        raise ReferenceFamilyArchiveError("reference family activation ownership is invalid")
    validated_manifest = validate_reference_family_manifest(manifest)
    if (
        validated_manifest.importer_id != ownership.importer_id
        or expected_incumbent.importer_id != ownership.importer_id
    ):
        raise ReferenceFamilyArchiveError("reference family activation scope differs")
    spec = reference_family_spec(ownership.importer_id)
    await _lock_and_verify_activation(session, spec, ownership, expected_incumbent)
    tables = await _validate_stage_manifest(
        session,
        ownership=ownership,
        manifest=validated_manifest,
    )
    predecessor_schema_name = await _rotate_family_relations(
        session,
        spec,
        ownership,
        expected_incumbent,
    )
    if spec.importer_id == "mrf":
        await _merge_mrf_canonical_address(
            session, ownership, expected_incumbent.schema_name, validated_manifest.auxiliary
        )
    await _drop_empty_stage_schema(session, ownership)
    await publish_adopted_reference_family_generation(
        session,
        importer_id=spec.importer_id,
        schema_name=expected_incumbent.schema_name,
        source_generation=validated_manifest.source_serving_generation if spec.importer_id == "label" else None,
    )
    return await _activation_receipt(
        session,
        spec,
        ownership,
        expected_incumbent,
        validated_manifest,
        tables,
        predecessor_schema_name,
    )


async def _complete_validated_stage_activation(
    session: Any,
    spec: ReferenceFamilySpec,
    ownership: ReferenceFamilyStageOwnership,
    expected_incumbent: ReferenceFamilyIncumbent,
    manifest: ReferenceFamilyManifest,
) -> tuple[str | None, list[tuple[str, int | None]]]:
    """Rotate, merge MRF canonical rows, and verify the activated relation OIDs."""

    predecessor_schema_name = await _rotate_family_relations(session, spec, ownership, expected_incumbent)
    if spec.importer_id == "mrf":
        await _merge_mrf_canonical_address(session, ownership, expected_incumbent.schema_name, manifest.auxiliary)
    await _drop_empty_stage_schema(session, ownership)
    live_pairs = await _incumbent_pairs(session, spec, expected_incumbent.schema_name)
    if tuple(sorted(live_pairs)) != ownership.relation_oids:
        raise ReferenceFamilyArchiveError("reference family activated relation OID differs")
    return predecessor_schema_name, live_pairs


async def activate_validated_reference_family_stage(
    session: Any,
    *,
    ownership: ReferenceFamilyStageOwnership,
    manifest: Mapping[str, Any] | ReferenceFamilyManifest,
    expected_incumbent: ReferenceFamilyIncumbent,
    validation_receipt: Mapping[str, Any] | ReferenceFamilyValidationReceipt,
    cutover: ReferenceFamilyCutoverAuthority,
    contribution_effect_receipt: Mapping[str, Any] | None = None,
) -> ReferenceFamilyActivationReceipt:
    """CAS-rotate one publisher-validated immutable stage without recounting."""

    _require_transaction(session)
    if not isinstance(cutover, ReferenceFamilyCutoverAuthority):
        raise ReferenceFamilyArchiveError("reference family cutover authority is invalid")
    if cutover.authority not in {"manual", "automatic"}:
        raise ReferenceFamilyArchiveError("reference family activation authority is unsupported")
    if not isinstance(ownership, ReferenceFamilyStageOwnership) or not isinstance(
        expected_incumbent,
        ReferenceFamilyIncumbent,
    ):
        raise ReferenceFamilyArchiveError("reference family activation ownership is invalid")
    validated_manifest = validate_reference_family_manifest(manifest)
    validation = validate_reference_family_validation_receipt(validation_receipt)
    _require_validated_cutover_binding(ownership, expected_incumbent, validated_manifest, validation, cutover)
    spec = reference_family_spec(ownership.importer_id)
    await _lock_and_verify_activation(session, spec, ownership, expected_incumbent)
    await _verify_stage_owner(session, ownership, cutover.expected_stage_owner_oid)
    incoming_generation = _cutover_source_generation(cutover)
    if cutover.authority == "automatic":
        await _require_automatic_cutover_generation(
            session,
            spec,
            expected_incumbent,
            incoming_generation,
        )
    await _apply_validated_contribution(session, ownership, expected_incumbent, contribution_effect_receipt)
    predecessor_schema_name, live_pairs = await _complete_validated_stage_activation(
        session,
        spec,
        ownership,
        expected_incumbent,
        validated_manifest,
    )
    published_authority = await publish_adopted_reference_family_generation(
        session,
        importer_id=spec.importer_id,
        schema_name=expected_incumbent.schema_name,
        source_generation=incoming_generation,
    )
    _require_published_generation_binding(spec, live_pairs, incoming_generation, published_authority)
    return ReferenceFamilyActivationReceipt(
        spec.importer_id,
        validated_manifest.source_metadata_sha256,
        tuple((name, int(relation_oid)) for name, relation_oid in live_pairs),
        expected_incumbent.relation_oids,
        predecessor_schema_name,
        validation.tables,
    )


async def _apply_validated_contribution(session, ownership, expected_incumbent, receipt) -> None:
    """Apply only the contribution owned by this protected family cutover."""
    if ownership.importer_id == "facility-anchors":
        from process.facility_address_contribution_effects import (
            apply_facility_address_effects,
            validate_effect_receipt,
        )

        if receipt is None:
            raise ReferenceFamilyArchiveError("facility activation requires contribution evidence")
        receipt = validate_effect_receipt(receipt)
        if (
            receipt["schema"] != expected_incumbent.schema_name
            or receipt["stage_schema"] != ownership.schema_name
            or receipt["stage_schema_oid"] != ownership.schema_oid
            or receipt["contribution_oid"] != dict(ownership.relation_oids).get("facility_address_contribution")
        ):
            raise ReferenceFamilyArchiveError("facility contribution stage or destination differs")
        await apply_facility_address_effects(session, receipt)
    elif receipt is not None:
        raise ReferenceFamilyArchiveError("reference family contribution scope differs")


def _require_validated_cutover_binding(ownership, expected_incumbent, manifest, validation, cutover) -> None:
    """Bind the protected receipt to the exact stage, package, and incumbent."""

    manifest_sha256 = hashlib.sha256(_canonical_json(manifest.as_dict())).hexdigest()
    if (
        manifest.importer_id != ownership.importer_id
        or expected_incumbent.importer_id != ownership.importer_id
        or validation.importer_id != ownership.importer_id
        or validation.package_id != cutover.package_id
        or validation.profile_contract != cutover.profile_contract
        or validation.sealed_owner_oid != cutover.sealed_owner_oid
        or validation.stage_schema != ownership.schema_name
        or validation.stage_schema_oid != ownership.schema_oid
        or validation.relation_oids != ownership.relation_oids
        or validation.manifest_sha256 != manifest_sha256
        or validation.tables != manifest.tables
    ):
        raise ReferenceFamilyArchiveError("reference family validation authority differs")


def _cutover_source_generation(cutover):
    """Validate the optional portable source generation on trusted cutover authority."""

    if cutover.source_serving_generation is not None:
        try:
            return validate_reference_family_serving_generation(cutover.source_serving_generation)
        except ValueError as error:
            raise ReferenceFamilyArchiveError("reference family source generation is invalid") from error
    return None


def _generation_relation_oids(spec: ReferenceFamilySpec, relation_pairs) -> tuple[int | None, ...]:
    """Project serving ownership onto the ordinary generation contract."""

    relation_oids_by_name = dict(relation_pairs)
    try:
        return tuple(relation_oids_by_name[name] for name in RELATION_NAMES_BY_IMPORTER[spec.importer_id])
    except KeyError as error:
        raise ReferenceFamilyArchiveError("reference family generation inventory differs") from error


async def _require_automatic_cutover_generation(session, spec, expected_incumbent, incoming_generation) -> None:
    """Require empty bootstrap or a strictly newer same-lineage generation."""

    if incoming_generation is None:
        raise ReferenceFamilyArchiveError("reference family automatic source generation is unavailable")
    current_authority = await read_reference_family_result_generation_authority(
        session,
        importer_id=spec.importer_id,
        schema_name=expected_incumbent.schema_name,
        lock=True,
    )
    incumbent_oids = tuple(oid for _, oid in expected_incumbent.relation_oids)
    generation_incumbent_oids = _generation_relation_oids(spec, expected_incumbent.relation_oids)
    if current_authority.serving_generation is None:
        incumbent_presence_flags = tuple(oid is not None for oid in incumbent_oids)
        if not any(incumbent_presence_flags):
            return
        if not all(incumbent_presence_flags):
            raise ReferenceFamilyArchiveError("reference family incumbent is incomplete")
        for table_name in spec.table_names:
            populated = await session.scalar(
                text(
                    f"SELECT EXISTS (SELECT 1 FROM {_quoted(expected_incumbent.schema_name)}."
                    f"{_quoted(table_name)} LIMIT 1)"
                )
            )
            if populated:
                raise ReferenceFamilyArchiveError("reference family legacy incumbent requires manual adoption")
        return
    if current_authority.relation_oids != generation_incumbent_oids:
        raise ReferenceFamilyArchiveError("reference family incumbent generation drifted")
    try:
        require_reference_family_automatic_generation_order(incoming_generation, current_authority.serving_generation)
    except ValueError as error:
        raise ReferenceFamilyArchiveError("reference family automatic generation is stale or unrelated") from error


def _require_published_generation_binding(spec, live_pairs, incoming_generation, published_authority) -> None:
    """Verify adopted authority names the exact activated relation OIDs."""

    if incoming_generation is not None:
        ordered_live_oids = _generation_relation_oids(spec, live_pairs)
        if published_authority.relation_oids != ordered_live_oids:
            raise ReferenceFamilyArchiveError("reference family adopted generation OIDs differ")
    elif published_authority.serving_generation is not None or published_authority.relation_oids is not None:
        raise ReferenceFamilyArchiveError("reference family generation-less adoption differs")


__all__ = [
    "CONTRACT",
    "VALIDATION_CONTRACT",
    "ReferenceFamilyActivationReceipt",
    "ReferenceFamilyArchiveError",
    "ReferenceFamilyCutoverAuthority",
    "ReferenceFamilyIncumbent",
    "ReferenceFamilyManifest",
    "ReferenceFamilyPreparedSource",
    "ReferenceFamilySourceCapture",
    "ReferenceFamilyStageCapture",
    "ReferenceFamilyStageOwnership",
    "ReferenceFamilyValidationReceipt",
    "ReferenceFamilySpec",
    "ReferenceTableReceipt",
    "activate_reference_family_stage",
    "activate_validated_reference_family_stage",
    "capture_reference_family_incumbent",
    "capture_reference_family_source",
    "capture_reference_family_stage_ownership",
    "cleanup_reference_family_stage",
    "export_reference_family_archive",
    "export_prepared_reference_family_archive",
    "precreate_reference_family_restore",
    "complete_reference_family_restore",
    "prepare_reference_family_archive_source",
    "prepare_reference_family_activation",
    "reference_family_spec",
    "reference_family_predecessor_schema",
    "reference_family_stage_schema",
    "validate_reference_family_manifest",
    "validate_reference_family_validation_receipt",
    "validate_reference_family_stage",
    "verify_reference_family_stage_ownership",
]
