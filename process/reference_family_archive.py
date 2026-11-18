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
import math
import re
from collections.abc import Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from tempfile import TemporaryFile
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    Column,
    DefaultClause,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    Sequence,
    Table,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateSequence, CreateTable, MetaData
from sqlalchemy.sql.elements import TextClause

from db import models
from db.tiger_models import Zip_zcta5, ZipState
from process import entity_address_snapshot_receipt as catalog_identity
from process.cms_doctors_immutable import (
    capture_doctors_serving_generation,
    doctors_capture_contract,
    doctors_manifest_spec,
    require_doctors_storage,
)
from process.cms_doctors_immutable import (
    protected_reference_family_publication as protected_reference_family_publication,
)
from process.entity_address_snapshot_receipt import _projected_row_identity
from process.ext.address_canon import archive_table_name
from process.geo_source_archive import (
    create_model_family_archive_schema,
    create_model_family_export_clone,
    create_model_family_predecessor_schema,
    has_compatible_geo_indexes,
    model_family_publisher_owner,
    preserve_geo_cut_reads,
    seal_model_family_relation,
)
from process.geo_source_archive import geo_cut_savepoint as _doctors_cut_savepoint
from process.mrf_address_publication import (
    ACCOUNTED_MODEL_CLOSURE_CONTRACT,
    ADDRESS_COVERAGE_CONTRACT,
    MODEL_CLOSURE_CONTRACT,
    MODEL_CLOSURE_CONTRACTS,
    STAGE_TABLE,
    CanonicalSourceFence,
    _clone_model_table,
    canonical_contribution_model,
    canonical_reference_filter,
    canonical_schema_identity,
    merge_canonical_contribution,
    referenced_address_filter,
    require_canonical_source_fence,
    require_native_read_catalog,
    validate_canonical_closure,
)
from process.mrf_publication_receipt import require_completed_publication
from process.native_copy_cleanup import cleanup_interrupted_copy, native_model_copy_driver
from process.nucc_retention_read_view import require_nucc_unretained_rotation as require_nucc_unretained_rotation
from process.provider_quality_parts.table_helpers import _index_name_for_table
from process.reference_family_composition import (
    ReferenceModelContribution as ReferenceModelContribution,
)
from process.reference_family_composition import (
    _is_model_projection_equal,
)
from process.reference_family_composition import (
    compose_model_family_stage as compose_model_family_stage,
)
from process.reference_family_dictionary import (
    _CLAIMS_SCOPED_MODELS as _CLAIMS_SCOPED_MODELS,
)
from process.reference_family_dictionary import (
    _DRUG_EFFECT_MODELS as _DRUG_EFFECT_MODELS,
)
from process.reference_family_dictionary import (
    _DRUG_SCOPED_MODELS as _DRUG_SCOPED_MODELS,
)
from process.reference_family_dictionary import (
    TERMINAL_CAPABILITIES as TERMINAL_CAPABILITIES,
)
from process.reference_family_dictionary import (
    ClaimsCodeCatalog as ClaimsCodeCatalog,
)
from process.reference_family_dictionary import (
    ClaimsCodeCrosswalk as ClaimsCodeCrosswalk,
)
from process.reference_family_dictionary import (
    DrugClaimsCodeCatalog as DrugClaimsCodeCatalog,
)
from process.reference_family_dictionary import (
    DrugClaimsCodeCrosswalk as DrugClaimsCodeCrosswalk,
)
from process.reference_family_dictionary import (
    _dictionary_key_join as _dictionary_key_join,
)
from process.reference_family_dictionary import (
    _ownership_spec as _ownership_spec,
)
from process.reference_family_dictionary import (
    _source_model_name as _source_model_name,
)
from process.reference_family_dictionary import (
    _source_model_predicate as _source_model_predicate,
)
from process.reference_family_dictionary import (
    apply_reference_dictionary_effects as apply_reference_dictionary_effects,
)
from process.reference_family_dictionary import (
    prepare_reference_dictionary_effects as prepare_reference_dictionary_effects,
)
from process.reference_family_dictionary import (
    reference_family_receive_spec as reference_family_receive_spec,
)
from process.reference_family_dictionary import (
    replace_claims_dictionary_slice as replace_claims_dictionary_slice,
)
from process.reference_family_dictionary import (
    terminal_incumbent_presence as terminal_incumbent_presence,
)
from process.reference_family_dictionary import (
    validate_claims_dictionary_closure as validate_claims_dictionary_closure,
)
from process.reference_family_dictionary import (
    validate_prescription_dictionary_closure as validate_prescription_dictionary_closure,
)
from process.reference_family_dictionary import (
    validate_reference_dictionary_effects as validate_reference_dictionary_effects,
)
from process.reference_family_model_indexes import (
    _additional_index_sql as _additional_index_sql,
)
from process.reference_family_model_indexes import (
    _create_model_indexes as _create_model_indexes,
)
from process.reference_family_model_indexes import (
    _create_table_constraints as _create_table_constraints,
)
from process.reference_family_model_indexes import (
    _declared_index_statements as _declared_index_statements,
)
from process.reference_family_model_indexes import (
    _is_reviewed_index_element as _is_reviewed_index_element,
)
from process.reference_family_model_indexes import (
    _model_index_catalog_inputs as _model_index_catalog_inputs,
)
from process.reference_family_model_indexes import (
    _model_index_catalog_plan as _model_index_catalog_plan,
)
from process.reference_family_model_indexes import (
    _model_index_input as _model_index_input,
)
from process.reference_family_model_indexes import (
    _reviewed_index_where as _reviewed_index_where,
)
from process.reference_family_model_indexes import (
    _validate_declared_model_relationships as _validate_declared_model_relationships,
)
from process.reference_family_model_indexes import (
    _witness_index_element as _witness_index_element,
)
from process.reference_family_mrf_receipts import _AUX_NATIVE_SET_CONTRACT as _AUX_NATIVE_SET_CONTRACT
from process.reference_family_mrf_receipts import _AUX_SCHEMA as _AUX_SCHEMA
from process.reference_family_mrf_receipts import _canonical_model_receipt as _canonical_model_receipt
from process.reference_family_mrf_receipts import _manifest_auxiliary as _manifest_auxiliary
from process.reference_family_mrf_receipts import _validate_mrf_auxiliary_receipt as _validate_mrf_auxiliary_receipt
from process.reference_family_mrf_receipts import mrf_publication_digest as mrf_publication_digest
from process.reference_family_result_generation import (
    RELATION_NAMES_BY_IMPORTER,
    ReferenceFamilyServingGeneration,
    capture_reference_family_serving_generation,
    publish_adopted_reference_family_generation,
    read_reference_family_result_generation_authority,
    require_reference_family_automatic_generation_order,
    validate_reference_family_serving_generation,
)
from process.reference_family_result_generation import (
    TABLE_NAME as GENERATION_TABLE,
)
from process.reference_family_result_generation import (
    _activate_verified_reference_family as _activate_verified_reference_family,
)
from process.reference_family_result_generation import (
    _adopt_validated_family_generation as _adopt_validated_family_generation,
)
from process.reference_family_result_generation import (
    _nucc_attempt as _nucc_attempt,
)
from process.reference_family_result_generation import (
    _nucc_result_authority as _nucc_result_authority,
)
from process.reference_family_result_generation import (
    _publish_nucc_handoff_generation as _publish_nucc_handoff_generation,
)
from process.reference_family_result_generation import (
    _read_immutable_activation_predecessor as _read_immutable_activation_predecessor,
)
from process.reference_family_result_generation import (
    _require_nucc_handoff_fields as _require_nucc_handoff_fields,
)
from process.reference_family_result_generation import (
    _require_nucc_precreated_handoff_binding as _require_nucc_precreated_handoff_binding,
)
from process.reference_family_result_generation import (
    _rotate_family_relations as _rotate_family_relations,
)
from process.reference_family_source_capture_contract import _validate_source_capture_contract
from process.selected_canonical_archive import selected_canonical_archive

logger = logging.getLogger(__name__)

from process.reference_family_result_generation import (
    _nucc_precreated_stage_value as _nucc_precreated_stage_value,
)
from process.reference_family_result_generation import (
    _persist_nucc_stage as _persist_nucc_stage,
)
from process.reference_family_result_generation import (
    _precreate_nucc_attempt as _precreate_nucc_attempt,
)
from process.reference_family_result_generation import (
    _require_nucc_native_builder as _require_nucc_native_builder,
)
from process.reference_family_result_generation import (
    _require_nucc_precreated_stage as _require_nucc_precreated_stage,
)
from process.reference_family_result_generation import (
    cleanup_nucc_native_stage as cleanup_nucc_native_stage,
)

CONTRACT = "reference-replacement-family.postgres.v1"
NUCC_CONTRACT = "nucc.postgres.v2"
MRF_CONTRACT = "mrf.postgres.v2"
TYPED_MRF_CONTRACT = "mrf.postgres.v3"
NUCC_HANDOFF_CONTRACT = "nucc-native-handoff.v1"
NUCC_PUBLICATION_CONTRACT = "nucc-native-publication.v1"
NUCC_IMMUTABLE_HANDOFF_CONTRACT = "nucc-native-handoff.v2"
NUCC_IMMUTABLE_PUBLICATION_CONTRACT = "nucc-native-publication.v2"
IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT = "reference-family-immutable-capture.v1"
NUCC_HANDOFF_PHASE = "nucc stage awaiting publication"
NUCC_ABANDONMENT_CONTRACT = "reference-family-unpublished-abandonment.v1"
NUCC_CLEANUP_CONTRACT = "nucc-native-candidate-cleanup.v1"
VALIDATION_CONTRACT = "reference-replacement-family.validation.v1"
GUARDED_SOURCE_CAPTURE_CONTRACT = "reference-family.guarded-source-capture.v1"
CAPTURED_TIGER_CONTRACT = "tiger.immutable-captured-epoch.v1"
CAPTURED_TIGER_SNAPSHOT_CONTRACT = "tiger.readonly-snapshot-captured-epoch.v2"
TERMINAL_CAPTURE_IMPORTERS = frozenset({"claims-pricing", "drug-claims"})
TERMINAL_CAPTURE_CONTRACT = "reference-family-terminal-capture.v1"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SNAPSHOT = re.compile(r"^[0-9A-Fa-f-]+$")
_STAGE_PREFIX = "reference_family_archive_"
_PREDECESSOR_PREFIX = "reference_family_predecessor_"
_LOCK_TIMEOUT = "500ms"
_CUTOVER_WAIT_SECONDS = 0.1
_FROZEN_CUTOVER = ContextVar("reference_family_frozen_cutover", default=False)
_CAPTURE_TIMEOUT = "5s"
_MAX_METADATA_BYTES = 16_384
_CLAIMS_DICTIONARY_SOURCE = "cms_physician_provider_service"


class ReferenceFamilyArchiveError(RuntimeError):
    """A closed family archive or its local ownership fence is invalid."""


@dataclass(frozen=True)
class ReferenceFamilySpec:
    """One reviewed replacement family; no names come from configuration."""

    importer_id: str
    model_types: tuple[type, ...]
    dependencies: tuple[str, ...] = ()
    relationships: tuple[tuple[type, str, type, str, bool, bool], ...] = ()

    @property
    def table_names(self) -> tuple[str, ...]:
        """Return the exact ordered relation names owned by this family."""

        return tuple(model_type.__tablename__ for model_type in self.model_types)

    @property
    def archive_names(self) -> tuple[str, ...]:
        """Return table names included in the portable archive."""

        return self.table_names + (
            (STAGE_TABLE,) if self.importer_id == "mrf" and STAGE_TABLE not in self.table_names else ()
        )


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
    source_capture_contract: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """Return the strict portable archive manifest."""

        if (self.publication_authority == "tracked-generation") != (
            self.source_serving_generation is not None
        ) or self.publication_authority not in {"manual-only", "tracked-generation", "captured-epoch"}:
            raise ReferenceFamilyArchiveError("reference family manifest authority is invalid")

        manifest_by_field = {
            "contract": TYPED_MRF_CONTRACT
            if (self.auxiliary or {}).get("contract") in MODEL_CLOSURE_CONTRACTS
            else CONTRACT,
            "importer_id": self.importer_id,
            "publication_authority": self.publication_authority,
            "tables": [table.as_dict() for table in self.tables],
            "source_metadata": dict(self.source_metadata),
            "source_metadata_sha256": self.source_metadata_sha256,
            "schema_sha256": self.schema_sha256,
        }
        if self.publication_authority == "tracked-generation":
            manifest_by_field["source_serving_generation"] = self.source_serving_generation.as_dict()
        if self.source_capture_contract is not None:
            manifest_by_field["source_capture_contract"] = self.source_capture_contract
            _validate_source_capture_contract(manifest_by_field)
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
    canonical_source_fence: CanonicalSourceFence | None = None
    frozen_source_projection: tuple | None = None


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

    @property
    def relation_names(self):
        """Expose only the recorded logical relation names."""
        return tuple(name for name, _address in self.relation_oids)

    @property
    def sequence_names(self):
        """Expose recorded sequence names and their declared table columns."""
        return tuple((name, table, column) for name, _address, table, column in self.sequence_oids)


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
    canonical_publication: Mapping[str, Any] | None = None
    result_generation_authority: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class ReferenceFamilyValidationReceipt:
    """Content evidence for one named protected stage, with opaque old editions."""

    importer_id: str
    package_id: str
    profile_contract: str
    stage_schema: str
    relations: tuple[tuple[str, str], ...]
    manifest_sha256: str
    tables: tuple[ReferenceTableReceipt, ...]
    validation_sha256: str
    legacy_receipt: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        """Return current named metadata or an unchanged historical receipt edition."""
        if self.legacy_receipt is not None:
            return json.loads(json.dumps(self.legacy_receipt))
        return {
            "contract": VALIDATION_CONTRACT,
            "importer_id": self.importer_id,
            "package_id": self.package_id,
            "profile_contract": self.profile_contract,
            "stage_schema": self.stage_schema,
            "relations": [{"schema_name": schema, "relation_name": name} for schema, name in self.relations],
            "manifest_sha256": self.manifest_sha256,
            "tables": [table.as_dict() for table in self.tables],
            "validation_sha256": self.validation_sha256,
        }


@dataclass(frozen=True)
class ReferenceFamilyCutoverAuthority:
    """Trusted controller bindings rechecked during a short cutover."""

    package_id: str
    profile_contract: str
    authority: str
    source_serving_generation: Mapping[str, Any] | None = None
    roles: object = None
    stage_read_only: bool = False


@dataclass(frozen=True)
class ReferenceFamilySourceCopy:
    """Trusted local native COPY callback with fixed whole-source resource bounds."""

    copy_rows: Callable[..., Awaitable[int]]
    max_bytes: int
    timeout: float

    def __post_init__(self):
        if (
            not callable(self.copy_rows)
            or type(self.max_bytes) is not int
            or not 0 < self.max_bytes < 2**63
            or type(self.timeout) not in (int, float)
            or not math.isfinite(self.timeout)
            or self.timeout <= 0
        ):
            raise ReferenceFamilyArchiveError("reference family source COPY bounds are invalid")


_SPECS = {
    spec.importer_id: spec
    for spec in (
        ReferenceFamilySpec("nucc", (models.NUCCTaxonomy,)),
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
        ReferenceFamilySpec(
            "cms-doctors",
            (models.DoctorClinicianAddress, models.CMSDoctorEducation, models.CMSDoctorGroupSite),
        ),
        ReferenceFamilySpec("facility-anchors", (models.FacilityAnchor, models.FacilityAddressContribution)),
        ReferenceFamilySpec("tiger", (ZipState, Zip_zcta5)),
        ReferenceFamilySpec(
            "medicare-enrollment",
            (models.MedicareEnrollmentCountyStats, models.MedicareEnrollmentStats),
        ),
        ReferenceFamilySpec("pharmacy-economics", (models.PharmacyEconomicsSummary,)),
        ReferenceFamilySpec("terminology-synonyms", (models.TerminologySynonym,)),
        ReferenceFamilySpec(
            "claims-pricing",
            (
                models.PricingProvider,
                models.PricingProcedure,
                models.PricingProviderProcedure,
                models.PricingProviderProcedureLocation,
                models.PricingProviderProcedureCostProfile,
                models.PricingProcedurePeerStats,
                models.PricingProcedureGeoBenchmark,
                models.PricingProcedureTaxonomySignal,
                ClaimsCodeCatalog,
                ClaimsCodeCrosswalk,
            ),
        ),
        ReferenceFamilySpec(
            "drug-claims",
            (
                models.PricingPrescription,
                models.PricingProviderPrescription,
                models.PricingProviderPrescriptionAutocomplete,
                DrugClaimsCodeCatalog,
                DrugClaimsCodeCrosswalk,
            ),
        ),
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


def reference_family_spec(importer_id: str, *, canonical=False) -> ReferenceFamilySpec:
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
    if canonical:
        if importer_id != "mrf":
            raise ReferenceFamilyArchiveError("reference canonical model family is unsupported")
        spec = replace(spec, model_types=(*spec.model_types, canonical_contribution_model(importer_id)))
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
    if publication.get("contract") == _AUX_NATIVE_SET_CONTRACT:
        count = await session.scalar(text(f"SELECT count(*) FROM {_quoted(schema_name)}.{_quoted(STAGE_TABLE)}"))
        digest = None
    else:
        count, digest = await _projected_row_identity(
            session, schema_name, STAGE_TABLE, row_json_sql="row_value.payload"
        )
    return archive_name, count, digest, publication["publication_sha256"]


async def _mrf_auxiliary_receipt(session, schema_name, *, is_source=False, publication=None, native_set=False):
    """Build the portable canonical-address receipt for source or restored data."""

    if is_source:
        archive_name = archive_table_name()
        if await _relation_oid(session, schema_name, archive_name) is None or publication is None:
            raise ReferenceFamilyArchiveError("MRF canonical source or publication receipt is unavailable")
        where_sql = referenced_address_filter(schema_name, lambda schema, name: f"{_quoted(schema)}.{_quoted(name)}")
        if native_set:
            count = await session.scalar(
                text(f"SELECT count(*) FROM {_quoted(schema_name)}.{_quoted(archive_name)} row_value {where_sql}")
            )
            digest = None
        else:
            count, digest = await _projected_row_identity(
                session, schema_name, archive_name, row_json_sql="to_jsonb(row_value)", where_sql=where_sql
            )
        publication_sha256 = mrf_publication_digest(publication)
    else:
        archive_name, count, digest, publication_sha256 = await _restored_mrf_auxiliary_identity(
            session, schema_name, publication
        )
    receipt_by_field = {
        "table_name": STAGE_TABLE,
        "archive_name": archive_name,
        "schema_sha256": hashlib.sha256(_AUX_SCHEMA.encode()).hexdigest(),
        "row_count": count,
        "publication_sha256": publication_sha256,
    }
    if digest is None:
        receipt_by_field["contract"] = _AUX_NATIVE_SET_CONTRACT
    else:
        receipt_by_field["content_sha256"] = digest
    return receipt_by_field


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
async def _bounded_capture(session: Any, *, lock_before_snapshot: bool = False):
    """Bind capture timeouts while preserving the caller's snapshot order."""

    previous_lock = await _timeout_value(session, "lock_timeout")
    previous_statement = await _timeout_value(session, "statement_timeout")
    if lock_before_snapshot:
        await session.execute(text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'"))
        await session.execute(text(f"SET LOCAL statement_timeout = '{_CAPTURE_TIMEOUT}'"))
    else:
        await _set_local_timeout(session, "lock_timeout", _LOCK_TIMEOUT)
        await _set_local_timeout(session, "statement_timeout", _CAPTURE_TIMEOUT)
    yield
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


async def _lock_source_family(
    session: Any, spec: ReferenceFamilySpec, schema_name: str, canonical_source_fence=None
) -> None:
    """Pin source relations before summaries; repeatable read pins their row versions."""

    names = RELATION_NAMES_BY_IMPORTER["mrf"] if spec.importer_id == "mrf" else spec.table_names
    if spec.importer_id in TERMINAL_CAPTURE_IMPORTERS:
        await _lock_family(
            session, schema_name, tuple(_source_model_name(model) for model in spec.model_types), "SHARE", nowait=True
        )
        return
    if spec.importer_id == "mrf" and STAGE_TABLE in spec.table_names:
        await _lock_family(
            session,
            schema_name,
            names if canonical_source_fence is not None else (*names, archive_table_name()),
            "SHARE ROW EXCLUSIVE",
            nowait=True,
        )
        if canonical_source_fence is not None:
            await require_canonical_source_fence(session, canonical_source_fence, schema_name)
        return
    if spec.importer_id == "label":
        # Repeatable-read/exported snapshots pin rows; this lock blocks replacement DDL.
        await _lock_family(session, schema_name, names, "ACCESS SHARE", nowait=True)
        return
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
    is_source: bool = False,
    frozen_source_projection: tuple | None = None,
) -> ReferenceTableReceipt:
    table_name = model_type.__tablename__
    storage_name = _source_model_name(model_type) if is_source else table_name
    relation_oid = await _relation_oid(session, schema_name, storage_name)
    if relation_oid is None:
        raise ReferenceFamilyArchiveError("reference family relation is missing")
    try:
        schema_sha256 = (
            await canonical_schema_identity(session, relation_oid, schema_name)
            if table_name in ("npi_canonical_address", STAGE_TABLE)
            else await _family_schema_identity(session, importer_id, relation_oid, schema_name, table_name)
        )
    except Exception as error:
        raise ReferenceFamilyArchiveError("reference family schema identity is unavailable") from error
    predicate = _source_model_predicate(model_type, schema_name) if is_source else ""
    source_sql = f"{_quoted(schema_name)}.{_quoted(storage_name)}"
    if frozen_source_projection is not None:
        source_sql = "(" + _frozen_source_query(frozen_source_projection, model_type) + ")"
    row_count = await session.scalar(text(f"SELECT count(*)::bigint FROM {source_sql} canonical {predicate}"))
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
    if table_name in {model.__tablename__ for model in (*_CLAIMS_SCOPED_MODELS, *_DRUG_SCOPED_MODELS)}:
        # The ordinary producer can append source_attribution to an existing
        # dictionary. COPY projects names; compare native column/key semantics,
        # not that historical physical order, using the existing catalog normalizer.
        return await canonical_schema_identity(session, relation_oid, schema_name)
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
    is_source = spec.importer_id in TERMINAL_CAPTURE_IMPORTERS and publication is not None
    metadata, metadata_sha256 = _source_metadata(source_metadata)
    receipts = tuple(
        [
            await _canonical_table_receipt(session, spec.importer_id, schema_name, model_type)
            if publication is not None and model_type.__tablename__ == STAGE_TABLE
            else await _table_receipt(
                session,
                importer_id=spec.importer_id,
                schema_name=schema_name,
                model_type=model_type,
                is_source=is_source,
            )
            for model_type in spec.model_types
        ]
    )
    schema_sha256 = _schema_digest(receipts)
    if spec.importer_id == "mrf" and STAGE_TABLE in spec.table_names:
        auxiliary = (
            _canonical_model_receipt(publication)
            if publication is not None
            else auxiliary or _canonical_model_receipt()
        )
    elif spec.importer_id == "mrf":
        auxiliary = await _mrf_auxiliary_receipt(
            session,
            schema_name,
            is_source=auxiliary is None,
            publication=publication if auxiliary is None else auxiliary,
            native_set=publication is not None and "native_address_coverage" in publication,
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


async def _canonical_table_receipt(session, importer_id, schema_name, model_type):
    archive_name = archive_table_name()
    relation_oid = await _relation_oid(session, schema_name, archive_name)
    if relation_oid is None:
        raise ReferenceFamilyArchiveError("canonical source model is unavailable")

    def qualified(schema, name):
        """Quote the trusted model's exact SOURCE relation."""
        return f"{_quoted(schema)}.{_quoted(name)}"

    predicate = canonical_reference_filter(importer_id, schema_name, qualified)
    count = await session.scalar(
        text(f"SELECT count(*) FROM {qualified(schema_name, archive_name)} canonical {predicate}")
    )
    return ReferenceTableReceipt(
        model_type.__name__,
        model_type.__tablename__,
        await canonical_schema_identity(session, relation_oid, schema_name),
        count,
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


def reference_family_profile_contract(manifest: ReferenceFamilyManifest) -> str:
    """Keep legacy row-hash payloads distinct from the explicit native set-proof envelope."""
    if manifest.importer_id == "nucc":
        return NUCC_CONTRACT
    if manifest.importer_id == "mrf" and manifest.auxiliary.get("contract") in MODEL_CLOSURE_CONTRACTS:
        return TYPED_MRF_CONTRACT
    if manifest.importer_id == "mrf" and manifest.auxiliary.get("contract") == _AUX_NATIVE_SET_CONTRACT:
        return MRF_CONTRACT
    return CONTRACT


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


def _manifest_family_spec(manifest_value) -> ReferenceFamilySpec:
    """Recognize only the exact historical CMS payload, without changing stage ownership."""
    spec = (
        reference_family_spec(manifest_value["importer_id"], canonical=True)
        if manifest_value["contract"] == TYPED_MRF_CONTRACT
        else reference_family_spec(manifest_value["importer_id"])
    )
    return doctors_manifest_spec(spec, manifest_value)


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
    if "source_capture_contract" in manifest_value:
        expected_fields.add("source_capture_contract")
        _validate_source_capture_contract(manifest_value)
    if set(manifest_value) - {"dependencies"} != expected_fields:
        raise ReferenceFamilyArchiveError("reference family manifest is invalid")
    _validate_manifest_authority(manifest_value, authority)
    spec = _manifest_family_spec(manifest_value)
    if spec.importer_id in TERMINAL_CAPTURE_IMPORTERS and authority != "manual-only":
        raise ReferenceFamilyArchiveError("terminal reference captures have no producer generation")
    if spec.importer_id == "label" and authority != "tracked-generation":
        raise ReferenceFamilyArchiveError("label source generation is required")
    metadata, metadata_sha256 = _source_metadata(manifest_value["source_metadata"])
    receipts = _manifest_table_receipts(manifest_value["tables"], spec)
    auxiliary = _manifest_auxiliary(manifest_value, spec, receipts)
    schema_sha256 = _schema_digest(receipts)
    if manifest_value["source_metadata_sha256"] != metadata_sha256 or manifest_value["schema_sha256"] != schema_sha256:
        raise ReferenceFamilyArchiveError("reference family manifest digest differs")
    dependencies = _dependency_packages(spec.importer_id, manifest_value.get("dependencies", {}))
    try:
        source_serving_generation = (
            None
            if authority != "tracked-generation"
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
        manifest_value.get("source_capture_contract"),
    )


def _validate_manifest_authority(manifest_value: Mapping[str, Any], authority: object) -> None:
    """Reject unsupported publication authority before deriving trusted model membership."""
    if manifest_value["contract"] not in (CONTRACT, TYPED_MRF_CONTRACT) or authority not in {
        "manual-only",
        "tracked-generation",
        "captured-epoch",
    }:
        raise ReferenceFamilyArchiveError("reference family manifest authority is invalid")
    if authority == "captured-epoch" and manifest_value.get("source_capture_contract") not in {
        CAPTURED_TIGER_CONTRACT,
        CAPTURED_TIGER_SNAPSHOT_CONTRACT,
    }:
        raise ReferenceFamilyArchiveError("captured epoch contract is required")


def _validation_digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(b"reference-family-validation/v1\0" + _canonical_json(dict(payload))).hexdigest()


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


def _validation_receipt_spec(receipt_value):
    spec = reference_family_spec(
        receipt_value["importer_id"], canonical=receipt_value["profile_contract"] == TYPED_MRF_CONTRACT
    )
    if spec.importer_id == "drug-claims":
        spec = reference_family_receive_spec(spec.importer_id)
    if (
        receipt_value["contract"] != VALIDATION_CONTRACT
        or receipt_value["profile_contract"]
        not in (
            (CONTRACT, MRF_CONTRACT, TYPED_MRF_CONTRACT)
            if spec.importer_id == "mrf"
            else (NUCC_CONTRACT if spec.importer_id == "nucc" else CONTRACT,)
        )
        or any(
            re.fullmatch(r"[0-9a-f]{64}", str(receipt_value[key])) is None
            for key in ("package_id", "manifest_sha256", "validation_sha256")
        )
    ):
        raise ReferenceFamilyArchiveError("reference family validation receipt is invalid")
    return spec


def _validation_receipt_names(receipt_value, spec, schema, is_legacy):
    try:
        if is_legacy:
            names = tuple(entry[0] for entry in receipt_value["relation_oids"])
        else:
            entries = receipt_value["relations"]
            if not isinstance(entries, list) or any(
                not isinstance(entry, Mapping)
                or set(entry) != {"schema_name", "relation_name"}
                or entry["schema_name"] != schema
                for entry in entries
            ):
                raise ValueError
            names = tuple(entry["relation_name"] for entry in entries)
        if names != tuple(sorted(spec.table_names)):
            raise ValueError
    except IndexError, KeyError, TypeError, ValueError:
        raise ReferenceFamilyArchiveError("reference family validation inventory is invalid") from None
    return names


def validate_reference_family_validation_receipt(receipt_value: object) -> ReferenceFamilyValidationReceipt:
    """Check full semantic scope; historical physical tokens remain opaque editions."""
    if isinstance(receipt_value, ReferenceFamilyValidationReceipt):
        receipt_value = receipt_value.as_dict()
    common_fields = {
        "contract",
        "importer_id",
        "package_id",
        "profile_contract",
        "stage_schema",
        "manifest_sha256",
        "tables",
        "validation_sha256",
    }
    if not isinstance(receipt_value, Mapping):
        raise ReferenceFamilyArchiveError("reference family validation receipt is invalid")
    is_legacy = set(receipt_value) == common_fields | {"stage_schema_oid", "relation_oids", "sealed_owner_oid"}
    if not is_legacy and set(receipt_value) != common_fields | {"relations"}:
        raise ReferenceFamilyArchiveError("reference family validation receipt is invalid")
    spec = _validation_receipt_spec(receipt_value)
    schema = _schema_name(receipt_value["stage_schema"])
    names = _validation_receipt_names(receipt_value, spec, schema, is_legacy)
    tables = _validation_tables(receipt_value, spec)
    if not is_legacy and receipt_value["validation_sha256"] != _validation_digest(
        {key: field_value for key, field_value in receipt_value.items() if key != "validation_sha256"}
    ):
        raise ReferenceFamilyArchiveError("reference family validation digest differs")
    return ReferenceFamilyValidationReceipt(
        spec.importer_id,
        receipt_value["package_id"],
        receipt_value["profile_contract"],
        schema,
        tuple((schema, name) for name in names),
        receipt_value["manifest_sha256"],
        tables,
        receipt_value["validation_sha256"],
        json.loads(json.dumps(dict(receipt_value))) if is_legacy else None,
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

    _require_transaction(session)
    reference_family_spec(importer_id)
    _schema_name(schema_name)
    _source_metadata(source_metadata)
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    return await _capture_reference_family_source(
        session,
        importer_id=importer_id,
        schema_name=schema_name,
        source_metadata=source_metadata,
        dependencies=dependencies,
    )


async def _capture_reference_family_source(
    session: Any,
    *,
    importer_id: str,
    schema_name: str,
    source_metadata: Mapping[str, Any],
    dependencies: Mapping[str, str] | None = None,
    source_capture_contract: str | None = None,
    native_auxiliary: bool = False,
    canonical_source_fence: CanonicalSourceFence | None = None,
) -> ReferenceFamilySourceCapture:
    """Capture under the caller's established isolation and source fences."""

    _require_transaction(session)
    spec = (
        reference_family_spec(importer_id, canonical=True)
        if importer_id == "mrf" and native_auxiliary
        else reference_family_spec(importer_id)
    )
    schema = _schema_name(schema_name)
    _source_metadata(source_metadata)
    if importer_id == "mrf" and STAGE_TABLE in spec.table_names:
        await _lock_source_family(session, spec, schema, canonical_source_fence)
        await _require_canonical_source_catalog(session, spec, schema)
    async with _bounded_capture(session, lock_before_snapshot=spec.importer_id == "facility-anchors"):
        await _lock_source_family(session, spec, schema, canonical_source_fence)
        publication = None
        if importer_id == "mrf":
            publication = await require_completed_publication(session, schema, native_set=native_auxiliary)
        if spec.importer_id == "cms-doctors":
            source_capture_contract = await doctors_capture_contract(session, schema, source_capture_contract)
        if spec.importer_id == "geo":
            from process.geo_source_storage import require_geo_storage

            await require_geo_storage(session, schema_name=schema, is_incumbent=True)
            if source_capture_contract not in (None, IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT):
                raise ReferenceFamilyArchiveError("Geo capture requires immutable native storage")
            source_capture_contract = IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT
        source_serving_generation = await _capture_source_generation(
            session, spec, schema, source_metadata, source_capture_contract
        )
        if spec.importer_id in TERMINAL_CAPTURE_IMPORTERS:
            publication = source_metadata
        manifest = await _family_manifest(
            session,
            spec=spec,
            schema_name=schema,
            source_metadata=source_metadata,
            dependencies=dependencies,
            publication=publication,
            source_serving_generation=source_serving_generation,
        )
        if source_serving_generation is not None and spec.importer_id != "label":
            manifest = replace(
                manifest, source_capture_contract=source_capture_contract or GUARDED_SOURCE_CAPTURE_CONTRACT
            )
        snapshot = (await session.execute(text("SELECT pg_export_snapshot()"))).scalar_one()
        if not isinstance(snapshot, str) or _SNAPSHOT.fullmatch(snapshot) is None:
            raise ReferenceFamilyArchiveError("reference family source snapshot is invalid")
    return ReferenceFamilySourceCapture(manifest, schema, snapshot, canonical_source_fence)


async def _require_canonical_source_catalog(session, spec, schema):
    """Attest every locked native SOURCE relation before deriving its model rows."""
    from process.mrf_address_publication import require_canonical_source_model, require_native_read_catalog

    await require_native_read_catalog(
        session, tuple([await _relation_oid(session, schema, name) for name in spec.table_names if name != STAGE_TABLE])
    )
    await require_canonical_source_model(session, schema, lambda s, n: f"{_quoted(s)}.{_quoted(n)}")


async def capture_terminal_reference_source(session, *, importer_id, schema_name, run_id):
    """Observe current locked heaps and authentic terminal evidence, not a producer generation."""
    _require_transaction(session)
    if importer_id not in TERMINAL_CAPTURE_IMPORTERS or not isinstance(run_id, str) or not run_id:
        raise ReferenceFamilyArchiveError("terminal reference source selector is invalid")
    schema = _schema_name(schema_name)
    spec = reference_family_spec(importer_id)
    capability = TERMINAL_CAPABILITIES[importer_id]
    await _lock_source_family(session, spec, schema)
    await require_native_read_catalog(
        session,
        tuple(
            [
                await _relation_oid(session, schema, name)
                for name in (*tuple(_source_model_name(model) for model in spec.model_types), "import_run")
            ]
        ),
    )
    terminal = (
        (
            await session.execute(
                text(
                    f"SELECT run_id,status,phase_detail,created_at::text AS created_at,"
                    f"started_at::text AS started_at,finished_at::text AS finished_at,metrics "
                    f"FROM {_quoted(schema)}.import_run WHERE importer=ANY(CAST(:importers AS text[])) "
                    "ORDER BY created_at DESC NULLS FIRST,run_id DESC LIMIT 1"
                ),
                {"importers": list(capability.importers)},
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        terminal is None
        or terminal["run_id"] != run_id
        or terminal["status"] != "succeeded"
        or terminal["phase_detail"] != capability.phase
        or any(terminal[key] is None for key in ("created_at", "started_at", "finished_at"))
        or not isinstance(terminal["metrics"], Mapping)
        or terminal["metrics"].get("schema") != schema
        or not isinstance(terminal["metrics"].get("stage_suffix"), str)
        or not terminal["metrics"]["stage_suffix"]
    ):
        raise ReferenceFamilyArchiveError("terminal reference source evidence is unavailable or superseded")
    return {
        "contract": TERMINAL_CAPTURE_CONTRACT,
        "observed_run_id": run_id,
        **{key: terminal[key] for key in ("created_at", "started_at", "finished_at")},
        "stage_suffix": terminal["metrics"]["stage_suffix"],
    }


async def _capture_source_generation(session, spec, schema, source_metadata, source_capture_contract):
    """Keep captured and ordinary generation admission meanings unchanged."""
    if source_capture_contract == IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT and spec.importer_id == "cms-doctors":
        return await capture_doctors_serving_generation(session, schema)
    if source_capture_contract == IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT and spec.importer_id == "geo":
        from process.geo_source_storage import capture_geo_generation

        return await capture_geo_generation(session, schema)
    if source_capture_contract == IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT:
        if spec.importer_id != "nucc":
            raise ReferenceFamilyArchiveError("immutable capture requires the NUCC family")
        from process.reference_family_result_generation import require_immutable_nucc_storage

        authority = await read_reference_family_result_generation_authority(
            session, importer_id="nucc", schema_name=schema, lock=False
        )
        storage = await require_immutable_nucc_storage(session, schema_name=schema)
        relation = storage["child"] if "child" in storage else (schema, "nucc_taxonomy")
        if authority.serving_generation is None or authority.relations != (relation,):
            raise ReferenceFamilyArchiveError("immutable NUCC generation is unavailable or its selected child changed")
        return authority.serving_generation
    if spec.importer_id in TERMINAL_CAPTURE_IMPORTERS:
        if source_capture_contract is not None or not isinstance(source_metadata, Mapping):
            raise ReferenceFamilyArchiveError("terminal reference capture authority differs")
        expected = await capture_terminal_reference_source(
            session,
            importer_id=spec.importer_id,
            schema_name=schema,
            run_id=source_metadata.get("observed_run_id"),
        )
        if dict(source_metadata) != expected:
            raise ReferenceFamilyArchiveError("terminal reference capture authority differs")
        return None
    if source_capture_contract is not None:
        if source_capture_contract != CAPTURED_TIGER_CONTRACT or spec.importer_id != "tiger" or schema != "tiger":
            raise ReferenceFamilyArchiveError("reference family capture contract is unsupported")
        from process.tiger_captured_epoch import validate_captured_origin

        validate_captured_origin(source_metadata)
        return None
    generation = await _source_serving_generation(session, spec, schema)
    if spec.importer_id == "facility-anchors":
        from process.facility_address_contribution_merge import validate_observations

        if generation is None:
            raise ReferenceFamilyArchiveError("facility source generation is unavailable")
        await validate_observations(session, stage_schema=schema, schema=schema, bind_alias=False)
    return generation


async def _source_serving_generation(session, spec, schema):
    """Keep legacy exports generationless and reject untracked claimed origins."""
    if spec.importer_id in TERMINAL_CAPTURE_IMPORTERS:
        return None
    if not await _has_source_generation_authority(session, spec, schema):
        return None
    authority = await read_reference_family_result_generation_authority(
        session, importer_id=spec.importer_id, schema_name=schema
    )
    if authority.serving_generation is None:
        if spec.importer_id == "label" or authority.relations is not None:
            raise ReferenceFamilyArchiveError("reference family source generation is incomplete")
        return None
    try:
        return await capture_reference_family_serving_generation(
            session, importer_id=spec.importer_id, schema_name=schema
        )
    except RuntimeError as error:
        raise ReferenceFamilyArchiveError("reference family source generation is unavailable or drifted") from error


def _require_native_source_copy(source_copy, on_precreated):
    """Historical readers remain supported; new clones require explicit protected COPY custody."""
    if not isinstance(source_copy, ReferenceFamilySourceCopy) or not callable(on_precreated):
        raise ReferenceFamilyArchiveError("reference family protected source COPY capability is unavailable")


async def _clone_source(
    session: Any,
    capture: ReferenceFamilySourceCapture,
    stage_schema: str,
    *,
    source_copy: ReferenceFamilySourceCopy | None = None,
    deadline: float | None = None,
    on_precreated: Callable[..., Awaitable[None]] | None = None,
    authorize_creation: Callable[..., Awaitable[None]] | None = None,
) -> None:
    """Precreate the complete empty family before loading its pinned source snapshot."""
    _require_native_source_copy(source_copy, on_precreated)
    _require_frozen_source_capture(capture)
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise ReferenceFamilyArchiveError("reference family source COPY deadline is unavailable")
    if deadline <= asyncio.get_running_loop().time():
        raise TimeoutError("reference family source COPY deadline expired")
    if _SNAPSHOT.fullmatch(capture.postgres_snapshot) is None:
        raise ReferenceFamilyArchiveError("reference family source snapshot is invalid")
    if capture.manifest.importer_id == "nucc":
        raise ReferenceFamilyArchiveError("NUCC protected source requires dedicated preparation")
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    await session.execute(text(f"SET TRANSACTION SNAPSHOT '{capture.postgres_snapshot}'"))
    if authorize_creation is not None:
        await authorize_creation(session)
    if capture.canonical_source_fence is not None:
        await require_canonical_source_fence(session, capture.canonical_source_fence, capture.schema_name)
    spec = _manifest_family_spec(capture.manifest.as_dict())
    if spec.importer_id == "mrf" and STAGE_TABLE not in spec.table_names:
        raise ReferenceFamilyArchiveError("new MRF source requires the canonical model contract")
    ownership = await create_model_family_export_clone(session, spec, stage_schema)
    await on_precreated(session, ownership)
    await _copy_source_tables(session, capture, stage_schema, spec, source_copy, deadline)
    await _create_model_indexes(session, spec, stage_schema, create_constraints=True)
    await _rebase_owned_sequences(session, stage_schema, spec.importer_id)
    await _validate_source_clone_sets(session, capture, stage_schema)


async def _copy_source_tables(session, capture, stage_schema, spec, source_copy, deadline):
    """Debit one raw byte budget for the complete family and optional canonical projection."""
    models_by_table = {model.__tablename__: model for model in spec.model_types}
    remaining_bytes = source_copy.max_bytes
    for table in capture.manifest.tables:
        source_ref = f"{_quoted(capture.schema_name)}.{_quoted(archive_table_name() if table.table_name == STAGE_TABLE else _source_model_name(models_by_table[table.table_name]))}"
        column_names = tuple(column.name for column in models_by_table[table.table_name].__table__.columns)
        columns = ", ".join(_quoted(name) for name in column_names)
        predicate = (
            canonical_reference_filter(
                spec.importer_id, capture.schema_name, lambda schema, name: f"{_quoted(schema)}.{_quoted(name)}"
            )
            if table.table_name == STAGE_TABLE
            else _source_model_predicate(models_by_table[table.table_name], capture.schema_name)
        )
        query = (
            _frozen_source_query(capture.frozen_source_projection, models_by_table[table.table_name])
            if capture.frozen_source_projection is not None
            else f"SELECT {columns} FROM {source_ref} canonical {predicate}"
        )
        remaining_bytes = await _copy_source_projection(
            session, source_copy, query, stage_schema, table.table_name, column_names, remaining_bytes, deadline
        )


async def _copy_source_projection(session, source_copy, query, schema_name, table_name, columns, remaining, deadline):
    timeout = deadline - asyncio.get_running_loop().time()
    if timeout <= 0:
        raise TimeoutError("reference family source COPY deadline expired")
    query_args = ()
    if isinstance(query, TextClause):
        compiled = query.compile(dialect=postgresql.asyncpg.dialect())
        query_args = tuple(compiled.params[name] for name in compiled.positiontup)
        query = str(compiled)
    copied = await source_copy.copy_rows(
        session,
        query,
        *query_args,
        schema_name=schema_name,
        table_name=table_name,
        columns=columns,
        max_bytes=remaining,
        timeout=timeout,
    )
    if type(copied) is not int or not 0 <= copied <= remaining:
        raise ReferenceFamilyArchiveError("reference family source COPY accounting is invalid")
    return remaining - copied


async def native_copy_projection(session, query, *args, schema_name, table_name, columns, max_bytes, timeout):
    """Copy OUT/IN sequentially on the owning transaction, preserving its uncommitted
    rows without a second snapshot or concurrent driver use, within one bounded spool.
    """
    if (
        (type(max_bytes) is not int or not 0 <= max_bytes < 2**63)
        or (type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 86400)
        or (not isinstance(query, str) or not query.startswith("SELECT "))
        or (not isinstance(columns, (list, tuple)) or not 1 <= len(columns) <= 1600)
        or any(
            not isinstance(name, str) or not _IDENTIFIER.fullmatch(name) for name in (schema_name, table_name, *columns)
        )
        or len(set(columns)) != len(columns)
    ):
        raise ReferenceFamilyArchiveError("native model projection COPY bounds differ")
    driver = await _native_model_copy_driver(session, ("copy_from_query", "copy_to_table"))
    return await _copy_native_projection(
        (session, driver), query, schema_name, table_name, columns, max_bytes, timeout, *args
    )


async def _native_model_copy_driver(session, methods):
    """Require the caller's actual driver transaction before any native protocol command."""
    return await native_model_copy_driver(session, methods, _require_transaction, ReferenceFamilyArchiveError)


async def _capture_native_projection(driver, query, spool, max_bytes, deadline, *query_args, copy_format="binary"):
    """Drain native COPY OUT with a fixed spool cap before restoring any payload."""
    if copy_format not in {"binary", "text"}:
        raise ReferenceFamilyArchiveError("native model projection COPY format differs")
    accounting_by_field = {"copied_bytes": 0, "error": None}

    async def consume(chunk):
        """Bound native wire bytes, without interpreting or validating records."""
        if accounting_by_field["error"] is not None:
            return
        if accounting_by_field["copied_bytes"] + len(chunk) > max_bytes:
            accounting_by_field["error"] = ReferenceFamilyArchiveError("native model projection COPY byte cap exceeded")
            return
        try:
            spool.write(chunk)
        except Exception as error:
            accounting_by_field["error"] = error
            return
        accounting_by_field["copied_bytes"] += len(chunk)

    status = await driver.copy_from_query(
        query, *query_args, output=consume, format=copy_format, timeout=deadline - asyncio.get_running_loop().time()
    )
    if accounting_by_field["error"] is not None:
        raise accounting_by_field["error"]
    return status, accounting_by_field["copied_bytes"]


async def _copy_native_projection(transport, query, schema_name, table_name, columns, max_bytes, timeout, *query_args):
    """Keep sequential COPY OUT/IN inside one native deadline and caller-owned transaction."""
    copy_owner, driver = transport
    with TemporaryFile(mode="w+b") as spool:
        try:
            async with asyncio.timeout(timeout) as deadline:
                captured, copied_bytes = await _capture_native_projection(
                    driver, query, spool, max_bytes, deadline.when(), *query_args
                )
                spool.seek(0)
                restored = await driver.copy_to_table(
                    table_name,
                    schema_name=schema_name,
                    columns=tuple(columns),
                    source=spool,
                    format="binary",
                    timeout=deadline.when() - asyncio.get_running_loop().time(),
                )
                if not isinstance(captured, str) or not re.fullmatch(r"COPY [0-9]+", captured) or captured != restored:
                    raise ReferenceFamilyArchiveError("native model projection COPY count differs")
        except (asyncio.CancelledError, TimeoutError) as interruption:
            await cleanup_interrupted_copy(copy_owner, driver, interruption)
            raise
    return copied_bytes


async def native_copy_record_batch(session, model_type, *, schema_name, table_name, columns, records, timeout=300):
    """Append one fixed-size model batch without INSERT fallback or row hooks."""
    _require_transaction(session)
    if (
        tuple(columns) != tuple(column.name for column in model_type.__table__.columns)
        or not isinstance(records, (list, tuple))
        or not 0 < len(records) <= 5000
        or type(timeout) not in (int, float)
        or not math.isfinite(timeout)
        or not 0 < timeout <= 300
        or any(not isinstance(name, str) or not _IDENTIFIER.fullmatch(name) for name in (schema_name, table_name))
    ):
        raise ReferenceFamilyArchiveError("native model acquisition COPY scope differs")
    driver = await _native_model_copy_driver(session, ("copy_records_to_table",))
    try:
        async with asyncio.timeout(timeout):
            copied = await driver.copy_records_to_table(
                table_name, schema_name=schema_name, columns=tuple(columns), records=records, timeout=timeout
            )
    except asyncio.CancelledError, TimeoutError:
        driver.terminate()
        raise
    if copied != f"COPY {len(records)}":
        raise ReferenceFamilyArchiveError("native model acquisition COPY count differs")
    return len(records)


def model_family_roles(roles=None):
    """Read configured administrative role names without inspecting database identities."""
    if roles is None:
        from process.source_profile_result_pins import load_role_policy

        owner, readers = load_role_policy()
    else:
        owner = roles.owner_role
        readers = (roles.publisher_role, *roles.runtime_roles)
    if not isinstance(owner, str) or not _IDENTIFIER.fullmatch(owner):
        raise ReferenceFamilyArchiveError("native model owner role declaration is required")
    readers = tuple(dict.fromkeys((owner, *readers)))
    if not readers or any(
        not isinstance(role, str) or not _IDENTIFIER.fullmatch(role) or len(role.encode()) > 63 for role in readers
    ):
        raise ReferenceFamilyArchiveError("native model reader role declaration is invalid")
    return owner, readers


def _model_family_names(ownership):
    names = getattr(ownership, "relation_names", None)
    if names is None:
        names = tuple(name for name, _legacy in ownership.relation_oids)
    names = tuple(names)
    if ownership.importer_id == "mrf" and STAGE_TABLE not in names:
        names = (*names, STAGE_TABLE)
    if not names or len(set(names)) != len(names) or any(not _IDENTIFIER.fullmatch(name) for name in names):
        raise ReferenceFamilyArchiveError("native model family declaration differs")
    return names


async def _require_model_family_namespace(session, ownership, schema, local_pairs):
    local_sequences = await _owned_sequences(session, await _schema_oid(session, schema), include_identity=True)
    sequence_names = getattr(ownership, "sequence_names", None)
    if sequence_names is None:
        sequence_names = tuple(
            (name, table, column) for name, _legacy, table, column in getattr(ownership, "sequence_oids", ())
        )
    owned_sequences = {(name, table, column) for name, _oid, table, column in local_sequences}
    family_tables = {name for name, _oid in local_pairs}
    # A migrated table owns its serial or identity sequence by name; each must belong to a family column.
    if any(table not in family_tables for _name, table, _column in owned_sequences) or not (
        set(sequence_names) <= owned_sequences
    ):
        raise ReferenceFamilyArchiveError("native model sequence declaration differs")
    expected = {oid for _name, oid in local_pairs} | {oid for _name, oid, _table, _column in local_sequences}
    for relation in await _namespace_relations(session, await _schema_oid(session, schema)):
        kind = relation["relkind"].decode() if isinstance(relation["relkind"], bytes) else relation["relkind"]
        if not (
            (kind in {"r", "S"} and relation["oid"] in expected)
            or (kind == "i" and relation["index_table_oid"] in expected)
        ):
            raise ReferenceFamilyArchiveError("native model namespace contains an unexpected relation")


async def seal_model_family_storage(
    session, ownership, legacy_owner=None, *, require_empty=False, roles=None, freeze_schema=True
):
    """Close named candidate writes using the existing configured administrative freeze."""
    from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot

    _require_transaction(session)
    if type(freeze_schema) is not bool:
        raise ReferenceFamilyArchiveError("native model freeze phase is invalid")
    owner, readers = model_family_roles(roles)
    schema = _schema_name(ownership.schema_name)
    names = _model_family_names(ownership)
    local_pairs = tuple([(name, await _relation_oid(session, schema, name)) for name in names])
    if any(oid is None for _name, oid in local_pairs):
        raise ReferenceFamilyArchiveError("native model family is incomplete")
    for name, _oid in local_pairs:
        if require_empty and await session.scalar(
            text(f"SELECT EXISTS(SELECT 1 FROM {_quoted(schema)}.{_quoted(name)})")
        ):
            raise ReferenceFamilyArchiveError("native model heap is not empty")
    await _require_model_family_namespace(session, ownership, schema, local_pairs)
    driver = await _native_model_copy_driver(session, ("execute", "fetch", "fetchval"))
    await driver.execute(f"ALTER SCHEMA {_quoted(schema)} OWNER TO {_quoted(owner)}")
    await freeze_fhir_source_snapshot(
        driver,
        schema,
        namespace_owner=owner,
        reader_roles=readers,
        application_roles=(),
        relation_names=None if freeze_schema else names,
        freeze_schema=freeze_schema,
    )


async def _copy_model_run_scope(
    session, spec, *, source_schema, target_schema, target_names, run_scope, source_copy, deadline
):
    """COPY a closed model/run projection under one family's byte cap and deadline."""
    if not isinstance(run_scope, (list, tuple)) or len(run_scope) != 2:
        raise ReferenceFamilyArchiveError("source COPY model/run scope is invalid")
    run_columns, run_ids = run_scope
    if (
        not isinstance(spec, ReferenceFamilySpec)
        or not isinstance(source_copy, ReferenceFamilySourceCopy)
        or not spec.model_types
        or not isinstance(run_ids, (list, tuple))
        or not 0 < len(run_ids) <= 64
        or any(not isinstance(run, str) or re.fullmatch(r"[0-9a-f]{32}|[0-9a-f]{64}", run) is None for run in run_ids)
        or len(set(run_ids)) != len(run_ids)
        or not isinstance(target_names, (list, tuple))
        or len(target_names) != len(spec.model_types)
    ):
        raise ReferenceFamilyArchiveError("source COPY model/run scope is invalid")
    if (
        not isinstance(run_columns, (list, tuple))
        or len(run_columns) != len(spec.model_types)
        or any(
            not isinstance(column, str) or column not in model.__table__.c
            for model, column in zip(spec.model_types, run_columns, strict=True)
        )
    ):
        raise ReferenceFamilyArchiveError("source COPY run column differs from the model")
    for name in (source_schema, target_schema, *target_names, *spec.table_names):
        if not isinstance(name, str) or _IDENTIFIER.fullmatch(name) is None:
            raise ReferenceFamilyArchiveError("source COPY model identifier is invalid")
    if len(set(target_names)) != len(target_names):
        raise ReferenceFamilyArchiveError("source COPY model/run scope is invalid")
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise ReferenceFamilyArchiveError("source COPY deadline is unavailable")
    run_literals = ",".join("'" + run + "'" for run in run_ids)
    remaining = source_copy.max_bytes
    for model, destination_name, column in zip(spec.model_types, target_names, run_columns, strict=True):
        columns = tuple(field.name for field in model.__table__.columns)
        query = (
            "SELECT "
            + ", ".join(_quoted(field) for field in columns)
            + f" FROM {_quoted(source_schema)}.{_quoted(model.__tablename__)}"
            + f" WHERE {_quoted(column)}=ANY(ARRAY[{run_literals}]::text[])"
        )
        remaining = await _copy_source_projection(
            session, source_copy, query, target_schema, destination_name, columns, remaining, deadline
        )
    return remaining


async def _validate_source_clone_sets(session, capture, stage_schema):
    """Compare every indexed model and the exact canonical projection in the pinned snapshot."""
    spec = _manifest_family_spec(capture.manifest.as_dict())
    if capture.frozen_source_projection is not None:
        await _validate_frozen_source_clone(session, capture, stage_schema, spec)
        return
    for model in spec.model_types:
        if not await _is_model_table_equal(
            session,
            model,
            left_schema=capture.schema_name,
            left_name=archive_table_name() if model.__tablename__ == STAGE_TABLE else _source_model_name(model),
            right_schema=stage_schema,
            right_name=model.__tablename__,
            left_predicate=(
                canonical_reference_filter(
                    spec.importer_id, capture.schema_name, lambda schema, name: f"{_quoted(schema)}.{_quoted(name)}"
                )
                if model.__tablename__ == STAGE_TABLE
                else _source_model_predicate(model, capture.schema_name) or None
            ),
        ):
            raise ReferenceFamilyArchiveError("reference family source clone differs from its pinned source")
    if spec.importer_id == "mrf" and STAGE_TABLE in spec.table_names:
        await validate_canonical_closure(
            session,
            spec.importer_id,
            stage_schema,
            lambda schema, name: f"{_quoted(schema)}.{_quoted(name)}",
            address_coverage=capture.manifest.auxiliary.get("address_coverage"),
        )
    if spec.importer_id == "drug-claims":
        await validate_prescription_dictionary_closure(session, stage_schema)


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
        await _lock_family(session, stage_schema, _ownership_spec(ownership).archive_names, "ACCESS EXCLUSIVE")
        # Reading each sequence retains a relation lock; verify the locked OIDs before nontransactional setval.
        for name, oid, _table, _column in ownership.sequence_oids:
            await session.execute(text(f"SELECT last_value FROM {_quoted(stage_schema)}.{_quoted(name)}"))
            sequence_oid_by_name[name] = f"{_quoted(stage_schema)}.{_quoted(name)}"
        ownership = await verify_reference_family_stage_ownership(session, ownership)
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
    *,
    include_identity: bool = False,
) -> tuple[tuple[str, int, str, str], ...]:
    dependency = "dependency.deptype IN ('a','i')" if include_identity else "dependency.deptype='a'"
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
                    f"AND {dependency} "
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
    return await capture_model_family_stage_ownership(session, spec, dataset_id)


def _require_model_family_spec(spec: ReferenceFamilySpec) -> None:
    if (
        not isinstance(spec, ReferenceFamilySpec)
        or not spec.model_types
        or len(set(spec.table_names)) != len(spec.table_names)
        or any(_IDENTIFIER.fullmatch(table_name) is None for table_name in spec.table_names)
    ):
        raise ReferenceFamilyArchiveError("reference family model declaration is invalid")


async def capture_model_family_stage_ownership(
    session: Any,
    spec: ReferenceFamilySpec,
    dataset_id: UUID,
    *,
    include_identity: bool = False,
) -> ReferenceFamilyStageOwnership:
    """Capture a complete UUID-owned model stage without registering a family."""

    _require_transaction(session)
    _require_model_family_spec(spec)
    if include_identity:
        return await _capture_model_family_ownership(session, spec, dataset_id, include_identity=True)
    return await _capture_model_family_ownership(session, spec, dataset_id)


async def _capture_model_family_ownership(
    session: Any,
    spec: ReferenceFamilySpec,
    dataset_id: UUID,
    *,
    include_identity: bool = False,
) -> ReferenceFamilyStageOwnership:
    """Capture installed model storage without adding an importer registry entry."""
    _require_transaction(session)
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
    if spec.importer_id == "mrf" and STAGE_TABLE not in spec.table_names:
        auxiliary_oid = await _relation_oid(session, schema_name, STAGE_TABLE)
        if auxiliary_oid is None:
            raise ReferenceFamilyArchiveError("MRF canonical auxiliary relation is missing")
        owned_oids.add(auxiliary_oid)
    sequence_oids = await _owned_sequences(session, schema_oid, include_identity=include_identity)
    expected_sequences = _expected_model_family_sequences(spec, include_identity=include_identity)
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


def _expected_model_family_sequences(spec: ReferenceFamilySpec, *, include_identity: bool):
    """Keep declared serials and optional native identities in one exact inventory."""
    expected_sequences = _OWNED_SEQUENCES.get(spec.importer_id, ())
    if include_identity:
        expected_sequences = tuple(
            sorted(
                (
                    *expected_sequences,
                    *(
                        (f"{model.__tablename__}_{column.name}_seq", model.__tablename__, column.name)
                        for model in spec.model_types
                        for column in model.__table__.columns
                        if column.identity is not None
                    ),
                )
            )
        )
    return expected_sequences


async def verify_reference_family_stage_ownership(
    session: Any,
    ownership: ReferenceFamilyStageOwnership,
) -> ReferenceFamilyStageOwnership:
    """Recheck a local owner token against current catalog identities."""

    _require_transaction(session)
    if not isinstance(ownership, ReferenceFamilyStageOwnership):
        raise ReferenceFamilyArchiveError("reference family stage ownership is invalid")
    return await verify_model_family_stage_ownership(session, _ownership_spec(ownership), ownership)


async def verify_model_family_stage_ownership(
    session: Any,
    spec: ReferenceFamilySpec,
    ownership: ReferenceFamilyStageOwnership,
    *,
    include_identity: bool = False,
) -> ReferenceFamilyStageOwnership:
    """Recheck exact model, heap and sequence custody in the caller transaction."""

    _require_transaction(session)
    _require_model_family_spec(spec)
    if not isinstance(ownership, ReferenceFamilyStageOwnership):
        raise ReferenceFamilyArchiveError("reference family stage ownership is invalid")
    observed = await capture_model_family_stage_ownership(
        session, spec, ownership.dataset_id, include_identity=include_identity
    )
    if (
        observed.importer_id != ownership.importer_id
        or observed.schema_name != ownership.schema_name
        or tuple(name for name, _ in observed.relation_oids) != tuple(name for name, _ in ownership.relation_oids)
        or tuple((name, table, column) for name, _oid, table, column in observed.sequence_oids)
        != tuple((name, table, column) for name, _oid, table, column in ownership.sequence_oids)
    ):
        raise ReferenceFamilyArchiveError("reference family stage declaration differs")
    return observed


def _is_legacy_cms_manifest(manifest: ReferenceFamilyManifest) -> bool:
    return manifest.importer_id == "cms-doctors" and len(manifest.tables) == 2


def _has_matching_manifest_stage_tables(manifest, tables) -> bool:
    if manifest.importer_id == "geo":
        return has_compatible_geo_indexes(manifest, tables)
    if not _is_legacy_cms_manifest(manifest):
        return tables == manifest.tables or _has_compatible_cms_group_indexes(manifest, tables)
    return (
        len(tables) == 3
        and tables[:2] == manifest.tables
        and tables[2].model_name == "CMSDoctorGroupSite"
        and tables[2].table_name == "cms_doctor_group_site"
        and tables[2].row_count == 0
    )


def _has_compatible_cms_group_indexes(manifest, tables) -> bool:
    """Allow only the reviewed address-index addition on an otherwise exact CMS family."""
    if manifest.importer_id != "cms-doctors" or len(manifest.tables) != 3 or len(tables) != 3:
        return False
    if tables[:2] != manifest.tables[:2]:
        return False
    expected, observed = manifest.tables[2], tables[2]
    expected_fields, observed_fields = expected.as_dict(), observed.as_dict()
    expected_fields.pop("schema_sha256")
    observed_fields.pop("schema_sha256")
    return expected_fields == observed_fields and any(
        (expected.schema_sha256, observed.schema_sha256) == hashes for hashes in _cms_group_index_schema_hashes()
    )


def _has_matching_family_manifest(manifest, observed) -> bool:
    """Keep exact manifest identity except the reviewed CMS and Geo index transitions."""
    expected_fields, observed_fields = manifest.as_dict(), observed.as_dict()
    if manifest.importer_id == "geo" and not has_compatible_geo_indexes(manifest, observed.tables):
        return False
    if manifest.importer_id == "geo" or _has_compatible_cms_group_indexes(manifest, observed.tables):
        observed_fields["tables"] = expected_fields["tables"]
        observed_fields["schema_sha256"] = expected_fields["schema_sha256"]
    return expected_fields == observed_fields


def _cms_group_index_schema_hashes():
    """Pair exact old/new catalog digests, retaining each PostgreSQL constraint shape."""
    columns = _legacy_cms_group_columns()
    for constraints in _cms_group_constraint_orders(columns):
        yield tuple(
            catalog_identity._canonical_digest(
                {
                    "table_name": "cms_doctor_group_site",
                    "columns": columns,
                    "constraints": constraints,
                    "indexes": sorted(indexes, key=_canonical_json),
                }
            )
            for indexes in (_legacy_cms_group_indexes()[:3], _legacy_cms_group_indexes())
        )


def _cms_group_constraint_orders(columns):
    """Retain exact catalog order under C and en_US collations without rehashing archives."""
    not_null_constraints = _legacy_cms_group_constraints(columns, [{"contype": "n"}])
    return (
        _legacy_cms_group_constraints(columns, []),
        sorted(not_null_constraints, key=lambda entry: (entry["contype"], entry["key_columns"])),
        sorted(not_null_constraints, key=lambda entry: (entry["contype"], entry["key_columns"].strip("{}"))),
    )


def _legacy_cms_group_columns():
    """Describe the synthesized group's columns from the trusted model."""
    columns = []
    for position, column in enumerate(models.CMSDoctorGroupSite.__table__.columns, 1):
        type_name = str(column.type.compile(dialect=postgresql.dialect())).lower()
        type_name = type_name.replace("varchar", "character varying")
        is_collatable = type_name == "text" or type_name.startswith("character varying")
        columns.append(
            {
                "attnum": position,
                "attname": column.name,
                "type": type_name,
                "attnotnull": not column.nullable,
                "attgenerated": "",
                "attidentity": "",
                "collation_schema": "pg_catalog" if is_collatable else None,
                "collation_name": "default" if is_collatable else None,
                "default_expression": None,
            }
        )
    return columns


def _legacy_cms_group_constraints(columns, constraints):
    """Describe the primary key and version-dependent NOT NULL catalog entries."""
    # PostgreSQL 18 also exposes NOT NULL constraints; column identity above
    # verifies their semantics on earlier supported PostgreSQL versions.
    expected_constraints = [
        {
            "contype": "p",
            "condeferrable": False,
            "condeferred": False,
            "convalidated": True,
            "key_columns": "{1}",
            "referenced_columns": None,
            "referenced_table": None,
            "referenced_in_archive_schema": None,
            "check_expression": None,
        }
    ]
    if any(constraint["contype"] == "n" for constraint in constraints):
        expected_constraints.extend(
            {
                **expected_constraints[0],
                "contype": "n",
                "key_columns": "{" + str(column["attnum"]) + "}",
            }
            for column in columns
            if column["attnotnull"]
        )
    return expected_constraints


def _legacy_cms_group_indexes():
    """Describe the reviewed primary key and three lookup indexes."""
    indexes = []
    for attribute, is_primary, opclass, is_collatable in (
        (1, True, "int8_ops", False),
        (2, False, "int8_ops", False),
        (4, False, "text_ops", True),
        (5, False, "text_ops", True),
    ):
        indexes.append(
            {
                "indisunique": is_primary,
                "indisprimary": is_primary,
                "indimmediate": True,
                "indisvalid": True,
                "indnkeyatts": 1,
                "indnatts": 1,
                "method": "btree",
                "predicate": None,
                "expressions": None,
                "keys": str(attribute),
                "options": "0",
                "key_attributes": [
                    {
                        "position": 0,
                        "attribute_number": attribute,
                        "collation_schema": "pg_catalog" if is_collatable else None,
                        "collation_name": "default" if is_collatable else None,
                        "opclass_schema": "pg_catalog",
                        "opclass_name": opclass,
                    }
                ],
            }
        )
    return indexes


async def _require_legacy_cms_group_schema(session, schema_name: str) -> None:
    """Reject synthesized empty tables whose schema differs from the trusted model."""
    relation_oid = await _relation_oid(session, schema_name, models.CMSDoctorGroupSite.__tablename__)
    columns = _legacy_cms_group_columns()
    observed_columns = await catalog_identity._catalog_columns(session, relation_oid)
    constraints = await catalog_identity._catalog_constraints(session, relation_oid, schema_name)
    for constraint in constraints:
        if isinstance(constraint["contype"], bytes):
            constraint["contype"] = constraint["contype"].decode("ascii")
    expected_constraints = _legacy_cms_group_constraints(columns, constraints)
    observed_indexes = await catalog_identity._catalog_indexes(session, relation_oid)
    known_indexes = _legacy_cms_group_indexes()
    if (
        observed_columns != columns
        or sorted(map(_canonical_json, constraints)) != sorted(map(_canonical_json, expected_constraints))
        or sorted(map(_canonical_json, observed_indexes))
        not in (sorted(map(_canonical_json, known_indexes[:3])), sorted(map(_canonical_json, known_indexes)))
    ):
        raise ReferenceFamilyArchiveError("legacy CMS synthesized group schema differs")


async def _validate_stage_manifest(
    session: Any,
    *,
    ownership: ReferenceFamilyStageOwnership,
    manifest: ReferenceFamilyManifest,
) -> tuple[ReferenceTableReceipt, ...]:
    """Require restored models, accounting and family closure to match the sealed manifest."""
    validated = validate_reference_family_manifest(manifest)
    if validated.importer_id != ownership.importer_id:
        raise ReferenceFamilyArchiveError("reference family stage scope differs")
    spec = (
        _ownership_spec(ownership) if _is_legacy_cms_manifest(validated) else _manifest_family_spec(validated.as_dict())
    )
    if spec.importer_id == "nucc":
        await validate_nucc_reference_set(session, ownership=ownership)
    observed = await _family_manifest(
        session,
        spec=spec,
        schema_name=ownership.schema_name,
        source_metadata=validated.source_metadata,
        dependencies=validated.dependencies,
        auxiliary=validated.auxiliary,
        source_serving_generation=validated.source_serving_generation,
    )
    observed = replace(
        observed,
        source_capture_contract=validated.source_capture_contract,
        publication_authority=validated.publication_authority,
    )
    if _is_legacy_cms_manifest(validated):
        if not _has_matching_manifest_stage_tables(validated, observed.tables):
            raise ReferenceFamilyArchiveError("reference family restored stage differs")
        await _require_legacy_cms_group_schema(session, ownership.schema_name)
    elif not _has_matching_family_manifest(validated, observed):
        raise ReferenceFamilyArchiveError("reference family restored stage differs")
    if spec.importer_id == "mrf" and STAGE_TABLE in spec.table_names:
        await _validate_stage_canonical_closure(session, ownership, validated)
    if spec.importer_id == "claims-pricing":
        await validate_claims_dictionary_closure(session, ownership.schema_name)
    if spec.importer_id == "drug-claims":
        await validate_prescription_dictionary_closure(session, ownership.schema_name)
        if _ownership_spec(ownership).model_types != spec.model_types:
            await validate_reference_dictionary_effects(session, ownership)
            return (
                *observed.tables,
                *tuple(
                    [
                        await _table_receipt(
                            session, importer_id=spec.importer_id, schema_name=ownership.schema_name, model_type=model
                        )
                        for model in _DRUG_EFFECT_MODELS
                    ]
                ),
            )
    return observed.tables


async def _validate_stage_canonical_closure(session, ownership, manifest):
    """Recheck canonical closure against the restored stage's recorded raw accounting."""
    await validate_canonical_closure(
        session,
        "mrf",
        ownership.schema_name,
        lambda schema, name: f"{_quoted(schema)}.{_quoted(name)}",
        address_coverage=manifest.auxiliary.get("address_coverage"),
    )


async def validate_nucc_reference_set(session: Any, *, ownership: ReferenceFamilyStageOwnership) -> dict[str, Any]:
    """Check the complete indexed taxonomy once, without fetching or encoding rows."""
    _require_transaction(session)
    if not isinstance(ownership, ReferenceFamilyStageOwnership) or ownership.importer_id != "nucc":
        raise ReferenceFamilyArchiveError("NUCC set ownership is invalid")
    ownership = await verify_reference_family_stage_ownership(session, ownership)
    model = reference_family_spec("nucc").model_types[0]
    return await _validate_nucc_table_set(session, ownership.schema_name, model.__tablename__)


async def _validate_nucc_table_set(session, schema_name, table_name):
    """Share one indexed aggregate validator between native and received NUCC candidates."""
    model = reference_family_spec("nucc").model_types[0]
    relation = f"{_quoted(schema_name)}.{_quoted(table_name)}"
    text_bytes = " + ".join(
        f"COALESCE(octet_length({_quoted(column.name)}),0)::bigint"
        for column in model.__table__.columns
        if column.name != "int_code"
    )
    census_by_field = (
        (
            await session.execute(
                text(
                    f"SELECT count(*)::bigint AS row_count, count(DISTINCT code)::bigint AS distinct_codes, "
                    "count(*) FILTER (WHERE code IS NULL OR code !~ '^[A-Z0-9]{10}$' OR int_code IS NULL)::bigint AS invalid, "
                    f"COALESCE(sum(36 + 2 * ({text_bytes})),0)::bigint AS csv_upper_bound FROM {relation}"
                )
            )
        )
        .mappings()
        .one()
    )
    count = census_by_field["row_count"]
    invalid = census_by_field["invalid"]
    upper_bound = census_by_field["csv_upper_bound"]
    if (
        type(count) is not int
        or not 1 <= count <= 10_000
        or census_by_field["distinct_codes"] != count
        or invalid != 0
        or type(upper_bound) is not int
        or not 0 < upper_bound <= 8 * 1024**2
    ):
        raise ReferenceFamilyArchiveError("NUCC indexed set is invalid or exceeds its bounds")
    return {"contract": "nucc-indexed-set.v1", "row_count": count, "csv_upper_bound": upper_bound}


async def prepare_nucc_native_publication_stage(session, *, stage_model, schema_name, native_stage=None):
    """Complete and validate the isolated named model before atomic cutover."""
    _require_transaction(session)
    schema, table = _schema_name(schema_name), stage_model.__tablename__
    if (
        not isinstance(table, str)
        or not table.startswith(models.NUCCTaxonomy.__tablename__ + "_")
        or _IDENTIFIER.fullmatch(table) is None
    ):
        raise ReferenceFamilyArchiveError("NUCC native candidate name is invalid")
    await session.execute(text(f"LOCK TABLE ONLY {_quoted(schema)}.{_quoted(table)} IN SHARE MODE NOWAIT"))
    oid = await _relation_oid(session, schema, table)
    if oid is None:
        raise ReferenceFamilyArchiveError("NUCC native candidate is missing")
    if native_stage is not None:
        await _require_nucc_precreated_stage(session, native_stage)
        if (native_stage["schema_name"], native_stage["stage"]["table_name"]) != (schema, table):
            raise ReferenceFamilyArchiveError("NUCC native candidate differs from attempt declaration")
    predicate = (
        "NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=relation.oid AND contype!='n') "
        if native_stage is not None
        else "EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=relation.oid AND contype='p' AND conkey=ARRAY[CAST(:code_attnum AS smallint)] AND convalidated AND NOT condeferrable AND NOT condeferred) "
    )
    accepted = await session.scalar(
        text(
            "SELECT relkind='r' AND relpersistence='p' AND NOT relispartition AND NOT relrowsecurity AND NOT relforcerowsecurity "
            "AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=relation.oid OR inhparent=relation.oid) "
            "AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=relation.oid AND NOT tgisinternal) "
            "AND NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=relation.oid AND contype NOT IN ('p','n')) AND "
            + predicate
            + "FROM pg_class relation WHERE relation.oid=:oid"
        ),
        {"oid": oid, "code_attnum": list(models.NUCCTaxonomy.__table__.columns.keys()).index("code") + 1},
    )
    if accepted is not True:
        raise ReferenceFamilyArchiveError("NUCC native candidate catalog differs")
    await _require_nucc_native_columns(session, oid)
    await _create_model_indexes(
        session,
        ReferenceFamilySpec("nucc", (stage_model,)),
        schema,
        **({"create_constraints": True} if native_stage is not None else {}),
    )
    evidence = await _validate_nucc_table_set(session, schema, table)
    if (
        await session.scalar(
            text(
                "SELECT NOT EXISTS(SELECT 1 FROM pg_index WHERE indrelid=:oid AND (NOT indisvalid OR NOT indisready))"
            ),
            {"oid": oid},
        )
        is not True
    ):
        raise ReferenceFamilyArchiveError("NUCC native candidate indexes differ")
    return {"relation": (schema, table), "row_count": evidence["row_count"]}


async def _require_nucc_native_columns(session, relation_oid):
    """Compare the actual native candidate to the fixed model, without custom defaults."""
    type_name_by_ddl = {"INTEGER": "integer", "VARCHAR": "character varying", "TEXT": "text"}
    expected_columns = [
        (column.name, type_name_by_ddl[str(column.type.compile(dialect=postgresql.dialect()))], not column.nullable)
        for column in models.NUCCTaxonomy.__table__.columns
    ]
    columns = await catalog_identity._catalog_columns(session, relation_oid)
    if [(column["attname"], column["type"], column["attnotnull"]) for column in columns] != expected_columns or any(
        column["attgenerated"] or column["attidentity"] or column["default_expression"] for column in columns
    ):
        raise ReferenceFamilyArchiveError("NUCC native candidate columns differ")


async def rename_nucc_published_indexes(session, *, schema_name, expected_relation_oid=None):
    """Free attempt index names using native named DDL; old address arguments are opaque."""
    _require_transaction(session)
    schema = _schema_name(schema_name)
    indexes = (
        await session.execute(
            text(
                "SELECT index_relation.relname FROM pg_catalog.pg_index index_entry "
                "JOIN pg_catalog.pg_class index_relation ON index_relation.oid=index_entry.indexrelid "
                "WHERE index_entry.indrelid=to_regclass(:relation) ORDER BY index_relation.relname"
            ),
            {"relation": f'{_quoted(schema)}."nucc_taxonomy"'},
        )
    ).all()
    if not 1 <= len(indexes) <= 16:
        raise ReferenceFamilyArchiveError("NUCC published index inventory differs")
    suffix = uuid4().hex
    for position, (name,) in enumerate(indexes):
        await session.execute(
            text(f"ALTER INDEX {_quoted(schema)}.{_quoted(name)} RENAME TO {_quoted(f'nucc_idx_{suffix}_{position}')}")
        )


def nucc_native_digest(value):
    """Hash bounded native control metadata with the existing canonical JSON encoding."""
    encoded = _canonical_json(value)
    if len(encoded) > 131_072:
        raise ReferenceFamilyArchiveError("NUCC native metadata exceeds its bound")
    return hashlib.sha256(encoded).hexdigest()


async def is_nucc_native_handoff_required(session, *, schema_name):
    """Route the fixed partitioned serving name through its atomic publisher path."""
    _require_transaction(session)
    kind = await session.scalar(
        text("SELECT relkind::text FROM pg_catalog.pg_class WHERE oid=to_regclass(:relation)"),
        {"relation": f'{_quoted(_schema_name(schema_name))}."nucc_taxonomy"'},
    )
    if kind not in (None, "r", "p"):
        raise ReferenceFamilyArchiveError("NUCC serving relation kind differs")
    return kind == "p"


async def bind_nucc_native_attempt(session, ctx, *, schema_name):
    """Record logical predecessor provenance before creating an isolated attempt."""
    if not await is_nucc_native_handoff_required(session, schema_name=schema_name):
        return None
    run_id, attempt_id, started, suffix = _nucc_attempt(ctx)
    authority = await read_reference_family_result_generation_authority(
        session, importer_id="nucc", schema_name=schema_name, lock=False
    )
    from process.reference_family_result_generation import require_nucc_native_predecessor_storage

    storage = await require_nucc_native_predecessor_storage(session, schema_name=schema_name)
    relation = storage["child"] if storage is not None and "child" in storage else (schema_name, "nucc_taxonomy")
    if authority.serving_generation is None or authority.relations != (relation,):
        raise ReferenceFamilyArchiveError("NUCC native predecessor is untracked or its selected child changed")
    ctx["import_date"] = suffix
    ctx.setdefault("context", {})["nucc_native_predecessor"] = authority.as_dict()
    stage = await _precreate_nucc_attempt(
        session, schema_name, run_id, attempt_id, started, suffix, incumbent=authority.as_dict()
    )
    ctx["context"]["nucc_native_stage"] = stage
    return stage


async def copy_nucc_native_batch(session, stage_receipt, stage_model, rows):
    """Authenticate the durable precreated heap before one bounded native COPY."""
    await _require_nucc_precreated_stage(session, stage_receipt)
    if stage_model.__tablename__ != stage_receipt["stage"]["table_name"]:
        raise ReferenceFamilyArchiveError("NUCC native COPY model differs")
    columns = tuple(models.NUCCTaxonomy.__table__.columns.keys())
    return await native_copy_record_batch(
        session,
        stage_model,
        schema_name=stage_receipt["schema_name"],
        table_name=stage_receipt["stage"]["table_name"],
        columns=columns,
        records=[tuple(row.get(name) for name in columns) for row in rows],
    )


def validate_nucc_native_handoff(handoff_by_field):
    """Decode current named or unchanged historical receipts without physical authority."""
    _require_nucc_handoff_fields(handoff_by_field)
    if len(_canonical_json(handoff_by_field)) > 131072:
        raise ReferenceFamilyArchiveError("NUCC native handoff metadata exceeds its bound")
    handoff = json.loads(_canonical_json(handoff_by_field))
    _run, _attempt, _started, suffix = _nucc_attempt(
        {
            "control_run_id": handoff["run_id"],
            "context": {
                "_control_attempt_id": handoff["attempt_id"],
                "_control_attempt_started_at": handoff["attempt_started_at"],
            },
        }
    )
    if (
        handoff["import_date"] != suffix
        or _schema_name(handoff["schema_name"]) != handoff["schema_name"]
        or type(handoff["node_id"]) is not str
        or not 0 < len(handoff["node_id"]) <= 128
        or type(handoff["row_count"]) is not int
        or not 0 < handoff["row_count"] <= 10_000
        or any(
            type(handoff[key]) is not str or re.fullmatch(r"[0-9a-f]{64}", handoff[key]) is None
            for key in ("source_contract_sha256", "handoff_sha256")
        )
    ):
        raise ReferenceFamilyArchiveError("NUCC native handoff scope differs")
    _validate_nucc_native_relation(handoff["stage"], suffix)
    if ("database_oid" in handoff) != ("relation_oid" in handoff["stage"]):
        raise ReferenceFamilyArchiveError("NUCC native handoff edition differs")
    is_immutable = handoff["contract"] == NUCC_IMMUTABLE_HANDOFF_CONTRACT
    authority = _nucc_result_authority(
        handoff["incumbent"], allow_untracked=is_immutable, declared_schema=handoff["schema_name"]
    )
    if authority.relations is not None:
        relation = authority.relations[0]
        if (
            relation != (handoff["schema_name"], "nucc_taxonomy")
            and re.fullmatch(r"reference_family_archive_[0-9a-f]{32}", relation[0]) is None
        ):
            raise ReferenceFamilyArchiveError("NUCC native predecessor location differs")
    if is_immutable:
        _require_nucc_precreated_handoff_binding(handoff)
    if (
        "database_oid" not in handoff
        and nucc_native_digest({key: field_value for key, field_value in handoff.items() if key != "handoff_sha256"})
        != handoff["handoff_sha256"]
    ):
        raise ReferenceFamilyArchiveError("NUCC native handoff digest differs")
    return handoff


def _validate_nucc_native_relation(stage, suffix):
    """Validate the recorded isolated name and bounded semantic index definitions."""
    fields = {"table_name", "indexes"}
    if (
        type(stage) is not dict
        or set(stage) not in (fields, fields | {"relation_oid", "relfilenode", "owner_oid"})
        or stage["table_name"] != "nucc_taxonomy_" + suffix
    ):
        raise ReferenceFamilyArchiveError("NUCC native relation differs")
    _validate_nucc_native_indexes(stage)


def _validate_nucc_native_indexes(stage):
    indexes = stage["indexes"]
    if type(indexes) is not list or not 1 <= len(indexes) <= 16:
        raise ReferenceFamilyArchiveError("NUCC native indexes differ")
    definitions = []
    names = []
    for index in indexes:
        if (
            type(index) is not dict
            or set(index) not in ({"index_name", "definition"}, {"index_oid", "relfilenode", "definition"})
            or type(index["definition"]) is not str
            or not 0 < len(index["definition"]) <= 8192
        ):
            raise ReferenceFamilyArchiveError("NUCC native indexes differ")
        definitions.append(index["definition"])
        if "index_name" in index:
            names.append(_schema_name(index["index_name"]))
    if len(definitions) != len(set(definitions)) or len(names) != len(set(names)):
        raise ReferenceFamilyArchiveError("NUCC native indexes repeat")
    if names and names != sorted(names):
        raise ReferenceFamilyArchiveError("NUCC native indexes are unordered")


@asynccontextmanager
async def _nucc_catalog_search_path(session):
    """Keep native index metadata independent of caller schemas without importing another producer."""
    previous = await session.scalar(text("SHOW search_path"))
    await session.execute(text("SET LOCAL search_path=pg_catalog,pg_temp"))
    try:
        yield
    except BaseException as failure:
        try:
            await session.execute(text("SELECT set_config('search_path',:path,true)"), {"path": previous})
        except Exception:
            raise failure from None
        raise
    else:
        await session.execute(text("SELECT set_config('search_path',:path,true)"), {"path": previous})


async def _nucc_native_stage(session, schema_name, table_name):
    """Observe one fixed-model stage independently of its request or comment."""
    async with _nucc_catalog_search_path(session):
        return await _observe_nucc_native_stage(session, schema_name, table_name)


async def _observe_nucc_native_stage(session, schema_name, table_name, *, native_revision=False):
    """Observe native layout and index definitions by the declared relation name."""
    schema, table = _schema_name(schema_name), _schema_name(table_name)
    hook_name = None
    has_native_hook = native_revision and await session.scalar(
        text("SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=to_regclass(:relation) AND NOT tgisinternal)"),
        {"relation": f"{_quoted(schema)}.{_quoted(table)}"},
    )
    if has_native_hook:
        from process import provider_directory_cms_native_registration as registration

        driver = await _native_model_copy_driver(
            session, ("fetch", "fetchrow", "fetchval", "execute", "is_in_transaction")
        )
        evidence = await registration.capture_native_input_adoption(
            driver, ledger_schema=schema, candidate=registration.NativeInputCandidate(schema, table, schema, table)
        )
        if evidence is not None:
            hook_name = "cms_native_input_mutation"
    oid = await session.scalar(
        text(
            "SELECT oid::bigint FROM pg_catalog.pg_class candidate WHERE oid=to_regclass(:relation) "
            "AND relkind='r' AND relpersistence='p' AND NOT relispartition AND NOT relrowsecurity AND NOT relforcerowsecurity "
            "AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=candidate.oid OR inhparent=candidate.oid) "
            "AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=candidate.oid AND NOT tgisinternal AND tgname IS DISTINCT FROM :hook) "
            "AND NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=candidate.oid AND contype NOT IN ('p','n')) "
            "AND NOT EXISTS(SELECT 1 FROM pg_index WHERE indrelid=candidate.oid AND (NOT indisvalid OR NOT indisready OR NOT indislive))"
        ),
        {"relation": f"{_quoted(schema)}.{_quoted(table)}", "hook": hook_name},
    )
    if oid is None:
        raise ReferenceFamilyArchiveError("NUCC native candidate is missing")
    await _require_nucc_native_columns(session, oid)
    await require_native_read_catalog(session, (oid,), allowed_type_oids=())
    indexes = (
        (
            await session.execute(
                text(
                    "SELECT relation.relname AS index_name,pg_catalog.pg_get_indexdef(indexrelid) AS definition "
                    "FROM pg_catalog.pg_index index_entry JOIN pg_catalog.pg_class relation ON relation.oid=indexrelid "
                    "WHERE indrelid=:oid ORDER BY relation.relname"
                ),
                {"oid": oid},
            )
        )
        .mappings()
        .all()
    )
    return {"table_name": table, "indexes": [dict(index) for index in indexes]}


async def _nucc_locked_attempt(session, handoff, statuses):
    """Hold the actual local run, attempt, database and original source parameters."""
    _require_transaction(session)
    schema = _schema_name(handoff["schema_name"])
    run = (
        (
            await session.execute(
                text(
                    f'SELECT node_id,engine,importer,status,progress,metrics,params,finished_at,error,phase_detail FROM "{schema}".import_run '
                    "WHERE run_id=:run_id AND octet_length(params::text)<=131072 AND octet_length(metrics::text)<=393216 FOR UPDATE NOWAIT"
                ),
                {"run_id": handoff["run_id"]},
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        run is None
        or run["engine"] != "healthcare-mrf-api"
        or run["importer"] != "nucc"
        or run["status"] not in statuses
        or run["error"] is not None
        or type(run["progress"]) is not dict
        or type(run["params"]) is not dict
        or (run["metrics"] is not None and type(run["metrics"]) is not dict)
        or run["progress"].get("attempt_id") != handoff["attempt_id"]
        or run["progress"].get("attempt_started_at") != handoff["attempt_started_at"]
        or run["params"].get("test")
        or run["params"].get("test_mode")
    ):
        raise ReferenceFamilyArchiveError("NUCC native attempt differs")
    return run


def _nucc_source_contract(handoff, params):
    """Bind source parameters to the actual worker run and attempt."""
    return nucc_native_digest(
        {key: handoff[key] for key in ("run_id", "attempt_id", "attempt_started_at", "schema_name")}
        | {"params": params}
    )


async def record_nucc_native_handoff(session, ctx, *, schema_name, stage_model, ready):
    """Persist the named complete candidate and exact logical attempt/source preimage."""
    run_id, attempt, started, suffix = _nucc_attempt(ctx)
    handoff_by_field = dict(
        contract=NUCC_HANDOFF_CONTRACT,
        run_id=run_id,
        attempt_id=attempt,
        attempt_started_at=started,
        schema_name=_schema_name(schema_name),
        import_date=suffix,
    )
    if ctx["import_date"] != suffix or stage_model.__tablename__ != "nucc_taxonomy_" + suffix:
        raise ReferenceFamilyArchiveError("NUCC native stage attempt differs")
    run = await _nucc_locked_attempt(session, handoff_by_field, ("running",))
    actual = await read_reference_family_result_generation_authority(
        session, importer_id="nucc", schema_name=schema_name, lock=False
    )
    if actual != _nucc_result_authority(
        ctx["context"].get("nucc_native_predecessor"), allow_untracked=True, declared_schema=schema_name
    ):
        raise ReferenceFamilyArchiveError("NUCC native predecessor changed")
    handoff_by_field.update(
        node_id=run["node_id"],
        incumbent=actual.as_dict(),
        row_count=ready["row_count"],
        stage=await _nucc_native_stage(session, schema_name, stage_model.__tablename__),
        source_contract_sha256=_nucc_source_contract(handoff_by_field, run["params"]),
    )
    original = ctx["context"].get("nucc_native_stage")
    if original is not None and original.get("contract") == "nucc-native-stage.v2":
        if (run["metrics"] or {}).get("nucc_native_stage") != original:
            raise ReferenceFamilyArchiveError("NUCC native persisted attempt differs")
        handoff_by_field.update(contract=NUCC_IMMUTABLE_HANDOFF_CONTRACT, precreated_stage=original)
    if tuple(ready["relation"]) != (schema_name, stage_model.__tablename__):
        raise ReferenceFamilyArchiveError("NUCC native indexed stage changed")
    handoff_by_field["handoff_sha256"] = nucc_native_digest(handoff_by_field)
    handoff_by_field = validate_nucc_native_handoff(handoff_by_field)
    await _nucc_record_handoff_run(session, handoff_by_field)
    return handoff_by_field


async def _nucc_record_handoff_run(session, handoff):
    """CAS only the exact still-running attempt while retaining all unrelated metrics."""
    changed = await session.scalar(
        text(
            f"UPDATE {_quoted(handoff['schema_name'])}.import_run SET status='finalizing',phase_detail=:phase,"
            "metrics=(COALESCE(metrics::jsonb,'{}'::jsonb)||jsonb_build_object('nucc_handoff',CAST(:handoff AS jsonb)))::json,"
            "heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE run_id=:run_id AND node_id=:node_id "
            "AND engine='healthcare-mrf-api' AND importer='nucc' AND status='running' "
            "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
            "AND metrics->'nucc_handoff' IS NULL AND finished_at IS NULL RETURNING run_id"
        ),
        {**handoff, "phase": NUCC_HANDOFF_PHASE, "handoff": _canonical_json(handoff).decode("ascii")},
    )
    if changed != handoff["run_id"]:
        raise ReferenceFamilyArchiveError("NUCC native handoff attempt changed")


async def _require_nucc_native_location(session, handoff, run):
    """Bind the logical worker run and exact original source parameters."""
    _require_transaction(session)
    if (
        run["node_id"] != handoff["node_id"]
        or _nucc_source_contract(handoff, run["params"]) != handoff["source_contract_sha256"]
    ):
        raise ReferenceFamilyArchiveError("NUCC native source or location differs")


async def protected_publisher_owner(session):
    """Authenticate the existing native protected owner without selecting an importer."""
    from process.entity_address_snapshot_preparation import EntityAddressSnapshotDestinationError, _publisher_authority

    _require_transaction(session)
    try:
        return await _publisher_authority(session)
    except EntityAddressSnapshotDestinationError as error:
        raise ReferenceFamilyArchiveError("native protected publisher is unavailable") from error


async def require_nucc_native_handoff(session, value):
    """Recheck persisted attempt, source contract, marker and exact physical stage."""
    handoff = validate_nucc_native_handoff(value)
    run = await _nucc_locked_attempt(session, handoff, ("finalizing",))
    await _require_nucc_native_location(session, handoff, run)
    if (
        run["finished_at"] is not None
        or run["phase_detail"] != NUCC_HANDOFF_PHASE
        or (run["metrics"] or {}).get("nucc_handoff") != handoff
    ):
        raise ReferenceFamilyArchiveError("NUCC native handoff source or location differs")
    await _require_nucc_handoff_stage(session, handoff)
    return handoff


async def _require_nucc_handoff_stage(session, handoff):
    """Lock the named complete candidate and compare semantic index definitions."""
    table = handoff["stage"]["table_name"]
    await session.execute(
        text(f"LOCK TABLE ONLY {_quoted(handoff['schema_name'])}.{_quoted(table)} IN ACCESS SHARE MODE NOWAIT")
    )
    observed = await _nucc_native_stage(session, handoff["schema_name"], table)
    expected = handoff["stage"]
    if observed["table_name"] != expected["table_name"] or sorted(
        index["definition"] for index in observed["indexes"]
    ) != sorted(index["definition"] for index in expected["indexes"]):
        raise ReferenceFamilyArchiveError("NUCC native stage layout differs")


def _nucc_abandonment(handoff, abandonment):
    """Decode only exact persisted abandoned custody, never caller-selected cleanup names."""
    if (
        type(abandonment) is not dict
        or set(abandonment)
        != {
            "contract",
            "preparation_id",
            "admission_sha256",
            "generation_id",
            "publication_fence",
            "reason",
            "stage_disposition",
            "physical_cleanup_completed",
            "same_run_readmission",
            "observed_run",
            "candidate_custody",
            "released_pins",
        }
        or abandonment["contract"] != NUCC_ABANDONMENT_CONTRACT
        or abandonment["reason"] not in {"terminal-cancellation", "completed-handoff-cancellation", "replaced-attempt"}
        or abandonment["stage_disposition"] != "retained-unpublished"
        or abandonment["physical_cleanup_completed"] is not False
        or abandonment["same_run_readmission"] != "requires-exact-candidate-cleanup-and-ledger-retirement"
        or abandonment["candidate_custody"]
        != {
            key: handoff[key]
            for key in (
                "schema_name",
                "attempt_id",
                "attempt_started_at",
                "handoff_sha256",
                "source_contract_sha256",
                "stage",
                "database_oid",
                "import_run_oid",
            )
            if key in handoff
        }
    ):
        raise ReferenceFamilyArchiveError("NUCC abandonment custody differs")
    for key in ("preparation_id", "generation_id", "publication_fence"):
        if str(UUID(abandonment[key])) != abandonment[key]:
            raise ReferenceFamilyArchiveError("NUCC abandonment identity differs")
    if (
        type(abandonment["admission_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", abandonment["admission_sha256"]) is None
    ):
        raise ReferenceFamilyArchiveError("NUCC abandonment digest differs")
    if len(_canonical_json(abandonment)) > 131072:
        raise ReferenceFamilyArchiveError("NUCC abandonment metadata exceeds its bound")
    return abandonment


async def _require_nucc_cleanup_run(session, handoff, abandonment):
    """Lock the genuine actual run/history and its retained abandonment preimage."""
    locked = await session.execute(
        text(
            "SELECT * FROM mrf.import_run WHERE run_id=:run_id AND node_id=:node_id "
            "AND engine='healthcare-mrf-api' AND importer='nucc' AND octet_length(metrics::text)<=393216 "
            "AND octet_length(params::text)<=131072 FOR UPDATE NOWAIT"
        ),
        handoff,
    )
    run = locked.mappings().one_or_none()
    if run is None or type(run["params"]) is not dict or run["params"].get("test") or run["params"].get("test_mode"):
        raise ReferenceFamilyArchiveError("NUCC cleanup run differs")
    await _require_nucc_native_location(session, handoff, run)
    metrics = run["metrics"] or {}
    current = validate_nucc_native_handoff(metrics.get("nucc_handoff"))
    observed_by_field = {
        "run_id": handoff["run_id"],
        "node_id": handoff["node_id"],
        "status": run["status"],
        "progress": {key: (run["progress"] or {}).get(key) for key in ("attempt_id", "attempt_started_at")},
        "handoff_sha256": current["handoff_sha256"],
        "phase_detail": run["phase_detail"],
        "finished_at": str(run["finished_at"]) if run["finished_at"] else None,
    }
    if abandonment["reason"] in {"replaced-attempt", "completed-handoff-cancellation"}:
        observed_by_field.update(
            status="finalizing" if abandonment["reason"] == "replaced-attempt" else "canceling",
            phase_detail=NUCC_HANDOFF_PHASE if abandonment["reason"] == "replaced-attempt" else "cancel requested",
            finished_at=None,
        )
    if (
        metrics.get("nucc_native_publication") is not None
        or (metrics.get("nucc_native_abandonments") or {}).get(abandonment["preparation_id"]) != abandonment
        or (metrics.get("nucc_native_candidate_cleanups") or {}).get(abandonment["preparation_id"]) is not None
        or abandonment["observed_run"] != observed_by_field
        or any(current[key] != handoff[key] for key in ("run_id", "node_id", "schema_name"))
    ):
        raise ReferenceFamilyArchiveError("NUCC cleanup receipt differs")
    _require_nucc_cleanup_terminal(run, handoff, current, abandonment["reason"])


def _require_nucc_cleanup_terminal(run, handoff, current, reason):
    """Only a protected completed cancellation or genuine replaced attempt closes custody."""
    progress = run["progress"] or {}
    if (progress.get("attempt_id"), progress.get("attempt_started_at")) != (
        current["attempt_id"],
        current["attempt_started_at"],
    ):
        raise ReferenceFamilyArchiveError("NUCC cleanup attempt differs")
    is_canceled = run["status"] in {"canceled", "cancelled"} and run["finished_at"] is not None
    is_unfinished = run["finished_at"] is None and run["error"] is None
    is_fenced = (
        run["status"] == "canceling"
        and run["phase_detail"] == progress.get("message") == "cancel requested"
        and is_unfinished
    )
    if reason == "replaced-attempt":
        is_finalizing = run["status"] == "finalizing" and run["phase_detail"] == NUCC_HANDOFF_PHASE and is_unfinished
        if (
            current["attempt_id"] == handoff["attempt_id"]
            or current["attempt_started_at"] == handoff["attempt_started_at"]
            or not (is_finalizing or is_fenced or is_canceled)
            or _nucc_source_contract(current, run["params"]) != current["source_contract_sha256"]
        ):
            raise ReferenceFamilyArchiveError("NUCC replacement is not authenticated")
        return
    if current != handoff:
        raise ReferenceFamilyArchiveError("NUCC cleanup handoff differs")
    is_canceled = is_canceled and (reason == "terminal-cancellation" or progress.get("message") == "canceled")
    if reason not in {"terminal-cancellation", "completed-handoff-cancellation"} or not (
        is_canceled or reason == "completed-handoff-cancellation" and is_fenced
    ):
        raise ReferenceFamilyArchiveError("NUCC cancellation is not authenticated")


async def _require_nucc_cleanup_custody(session, handoff, protected_owner_oid=None):
    """Keep an isolated candidate out of every named serving projection before DROP."""
    schema, table = handoff["schema_name"], handoff["stage"]["table_name"]
    await read_reference_family_result_generation_authority(session, importer_id="nucc", schema_name=schema, lock=True)
    await session.execute(
        text(
            f"LOCK TABLE ONLY {_quoted(schema)}.nucc_taxonomy,{_quoted(schema)}.reference_family_result_generation IN ACCESS SHARE MODE NOWAIT"
        )
    )
    safe = await session.scalar(
        text(
            "SELECT NOT EXISTS(SELECT 1 FROM " + _quoted(schema) + ".reference_family_result_generation "
            "WHERE serving_schema=:schema AND :name=ANY(serving_relation_names)) "
            "AND :name<>'nucc_taxonomy' AND NOT EXISTS(SELECT 1 FROM pg_depend d JOIN pg_class c ON c.oid=d.objid "
            "WHERE d.refobjid=to_regclass(:relation) AND d.classid='pg_class'::regclass AND c.relkind='S')"
        ),
        {"schema": schema, "name": table, "relation": f"{_quoted(schema)}.{_quoted(table)}"},
    )
    if safe is not True:
        raise ReferenceFamilyArchiveError("NUCC candidate is serving or owns a sequence")
    await _nucc_native_stage(session, schema, table)


async def cleanup_nucc_native_handoff(session, handoff, *, abandonment, runtime_owner_oids=None, assert_unreferenced):
    """Restrictively remove only the unreferenced named abandoned candidate."""
    _require_transaction(session)
    handoff = validate_nucc_native_handoff(handoff)
    abandonment = _nucc_abandonment(handoff, abandonment)
    if not callable(assert_unreferenced):
        raise ReferenceFamilyArchiveError("NUCC trusted cleanup authority is required")
    async with session.begin_nested():
        await session.connection()
        transaction = session.get_transaction()
        async with _nucc_catalog_search_path(session):
            await _require_nucc_cleanup_run(session, handoff, abandonment)
            if await assert_unreferenced(session, handoff, abandonment) is not True:
                raise ReferenceFamilyArchiveError("NUCC candidate remains referenced")
            await _require_nucc_handoff_stage(session, handoff)
            await _require_nucc_cleanup_custody(session, handoff)
            if (
                await assert_unreferenced(session, handoff, abandonment) is not True
                or not session.in_transaction()
                or session.get_transaction() is not transaction
            ):
                raise ReferenceFamilyArchiveError("NUCC cleanup transaction or references changed")
            await session.execute(
                text(f"DROP TABLE {_quoted(handoff['schema_name'])}.{_quoted(handoff['stage']['table_name'])} RESTRICT")
            )
        return dict(
            contract=NUCC_CLEANUP_CONTRACT,
            preparation_id=abandonment["preparation_id"],
            abandonment_sha256=nucc_native_digest(
                {
                    **{
                        key: abandonment[key]
                        for key in ("contract", "preparation_id", "generation_id", "publication_fence", "reason")
                    },
                    **{
                        key: handoff[key]
                        for key in ("run_id", "node_id", "attempt_id", "attempt_started_at", "row_count")
                    },
                    "relation": [handoff["schema_name"], handoff["stage"]["table_name"]],
                }
            ),
            handoff_sha256=handoff["handoff_sha256"],
            stage=handoff["stage"],
            stage_disposition="removed-unpublished",
            physical_cleanup_completed=True,
        )


async def _require_nucc_sealed_handoff(session, handoff, legacy_owner=None):
    """Recheck the persisted attempt and named candidate after native write closure."""
    run = await _nucc_locked_attempt(session, handoff, ("finalizing",))
    await _require_nucc_native_location(session, handoff, run)
    if (
        (run["metrics"] or {}).get("nucc_handoff") != handoff
        or run["finished_at"] is not None
        or run["phase_detail"] != NUCC_HANDOFF_PHASE
    ):
        raise ReferenceFamilyArchiveError("NUCC native sealed attempt differs")
    await _require_nucc_handoff_stage(session, handoff)


async def complete_nucc_native_handoff(session, handoff_by_field, *, publication_continuation):
    """Close writes and validate before the once-only atomic named publication."""
    if not callable(publication_continuation):
        raise ReferenceFamilyArchiveError("NUCC trusted publication continuation is required")
    _require_transaction(session)
    async with session.begin_nested():
        await session.connection()
        handoff = await require_nucc_native_handoff(session, handoff_by_field)
        schema, table = handoff["schema_name"], handoff["stage"]["table_name"]
        transaction = session.get_transaction()
        from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot

        owner, readers = model_family_roles()
        driver = await _native_model_copy_driver(session, ("execute", "fetch", "fetchval"))
        await freeze_fhir_source_snapshot(
            driver,
            schema,
            namespace_owner=owner,
            reader_roles=readers,
            application_roles=(),
            relation_names=(table,),
            freeze_schema=False,
        )
        validation = await _validate_nucc_table_set(session, schema, table)
        if validation["row_count"] != handoff["row_count"]:
            raise ReferenceFamilyArchiveError("NUCC native row accounting changed")
        prepared_dict = dict(handoff=handoff, validation=validation, validation_sha256=nucc_native_digest(validation))
        cutover_by_field = {"has_run": False}

        async def cutover():
            """Run the once-only named rename and generation CAS in the owning transaction."""
            if cutover_by_field["has_run"]:
                raise ReferenceFamilyArchiveError("NUCC native cutover already used")
            cutover_by_field["has_run"] = True
            if not session.in_transaction() or session.get_transaction() is not transaction:
                raise ReferenceFamilyArchiveError("NUCC native publisher transaction changed")
            await _require_nucc_sealed_handoff(session, handoff)
            if await _relation_oid(session, schema, "nucc_taxonomy") is not None:
                raise ReferenceFamilyArchiveError("NUCC retained predecessor was not relocated")
            await session.execute(text(f"ALTER TABLE {_quoted(schema)}.{_quoted(table)} RENAME TO nucc_taxonomy"))
            await rename_nucc_published_indexes(session, schema_name=schema)
            published = await _publish_nucc_handoff_generation(session, handoff)
            if published.relations != ((schema, "nucc_taxonomy"),):
                raise ReferenceFamilyArchiveError("NUCC native published relation differs")
            receipt_by_field = dict(
                contract=NUCC_IMMUTABLE_PUBLICATION_CONTRACT
                if handoff["contract"] == NUCC_IMMUTABLE_HANDOFF_CONTRACT
                else NUCC_PUBLICATION_CONTRACT,
                **prepared_dict,
                result_generation=published.as_dict(),
            )
            validate_nucc_native_publication(receipt_by_field)
            await _finish_nucc_native_attempt(session, receipt_by_field)
            return receipt_by_field

        publication_result = await publication_continuation(session, prepared_dict, cutover)
        if not cutover_by_field["has_run"]:
            raise ReferenceFamilyArchiveError("NUCC native cutover was not completed")
        return publication_result


def validate_nucc_native_publication(receipt_by_field):
    """Preserve historical bytes while validating semantic data and generation progress."""
    fields = {"contract", "handoff", "validation", "validation_sha256", "result_generation"}
    if (
        type(receipt_by_field) is not dict
        or set(receipt_by_field) not in (fields, fields | {"sealed_owner_oid"})
        or receipt_by_field["contract"] not in {NUCC_PUBLICATION_CONTRACT, NUCC_IMMUTABLE_PUBLICATION_CONTRACT}
    ):
        raise ReferenceFamilyArchiveError("NUCC native publication differs")
    receipt = json.loads(_canonical_json(receipt_by_field))
    handoff = validate_nucc_native_handoff(receipt["handoff"])
    if ("sealed_owner_oid" in receipt) != ("database_oid" in handoff):
        raise ReferenceFamilyArchiveError("NUCC native publication edition differs")
    is_immutable = receipt["contract"] == NUCC_IMMUTABLE_PUBLICATION_CONTRACT
    if is_immutable != (handoff["contract"] == NUCC_IMMUTABLE_HANDOFF_CONTRACT):
        raise ReferenceFamilyArchiveError("NUCC native publication contract differs")
    validation = receipt["validation"]
    if (
        type(validation) is not dict
        or set(validation) != {"contract", "row_count", "csv_upper_bound"}
        or validation["contract"] != "nucc-indexed-set.v1"
        or type(validation["row_count"]) is not int
        or validation["row_count"] != handoff["row_count"]
        or type(validation["csv_upper_bound"]) is not int
        or not 0 < validation["csv_upper_bound"] <= 8 * 1024**2
        or nucc_native_digest(validation) != receipt["validation_sha256"]
    ):
        raise ReferenceFamilyArchiveError("NUCC native publication validation differs")
    authority = _nucc_result_authority(receipt["result_generation"], declared_schema=handoff["schema_name"])
    incumbent = _nucc_result_authority(
        handoff["incumbent"], allow_untracked=is_immutable, declared_schema=handoff["schema_name"]
    )
    if (
        authority.local_lineage_id != incumbent.local_lineage_id
        or authority.local_generation != incumbent.local_generation + 1
        or authority.serving_generation.origin_lineage_id != authority.local_lineage_id
        or authority.serving_generation.origin_generation != authority.local_generation
        or authority.relations != ((handoff["schema_name"], "nucc_taxonomy"),)
    ):
        raise ReferenceFamilyArchiveError("NUCC native publication generation differs")
    return receipt


async def _finish_nucc_native_attempt(session, receipt):
    """Keep handoff/attempt/unrelated metrics while CAS-completing this real run."""
    handoff = receipt["handoff"]
    changed = await session.scalar(
        text(
            f"UPDATE {_quoted(handoff['schema_name'])}.import_run SET status='succeeded',phase_detail='nucc published',"
            "finished_at=clock_timestamp() AT TIME ZONE 'UTC',heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC',error=NULL,"
            "metrics=(metrics::jsonb||jsonb_build_object('nucc_native_publication',CAST(:receipt AS jsonb)))::json,"
            "progress=(progress::jsonb||jsonb_build_object('phase','nucc published','pct',100,'unit','rows',"
            "'done',CAST(:rows AS bigint),'total',CAST(:rows AS bigint),'message','succeeded'))::json "
            "WHERE run_id=:run_id AND node_id=:node_id AND importer='nucc' AND status='finalizing' "
            "AND phase_detail=:phase AND finished_at IS NULL AND error IS NULL "
            "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
            "AND metrics::jsonb->'nucc_handoff'=CAST(:handoff AS jsonb) RETURNING run_id"
        ),
        {
            **handoff,
            "phase": NUCC_HANDOFF_PHASE,
            "rows": handoff["row_count"],
            "handoff": _canonical_json(handoff).decode("ascii"),
            "receipt": _canonical_json(receipt).decode("ascii"),
        },
    )
    if changed != handoff["run_id"]:
        raise ReferenceFamilyArchiveError("NUCC native success fence changed")


async def read_nucc_native_publication(session, value):
    """Read exact historical committed success without assuming it is still current."""
    handoff = validate_nucc_native_handoff(value)
    run = await _nucc_locked_attempt(
        session, handoff, ("finalizing", "succeeded", "canceling", "canceled", "cancelled")
    )
    await _require_nucc_native_location(session, handoff, run)
    receipt = (run["metrics"] or {}).get("nucc_native_publication")
    if receipt is None:
        return None
    receipt = validate_nucc_native_publication(receipt)
    if receipt["handoff"] != handoff or run["status"] != "succeeded" or run["finished_at"] is None:
        raise ReferenceFamilyArchiveError("NUCC native committed receipt differs")
    return receipt


async def read_nucc_native_handoff(session, value):
    """Resolve a possible worker COMMIT using authentic persisted attempt evidence."""
    handoff = validate_nucc_native_handoff(value)
    run = await _nucc_locked_attempt(
        session, handoff, ("running", "finalizing", "succeeded", "canceling", "canceled", "cancelled")
    )
    await _require_nucc_native_location(session, handoff, run)
    persisted = (run["metrics"] or {}).get("nucc_handoff")
    if persisted is None:
        return None
    if validate_nucc_native_handoff(persisted) != handoff or run["status"] == "running":
        raise ReferenceFamilyArchiveError("NUCC native committed handoff differs")
    return handoff


async def reconcile_nucc_native_handoff(database, handoff):
    """Finish a shielded readback before classifying transport failure or cancellation."""

    async def read():
        """Read the exact possible commit in its own real transaction."""
        async with database.transaction() as session:
            return await read_nucc_native_handoff(session, handoff)

    pending = asyncio.create_task(read())
    while True:
        try:
            return await asyncio.shield(pending)
        except asyncio.CancelledError:
            if pending.cancelled():
                raise


async def cleanup_reference_family_stage(
    session: Any,
    ownership: ReferenceFamilyStageOwnership,
) -> None:
    """Drop only an unchanged UUID-owned stage using restrictive DDL."""

    await cleanup_model_family_stage(session, _ownership_spec(ownership), ownership)


async def cleanup_model_family_stage(
    session: Any,
    spec: ReferenceFamilySpec,
    ownership: ReferenceFamilyStageOwnership,
    *,
    include_identity: bool = False,
) -> None:
    """Restrictively retire only the unchanged, complete UUID-owned model stage."""

    _require_transaction(session)
    _require_model_family_spec(spec)
    if not isinstance(ownership, ReferenceFamilyStageOwnership):
        raise ReferenceFamilyArchiveError("reference family stage ownership is invalid")
    if (
        ownership.importer_id != spec.importer_id
        or ownership.schema_name != reference_family_stage_schema(ownership.dataset_id)
        or tuple(name for name, _oid in ownership.relation_oids) != tuple(sorted(spec.table_names))
        or tuple((name, table_name, column_name) for name, _oid, table_name, column_name in ownership.sequence_oids)
        != _expected_model_family_sequences(spec, include_identity=include_identity)
    ):
        raise ReferenceFamilyArchiveError("reference family stage ownership differs")
    await _cleanup_model_family_stage(session, ownership, spec, include_identity=include_identity)


async def _cleanup_model_family_stage(session, ownership, spec, *, include_identity=False):
    """Retire the exact model inventory without admitting peer-selected relations."""

    _require_transaction(session)
    current_schema_oid = await session.scalar(
        text("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=:schema_name"),
        {"schema_name": ownership.schema_name},
    )
    if current_schema_oid is None:
        return
    ownership = await verify_model_family_stage_ownership(session, spec, ownership, include_identity=include_identity)
    relations = ", ".join(f"{_quoted(ownership.schema_name)}.{_quoted(name)}" for name in spec.archive_names)
    # A sealed stage keeps SELECT only, even for its owner; DROP takes its own lock through ownership.
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    await session.execute(text(f"DROP TABLE {relations} RESTRICT"))
    if int(
        await session.scalar(
            text("SELECT count(*) FROM pg_catalog.pg_class WHERE relnamespace=to_regnamespace(:schema_name)"),
            {"schema_name": ownership.schema_name},
        )
        or 0
    ):
        raise ReferenceFamilyArchiveError("reference family owned schema is not empty")
    await session.execute(text(f"DROP SCHEMA {_quoted(ownership.schema_name)}"))


def _nucc_retained_inventory(inventory):
    """Decode one recorded retained QName; old physical fields remain opaque."""
    fields = {"schema_name", "relation_name"}
    legacy_fields = {"relation_oid", "relfilenode", "owner_oid", "schema_oid", "schema_owner_oid"}
    if type(inventory) is not dict or set(inventory) not in ({"relations"}, {"database_oid", "relations"}):
        raise ReferenceFamilyArchiveError("NUCC retained inventory differs")
    relations = inventory["relations"]
    if (
        type(relations) is not list
        or len(relations) != 1
        or type(relations[0]) is not dict
        or set(relations[0]) not in (fields, fields | legacy_fields)
    ):
        raise ReferenceFamilyArchiveError("NUCC retained family differs")
    relation = relations[0]
    if (
        relation["relation_name"] != models.NUCCTaxonomy.__tablename__
        or type(relation["schema_name"]) is not str
        or re.fullmatch(_PREDECESSOR_PREFIX + r"[0-9a-f]{32}", relation["schema_name"]) is None
    ):
        raise ReferenceFamilyArchiveError("NUCC retained location differs")
    return json.loads(_canonical_json(inventory))


async def _require_nucc_retained_physical(session, inventory, owner_oid=None):
    """Validate the named native retained model and its complete isolated namespace."""
    relation = inventory["relations"][0]
    schema, table = relation["schema_name"], relation["relation_name"]
    safe = await session.scalar(
        text(
            "SELECT relkind='r' AND relpersistence='p' AND NOT relispartition AND NOT relrowsecurity AND NOT relforcerowsecurity "
            "AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) "
            "AND NOT EXISTS(SELECT FROM mrf.reference_family_result_generation WHERE serving_schema=:schema AND :name=ANY(serving_relation_names)) "
            "AND (SELECT count(*) FROM pg_index WHERE indrelid=c.oid) BETWEEN 1 AND 16 "
            "AND NOT EXISTS(SELECT FROM pg_index i WHERE i.indrelid=c.oid AND (NOT i.indisvalid OR NOT i.indisready OR NOT i.indislive)) "
            "AND NOT EXISTS(SELECT FROM pg_class other WHERE other.relnamespace=c.relnamespace AND other.oid<>c.oid "
            "AND NOT EXISTS(SELECT FROM pg_index i WHERE i.indrelid=c.oid AND i.indexrelid=other.oid)) "
            "FROM pg_class c WHERE c.oid=to_regclass(:relation)"
        ),
        {"schema": schema, "name": table, "relation": f"{_quoted(schema)}.{_quoted(table)}"},
    )
    if safe is not True:
        raise ReferenceFamilyArchiveError("NUCC retained layout or serving authority differs")
    await _nucc_native_stage(session, schema, table)


async def cleanup_nucc_retained_publication(session, *, inventory, assert_unreferenced):
    """Drop a named predecessor; the trusted callback holds its reference-claim locks through COMMIT."""
    _require_transaction(session)
    inventory = _nucc_retained_inventory(inventory)
    if not callable(assert_unreferenced):
        raise ReferenceFamilyArchiveError("NUCC trusted retirement authority is required")
    async with session.begin_nested():
        await session.connection()
        transaction = session.get_transaction()
        relation = inventory["relations"][0]
        qualified = f"{_quoted(relation['schema_name'])}.{_quoted(relation['relation_name'])}"
        async with _nucc_catalog_search_path(session):
            await read_reference_family_result_generation_authority(
                session, importer_id="nucc", schema_name="mrf", lock=True
            )
            if await assert_unreferenced(session, inventory) is not True:
                raise ReferenceFamilyArchiveError("NUCC retained publication remains referenced")
            await session.execute(text(f"LOCK TABLE ONLY {qualified} IN ACCESS SHARE MODE NOWAIT"))
            await session.execute(
                text(
                    "LOCK TABLE ONLY mrf.nucc_taxonomy,mrf.reference_family_result_generation IN ACCESS SHARE MODE NOWAIT"
                )
            )
            await _require_nucc_retained_physical(session, inventory)
            if (
                await assert_unreferenced(session, inventory) is not True
                or not session.in_transaction()
                or session.get_transaction() is not transaction
            ):
                raise ReferenceFamilyArchiveError("NUCC retirement transaction or references changed")
            await session.execute(text(f"DROP TABLE {qualified} RESTRICT"))
            await session.execute(text(f"DROP SCHEMA {_quoted(relation['schema_name'])} RESTRICT"))
        named_inventory_by_field = {
            "relations": [{"schema_name": relation["schema_name"], "relation_name": relation["relation_name"]}]
        }
        return {
            "inventory_sha256": nucc_native_digest(named_inventory_by_field),
            "removed_schema": relation["schema_name"],
        }


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
    **source_options: Any,
) -> ReferenceFamilyManifest:
    """Clone, validate, dump, and exactly clean one closed family stage."""

    if set(source_options) != {"source_copy", "on_precreated", "verify_custody"} or not callable(
        source_options["verify_custody"]
    ):
        raise ReferenceFamilyArchiveError("reference family protected source custody capability is unavailable")
    _require_native_source_copy(source_options["source_copy"], source_options["on_precreated"])
    ownership = None
    try:

        async def retain_prepared_source(_session, _prepared_source):
            """The convenience wrapper owns cleanup rather than durable retention."""

            await source_options["verify_custody"](_session, _prepared_source)

        prepared_source = await prepare_reference_family_archive_source(
            session_factory,
            importer_id=importer_id,
            schema_name=schema_name,
            source_metadata=source_metadata,
            dataset_id=dataset_id,
            on_prepared=retain_prepared_source,
            source_copy=source_options["source_copy"],
            on_precreated=source_options["on_precreated"],
        )
        ownership = prepared_source.ownership
        await export_prepared_reference_family_archive(
            session_factory,
            prepared=prepared_source,
            archive_copy=archive_copy,
            verify_custody=source_options["verify_custody"],
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


async def _capture_source_dependencies(
    source_session,
    importer_id,
    schema_name,
    source_metadata,
    source_metadata_factory,
    dependency_factory,
    canonical_source_fence,
):
    """Capture metadata and dependency packages under the original source-family fence."""
    effective_source_metadata = source_metadata
    package_by_dataset = {}
    if source_metadata_factory is not None or dependency_factory is not None:
        spec = (
            reference_family_spec(importer_id, canonical=True)
            if importer_id == "mrf"
            else reference_family_spec(importer_id)
        )
        async with _bounded_capture(source_session, lock_before_snapshot=importer_id == "facility-anchors"):
            await _lock_source_family(source_session, spec, _schema_name(schema_name), canonical_source_fence)
            if source_metadata_factory is not None:
                effective_source_metadata = await source_metadata_factory(source_session)
            if dependency_factory is not None:
                package_by_dataset = await dependency_factory(source_session)
    if effective_source_metadata is None:
        raise ReferenceFamilyArchiveError("reference family source metadata is required")
    return effective_source_metadata, package_by_dataset


async def prepare_reference_family_archive_source(
    session_factory: Any,
    *,
    importer_id: str,
    schema_name: str,
    source_metadata: Mapping[str, Any] | None,
    dataset_id: UUID,
    on_prepared: Callable[[Any, ReferenceFamilyPreparedSource], Awaitable[None]],
    dependency_factory: Callable[[Any], Awaitable[Mapping[str, str]]] | None = None,
    **source_options: Any,
) -> ReferenceFamilyPreparedSource:
    """Clone once and persist its exact owner before the clone transaction commits."""

    source_metadata_factory, source_copy, on_precreated, source_sessions = _source_capture_options(
        session_factory, importer_id, source_options
    )
    canonical_source_fence = source_options.get("canonical_source_fence")
    async with (
        asyncio.timeout(source_copy.timeout) as deadline,
        source_sessions() as source_session,
        source_session.begin(),
    ):
        await source_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        if importer_id == "mrf":
            await _lock_source_family(
                source_session,
                reference_family_spec(importer_id, canonical=True),
                _schema_name(schema_name),
                canonical_source_fence,
            )
        effective_source_metadata, package_by_dataset = await _capture_source_dependencies(
            source_session,
            importer_id,
            schema_name,
            source_metadata,
            source_metadata_factory,
            dependency_factory,
            canonical_source_fence,
        )
        capture = await _capture_reference_family_source(
            source_session,
            importer_id=importer_id,
            schema_name=schema_name,
            source_metadata=effective_source_metadata,
            dependencies=package_by_dataset,
            native_auxiliary=True,
            canonical_source_fence=canonical_source_fence,
        )
        return await _prepare_source_clone(
            session_factory,
            capture,
            dataset_id,
            on_prepared,
            source_copy,
            on_precreated,
            deadline.when(),
            authorize_creation=source_options.get("authorize_creation"),
        )


def _source_capture_options(session_factory, importer_id, source_options):
    """Fail closed at the shared creation root without changing historical readers."""
    allowed_keys = {
        "source_metadata_factory",
        "source_copy",
        "on_precreated",
        "source_sessions",
        "canonical_source_fence",
        "authorize_creation",
    }
    if set(source_options) - allowed_keys or (
        source_options.get("authorize_creation") is not None and not callable(source_options["authorize_creation"])
    ):
        raise ReferenceFamilyArchiveError("reference family source options are invalid")
    if importer_id != "mrf" and source_options.get("canonical_source_fence") is not None:
        raise ReferenceFamilyArchiveError("reference family canonical source fence scope differs")
    source_copy, on_precreated = source_options.get("source_copy"), source_options.get("on_precreated")
    _require_native_source_copy(source_copy, on_precreated)
    if importer_id == "nucc":
        raise ReferenceFamilyArchiveError("NUCC protected source requires dedicated preparation")
    source_sessions = source_options.get("source_sessions", session_factory)
    if not callable(source_sessions):
        raise ReferenceFamilyArchiveError("reference family source owning session is unavailable")
    return source_options.get("source_metadata_factory"), source_copy, on_precreated, source_sessions


async def _prepare_source_clone(
    session_factory, capture, dataset_id, on_prepared, source_copy, on_precreated, deadline, authorize_creation=None
):
    """Commit exact clone custody only with its creation transaction's successful recorder."""
    stage_schema = reference_family_stage_schema(dataset_id)
    async with session_factory() as clone_session, clone_session.begin():
        await _clone_source(
            clone_session,
            capture,
            stage_schema,
            source_copy=source_copy,
            deadline=deadline,
            on_precreated=on_precreated,
            authorize_creation=authorize_creation,
        )
        ownership = await _capture_model_family_ownership(
            clone_session,
            _manifest_family_spec(capture.manifest.as_dict()),
            dataset_id,
        )
        prepared = ReferenceFamilyPreparedSource(capture.manifest, ownership)
        await _validate_stage_manifest(clone_session, ownership=ownership, manifest=capture.manifest)
        if getattr(capture, "canonical_source_fence", None) is not None:
            await require_canonical_source_fence(clone_session, capture.canonical_source_fence, capture.schema_name)
        await on_prepared(clone_session, prepared)
        return prepared


async def prepare_nucc_reference_archive_source(
    session_factory: Any,
    *,
    dataset_id: UUID,
    on_prepared: Callable[[Any, ReferenceFamilyPreparedSource], Awaitable[None]],
    source_copy: ReferenceFamilySourceCopy,
    source_metadata_factory: Callable[[Any], Awaitable[Mapping[str, Any]]],
    on_precreated: Callable[..., Awaitable[None]] | None = None,
    source_capture_contract_factory: Callable[[Any], Awaitable[str | None]] | None = None,
) -> ReferenceFamilyPreparedSource:
    """Pin a genuine source proof, COPY the model, and persist exact native ownership."""
    if (
        not isinstance(source_copy, ReferenceFamilySourceCopy)
        or source_copy.max_bytes > 8 * 1024**2
        or source_copy.timeout > 120
        or not callable(source_metadata_factory)
        or not callable(on_prepared)
        or not callable(on_precreated)
        or (source_capture_contract_factory is not None and not callable(source_capture_contract_factory))
    ):
        raise ReferenceFamilyArchiveError("NUCC source COPY capability is unavailable")
    source_schema = _schema_name(models.NUCCTaxonomy.__table__.schema)
    async with asyncio.timeout(source_copy.timeout) as deadline:
        async with session_factory() as source_session, source_session.begin():
            # The exact SHARE/source-generation fences make source rows immutable
            # through this one transaction's COPY, validation and ownership record.
            metadata_by_field = await source_metadata_factory(source_session)
            source_capture_contract = (
                await source_capture_contract_factory(source_session) if source_capture_contract_factory else None
            )
            if source_capture_contract not in {None, IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT}:
                raise ReferenceFamilyArchiveError("NUCC source capture contract differs")
            capture = await _capture_reference_family_source(
                source_session,
                importer_id="nucc",
                schema_name=source_schema,
                source_metadata=metadata_by_field,
                source_capture_contract=source_capture_contract,
            )
            if capture.manifest.source_serving_generation is None:
                raise ReferenceFamilyArchiveError("NUCC source generation is unavailable")
            ownership = await precreate_reference_family_restore(
                source_session, importer_id="nucc", dataset_id=dataset_id
            )
            await on_precreated(source_session, ownership)
            await _copy_nucc_source_model(source_session, capture, ownership, source_copy, deadline.when())
            await complete_reference_family_restore(source_session, ownership)
            prepared = await _validate_nucc_source_clone(source_session, capture, ownership)
            await on_prepared(source_session, prepared)
    return prepared


async def _copy_nucc_source_model(session, capture, ownership, source_copy, deadline):
    """Select only fixed model columns from the caller-held source snapshot."""
    model = reference_family_spec("nucc").model_types[0]
    columns = tuple(column.name for column in model.__table__.columns)
    query = (
        "SELECT "
        + ", ".join(_quoted(column) for column in columns)
        + f' FROM {_quoted(capture.schema_name)}.{_quoted(model.__tablename__)} ORDER BY code COLLATE "C"'
    )
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise TimeoutError("NUCC source COPY deadline expired")
    copied_bytes = await source_copy.copy_rows(
        session,
        query,
        schema_name=ownership.schema_name,
        table_name=model.__tablename__,
        columns=columns,
        max_bytes=source_copy.max_bytes,
        timeout=remaining,
    )
    if type(copied_bytes) is not int or not 0 < copied_bytes <= source_copy.max_bytes:
        raise ReferenceFamilyArchiveError("NUCC source COPY accounting is invalid")


async def _validate_nucc_source_clone(session, capture, ownership):
    """Require indexed model equality before computing the portable clone receipt."""
    model = reference_family_spec("nucc").model_types[0]
    await validate_nucc_reference_set(session, ownership=ownership)
    if not await _is_model_table_equal(
        session,
        model,
        left_schema=capture.schema_name,
        left_name=model.__tablename__,
        right_schema=ownership.schema_name,
        right_name=model.__tablename__,
    ):
        raise ReferenceFamilyArchiveError("NUCC source clone differs from its pinned source")
    table = await _table_receipt(session, importer_id="nucc", schema_name=ownership.schema_name, model_type=model)
    manifest = replace(capture.manifest, tables=(table,), schema_sha256=_schema_digest((table,)))
    return ReferenceFamilyPreparedSource(manifest, ownership)


async def export_prepared_reference_family_archive(
    session_factory: Any,
    *,
    prepared: ReferenceFamilyPreparedSource,
    archive_copy: Callable[[ReferenceFamilyStageCapture], Awaitable[None]],
    verify_custody: Callable[..., Awaitable[None]] | None = None,
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
            # The frozen clone is read-only for every role, its owner included; a read lock pins its names.
            await _lock_family(
                stage_session, stage_schema, _ownership_spec(prepared.ownership).archive_names, "ACCESS SHARE"
            )
            if verify_custody is not None:
                await verify_custody(stage_session, prepared)
            ownership = await verify_reference_family_stage_ownership(stage_session, ownership)
            await _validate_stage_manifest(stage_session, ownership=ownership, manifest=manifest)
            snapshot = (await stage_session.execute(text("SELECT pg_export_snapshot()"))).scalar_one()
            if not isinstance(snapshot, str) or _SNAPSHOT.fullmatch(snapshot) is None:
                raise ReferenceFamilyArchiveError("reference family stage snapshot is invalid")
        await archive_copy(ReferenceFamilyStageCapture(manifest, ownership, snapshot))
    return manifest


async def precreate_reference_family_restore(
    session: Any,
    *,
    importer_id: str,
    dataset_id: UUID,
    canonical: bool = False,
) -> ReferenceFamilyStageOwnership:
    """Create model heaps and sequences for a native data-only restore."""

    _require_transaction(session)
    spec = reference_family_spec(importer_id, canonical=canonical)
    if importer_id == "drug-claims":
        spec = reference_family_receive_spec(importer_id)
    return await precreate_model_family_stage(session, spec, dataset_id)


async def precreate_model_family_stage(
    session: Any,
    spec: ReferenceFamilySpec,
    dataset_id: UUID,
    *,
    include_identity: bool = False,
) -> ReferenceFamilyStageOwnership:
    """Create ordinary heaps and owned sequences, deferring indexes and constraints."""

    _require_transaction(session)
    _require_model_family_spec(spec)
    schema_name = reference_family_stage_schema(dataset_id)
    await _create_model_family(session, spec, schema_name, create_indexes=False, ordinary_heaps=True)
    return await capture_model_family_stage_ownership(session, spec, dataset_id, include_identity=include_identity)


async def _is_model_table_equal(
    session: Any,
    model_type: type,
    *,
    left_schema: str,
    left_name: str,
    right_schema: str,
    right_name: str,
    scope: tuple[str, tuple[str, ...]] | None = None,
    left_predicate: str | None = None,
) -> bool:
    """Compare indexed, isolated model tables exactly without row serialization or hashes."""
    _require_transaction(session)
    parameters_by_field = {}
    left = f"{_quoted(left_schema)}.{_quoted(left_name)}"
    right = f"{_quoted(right_schema)}.{_quoted(right_name)}"
    if left_predicate is not None:
        left = f"(SELECT canonical.* FROM {left} canonical {left_predicate})"
    if scope is not None:
        scope_column, scope_values = scope
        if scope_column not in model_type.__table__.c or not isinstance(scope_values, tuple):
            raise ReferenceFamilyArchiveError("set comparison scope differs from the installed model")
        parameters_by_field["scope_values"] = list(scope_values)
        predicate = f"{_quoted(scope_column)}=ANY(CAST(:scope_values AS text[]))"
        left = f"(SELECT * FROM {left} WHERE {predicate})"
        right = f"(SELECT * FROM {right} WHERE {predicate})"
    return await _is_model_projection_equal(session, model_type, left, right, parameters_by_field)


async def _create_model_family(
    session: Any,
    spec: ReferenceFamilySpec,
    schema_name: str,
    *,
    create_indexes: bool = True,
    ordinary_heaps: bool = False,
    protected_owner_oid: int | None = None,
) -> None:
    """Clone installed model types, optionally as unpartitioned isolated heaps."""
    if await create_model_family_archive_schema(session, spec.importer_id, schema_name, protected_owner_oid):
        await _create_model_heaps(
            session, spec, schema_name, create_indexes=create_indexes, ordinary_heaps=ordinary_heaps
        )
        return
    authorization = ""
    if protected_owner_oid is not None:
        owner_name, _readers = model_family_roles()
        authorization = f" AUTHORIZATION {_quoted(owner_name)}"
    await session.execute(text(f"CREATE SCHEMA {_quoted(schema_name)}{authorization}"))
    await _create_model_heaps(session, spec, schema_name, create_indexes=create_indexes, ordinary_heaps=ordinary_heaps)


def _declared_owner_columns(spec: ReferenceFamilySpec, table_name: str) -> set[str]:
    """Select only columns owned by declared sequences for this exact model table."""
    return {
        column_name
        for _, owner_table, column_name in _OWNED_SEQUENCES.get(spec.importer_id, ())
        if owner_table == table_name
    }


async def _create_model_heaps(
    session: Any,
    spec: ReferenceFamilySpec,
    schema_name: str,
    *,
    create_indexes: bool = True,
    ordinary_heaps: bool = False,
    physical_names=None,
) -> None:
    """Create trusted model heaps within the caller's already authenticated namespace."""
    if spec.importer_id == "mrf" and STAGE_TABLE not in spec.table_names:
        await session.execute(
            text(
                f"CREATE TABLE {_quoted(schema_name)}.{_quoted(STAGE_TABLE)} "
                f"(address_key uuid {'PRIMARY KEY' if create_indexes else 'NOT NULL'}, payload jsonb NOT NULL)"
            )
        )
    metadata = MetaData(schema=schema_name)
    for model_type in spec.model_types:
        table = _clone_model_table(model_type.__table__, metadata, schema=schema_name)
        if physical_names is not None:
            table.name = physical_names[model_type.__tablename__]
        if ordinary_heaps:
            table.dialect_options["postgresql"]["partition_by"] = None
            owned_columns = _declared_owner_columns(spec, table.name)
            for column in table.columns:
                if column.identity is None and column.name not in owned_columns:
                    column.autoincrement = False
        if spec.importer_id == "provider-quality" and table.primary_key.columns:
            table.primary_key.name = _index_name_for_table(table.name, f"{schema_name}_{table.name}_pkey")
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
        if not create_indexes:
            _defer_table_constraints(table)
        statement = str(CreateTable(table).compile(dialect=postgresql.dialect()))
        await _execute_model_statement(session, text(statement))
        for sequence_name, column_name in explicit_sequences:
            await session.execute(
                text(
                    f"ALTER SEQUENCE {_quoted(schema_name)}.{_quoted(sequence_name)} OWNED BY "
                    f"{_quoted(schema_name)}.{_quoted(table.name)}.{_quoted(column_name)}"
                )
            )
    if create_indexes:
        await _create_model_indexes(session, spec, schema_name, physical_names=physical_names)


def _defer_table_constraints(table: Table) -> None:
    """Keep column semantics while delaying native constraints until family completion."""
    for column in table.columns:
        for constraint in tuple(column.constraints):
            column.constraints.remove(constraint)
            table.append_constraint(constraint)
    for constraint in table.constraints:
        constraint.ddl_if(callable_=lambda *_args, **_kwargs: False)


async def _validate_model_foreign_keys(session: Any, metadata: MetaData) -> None:
    """Check model relationships once on indexed isolated sets; install no foreign keys."""
    for table in metadata.tables.values():
        for constraint in sorted(table.foreign_key_constraints, key=lambda item: str(item.name or "")):
            elements = tuple(constraint.elements)
            parent = elements[0].column.table
            if constraint.match not in {None, "SIMPLE", "FULL"} or any(
                element.column.table is not parent for element in elements
            ):
                raise ReferenceFamilyArchiveError("model relationship match is unsupported")
            join = " AND ".join(
                f"p.{_quoted(element.column.name)}=c.{_quoted(element.parent.name)}" for element in elements
            )
            present = " AND ".join(f"c.{_quoted(element.parent.name)} IS NOT NULL" for element in elements)
            invalid = f"({present}) AND NOT EXISTS(SELECT 1 FROM {_quoted(parent.schema)}.{_quoted(parent.name)} p WHERE {join})"
            if constraint.match == "FULL":
                any_present = " OR ".join(f"c.{_quoted(element.parent.name)} IS NOT NULL" for element in elements)
                invalid = f"(({any_present}) AND NOT({present})) OR ({invalid})"
            violates = await session.scalar(
                text(f"SELECT EXISTS(SELECT 1 FROM {_quoted(table.schema)}.{_quoted(table.name)} c WHERE {invalid})")
            )
            if violates is not False:
                raise ReferenceFamilyArchiveError("staged model relationship differs")


async def complete_reference_family_restore(
    session: Any, ownership: ReferenceFamilyStageOwnership, *, max_bytes=None
) -> None:
    """Build reviewed model indexes after data restore, before freezing the owned stage."""

    _require_transaction(session)
    if not isinstance(ownership, ReferenceFamilyStageOwnership):
        raise ReferenceFamilyArchiveError("reference family stage ownership is invalid")
    await complete_model_family_stage(session, _ownership_spec(ownership), ownership)
    if ownership.importer_id == "drug-claims":
        await prepare_reference_dictionary_effects(session, ownership, max_bytes=max_bytes)


async def complete_model_family_stage(
    session: Any,
    spec: ReferenceFamilySpec,
    ownership: ReferenceFamilyStageOwnership,
    *,
    include_identity: bool = False,
) -> None:
    """Verify custody, then finish native indexes and indexed set relationship checks."""

    await verify_model_family_stage_ownership(session, spec, ownership, include_identity=include_identity)
    await _create_model_indexes(session, spec, ownership.schema_name, create_constraints=True)


async def _execute_model_statement(session, statement):
    """Compile the same trusted model DDL for a session or its native driver."""
    if callable(getattr(session, "is_in_transaction", None)):
        if not session.is_in_transaction():
            raise ReferenceFamilyArchiveError("model DDL requires a caller transaction")
        return await session.execute(str(statement.compile(dialect=postgresql.dialect())))
    return await session.execute(statement)


def ndc_model_family_spec():
    """Resolve the exact source-bound product/package model and indexed set edge."""
    from drug_snapshot_runtime.ndc import RELATIONSHIPS, Package, Product

    return ReferenceFamilySpec("ndc", (Product, Package), relationships=RELATIONSHIPS)


async def precreate_ndc_model_heaps(connection, schema_name, physical_names):
    """Use model DDL for the existing operation-owned NDC pair, without indexes."""
    spec = ndc_model_family_spec()
    await _require_ndc_model_targets(connection, spec, schema_name, physical_names)
    await _create_model_heaps(
        connection, spec, schema_name, create_indexes=False, ordinary_heaps=True, physical_names=physical_names
    )


async def complete_ndc_model_heaps(connection, schema_name, physical_names, relation_oids):
    """Finish the exact NDC OIDs with the shared model index/set-check compiler."""
    spec = ndc_model_family_spec()
    await _require_ndc_model_targets(connection, spec, schema_name, physical_names)
    if (
        not isinstance(relation_oids, Mapping)
        or set(relation_oids) != set(spec.table_names)
        or any(type(oid) is not int or oid <= 0 for oid in relation_oids.values())
    ):
        raise ReferenceFamilyArchiveError("NDC model heap custody differs")
    for table_name in spec.table_names:
        physical = f"{_quoted(schema_name)}.{_quoted(physical_names[table_name])}"
        await connection.execute(f"LOCK TABLE ONLY {physical} IN SHARE MODE NOWAIT")
        if await connection.fetchval("SELECT pg_catalog.to_regclass($1)::oid", physical) != relation_oids[table_name]:
            raise ReferenceFamilyArchiveError("NDC model heap custody differs")
    await _create_model_indexes(connection, spec, schema_name, create_constraints=True, physical_names=physical_names)


async def _require_ndc_model_targets(connection, spec, schema_name, physical_names):
    """Admit only the existing closed operation table names on a native transaction."""
    await _native_model_copy_driver(connection, ("execute", "fetchval"))
    _schema_name(schema_name)
    if not isinstance(physical_names, Mapping) or set(physical_names) != set(spec.table_names):
        raise ReferenceFamilyArchiveError("NDC model heap targets differ")
    identities = set()
    for table in spec.table_names:
        name = physical_names[table]
        match = (
            re.fullmatch(r"hp_ndc_a_([0-9a-f]{32})_" + table + r"|hp_ndc_" + table + r"_s_([0-9a-f]{32})", name)
            if isinstance(name, str)
            else None
        )
        if match is None:
            raise ReferenceFamilyArchiveError("NDC model heap targets differ")
        identities.add((match[1] is not None, match[1] or match[2]))
    if len(identities) != 1:
        raise ReferenceFamilyArchiveError("NDC model heap targets differ")


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
            _ownership_spec(ownership).archive_names,
            "ACCESS SHARE",
        )
        ownership = await verify_reference_family_stage_ownership(session, ownership)
    return await _validate_stage_manifest(
        session,
        ownership=ownership,
        manifest=validated_manifest,
    )


async def prepare_reference_family_activation(
    session: Any,
    *,
    ownership: ReferenceFamilyStageOwnership,
    manifest: Mapping[str, Any] | ReferenceFamilyManifest,
    package_id: str,
    profile_contract: str,
    sealed_owner_oid=None,
    roles=None,
) -> ReferenceFamilyValidationReceipt:
    """Perform long validation over an already publisher-frozen stage."""

    _require_transaction(session)
    validated_manifest = validate_reference_family_manifest(manifest)
    if (
        profile_contract != reference_family_profile_contract(validated_manifest)
        or re.fullmatch(r"[0-9a-f]{64}", str(package_id)) is None
        or validated_manifest.importer_id != ownership.importer_id
    ):
        raise ReferenceFamilyArchiveError("reference family validation scope differs")
    ownership = await verify_reference_family_stage_ownership(session, ownership)
    table_receipts = await _validate_stage_manifest(session, ownership=ownership, manifest=validated_manifest)
    validation_by_field = {
        "contract": VALIDATION_CONTRACT,
        "importer_id": ownership.importer_id,
        "package_id": package_id,
        "profile_contract": profile_contract,
        "stage_schema": ownership.schema_name,
        "relations": [
            {"schema_name": ownership.schema_name, "relation_name": name} for name in ownership.relation_names
        ],
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
    canonical: bool = False,
) -> ReferenceFamilyIncumbent:
    """Capture all-present or all-absent live relation OIDs for later CAS."""

    _require_transaction(session)
    spec = reference_family_spec(importer_id, canonical=canonical)
    if importer_id == "drug-claims":
        spec = reference_family_receive_spec(importer_id)
    schema = _schema_name(schema_name)
    async with _bounded_capture(session):
        pairs = await _incumbent_pairs(session, spec, schema)
        present_flags = [oid is not None for name, oid in pairs if name != STAGE_TABLE]
        if importer_id in TERMINAL_CAPTURE_IMPORTERS:
            present_flags = terminal_incumbent_presence(importer_id, pairs)
        if any(present_flags) and not all(present_flags):
            raise ReferenceFamilyArchiveError("reference family incumbent is incomplete")
        if all(present_flags):
            await _lock_family(session, schema, tuple(name for name, oid in pairs if oid is not None), "ACCESS SHARE")
            if tuple((name, oid is not None) for name, oid in await _incumbent_pairs(session, spec, schema)) != tuple(
                (name, oid is not None) for name, oid in pairs
            ):
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
    spec = reference_family_spec(
        expected.importer_id, canonical=expected.importer_id == "mrf" and STAGE_TABLE in dict(expected.relation_oids)
    )
    if expected.importer_id == "drug-claims":
        spec = reference_family_receive_spec(expected.importer_id)
    observed_oids = await _incumbent_pairs(session, spec, expected.schema_name)
    if tuple((name, oid is not None) for name, oid in observed_oids) != tuple(
        (name, oid is not None) for name, oid in expected.relation_oids
    ):
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
    *,
    wait_for_readers: bool = False,
    stage_read_only: bool = False,
) -> ReferenceFamilyStageOwnership:
    """Read-lock frozen stages; mutable/manual callers retain exclusive verification."""
    lock_mode = "ACCESS SHARE" if stage_read_only else "ACCESS EXCLUSIVE"
    async with _bounded_capture(session):
        await _lock_family(session, ownership.schema_name, spec.archive_names, lock_mode, nowait=True)
        incumbent_names = tuple(name for name, oid in expected_incumbent.relation_oids if oid is not None)
        if incumbent_names and wait_for_readers and not stage_read_only:
            from process.entity_address_cutover_contract import lock_live_serving_relations

            await lock_live_serving_relations(
                lambda sql: session.execute(text(sql)), expected_incumbent.schema_name, incumbent_names
            )
        elif incumbent_names:
            await _lock_family(session, expected_incumbent.schema_name, incumbent_names, lock_mode, nowait=True)
        ownership = await verify_reference_family_stage_ownership(session, ownership)
        await _verify_incumbent(session, expected_incumbent)
    return ownership


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


def _selected_canonical_sources(inventories, selected_relations, owner_oid):
    """Resolve only complete protected native families selected by the existing role pins."""
    from process.mrf_address_publication import CANONICAL_POLICIES
    from process.npi_result_archive import npi_archive_names

    expected_names_by_importer = {
        "npi": npi_archive_names(canonical=True),
        "mrf": reference_family_spec("mrf", canonical=True).table_names,
    }
    relations, contributions = [], {}
    for importer in sorted(inventories):
        family = _selected_canonical_relations(
            inventories[importer], selected_relations[importer], owner_oid, expected_names_by_importer[importer]
        )
        relations.extend(family)
        contribution = next(
            relation for relation in family if relation["relation_name"] == CANONICAL_POLICIES[importer][0]
        )
        contributions[importer] = f"{_quoted(contribution['schema_name'])}.{_quoted(contribution['relation_name'])}"
    return relations, contributions


def _selected_canonical_relations(inventory, selected, owner_oid, expected_names):
    """Bind the complete named family to its admitted logical Address role."""
    relations = inventory["relations"]
    if (
        len(relations) != len(expected_names)
        or {entry["relation_name"] for entry in relations} != set(expected_names)
        or len({entry["schema_name"] for entry in relations}) != 1
    ):
        raise ReferenceFamilyArchiveError("selected canonical complete family differs")
    for entry in relations:
        _schema_name(entry["schema_name"])
    match = next((entry for entry in relations if entry["relation_name"] == selected["table_name"]), None)
    if match is None or match["schema_name"] != selected["schema_name"]:
        raise ReferenceFamilyArchiveError("selected canonical role identity differs")
    return [{"schema_name": entry["schema_name"], "relation_name": entry["relation_name"]} for entry in relations]


@asynccontextmanager
async def selected_mrf_canonical_archive(session, *, schema_name, owner_oid, selected_relation, authenticate_inventory):
    """Validate an authenticated auxiliary's future coverage without publishing canonical data.

    The callback rechecks the complete protected selected family in this owning
    transaction; metadata alone is not admission. SHARE locks preserve ordinary
    readers. A failed validation rolls back its private view with the caller's
    transaction; success drops it before returning. No shared heap/index changes.
    """
    _require_transaction(session)
    if not callable(authenticate_inventory):
        raise ReferenceFamilyArchiveError("selected MRF publisher authority differs")
    inventory = await authenticate_inventory(session)
    archive = f"{_quoted(schema_name)}.{_quoted(archive_table_name())}"
    await session.execute(text(f"LOCK TABLE {archive} IN ACCESS SHARE MODE NOWAIT"))
    if inventory is None:
        # Only the authenticating callback may select an already-installed MRF.
        yield None
        return
    relations = _selected_mrf_relations(inventory, selected_relation, owner_oid)
    await session.execute(
        text(
            "LOCK TABLE "
            + ", ".join(
                f"{_quoted(relation['schema_name'])}.{_quoted(relation['relation_name'])}" for relation in relations
            )
            + " IN ACCESS SHARE MODE NOWAIT"
        )
    )
    await _require_selected_mrf_catalog(session, relations, owner_oid)
    auxiliary = next(relation for relation in relations if relation["relation_name"] == STAGE_TABLE)
    stage = f"{_quoted(auxiliary['schema_name'])}.{_quoted(STAGE_TABLE)}"
    await _validate_mrf_canonical_address_merge(session, stage, archive)
    view_name = "address_mrf_archive_validation"
    await session.execute(
        text(f"CREATE TEMP VIEW {_quoted(view_name)} AS " + _mrf_archive_projection_sql(stage, archive))
    )
    temporary_schema = await session.scalar(
        text("SELECT nspname::text FROM pg_catalog.pg_namespace WHERE oid=pg_catalog.pg_my_temp_schema()")
    )
    try:
        yield (_schema_name(temporary_schema), view_name)
    except BaseException:
        # Failed SQL can abort the owning transaction. Do not mask its error
        # with cleanup SQL; rollback removes this attempt's uncommitted view.
        raise
    else:
        await session.execute(text(f"DROP VIEW {_quoted(temporary_schema)}.{_quoted(view_name)} RESTRICT"))


def _mrf_archive_projection_sql(stage, archive):
    """Preserve the auxiliary's indexed native key and the exact canonical merge precedence."""
    columns = tuple(column.name for column in models.AddressArchiveV2.__table__.columns)
    canonical_columns = ", ".join(_quoted(name) for name in columns)
    auxiliary_columns = ", ".join(
        "source.address_key" if name == "address_key" else "typed." + _quoted(name) for name in columns
    )
    return (
        f"SELECT {canonical_columns} FROM {archive} UNION ALL SELECT {auxiliary_columns} FROM {stage} AS source "
        f"CROSS JOIN LATERAL jsonb_populate_record(NULL::{archive},source.payload) AS typed "
        f"WHERE NOT EXISTS(SELECT 1 FROM {archive} AS target WHERE target.address_key=source.address_key)"
    )


def _selected_mrf_relations(inventory, selected_relation, owner_oid):
    return sorted(
        _selected_canonical_relations(
            inventory, selected_relation, owner_oid, reference_family_spec("mrf").archive_names
        ),
        key=lambda entry: (entry["schema_name"], entry["relation_name"]),
    )


async def _require_selected_mrf_catalog(session, relations, owner_oid):
    """Check complete named heap/index shape after reader locks, without ACL identity checks."""
    for relation in relations:
        name = f"{_quoted(_schema_name(relation['schema_name']))}.{_quoted(relation['relation_name'])}"
        accepted = await session.scalar(
            text("""
            SELECT c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
              AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
              AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
              AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_index WHERE indrelid=c.oid AND (NOT indisvalid OR NOT indisready OR NOT indislive))
              AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid=c.oid AND contype NOT IN ('p','u','n','c'))
            FROM pg_catalog.pg_class c WHERE c.oid=pg_catalog.to_regclass(:relation)
        """),
            {"relation": name},
        )
        if accepted is not True:
            raise ReferenceFamilyArchiveError("selected MRF locked catalog differs")


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
    canonical_publication=None,
) -> ReferenceFamilyActivationReceipt:
    live_pairs = await _incumbent_pairs(session, spec, expected_incumbent.schema_name)
    if any(type(oid) is not int or oid <= 0 for _, oid in live_pairs):
        raise ReferenceFamilyArchiveError("reference family activated relation is unavailable")
    if tuple(sorted(name for name, oid in live_pairs if oid is not None)) != ownership.relation_names:
        raise ReferenceFamilyArchiveError("reference family activated relation declaration differs")
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
        local_manifest = replace(local_manifest, source_capture_contract=manifest.source_capture_contract)
        if _is_legacy_cms_manifest(manifest):
            if local_manifest.tables != tables or not _has_matching_manifest_stage_tables(manifest, tables):
                raise ReferenceFamilyArchiveError("reference family activated receipt differs")
        elif not _has_matching_family_manifest(manifest, local_manifest):
            raise ReferenceFamilyArchiveError("reference family activated receipt differs")
    return ReferenceFamilyActivationReceipt(
        spec.importer_id,
        manifest.source_metadata_sha256,
        tuple((name, int(oid)) for name, oid in live_pairs),
        expected_incumbent.relation_oids,
        predecessor_schema_name,
        tables,
        canonical_publication,
    )


async def _return_manual_mrf_ownership(session, ownership, schema_name):
    """Return the verified locked heaps and their attached sequences to the ordinary publisher."""
    for table_name, _relation_oid in ownership.relation_oids:
        await session.execute(text(f"ALTER TABLE {_quoted(schema_name)}.{_quoted(table_name)} OWNER TO CURRENT_USER"))


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
    if isinstance(ownership, ReferenceFamilyStageOwnership) and ownership.importer_id in {
        "facility-anchors",
        *TERMINAL_CAPTURE_IMPORTERS,
    }:
        raise ReferenceFamilyArchiveError("family activation requires protected contribution preparation")
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
    spec = _ownership_spec(ownership)
    if spec.importer_id == "nucc" and await is_nucc_native_handoff_required(
        session, schema_name=expected_incumbent.schema_name
    ):
        raise ReferenceFamilyArchiveError("NUCC protected custody requires validated publisher activation")
    ownership = await _lock_and_verify_activation(session, spec, ownership, expected_incumbent)
    tables = await _validate_stage_manifest(session, ownership=ownership, manifest=validated_manifest)
    predecessor_schema_name, _live_pairs, canonical_publication = await _complete_validated_stage_activation(
        session, spec, ownership, expected_incumbent, validated_manifest
    )
    if spec.importer_id == "mrf":
        await _return_manual_mrf_ownership(session, ownership, expected_incumbent.schema_name)
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
        canonical_publication,
    )


async def _complete_validated_stage_activation(
    session: Any,
    spec: ReferenceFamilySpec,
    ownership: ReferenceFamilyStageOwnership,
    expected_incumbent: ReferenceFamilyIncumbent,
    manifest: ReferenceFamilyManifest,
    *,
    roles=None,
) -> tuple[str | None, list[tuple[str, int | None]], Mapping[str, Any] | None]:
    """Rotate, merge MRF canonical rows, and verify the activated relation OIDs."""

    if spec.importer_id == "claims-pricing":
        incumbent_oids_by_name = dict(expected_incumbent.relation_oids)
        dictionary_presence_flags = tuple(
            incumbent_oids_by_name.get(model.__tablename__) is not None for model in _CLAIMS_SCOPED_MODELS
        )
        if len(set(dictionary_presence_flags)) != 1:
            raise ReferenceFamilyArchiveError("claims dictionary predecessor is incomplete")
        await replace_claims_dictionary_slice(
            session,
            incoming_schema=ownership.schema_name,
            current_schema=expected_incumbent.schema_name if all(dictionary_presence_flags) else None,
            destination_schema=expected_incumbent.schema_name,
        )
    if spec.importer_id == "drug-claims":
        await apply_reference_dictionary_effects(
            session,
            incoming_schema=ownership.schema_name,
            current_schema=expected_incumbent.schema_name,
            destination_schema=expected_incumbent.schema_name,
        )
    predecessor_schema_name = await _rotate_family_relations(
        session, spec, ownership, expected_incumbent, **({"roles": roles} if spec.importer_id == "nucc" else {})
    )
    canonical_publication = await _publish_canonical_contribution(
        session, spec, ownership, expected_incumbent, manifest
    )
    await _drop_empty_stage_schema(session, ownership)
    live_pairs = await _incumbent_pairs(session, spec, expected_incumbent.schema_name)
    if tuple(sorted(name for name, oid in live_pairs if oid is not None)) != ownership.relation_names:
        raise ReferenceFamilyArchiveError("reference family activated relation declaration differs")
    return predecessor_schema_name, live_pairs, canonical_publication


async def _publish_canonical_contribution(session, spec, ownership, incumbent, manifest):
    """Retain typed heaps while preserving historical JSON merge interpretation."""
    if spec.importer_id != "mrf":
        return None
    if STAGE_TABLE not in spec.table_names:
        await _merge_mrf_canonical_address(session, ownership, incumbent.schema_name, manifest.auxiliary)
        return None
    return await merge_canonical_contribution(
        session,
        "mrf",
        f"{_quoted(incumbent.schema_name)}.{_quoted(STAGE_TABLE)}",
        f"{_quoted(incumbent.schema_name)}.{_quoted(archive_table_name())}",
    )


def _require_cutover_authority(ownership, expected_incumbent, cutover) -> None:
    """Reject malformed activation scope before inspecting or locking a stage."""

    if not isinstance(cutover, ReferenceFamilyCutoverAuthority):
        raise ReferenceFamilyArchiveError("reference family cutover authority is invalid")
    if type(cutover.stage_read_only) is not bool:
        raise ReferenceFamilyArchiveError("reference family read-only stage declaration is invalid")
    if cutover.authority not in {"manual", "automatic"}:
        raise ReferenceFamilyArchiveError("reference family activation authority is unsupported")
    if not isinstance(ownership, ReferenceFamilyStageOwnership) or not isinstance(
        expected_incumbent, ReferenceFamilyIncumbent
    ):
        raise ReferenceFamilyArchiveError("reference family activation ownership is invalid")


def _validated_activation_inputs(session, ownership, manifest, expected_incumbent, validation_receipt, cutover):
    """Validate the complete activation authority before acquiring any cutover lock."""
    _require_transaction(session)
    _require_cutover_authority(ownership, expected_incumbent, cutover)
    validated_manifest = validate_reference_family_manifest(manifest)
    validation = validate_reference_family_validation_receipt(validation_receipt)
    _require_validated_cutover_binding(ownership, expected_incumbent, validated_manifest, validation, cutover)
    spec = _ownership_spec(ownership)
    _require_terminal_manual_cutover(spec, validated_manifest, cutover)
    return validated_manifest, validation, spec


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

    validated_manifest, validation, spec = _validated_activation_inputs(
        session, ownership, manifest, expected_incumbent, validation_receipt, cutover
    )
    async with _frozen_activation_lock_wait(session, cutover.stage_read_only):
        ownership = await _lock_and_verify_activation(
            session,
            spec,
            ownership,
            expected_incumbent,
            wait_for_readers=spec.importer_id not in {"mrf", "facility-anchors"},
            stage_read_only=cutover.stage_read_only,
        )
        async with _validated_reference_publication(session, spec, expected_incumbent, cutover):
            return await _activate_verified_reference_family(
                session,
                spec,
                ownership,
                expected_incumbent,
                validated_manifest,
                validation,
                cutover,
                contribution_effect_receipt,
            )


@asynccontextmanager
async def _frozen_activation_lock_wait(session, stage_read_only):
    """Let native DDL take swap locks without granting DML or changing the scan timeout."""
    if not stage_read_only:
        yield
        return
    previous_lock = await _timeout_value(session, "lock_timeout")
    await _set_local_timeout(session, "lock_timeout", _LOCK_TIMEOUT)
    token = _FROZEN_CUTOVER.set(True)
    try:
        yield
    except BaseException:
        # A failed transaction rolls back SET LOCAL; preserve the original exception.
        raise
    else:
        await _set_local_timeout(session, "lock_timeout", previous_lock)
    finally:
        _FROZEN_CUTOVER.reset(token)


def cutover_wait_deadline():
    """Start one shared exclusive-lock budget for the live tables of a frozen cutover."""
    return asyncio.get_running_loop().time() + _CUTOVER_WAIT_SECONDS if _FROZEN_CUTOVER.get() else None


async def bound_cutover_wait(session, deadline):
    """Each later live-table lock waits only for what remains of the shared budget, never less than 1 ms."""
    if deadline is not None:
        remaining_ms = max(1, int((deadline - asyncio.get_running_loop().time()) * 1000))
        await _set_local_timeout(session, "lock_timeout", f"{remaining_ms}ms")


@asynccontextmanager
async def _validated_reference_publication(session, spec, incumbent, cutover):
    from process.provider_directory_generic_source_policy import _PUBLISHER

    if spec.importer_id not in {"geo", "cms-doctors"} or _PUBLISHER.get() is not None:
        yield
        return
    async with protected_reference_family_publication(
        session,
        importer_id=spec.importer_id,
        schema_name=incumbent.schema_name,
        roles=getattr(cutover, "roles", None),
    ):
        yield


def _require_terminal_manual_cutover(spec, manifest, cutover):
    """Terminal history never authorizes an automatic producer-generation cutover."""
    if spec.importer_id in TERMINAL_CAPTURE_IMPORTERS and (
        cutover.authority != "manual"
        or manifest.publication_authority != "manual-only"
        or manifest.source_serving_generation is not None
    ):
        raise ReferenceFamilyArchiveError("terminal reference captures require explicit manual activation")


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
            or "facility_address_contribution" not in ownership.relation_names
            or receipt.get("contribution_relation_name", "facility_address_contribution")
            != "facility_address_contribution"
        ):
            raise ReferenceFamilyArchiveError("facility contribution stage or destination differs")
        await apply_facility_address_effects(session, receipt)
    elif receipt is not None:
        raise ReferenceFamilyArchiveError("reference family contribution scope differs")


def _require_validated_cutover_binding(ownership, expected_incumbent, manifest, validation, cutover) -> None:
    """Bind the protected receipt to the exact stage, package, and incumbent."""

    if _is_legacy_cms_manifest(manifest) and cutover.authority != "manual":
        raise ReferenceFamilyArchiveError("legacy CMS archive requires manual activation")
    if (
        cutover.authority == "automatic"
        and manifest.importer_id != "label"
        and manifest.source_capture_contract != GUARDED_SOURCE_CAPTURE_CONTRACT
        and not (
            manifest.importer_id in {"nucc", "cms-doctors", "geo"}
            and manifest.source_capture_contract == IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT
        )
    ):
        raise ReferenceFamilyArchiveError("reference family automatic activation requires guarded source capture")
    manifest_sha256 = hashlib.sha256(_canonical_json(manifest.as_dict())).hexdigest()
    if (
        manifest.importer_id != ownership.importer_id
        or expected_incumbent.importer_id != ownership.importer_id
        or validation.importer_id != ownership.importer_id
        or validation.package_id != cutover.package_id
        or validation.profile_contract != cutover.profile_contract
        or validation.stage_schema != ownership.schema_name
        or validation.relations != tuple((ownership.schema_name, name) for name in ownership.relation_names)
        or validation.manifest_sha256 != manifest_sha256
        or not _has_matching_manifest_stage_tables(
            manifest,
            tuple(
                table
                for table in validation.tables
                if table.table_name in reference_family_spec(manifest.importer_id).table_names
            )
            if manifest.importer_id == "drug-claims"
            else validation.tables,
        )
    ):
        raise ReferenceFamilyArchiveError("reference family validation authority differs")


def _activation_source_generation(manifest, cutover):
    """Keep a legacy two-relation generation as provenance, never current-family authority."""
    incoming_generation = _cutover_source_generation(cutover)
    if incoming_generation != manifest.source_serving_generation:
        raise ReferenceFamilyArchiveError("reference family source generation differs from captured manifest")
    return None if _is_legacy_cms_manifest(manifest) else incoming_generation


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


async def _require_automatic_cutover_generation(
    session, spec, expected_incumbent, incoming_generation, *, source_capture_contract=None
) -> None:
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
    if current_authority.relations != tuple(
        (expected_incumbent.schema_name, name) for name in RELATION_NAMES_BY_IMPORTER[spec.importer_id]
    ):
        raise ReferenceFamilyArchiveError("reference family incumbent generation drifted")
    if spec.importer_id == "nucc" and source_capture_contract == IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT:
        from process.reference_family_result_generation import require_nucc_native_predecessor_storage

        await require_nucc_native_predecessor_storage(
            session,
            schema_name=expected_incumbent.schema_name,
            expected_relation_oid=dict(expected_incumbent.relation_oids)["nucc_taxonomy"],
        )
    elif spec.importer_id == "geo":
        from process.geo_source_storage import capture_geo_generation

        await capture_geo_generation(session, expected_incumbent.schema_name)
    elif spec.importer_id == "cms-doctors":
        await require_doctors_storage(session, schema_name=expected_incumbent.schema_name, legacy=True)
    elif spec.importer_id != "label":
        from process.reference_family_result_generation import require_reference_family_source_consistency

        await require_reference_family_source_consistency(
            session, importer_id=spec.importer_id, schema_name=expected_incumbent.schema_name
        )
    try:
        require_reference_family_automatic_generation_order(incoming_generation, current_authority.serving_generation)
    except ValueError as error:
        raise ReferenceFamilyArchiveError("reference family automatic generation is stale or unrelated") from error


def _require_published_generation_binding(
    spec, live_pairs, incoming_generation, published_authority, *, schema_name=None
) -> None:
    """Bind adopted authority to the complete declared serving location."""
    if incoming_generation is not None:
        relations = published_authority.relations
        if (
            relations is None
            or tuple(name for _schema, name in relations) != RELATION_NAMES_BY_IMPORTER[spec.importer_id]
            or len({schema for schema, _name in relations}) != 1
            or (
                schema_name is not None
                and relations != tuple((schema_name, name) for name in RELATION_NAMES_BY_IMPORTER[spec.importer_id])
            )
        ):
            raise ReferenceFamilyArchiveError("reference family adopted generation names differ")
    elif published_authority.serving_generation is not None or published_authority.relations is not None:
        raise ReferenceFamilyArchiveError("reference family generation-less adoption differs")


__all__ = [
    "CONTRACT",
    "NUCC_CONTRACT",
    "VALIDATION_CONTRACT",
    "ReferenceFamilyActivationReceipt",
    "ReferenceFamilyArchiveError",
    "ReferenceFamilyCutoverAuthority",
    "ReferenceFamilySourceCopy",
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
    "protected_reference_family_publication",
    "capture_reference_family_incumbent",
    "capture_reference_family_source",
    "capture_reference_family_stage_ownership",
    "capture_model_family_stage_ownership",
    "cleanup_reference_family_stage",
    "cleanup_model_family_stage",
    "export_reference_family_archive",
    "export_prepared_reference_family_archive",
    "precreate_reference_family_restore",
    "complete_reference_family_restore",
    "precreate_model_family_stage",
    "complete_model_family_stage",
    "prepare_reference_family_archive_source",
    "prepare_nucc_reference_archive_source",
    "prepare_reference_family_activation",
    "reference_family_spec",
    "reference_family_predecessor_schema",
    "reference_family_stage_schema",
    "validate_reference_family_manifest",
    "validate_reference_family_validation_receipt",
    "validate_reference_family_stage",
    "validate_nucc_reference_set",
    "verify_reference_family_stage_ownership",
    "verify_model_family_stage_ownership",
]


def _frozen_source_query(projection, model):
    """Render only a finite ordered TIGER projection of qualified names."""
    if (
        type(projection) is not tuple
        or len(projection) != 2
        or any(type(pair) is not tuple or len(pair) != 2 or type(pair[1]) is not tuple for pair in projection)
        or tuple(pair[0] for pair in projection) != ("zip_state", "zcta5")
    ):
        raise ReferenceFamilyArchiveError("frozen TIGER projection is invalid")
    statements, names = [], set()
    columns = ", ".join(_quoted(column.name) for column in model.__table__.columns)
    for logical_table, relations in projection:
        if not 1 <= len(relations) <= 4096:
            raise ReferenceFamilyArchiveError("frozen TIGER projection is empty or unbounded")
        for relation in relations:
            if (
                type(relation) is not tuple
                or len(relation) != 2
                or any(type(part) is not str for part in relation)
                or relation in names
            ):
                raise ReferenceFamilyArchiveError("frozen TIGER qualified name is invalid")
            schema, table = relation
            qualified = _quoted(schema) + "." + _quoted(table)
            names.add(relation)
            if logical_table == model.__tablename__:
                statements.append(f"SELECT {columns} FROM ONLY {qualified}")
        if tuple(sorted(relations)) != relations or relations.count(("tiger", logical_table)) != 1:
            raise ReferenceFamilyArchiveError("frozen TIGER canonical root differs")
    if not statements or len(names) > 4096:
        raise ReferenceFamilyArchiveError("frozen TIGER projection is empty or unbounded")
    return " UNION ALL ".join(statements)


def _require_frozen_source_capture(capture):
    """A new finite projection cannot be relabeled as a legacy captured origin."""
    is_finite = capture.manifest.source_capture_contract == CAPTURED_TIGER_SNAPSHOT_CONTRACT
    if is_finite != (capture.frozen_source_projection is not None):
        raise ReferenceFamilyArchiveError("frozen TIGER capture contract differs")
    if is_finite:
        if capture.manifest.importer_id != "tiger" or capture.schema_name != "tiger":
            raise ReferenceFamilyArchiveError("frozen projection requires the TIGER source")
        _validate_source_capture_contract(capture.manifest.as_dict())
        for model in reference_family_spec("tiger").model_types:
            _frozen_source_query(capture.frozen_source_projection, model)


async def _validate_frozen_source_clone(session, capture, stage_schema, spec):
    """Compare full model multisets against exactly the same finite COPY projection."""
    _require_frozen_source_capture(capture)
    for model in spec.model_types:
        source = "(" + _frozen_source_query(capture.frozen_source_projection, model) + ")"
        fields = ", ".join(
            _quoted(column.name) + ("::text" if str(column.type).lower().startswith(("geometry", "geography")) else "")
            for column in model.__table__.columns
        )
        target = f"{_quoted(stage_schema)}.{_quoted(model.__tablename__)}"
        equal = await session.scalar(
            text(
                f"SELECT NOT EXISTS((SELECT {fields} FROM {source} canonical EXCEPT ALL SELECT {fields} FROM {target}) "
                f"UNION ALL (SELECT {fields} FROM {target} EXCEPT ALL SELECT {fields} FROM {source} canonical))"
            )
        )
        if equal is not True:
            raise ReferenceFamilyArchiveError("reference family source clone differs from its pinned source")


async def _frozen_source_manifest(session, *, source_metadata, frozen_source_projection):
    """Count a finite TIGER model with the same ONLY projection later copied."""
    spec = reference_family_spec("tiger")
    metadata, metadata_sha256 = _source_metadata(source_metadata)
    receipts = tuple(
        [
            await _table_receipt(
                session,
                importer_id="tiger",
                schema_name="tiger",
                model_type=model,
                frozen_source_projection=frozen_source_projection,
            )
            for model in spec.model_types
        ]
    )
    return ReferenceFamilyManifest(
        "tiger",
        receipts,
        metadata,
        metadata_sha256,
        _schema_digest(receipts),
        publication_authority="captured-epoch",
        source_capture_contract=CAPTURED_TIGER_SNAPSHOT_CONTRACT,
    )
