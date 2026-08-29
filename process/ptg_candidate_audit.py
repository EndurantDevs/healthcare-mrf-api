# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Automatic release audit and activation for one strict PTG V3 candidate."""

from __future__ import annotations

import contextlib
import json
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any, AsyncIterator, Awaitable, Callable, Mapping, Sequence
from urllib.parse import urlsplit

from db.connection import db
from process.control_lifecycle import mark_control_run
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.domain import PTG2_CANDIDATE_ACTIVATION_CONTRACT
from process.ptg_parts.ptg2_fast_candidate_audit import (
    FastAuditHttpConfig,
    FastAuditTarget,
    FastCandidateAuditError,
    run_fast_candidate_audit,
)
from process.ptg_parts.ptg2_batch_candidate_audit import (
    BatchCandidateAuditContractError,
    BatchCandidateAuditTransportError,
)
from process.ptg_parts.ptg2_batch_candidate_audit_report import (
    BatchAuditReportTarget,
)
from process.ptg_parts.ptg2_candidate_attestation import (
    CANDIDATE_SOURCE_RECORDS_SQL,
    PTG2_CANDIDATE_ACTIVATION_INTENT_AUDIT_AND_ACTIVATE,
    PTG2_CANDIDATE_ACTIVATION_INTENT_AUDIT_ONLY,
    PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4,
    PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT,
    PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS,
    PTG2_TRUSTED_CLUSTER_HTTP_TRANSPORT,
    PTG2_VERIFIED_HTTPS_TRANSPORT,
    load_held_candidate_audit_attestation,
    record_candidate_audit_attestation,
)
from process.ptg_parts.ptg2_candidate_layout_identity import (
    PTG2_CANDIDATE_ARCH_VERSION,
    PTG2_CANDIDATE_V3_GENERATION,
    validate_candidate_layout_identity,
)
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_evidence,
    validate_provider_identifier_quarantine_evidence,
    validate_provider_identifier_quarantine,
)
from process.ptg_parts.ptg2_candidate_audit_plan_store import (
    load_persisted_audit_sample,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit import (
    PartitionFailureCallback,
    run_partitioned_candidate_audit,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit_contract import (
    PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_IN_FLIGHT,
)
from process.ptg_parts.ptg2_source_witness import (
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
    source_set_digest,
)
from process.ptg_parts.ptg2_source_witness_store import load_shared_source_witness
from process.ptg_parts.frozen_rate_candidate import (
    validate_frozen_candidate_evidence,
)
from process.ptg_parts.frozen_rate_binding import (
    INVALID_PRICE_EXCLUSION_POLICY_FIELD,
)
from process.ptg_parts.frozen_rate_files import (
    FrozenRateFileMismatchError,
    FrozenRateFileValidationError,
)
from process.ptg_parts.ptg2_invalid_price_exclusion import (
    validate_candidate_invalid_price_exclusion_evidence,
    validated_candidate_invalid_price_exclusion_policy,
)
from process.ptg_parts.source_snapshot_control import promote_ptg2_source_snapshot
from scripts.validation import ptg2_v3_source_api_audit


IMPORTER_NAME = "ptg-candidate-audit"
ARCH_VERSION = PTG2_CANDIDATE_ARCH_VERSION
STORAGE_GENERATION = PTG2_CANDIDATE_V3_GENERATION
CANDIDATE_AUDIT_MODE_AUDIT_AND_ACTIVATE = (
    PTG2_CANDIDATE_ACTIVATION_INTENT_AUDIT_AND_ACTIVATE
)
CANDIDATE_AUDIT_MODE_AUDIT_ONLY = (
    PTG2_CANDIDATE_ACTIVATION_INTENT_AUDIT_ONLY
)
CANDIDATE_AUDIT_MODES = (
    CANDIDATE_AUDIT_MODE_AUDIT_AND_ACTIVATE,
    CANDIDATE_AUDIT_MODE_AUDIT_ONLY,
)
PTG2_BATCH_AUDIT_WRITER_ENABLED = (
    PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT
    == PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
)
API_BASE_URL_ENV = "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL"
AUTH_HEADER_ENV = "HLTHPRT_PTG2_CANDIDATE_AUDIT_AUTH_HEADER"
AUTH_SCHEME_ENV = "HLTHPRT_PTG2_CANDIDATE_AUDIT_AUTH_SCHEME"
TRUSTED_CLUSTER_HTTP_ENV = "HLTHPRT_PTG2_CANDIDATE_AUDIT_TRUSTED_CLUSTER_HTTP"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REBUILD_RUN_SUFFIX_RE = re.compile(r":rebuild-[0-9a-f]{24}$")
_CANDIDATE_TARGET_SQL = """
    SELECT snapshot.snapshot_id,
           snapshot.import_run_id,
           snapshot.status,
           snapshot.previous_snapshot_id,
           snapshot.manifest,
           internal_run.options -> 'invalid_price_exclusion_policy'
               AS invalid_price_exclusion_policy,
           binding.snapshot_key,
           scope.plan_id,
           scope.plan_market_type,
           layout.state AS layout_state,
           layout.generation AS layout_generation,
           layout.mapping_digest AS layout_mapping_digest,
           layout.layout_manifest,
           v4_root.state AS v4_root_state,
           v4_root.map_digest AS v4_root_map_digest,
           current_pointer.snapshot_id AS current_snapshot_id,
           attestation.report_digest AS audit_report_digest,
           attestation.report AS audit_report,
           attestation.activation_intent AS audit_activation_intent,
           attestation.activated_at AS audit_activated_at,
           frozen_binding.binding_payload AS frozen_binding_payload,
           current_snapshot.import_run_id AS current_import_run_id,
           current_snapshot.status AS current_status,
           current_snapshot.previous_snapshot_id AS current_previous_snapshot_id,
           current_snapshot.manifest AS current_manifest,
           current_internal_run.options -> 'invalid_price_exclusion_policy'
               AS current_invalid_price_exclusion_policy,
           current_binding.snapshot_key AS current_snapshot_key,
           current_scope.plan_id AS current_plan_id,
           current_scope.plan_market_type AS current_plan_market_type,
           current_layout.state AS current_layout_state,
           current_layout.generation AS current_layout_generation,
           current_layout.mapping_digest AS current_layout_mapping_digest,
           current_layout.layout_manifest AS current_layout_manifest,
           current_v4_root.state AS current_v4_root_state,
           current_v4_root.map_digest AS current_v4_root_map_digest,
           current_attestation.report_digest AS current_audit_report_digest,
           current_attestation.report AS current_audit_report,
           current_attestation.activation_intent
               AS current_audit_activation_intent,
           current_attestation.activated_at AS current_audit_activated_at,
           current_frozen_binding.binding_payload
               AS current_frozen_binding_payload
      FROM {schema}.ptg2_snapshot AS snapshot
      JOIN {schema}.ptg2_v3_snapshot_binding AS binding
        ON binding.snapshot_id = snapshot.snapshot_id
      JOIN {schema}.ptg2_v3_snapshot_scope AS scope
        ON scope.snapshot_id = snapshot.snapshot_id
      JOIN {schema}.ptg2_v3_snapshot_layout AS layout
        ON layout.snapshot_key = binding.snapshot_key
      LEFT JOIN {schema}.ptg2_import_run AS internal_run
        ON internal_run.import_run_id = snapshot.import_run_id
      LEFT JOIN {schema}.ptg2_v4_snapshot_map_root AS v4_root
        ON v4_root.snapshot_key = layout.snapshot_key
      LEFT JOIN {schema}.ptg2_current_source_snapshot AS current_pointer
        ON current_pointer.source_key = lower(
            snapshot.manifest->'activation'->>'source_key'
        )
      LEFT JOIN {schema}.ptg2_snapshot AS current_snapshot
        ON current_snapshot.snapshot_id = current_pointer.snapshot_id
      LEFT JOIN {schema}.ptg2_import_run AS current_internal_run
        ON current_internal_run.import_run_id =
           current_snapshot.import_run_id
      LEFT JOIN {schema}.ptg2_v3_snapshot_binding AS current_binding
        ON current_binding.snapshot_id = current_snapshot.snapshot_id
      LEFT JOIN {schema}.ptg2_v3_snapshot_scope AS current_scope
        ON current_scope.snapshot_id = current_snapshot.snapshot_id
      LEFT JOIN {schema}.ptg2_v3_snapshot_layout AS current_layout
        ON current_layout.snapshot_key = current_binding.snapshot_key
      LEFT JOIN {schema}.ptg2_v4_snapshot_map_root AS current_v4_root
        ON current_v4_root.snapshot_key = current_layout.snapshot_key
      LEFT JOIN {schema}.ptg2_v3_candidate_audit_attestation AS attestation
        ON attestation.snapshot_id = snapshot.snapshot_id
       AND attestation.contract = ANY(CAST(:supported_contracts AS text[]))
      LEFT JOIN {schema}.ptg2_v3_candidate_audit_attestation AS current_attestation
        ON current_attestation.snapshot_id = current_snapshot.snapshot_id
       AND current_attestation.contract = ANY(
           CAST(:supported_contracts AS text[])
       )
      LEFT JOIN {schema}.ptg2_frozen_source_file_binding AS frozen_binding
        ON frozen_binding.internal_run_id = snapshot.import_run_id
      LEFT JOIN {schema}.ptg2_frozen_source_file_binding
           AS current_frozen_binding
        ON current_frozen_binding.internal_run_id =
           current_snapshot.import_run_id
     WHERE snapshot.import_run_id = :candidate_run_id
     ORDER BY snapshot.snapshot_id
"""


def _candidate_audit_mode(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in CANDIDATE_AUDIT_MODES:
        raise ValueError("candidate_audit_mode is unsupported")
    return normalized


@dataclass(frozen=True)
class CandidateAuditTarget:
    candidate_run_id: str
    snapshot_id: str
    snapshot_status: str
    snapshot_key: int
    source_key: str
    plan_id: str
    plan_market_type: str
    expected_current_snapshot_id: str | None
    current_snapshot_id: str | None
    raw_container_sha256: tuple[str, ...]
    provider_identifier_quarantine: Mapping[str, Any]
    source_witness: Mapping[str, Any]
    audit_sample: Mapping[str, Any]
    activated: bool
    storage_generation: str = STORAGE_GENERATION
    audit_report: Mapping[str, Any] | None = None
    audit_report_digest: str | None = None
    equivalent_current_snapshot_id: str | None = None
    equivalent_current_import_run_id: str | None = None
    equivalent_audit_report: Mapping[str, Any] | None = None
    equivalent_audit_report_digest: str | None = None
    frozen_candidate_identity: str | None = None


@dataclass(frozen=True)
class _SealedCandidateEvidence:
    snapshot_id: str
    activation_by_name: Mapping[str, Any]
    provider_identifier_quarantine: Mapping[str, Any]
    source_witness: Mapping[str, Any]
    audit_sample: Mapping[str, Any]
    storage_generation: str
    frozen_candidate_identity: str | None


@dataclass(frozen=True)
class _CandidateActivationState:
    source_key: str
    plan_id: str
    plan_market_type: str
    expected_current_snapshot_id: str | None
    current_snapshot_id: str | None
    snapshot_status: str
    is_activated: bool
    audit_report: Mapping[str, Any] | None
    audit_report_digest: str | None


@dataclass(frozen=True)
class _CandidateManifestState:
    snapshot_id: str
    manifest_by_name: Mapping[str, Any]
    serving_index_by_name: Mapping[str, Any]
    activation_by_name: Mapping[str, Any]
    layout_serving_index_by_name: Mapping[str, Any]


@dataclass(frozen=True)
class _CandidateScopeState:
    source_key: str
    plan_id: str
    plan_market_type: str
    expected_current_snapshot_id: str | None
    current_snapshot_id: str | None


class _CandidateAuditProcessError(RuntimeError):
    """Process failure retaining only authenticated partition diagnostics."""

    def __init__(
        self,
        message: str,
        *,
        partition_index: int | None = None,
        partition_count: int | None = None,
        partition_digest: str | None = None,
        plan_digest: str | None = None,
        request_digest: str | None = None,
    ) -> None:
        self.partition_index = partition_index
        self.partition_count = partition_count
        self.partition_digest = partition_digest
        self.plan_digest = plan_digest
        self.request_digest = request_digest
        diagnostic_suffix = (
            ""
            if partition_index is None
            else (
                f" [partition_index={partition_index}, "
                f"partition_count={partition_count}, "
                f"partition_digest={partition_digest}, "
                f"plan_digest={plan_digest}, request_digest={request_digest}]"
            )
        )
        super().__init__(f"{message}{diagnostic_suffix}")


class CandidateAuditReleaseGateError(_CandidateAuditProcessError):
    """A deterministic release-audit mismatch that must not be retried."""

    control_error_code = "ptg_candidate_audit_release_gate_failed"
    retryable = False


class CandidateAuditTransportError(_CandidateAuditProcessError):
    """An audit transport failure that requires an explicit retry."""

    control_error_code = "ptg_candidate_audit_transport_failed"
    retryable = False


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _row_mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(getattr(row, "_mapping", row))


def _normalized_digest(value: Any, *, field: str) -> str:
    if isinstance(value, (bytes, bytearray, memoryview)):
        normalized = bytes(value).hex()
    else:
        normalized = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise ValueError(f"candidate {field} is invalid")
    return normalized


def _candidate_import_id(candidate_run_id: str) -> str:
    scoped_import_id = candidate_run_id.removeprefix("ptg2:")
    return _REBUILD_RUN_SUFFIX_RE.sub("", scoped_import_id)


def _validate_corroboration(
    *,
    candidate_run_id: str,
    observed_snapshot_id: str,
    snapshot_id: str | None,
    import_id: str | None,
) -> None:
    expected_snapshot_id = str(snapshot_id or "").strip()
    if expected_snapshot_id and expected_snapshot_id != observed_snapshot_id:
        raise ValueError("snapshot_id does not corroborate candidate_run_id")
    expected_import_id = str(import_id or "").strip()
    if expected_import_id and expected_import_id not in {
        candidate_run_id,
        _candidate_import_id(candidate_run_id),
    }:
        raise ValueError("import_id does not corroborate candidate_run_id")


async def _candidate_rows(candidate_run_id: str) -> list[dict[str, Any]]:
    """Load one candidate and the current source pointer in the same query."""

    schema = _quote_ident(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    candidate_rows = await db.all(
        _CANDIDATE_TARGET_SQL.format(schema=schema),
        candidate_run_id=candidate_run_id,
        supported_contracts=list(
            PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS
        ),
    )
    return [_row_mapping(candidate_row) for candidate_row in candidate_rows]


class _CandidateRawSources(tuple[str, ...]):
    """Tuple-compatible raw hashes carrying detailed DB corroboration."""

    source_records: tuple[dict[str, Any], ...]

    def __new__(
        cls,
        raw_digest_values: Sequence[str],
        source_records: Sequence[Mapping[str, Any]],
    ):
        instance = super().__new__(cls, tuple(raw_digest_values))
        instance.source_records = tuple(
            dict(source_record) for source_record in source_records
        )
        return instance


async def _candidate_raw_sources(snapshot_id: str) -> tuple[str, ...]:
    """Load dense candidate sources with their complete version identities."""

    schema = _quote_ident(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    database_source_rows = await db.all(
        CANDIDATE_SOURCE_RECORDS_SQL.format(schema=schema),
        snapshot_id=snapshot_id,
    )
    source_records = [
        _row_mapping(database_source_row)
        for database_source_row in database_source_rows
    ]
    try:
        source_ordinals = [
            int(source_record.get("source_key"))
            for source_record in source_records
        ]
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "candidate source scope contains an invalid ordinal"
        ) from exc
    if source_ordinals != list(range(len(source_records))):
        raise ValueError("candidate source scope is not dense")
    raw_digest_values = tuple(
        _normalized_digest(
            source_record.get("raw_container_sha256"),
            field="raw container digest",
        )
        for source_record in source_records
    )
    if not raw_digest_values:
        raise ValueError("candidate has no public raw source bindings")
    if len(raw_digest_values) != len(set(raw_digest_values)):
        raise ValueError("candidate raw source bindings are ambiguous")
    return _CandidateRawSources(raw_digest_values, source_records)


def _validated_candidate_quarantine(
    serving_index_by_name: Mapping[str, Any],
    layout_serving_index_by_name: Mapping[str, Any],
) -> Mapping[str, Any]:
    try:
        quarantine_by_name = validate_provider_identifier_quarantine(
            serving_index_by_name.get("provider_identifier_quarantine")
        )
        layout_quarantine_by_name = (
            validate_provider_identifier_quarantine(
                layout_serving_index_by_name.get(
                    "provider_identifier_quarantine"
                )
            )
        )
    except ValueError as exc:
        raise ValueError(
            "candidate provider identifier quarantine is invalid"
        ) from exc
    if quarantine_by_name != layout_quarantine_by_name:
        raise ValueError(
            "candidate provider identifier quarantine changed after "
            "layout sealing"
        )
    return quarantine_by_name


def _validated_candidate_witness(
    serving_index_by_name: Mapping[str, Any],
    layout_serving_index_by_name: Mapping[str, Any],
    raw_container_sha256: tuple[str, ...],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    source_set_by_name = _mapping(
        serving_index_by_name.get("source_set")
    )
    source_witness_by_name = _mapping(
        serving_index_by_name.get("source_witness")
    )
    layout_witness_by_name = _mapping(
        layout_serving_index_by_name.get("source_witness")
    )
    audit_sample_by_name = _mapping(
        serving_index_by_name.get("audit_sample")
    )
    layout_sample_by_name = _mapping(
        layout_serving_index_by_name.get("audit_sample")
    )
    expected_digest = source_set_digest(raw_container_sha256)
    if (
        source_witness_by_name != layout_witness_by_name
        or audit_sample_by_name != layout_sample_by_name
        or source_witness_by_name.get("contract")
        != PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT
        or int(source_witness_by_name.get("source_count") or -1)
        != len(raw_container_sha256)
        or source_witness_by_name.get("source_set_digest")
        != expected_digest
        or source_set_by_name.get("raw_container_sha256_digest")
        != expected_digest
    ):
        raise ValueError(
            "candidate source witness changed after layout sealing"
        )
    return source_witness_by_name, audit_sample_by_name


def _validated_frozen_candidate_identity(
    manifest_by_name: Mapping[str, Any],
    candidate_row: Mapping[str, Any],
    *,
    candidate_run_id: str,
    raw_container_sha256: tuple[str, ...],
) -> str | None:
    raw_database_binding = candidate_row.get("frozen_binding_payload")
    database_binding = (
        None if raw_database_binding is None else _mapping(raw_database_binding)
    )
    if raw_database_binding is not None and not database_binding:
        raise ValueError("candidate frozen source-file binding changed")
    try:
        return validate_frozen_candidate_evidence(
            manifest_by_name,
            candidate_run_id=candidate_run_id,
            database_binding=database_binding,
            database_sources=getattr(
                raw_container_sha256,
                "source_records",
                None,
            ),
        )
    except (
        FrozenRateFileMismatchError,
        FrozenRateFileValidationError,
    ) as exc:
        raise CandidateAuditReleaseGateError(str(exc)) from exc


def _candidate_manifest_state(
    candidate_row: Mapping[str, Any],
    *,
    candidate_run_id: str,
) -> _CandidateManifestState:
    observed_run_id = str(candidate_row.get("import_run_id") or "").strip()
    if observed_run_id != candidate_run_id:
        raise ValueError("candidate run binding changed during resolution")
    manifest_by_name = _mapping(candidate_row.get("manifest"))
    layout_manifest_by_name = _mapping(candidate_row.get("layout_manifest"))
    return _CandidateManifestState(
        snapshot_id=str(candidate_row.get("snapshot_id") or "").strip(),
        manifest_by_name=manifest_by_name,
        serving_index_by_name=_mapping(
            manifest_by_name.get("serving_index")
        ),
        activation_by_name=_mapping(manifest_by_name.get("activation")),
        layout_serving_index_by_name=_mapping(
            layout_manifest_by_name.get("serving_index")
        ),
    )


def _sealed_candidate_evidence(
    candidate_row: Mapping[str, Any],
    *,
    candidate_run_id: str,
    raw_container_sha256: tuple[str, ...],
) -> _SealedCandidateEvidence:
    manifest_state = _candidate_manifest_state(
        candidate_row, candidate_run_id=candidate_run_id
    )
    quarantine_by_name = _validated_candidate_quarantine(
        manifest_state.serving_index_by_name,
        manifest_state.layout_serving_index_by_name,
    )
    source_witness_by_name, audit_sample_by_name = (
        _validated_candidate_witness(
            manifest_state.serving_index_by_name,
            manifest_state.layout_serving_index_by_name,
            raw_container_sha256,
        )
    )
    storage_generation = validate_candidate_layout_identity(
        candidate_row,
        manifest_state.serving_index_by_name,
        manifest_state.layout_serving_index_by_name,
    )
    frozen_candidate_identity = _validated_frozen_candidate_identity(
        manifest_state.manifest_by_name,
        candidate_row,
        candidate_run_id=candidate_run_id,
        raw_container_sha256=raw_container_sha256,
    )
    binding_by_name = _mapping(candidate_row.get("frozen_binding_payload"))
    invalid_price_policy = validated_candidate_invalid_price_exclusion_policy(
        candidate_row.get(INVALID_PRICE_EXCLUSION_POLICY_FIELD),
        binding_by_name or None,
        raw_container_sha256,
    )
    validate_candidate_invalid_price_exclusion_evidence(
        invalid_price_policy,
        manifest_state.serving_index_by_name.get("invalid_price_exclusion"),
        manifest_state.layout_serving_index_by_name.get("invalid_price_exclusion"),
        raw_container_sha256,
    )
    if (
        not manifest_state.snapshot_id
        or manifest_state.activation_by_name.get("contract")
        != PTG2_CANDIDATE_ACTIVATION_CONTRACT
    ):
        raise ValueError("candidate is not an exact strict shared snapshot")
    return _SealedCandidateEvidence(
        snapshot_id=manifest_state.snapshot_id,
        activation_by_name=manifest_state.activation_by_name,
        provider_identifier_quarantine=quarantine_by_name,
        source_witness=source_witness_by_name,
        audit_sample=audit_sample_by_name,
        storage_generation=storage_generation,
        frozen_candidate_identity=frozen_candidate_identity,
    )


def _candidate_scope_state(
    candidate_row: Mapping[str, Any],
    activation_by_name: Mapping[str, Any],
) -> _CandidateScopeState:
    source_key = str(
        activation_by_name.get("source_key") or ""
    ).strip().lower()
    plan_id = str(candidate_row.get("plan_id") or "").strip()
    plan_market_type = str(
        candidate_row.get("plan_market_type") or ""
    ).strip().lower()
    expected_current = str(
        activation_by_name.get("expected_previous_snapshot_id") or ""
    ).strip() or None
    row_previous = str(
        candidate_row.get("previous_snapshot_id") or ""
    ).strip() or None
    current_snapshot = str(
        candidate_row.get("current_snapshot_id") or ""
    ).strip() or None
    if not source_key or not plan_id or not plan_market_type:
        raise ValueError("candidate public source scope is incomplete")
    if row_previous != expected_current:
        raise ValueError("candidate predecessor binding is inconsistent")
    return _CandidateScopeState(
        source_key=source_key,
        plan_id=plan_id,
        plan_market_type=plan_market_type,
        expected_current_snapshot_id=expected_current,
        current_snapshot_id=current_snapshot,
    )


def _validated_activation_status(
    candidate_row: Mapping[str, Any],
    evidence: _SealedCandidateEvidence,
    scope_state: _CandidateScopeState,
    *,
    allow_superseded: bool,
) -> tuple[str, bool]:
    status = str(candidate_row.get("status") or "").strip().lower()
    activation_state = str(
        evidence.activation_by_name.get("state") or ""
    ).strip().lower()
    is_activated = status == "published" and activation_state == "activated"
    if is_activated:
        expected_intent_by_mode = {
            "audited_control": (
                PTG2_CANDIDATE_ACTIVATION_INTENT_AUDIT_AND_ACTIVATE
            ),
            "reviewed_audit_only_control": (
                PTG2_CANDIDATE_ACTIVATION_INTENT_AUDIT_ONLY
            ),
        }
        activation_mode = str(
            evidence.activation_by_name.get("mode") or ""
        ).strip()
        activation_intent = str(
            candidate_row.get("audit_activation_intent") or ""
        ).strip()
        if (
            expected_intent_by_mode.get(activation_mode)
            != activation_intent
            or scope_state.current_snapshot_id != evidence.snapshot_id
            or candidate_row.get("audit_activated_at") is None
        ):
            raise ValueError("activated candidate cannot be corroborated")
    elif (
        status != "validated"
        or activation_state != "validated"
        or scope_state.current_snapshot_id == evidence.snapshot_id
        or (
            scope_state.current_snapshot_id
            != scope_state.expected_current_snapshot_id
            and (
                not allow_superseded
                or scope_state.current_snapshot_id is None
            )
        )
    ):
        raise ValueError(
            "candidate is not validated with deferred activation"
        )
    return status, is_activated


def _candidate_audit_report(
    candidate_row: Mapping[str, Any],
    *,
    is_activated: bool,
) -> tuple[Mapping[str, Any] | None, str | None]:
    audit_report = (
        _mapping(candidate_row.get("audit_report"))
        if is_activated
        else None
    )
    audit_report_digest = (
        _normalized_digest(
            candidate_row.get("audit_report_digest"),
            field="audit report digest",
        )
        if is_activated
        else None
    )
    if is_activated and not audit_report:
        raise ValueError(
            "activated candidate has no corroborating audit report"
        )
    return audit_report, audit_report_digest


def _candidate_activation_state(
    candidate_row: Mapping[str, Any],
    evidence: _SealedCandidateEvidence,
    *,
    allow_superseded: bool,
) -> _CandidateActivationState:
    scope_state = _candidate_scope_state(
        candidate_row,
        evidence.activation_by_name,
    )
    status, is_activated = _validated_activation_status(
        candidate_row,
        evidence,
        scope_state,
        allow_superseded=allow_superseded,
    )
    audit_report, audit_report_digest = _candidate_audit_report(
        candidate_row,
        is_activated=is_activated,
    )
    return _CandidateActivationState(
        source_key=scope_state.source_key,
        plan_id=scope_state.plan_id,
        plan_market_type=scope_state.plan_market_type,
        expected_current_snapshot_id=(
            scope_state.expected_current_snapshot_id
        ),
        current_snapshot_id=scope_state.current_snapshot_id,
        snapshot_status=status,
        is_activated=is_activated,
        audit_report=audit_report,
        audit_report_digest=audit_report_digest,
    )


def _candidate_target_from_row(
    candidate_row: Mapping[str, Any],
    *,
    candidate_run_id: str,
    raw_container_sha256: tuple[str, ...],
    allow_superseded: bool = False,
) -> CandidateAuditTarget:
    """Validate a resolved candidate row and return its exact audit target."""

    evidence = _sealed_candidate_evidence(
        candidate_row,
        candidate_run_id=candidate_run_id,
        raw_container_sha256=raw_container_sha256,
    )
    activation_state = _candidate_activation_state(
        candidate_row,
        evidence,
        allow_superseded=allow_superseded,
    )
    return CandidateAuditTarget(
        candidate_run_id=candidate_run_id,
        snapshot_id=evidence.snapshot_id,
        snapshot_status=activation_state.snapshot_status,
        snapshot_key=int(candidate_row["snapshot_key"]),
        source_key=activation_state.source_key,
        plan_id=activation_state.plan_id,
        plan_market_type=activation_state.plan_market_type,
        expected_current_snapshot_id=(
            activation_state.expected_current_snapshot_id
        ),
        current_snapshot_id=activation_state.current_snapshot_id,
        raw_container_sha256=raw_container_sha256,
        provider_identifier_quarantine=(
            evidence.provider_identifier_quarantine
        ),
        source_witness=evidence.source_witness,
        audit_sample=evidence.audit_sample,
        activated=activation_state.is_activated,
        storage_generation=evidence.storage_generation,
        audit_report=activation_state.audit_report,
        audit_report_digest=activation_state.audit_report_digest,
        frozen_candidate_identity=evidence.frozen_candidate_identity,
    )


def _current_snapshot_row(candidate_row: Mapping[str, Any]) -> dict[str, Any] | None:
    snapshot_id = str(candidate_row.get("current_snapshot_id") or "").strip()
    if not snapshot_id:
        return None
    return {
        "snapshot_id": snapshot_id,
        "import_run_id": candidate_row.get("current_import_run_id"),
        "status": candidate_row.get("current_status"),
        "previous_snapshot_id": candidate_row.get(
            "current_previous_snapshot_id"
        ),
        "manifest": candidate_row.get("current_manifest"),
        "invalid_price_exclusion_policy": candidate_row.get(
            "current_invalid_price_exclusion_policy"
        ),
        "snapshot_key": candidate_row.get("current_snapshot_key"),
        "plan_id": candidate_row.get("current_plan_id"),
        "plan_market_type": candidate_row.get("current_plan_market_type"),
        "layout_state": candidate_row.get("current_layout_state"),
        "layout_generation": candidate_row.get("current_layout_generation"),
        "layout_mapping_digest": candidate_row.get(
            "current_layout_mapping_digest"
        ),
        "layout_manifest": candidate_row.get("current_layout_manifest"),
        "v4_root_state": candidate_row.get("current_v4_root_state"),
        "v4_root_map_digest": candidate_row.get(
            "current_v4_root_map_digest"
        ),
        "current_snapshot_id": snapshot_id,
        "audit_report_digest": candidate_row.get(
            "current_audit_report_digest"
        ),
        "audit_report": candidate_row.get("current_audit_report"),
        "audit_activation_intent": candidate_row.get(
            "current_audit_activation_intent"
        ),
        "audit_activated_at": candidate_row.get("current_audit_activated_at"),
        "frozen_binding_payload": candidate_row.get(
            "current_frozen_binding_payload"
        ),
    }


async def _reuse_equivalent_current_target(
    candidate_row: Mapping[str, Any],
    candidate_target: CandidateAuditTarget,
) -> CandidateAuditTarget:
    current_row = _current_snapshot_row(candidate_row)
    if current_row is None:
        raise ValueError("candidate is not validated with deferred activation")
    current_run_id = str(current_row.get("import_run_id") or "").strip()
    current_snapshot_id = str(current_row.get("snapshot_id") or "").strip()
    if not current_run_id or not current_snapshot_id:
        raise ValueError("candidate was superseded by an invalid snapshot")
    current_raw_digests = await _candidate_raw_sources(current_snapshot_id)
    current_target = _candidate_target_from_row(
        current_row,
        candidate_run_id=current_run_id,
        raw_container_sha256=current_raw_digests,
    )
    equivalent_identity = (
        candidate_target.snapshot_key,
        candidate_target.storage_generation,
        candidate_target.source_key,
        candidate_target.plan_id,
        candidate_target.plan_market_type,
        candidate_target.raw_container_sha256,
        dict(candidate_target.provider_identifier_quarantine),
        dict(candidate_target.source_witness),
        dict(candidate_target.audit_sample),
        candidate_target.frozen_candidate_identity,
    )
    current_identity = (
        current_target.snapshot_key,
        current_target.storage_generation,
        current_target.source_key,
        current_target.plan_id,
        current_target.plan_market_type,
        current_target.raw_container_sha256,
        dict(current_target.provider_identifier_quarantine),
        dict(current_target.source_witness),
        dict(current_target.audit_sample),
        current_target.frozen_candidate_identity,
    )
    if not current_target.activated or equivalent_identity != current_identity:
        raise ValueError("candidate was superseded by a non-equivalent snapshot")
    if (
        current_target.audit_report is None
        or current_target.audit_report_digest is None
    ):
        raise ValueError("equivalent current snapshot has no audit attestation")
    return replace(
        candidate_target,
        equivalent_current_snapshot_id=current_target.snapshot_id,
        equivalent_current_import_run_id=current_target.candidate_run_id,
        equivalent_audit_report=current_target.audit_report,
        equivalent_audit_report_digest=current_target.audit_report_digest,
    )


async def load_candidate_audit_target(
    *,
    candidate_run_id: str,
    snapshot_id: str | None = None,
    import_id: str | None = None,
) -> CandidateAuditTarget:
    """Derive one exact candidate solely from public PostgreSQL snapshot state."""

    normalized_run_id = str(candidate_run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("candidate_run_id is required")
    candidate_rows = await _candidate_rows(normalized_run_id)
    if not candidate_rows:
        raise ValueError("candidate_run_id did not resolve a candidate")
    if len(candidate_rows) != 1:
        raise ValueError("candidate_run_id does not resolve exactly one candidate")
    observed_snapshot_id = str(
        candidate_rows[0].get("snapshot_id") or ""
    ).strip()
    _validate_corroboration(
        candidate_run_id=normalized_run_id,
        observed_snapshot_id=observed_snapshot_id,
        snapshot_id=snapshot_id,
        import_id=import_id,
    )
    raw_digests = await _candidate_raw_sources(observed_snapshot_id)
    candidate_target = _candidate_target_from_row(
        candidate_rows[0],
        candidate_run_id=normalized_run_id,
        raw_container_sha256=raw_digests,
        allow_superseded=True,
    )
    if (
        not candidate_target.activated
        and candidate_target.current_snapshot_id
        != candidate_target.expected_current_snapshot_id
    ):
        return await _reuse_equivalent_current_target(
            candidate_rows[0], candidate_target
        )
    return candidate_target


def _audit_configuration(
    snapshot_id: str,
    *,
    batch_writer: bool = PTG2_BATCH_AUDIT_WRITER_ENABLED,
) -> FastAuditHttpConfig:
    api_base_url = str(
        os.getenv(API_BASE_URL_ENV) or os.getenv("PTG_AUDIT_API_BASE_URL") or ""
    ).strip().rstrip("/")
    token = str(os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    auth_header = str(os.getenv(AUTH_HEADER_ENV) or "Authorization").strip()
    default_scheme = "Bearer" if auth_header.lower() == "authorization" else ""
    auth_scheme = str(os.getenv(AUTH_SCHEME_ENV, default_scheme)).strip()
    trusted_cluster_http_text = str(
        os.getenv(TRUSTED_CLUSTER_HTTP_ENV, "false")
    ).strip().lower()
    if not api_base_url:
        raise ValueError(f"{API_BASE_URL_ENV} is required")
    if not token:
        raise ValueError("HLTHPRT_CONTROL_API_TOKEN is required")
    if not auth_header or any(character in auth_header for character in "\r\n"):
        raise ValueError("candidate audit auth header is invalid")
    if trusted_cluster_http_text not in {"true", "false"}:
        raise ValueError(f"{TRUSTED_CLUSTER_HTTP_ENV} must be true or false")
    parsed = urlsplit(api_base_url)
    is_trusted_cluster_http = trusted_cluster_http_text == "true"
    if parsed.scheme == "https" and parsed.netloc:
        should_verify_tls = True
        transport_contract = PTG2_VERIFIED_HTTPS_TRANSPORT
    elif (
        is_trusted_cluster_http
        and ptg2_v3_source_api_audit._is_cluster_http_api_origin(api_base_url)
    ):
        should_verify_tls = False
        transport_contract = PTG2_TRUSTED_CLUSTER_HTTP_TRANSPORT
    else:
        raise ValueError(
            "candidate audit requires verified HTTPS or explicit cluster HTTP"
        )
    token_value = f"{auth_scheme} {token}".strip()
    return FastAuditHttpConfig(
        api_base_url=api_base_url,
        headers={
            auth_header: token_value,
            ptg2_v3_source_api_audit.CANDIDATE_AUDIT_HEADER: snapshot_id,
            "Accept": "application/json",
            "User-Agent": (
                "ptg2-v3-partitioned-candidate-audit/4.1"
                if batch_writer
                else "ptg2-v3-fast-candidate-audit/1.0"
            ),
        },
        verify_tls=should_verify_tls,
        transport_contract=transport_contract,
        concurrency=(
            PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_IN_FLIGHT
            if batch_writer
            else 32
        ),
    )


def _fast_audit_target(
    candidate_target: CandidateAuditTarget,
) -> FastAuditTarget:
    """Build the legacy writer target used during the reader-first phase."""

    return FastAuditTarget(
        snapshot_id=candidate_target.snapshot_id,
        source_key=candidate_target.source_key,
        plan_id=candidate_target.plan_id,
        plan_market_type=candidate_target.plan_market_type,
        source_count=len(candidate_target.raw_container_sha256),
        source_set_digest=source_set_digest(
            candidate_target.raw_container_sha256
        ),
        audit_sample=candidate_target.audit_sample,
        provider_identifier_quarantine=(
            candidate_target.provider_identifier_quarantine
        ),
    )


def _require_passing_audit_report(audit_report: Mapping[str, Any]) -> None:
    """Reject a report that did not pass its strict release profile."""

    if (
        audit_report.get("status") != "pass"
        or audit_report.get("release_gate_eligible") is not True
    ):
        raise CandidateAuditReleaseGateError(
            "candidate release audit did not pass the release gate"
        )


def _require_v3_quarantine_match(
    audit_report: Mapping[str, Any],
    candidate_target: CandidateAuditTarget,
) -> None:
    """Bind a legacy full quarantine payload to the candidate."""

    report_source = _mapping(audit_report.get("source"))
    try:
        observed_quarantine = validate_provider_identifier_quarantine(
            report_source.get("provider_identifier_quarantine")
        )
    except ValueError as exc:
        raise CandidateAuditReleaseGateError(
            "candidate release audit has invalid provider identifier quarantine evidence"
        ) from exc
    if observed_quarantine != dict(
        candidate_target.provider_identifier_quarantine
    ):
        raise CandidateAuditReleaseGateError(
            "candidate release audit provider identifier quarantine does not match publication"
        )


def _require_v4_quarantine_match(
    audit_report: Mapping[str, Any],
    candidate_target: CandidateAuditTarget,
) -> None:
    """Bind redacted quarantine evidence to the candidate."""

    report_source = _mapping(audit_report.get("source"))
    try:
        observed_quarantine = validate_provider_identifier_quarantine_evidence(
            report_source.get("provider_identifier_quarantine")
        )
    except ValueError as exc:
        raise CandidateAuditReleaseGateError(
            "candidate release audit has invalid provider identifier quarantine evidence"
        ) from exc
    expected_quarantine = provider_identifier_quarantine_evidence(
        candidate_target.provider_identifier_quarantine
    )
    if observed_quarantine != expected_quarantine:
        raise CandidateAuditReleaseGateError(
            "candidate release audit provider identifier quarantine does not match publication"
        )


async def run_release_audit(
    candidate_target: CandidateAuditTarget,
    witness: Any,
    *,
    http_config: FastAuditHttpConfig | None = None,
) -> dict[str, Any]:
    """Run the deployed V3 writer while V4-capable readers roll out."""

    try:
        audit_report = await run_fast_candidate_audit(
            witness=witness,
            audit_target=_fast_audit_target(candidate_target),
            http=(
                http_config
                if http_config is not None
                else _audit_configuration(
                    candidate_target.snapshot_id,
                    batch_writer=False,
                )
            ),
        )
    except FastCandidateAuditError as exc:
        raise CandidateAuditReleaseGateError(
            f"candidate release audit failed: {exc.reason}"
        ) from exc
    _require_v3_quarantine_match(audit_report, candidate_target)
    _require_passing_audit_report(audit_report)
    return audit_report


async def run_batch_release_audit(
    candidate_target: CandidateAuditTarget,
    *,
    http_config: FastAuditHttpConfig | None = None,
    progress_callback: Callable[[int, int], Awaitable[None]] | None = None,
    failure_callback: PartitionFailureCallback | None = None,
) -> dict[str, Any]:
    """Load sealed evidence once, then run bounded API partitions."""

    try:
        witness, persisted_sample = await _load_partitioned_audit_evidence(
            candidate_target
        )
        audit_report = await run_partitioned_candidate_audit(
            audit_target=_partitioned_audit_target(candidate_target),
            witness=witness,
            persisted_sample=persisted_sample,
            progress_callback=progress_callback,
            failure_callback=failure_callback,
            http_config=(
                http_config
                if http_config is not None
                else _audit_configuration(
                    candidate_target.snapshot_id,
                    batch_writer=True,
                )
            ),
        )
    except BatchCandidateAuditContractError as exc:
        raise CandidateAuditReleaseGateError(
            f"candidate release audit failed: {exc.reason}",
            partition_index=exc.partition_index,
            partition_count=exc.partition_count,
            partition_digest=exc.partition_digest,
            plan_digest=exc.plan_digest,
            request_digest=exc.request_digest,
        ) from exc
    except BatchCandidateAuditTransportError as exc:
        raise CandidateAuditTransportError(
            f"candidate release audit transport failed: {exc.reason}",
            partition_index=exc.partition_index,
            partition_count=exc.partition_count,
            partition_digest=exc.partition_digest,
            plan_digest=exc.plan_digest,
            request_digest=exc.request_digest,
        ) from exc
    _require_v4_quarantine_match(audit_report, candidate_target)
    _require_passing_audit_report(audit_report)
    return audit_report


async def _load_partitioned_audit_evidence(
    candidate_target: CandidateAuditTarget,
) -> tuple[Any, Any]:
    """Load the sealed source and persisted samples for one V4 audit."""

    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    witness = await load_shared_source_witness(
        schema_name=schema_name,
        snapshot_key=candidate_target.snapshot_key,
        expected_raw_source_sha256=candidate_target.raw_container_sha256,
        expected_metadata=candidate_target.source_witness,
    )
    persisted_sample = await load_persisted_audit_sample(
        schema_name=schema_name,
        snapshot_key=candidate_target.snapshot_key,
        expected_metadata=candidate_target.audit_sample,
    )
    return witness, persisted_sample


def _partitioned_audit_target(
    candidate_target: CandidateAuditTarget,
) -> BatchAuditReportTarget:
    """Shape the immutable report target for partition execution."""

    return BatchAuditReportTarget(
        snapshot_id=candidate_target.snapshot_id,
        source_key=candidate_target.source_key,
        plan_id=candidate_target.plan_id,
        plan_market_type=candidate_target.plan_market_type,
        raw_container_sha256=candidate_target.raw_container_sha256,
        source_witness=candidate_target.source_witness,
        audit_sample=candidate_target.audit_sample,
        provider_identifier_quarantine=(
            candidate_target.provider_identifier_quarantine
        ),
        storage_generation=candidate_target.storage_generation,
    )


def _integer_metrics(mapping: Mapping[str, Any], keys: Sequence[str]) -> dict[str, int]:
    return {
        key: value
        for key in keys
        if isinstance((value := mapping.get(key)), int) and not isinstance(value, bool)
    }


def _audit_summary(report: Mapping[str, Any], report_digest: str) -> dict[str, Any]:
    checks = _mapping(report.get("checks"))
    http = _mapping(report.get("http"))
    counts = _integer_metrics(
        checks,
        (
            "source_witnesses",
            "api_witnesses_matched",
            "api_challenges_executed",
            "provider_witnesses_validated",
            "api_audit_occurrences_validated",
            "source_occurrence_witnesses_matched",
            "unique_source_conditions_executed",
            "persisted_audit_occurrences_validated",
            "batch_requests_executed",
        ),
    )
    counts.update(
        _integer_metrics(
            http,
            (
                "standard_api_actual_http_requests",
                "batch_api_planned_http_requests",
                "batch_api_actual_http_requests",
                "batch_api_completed_http_requests",
                "batch_api_failed_http_requests",
                "retry_count",
                "max_concurrency",
            ),
        )
    )
    audit_timings_by_metric: dict[str, Any] = {}
    duration = report.get("duration_seconds")
    if isinstance(duration, (int, float)) and not isinstance(duration, bool):
        audit_timings_by_metric["duration_seconds"] = float(duration)
    latency = _mapping(report.get("latency"))
    for output_key, source_key in (
        ("request_p50_ms", "request_p50_ms"),
        ("request_p95_ms", "request_p95_ms"),
        ("request_max_ms", "request_max_ms"),
    ):
        p95_milliseconds = latency.get(source_key)
        if isinstance(p95_milliseconds, (int, float)) and not isinstance(
            p95_milliseconds, bool
        ):
            audit_timings_by_metric[output_key] = float(p95_milliseconds)
    batch = _mapping(report.get("batch"))
    endpoint_duration_ms = batch.get("endpoint_duration_ms")
    if isinstance(endpoint_duration_ms, (int, float)) and not isinstance(
        endpoint_duration_ms, bool
    ):
        audit_timings_by_metric["endpoint_duration_ms"] = float(
            endpoint_duration_ms
        )
    return {
        "audit_report_digest": report_digest,
        "audit_counts": counts,
        "audit_timings": audit_timings_by_metric,
    }


def _success_result(
    candidate_audit_target: CandidateAuditTarget,
    *,
    report: Mapping[str, Any],
    report_digest: str,
    idempotent: bool,
) -> dict[str, Any]:
    summary = _audit_summary(report, report_digest)
    is_equivalent_reuse = (
        candidate_audit_target.equivalent_current_snapshot_id is not None
    )
    active_snapshot_id = (
        candidate_audit_target.equivalent_current_snapshot_id
        or candidate_audit_target.snapshot_id
    )
    active_import_run_id = (
        candidate_audit_target.equivalent_current_import_run_id
        or candidate_audit_target.candidate_run_id
    )
    audit_metrics_by_name = {
        "arch_version": ARCH_VERSION,
        "storage_generation": candidate_audit_target.storage_generation,
        "snapshot_status": "published",
        "activation_status": "activated",
        "snapshot_id": active_snapshot_id,
        "candidate_snapshot_id": candidate_audit_target.snapshot_id,
        "import_run_id": candidate_audit_target.candidate_run_id,
        "candidate_run_id": candidate_audit_target.candidate_run_id,
        "activated_import_run_id": active_import_run_id,
        "activation_mode": (
            "equivalent_current_layout"
            if is_equivalent_reuse
            else "audited_control"
        ),
        "equivalent_reuse": is_equivalent_reuse,
        "idempotent": idempotent,
        **summary,
    }
    return {
        **audit_metrics_by_name,
        "metrics": dict(audit_metrics_by_name),
    }


def _audit_only_result(
    candidate_audit_target: CandidateAuditTarget,
    *,
    report: Mapping[str, Any],
    report_digest: str,
    attestation: Mapping[str, Any],
    idempotent: bool = False,
) -> dict[str, Any]:
    summary = _audit_summary(report, report_digest)
    audit_counts = _mapping(summary.get("audit_counts"))
    terminal_count = next(
        (
            int(audit_counts[name])
            for name in (
                "batch_requests_executed",
                "api_challenges_executed",
                "source_witnesses",
            )
            if isinstance(audit_counts.get(name), int)
            and not isinstance(audit_counts.get(name), bool)
            and int(audit_counts[name]) >= 0
        ),
        0,
    )
    terminal_progress_by_field = {
        "unit": "audit_requests",
        "done": terminal_count,
        "total": terminal_count,
        "pct": 100,
        "message": "retained passing attestation without promotion",
        "phase": "candidate audit-only complete",
    }
    audit_metrics_by_name = {
        "arch_version": ARCH_VERSION,
        "storage_generation": candidate_audit_target.storage_generation,
        "snapshot_status": "validated",
        "activation_status": "deferred",
        "snapshot_id": candidate_audit_target.snapshot_id,
        "candidate_snapshot_id": candidate_audit_target.snapshot_id,
        "import_run_id": candidate_audit_target.candidate_run_id,
        "candidate_run_id": candidate_audit_target.candidate_run_id,
        "candidate_audit_mode": CANDIDATE_AUDIT_MODE_AUDIT_ONLY,
        "activation_mode": CANDIDATE_AUDIT_MODE_AUDIT_ONLY,
        "equivalent_reuse": False,
        "idempotent": idempotent,
        "attestation_status": str(attestation.get("status") or "attested"),
        "attestation_expires_at": attestation.get("expires_at"),
        "attestation_digest": attestation.get("attestation_digest"),
        "count": terminal_count,
        "terminal_progress": terminal_progress_by_field,
        **summary,
    }
    return {
        **audit_metrics_by_name,
        "metrics": dict(audit_metrics_by_name),
    }


async def _progress(
    run_id: str | None,
    *,
    snapshot_id: str | None,
    phase: str,
    message: str,
    pct: int,
) -> None:
    if not run_id:
        return
    await mark_control_run(
        run_id,
        status="running",
        phase_detail=phase,
        progress_message=message,
        snapshot_id=snapshot_id,
        progress={
            "unit": "phase",
            "done": pct,
            "total": 100,
            "pct": pct,
            "message": message,
            "phase": phase,
        },
    )


async def _partition_progress(
    run_id: str | None,
    *,
    snapshot_id: str | None,
    completed: int,
    total: int,
) -> None:
    """Publish exact audit counters within the release-audit progress band."""

    if not run_id:
        return
    bounded_total = max(int(total), 1)
    bounded_completed = min(max(int(completed), 0), bounded_total)
    pct = 20 + int((bounded_completed / bounded_total) * 64)
    message = (
        f"completed {bounded_completed:,} of {bounded_total:,} "
        "audit partitions"
    )
    await mark_control_run(
        run_id,
        status="running",
        phase_detail="candidate release audit",
        progress_message=message,
        snapshot_id=snapshot_id,
        progress={
            "unit": "partition",
            "done": bounded_completed,
            "total": bounded_total,
            "pct": pct,
            "message": message,
            "phase": "candidate release audit",
        },
    )


async def _partition_failure_progress(
    run_id: str | None,
    *,
    snapshot_id: str | None,
    completed: int,
    total: int,
    failure: (
        BatchCandidateAuditContractError
        | BatchCandidateAuditTransportError
    ),
) -> None:
    """Publish safe authenticated request identity for one failed partition."""

    if not run_id or failure.partition_index is None:
        return
    bounded_total = max(int(total), 1)
    bounded_completed = min(max(int(completed), 0), bounded_total)
    partition_index = int(failure.partition_index)
    message = (
        f"audit partition index {partition_index} failed after "
        f"{bounded_completed:,} of {bounded_total:,} completed"
    )
    await mark_control_run(
        run_id,
        status="running",
        phase_detail="candidate release audit",
        progress_message=message,
        snapshot_id=snapshot_id,
        progress={
            "unit": "partition",
            "done": bounded_completed,
            "total": bounded_total,
            "pct": 20 + int((bounded_completed / bounded_total) * 64),
            "message": message,
            "phase": "candidate release audit",
            "failed_partition_index": partition_index,
            "partition_count": failure.partition_count,
            "partition_digest": failure.partition_digest,
            "plan_digest": failure.plan_digest,
            "request_digest": failure.request_digest,
            "failure_reason": failure.reason,
        },
    )


@asynccontextmanager
async def candidate_audit_guard(candidate_run_id: str) -> AsyncIterator[None]:
    """Serialize duplicate audits for one candidate across workers and nodes."""

    if db.engine is None:
        await db.connect()
    assert db.engine is not None
    lock_name = f"ptg-candidate-audit:{candidate_run_id}"
    async with db.engine.connect() as connection:
        autocommit = connection.execution_options(isolation_level="AUTOCOMMIT")
        if hasattr(autocommit, "__await__"):
            autocommit = await autocommit
        acquired = await autocommit.scalar(
            db.text("SELECT pg_advisory_lock(hashtextextended(:lock_name, 0))"),
            {"lock_name": lock_name},
        )
        if acquired not in (None, True):
            raise RuntimeError("candidate audit database guard was not acquired")
        try:
            yield
        finally:
            with contextlib.suppress(Exception):
                await autocommit.scalar(
                    db.text("SELECT pg_advisory_unlock(hashtextextended(:lock_name, 0))"),
                    {"lock_name": lock_name},
                )


async def _execute_release_audit(
    candidate_target: CandidateAuditTarget,
    *,
    control_run_id: str | None,
    http_config: FastAuditHttpConfig | None,
) -> dict[str, Any]:
    """Run the configured writer after its compatible readers are deployed."""

    if PTG2_BATCH_AUDIT_WRITER_ENABLED:
        return await _execute_partitioned_release_audit(
            candidate_target,
            control_run_id=control_run_id,
            http_config=http_config,
        )
    return await _execute_rolling_release_audit(
        candidate_target,
        control_run_id=control_run_id,
        http_config=http_config,
    )


async def _execute_partitioned_release_audit(
    candidate_target: CandidateAuditTarget,
    *,
    control_run_id: str | None,
    http_config: FastAuditHttpConfig | None,
) -> dict[str, Any]:
    """Submit authenticated partitions with progress and failure diagnostics."""

    await _progress(
        control_run_id,
        snapshot_id=candidate_target.snapshot_id,
        phase="candidate release audit",
        message=(
            "submitting authenticated bounded API partitions for "
            f"{int(candidate_target.source_witness['occurrence_witness_count']):,} "
            "sealed source occurrences"
        ),
        pct=20,
    )
    return await run_batch_release_audit(
        candidate_target,
        http_config=http_config,
        progress_callback=lambda completed, total: _partition_progress(
            control_run_id,
            snapshot_id=candidate_target.snapshot_id,
            completed=completed,
            total=total,
        ),
        failure_callback=lambda completed, total, failure: (
            _partition_failure_progress(
                control_run_id,
                snapshot_id=candidate_target.snapshot_id,
                completed=completed,
                total=total,
                failure=failure,
            )
        ),
    )


async def _execute_rolling_release_audit(
    candidate_target: CandidateAuditTarget,
    *,
    control_run_id: str | None,
    http_config: FastAuditHttpConfig | None,
) -> dict[str, Any]:
    """Run the compatibility writer against one locally loaded witness."""

    await _progress(
        control_run_id,
        snapshot_id=candidate_target.snapshot_id,
        phase="candidate source validation",
        message="loading sealed source witnesses from PostgreSQL",
        pct=20,
    )
    witness = await load_shared_source_witness(
        schema_name=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
        snapshot_key=candidate_target.snapshot_key,
        expected_raw_source_sha256=candidate_target.raw_container_sha256,
        expected_metadata=candidate_target.source_witness,
    )
    await _progress(
        control_run_id,
        snapshot_id=candidate_target.snapshot_id,
        phase="candidate release audit",
        message=(
            f"auditing {len(witness.occurrence_records):,} sealed source occurrences "
            "through concurrent public API requests"
        ),
        pct=35,
    )
    return await run_release_audit(
        candidate_target,
        witness,
        http_config=http_config,
    )


async def _audit_and_activate(
    candidate_target: CandidateAuditTarget,
    *,
    control_run_id: str | None,
    http_config: FastAuditHttpConfig | None = None,
    candidate_audit_mode: str = CANDIDATE_AUDIT_MODE_AUDIT_AND_ACTIVATE,
) -> dict[str, Any]:
    """Audit sealed source witnesses, attest the report, and promote."""

    normalized_mode = _candidate_audit_mode(candidate_audit_mode)
    report = await _execute_release_audit(
        candidate_target,
        control_run_id=control_run_id,
        http_config=http_config,
    )
    attestation, report_digest = await _record_passing_attestation(
        candidate_target,
        report=report,
        control_run_id=control_run_id,
        activation_intent=normalized_mode,
    )
    if normalized_mode == CANDIDATE_AUDIT_MODE_AUDIT_ONLY:
        await _publish_audit_only_complete(
            control_run_id,
            snapshot_id=candidate_target.snapshot_id,
            replay=False,
        )
        return _audit_only_result(
            candidate_target,
            report=report,
            report_digest=report_digest,
            attestation=attestation,
            idempotent=False,
        )
    await _promote_audited_candidate(
        candidate_target,
        control_run_id=control_run_id,
    )
    return _success_result(
        candidate_target,
        report=report,
        report_digest=report_digest,
        idempotent=False,
    )


async def _record_passing_attestation(
    candidate_target: CandidateAuditTarget,
    *,
    report: Mapping[str, Any],
    control_run_id: str | None,
    activation_intent: str,
) -> tuple[dict[str, Any], str]:
    """Persist one passing report with its durable activation intent."""

    await _progress(
        control_run_id,
        snapshot_id=candidate_target.snapshot_id,
        phase="candidate attestation",
        message="recording passing audit attestation",
        pct=85,
    )
    attestation = await record_candidate_audit_attestation(
        snapshot_id=candidate_target.snapshot_id,
        source_key=candidate_target.source_key,
        plan_id=candidate_target.plan_id,
        plan_market_type=candidate_target.plan_market_type,
        storage_generation=candidate_target.storage_generation,
        report=report,
        activation_intent=activation_intent,
    )
    report_digest = _normalized_digest(
        attestation.get("report_digest"),
        field="audit report digest",
    )
    return dict(attestation), report_digest


async def _publish_audit_only_complete(
    control_run_id: str | None,
    *,
    snapshot_id: str,
    replay: bool,
) -> None:
    """Publish the terminal held-candidate progress phase."""

    message = (
        "reusing held passing attestation without promotion"
        if replay
        else "retaining passing attestation without promotion"
    )
    await _progress(
        control_run_id,
        snapshot_id=snapshot_id,
        phase="candidate audit-only complete",
        message=message,
        pct=100,
    )


async def _promote_audited_candidate(
    candidate_target: CandidateAuditTarget,
    *,
    control_run_id: str | None,
) -> None:
    """Promote one candidate whose attestation permits activation."""

    await _progress(
        control_run_id,
        snapshot_id=candidate_target.snapshot_id,
        phase="candidate promotion",
        message="atomically promoting audited candidate",
        pct=92,
    )
    promotion = await promote_ptg2_source_snapshot(
        source_key=candidate_target.source_key,
        snapshot_id=candidate_target.snapshot_id,
        expected_current_snapshot_id=(
            candidate_target.expected_current_snapshot_id
        ),
    )
    if promotion.get("status") != "promoted":
        raise RuntimeError("candidate promotion did not complete")


async def _held_audit_only_result(
    candidate_target: CandidateAuditTarget,
    *,
    run_id: str | None,
) -> dict[str, Any] | None:
    """Return a current held attestation without release-audit I/O."""

    held_attestation = await load_held_candidate_audit_attestation(
        snapshot_id=candidate_target.snapshot_id,
        source_key=candidate_target.source_key,
        plan_id=candidate_target.plan_id,
        plan_market_type=candidate_target.plan_market_type,
        storage_generation=candidate_target.storage_generation,
    )
    if held_attestation is None:
        return None
    await _publish_audit_only_complete(
        run_id,
        snapshot_id=candidate_target.snapshot_id,
        replay=True,
    )
    return _audit_only_result(
        candidate_target,
        report=_mapping(held_attestation["report"]),
        report_digest=str(held_attestation["report_digest"]),
        attestation=held_attestation,
        idempotent=True,
    )


async def _existing_candidate_audit_result(
    candidate_target: CandidateAuditTarget,
    *,
    candidate_audit_mode: str,
    run_id: str | None,
) -> dict[str, Any] | None:
    """Return an already active, equivalent, or held candidate result."""

    if (
        candidate_audit_mode == CANDIDATE_AUDIT_MODE_AUDIT_ONLY
        and (
            candidate_target.activated
            or candidate_target.equivalent_current_snapshot_id is not None
        )
    ):
        raise ValueError("audit-only candidate is already active or equivalent")
    if candidate_target.activated:
        assert candidate_target.audit_report is not None
        assert candidate_target.audit_report_digest is not None
        return _success_result(
            candidate_target,
            report=candidate_target.audit_report,
            report_digest=candidate_target.audit_report_digest,
            idempotent=True,
        )
    if candidate_target.equivalent_current_snapshot_id is not None:
        assert candidate_target.equivalent_audit_report is not None
        assert candidate_target.equivalent_audit_report_digest is not None
        return _success_result(
            candidate_target,
            report=candidate_target.equivalent_audit_report,
            report_digest=candidate_target.equivalent_audit_report_digest,
            idempotent=True,
        )
    if candidate_audit_mode == CANDIDATE_AUDIT_MODE_AUDIT_ONLY:
        return await _held_audit_only_result(
            candidate_target,
            run_id=run_id,
        )
    return None


async def _audit_candidate_under_guard(
    normalized_candidate_run_id: str,
    *,
    snapshot_id: str | None,
    import_id: str | None,
    run_id: str | None,
    candidate_audit_mode: str,
) -> dict[str, Any]:
    """Resolve one guarded candidate and execute only the required phase."""

    await _progress(
        run_id,
        snapshot_id=None,
        phase="candidate resolution",
        message="loading candidate from PostgreSQL",
        pct=10,
    )
    candidate_target = await load_candidate_audit_target(
        candidate_run_id=normalized_candidate_run_id,
        snapshot_id=snapshot_id,
        import_id=import_id,
    )
    existing_result = await _existing_candidate_audit_result(
        candidate_target,
        candidate_audit_mode=candidate_audit_mode,
        run_id=run_id,
    )
    if existing_result is not None:
        return existing_result
    http_config = _audit_configuration(
        candidate_target.snapshot_id,
        batch_writer=PTG2_BATCH_AUDIT_WRITER_ENABLED,
    )
    return await _audit_and_activate(
        candidate_target,
        control_run_id=run_id,
        http_config=http_config,
        candidate_audit_mode=candidate_audit_mode,
    )


async def run_ptg_candidate_audit_command(
    *,
    candidate_run_id: str,
    snapshot_id: str | None = None,
    import_id: str | None = None,
    run_id: str | None = None,
    candidate_audit_mode: str = CANDIDATE_AUDIT_MODE_AUDIT_AND_ACTIVATE,
) -> dict[str, Any]:
    """Audit, attest, and atomically activate one strict V3 candidate."""

    normalized_candidate_run_id = str(candidate_run_id or "").strip()
    if not normalized_candidate_run_id:
        raise ValueError("candidate_run_id is required")
    normalized_mode = _candidate_audit_mode(candidate_audit_mode)
    async with candidate_audit_guard(normalized_candidate_run_id):
        return await _audit_candidate_under_guard(
            normalized_candidate_run_id,
            snapshot_id=snapshot_id,
            import_id=import_id,
            run_id=run_id,
            candidate_audit_mode=normalized_mode,
        )


main = run_ptg_candidate_audit_command
main.__name__ = "main"


__all__ = [
    "ARCH_VERSION",
    "CandidateAuditTarget",
    "IMPORTER_NAME",
    "PTG2_BATCH_AUDIT_WRITER_ENABLED",
    "candidate_audit_guard",
    "load_candidate_audit_target",
    "main",
    "run_batch_release_audit",
    "run_release_audit",
]
