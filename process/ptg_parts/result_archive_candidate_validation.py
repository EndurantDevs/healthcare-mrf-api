# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finalize one adopted PTG candidate for a fresh destination audit.

Archive preparation produces destination-owned logical evidence and a sealed
destination layout.  This module joins those receipts back to their local rows,
installs only the sealed layout's serving manifest, and applies the normal
candidate-audit target validator.  It does not copy or create an attestation,
change a serving pointer, enqueue work, or activate the candidate.
"""

from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from process.ptg_candidate_audit import (
    CANDIDATE_AUDIT_MODE_AUDIT_ONLY,
    CandidateAuditTarget,
    validate_candidate_audit_target_state,
)
from process.ptg_candidate_audit import (
    IMPORTER_NAME as CANDIDATE_AUDIT_IMPORTER,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.frozen_rate_binding import _canonical_source_key
from process.ptg_parts.frozen_rate_files import FrozenRateFileValidationError
from process.ptg_parts.ptg2_candidate_attestation import CANDIDATE_SOURCE_RECORDS_SQL
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_source_lifecycle_lock
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.ptg2_shared_source_set import shared_source_set_metadata
from process.ptg_parts.result_archive_adoption import (
    RESULT_ARCHIVE_ADOPTION_CONTRACT,
    PreparedResultArchiveLayout,
)
from process.ptg_parts.result_archive_candidate_preparation import (
    RESULT_ARCHIVE_CANDIDATE_PREPARATION_CONTRACT,
    PreparedResultArchiveCandidate,
)
from process.ptg_parts.source_pointers import (
    _stage_snapshot_in_pointer_transaction,
    candidate_snapshot_attributes,
)

RESULT_ARCHIVE_CANDIDATE_VALIDATION_CONTRACT = "ptg_result_archive_candidate_validation_v1"
_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_ALLOWED_AMOUNT_TABLES = (
    "ptg2_allowed_amount_plan",
    "ptg2_allowed_amount_item",
    "ptg2_allowed_amount_payment",
    "ptg2_allowed_amount_provider_payment",
)
_LOCKED_CANDIDATE_SQL = """
    SELECT snapshot.snapshot_id, snapshot.import_run_id,
           snapshot.import_month, snapshot.status,
           snapshot.created_at, snapshot.validated_at,
           snapshot.published_at, snapshot.previous_snapshot_id,
           snapshot.manifest, internal_run.status AS run_status,
           internal_run.report AS run_report,
           internal_run.options -> 'invalid_price_exclusion_policy'
               AS invalid_price_exclusion_policy,
           binding.snapshot_key, scope.plan_id,
           scope.plan_market_type, scope.coverage_scope_id,
           layout.state AS layout_state,
           layout.generation AS layout_generation,
           layout.mapping_digest AS layout_mapping_digest,
           layout.layout_manifest,
           v4_root.state AS v4_root_state,
           v4_root.map_digest AS v4_root_map_digest,
           current_pointer.snapshot_id AS current_snapshot_id,
           frozen.binding_sha256 AS frozen_binding_sha256,
           frozen.binding_payload AS frozen_binding_payload,
           EXISTS (
               SELECT 1 FROM {schema}.ptg2_v3_candidate_audit_attestation attestation
                WHERE attestation.snapshot_id = snapshot.snapshot_id
           ) AS has_attestation,
           timezone('UTC', statement_timestamp()) AS staged_at
      FROM {schema}.ptg2_snapshot snapshot
      JOIN {schema}.ptg2_import_run internal_run
        ON internal_run.import_run_id = snapshot.import_run_id
      JOIN {schema}.ptg2_v3_snapshot_binding binding
        ON binding.snapshot_id = snapshot.snapshot_id
      JOIN {schema}.ptg2_v3_snapshot_scope scope
        ON scope.snapshot_id = snapshot.snapshot_id
      JOIN {schema}.ptg2_v3_snapshot_layout layout
        ON layout.snapshot_key = binding.snapshot_key
      JOIN {schema}.ptg2_v4_snapshot_map_root v4_root
        ON v4_root.snapshot_key = layout.snapshot_key
      JOIN {schema}.ptg2_frozen_source_file_binding frozen
        ON frozen.internal_run_id = snapshot.import_run_id
      LEFT JOIN {schema}.ptg2_current_source_snapshot current_pointer
        ON current_pointer.source_key = :source_key
     WHERE snapshot.snapshot_id = :snapshot_id
     FOR UPDATE OF snapshot, internal_run, binding, scope
"""


class ResultArchiveCandidateValidationError(RuntimeError):
    """The adopted destination state cannot enter the normal audit workflow."""


@dataclass(frozen=True)
class ValidatedResultArchiveCandidate:
    """Committed-state work that the coordinator must enqueue after this transaction."""

    contract: str
    destination_snapshot_id: str
    destination_import_run_id: str
    destination_snapshot_key: int
    source_key: str
    expected_current_snapshot_id: str | None
    next_importer: str
    next_parameters: Mapping[str, Any]
    status: str = "audit_required"
    requires_fresh_destination_attestation: bool = True


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return dict(getattr(value, "_mapping", value) or {})


def _require_transaction(session: Any) -> None:
    in_transaction = getattr(session, "in_transaction", None)
    if not callable(in_transaction) or not in_transaction():
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation requires an already-open caller transaction"
        )


def _validated_schema(schema_name: str) -> str:
    normalized = str(schema_name or "").strip()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError("schema_name must be a simple PostgreSQL identifier")
    if normalized != resolve_ptg2_schema():
        raise ValueError("schema_name must match the configured PTG schema")
    return normalized


def _validate_preparation_receipts(
    prepared_candidate: PreparedResultArchiveCandidate,
    prepared_layout: PreparedResultArchiveLayout,
) -> str:
    if (
        not isinstance(prepared_candidate, PreparedResultArchiveCandidate)
        or prepared_candidate.contract != RESULT_ARCHIVE_CANDIDATE_PREPARATION_CONTRACT
        or prepared_candidate.requires_fresh_destination_attestation is not True
    ):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation logical preparation receipt is invalid"
        )
    if (
        not isinstance(prepared_layout, PreparedResultArchiveLayout)
        or prepared_layout.contract != RESULT_ARCHIVE_ADOPTION_CONTRACT
        or prepared_layout.requires_fresh_destination_attestation is not True
        or len(bytes(prepared_layout.mapping_digest or b"")) != 32
    ):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation layout preparation receipt is invalid"
        )
    snapshot_id = str(prepared_candidate.destination_snapshot_id or "").strip()
    if not snapshot_id or snapshot_id != prepared_layout.destination_snapshot_id:
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation preparation receipts identify different snapshots"
        )
    return snapshot_id


def _validated_source_key(value: Any) -> str:
    """Use frozen-binding admission for the lifecycle lock and pointer identity."""

    try:
        return _canonical_source_key(value)
    except FrozenRateFileValidationError as exc:
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation destination source scope is unavailable"
        ) from exc


async def _candidate_source_key(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
) -> str:
    source_key_query = await session.execute(
        text(
            f"SELECT manifest->'activation'->>'source_key' "
            f"FROM {_quote_ident(schema_name)}.ptg2_snapshot "
            "WHERE snapshot_id = :snapshot_id"
        ),
        {"snapshot_id": snapshot_id},
    )
    source_key_rows = source_key_query.all()
    raw_source_key = source_key_rows[0][0] if len(source_key_rows) == 1 else None
    return _validated_source_key(raw_source_key)


async def _locked_candidate_row(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    source_key: str,
) -> dict[str, Any]:
    candidate_query = await session.execute(
        text(_LOCKED_CANDIDATE_SQL.format(schema=_quote_ident(schema_name))),
        {"snapshot_id": snapshot_id, "source_key": source_key},
    )
    candidate_rows = [_mapping(candidate_record) for candidate_record in candidate_query]
    if len(candidate_rows) != 1:
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation local candidate is missing or ambiguous"
        )
    return candidate_rows[0]


async def _source_records(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
) -> list[dict[str, Any]]:
    source_query = await session.execute(
        text(CANDIDATE_SOURCE_RECORDS_SQL.format(schema=_quote_ident(schema_name))),
        {"snapshot_id": snapshot_id},
    )
    return [_mapping(source_record) for source_record in source_query]


async def _allowed_amount_counts(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
) -> dict[str, int]:
    schema = _quote_ident(schema_name)
    counts_by_table: dict[str, int] = {}
    for table_name in _ALLOWED_AMOUNT_TABLES:
        count_query = await session.execute(
            text(f"SELECT COUNT(*) FROM {schema}.{_quote_ident(table_name)} WHERE snapshot_id = :snapshot_id"),
            {"snapshot_id": snapshot_id},
        )
        counts_by_table[table_name] = int(count_query.scalar_one())
    return counts_by_table


def _validated_layout_serving_index(
    candidate_row: Mapping[str, Any],
    prepared_layout: PreparedResultArchiveLayout,
) -> dict[str, Any]:
    if (
        int(candidate_row.get("snapshot_key") or 0) != prepared_layout.destination_snapshot_key
        or candidate_row.get("layout_state") != "sealed"
        or candidate_row.get("layout_generation") != "shared_blocks_v4"
        or bytes(candidate_row.get("layout_mapping_digest") or b"") != bytes(prepared_layout.mapping_digest)
        or candidate_row.get("v4_root_state") != "complete"
        or bytes(candidate_row.get("v4_root_map_digest") or b"") != bytes(prepared_layout.mapping_digest)
    ):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation layout differs from its preparation receipt"
        )
    layout_manifest = _mapping(candidate_row.get("layout_manifest"))
    serving_index = _mapping(layout_manifest.get("serving_index"))
    if serving_index.get("shared_snapshot_key") != prepared_layout.destination_snapshot_key:
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation layout has no destination-local serving key"
        )
    return serving_index


async def _validate_logical_preparation(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    candidate_row: Mapping[str, Any],
    prepared_candidate: PreparedResultArchiveCandidate,
) -> None:
    expected_counts_by_table = {
        str(table_name): int(row_count)
        for table_name, row_count in prepared_candidate.allowed_amount_row_counts.items()
    }
    if (
        expected_counts_by_table
        != await _allowed_amount_counts(
            session,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
        )
        or str(candidate_row.get("frozen_binding_sha256") or "") != prepared_candidate.frozen_binding_sha256
    ):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation logical evidence differs from its preparation receipt"
        )


def _candidate_attributes(
    candidate_row: Mapping[str, Any],
    *,
    source_key: str,
    serving_index: Mapping[str, Any],
) -> dict[str, Any]:
    status = str(candidate_row.get("status") or "").strip().lower()
    run_status = str(candidate_row.get("run_status") or "").strip().lower()
    manifest = _mapping(candidate_row.get("manifest"))
    activation = _mapping(manifest.get("activation"))
    if status == "building" and run_status == "running":
        if activation.get("state") != "building" or "serving_index" in manifest:
            raise ResultArchiveCandidateValidationError("archive candidate validation building state is not pristine")
        previous_snapshot_id = candidate_row.get("current_snapshot_id")
        validated_at = candidate_row.get("staged_at")
    elif status == "validated" and run_status == "validated":
        if manifest.get("serving_index") != serving_index:
            raise ResultArchiveCandidateValidationError("archive candidate validation replay serving manifest changed")
        previous_snapshot_id = candidate_row.get("previous_snapshot_id")
        validated_at = candidate_row.get("validated_at")
    else:
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation destination state is not building or replayable"
        )
    if _validated_source_key(activation.get("source_key")) != source_key:
        raise ResultArchiveCandidateValidationError("archive candidate validation destination source scope changed")
    manifest["serving_index"] = copy.deepcopy(dict(serving_index))
    return candidate_snapshot_attributes(
        {
            "snapshot_id": candidate_row["snapshot_id"],
            "import_run_id": candidate_row["import_run_id"],
            "import_month": candidate_row["import_month"],
            "created_at": candidate_row["created_at"],
            "validated_at": validated_at,
            "published_at": None,
            "previous_snapshot_id": previous_snapshot_id,
            "manifest": manifest,
        },
        source_key=source_key,
        previous_snapshot_id=previous_snapshot_id,
    )


def _attach_destination_source_identity(
    serving_index: Mapping[str, Any],
    *,
    source_key: str,
    source_records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Bind locked logical scope and copied source rows to destination serving."""

    sealed_source_key = serving_index.get("source_key")
    if "source_key" in serving_index and (
        not isinstance(sealed_source_key, str) or sealed_source_key.strip().lower() != source_key
    ):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation sealed source key differs from local scope"
        )

    try:
        source_set = shared_source_set_metadata(
            source_record.get("raw_container_sha256") for source_record in source_records
        )
    except ValueError as error:
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation destination source set is invalid"
        ) from error
    sealed_source_set = serving_index.get("source_set")
    if sealed_source_set is not None and (
        not isinstance(sealed_source_set, Mapping) or dict(sealed_source_set) != source_set
    ):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation sealed source set differs from local evidence"
        )
    return {
        **serving_index,
        "source_key": source_key,
        "source_set": source_set,
    }


async def _complete_local_run(
    session: Any,
    *,
    schema_name: str,
    candidate_attributes: Mapping[str, Any],
) -> None:
    schema = _quote_ident(schema_name)
    manifest_json = json.dumps(candidate_attributes["manifest"], default=str)
    completion_query = await session.execute(
        text(
            f"""
            WITH completed AS (
                UPDATE {schema}.ptg2_import_run
                   SET status = 'validated',
                       finished_at = statement_timestamp(),
                       heartbeat_at = statement_timestamp(),
                       report = CAST(:manifest_json AS jsonb),
                       error = NULL
                 WHERE import_run_id = :import_run_id
                   AND status = 'running'
                RETURNING import_run_id
            )
            SELECT import_run_id FROM completed
            UNION ALL
            SELECT import_run_id
              FROM {schema}.ptg2_import_run
             WHERE import_run_id = :import_run_id
               AND status = 'validated'
               AND report::jsonb = CAST(:manifest_json AS jsonb)
               AND NOT EXISTS (SELECT 1 FROM completed)
            LIMIT 1
            """
        ),
        {
            "import_run_id": candidate_attributes["import_run_id"],
            "manifest_json": manifest_json,
        },
    )
    if completion_query.first() is None:
        raise ResultArchiveCandidateValidationError("archive candidate validation local run could not become terminal")


def _audit_validation_row(
    candidate_row: Mapping[str, Any],
    candidate_attributes: Mapping[str, Any],
) -> dict[str, Any]:
    audit_state_by_name = dict(candidate_row)
    audit_state_by_name.update(candidate_attributes)
    audit_state_by_name.update(
        {
            "audit_report_digest": None,
            "audit_report": None,
            "audit_activation_intent": None,
            "audit_activated_at": None,
        }
    )
    return audit_state_by_name


async def _validated_audit_target(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    source_key: str,
    candidate_state: Mapping[str, Any],
    prepared_candidate: PreparedResultArchiveCandidate,
    prepared_layout: PreparedResultArchiveLayout,
) -> tuple[CandidateAuditTarget, dict[str, Any]]:
    """Validate local evidence with the normal audit target contract."""

    if bool(candidate_state.get("has_attestation")):
        raise ResultArchiveCandidateValidationError(
            "archive candidate validation requires a fresh destination attestation"
        )
    await _validate_logical_preparation(
        session,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        candidate_row=candidate_state,
        prepared_candidate=prepared_candidate,
    )
    candidate_sources = await _source_records(
        session,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
    )
    serving_index = _attach_destination_source_identity(
        _validated_layout_serving_index(
            candidate_state,
            prepared_layout,
        ),
        source_key=source_key,
        source_records=candidate_sources,
    )
    candidate_attributes = _candidate_attributes(
        candidate_state,
        source_key=source_key,
        serving_index=serving_index,
    )
    try:
        audit_target = validate_candidate_audit_target_state(
            _audit_validation_row(candidate_state, candidate_attributes),
            candidate_run_id=str(candidate_attributes["import_run_id"]),
            source_records=candidate_sources,
        )
    except (RuntimeError, ValueError) as error:
        raise ResultArchiveCandidateValidationError(
            f"archive candidate validation normal audit target rejected: {error}"
        ) from error
    return audit_target, candidate_attributes


def _audit_handoff(
    audit_target: CandidateAuditTarget,
) -> ValidatedResultArchiveCandidate:
    """Return only the resumable post-commit audit-only work request."""

    return ValidatedResultArchiveCandidate(
        contract=RESULT_ARCHIVE_CANDIDATE_VALIDATION_CONTRACT,
        destination_snapshot_id=audit_target.snapshot_id,
        destination_import_run_id=audit_target.candidate_run_id,
        destination_snapshot_key=audit_target.snapshot_key,
        source_key=audit_target.source_key,
        expected_current_snapshot_id=audit_target.expected_current_snapshot_id,
        next_importer=CANDIDATE_AUDIT_IMPORTER,
        next_parameters={
            "candidate_run_id": audit_target.candidate_run_id,
            "snapshot_id": audit_target.snapshot_id,
            "candidate_audit_mode": CANDIDATE_AUDIT_MODE_AUDIT_ONLY,
        },
    )


async def validate_result_archive_candidate_for_audit(
    session: Any,
    *,
    schema_name: str,
    prepared_candidate: PreparedResultArchiveCandidate,
    prepared_layout: PreparedResultArchiveLayout,
) -> ValidatedResultArchiveCandidate:
    """Stage one local candidate and return its post-commit audit-only request."""

    _require_transaction(session)
    destination_schema = _validated_schema(schema_name)
    snapshot_id = _validate_preparation_receipts(prepared_candidate, prepared_layout)
    source_key = await _candidate_source_key(
        session,
        schema_name=destination_schema,
        snapshot_id=snapshot_id,
    )
    await acquire_ptg2_source_lifecycle_lock(session, source_key=source_key)
    candidate_state = await _locked_candidate_row(
        session,
        schema_name=destination_schema,
        snapshot_id=snapshot_id,
        source_key=source_key,
    )
    audit_target, candidate_attributes = await _validated_audit_target(
        session,
        schema_name=destination_schema,
        snapshot_id=snapshot_id,
        source_key=source_key,
        candidate_state=candidate_state,
        prepared_candidate=prepared_candidate,
        prepared_layout=prepared_layout,
    )
    await _stage_snapshot_in_pointer_transaction(
        session,
        schema_name=destination_schema,
        snapshot_attributes=candidate_attributes,
    )
    await _complete_local_run(
        session,
        schema_name=destination_schema,
        candidate_attributes=candidate_attributes,
    )
    return _audit_handoff(audit_target)


__all__ = [
    "RESULT_ARCHIVE_CANDIDATE_VALIDATION_CONTRACT",
    "ResultArchiveCandidateValidationError",
    "ValidatedResultArchiveCandidate",
    "validate_result_archive_candidate_for_audit",
]
