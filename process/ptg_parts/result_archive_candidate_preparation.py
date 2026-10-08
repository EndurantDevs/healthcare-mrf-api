# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare destination-local logical PTG evidence restored from an archive.

This module is deliberately separate from physical-layout preparation.  The
caller must first create and own a destination candidate with its local import
attempt, source metadata, and frozen-input manifest.  This module only copies
the four snapshot-ID-scoped allowed-amount relations from a locally restored
staging schema, then verifies the candidate's already-local frozen evidence in
the caller's transaction.  It never creates candidates, imports source
attestations, or updates current/plan/source pointers.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.frozen_rate_binding import (
    FROZEN_RATE_FILE_BINDING_OPTION,
    _canonical_source_key,
    frozen_internal_run_id,
    frozen_rate_binding_from_params,
    frozen_rate_binding_sha256,
)
from process.ptg_parts.frozen_rate_binding_store import (
    insert_or_compare_frozen_binding,
    recheck_frozen_binding_on_connection,
)
from process.ptg_parts.frozen_rate_candidate import (
    validate_frozen_candidate_evidence,
)
from process.ptg_parts.frozen_rate_files import FrozenRateFileValidationError
from process.ptg_parts.ptg2_candidate_attestation import (
    CANDIDATE_SOURCE_RECORDS_SQL,
)
from process.ptg_parts.ptg2_invalid_price_exclusion import (
    INVALID_PRICE_EXCLUSION_POLICY_FIELD,
)
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema

RESULT_ARCHIVE_CANDIDATE_PREPARATION_CONTRACT = "ptg_result_archive_candidate_preparation_v1"
_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_ALLOWED_AMOUNT_TABLES = (
    "ptg2_allowed_amount_plan",
    "ptg2_allowed_amount_item",
    "ptg2_allowed_amount_payment",
    "ptg2_allowed_amount_provider_payment",
)


class ResultArchiveCandidatePreparationError(RuntimeError):
    """The restored logical closure cannot be attached to this candidate."""


@dataclass(frozen=True)
class PreparedResultArchiveCandidate:
    """Destination-owned logical evidence ready for fresh local attestation."""

    contract: str
    destination_snapshot_id: str
    source_snapshot_id: str
    allowed_amount_row_counts: Mapping[str, int]
    frozen_binding_sha256: str | None
    requires_fresh_destination_attestation: bool = True
    authority_contract: str | None = None
    authority_sha256: str | None = None


def _safe_identifier(value: str, *, label: str) -> str:
    normalized = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"{label} must be a simple PostgreSQL identifier")
    return normalized


def _required_snapshot_id(value: str, *, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 96:
        raise ValueError(f"{label} is required")
    return normalized


def _mapping(row: Any) -> dict[str, Any]:
    return dict(getattr(row, "_mapping", row) or {})


async def _one(
    session: Any,
    statement: str,
    parameters: Mapping[str, Any],
    label: str,
) -> dict[str, Any]:
    result = await session.execute(text(statement), dict(parameters))
    rows = [_mapping(row) for row in result]
    if len(rows) != 1:
        raise ResultArchiveCandidatePreparationError(f"archive candidate preparation {label} is missing or ambiguous")
    return rows[0]


def _activation_scope(
    manifest: Any,
    *,
    label: str,
) -> tuple[str, tuple[str, str] | None]:
    manifest_by_name = _mapping(manifest)
    activation_by_name = _mapping(manifest_by_name.get("activation"))
    try:
        source_key = _canonical_source_key(activation_by_name.get("source_key"))
    except FrozenRateFileValidationError as exc:
        raise ResultArchiveCandidatePreparationError(
            f"archive candidate preparation {label} has no valid activation scope"
        ) from exc
    has_plan_id_declaration = "plan_id" in activation_by_name
    has_market_type_declaration = "plan_market_type" in activation_by_name
    if has_plan_id_declaration != has_market_type_declaration:
        raise ResultArchiveCandidatePreparationError(
            f"archive candidate preparation {label} has a partial activation plan scope"
        )
    if not has_plan_id_declaration:
        return source_key, None
    plan_id = str(activation_by_name.get("plan_id") or "").strip()
    market_type = str(activation_by_name.get("plan_market_type") or "").strip().lower()
    if not plan_id or not market_type:
        raise ResultArchiveCandidatePreparationError(
            f"archive candidate preparation {label} has an invalid activation plan scope"
        )
    return source_key, (plan_id, market_type)


def _database_scope(snapshot: Mapping[str, Any], *, label: str) -> tuple[str, str]:
    plan_id = str(snapshot.get("scope_plan_id") or "").strip()
    market_type = str(snapshot.get("scope_plan_market_type") or "").strip().lower()
    if not plan_id or not market_type:
        raise ResultArchiveCandidatePreparationError(
            f"archive candidate preparation {label} has no authoritative plan scope"
        )
    return plan_id, market_type


async def _locked_candidate(
    session: Any,
    *,
    schema_name: str,
    destination_snapshot_id: str,
) -> dict[str, Any]:
    schema = _quote_ident(schema_name)
    return await _one(
        session,
        f"""
        SELECT snapshot.snapshot_id, snapshot.import_run_id, snapshot.manifest,
               scope.plan_id AS scope_plan_id,
               scope.plan_market_type AS scope_plan_market_type
          FROM {schema}.ptg2_snapshot AS snapshot
          JOIN {schema}.ptg2_v3_snapshot_scope AS scope
            ON scope.snapshot_id = snapshot.snapshot_id
         WHERE snapshot.snapshot_id = :snapshot_id
         FOR UPDATE OF snapshot, scope
        """,
        {"snapshot_id": destination_snapshot_id},
        "destination candidate",
    )


async def _locked_staging_snapshot(
    session: Any,
    *,
    staging_schema_name: str,
    source_snapshot_key: int,
) -> dict[str, Any]:
    staging = _quote_ident(staging_schema_name)
    return await _one(
        session,
        f"""
        SELECT snapshot.snapshot_id, snapshot.manifest,
               scope.plan_id AS scope_plan_id,
               scope.plan_market_type AS scope_plan_market_type
          FROM {staging}.ptg2_v3_snapshot_binding AS binding
          JOIN {staging}.ptg2_snapshot AS snapshot
            ON snapshot.snapshot_id = binding.snapshot_id
          JOIN {staging}.ptg2_v3_snapshot_scope AS scope
            ON scope.snapshot_id = snapshot.snapshot_id
         WHERE binding.snapshot_key = :source_snapshot_key
         FOR SHARE OF binding, snapshot, scope
        """,
        {"source_snapshot_key": int(source_snapshot_key)},
        "staging snapshot binding",
    )


async def _lock_staged_allowed_amount_family(
    session: Any,
    *,
    staging_schema_name: str,
) -> None:
    """Hold the complete admitted staged family stable through copy and verification."""

    staging = _quote_ident(staging_schema_name)
    for table_name in _ALLOWED_AMOUNT_TABLES:
        await session.execute(text(f"LOCK TABLE {staging}.{_quote_ident(table_name)} IN SHARE MODE"))


def _assert_scope_matches(
    destination_candidate: Mapping[str, Any],
    staging_snapshot: Mapping[str, Any],
) -> None:
    destination_source_key, destination_declared_scope = _activation_scope(
        destination_candidate.get("manifest"),
        label="destination candidate",
    )
    staging_source_key, staging_declared_scope = _activation_scope(
        staging_snapshot.get("manifest"),
        label="staging snapshot",
    )
    destination_database_scope = _database_scope(
        destination_candidate,
        label="destination candidate",
    )
    staging_database_scope = _database_scope(
        staging_snapshot,
        label="staging snapshot",
    )
    if (
        destination_source_key != staging_source_key
        or destination_database_scope != staging_database_scope
        or (destination_declared_scope is not None and destination_declared_scope != destination_database_scope)
        or (staging_declared_scope is not None and staging_declared_scope != staging_database_scope)
    ):
        raise ResultArchiveCandidatePreparationError("archive candidate preparation source scope differs from staging")


def _portable_frozen_binding(manifest: Any, *, label: str) -> dict[str, Any]:
    """Normalize all portable frozen inputs, excluding only the destination filing ID."""

    manifest_by_name = _mapping(manifest)
    binding_by_name = _mapping(manifest_by_name.get(FROZEN_RATE_FILE_BINDING_OPTION))
    source_file_import_id = str(binding_by_name.get("source_file_import_id") or "").strip()
    frozen_params_by_name = {
        "import_id": source_file_import_id,
        "source_file_import_id": source_file_import_id,
        "source_key": binding_by_name.get("source_key"),
        "import_month": binding_by_name.get("import_month"),
        "plan_ids": binding_by_name.get("plan_ids"),
        "plan_market_types": binding_by_name.get("plan_market_types"),
        "frozen_rate_file_set_contract": manifest_by_name.get("frozen_rate_file_set_contract"),
        "frozen_rate_files": manifest_by_name.get("frozen_rate_files"),
        "frozen_rate_file_set_sha256": manifest_by_name.get("frozen_rate_file_set_sha256"),
        "frozen_rate_file_count": manifest_by_name.get("frozen_rate_file_count"),
    }
    if INVALID_PRICE_EXCLUSION_POLICY_FIELD in binding_by_name:
        frozen_params_by_name[INVALID_PRICE_EXCLUSION_POLICY_FIELD] = binding_by_name[
            INVALID_PRICE_EXCLUSION_POLICY_FIELD
        ]
    try:
        normalized_binding = frozen_rate_binding_from_params(frozen_params_by_name)
    except ValueError as error:
        raise ResultArchiveCandidatePreparationError(
            f"archive candidate preparation {label} frozen input is invalid"
        ) from error
    if normalized_binding is None or normalized_binding != binding_by_name:
        raise ResultArchiveCandidatePreparationError(f"archive candidate preparation {label} frozen input is invalid")
    normalized_binding.pop("source_file_import_id")
    return normalized_binding


def _assert_portable_frozen_input_matches(
    destination_candidate: Mapping[str, Any],
    staging_snapshot: Mapping[str, Any],
    frozen_binding_params: Mapping[str, Any],
) -> None:
    """Require staged, local, and request inputs to name one portable frozen generation."""

    try:
        requested_binding = frozen_rate_binding_from_params(frozen_binding_params)
    except ValueError as error:
        raise ResultArchiveCandidatePreparationError(
            "archive candidate preparation requested frozen input is invalid"
        ) from error
    if requested_binding is None:
        raise ResultArchiveCandidatePreparationError(
            "archive candidate preparation requires frozen source-file evidence"
        )
    requested_binding.pop("source_file_import_id")
    if (
        _portable_frozen_binding(destination_candidate.get("manifest"), label="destination candidate")
        != requested_binding
        or _portable_frozen_binding(staging_snapshot.get("manifest"), label="staging snapshot") != requested_binding
    ):
        raise ResultArchiveCandidatePreparationError(
            "archive candidate preparation portable frozen input differs from staging"
        )


async def _candidate_source_records(
    session: Any,
    *,
    schema_name: str,
    destination_snapshot_id: str,
) -> list[dict[str, Any]]:
    result = await session.execute(
        text(CANDIDATE_SOURCE_RECORDS_SQL.format(schema=_quote_ident(schema_name))),
        {"snapshot_id": destination_snapshot_id},
    )
    return [_mapping(row) for row in result]


async def _table_columns(
    session: Any,
    *,
    schema_name: str,
    table_name: str,
) -> tuple[str, ...]:
    result = await session.execute(
        text(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = :schema_name AND table_name = :table_name
             ORDER BY ordinal_position
            """
        ),
        {"schema_name": schema_name, "table_name": table_name},
    )
    columns = tuple(str(row[0]) for row in result)
    if not columns or "snapshot_id" not in columns or len(columns) != len(set(columns)):
        raise ResultArchiveCandidatePreparationError(
            f"archive candidate preparation {table_name} has an unsupported key shape"
        )
    return columns


async def _matching_allowed_amount_columns(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    table_name: str,
) -> tuple[str, ...]:
    """Require the destination and locked staging relation to have one exact column shape."""

    destination_columns = await _table_columns(session, schema_name=schema_name, table_name=table_name)
    staging_columns = await _table_columns(session, schema_name=staging_schema_name, table_name=table_name)
    if destination_columns != staging_columns:
        raise ResultArchiveCandidatePreparationError(
            f"archive candidate preparation {table_name} column contract differs from staging"
        )
    return destination_columns


async def _copy_allowed_amount_rows(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    table_name: str,
    columns: tuple[str, ...],
    source_snapshot_id: str,
    destination_snapshot_id: str,
) -> None:
    """Copy one stable staged relation while rekeying only its snapshot identifier."""

    schema = _quote_ident(schema_name)
    staging = _quote_ident(staging_schema_name)
    table = _quote_ident(table_name)
    quoted_columns = ", ".join(_quote_ident(column) for column in columns)
    copied_columns = ", ".join(
        ":destination_snapshot_id" if column == "snapshot_id" else _quote_ident(column) for column in columns
    )
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.{table} ({quoted_columns})
            SELECT {copied_columns}
              FROM {staging}.{table}
             WHERE snapshot_id = :source_snapshot_id
            ON CONFLICT DO NOTHING
            """
        ),
        {
            "source_snapshot_id": source_snapshot_id,
            "destination_snapshot_id": destination_snapshot_id,
        },
    )


async def _verified_allowed_amount_count(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    table_name: str,
    columns: tuple[str, ...],
    source_snapshot_id: str,
    destination_snapshot_id: str,
) -> int:
    """Require the copied destination rows to equal the locked staged relation exactly."""

    schema = _quote_ident(schema_name)
    staging = _quote_ident(staging_schema_name)
    table = _quote_ident(table_name)
    compared_columns = tuple(column for column in columns if column != "snapshot_id")
    select_columns = ", ".join(_quote_ident(column) for column in compared_columns) or "1"
    difference_result = await session.execute(
        text(
            f"""
            SELECT EXISTS (
                (SELECT {select_columns} FROM {staging}.{table}
                  WHERE snapshot_id = :source_snapshot_id)
                EXCEPT ALL
                (SELECT {select_columns} FROM {schema}.{table}
                  WHERE snapshot_id = :destination_snapshot_id)
            ) OR EXISTS (
                (SELECT {select_columns} FROM {schema}.{table}
                  WHERE snapshot_id = :destination_snapshot_id)
                EXCEPT ALL
                (SELECT {select_columns} FROM {staging}.{table}
                  WHERE snapshot_id = :source_snapshot_id)
            ) AS differs
            """
        ),
        {
            "source_snapshot_id": source_snapshot_id,
            "destination_snapshot_id": destination_snapshot_id,
        },
    )
    if bool(difference_result.scalar()):
        raise ResultArchiveCandidatePreparationError(
            f"archive candidate preparation {table_name} conflicts with destination rows"
        )
    count_result = await session.execute(
        text(f"SELECT COUNT(*) FROM {schema}.{table} WHERE snapshot_id = :destination_snapshot_id"),
        {"destination_snapshot_id": destination_snapshot_id},
    )
    return int(count_result.scalar_one())


async def _copy_allowed_amount_table(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    table_name: str,
    source_snapshot_id: str,
    destination_snapshot_id: str,
) -> int:
    """Copy and compare one relation from the already-pinned allowed-amount family."""

    columns = await _matching_allowed_amount_columns(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        table_name=table_name,
    )
    await _copy_allowed_amount_rows(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        table_name=table_name,
        columns=columns,
        source_snapshot_id=source_snapshot_id,
        destination_snapshot_id=destination_snapshot_id,
    )
    return await _verified_allowed_amount_count(
        session,
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        table_name=table_name,
        columns=columns,
        source_snapshot_id=source_snapshot_id,
        destination_snapshot_id=destination_snapshot_id,
    )


async def _copy_allowed_amount_evidence(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_id: str,
    destination_snapshot_id: str,
) -> dict[str, int]:
    return {
        table_name: await _copy_allowed_amount_table(
            session,
            schema_name=schema_name,
            staging_schema_name=staging_schema_name,
            table_name=table_name,
            source_snapshot_id=source_snapshot_id,
            destination_snapshot_id=destination_snapshot_id,
        )
        for table_name in _ALLOWED_AMOUNT_TABLES
    }


async def _validate_local_frozen_candidate(
    session: Any,
    *,
    schema_name: str,
    destination_candidate: Mapping[str, Any],
    frozen_binding_params: Mapping[str, Any],
) -> str:
    expected_binding = frozen_rate_binding_from_params(frozen_binding_params)
    if expected_binding is None:
        raise ResultArchiveCandidatePreparationError(
            "archive candidate preparation requires frozen source-file evidence"
        )
    destination_snapshot_id = str(destination_candidate["snapshot_id"])
    candidate_run_id = str(destination_candidate.get("import_run_id") or "").strip()
    if candidate_run_id != frozen_internal_run_id(str(expected_binding["source_file_import_id"])):
        raise ResultArchiveCandidatePreparationError(
            "archive candidate preparation frozen binding does not match the local attempt"
        )
    candidate_manifest = _mapping(destination_candidate.get("manifest"))
    if candidate_manifest.get(FROZEN_RATE_FILE_BINDING_OPTION) != expected_binding:
        raise ResultArchiveCandidatePreparationError(
            "archive candidate preparation frozen binding does not match the local candidate"
        )
    destination_source_key, _declared_scope = _activation_scope(
        candidate_manifest,
        label="destination candidate",
    )
    if destination_source_key != str(expected_binding.get("source_key") or "").strip().lower():
        raise ResultArchiveCandidatePreparationError(
            "archive candidate preparation frozen binding has the wrong source scope"
        )
    async with db.bind_existing_session(session):
        stored_binding = await insert_or_compare_frozen_binding(db, frozen_binding_params)
        await recheck_frozen_binding_on_connection(db, frozen_binding_params)
    source_records = await _candidate_source_records(
        session,
        schema_name=schema_name,
        destination_snapshot_id=destination_snapshot_id,
    )
    validate_frozen_candidate_evidence(
        candidate_manifest,
        candidate_run_id=candidate_run_id,
        database_binding=stored_binding,
        database_sources=source_records,
    )
    return frozen_rate_binding_sha256(expected_binding)


async def _locked_candidate_inputs(
    session: Any,
    *,
    destination_schema: str,
    staging_schema: str,
    source_snapshot_key: int,
    destination_snapshot_id: str,
    frozen_binding_params: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    """Lock and validate one admitted staged generation before any destination write."""

    destination_candidate = await _locked_candidate(
        session,
        schema_name=destination_schema,
        destination_snapshot_id=destination_snapshot_id,
    )
    staging_snapshot = await _locked_staging_snapshot(
        session,
        staging_schema_name=staging_schema,
        source_snapshot_key=source_snapshot_key,
    )
    source_snapshot_id = _required_snapshot_id(
        str(staging_snapshot.get("snapshot_id") or ""), label="staging snapshot_id"
    )
    if source_snapshot_id == destination_snapshot_id:
        raise ResultArchiveCandidatePreparationError(
            "archive candidate preparation requires a remapped destination snapshot ID"
        )
    _assert_scope_matches(destination_candidate, staging_snapshot)
    _assert_portable_frozen_input_matches(
        destination_candidate,
        staging_snapshot,
        frozen_binding_params,
    )
    await _lock_staged_allowed_amount_family(session, staging_schema_name=staging_schema)
    return destination_candidate, source_snapshot_id


async def _prepare_published_result_candidate(
    session: Any,
    *,
    destination_schema: str,
    staging_schema: str,
    source_snapshot_key: int,
    destination_snapshot: str,
    frozen_binding_params: Mapping[str, Any],
    published_result_receipt: Mapping[str, Any],
) -> PreparedResultArchiveCandidate:
    """Authenticate copied result evidence and prepare only destination-owned rows."""
    if frozen_binding_params:
        raise ResultArchiveCandidatePreparationError("published result cannot carry frozen input parameters")
    from process.ptg_parts.result_archive_candidate_initialization import ResultArchiveCandidateInitializationError
    from process.ptg_parts.result_archive_receive_binding import (
        authenticate_published_result_stage,
        validate_local_published_result,
    )

    try:
        staged = await authenticate_published_result_stage(
            session,
            staging_schema_name=staging_schema,
            source_snapshot_key=source_snapshot_key,
            receipt=published_result_receipt,
        )
        if staged.source_snapshot_id == destination_snapshot:
            raise ResultArchiveCandidatePreparationError("published result requires a remapped destination snapshot")
        digest = await validate_local_published_result(
            session,
            schema_name=destination_schema,
            snapshot_id=destination_snapshot,
            receipt=published_result_receipt,
        )
    except (ValueError, ResultArchiveCandidateInitializationError) as exc:
        raise ResultArchiveCandidatePreparationError("published result candidate evidence differs") from exc
    await _lock_staged_allowed_amount_family(session, staging_schema_name=staging_schema)
    counts = await _copy_allowed_amount_evidence(
        session,
        schema_name=destination_schema,
        staging_schema_name=staging_schema,
        source_snapshot_id=staged.source_snapshot_id,
        destination_snapshot_id=destination_snapshot,
    )
    return PreparedResultArchiveCandidate(
        contract=RESULT_ARCHIVE_CANDIDATE_PREPARATION_CONTRACT,
        destination_snapshot_id=destination_snapshot,
        source_snapshot_id=staged.source_snapshot_id,
        allowed_amount_row_counts=counts,
        frozen_binding_sha256=None,
        authority_contract=published_result_receipt["contract"],
        authority_sha256=digest,
    )


async def _prepare_frozen_result_candidate(
    session: Any,
    *,
    destination_schema: str,
    staging_schema: str,
    source_snapshot_key: int,
    destination_snapshot: str,
    frozen_binding_params: Mapping[str, Any],
) -> PreparedResultArchiveCandidate:
    """Prepare a destination candidate under the existing frozen-file contract."""
    destination_candidate, source_snapshot_id = await _locked_candidate_inputs(
        session,
        destination_schema=destination_schema,
        staging_schema=staging_schema,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_id=destination_snapshot,
        frozen_binding_params=frozen_binding_params,
    )
    frozen_binding_digest = await _validate_local_frozen_candidate(
        session,
        schema_name=destination_schema,
        destination_candidate=destination_candidate,
        frozen_binding_params=frozen_binding_params,
    )
    counts = await _copy_allowed_amount_evidence(
        session,
        schema_name=destination_schema,
        staging_schema_name=staging_schema,
        source_snapshot_id=source_snapshot_id,
        destination_snapshot_id=destination_snapshot,
    )
    return PreparedResultArchiveCandidate(
        contract=RESULT_ARCHIVE_CANDIDATE_PREPARATION_CONTRACT,
        destination_snapshot_id=destination_snapshot,
        source_snapshot_id=source_snapshot_id,
        allowed_amount_row_counts=counts,
        frozen_binding_sha256=frozen_binding_digest,
    )


async def prepare_result_archive_candidate_evidence(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_id: str,
    frozen_binding_params: Mapping[str, Any],
    published_result_receipt: Mapping[str, Any] | None = None,
) -> PreparedResultArchiveCandidate:
    """Compare staged evidence against local authority in the caller transaction.
    Require the configured schema; fresh attestation and activation remain separate.
    """

    is_in_transaction = getattr(session, "in_transaction", None)
    if not callable(is_in_transaction) or not is_in_transaction():
        raise ResultArchiveCandidatePreparationError(
            "archive candidate preparation requires an already-open caller transaction"
        )
    destination_schema = _safe_identifier(schema_name, label="schema_name")
    staging_schema = _safe_identifier(staging_schema_name, label="staging_schema_name")
    destination_snapshot = _required_snapshot_id(destination_snapshot_id, label="destination_snapshot_id")
    if destination_schema == staging_schema:
        raise ValueError("staging_schema_name must differ from schema_name")
    if destination_schema != resolve_ptg2_schema():
        raise ValueError("schema_name must match the configured PTG schema")
    if published_result_receipt is not None:
        return await _prepare_published_result_candidate(
            session,
            destination_schema=destination_schema,
            staging_schema=staging_schema,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot=destination_snapshot,
            frozen_binding_params=frozen_binding_params,
            published_result_receipt=published_result_receipt,
        )
    return await _prepare_frozen_result_candidate(
        session,
        destination_schema=destination_schema,
        staging_schema=staging_schema,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot=destination_snapshot,
        frozen_binding_params=frozen_binding_params,
    )


async def audit_local_data_candidate(session, *, ownership, metadata, initialized, control_sha256):
    """Audit actual isolated witness data and local controls in the publisher's transaction.

    This deterministic native set-audit result must be persisted by the existing
    protected preparation ledger. It is neither a release report nor a serving
    capability; no copied status, attestation or source token authorizes writes.
    """
    from process.ptg_parts import ptg2_physical_binding as native
    from process.ptg_parts import result_archive_closure as closure
    from process.ptg_parts.ptg2_shared_audit import sealed_audit_sample_metadata
    from process.ptg_parts.ptg2_source_witness_store import load_shared_source_witness

    _require_local_audit_transaction(session)
    await native.verify_local_data_family(session, ownership)
    scope = native.validate_local_serving_scope(metadata["closure_metadata"]["serving_scope"])
    candidate = await _local_audit_control(session, initialized, control_sha256, scope)
    layout = await closure._locked_layout(
        session,
        schema=_quote_ident(ownership.schema_name),
        snapshot_id=scope["snapshot_id"],
        payload_snapshot_key=metadata["source_snapshot_key"],
    )
    closure._validate_layout(layout)
    sample = await sealed_audit_sample_metadata(
        session,
        schema_name=ownership.schema_name,
        snapshot_key=metadata["source_snapshot_key"],
        logical_snapshot_id=scope["snapshot_id"],
        expected_generation=layout["generation"],
    )
    async with db.bind_existing_session(session):
        witness = await load_shared_source_witness(
            schema_name=ownership.schema_name,
            snapshot_key=metadata["source_snapshot_key"],
            expected_raw_source_sha256=[
                assignment["raw_container_sha256"] for assignment in scope["source_assignments"]
            ],
            expected_metadata=layout["layout_manifest"]["serving_index"]["source_witness"],
        )
    identity = await _local_audit_identity(session, candidate, layout, ownership.schema_name, scope, metadata)
    if (
        sample["sample_digest"] != identity["audit_sample_digest"].hex()
        or witness.metadata["payload_sha256"] != identity["source_witness_digest"].hex()
    ):
        raise ResultArchiveCandidatePreparationError("local data persisted audit evidence differs")
    for field in ("coverage_scope_id", "source_set_digest", "source_witness_digest", "audit_sample_digest"):
        identity[field] = identity[field].hex()
    return {
        "contract": "ptg-local-data.native-set-audit.v1",
        "identity": identity,
        "control_sha256": control_sha256,
        "model_sha256": native.local_data_model_digest(),
        "catalog_sha256": await native.local_data_catalog_digest(session, ownership),
    }


def _require_local_audit_transaction(session):
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise ResultArchiveCandidatePreparationError("local data audit requires a caller transaction")


async def _local_audit_control(session, initialized, control_sha256, scope):
    """Rejoin destination-owned controls, not the copied source status or lease."""
    from process.ptg_parts import result_archive_candidate_initialization as initialization

    schema_name = resolve_ptg2_schema()
    schema = _quote_ident(schema_name)
    candidate = await initialization._one(
        session,
        f"SELECT snapshot.*,run.options,run.status AS run_status,scope.plan_id,scope.plan_market_type,scope.coverage_scope_id,"
        f"frozen.binding_payload AS frozen_binding_payload FROM {schema}.ptg2_snapshot snapshot "
        f"JOIN {schema}.ptg2_import_run run ON run.import_run_id=snapshot.import_run_id "
        f"JOIN {schema}.ptg2_v3_snapshot_scope scope ON scope.snapshot_id=snapshot.snapshot_id "
        f"LEFT JOIN {schema}.ptg2_frozen_source_file_binding frozen ON frozen.internal_run_id=run.import_run_id "
        "WHERE snapshot.snapshot_id=:snapshot_id FOR UPDATE OF snapshot,run,scope",
        {"snapshot_id": initialized.destination_snapshot_id},
        "local native audit controls",
    )
    plans = await initialization._staged_plan_scopes(
        session, staging_schema=schema_name, source_snapshot_id=initialized.destination_snapshot_id
    )
    if (
        candidate["status"] != "building"
        or candidate["run_status"] != "running"
        or candidate["import_run_id"] != initialized.destination_import_run_id
        or candidate["options"].get("source_key") != scope["source_key"]
        or [candidate["plan_id"], candidate["plan_market_type"]] != scope["primary_plan"]
        or bytes(candidate["coverage_scope_id"]).hex() != scope["coverage_scope_id"]
        or plans != tuple(tuple(plan) for plan in scope["plan_scopes"])
        or initialization._local_control_sha256(candidate["manifest"], candidate["options"], plans) != control_sha256
    ):
        raise ResultArchiveCandidatePreparationError("local data native audit controls differ")
    return candidate


async def _local_audit_identity(session, candidate, layout, schema_name, scope, metadata):
    """Reuse strict canonical identity checks with authenticated isolated relation inputs."""
    from process.ptg_parts import ptg2_candidate_attestation as attestation
    from process.ptg_parts import result_archive_candidate_initialization as initialization
    from process.ptg_parts.result_archive_candidate_validation import _attach_destination_source_identity

    source_records = await initialization._rows(
        session,
        CANDIDATE_SOURCE_RECORDS_SQL.format(schema=_quote_ident(schema_name)),
        {"snapshot_id": scope["snapshot_id"]},
    )
    serving_index_by_field = _attach_destination_source_identity(
        layout["layout_manifest"]["serving_index"],
        source_key=scope["source_key"],
        source_records=source_records,
    )
    database_by_field = {
        **candidate,
        "snapshot_key": metadata["source_snapshot_key"],
        "layout_manifest": layout["layout_manifest"],
        "raw_container_sha256_values": [
            assignment["raw_container_sha256"] for assignment in scope["source_assignments"]
        ],
        INVALID_PRICE_EXCLUSION_POLICY_FIELD: candidate["options"].get(INVALID_PRICE_EXCLUSION_POLICY_FIELD),
        "frozen_source_records": source_records,
    }
    return attestation._candidate_evidence_identity(
        database_by_field,
        activation_by_field=candidate["manifest"]["activation"],
        serving_index_by_field=serving_index_by_field,
        layout_serving_index_by_field=layout["layout_manifest"]["serving_index"],
        storage_generation=layout["generation"],
    )


def physical_family_spec():
    """Declare the payload family through installed types, not portable relation paths."""
    from db import models
    from process.reference_family_archive import ReferenceFamilySpec

    return ReferenceFamilySpec(
        "ptg-snapshot",
        (
            models.PTG2V3Code,
            models.PTG2V3PriceAttr,
            models.PTG2V3ProviderSet,
            models.PTG2V3SnapshotBlock,
            models.PTG2V4SnapshotMapPack,
            models.PTG2V4FinalizerMapRoot,
            models.PTG2V4FinalizerMapPack,
            models.PTG2V4FinalizerMapTarget,
            models.PTG2V4NPIScope,
            models.PTG2V3ProviderGroup,
            models.PTG2V4ProviderComponent,
            models.PTG2V4Pattern,
            models.PTG2V4RelationManifest,
            models.PTG2V4HeavyOwner,
            models.PTG2V4NPIPrefix,
            models.PTG2V4ProviderGraphDiagnostic,
            models.PTG2V4InferredTaxonomyCandidate,
            models.PTG2V3AuditOccurrence,
            models.PTG2V3SourceAuditWitness,
            models.PTG2WitnessPart,
            models.PTG2ProviderTaxIdentityManifest,
            models.PTG2ProviderTaxIdentity,
            models.PTG2ProviderGroupTaxIdentity,
            models.PTG2TaxIdentitySourceManifest,
            models.PTG2TaxIdentitySourceBinding,
            models.PTG2GroupTaxIdentitySource,
            models.PTG2V4SnapshotMapRoot,
            models.PTG2AllowedAmountPlan,
            models.PTG2AllowedAmountItem,
            models.PTG2AllowedAmountPayment,
            models.PTG2AllowedAmountProviderPayment,
            models.PTG2V3Block,
        ),
    )


def local_data_family_spec():
    """Close selected payload, provenance and coordinate data, never authority."""
    from db import models
    from process.reference_family_archive import ReferenceFamilySpec

    payload_family = physical_family_spec()
    return ReferenceFamilySpec(
        payload_family.importer_id,
        (
            *payload_family.model_types,
            models.PTG2V3SnapshotLayout,
            models.PTG2Snapshot,
            models.PTG2SourceIdentity,
            models.PTG2ContentIdentity,
            models.PTG2SourceFileVersion,
            models.PTG2SourceTrace,
            models.PTG2SourceTraceSet,
            models.PTG2V3SnapshotSource,
            models.PTG2V3SnapshotScope,
            models.PTG2V3SnapshotPlanScope,
        ),
        relationships=(
            (models.PTG2V3PriceAttr, "snapshot_key", models.PTG2V3SnapshotLayout, "snapshot_key", False, False),
            (
                models.PTG2SourceFileVersion,
                "source_identity_hash",
                models.PTG2SourceIdentity,
                "source_identity_hash",
                False,
                True,
            ),
            (models.PTG2SourceFileVersion, "content_hash", models.PTG2ContentIdentity, "content_hash", False, True),
            (
                models.PTG2SourceTrace,
                "source_file_version_id",
                models.PTG2SourceFileVersion,
                "source_file_version_id",
                False,
                True,
            ),
            (models.PTG2SourceTraceSet, "source_trace_hashes", models.PTG2SourceTrace, "source_trace_hash", True, True),
        ),
    )


def local_data_model_digest():
    """Bind the trusted heap, identity, constraint, index and relationship declarations."""
    import hashlib

    from sqlalchemy import MetaData
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import AddConstraint, CreateColumn, CreateIndex

    from process.reference_family_archive import _additional_index_sql

    metadata = MetaData(schema="snapshot_model")
    spec = local_data_family_spec()
    for model in spec.model_types:
        table = model.__table__.to_metadata(metadata, schema=metadata.schema)
        table.dialect_options["postgresql"]["partition_by"] = None
        for column in table.columns:
            if column.identity is None:
                column.autoincrement = False
    statements = []
    for model in spec.model_types:
        table = metadata.tables[f"{metadata.schema}.{model.__tablename__}"]
        statements.append(table.name)
        for column in table.columns:
            for constraint in tuple(column.constraints):
                column.constraints.remove(constraint)
                table.append_constraint(constraint)
            statements.append(str(CreateColumn(column).compile(dialect=postgresql.dialect())))
        statements.extend(
            sorted(
                str(AddConstraint(constraint).compile(dialect=postgresql.dialect())) for constraint in table.constraints
            )
        )
        statements.extend(
            str(CreateIndex(index).compile(dialect=postgresql.dialect()))
            for index in sorted(table.indexes, key=lambda item: item.name)
        )
        for index in (*getattr(model, "__my_initial_indexes__", ()), *getattr(model, "__my_additional_indexes__", ())):
            statements.append(_additional_index_sql(metadata.schema, model, index))
    statements.extend(
        repr((child.__tablename__, column, parent.__tablename__, parent_column, is_array, allows_null))
        for child, column, parent, parent_column, is_array, allows_null in spec.relationships
    )
    return hashlib.sha256("\n".join(statements).encode()).hexdigest()


__all__ = [
    "PreparedResultArchiveCandidate",
    "RESULT_ARCHIVE_CANDIDATE_PREPARATION_CONTRACT",
    "ResultArchiveCandidatePreparationError",
    "prepare_result_archive_candidate_evidence",
    "audit_local_data_candidate",
]
