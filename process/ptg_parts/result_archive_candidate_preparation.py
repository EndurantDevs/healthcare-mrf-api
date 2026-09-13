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
    frozen_binding_sha256: str
    requires_fresh_destination_attestation: bool = True


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


def _activation_scope(manifest: Any, *, label: str) -> tuple[str, str, str]:
    manifest_by_name = _mapping(manifest)
    activation_by_name = _mapping(manifest_by_name.get("activation"))
    source_key = str(activation_by_name.get("source_key") or "").strip().lower()
    plan_id = str(activation_by_name.get("plan_id") or "").strip()
    market_type = str(activation_by_name.get("plan_market_type") or "").strip().lower()
    if not all((source_key, plan_id, market_type)):
        raise ResultArchiveCandidatePreparationError(f"archive candidate preparation {label} has no activation scope")
    return source_key, plan_id, market_type


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
    destination_scope = _activation_scope(
        destination_candidate.get("manifest"),
        label="destination candidate",
    )
    staging_scope = _activation_scope(
        staging_snapshot.get("manifest"),
        label="staging snapshot",
    )
    destination_database_scope = (
        str(destination_candidate.get("scope_plan_id") or "").strip(),
        str(destination_candidate.get("scope_plan_market_type") or "").strip().lower(),
    )
    staging_database_scope = (
        str(staging_snapshot.get("scope_plan_id") or "").strip(),
        str(staging_snapshot.get("scope_plan_market_type") or "").strip().lower(),
    )
    if (
        destination_scope != staging_scope
        or destination_database_scope != destination_scope[1:]
        or staging_database_scope != staging_scope[1:]
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
    destination_scope = _activation_scope(candidate_manifest, label="destination candidate")
    if destination_scope[0] != str(expected_binding.get("source_key") or "").strip().lower():
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


async def prepare_result_archive_candidate_evidence(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_id: str,
    frozen_binding_params: Mapping[str, Any],
) -> PreparedResultArchiveCandidate:
    """Attach copied allowed evidence to a pre-created local candidate only.

    ``session`` is an already-open caller transaction.  ``schema_name`` must
    be the configured PTG schema because the native frozen-binding store uses
    that schema to retain immutable attempt evidence.  The caller must prepare
    the destination candidate's source records and manifest independently;
    staged source metadata is compared, never adopted as destination authority.
    Fresh destination candidate attestation and activation remain separate.
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


__all__ = [
    "PreparedResultArchiveCandidate",
    "RESULT_ARCHIVE_CANDIDATE_PREPARATION_CONTRACT",
    "ResultArchiveCandidatePreparationError",
    "prepare_result_archive_candidate_evidence",
]
