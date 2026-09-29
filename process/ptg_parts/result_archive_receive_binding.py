# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Derive destination-owned frozen input for one authenticated PTG archive.

The restored source rows remain evidence.  This adapter reads and authenticates
that evidence in the caller transaction, replaces only the source filing
identity with a deterministic destination identity, and returns parameters for
the existing result-archive candidate initializer.  It creates no local rows,
registration, attestation, pointer, or activation authority.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
from dataclasses import dataclass
from typing import Any, Mapping

from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.frozen_rate_binding import (
    frozen_internal_run_id,
    frozen_rate_binding_from_params,
    normalize_protected_frozen_rate_params,
)
from process.ptg_parts.frozen_rate_candidate import validate_frozen_candidate_evidence
from process.ptg_parts.frozen_rate_files import FrozenRateFileMismatchError
from process.ptg_parts.ptg2_invalid_price_exclusion import INVALID_PRICE_EXCLUSION_POLICY_FIELD
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.result_archive_source_authority import (
    PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT,
    PtgResultArchiveSourceAuthorityError,
    validate_ptg_result_archive_source_authority,
)

RESULT_ARCHIVE_RECEIVE_BINDING_CONTRACT = "ptg_result_archive_receive_binding_v1"


def published_result_authority_sha256(receipt: Mapping[str, Any]) -> str:
    """Hash the closed authenticated result receipt, without inventing file proof."""
    from process.ptg_parts.result_archive_published_authority import validate_ptg_published_result_source_authority

    validated = validate_ptg_published_result_source_authority(receipt)
    return hashlib.sha256(canonical_json_dumps(validated).encode()).hexdigest()


def published_result_run_id(schema_name: str, snapshot_id: str, receipt: Mapping[str, Any]) -> str:
    """Derive the destination-owned attempt identity from the complete source receipt."""
    payload = canonical_json_dumps([schema_name, snapshot_id, published_result_authority_sha256(receipt)])
    return "ptg2:archive-" + hashlib.sha256(payload.encode()).hexdigest()[:56]


async def _lock_published_staging_tables(session: Any, staging_schema: str) -> None:
    """Fence inserts and updates while authenticating the selected restored scopes."""
    from sqlalchemy import text

    from process.ptg_parts.db_tables import _quote_ident

    # Staging is operation-owned; table locks also fence inserts into selected scopes.
    await initialization._lock_staging_source_family(session, staging_schema)
    for table in (
        "ptg2_snapshot",
        "ptg2_import_run",
        "ptg2_v3_snapshot_binding",
        "ptg2_v3_snapshot_scope",
        "ptg2_v3_snapshot_plan_scope",
        "ptg2_v3_snapshot_layout",
        "ptg2_v4_snapshot_map_root",
        "ptg2_v4_finalizer_map_root",
        "ptg2_frozen_source_file_binding",
        "ptg2_artifact_manifest",
    ):
        await session.execute(text(f"LOCK TABLE {_quote_ident(staging_schema)}.{table} IN SHARE MODE"))


async def authenticate_published_result_stage(
    session: Any, *, staging_schema_name: str, source_snapshot_key: int, receipt: Mapping[str, Any]
) -> initialization._AuthenticatedStagedCandidate:
    """Authenticate the complete selected result against its pinned source receipt."""
    from process.ptg_parts.result_archive_published_authority import (
        _authority_from_identity,
        validate_ptg_published_result_source_authority,
    )
    from process.ptg_parts.result_archive_published_identity import load_published_result_identity

    initialization._require_transaction(session)
    staging = initialization._required_schema(staging_schema_name, field_name="staging_schema_name")
    validated = validate_ptg_published_result_source_authority(receipt)
    identity = validated["identity"]
    if type(source_snapshot_key) is not int or source_snapshot_key != identity["snapshot_key"]:
        raise initialization.ResultArchiveCandidateInitializationError("published result source layout differs")
    await _lock_published_staging_tables(session, staging)
    from process.ptg_parts.db_tables import _quote_ident

    observed = await load_published_result_identity(
        session, schema_name=staging, snapshot_id=identity["snapshot_id"], lock=True
    )
    if _authority_from_identity(validated["operation_id"], observed).as_dict() != validated:
        raise initialization.ResultArchiveCandidateInitializationError("published result restored authority differs")
    source_snapshot_by_field = await initialization._one(
        session,
        f"SELECT snapshot.manifest, internal_run.options -> '{INVALID_PRICE_EXCLUSION_POLICY_FIELD}' "
        f"AS invalid_price_exclusion_policy FROM {_quote_ident(staging)}.ptg2_snapshot AS snapshot "
        f"JOIN {_quote_ident(staging)}.ptg2_import_run AS internal_run "
        "ON internal_run.import_run_id=snapshot.import_run_id WHERE snapshot.snapshot_id=:snapshot_id",
        {"snapshot_id": identity["snapshot_id"]},
        "published source snapshot",
    )
    return initialization._AuthenticatedStagedCandidate(
        source_snapshot_id=identity["snapshot_id"],
        source_manifest=source_snapshot_by_field["manifest"],
        primary_plan_id=identity["plan_id"],
        primary_plan_market_type=identity["plan_market_type"],
        coverage_scope_id=bytes.fromhex(identity["coverage_scope_id"]),
        plan_scopes=await initialization._staged_plan_scopes(
            session, staging_schema=staging, source_snapshot_id=identity["snapshot_id"]
        ),
        source_records=tuple(
            await initialization._rows(
                session,
                initialization.CANDIDATE_SOURCE_RECORDS_SQL.format(schema=_quote_ident(staging)),
                {"snapshot_id": identity["snapshot_id"]},
            )
        ),
        invalid_price_exclusion_policy=source_snapshot_by_field["invalid_price_exclusion_policy"],
    )


def _validate_local_published_identity(
    candidate_by_field: Mapping[str, Any], receipt: Mapping[str, Any], schema_name: str, snapshot_id: str
) -> None:
    """Reject changes to destination attempt, manifest, month or protected provenance."""
    from process.ptg_parts.result_archive_source_authority import result_archive_manifest_sha256

    identity = receipt["identity"]
    manifest = initialization._mapping(candidate_by_field.get("manifest"))
    options_by_name = initialization._mapping(candidate_by_field.get("options"))
    stable_manifest_by_name = {key: field_value for key, field_value in manifest.items() if key != "serving_index"}
    if (
        candidate_by_field["import_run_id"] != published_result_run_id(schema_name, snapshot_id, receipt)
        or str(candidate_by_field["import_month"]) != identity["import_month"]
        or str(candidate_by_field["run_import_month"]) != identity["import_month"]
        or manifest.get("import_month") != identity["import_month"]
        or options_by_name.get("result_archive_source") != receipt
        or manifest.get("result_archive_source") != receipt
        or options_by_name.get("source_key") != identity["source_key"]
        or manifest.get("source_key") != identity["source_key"]
        or initialization._mapping(manifest.get("activation")).get("source_key") != identity["source_key"]
        or initialization._mapping(manifest.get("activation")).get("plan_id", identity["plan_id"])
        != identity["plan_id"]
        or initialization._mapping(manifest.get("activation")).get("plan_market_type", identity["plan_market_type"])
        != identity["plan_market_type"]
        or candidate_by_field["plan_id"] != identity["plan_id"]
        or candidate_by_field["plan_market_type"] != identity["plan_market_type"]
        or bytes(candidate_by_field["coverage_scope_id"]).hex() != identity["coverage_scope_id"]
        or result_archive_manifest_sha256(stable_manifest_by_name)
        != options_by_name.get("result_archive_candidate_manifest_sha256")
        or candidate_by_field.get("binding_payload") is not None
    ):
        raise initialization.ResultArchiveCandidateInitializationError("published result local authority changed")
    validate_frozen_candidate_evidence(
        manifest, candidate_run_id=candidate_by_field["import_run_id"], database_binding=None, database_sources=None
    )


async def validate_local_published_result(
    session: Any, *, schema_name: str, snapshot_id: str, receipt: Mapping[str, Any]
) -> str:
    """Rejoin a local candidate to its exact receipt, scope and source assignments."""
    from process.ptg_parts.db_tables import _quote_ident
    from process.ptg_parts.result_archive_published_authority import validate_ptg_published_result_source_authority

    receipt = validate_ptg_published_result_source_authority(receipt)
    identity = receipt["identity"]
    schema = _quote_ident(initialization._required_schema(schema_name, field_name="schema_name"))
    candidate_by_field = await initialization._one(
        session,
        f"""SELECT snapshot.import_run_id, snapshot.import_month, snapshot.manifest,
                   run.options, run.import_month AS run_import_month,
                   scope.plan_id, scope.plan_market_type, scope.coverage_scope_id,
                   frozen.binding_payload
              FROM {schema}.ptg2_snapshot snapshot
              JOIN {schema}.ptg2_import_run run ON run.import_run_id=snapshot.import_run_id
              JOIN {schema}.ptg2_v3_snapshot_scope scope ON scope.snapshot_id=snapshot.snapshot_id
              LEFT JOIN {schema}.ptg2_frozen_source_file_binding frozen
                ON frozen.internal_run_id=snapshot.import_run_id
             WHERE snapshot.snapshot_id=:snapshot_id
             FOR UPDATE OF snapshot, run, scope""",
        {"snapshot_id": snapshot_id},
        "published local candidate",
    )
    _validate_local_published_identity(candidate_by_field, receipt, schema_name, snapshot_id)
    plan_scopes = await initialization._staged_plan_scopes(
        session, staging_schema=schema_name, source_snapshot_id=snapshot_id
    )
    assignments = await initialization._rows(
        session,
        f"""SELECT source_key, source_type, identity_kind, identity_sha256, raw_container_sha256,
                   logical_json_sha256, logical_hash_deferred, source_trace_set_hash
              FROM {schema}.ptg2_v3_snapshot_source WHERE snapshot_id=:snapshot_id
             ORDER BY source_key FOR SHARE""",
        {"snapshot_id": snapshot_id},
    )
    if (
        len(assignments) != identity["source_count"]
        or hashlib.sha256(canonical_json_dumps(assignments).encode()).hexdigest()
        != identity["source_assignments_sha256"]
        or hashlib.sha256(canonical_json_dumps([list(scope) for scope in plan_scopes]).encode()).hexdigest()
        != identity["plan_scopes_sha256"]
    ):
        raise initialization.ResultArchiveCandidateInitializationError(
            "published result local source or plan scope changed"
        )
    return published_result_authority_sha256(receipt)


_RECEIVE_BINDING_ERRORS = (
    TypeError,
    ValueError,
    FrozenRateFileMismatchError,
    PtgResultArchiveSourceAuthorityError,
)


@dataclass(frozen=True)
class _ReceiveContext:
    destination_schema: str
    staging_schema: str
    snapshot_key: int
    destination_snapshot: str
    source_key: str
    source_receipt: Mapping[str, Any]
    destination_filing_id: str


def _destination_filing_id(
    *,
    schema_name: str,
    destination_snapshot_id: str,
    source_key: str,
    source_receipt: Mapping[str, Any],
) -> str:
    """Return one bounded, replay-stable destination filing identity."""

    payload = canonical_json_dumps(
        {
            "contract": RESULT_ARCHIVE_RECEIVE_BINDING_CONTRACT,
            "schema_name": schema_name,
            "destination_snapshot_id": destination_snapshot_id,
            "source_key": source_key,
            "source_snapshot_id": source_receipt["snapshot_id"],
            "source_manifest_sha256": source_receipt["snapshot_manifest_sha256"],
            "source_frozen_binding_sha256": source_receipt["frozen_binding_sha256"],
        }
    )
    return "archive-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:56]


def _received_parameters(
    *,
    staged: Mapping[str, Any],
    destination_filing_id: str,
    source_key: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build the strict local parameter projection and its canonical binding."""

    source_binding = initialization._mapping(staged.get("binding_payload"))
    source_manifest = initialization._mapping(staged.get("manifest"))
    parameter_mapping = {
        "import_id": destination_filing_id,
        "source_file_import_id": destination_filing_id,
        "source_key": source_key,
        "import_month": source_binding.get("import_month"),
        "plan_ids": copy.deepcopy(source_binding.get("plan_ids")),
        "plan_market_types": copy.deepcopy(source_binding.get("plan_market_types")),
        "frozen_rate_file_set_contract": source_binding.get("frozen_rate_file_set_contract"),
        "frozen_rate_files": copy.deepcopy(source_manifest.get("frozen_rate_files")),
        "frozen_rate_file_set_sha256": source_binding.get("frozen_rate_file_set_sha256"),
        "frozen_rate_file_count": source_binding.get("frozen_rate_file_count"),
    }
    if INVALID_PRICE_EXCLUSION_POLICY_FIELD in source_binding:
        parameter_mapping[INVALID_PRICE_EXCLUSION_POLICY_FIELD] = copy.deepcopy(
            source_binding[INVALID_PRICE_EXCLUSION_POLICY_FIELD]
        )
    try:
        normalized = normalize_protected_frozen_rate_params(parameter_mapping)
        local_binding = frozen_rate_binding_from_params(normalized)
    except ValueError as error:
        raise initialization.ResultArchiveCandidateInitializationError(
            "archive candidate initialization received frozen input is invalid"
        ) from error
    if local_binding is None:
        raise initialization.ResultArchiveCandidateInitializationError(
            "archive candidate initialization received frozen input is unavailable"
        )
    return normalized, local_binding


def _validated_receive_context(
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_id: str,
    source_key: str,
    authenticated_source_archive_metadata: Mapping[str, Any],
) -> _ReceiveContext:
    destination_schema = initialization._required_schema(schema_name, field_name="schema_name")
    staging_schema = initialization._required_schema(
        staging_schema_name,
        field_name="staging_schema_name",
    )
    if destination_schema == staging_schema:
        raise ValueError("staging_schema_name must differ from schema_name")
    if destination_schema != resolve_ptg2_schema():
        raise ValueError("schema_name must match the configured PTG schema")
    if type(source_snapshot_key) is not int or source_snapshot_key < 0:
        raise ValueError("source_snapshot_key must be non-negative")
    destination_snapshot = initialization._required_snapshot_id(
        destination_snapshot_id,
        field_name="destination snapshot",
    )
    selected_source_key = initialization._required_source_key(source_key)
    source_receipt = validate_ptg_result_archive_source_authority(authenticated_source_archive_metadata)
    if source_receipt.get("contract") == PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT:
        raise PtgResultArchiveSourceAuthorityError("frozen receive requires frozen source authority")
    if initialization._required_source_key(source_receipt["source_key"]) != selected_source_key:
        raise initialization.ResultArchiveCandidateInitializationError(
            "archive candidate initialization received source key differs"
        )
    destination_filing_id = _destination_filing_id(
        schema_name=destination_schema,
        destination_snapshot_id=destination_snapshot,
        source_key=selected_source_key,
        source_receipt=source_receipt,
    )
    if (
        destination_snapshot == source_receipt["snapshot_id"]
        or destination_filing_id == source_receipt["source_file_import_id"]
    ):
        raise initialization.ResultArchiveCandidateInitializationError(
            "archive candidate initialization requires new local attempt identities"
        )
    return _ReceiveContext(
        destination_schema=destination_schema,
        staging_schema=staging_schema,
        snapshot_key=source_snapshot_key,
        destination_snapshot=destination_snapshot,
        source_key=selected_source_key,
        source_receipt=source_receipt,
        destination_filing_id=destination_filing_id,
    )


def _validate_received_source_evidence(
    *,
    authenticated: initialization._AuthenticatedStagedCandidate,
    local_binding: Mapping[str, Any],
    source_receipt: Mapping[str, Any],
) -> None:
    source_binding = initialization._source_binding_for_receipt(
        local_binding,
        source_receipt["source_file_import_id"],
    )
    validate_frozen_candidate_evidence(
        authenticated.source_manifest,
        candidate_run_id=frozen_internal_run_id(source_receipt["source_file_import_id"]),
        database_binding=source_binding,
        database_sources=authenticated.source_records,
    )


async def receive_frozen_binding_params(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_id: str,
    source_key: str,
    authenticated_source_archive_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    """Authenticate staged evidence and derive a new local frozen binding.

    The caller owns the transaction and subsequently passes the returned map to
    ``initialize_result_archive_candidate`` in that same transaction.
    """

    initialization._require_transaction(session)
    try:
        context = _validated_receive_context(
            schema_name=schema_name,
            staging_schema_name=staging_schema_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_id=destination_snapshot_id,
            source_key=source_key,
            authenticated_source_archive_metadata=authenticated_source_archive_metadata,
        )
        staged = await initialization._locked_staged_candidate_row(
            session,
            staging_schema=context.staging_schema,
            source_snapshot_key=context.snapshot_key,
        )
        parameters, local_binding = _received_parameters(
            staged=staged,
            destination_filing_id=context.destination_filing_id,
            source_key=context.source_key,
        )
        authenticated = await initialization._authenticated_staged_candidate(
            session,
            staging_schema=context.staging_schema,
            source_snapshot_key=context.snapshot_key,
            local_binding=local_binding,
            source_receipt=context.source_receipt,
        )
        _validate_received_source_evidence(
            authenticated=authenticated,
            local_binding=local_binding,
            source_receipt=context.source_receipt,
        )
        return parameters
    except asyncio.CancelledError:
        raise
    except initialization.ResultArchiveCandidateInitializationError:
        raise
    except _RECEIVE_BINDING_ERRORS as error:
        raise initialization.ResultArchiveCandidateInitializationError(
            "archive candidate initialization receive binding is invalid"
        ) from error


__all__ = ["receive_frozen_binding_params"]
