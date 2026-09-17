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
from typing import Any, Mapping

from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.frozen_rate_binding import (
    FROZEN_RATE_FILE_BINDING_OPTION,
    frozen_internal_run_id,
    frozen_rate_binding_from_params,
    normalize_protected_frozen_rate_params,
)
from process.ptg_parts.frozen_rate_candidate import validate_frozen_candidate_evidence
from process.ptg_parts.ptg2_invalid_price_exclusion import INVALID_PRICE_EXCLUSION_POLICY_FIELD
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts.result_archive_source_authority import validate_ptg_result_archive_source_authority

RESULT_ARCHIVE_RECEIVE_BINDING_CONTRACT = "ptg_result_archive_receive_binding_v1"


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
    parameters = {
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
        parameters[INVALID_PRICE_EXCLUSION_POLICY_FIELD] = copy.deepcopy(
            source_binding[INVALID_PRICE_EXCLUSION_POLICY_FIELD]
        )
    try:
        normalized = normalize_protected_frozen_rate_params(parameters)
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
        snapshot_key = source_snapshot_key
        destination_snapshot = initialization._required_snapshot_id(
            destination_snapshot_id,
            field_name="destination snapshot",
        )
        selected_source_key = initialization._required_source_key(source_key)
        source_receipt = validate_ptg_result_archive_source_authority(authenticated_source_archive_metadata)
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
        staged = await initialization._locked_staged_candidate_row(
            session,
            staging_schema=staging_schema,
            source_snapshot_key=snapshot_key,
        )
        parameters, local_binding = _received_parameters(
            staged=staged,
            destination_filing_id=destination_filing_id,
            source_key=selected_source_key,
        )
        authenticated = await initialization._authenticated_staged_candidate(
            session,
            staging_schema=staging_schema,
            source_snapshot_key=snapshot_key,
            local_binding=local_binding,
            source_receipt=source_receipt,
        )
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
        if (
            initialization._mapping(authenticated.source_manifest).get(FROZEN_RATE_FILE_BINDING_OPTION)
            != source_binding
        ):
            raise initialization.ResultArchiveCandidateInitializationError(
                "archive candidate initialization received source binding differs"
            )
        return parameters
    except asyncio.CancelledError:
        raise
    except initialization.ResultArchiveCandidateInitializationError:
        raise
    except (TypeError, ValueError, RuntimeError) as error:
        raise initialization.ResultArchiveCandidateInitializationError(
            "archive candidate initialization receive binding is invalid"
        ) from error


__all__ = ["receive_frozen_binding_params"]
