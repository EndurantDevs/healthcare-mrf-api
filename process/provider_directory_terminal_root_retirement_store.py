# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional preview and apply storage for terminal root retirement."""

from __future__ import annotations

import json
from typing import Any

from process.provider_directory_terminal_root_retirement_contract import (
    RETIREMENT_METADATA_KEY,
    RETIREMENT_STATUS,
    RETIREMENT_VALID_FUNCTION,
    TerminalRootRetirementError,
    TerminalRootRetirementRequest,
    TerminalRootRetirementResult,
    TerminalRootRetirementSelection,
    canonical_json_sha256,
    quoted_relation,
)
from process.provider_directory_terminal_root_retirement_selection import (
    selected_terminal_root_retirement,
)


async def _require_read_committed(database: Any) -> None:
    isolation = await database.scalar(
        "SELECT pg_catalog.current_setting('transaction_isolation');"
    )
    if isolation != "read committed":
        raise TerminalRootRetirementError("state_invalid")


async def _require_valid_retirement(database: Any, dataset_id: str) -> None:
    is_valid = await database.scalar(
        f"SELECT {quoted_relation(RETIREMENT_VALID_FUNCTION)}(:dataset_id);",
        dataset_id=dataset_id,
    )
    if is_valid is not True:
        raise TerminalRootRetirementError("state_invalid")


def _selection_evidence_sha256(selection: TerminalRootRetirementSelection) -> str:
    return canonical_json_sha256(selection.marker_by_field["evidence"])


async def preview_terminal_root_retirement_transaction(
    database: Any,
    request: TerminalRootRetirementRequest,
) -> str:
    """Return the locked evidence token without changing any persisted row."""

    if request.expected_evidence_sha256 is not None:
        raise TerminalRootRetirementError("request_invalid")
    async with database.transaction():
        await _require_read_committed(database)
        selection = await selected_terminal_root_retirement(database, request)
        if selection.prior_status == RETIREMENT_STATUS:
            await _require_valid_retirement(database, request.dataset_id)
        return _selection_evidence_sha256(selection)


async def _apply_parent_cas(
    database: Any,
    selection: TerminalRootRetirementSelection,
) -> None:
    request = selection.request
    marker_json = json.dumps(
        selection.marker_by_field,
        sort_keys=True,
        separators=(",", ":"),
    )
    metadata_json = json.dumps(
        selection.observed_metadata,
        sort_keys=True,
        separators=(",", ":"),
    )
    updated_count = await database.status(
        f"""
        UPDATE {quoted_relation('provider_directory_endpoint_dataset')}
           SET status = :retirement_status,
               publication_metadata_json =
                   publication_metadata_json::jsonb
                   || pg_catalog.jsonb_build_object(
                        CAST(:marker_key AS text),
                        CAST(:marker_json AS jsonb)
                   )
         WHERE dataset_id = :dataset_id
           AND endpoint_id = :endpoint_id
           AND import_run_id = :owner_run_id
           AND acquisition_root_run_id = :root_run_id
           AND previous_dataset_id = :predecessor_id
           AND status = :prior_status
           AND is_current = false
           AND dataset_hash IS NULL
           AND validated_at IS NULL
           AND published_at IS NULL
           AND superseded_at IS NULL
           AND completion_proof_required_version IS NULL
           AND completion_proof_json IS NULL
           AND completion_proof_sha256 IS NULL
           AND publication_metadata_json::jsonb = CAST(:metadata_json AS jsonb)
           AND NOT (publication_metadata_json::jsonb ? :marker_key);
        """,
        retirement_status=RETIREMENT_STATUS,
        marker_key=RETIREMENT_METADATA_KEY,
        marker_json=marker_json,
        dataset_id=request.dataset_id,
        endpoint_id=request.endpoint_id,
        owner_run_id=request.owner_run_id,
        root_run_id=request.acquisition_root_run_id,
        predecessor_id=request.expected_current_dataset_id,
        prior_status=selection.prior_status,
        metadata_json=metadata_json,
    )
    if updated_count != 1:
        raise TerminalRootRetirementError("state_invalid")


async def apply_terminal_root_retirement_transaction(
    database: Any,
    request: TerminalRootRetirementRequest,
) -> TerminalRootRetirementResult:
    """Apply one evidence-token-bound parent-only CAS, or verify its replay."""

    if request.expected_evidence_sha256 is None:
        raise TerminalRootRetirementError("request_invalid")
    async with database.transaction():
        await _require_read_committed(database)
        selection = await selected_terminal_root_retirement(database, request)
        marker_sha256 = canonical_json_sha256(selection.marker_by_field)
        if selection.prior_status == RETIREMENT_STATUS:
            await _require_valid_retirement(database, request.dataset_id)
            return TerminalRootRetirementResult(
                retired=False,
                marker_sha256=marker_sha256,
            )
        if _selection_evidence_sha256(selection) != request.expected_evidence_sha256:
            raise TerminalRootRetirementError("evidence_changed")
        await _apply_parent_cas(database, selection)
        await _require_valid_retirement(database, request.dataset_id)
        return TerminalRootRetirementResult(
            retired=True,
            marker_sha256=marker_sha256,
        )


__all__ = (
    "apply_terminal_root_retirement_transaction",
    "preview_terminal_root_retirement_transaction",
)
