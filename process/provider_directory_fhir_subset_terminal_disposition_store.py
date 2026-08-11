# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional storage for one reviewed mixed-terminal disposition."""

from __future__ import annotations

import json
from typing import Any, Awaitable, Callable, Mapping, Sequence

from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONMENT_METADATA_KEY,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    TERMINAL_DISPOSITION_CHECKPOINT_STATE,
    TERMINAL_DISPOSITION_METADATA_KEY,
    TERMINAL_DISPOSITION_STATUS,
    ReviewedSubsetTerminalDispositionError,
    ReviewedSubsetTerminalDispositionResult,
    ReviewedSubsetTerminalDispositionSelection,
)
from process.provider_directory_fhir_subset_terminal_disposition_selection import (
    selected_reviewed_subset_terminal_disposition,
)
from process.provider_directory_fhir_subset_terminal_disposition_util import (
    quoted_relation,
    schema_name,
)


_CHECKPOINT_GUARD = (
    "provider_directory_subset_terminal_disposition_checkpoint_guard"
)
_DATASET_GUARD = "pd_subset_terminal_disposition_dataset_consistency_guard"
_VALID_FUNCTION = "provider_directory_subset_terminal_disposition_valid"


async def _force_terminal_disposition_guards(
    database: Any,
    dataset_id: str,
) -> None:
    await database.status(
        "SET CONSTRAINTS "
        f'"{schema_name()}"."{_CHECKPOINT_GUARD}", '
        f'"{schema_name()}"."{_DATASET_GUARD}" IMMEDIATE;'
    )
    is_valid = await database.scalar(
        f"SELECT {quoted_relation(_VALID_FUNCTION)}(:dataset_id);",
        dataset_id=dataset_id,
    )
    if is_valid is not True:
        raise ReviewedSubsetTerminalDispositionError("state")


async def _seal_checkpoints(
    database: Any,
    selection: ReviewedSubsetTerminalDispositionSelection,
    checkpoint_rows: Sequence[Mapping[str, Any]],
) -> None:
    for checkpoint in checkpoint_rows:
        updated_count = await database.status(
            f"""
            UPDATE {quoted_relation('provider_directory_pagination_checkpoint')}
               SET state = :disposed_state,
                   completed_at = COALESCE(
                       completed_at,
                       pg_catalog.transaction_timestamp()
                   ),
                   updated_at = pg_catalog.transaction_timestamp()
             WHERE canonical_api_base = :canonical_api_base
               AND resource_type = :resource_type
               AND source_scope_hash = :source_scope_hash
               AND acquisition_root_run_id = :root_run_id
               AND dataset_id = :dataset_id
               AND owner_run_id = :owner_run_id
               AND retry_of_run_id IS NOT DISTINCT FROM :retry_of_run_id
               AND start_url_hash = :start_url_hash
               AND next_url IS NOT DISTINCT FROM :next_url
               AND state = :prior_state
               AND pages_processed = :pages_processed
               AND rows_processed = :rows_processed
               AND recent_cursor_hashes::jsonb = CAST(:recent_hashes AS jsonb)
               AND completeness_json::jsonb = CAST(:completeness AS jsonb)
               AND updated_at IS NOT DISTINCT FROM :updated_at;
            """,
            disposed_state=TERMINAL_DISPOSITION_CHECKPOINT_STATE,
            canonical_api_base=selection.canonical_api_base,
            resource_type=checkpoint["resource_type"],
            source_scope_hash=selection.source_scope_sha256,
            root_run_id=selection.acquisition_root_run_id,
            dataset_id=selection.dataset_id,
            owner_run_id=selection.owner_run_id,
            retry_of_run_id=checkpoint.get("retry_of_run_id"),
            start_url_hash=checkpoint["start_url_hash"],
            next_url=checkpoint.get("next_url"),
            prior_state=checkpoint["state"],
            pages_processed=checkpoint["pages_processed"],
            rows_processed=checkpoint["rows_processed"],
            recent_hashes=json.dumps(checkpoint["recent_cursor_hashes"]),
            completeness=json.dumps(checkpoint["completeness_json"]),
            updated_at=checkpoint["updated_at"],
        )
        if updated_count != 1:
            raise ReviewedSubsetTerminalDispositionError("state")


async def _seal_candidate(
    database: Any,
    selection: ReviewedSubsetTerminalDispositionSelection,
) -> None:
    updated_count = await database.status(
        f"""
        UPDATE {quoted_relation('provider_directory_endpoint_dataset')}
           SET status = :disposed_status,
               resource_count = :resource_count,
               publication_metadata_json =
                   publication_metadata_json::jsonb
                   || pg_catalog.jsonb_build_object(
                        CAST(:disposition_key AS text),
                        CAST(:disposition_marker AS jsonb)
                   )
         WHERE dataset_id = :dataset_id
           AND endpoint_id = :endpoint_id
           AND import_run_id = :owner_run_id
           AND acquisition_root_run_id = :root_run_id
           AND status = :prior_status
           AND is_current = false
           AND validated_at IS NULL
           AND published_at IS NULL
           AND superseded_at IS NULL
           AND completion_proof_required_version = 3
           AND completion_proof_json IS NULL
           AND completion_proof_sha256 IS NULL
           AND resource_count = :observed_resource_count
           AND publication_metadata_json::jsonb = CAST(:observed_metadata AS jsonb)
           AND NOT (publication_metadata_json::jsonb ? :abandonment_key)
           AND NOT (publication_metadata_json::jsonb ? :disposition_key);
        """,
        disposed_status=TERMINAL_DISPOSITION_STATUS,
        resource_count=selection.marker_by_field["resource_count"],
        disposition_key=TERMINAL_DISPOSITION_METADATA_KEY,
        disposition_marker=json.dumps(
            selection.marker_by_field,
            sort_keys=True,
            separators=(",", ":"),
        ),
        dataset_id=selection.dataset_id,
        endpoint_id=selection.endpoint_id,
        owner_run_id=selection.owner_run_id,
        root_run_id=selection.acquisition_root_run_id,
        prior_status=selection.prior_status,
        observed_resource_count=selection.observed_resource_count,
        observed_metadata=json.dumps(
            selection.observed_candidate_metadata,
            sort_keys=True,
            separators=(",", ":"),
        ),
        abandonment_key=ABANDONMENT_METADATA_KEY,
    )
    if updated_count != 1:
        raise ReviewedSubsetTerminalDispositionError("state")
    await _force_terminal_disposition_guards(database, selection.dataset_id)


_SelectionFunction = Callable[
    [Any, str],
    Awaitable[
        tuple[
            ReviewedSubsetTerminalDispositionSelection,
            tuple[dict[str, Any], ...],
        ]
    ],
]


async def _sync_terminal_disposition_transaction(
    database: Any,
    expected_source_id: str,
    selection_function: _SelectionFunction,
) -> ReviewedSubsetTerminalDispositionResult:
    async with database.transaction():
        isolation = await database.scalar(
            "SELECT pg_catalog.current_setting('transaction_isolation');"
        )
        if isolation != "read committed":
            raise ReviewedSubsetTerminalDispositionError("state")
        selection, checkpoint_rows = await selection_function(
            database,
            expected_source_id,
        )
        if not checkpoint_rows:
            is_valid = await database.scalar(
                f"SELECT {quoted_relation(_VALID_FUNCTION)}(:dataset_id);",
                dataset_id=selection.dataset_id,
            )
            if is_valid is not True:
                raise ReviewedSubsetTerminalDispositionError("state")
            return ReviewedSubsetTerminalDispositionResult(disposed=False)
        await _seal_checkpoints(database, selection, checkpoint_rows)
        await _seal_candidate(database, selection)
        return ReviewedSubsetTerminalDispositionResult(disposed=True)


async def sync_reviewed_subset_terminal_disposition_transaction(
    database: Any,
    expected_source_id: str,
) -> ReviewedSubsetTerminalDispositionResult:
    """Lock and seal the sole reviewed mixed-terminal retained root."""

    return await _sync_terminal_disposition_transaction(
        database,
        expected_source_id,
        selected_reviewed_subset_terminal_disposition,
    )


async def sync_v4_terminal_disposition(
    database: Any,
    expected_source_id: str,
) -> ReviewedSubsetTerminalDispositionResult:
    """Lock and seal the sole reviewed direct-v4 retained root."""

    from process.provider_directory_fhir_subset_terminal_disposition_v4_selection import (
        selected_direct_v4_terminal_disposition,
    )

    return await _sync_terminal_disposition_transaction(
        database,
        expected_source_id,
        selected_direct_v4_terminal_disposition,
    )


async def sync_v5_terminal_disposition(
    database: Any,
    expected_source_id: str,
) -> ReviewedSubsetTerminalDispositionResult:
    """Lock and seal the sole reviewed direct-v5 HTTP-410 root."""

    from process.provider_directory_fhir_subset_terminal_disposition_v5_selection import (
        selected_direct_v5_terminal_disposition,
    )

    return await _sync_terminal_disposition_transaction(
        database,
        expected_source_id,
        selected_direct_v5_terminal_disposition,
    )


__all__ = (
    "sync_v4_terminal_disposition",
    "sync_v5_terminal_disposition",
    "sync_reviewed_subset_terminal_disposition_transaction",
)
