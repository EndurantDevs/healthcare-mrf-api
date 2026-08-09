# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional storage for one reviewed subset abandonment."""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONED_CHECKPOINT_STATE,
    ABANDONED_STATUS,
    ABANDONMENT_METADATA_KEY,
    ReviewedSubsetAbandonmentError,
    ReviewedSubsetAbandonmentResult,
    ReviewedSubsetAbandonmentSelection,
    _quoted_relation,
    _schema_name,
)
from process.provider_directory_fhir_subset_abandonment_selection import (
    selected_reviewed_subset_abandonment,
)

_CHECKPOINT_GUARD = "provider_directory_subset_abandonment_checkpoint_guard"
_DATASET_GUARD = "pd_subset_abandonment_dataset_consistency_guard"
_VALID_FUNCTION = "provider_directory_subset_abandonment_valid"


async def _force_abandonment_guards(database: Any, dataset_id: str) -> None:
    await database.status(
        "SET CONSTRAINTS "
        f'"{_schema_name()}"."{_CHECKPOINT_GUARD}", '
        f'"{_schema_name()}"."{_DATASET_GUARD}" IMMEDIATE;'
    )
    is_valid = await database.scalar(
        f"SELECT {_quoted_relation(_VALID_FUNCTION)}(:dataset_id);",
        dataset_id=dataset_id,
    )
    if is_valid is not True:
        raise ReviewedSubsetAbandonmentError("state")


async def _abandon_checkpoints(
    database: Any,
    selection: ReviewedSubsetAbandonmentSelection,
    checkpoint_rows: Sequence[Mapping[str, Any]],
) -> None:
    for checkpoint_row in checkpoint_rows:
        updated_count = await database.status(
            f"""
            UPDATE {_quoted_relation('provider_directory_pagination_checkpoint')}
               SET state = :abandoned_state,
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
            abandoned_state=ABANDONED_CHECKPOINT_STATE,
            canonical_api_base=selection.canonical_api_base,
            resource_type=checkpoint_row["resource_type"],
            source_scope_hash=selection.source_scope_sha256,
            root_run_id=selection.acquisition_root_run_id,
            dataset_id=selection.dataset_id,
            owner_run_id=selection.owner_run_id,
            retry_of_run_id=checkpoint_row.get("retry_of_run_id"),
            start_url_hash=checkpoint_row["start_url_hash"],
            next_url=checkpoint_row.get("next_url"),
            prior_state=checkpoint_row["state"],
            pages_processed=checkpoint_row["pages_processed"],
            rows_processed=checkpoint_row["rows_processed"],
            recent_hashes=json.dumps(checkpoint_row["recent_cursor_hashes"]),
            completeness=json.dumps(checkpoint_row["completeness_json"]),
            updated_at=checkpoint_row["updated_at"],
        )
        if updated_count != 1:
            raise ReviewedSubsetAbandonmentError("state")


async def _abandon_candidate(
    database: Any,
    selection: ReviewedSubsetAbandonmentSelection,
) -> None:
    """Seal one exact parent row and force both deferred guards."""

    updated_count = await database.status(
        f"""
        UPDATE {_quoted_relation('provider_directory_endpoint_dataset')}
           SET status = :abandoned_status,
               resource_count = :resource_count,
               publication_metadata_json =
                   publication_metadata_json::jsonb
                   || pg_catalog.jsonb_build_object(
                        CAST(:abandonment_key AS text),
                        CAST(:abandonment_marker AS jsonb)
                   )
         WHERE dataset_id = :dataset_id
           AND endpoint_id = :endpoint_id
           AND import_run_id = :owner_run_id
           AND acquisition_root_run_id = :root_run_id
           AND status = :prior_status
           AND is_current = false
           AND completion_proof_required_version = 3
           AND completion_proof_json IS NULL
           AND completion_proof_sha256 IS NULL
           AND resource_count = :observed_resource_count
           AND publication_metadata_json::jsonb = CAST(:observed_metadata AS jsonb)
           AND NOT (publication_metadata_json::jsonb ? :abandonment_key);
        """,
        abandoned_status=ABANDONED_STATUS,
        resource_count=selection.marker_by_field["resource_count"],
        abandonment_key=ABANDONMENT_METADATA_KEY,
        abandonment_marker=json.dumps(
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
            selection.observed_metadata,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    if updated_count != 1:
        raise ReviewedSubsetAbandonmentError("state")
    await _force_abandonment_guards(database, selection.dataset_id)


async def sync_reviewed_subset_abandonment_transaction(
    database: Any,
    expected_source_id: str,
    resource_types: tuple[str, ...],
    *,
    held_pagination_guard_key: str | None = None,
) -> ReviewedSubsetAbandonmentResult:
    """Lock and seal the sole expired reviewed subset root."""

    async with database.transaction():
        isolation = await database.scalar(
            "SELECT pg_catalog.current_setting('transaction_isolation');"
        )
        if isolation != "read committed":
            raise ReviewedSubsetAbandonmentError("state")
        selection, checkpoint_rows = await selected_reviewed_subset_abandonment(
            database,
            expected_source_id,
            resource_types,
            held_pagination_guard_key=held_pagination_guard_key,
        )
        if not checkpoint_rows:
            is_valid = await database.scalar(
                f"SELECT {_quoted_relation(_VALID_FUNCTION)}(:dataset_id);",
                dataset_id=selection.dataset_id,
            )
            if is_valid is not True:
                raise ReviewedSubsetAbandonmentError("state")
            return ReviewedSubsetAbandonmentResult(abandoned=False)
        await _abandon_checkpoints(database, selection, checkpoint_rows)
        await _abandon_candidate(database, selection)
        return ReviewedSubsetAbandonmentResult(abandoned=True)
