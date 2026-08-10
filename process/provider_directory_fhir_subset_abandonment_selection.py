# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Locked evidence selection for reviewed subset abandonment."""

from __future__ import annotations

import json
import re
from typing import Any, Mapping

from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONED_STATUS,
    ABANDONMENT_METADATA_KEY,
    ELIGIBLE_PRIOR_STATUSES,
    ReviewedSubsetAbandonmentError,
    ReviewedSubsetAbandonmentSelection,
    _json_object,
    _json_text_tuple,
    _quoted_relation,
    _row_mapping,
    _text,
    abandonment_marker,
    validated_abandonment_marker,
    validated_terminal_diagnostics,
)
from process.provider_directory_fhir_subset_abandonment_evidence import (
    require_distinct_scope_domains,
    retained_evidence_counts,
    validated_checkpoint_summary,
)
from process.provider_directory_fhir_subset_abandonment_policy import (
    has_matching_reviewed_root_policy as _has_matching_reviewed_root_policy,
    reviewed_abandonment_source_state as _reviewed_source_state,
)
from process.provider_directory_fhir_subset_identity import (
    subset_source_endpoint_identity,
)


def _is_pending_source(metadata_by_field: Mapping[str, Any]) -> bool:
    return _reviewed_source_state(metadata_by_field) == "pending"


def _is_activated_source(metadata_by_field: Mapping[str, Any]) -> bool:
    return _reviewed_source_state(metadata_by_field) == "activated"


def _configured_endpoint_id(source_row: Mapping[str, Any]) -> str | None:
    try:
        return subset_source_endpoint_identity(source_row)[1]
    except ValueError:
        return None


async def _initial_source_row(
    database: Any,
    expected_source_id: str,
) -> dict[str, Any]:
    source_rows = await database.all(
        f"""
        SELECT source.*
          FROM {_quoted_relation('provider_directory_source')} AS source
         WHERE source.source_id = :source_id
         LIMIT 2;
        """,
        source_id=expected_source_id,
    )
    if len(source_rows) != 1:
        raise ReviewedSubsetAbandonmentError("state")
    source_row = _row_mapping(source_rows[0])
    source_metadata = _json_object(source_row.get("metadata_json"))
    if (
        _text(source_row.get("source_id")) != expected_source_id
        or _text(source_row.get("endpoint_id")) is None
        or _configured_endpoint_id(source_row) is None
        or _text(source_row.get("canonical_api_base")) is None
        or not (
            _is_pending_source(source_metadata) or _is_activated_source(source_metadata)
        )
    ):
        raise ReviewedSubsetAbandonmentError("state")
    return source_row


async def _lock_endpoint_scope(
    database: Any,
    source_row: Mapping[str, Any],
    held_pagination_guard_key: str | None,
) -> None:
    endpoint_id = _configured_endpoint_id(source_row) or ""
    canonical_api_base = _text(source_row.get("canonical_api_base")) or ""
    pagination_lock_key = f"provider-directory-pagination:{canonical_api_base}"
    if (
        held_pagination_guard_key is not None
        and held_pagination_guard_key != pagination_lock_key
    ):
        raise ReviewedSubsetAbandonmentError("state")
    is_acquired = await database.scalar(
        """
        SELECT pg_catalog.pg_try_advisory_xact_lock(
                   pg_catalog.hashtextextended(CAST(:lock_key AS text), 0)
               );
        """,
        lock_key=pagination_lock_key,
    )
    if is_acquired is not True:
        raise ReviewedSubsetAbandonmentError("busy")
    if held_pagination_guard_key is None:
        endpoint_lock_acquired = await database.scalar(
            """
            SELECT pg_catalog.pg_try_advisory_xact_lock(
                       pg_catalog.hashtextextended(CAST(:endpoint_id AS text), 0)
                   );
            """,
            endpoint_id=endpoint_id,
        )
        if endpoint_lock_acquired is not True:
            raise ReviewedSubsetAbandonmentError("busy")
    else:
        await database.scalar(
            """
            SELECT pg_catalog.pg_advisory_xact_lock(
                       pg_catalog.hashtextextended(CAST(:endpoint_id AS text), 0)
                   );
            """,
            endpoint_id=endpoint_id,
        )
    endpoint_rows = await database.all(
        f"""
        SELECT endpoint.endpoint_id
          FROM {_quoted_relation('provider_directory_api_endpoint')} AS endpoint
         WHERE endpoint.endpoint_id = :endpoint_id
         FOR UPDATE OF endpoint;
        """,
        endpoint_id=endpoint_id,
    )
    if len(endpoint_rows) != 1:
        raise ReviewedSubsetAbandonmentError("state")
    await database.status(
        "LOCK TABLE "
        f"{_quoted_relation('provider_directory_source')} "
        "IN SHARE MODE;"
    )


async def _locked_source_row(
    database: Any,
    expected_source_id: str,
    configured_endpoint_id: str,
    serving_endpoint_id: str,
    canonical_api_base: str,
) -> dict[str, Any]:
    source_rows = await database.all(
        f"""
        SELECT source.*
         FROM {_quoted_relation('provider_directory_source')} AS source
         WHERE source.source_id = :source_id
            OR source.endpoint_id = :endpoint_id
            OR source.metadata_json::jsonb
                   ->> 'provider_directory_configured_endpoint_id'
                   = :endpoint_id
         ORDER BY source.source_id
         FOR UPDATE OF source;
        """,
        source_id=expected_source_id,
        endpoint_id=configured_endpoint_id,
    )
    if len(source_rows) != 1:
        raise ReviewedSubsetAbandonmentError("evidence")
    source_row = _row_mapping(source_rows[0])
    source_metadata = _json_object(source_row.get("metadata_json"))
    if (
        _text(source_row.get("source_id")) != expected_source_id
        or _text(source_row.get("endpoint_id")) != serving_endpoint_id
        or _configured_endpoint_id(source_row) != configured_endpoint_id
        or _text(source_row.get("canonical_api_base")) != canonical_api_base
        or not (
            _is_pending_source(source_metadata)
            or _is_activated_source(source_metadata)
        )
    ):
        raise ReviewedSubsetAbandonmentError("evidence")
    return source_row


async def _locked_candidate_row(
    database: Any,
    *,
    source_id: str,
    endpoint_id: str,
    resource_types: tuple[str, ...],
) -> dict[str, Any]:
    candidate_rows = await database.all(
        f"""
        SELECT dataset.*
          FROM {_quoted_relation('provider_directory_endpoint_dataset')}
               AS dataset
         WHERE dataset.endpoint_id = :endpoint_id
           AND dataset.is_current = false
           AND dataset.completion_proof_required_version = 3
           AND dataset.completion_proof_json IS NULL
           AND dataset.completion_proof_sha256 IS NULL
           AND dataset.status = ANY(CAST(:statuses AS varchar[]))
           AND dataset.publication_metadata_json::jsonb -> 'source_ids'
                   = CAST(:source_ids AS jsonb)
           AND dataset.publication_metadata_json::jsonb -> 'selected_resources'
                   = CAST(:resource_types AS jsonb)
         ORDER BY dataset.dataset_id
         FOR UPDATE OF dataset;
        """,
        endpoint_id=endpoint_id,
        statuses=[*sorted(ELIGIBLE_PRIOR_STATUSES), ABANDONED_STATUS],
        source_ids=json.dumps([source_id]),
        resource_types=json.dumps(resource_types),
    )
    if len(candidate_rows) != 1:
        raise ReviewedSubsetAbandonmentError("evidence")
    return _row_mapping(candidate_rows[0])


async def _locked_checkpoint_rows(
    database: Any,
    candidate_row: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    checkpoint_rows = await database.all(
        f"""
        SELECT checkpoint.*
          FROM {_quoted_relation('provider_directory_pagination_checkpoint')}
               AS checkpoint
         WHERE checkpoint.dataset_id = :dataset_id
           AND checkpoint.acquisition_root_run_id = :root_run_id
         ORDER BY checkpoint.resource_type
         FOR UPDATE OF checkpoint;
        """,
        dataset_id=candidate_row["dataset_id"],
        root_run_id=candidate_row["acquisition_root_run_id"],
    )
    return tuple(_row_mapping(row) for row in checkpoint_rows)


def _source_terminal_diagnostics(
    source_row: Mapping[str, Any],
    candidate_row: Mapping[str, Any],
    resource_types: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    source_metadata = _json_object(source_row.get("metadata_json"))
    last_import = _json_object(source_metadata.get("last_resource_import"))
    if _text(last_import.get("run_id")) != _text(candidate_row.get("import_run_id")):
        raise ReviewedSubsetAbandonmentError("evidence")
    return validated_terminal_diagnostics(
        _json_object(last_import.get("resources")),
        resource_types,
    )


def _validate_candidate_verification_identity(
    source_row: Mapping[str, Any],
    candidate_row: Mapping[str, Any],
    expected_source_id: str,
    expected_endpoint_id: str,
    resource_types: tuple[str, ...],
) -> None:
    source_metadata = _json_object(source_row.get("metadata_json"))
    candidate_metadata = _json_object(
        candidate_row.get("publication_metadata_json")
    )
    verification_scope = _text(
        candidate_metadata.get("verification_source_scope_hash")
    )
    verification_campaign = _text(
        candidate_metadata.get("verification_campaign_id")
    )
    if (
        _text(candidate_row.get("endpoint_id")) != expected_endpoint_id
        or candidate_row.get("is_current") is not False
        or candidate_row.get("completion_proof_required_version") != 3
        or candidate_row.get("completion_proof_json") is not None
        or candidate_row.get("completion_proof_sha256") is not None
        or _text(candidate_row.get("status"))
        not in (ELIGIBLE_PRIOR_STATUSES | {ABANDONED_STATUS})
        or _json_text_tuple(candidate_metadata.get("source_ids"))
        != (expected_source_id,)
        or _json_text_tuple(candidate_metadata.get("selected_resources"))
        != resource_types
        or verification_scope is None
        or re.fullmatch(r"[0-9a-f]{64}", verification_scope) is None
        or verification_campaign is None
        or verification_campaign
        != _text(
            source_metadata.get("provider_directory_verification_campaign_id")
        )
        or not _has_matching_reviewed_root_policy(
            source_metadata,
            candidate_metadata,
        )
    ):
        raise ReviewedSubsetAbandonmentError("evidence")


def _selection_from_rows(
    *,
    expected_source_id: str,
    endpoint_id: str,
    source_by_field: Mapping[str, Any],
    candidate_by_field: Mapping[str, Any],
    resource_types: tuple[str, ...],
    marker_by_field: dict[str, Any],
    diagnostics_by_resource: dict[str, dict[str, Any]],
    prior_status: str,
) -> ReviewedSubsetAbandonmentSelection:
    return ReviewedSubsetAbandonmentSelection(
        source_id=expected_source_id,
        endpoint_id=endpoint_id,
        dataset_id=_text(candidate_by_field.get("dataset_id")) or "",
        acquisition_root_run_id=(
            _text(candidate_by_field.get("acquisition_root_run_id")) or ""
        ),
        owner_run_id=_text(candidate_by_field.get("import_run_id")) or "",
        canonical_api_base=_text(source_by_field.get("canonical_api_base")) or "",
        source_scope_sha256=marker_by_field["source_scope_sha256"],
        resource_types=resource_types,
        marker_by_field=marker_by_field,
        diagnostic_by_resource=diagnostics_by_resource,
        prior_status=prior_status,
        observed_resource_count=int(candidate_by_field.get("resource_count") or 0),
        observed_metadata=_json_object(
            candidate_by_field.get("publication_metadata_json")
        ),
    )


def _already_applied_selection(
    *,
    expected_source_id: str,
    endpoint_id: str,
    source_by_field: Mapping[str, Any],
    candidate_by_field: Mapping[str, Any],
    resource_types: tuple[str, ...],
) -> tuple[ReviewedSubsetAbandonmentSelection, tuple[dict[str, Any], ...]]:
    source_metadata = _json_object(source_by_field.get("metadata_json"))
    if not (
        _is_pending_source(source_metadata) or _is_activated_source(source_metadata)
    ):
        raise ReviewedSubsetAbandonmentError("evidence")
    candidate_metadata = _json_object(
        candidate_by_field.get("publication_metadata_json")
    )
    marker_by_field = validated_abandonment_marker(
        _json_object(candidate_metadata.get(ABANDONMENT_METADATA_KEY))
    )
    return (
        _selection_from_rows(
            expected_source_id=expected_source_id,
            endpoint_id=endpoint_id,
            source_by_field=source_by_field,
            candidate_by_field=candidate_by_field,
            resource_types=resource_types,
            marker_by_field=marker_by_field,
            diagnostics_by_resource={},
            prior_status=ABANDONED_STATUS,
        ),
        (),
    )


async def _require_no_bulk_checkpoint(
    database: Any,
    candidate_by_field: Mapping[str, Any],
) -> None:
    bulk_checkpoint_count = await database.scalar(
        f"""
        SELECT count(*)
          FROM {_quoted_relation('provider_directory_bulk_acquisition_checkpoint')}
         WHERE dataset_id = :dataset_id
            OR acquisition_root_run_id = :root_run_id;
        """,
        dataset_id=candidate_by_field["dataset_id"],
        root_run_id=candidate_by_field["acquisition_root_run_id"],
    )
    if int(bulk_checkpoint_count or 0) != 0:
        raise ReviewedSubsetAbandonmentError("evidence")


async def _new_abandonment_selection(
    database: Any,
    *,
    expected_source_id: str,
    endpoint_id: str,
    source_by_field: Mapping[str, Any],
    candidate_by_field: Mapping[str, Any],
    resource_types: tuple[str, ...],
    prior_status: str,
) -> tuple[ReviewedSubsetAbandonmentSelection, tuple[dict[str, Any], ...]]:
    if not _is_pending_source(_json_object(source_by_field.get("metadata_json"))):
        raise ReviewedSubsetAbandonmentError("evidence")
    diagnostics_by_resource = _source_terminal_diagnostics(
        source_by_field,
        candidate_by_field,
        resource_types,
    )
    checkpoint_rows = await _locked_checkpoint_rows(database, candidate_by_field)
    await _require_no_bulk_checkpoint(database, candidate_by_field)
    source_scope_sha256, pages_processed, rows_processed = validated_checkpoint_summary(
        checkpoint_rows,
        candidate_by_field,
        source_by_field,
        resource_types,
    )
    require_distinct_scope_domains(candidate_by_field, source_scope_sha256)
    resource_count, proof_shard_count, proof_row_count = await retained_evidence_counts(
        database,
        candidate_by_field,
        checkpoint_rows,
    )
    marker_by_field = abandonment_marker(
        source_scope_sha256=source_scope_sha256,
        resource_types=resource_types,
        checkpoint_count=len(checkpoint_rows),
        pages_processed=pages_processed,
        rows_processed=rows_processed,
        resource_count=resource_count,
        proof_shard_count=proof_shard_count,
        proof_row_count=proof_row_count,
    )
    return (
        _selection_from_rows(
            expected_source_id=expected_source_id,
            endpoint_id=endpoint_id,
            source_by_field=source_by_field,
            candidate_by_field=candidate_by_field,
            resource_types=resource_types,
            marker_by_field=marker_by_field,
            diagnostics_by_resource=diagnostics_by_resource,
            prior_status=prior_status,
        ),
        checkpoint_rows,
    )


async def selected_reviewed_subset_abandonment(
    database: Any,
    expected_source_id: str,
    resource_types: tuple[str, ...],
    *,
    held_pagination_guard_key: str | None = None,
) -> tuple[ReviewedSubsetAbandonmentSelection, tuple[dict[str, Any], ...]]:
    """Lock and validate the sole retained root eligible for abandonment."""

    initial_source = await _initial_source_row(database, expected_source_id)
    await _lock_endpoint_scope(
        database,
        initial_source,
        held_pagination_guard_key,
    )
    endpoint_id = _configured_endpoint_id(initial_source) or ""
    source_by_field = await _locked_source_row(
        database,
        expected_source_id,
        endpoint_id,
        _text(initial_source.get("endpoint_id")) or "",
        _text(initial_source.get("canonical_api_base")) or "",
    )
    candidate_by_field = await _locked_candidate_row(
        database,
        source_id=expected_source_id,
        endpoint_id=endpoint_id,
        resource_types=resource_types,
    )
    _validate_candidate_verification_identity(
        source_by_field,
        candidate_by_field,
        expected_source_id,
        endpoint_id,
        resource_types,
    )
    prior_status = _text(candidate_by_field.get("status")) or ""
    if prior_status == ABANDONED_STATUS:
        return _already_applied_selection(
            expected_source_id=expected_source_id,
            endpoint_id=endpoint_id,
            source_by_field=source_by_field,
            candidate_by_field=candidate_by_field,
            resource_types=resource_types,
        )
    return await _new_abandonment_selection(
        database,
        expected_source_id=expected_source_id,
        endpoint_id=endpoint_id,
        source_by_field=source_by_field,
        candidate_by_field=candidate_by_field,
        resource_types=resource_types,
        prior_status=prior_status,
    )
