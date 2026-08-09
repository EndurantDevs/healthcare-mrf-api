# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional storage for reviewed Provider Directory subset activation."""

from __future__ import annotations

import json
from typing import Any, Mapping

from process.provider_directory_fhir_subset_activation_contract import (
    ACTIVATION_METADATA_KEY,
    PENDING_STATUS,
    VERIFIED_STATUS,
    ReviewedSubsetActivationError,
    ReviewedSubsetActivationEvidence,
    ReviewedSubsetActivationResult,
    ReviewedSubsetActivationSelection,
    _quoted_relation,
    _row_mapping,
    _schema_name,
    _text,
)
from process.provider_directory_fhir_subset_activation_selection import (
    validated_reviewed_subset_activation_selection,
)


async def _initial_source_record(
    database: Any,
    expected_source_id: str,
) -> dict[str, Any]:
    source_rows = await database.all(
        f"""
        SELECT source.source_id, source.endpoint_id, source.metadata_json
          FROM {_quoted_relation('provider_directory_source')} AS source
         WHERE source.source_id = :source_id
         ORDER BY source.endpoint_id
         LIMIT 2;
        """,
        source_id=expected_source_id,
    )
    if len(source_rows) != 1:
        raise ReviewedSubsetActivationError("state")
    source_row = _row_mapping(source_rows[0])
    if (
        _text(source_row.get("source_id")) != expected_source_id
        or _text(source_row.get("endpoint_id")) is None
    ):
        raise ReviewedSubsetActivationError("state")
    return source_row


async def _lock_activation_endpoint(
    database: Any,
    endpoint_id: str,
) -> None:
    is_acquired = await database.scalar(
        """
        SELECT pg_catalog.pg_try_advisory_xact_lock(
                   pg_catalog.hashtextextended(:endpoint_id, 0)
               );
        """,
        endpoint_id=endpoint_id,
    )
    if is_acquired is not True:
        raise ReviewedSubsetActivationError("busy")


async def _lock_activation_api_endpoint(
    database: Any,
    endpoint_id: str,
) -> None:
    endpoint_rows = await database.all(
        f"""
        SELECT endpoint.endpoint_id
          FROM {_quoted_relation('provider_directory_api_endpoint')} AS endpoint
         WHERE endpoint.endpoint_id = :endpoint_id
         FOR UPDATE OF endpoint;
        """,
        endpoint_id=endpoint_id,
    )
    if len(endpoint_rows) != 1 or (
        _text(_row_mapping(endpoint_rows[0]).get("endpoint_id"))
        != endpoint_id
    ):
        raise ReviewedSubsetActivationError("state")


async def _locked_source_rows(
    database: Any,
    *,
    expected_source_id: str,
    endpoint_id: str,
) -> tuple[dict[str, Any], ...]:
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
        endpoint_id=endpoint_id,
    )
    return tuple(_row_mapping(source_row) for source_row in source_rows)


async def _lock_activation_source_table(database: Any) -> None:
    await database.status(
        "LOCK TABLE "
        f"{_quoted_relation('provider_directory_source')} "
        "IN SHARE MODE;"
    )


async def _locked_dataset_rows(
    database: Any,
    *,
    endpoint_id: str,
    campaign_id: str,
    scope_sha256: str,
) -> tuple[dict[str, Any], ...]:
    dataset_rows = await database.all(
        f"""
        SELECT dataset.*
          FROM {_quoted_relation('provider_directory_endpoint_dataset')}
               AS dataset
         WHERE dataset.endpoint_id = :endpoint_id
           AND dataset.completion_proof_required_version = 3
           AND dataset.status IN (
               'verification_baseline',
               'verification_mismatch',
               'validated',
               'published',
               'superseded'
           )
           AND dataset.publication_metadata_json::jsonb
                   ->> 'verification_campaign_id' = :campaign_id
           AND dataset.publication_metadata_json::jsonb
                   ->> 'verification_source_scope_hash' = :scope_sha256
         ORDER BY dataset.dataset_id
         FOR UPDATE OF dataset;
        """,
        endpoint_id=endpoint_id,
        campaign_id=campaign_id,
        scope_sha256=scope_sha256,
    )
    return tuple(_row_mapping(dataset_row) for dataset_row in dataset_rows)


def _activation_campaign_id(source_row: Mapping[str, Any]) -> str:
    source_metadata = source_row.get("metadata_json")
    if not isinstance(source_metadata, Mapping):
        raise ReviewedSubsetActivationError("evidence")
    campaign_id = _text(
        source_metadata.get("provider_directory_verification_campaign_id")
    )
    if campaign_id is None:
        raise ReviewedSubsetActivationError("evidence")
    return campaign_id


def _has_exact_activation_marker(
    source_row: Mapping[str, Any],
    marker_by_field: Mapping[str, Any],
) -> bool:
    source_metadata = source_row.get("metadata_json")
    return bool(
        isinstance(source_metadata, Mapping)
        and source_metadata.get("provider_directory_candidate_status")
        == VERIFIED_STATUS
        and source_metadata.get(ACTIVATION_METADATA_KEY) == marker_by_field
    )


async def _activate_source(
    database: Any,
    *,
    selection: ReviewedSubsetActivationSelection,
    source_row: Mapping[str, Any],
) -> ReviewedSubsetActivationResult:
    marker_by_field = selection.metadata_marker()
    source_metadata = source_row.get("metadata_json")
    if not isinstance(source_metadata, Mapping):
        raise ReviewedSubsetActivationError("state")
    if _has_exact_activation_marker(source_row, marker_by_field):
        return ReviewedSubsetActivationResult(activated=False)
    if (
        source_metadata.get("provider_directory_candidate_status")
        != PENDING_STATUS
        or ACTIVATION_METADATA_KEY in source_metadata
    ):
        raise ReviewedSubsetActivationError("state")
    updated_count = await database.status(
        f"""
        UPDATE {_quoted_relation('provider_directory_source')}
           SET metadata_json = pg_catalog.jsonb_set(
                   pg_catalog.jsonb_set(
                       metadata_json::jsonb,
                       ARRAY['provider_directory_candidate_status']::text[],
                       pg_catalog.to_jsonb(CAST(:verified_status AS text)),
                       true
                   ),
                   ARRAY[:activation_key]::text[],
                   CAST(:activation_marker AS jsonb),
                   true
               ),
               updated_at = pg_catalog.transaction_timestamp()
         WHERE source_id = :source_id
           AND endpoint_id = :endpoint_id
           AND metadata_json::jsonb
                   ->> 'provider_directory_candidate_status' = :pending_status
           AND NOT (metadata_json::jsonb ? :activation_key);
        """,
        source_id=selection.source_id,
        endpoint_id=selection.endpoint_id,
        pending_status=PENDING_STATUS,
        verified_status=VERIFIED_STATUS,
        activation_key=ACTIVATION_METADATA_KEY,
        activation_marker=json.dumps(
            marker_by_field,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    if updated_count != 1:
        raise ReviewedSubsetActivationError("state")
    await database.status(
        "SET CONSTRAINTS "
        f'"{_schema_name()}".'
        '"provider_directory_reviewed_subset_activation_source_guard" '
        "IMMEDIATE;"
    )
    return ReviewedSubsetActivationResult(activated=True)


async def sync_reviewed_subset_transaction(
    runtime_database: Any,
    expected_source_id: str,
    evidence: ReviewedSubsetActivationEvidence,
) -> ReviewedSubsetActivationResult:
    """Lock, validate, and apply one exact reviewed source state."""

    async with runtime_database.transaction():
        isolation = await runtime_database.scalar(
            "SELECT pg_catalog.current_setting('transaction_isolation');"
        )
        if isolation != "read committed":
            raise ReviewedSubsetActivationError("state")
        initial_source = await _initial_source_record(
            runtime_database,
            expected_source_id,
        )
        endpoint_id = _text(initial_source.get("endpoint_id")) or ""
        campaign_id = _activation_campaign_id(initial_source)
        await _lock_activation_endpoint(runtime_database, endpoint_id)
        await _lock_activation_api_endpoint(runtime_database, endpoint_id)
        await _lock_activation_source_table(runtime_database)
        source_rows = await _locked_source_rows(
            runtime_database,
            expected_source_id=expected_source_id,
            endpoint_id=endpoint_id,
        )
        if len(source_rows) != 1:
            raise ReviewedSubsetActivationError("evidence")
        dataset_rows = await _locked_dataset_rows(
            runtime_database,
            endpoint_id=endpoint_id,
            campaign_id=campaign_id,
            scope_sha256=evidence.verification_source_scope_sha256,
        )
        selection = validated_reviewed_subset_activation_selection(
            source_rows=source_rows,
            dataset_rows=dataset_rows,
            expected_source_id=expected_source_id,
            evidence=evidence,
        )
        return await _activate_source(
            runtime_database,
            selection=selection,
            source_row=source_rows[0],
        )
