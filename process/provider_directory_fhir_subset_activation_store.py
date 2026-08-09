# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional storage for reviewed Provider Directory subset activation."""

from __future__ import annotations

import json
from typing import Any, Mapping

from process.provider_directory_fhir_subset_activation_contract import (
    ACTIVATION_METADATA_KEY,
    ACTIVATION_METADATA_KEY_V2,
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
from process.provider_directory_fhir_subset_identity import (
    CONFIGURED_ENDPOINT_ID_METADATA_FIELD,
    subset_source_endpoint_identity,
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
    try:
        subset_source_endpoint_identity(source_row)
    except ValueError:
        raise ReviewedSubsetActivationError("state") from None
    if _text(source_row.get("source_id")) != expected_source_id:
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
    configured_endpoint_id: str,
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
        endpoint_id=configured_endpoint_id,
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
    configured_endpoint_id: str,
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
        endpoint_id=configured_endpoint_id,
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
    selection: ReviewedSubsetActivationSelection,
    marker_by_field: Mapping[str, Any],
) -> bool:
    source_metadata = source_row.get("metadata_json")
    other_activation_key = (
        ACTIVATION_METADATA_KEY
        if selection.activation_metadata_key == ACTIVATION_METADATA_KEY_V2
        else ACTIVATION_METADATA_KEY_V2
    )
    return bool(
        isinstance(source_metadata, Mapping)
        and source_metadata.get("provider_directory_candidate_status")
        == selection.verified_status
        and source_metadata.get(selection.activation_metadata_key)
        == marker_by_field
        and other_activation_key not in source_metadata
    )


def _activation_endpoint_identity(
    selection: ReviewedSubsetActivationSelection,
    source_row: Mapping[str, Any],
) -> tuple[str, str]:
    """Return the exact serving snapshot and configured activation endpoint."""

    try:
        serving_endpoint_id, configured_endpoint_id = (
            subset_source_endpoint_identity(source_row)
        )
    except ValueError:
        raise ReviewedSubsetActivationError("state") from None
    if configured_endpoint_id != selection.endpoint_id:
        raise ReviewedSubsetActivationError("state")
    return serving_endpoint_id, configured_endpoint_id


async def _cas_activate_source(
    database: Any,
    selection: ReviewedSubsetActivationSelection,
    marker_by_field: Mapping[str, Any],
    serving_endpoint_id: str,
    configured_endpoint_id: str,
) -> None:
    """Compare and swap one pending source to the reviewed verified state."""

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
           AND endpoint_id = :serving_endpoint_id
           AND metadata_json::jsonb ->> :configured_endpoint_key
                   = :configured_endpoint_id
           AND metadata_json::jsonb
                   ->> 'provider_directory_candidate_status' = :pending_status
           AND NOT (
               metadata_json::jsonb
               ?| CAST(:activation_keys AS text[])
           );
        """,
        source_id=selection.source_id,
        serving_endpoint_id=serving_endpoint_id,
        configured_endpoint_key=CONFIGURED_ENDPOINT_ID_METADATA_FIELD,
        configured_endpoint_id=configured_endpoint_id,
        pending_status=selection.pending_status,
        verified_status=selection.verified_status,
        activation_key=selection.activation_metadata_key,
        activation_keys=[
            ACTIVATION_METADATA_KEY,
            ACTIVATION_METADATA_KEY_V2,
        ],
        activation_marker=json.dumps(
            marker_by_field,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    if updated_count != 1:
        raise ReviewedSubsetActivationError("state")


async def _activate_source(
    database: Any,
    *,
    selection: ReviewedSubsetActivationSelection,
    source_row: Mapping[str, Any],
) -> ReviewedSubsetActivationResult:
    """Apply or replay one exact reviewed activation marker."""

    marker_by_field = selection.metadata_marker()
    source_metadata = source_row.get("metadata_json")
    if not isinstance(source_metadata, Mapping):
        raise ReviewedSubsetActivationError("state")
    serving_endpoint_id, configured_endpoint_id = _activation_endpoint_identity(
        selection,
        source_row,
    )
    if _has_exact_activation_marker(
        source_row,
        selection,
        marker_by_field,
    ):
        return ReviewedSubsetActivationResult(activated=False)
    if (
        source_metadata.get("provider_directory_candidate_status")
        != selection.pending_status
        or any(
            activation_key in source_metadata
            for activation_key in (
                ACTIVATION_METADATA_KEY,
                ACTIVATION_METADATA_KEY_V2,
            )
        )
    ):
        raise ReviewedSubsetActivationError("state")
    await _cas_activate_source(
        database,
        selection,
        marker_by_field,
        serving_endpoint_id,
        configured_endpoint_id,
    )
    await database.status(
        "SET CONSTRAINTS "
        f'"{_schema_name()}".'
        '"provider_directory_reviewed_subset_activation_source_guard" '
        "IMMEDIATE;"
    )
    return ReviewedSubsetActivationResult(activated=True)


async def _initial_activation_identity(
    database: Any,
    expected_source_id: str,
) -> tuple[str, str, str]:
    """Return the serving, configured, and campaign identity snapshot."""

    initial_source = await _initial_source_record(database, expected_source_id)
    serving_endpoint_id, configured_endpoint_id = (
        subset_source_endpoint_identity(initial_source)
    )
    return (
        serving_endpoint_id,
        configured_endpoint_id,
        _activation_campaign_id(initial_source),
    )


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
        serving_endpoint_id, configured_endpoint_id, campaign_id = (
            await _initial_activation_identity(
                runtime_database,
                expected_source_id,
            )
        )
        await _lock_activation_endpoint(runtime_database, configured_endpoint_id)
        await _lock_activation_api_endpoint(
            runtime_database, configured_endpoint_id
        )
        await _lock_activation_source_table(runtime_database)
        source_rows = await _locked_source_rows(
            runtime_database,
            expected_source_id=expected_source_id,
            configured_endpoint_id=configured_endpoint_id,
        )
        if len(source_rows) != 1:
            raise ReviewedSubsetActivationError("evidence")
        try:
            locked_endpoint_identity = subset_source_endpoint_identity(
                source_rows[0]
            )
        except ValueError:
            raise ReviewedSubsetActivationError("evidence") from None
        if locked_endpoint_identity != (
            serving_endpoint_id,
            configured_endpoint_id,
        ):
            raise ReviewedSubsetActivationError("evidence")
        dataset_rows = await _locked_dataset_rows(
            runtime_database,
            configured_endpoint_id=configured_endpoint_id,
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
