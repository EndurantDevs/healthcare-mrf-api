# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Locked selection for the direct-v4 terminal profile."""

from __future__ import annotations

import json
from typing import Any, Mapping

from process.provider_directory_fhir_root_policy import (
    REVIEWED_ROOT_POLICY_METADATA_KEY,
)
from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONMENT_METADATA_KEY,
    ReviewedSubsetAbandonmentError,
)
from process.provider_directory_fhir_subset_abandonment_evidence import (
    require_valid_proof_shards,
    require_distinct_scope_domains,
    retained_evidence_counts,
    validated_checkpoint_summary,
)
from process.provider_directory_fhir_subset_abandonment_selection import (
    _configured_endpoint_id,
    _initial_source_row,
    _lock_endpoint_scope,
    _locked_source_row,
    _require_no_bulk_checkpoint,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    TERMINAL_DISPOSITION_METADATA_KEY,
    TERMINAL_DISPOSITION_PRIOR_STATUS,
    TERMINAL_DISPOSITION_STATUS,
    ReviewedSubsetTerminalDispositionError,
    ReviewedSubsetTerminalDispositionSelection,
    canonical_evidence_sha256,
    validated_terminal_disposition_marker,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    DIRECT_V4_CAMPAIGN_ID,
    DIRECT_V4_CONTRACT_VERSION,
    DIRECT_V4_PROOF_CONTRACT_VERSION,
    DIRECT_V4_TERMINAL_MARKER_SHA256,
    EXPECTED_RESOURCE_TYPES,
)
from process.provider_directory_fhir_subset_terminal_disposition_selection import (
    _candidate_diagnostic_copies,
    _selection,
)
from process.provider_directory_fhir_subset_terminal_disposition_evidence import (
    validated_resource_dispositions,
)
from process.provider_directory_fhir_subset_terminal_disposition_source import (
    direct_v4_lineage_evidence,
    expected_terminal_start_hashes,
    is_candidate_policy_one,
    is_policy_one_pending,
)
from process.provider_directory_fhir_subset_terminal_disposition_util import (
    clean_text,
    json_object,
    json_text_tuple,
    quoted_relation,
    row_mapping,
)
from process.provider_directory_fhir_subset_terminal_disposition_v4_contract import (
    direct_v4_terminal_marker,
)
from process.provider_directory_resource_hash import (
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
)


_SHARE_LOCK_RELATIONS = (
    "provider_directory_endpoint_dataset",
    "provider_directory_dataset_resource",
    "provider_directory_dataset_proof_shard",
    "provider_directory_pagination_checkpoint",
    "provider_directory_bulk_acquisition_checkpoint",
    "import_run",
)


async def _lock_direct_evidence_relations(database: Any) -> None:
    relation_list = ", ".join(
        quoted_relation(relation_name) for relation_name in _SHARE_LOCK_RELATIONS
    )
    await database.status(f"LOCK TABLE {relation_list} IN SHARE MODE;")


async def _locked_candidate(
    database: Any,
    source_id: str,
    endpoint_id: str,
) -> dict[str, Any]:
    candidate_rows = await database.all(
        f"""
        SELECT dataset.*
          FROM {quoted_relation('provider_directory_endpoint_dataset')} AS dataset
         WHERE dataset.is_current = false
           AND dataset.completion_proof_required_version = :proof_version
           AND dataset.completion_proof_json IS NULL
           AND dataset.completion_proof_sha256 IS NULL
           AND dataset.publication_metadata_json::jsonb -> 'source_ids'
                   = CAST(:source_ids AS jsonb)
           AND dataset.publication_metadata_json::jsonb -> 'selected_resources'
                   = CAST(:resource_types AS jsonb)
           AND (
                (
                    dataset.status = :prior_status
                    AND dataset.endpoint_id = :endpoint_id
                    AND dataset.publication_metadata_json::jsonb
                            ->> 'verification_campaign_id' = :campaign_id
                    AND NOT (
                        dataset.publication_metadata_json::jsonb
                        ? :disposition_key
                    )
                )
                OR (
                    dataset.status = :disposed_status
                    AND dataset.publication_metadata_json::jsonb
                            #>> ARRAY[
                                :disposition_key,
                                'contract_version'
                            ]::text[] = :contract_version
                )
           )
         ORDER BY dataset.dataset_id
         FOR UPDATE OF dataset;
        """,
        endpoint_id=endpoint_id,
        proof_version=DIRECT_V4_PROOF_CONTRACT_VERSION,
        source_ids=json.dumps([source_id]),
        resource_types=json.dumps(EXPECTED_RESOURCE_TYPES),
        campaign_id=DIRECT_V4_CAMPAIGN_ID,
        prior_status=TERMINAL_DISPOSITION_PRIOR_STATUS,
        disposed_status=TERMINAL_DISPOSITION_STATUS,
        disposition_key=TERMINAL_DISPOSITION_METADATA_KEY,
        contract_version=DIRECT_V4_CONTRACT_VERSION,
    )
    if len(candidate_rows) != 1:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return row_mapping(candidate_rows[0])


async def _locked_checkpoints(
    database: Any,
    candidate_by_field: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    checkpoint_rows = await database.all(
        f"""
        SELECT checkpoint.*
          FROM {quoted_relation('provider_directory_pagination_checkpoint')}
               AS checkpoint
         WHERE checkpoint.dataset_id = :dataset_id
         ORDER BY checkpoint.acquisition_root_run_id, checkpoint.resource_type
         FOR UPDATE OF checkpoint;
        """,
        dataset_id=candidate_by_field["dataset_id"],
    )
    return tuple(row_mapping(row) for row in checkpoint_rows)


def _is_new_candidate_valid(
    source_by_field: Mapping[str, Any],
    candidate_by_field: Mapping[str, Any],
    source_id: str,
    endpoint_id: str,
) -> bool:
    source_metadata = json_object(source_by_field.get("metadata_json"))
    candidate_metadata = json_object(
        candidate_by_field.get("publication_metadata_json")
    )
    owner_run_id = clean_text(candidate_by_field.get("import_run_id"))
    root_run_id = clean_text(candidate_by_field.get("acquisition_root_run_id"))
    return bool(
        clean_text(source_by_field.get("source_id")) == source_id
        and clean_text(candidate_by_field.get("endpoint_id")) == endpoint_id
        and clean_text(candidate_by_field.get("status"))
        == TERMINAL_DISPOSITION_PRIOR_STATUS
        and candidate_by_field.get("is_current") is False
        and candidate_by_field.get("previous_dataset_id") is None
        and candidate_by_field.get("dataset_hash") is None
        and candidate_by_field.get("validated_at") is None
        and candidate_by_field.get("published_at") is None
        and candidate_by_field.get("superseded_at") is None
        and candidate_by_field.get("completion_proof_required_version")
        == DIRECT_V4_PROOF_CONTRACT_VERSION
        and candidate_by_field.get("completion_proof_json") is None
        and candidate_by_field.get("completion_proof_sha256") is None
        and owner_run_id is not None
        and owner_run_id == root_run_id
        and json_text_tuple(candidate_metadata.get("source_ids")) == (source_id,)
        and json_text_tuple(candidate_metadata.get("selected_resources"))
        == EXPECTED_RESOURCE_TYPES
        and json_text_tuple(candidate_metadata.get("expected_resources"))
        == EXPECTED_RESOURCE_TYPES
        and candidate_metadata.get("verification_campaign_id")
        == DIRECT_V4_CAMPAIGN_ID
        and candidate_metadata.get("resource_hash_contract")
        == TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
        and candidate_metadata.get("reused_from_checkpoint") is False
        and is_policy_one_pending(source_metadata)
        and is_candidate_policy_one(candidate_metadata)
        and candidate_metadata.get(REVIEWED_ROOT_POLICY_METADATA_KEY)
        == source_metadata.get(REVIEWED_ROOT_POLICY_METADATA_KEY)
        and ABANDONMENT_METADATA_KEY not in candidate_metadata
        and TERMINAL_DISPOSITION_METADATA_KEY not in candidate_metadata
    )


async def _locked_new_evidence(
    database: Any,
    source_by_field: Mapping[str, Any],
    candidate_by_field: Mapping[str, Any],
) -> tuple[tuple[dict[str, Any], ...], str, int, dict[str, Any]]:
    checkpoint_rows = await _locked_checkpoints(database, candidate_by_field)
    await _require_no_bulk_checkpoint(database, candidate_by_field)
    source_scope, _pages, _rows = validated_checkpoint_summary(
        checkpoint_rows,
        candidate_by_field,
        source_by_field,
        EXPECTED_RESOURCE_TYPES,
    )
    require_distinct_scope_domains(candidate_by_field, source_scope)
    await require_valid_proof_shards(
        database,
        candidate_by_field,
        EXPECTED_RESOURCE_TYPES,
    )
    resource_count, proof_shard_count, proof_row_count = (
        await retained_evidence_counts(
            database,
            candidate_by_field,
            checkpoint_rows,
        )
    )
    lineage_by_field = await direct_v4_lineage_evidence(
        database,
        candidate_by_field,
        checkpoint_rows,
    )
    if (
        candidate_by_field.get("resource_count") != resource_count
        or resource_count != proof_row_count
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return (
        checkpoint_rows,
        source_scope,
        proof_shard_count,
        lineage_by_field,
    )


def _new_marker_by_field(
    source_by_field: Mapping[str, Any],
    candidate_metadata: Mapping[str, Any],
    diagnostics_by_type: Mapping[str, Any],
    checkpoint_rows: tuple[dict[str, Any], ...],
    source_scope: str,
    proof_shard_count: int,
    source_import: Mapping[str, Any],
    lineage_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    expected_start_hash_by_type = expected_terminal_start_hashes(
        source_by_field,
        candidate_metadata,
        diagnostics_by_type,
    )
    resources_by_type = validated_resource_dispositions(
        diagnostics_by_type,
        checkpoint_rows,
        candidate_metadata,
        expected_start_hash_by_type=expected_start_hash_by_type,
        direct_v4=True,
    )
    marker_by_field = direct_v4_terminal_marker(
        source_scope_sha256=source_scope,
        resource_dispositions=resources_by_type,
        proof_shard_count=proof_shard_count,
        source_diagnostics=diagnostics_by_type,
        source_import=source_import,
        candidate_metadata=candidate_metadata,
        direct_lineage=lineage_by_field,
    )
    if (
        canonical_evidence_sha256(marker_by_field)
        != DIRECT_V4_TERMINAL_MARKER_SHA256
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return marker_by_field


async def _new_selection(
    database: Any,
    source_by_field: Mapping[str, Any],
    candidate_by_field: Mapping[str, Any],
    source_id: str,
    endpoint_id: str,
) -> tuple[ReviewedSubsetTerminalDispositionSelection, tuple[dict[str, Any], ...]]:
    """Build the exact marker for the sole failed direct-v4 candidate."""

    if not _is_new_candidate_valid(
        source_by_field,
        candidate_by_field,
        source_id,
        endpoint_id,
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    diagnostics_by_type, source_import, candidate_metadata = (
        _candidate_diagnostic_copies(source_by_field, candidate_by_field)
    )
    (
        checkpoint_rows,
        source_scope,
        proof_shard_count,
        lineage_by_field,
    ) = await _locked_new_evidence(
        database, source_by_field, candidate_by_field
    )
    marker_by_field = _new_marker_by_field(
        source_by_field,
        candidate_metadata,
        diagnostics_by_type,
        checkpoint_rows,
        source_scope,
        proof_shard_count,
        source_import,
        lineage_by_field,
    )
    return (
        _selection(
            source_by_field,
            candidate_by_field,
            source_id,
            marker_by_field,
            TERMINAL_DISPOSITION_PRIOR_STATUS,
        ),
        checkpoint_rows,
    )


def _is_replay_candidate_valid(
    candidate_by_field: Mapping[str, Any],
    marker_by_field: Mapping[str, Any],
    observed_metadata_by_field: Mapping[str, Any],
    diagnostics_by_type: Mapping[str, Any],
    completion_by_field: Mapping[str, Any],
    source_id: str,
) -> bool:
    owner_run_id = clean_text(candidate_by_field.get("import_run_id"))
    root_run_id = clean_text(candidate_by_field.get("acquisition_root_run_id"))
    return bool(
        marker_by_field.get("contract_version") == DIRECT_V4_CONTRACT_VERSION
        and clean_text(candidate_by_field.get("status"))
        == TERMINAL_DISPOSITION_STATUS
        and candidate_by_field.get("is_current") is False
        and candidate_by_field.get("previous_dataset_id") is None
        and candidate_by_field.get("dataset_hash") is None
        and owner_run_id is not None
        and owner_run_id == root_run_id
        and candidate_by_field.get("resource_count")
        == marker_by_field["resource_count"]
        and set(diagnostics_by_type) == set(EXPECTED_RESOURCE_TYPES)
        and completion_by_field.get("resource_diagnostics")
        == diagnostics_by_type
        and completion_by_field.get("verification_campaign_id")
        == DIRECT_V4_CAMPAIGN_ID
        and clean_text(completion_by_field.get("acquisition_root_run_id"))
        == root_run_id
        and clean_text(completion_by_field.get("terminal_run_id"))
        == owner_run_id
        and json_text_tuple(completion_by_field.get("source_ids")) == (source_id,)
        and json_text_tuple(completion_by_field.get("selected_resources"))
        == EXPECTED_RESOURCE_TYPES
        and observed_metadata_by_field.get("reused_from_checkpoint") is False
        and observed_metadata_by_field.get("verification_campaign_id")
        == DIRECT_V4_CAMPAIGN_ID
        and marker_by_field["candidate_metadata_sha256"]
        == canonical_evidence_sha256(observed_metadata_by_field)
        and marker_by_field["source_diagnostics_sha256"]
        == canonical_evidence_sha256(diagnostics_by_type)
        and all(
            marker_by_field["resource_dispositions"][resource_type][
                "diagnostic_sha256"
            ]
            == canonical_evidence_sha256(diagnostics_by_type[resource_type])
            for resource_type in EXPECTED_RESOURCE_TYPES
        )
    )


async def _replay_selection(
    database: Any,
    source_by_field: Mapping[str, Any],
    candidate_by_field: Mapping[str, Any],
    source_id: str,
) -> tuple[ReviewedSubsetTerminalDispositionSelection, tuple[dict[str, Any], ...]]:
    """Validate an already-disposed v2 marker without mutable source evidence."""

    candidate_metadata = json_object(
        candidate_by_field.get("publication_metadata_json")
    )
    marker_by_field = validated_terminal_disposition_marker(
        json_object(candidate_metadata.get(TERMINAL_DISPOSITION_METADATA_KEY))
    )
    if (
        canonical_evidence_sha256(marker_by_field)
        != DIRECT_V4_TERMINAL_MARKER_SHA256
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    observed_metadata_by_field = dict(candidate_metadata)
    observed_metadata_by_field.pop(TERMINAL_DISPOSITION_METADATA_KEY)
    diagnostics_by_type = json_object(
        observed_metadata_by_field.get("resource_diagnostics")
    )
    completion_by_field = json_object(
        observed_metadata_by_field.get("completion_proof_v1")
    )
    if not _is_replay_candidate_valid(
        candidate_by_field,
        marker_by_field,
        observed_metadata_by_field,
        diagnostics_by_type,
        completion_by_field,
        source_id,
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return (
        _selection(
            source_by_field,
            candidate_by_field,
            source_id,
            marker_by_field,
            TERMINAL_DISPOSITION_STATUS,
        ),
        (),
    )


async def selected_direct_v4_terminal_disposition(
    database: Any,
    expected_source_id: str,
) -> tuple[ReviewedSubsetTerminalDispositionSelection, tuple[dict[str, Any], ...]]:
    """Lock and validate the sole direct-v4 failed or disposed root."""

    try:
        initial_source = await _initial_source_row(database, expected_source_id)
        await _lock_endpoint_scope(database, initial_source, None)
        endpoint_id = _configured_endpoint_id(initial_source) or ""
        source_by_field = await _locked_source_row(
            database,
            expected_source_id,
            endpoint_id,
            clean_text(initial_source.get("endpoint_id")) or "",
            clean_text(initial_source.get("canonical_api_base")) or "",
        )
        await _lock_direct_evidence_relations(database)
        candidate_by_field = await _locked_candidate(
            database,
            expected_source_id,
            endpoint_id,
        )
        if clean_text(candidate_by_field.get("status")) == TERMINAL_DISPOSITION_STATUS:
            return await _replay_selection(
                database,
                source_by_field,
                candidate_by_field,
                expected_source_id,
            )
        return await _new_selection(
            database,
            source_by_field,
            candidate_by_field,
            expected_source_id,
            endpoint_id,
        )
    except ReviewedSubsetAbandonmentError as error:
        raise ReviewedSubsetTerminalDispositionError(error.code) from None


__all__ = ("selected_direct_v4_terminal_disposition",)
