# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Locked selection for the direct-v5 HTTP-410 terminal profile."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_fhir_subset_abandonment_contract import (
    ReviewedSubsetAbandonmentError,
)
from process.provider_directory_fhir_subset_abandonment_evidence import (
    require_distinct_scope_domains,
    require_valid_proof_shards,
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
    DIRECT_V5_CAMPAIGN_ID,
    DIRECT_V5_CONTRACT_VERSION,
    DIRECT_V5_PROOF_CONTRACT_VERSION,
    DIRECT_V5_TERMINAL_MARKER_SHA256,
    EXPECTED_RESOURCE_TYPES,
)
from process.provider_directory_fhir_subset_terminal_disposition_selection import (
    _candidate_diagnostic_copies,
    _selection,
)
from process.provider_directory_fhir_subset_terminal_disposition_source import (
    direct_v5_lineage_evidence,
    expected_terminal_start_hashes,
)
from process.provider_directory_fhir_subset_terminal_disposition_util import (
    clean_text,
    json_object,
)
from process.provider_directory_fhir_subset_terminal_disposition_v4_selection import (
    _is_new_candidate_valid,
    _is_replay_candidate_valid,
    _lock_direct_evidence_relations,
    _locked_candidate,
    _locked_checkpoints,
)
from process.provider_directory_fhir_subset_terminal_disposition_v5_contract import (
    direct_v5_terminal_marker,
)
from process.provider_directory_fhir_subset_terminal_disposition_v5_evidence import (
    validated_v5_resource_dispositions,
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
    lineage_by_field = await direct_v5_lineage_evidence(
        database,
        candidate_by_field,
        checkpoint_rows,
    )
    if (
        candidate_by_field.get("resource_count") != resource_count
        or resource_count != proof_row_count
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return checkpoint_rows, source_scope, proof_shard_count, lineage_by_field


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
    resources_by_type = validated_v5_resource_dispositions(
        diagnostics_by_type,
        checkpoint_rows,
        candidate_metadata,
        expected_start_hash_by_type=expected_start_hash_by_type,
    )
    marker_by_field = direct_v5_terminal_marker(
        source_scope_sha256=source_scope,
        resource_dispositions=resources_by_type,
        proof_shard_count=proof_shard_count,
        source_diagnostics=diagnostics_by_type,
        source_import=source_import,
        candidate_metadata=candidate_metadata,
        direct_lineage=lineage_by_field,
    )
    if canonical_evidence_sha256(marker_by_field) != DIRECT_V5_TERMINAL_MARKER_SHA256:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return marker_by_field


async def _new_selection(
    database: Any,
    source_by_field: Mapping[str, Any],
    candidate_by_field: Mapping[str, Any],
    source_id: str,
    endpoint_id: str,
) -> tuple[ReviewedSubsetTerminalDispositionSelection, tuple[dict[str, Any], ...]]:
    """Build the exact marker for the sole failed direct-v5 candidate."""

    if not _is_new_candidate_valid(
        source_by_field,
        candidate_by_field,
        source_id,
        endpoint_id,
        proof_version=DIRECT_V5_PROOF_CONTRACT_VERSION,
        campaign_id=DIRECT_V5_CAMPAIGN_ID,
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    diagnostics_by_type, source_import, candidate_metadata = (
        _candidate_diagnostic_copies(source_by_field, candidate_by_field)
    )
    checkpoint_rows, source_scope, proof_shard_count, lineage_by_field = (
        await _locked_new_evidence(
            database,
            source_by_field,
            candidate_by_field,
        )
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


async def _replay_selection(
    source_by_field: Mapping[str, Any],
    candidate_by_field: Mapping[str, Any],
    source_id: str,
) -> tuple[ReviewedSubsetTerminalDispositionSelection, tuple[dict[str, Any], ...]]:
    """Validate an already-disposed v3 marker without mutable source evidence."""

    candidate_metadata = json_object(
        candidate_by_field.get("publication_metadata_json")
    )
    marker_by_field = validated_terminal_disposition_marker(
        json_object(candidate_metadata.get(TERMINAL_DISPOSITION_METADATA_KEY))
    )
    if canonical_evidence_sha256(marker_by_field) != DIRECT_V5_TERMINAL_MARKER_SHA256:
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
        contract_version=DIRECT_V5_CONTRACT_VERSION,
        campaign_id=DIRECT_V5_CAMPAIGN_ID,
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


async def selected_direct_v5_terminal_disposition(
    database: Any,
    expected_source_id: str,
) -> tuple[ReviewedSubsetTerminalDispositionSelection, tuple[dict[str, Any], ...]]:
    """Lock and validate the sole direct-v5 failed or disposed root."""

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
            proof_version=DIRECT_V5_PROOF_CONTRACT_VERSION,
            campaign_id=DIRECT_V5_CAMPAIGN_ID,
            contract_version=DIRECT_V5_CONTRACT_VERSION,
        )
        if clean_text(candidate_by_field.get("status")) == TERMINAL_DISPOSITION_STATUS:
            return await _replay_selection(
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


__all__ = ("selected_direct_v5_terminal_disposition",)
