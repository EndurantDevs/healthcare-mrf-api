# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Locked selection for one reviewed mixed-terminal root disposition."""

from __future__ import annotations

import datetime
import json
from typing import Any, Mapping

from process.provider_directory_fhir_root_policy import REVIEWED_ROOT_POLICY_METADATA_KEY
from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONMENT_METADATA_KEY,
    ReviewedSubsetAbandonmentError,
)
from process.provider_directory_fhir_subset_abandonment_evidence import (
    require_distinct_scope_domains,
    retained_evidence_counts,
    validated_checkpoint_summary,
)
from process.provider_directory_fhir_subset_terminal_disposition_evidence import (
    validated_resource_dispositions,
)
from process.provider_directory_fhir_subset_abandonment_selection import (
    _configured_endpoint_id,
    _initial_source_row,
    _lock_endpoint_scope,
    _locked_checkpoint_rows,
    _locked_source_row,
    _require_no_bulk_checkpoint,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    EXPECTED_RESOURCE_TYPES,
    TERMINAL_DISPOSITION_CONTRACT_VERSION,
    TERMINAL_DISPOSITION_METADATA_KEY,
    TERMINAL_DISPOSITION_PRIOR_STATUS,
    TERMINAL_DISPOSITION_STATUS,
    ReviewedSubsetTerminalDispositionError,
    ReviewedSubsetTerminalDispositionSelection,
    canonical_evidence_sha256,
    terminal_disposition_marker,
    validated_terminal_disposition_marker,
)
from process.provider_directory_fhir_subset_terminal_disposition_source import (
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
from process.provider_directory_resource_hash import TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT


_COMPLETION_PROOF_FIELDS = frozenset({
    "acquisition_root_run_id", "terminal_run_id", "source_ids",
    "selected_resources", "resource_diagnostics", "verification_campaign_id",
    "verification_source_scope_hash",
})
_SOURCE_IMPORT_FIELDS = frozenset({"run_id", "observed_at", "resources"})


def _has_valid_source_import_observed_at(value: Any) -> bool:
    if type(value) is not str:
        return False
    try:
        parsed = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ") == value


def _translate_evidence_error(error: Exception) -> None:
    if isinstance(error, ReviewedSubsetAbandonmentError):
        raise ReviewedSubsetTerminalDispositionError(error.code) from None
    raise error


async def _locked_legacy_disposed_row(
    database: Any,
    source_id: str,
) -> dict[str, Any] | None:
    disposed_rows = await database.all(
        f"""
        SELECT dataset.*
          FROM {quoted_relation('provider_directory_endpoint_dataset')}
               AS dataset
         WHERE dataset.is_current = false
           AND dataset.completion_proof_required_version = 3
           AND dataset.completion_proof_json IS NULL
           AND dataset.completion_proof_sha256 IS NULL
           AND dataset.status = :disposed_status
           AND dataset.publication_metadata_json::jsonb -> 'source_ids'
                   = CAST(:source_ids AS jsonb)
           AND dataset.publication_metadata_json::jsonb
                   #>> ARRAY[
                       :disposition_key,
                       'contract_version'
                   ]::text[] = :contract_version
         ORDER BY dataset.dataset_id
         FOR UPDATE OF dataset;
        """,
        disposed_status=TERMINAL_DISPOSITION_STATUS,
        source_ids=json.dumps([source_id]),
        disposition_key=TERMINAL_DISPOSITION_METADATA_KEY,
        contract_version=TERMINAL_DISPOSITION_CONTRACT_VERSION,
    )
    if len(disposed_rows) > 1:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return row_mapping(disposed_rows[0]) if disposed_rows else None


async def _locked_legacy_failed_row(
    database: Any,
    source_id: str,
    endpoint_id: str,
) -> dict[str, Any]:
    failed_rows = await database.all(
        f"""
        SELECT dataset.*
          FROM {quoted_relation('provider_directory_endpoint_dataset')}
               AS dataset
         WHERE dataset.endpoint_id = :endpoint_id
           AND dataset.is_current = false
           AND dataset.completion_proof_required_version = 3
           AND dataset.completion_proof_json IS NULL
           AND dataset.completion_proof_sha256 IS NULL
           AND dataset.status = :prior_status
           AND dataset.publication_metadata_json::jsonb -> 'source_ids'
                   = CAST(:source_ids AS jsonb)
           AND dataset.publication_metadata_json::jsonb -> 'selected_resources'
                   = CAST(:resource_types AS jsonb)
         ORDER BY dataset.dataset_id
         FOR UPDATE OF dataset;
        """,
        endpoint_id=endpoint_id,
        prior_status=TERMINAL_DISPOSITION_PRIOR_STATUS,
        source_ids=json.dumps([source_id]),
        resource_types=json.dumps(EXPECTED_RESOURCE_TYPES),
    )
    if len(failed_rows) != 1:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return row_mapping(failed_rows[0])


async def _locked_candidate_row(
    database: Any,
    source_id: str,
    endpoint_id: str,
) -> dict[str, Any]:
    """Select only the exact legacy-v1 disposed or failed candidate."""

    disposed_row = await _locked_legacy_disposed_row(database, source_id)
    if disposed_row is not None:
        return disposed_row
    return await _locked_legacy_failed_row(database, source_id, endpoint_id)


def _candidate_diagnostic_copies(
    source_row: Mapping[str, Any],
    candidate_row: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    source_metadata = json_object(source_row.get("metadata_json"))
    candidate_metadata = json_object(
        candidate_row.get("publication_metadata_json")
    )
    source_import = json_object(source_metadata.get("last_resource_import"))
    diagnostics = json_object(candidate_metadata.get("resource_diagnostics"))
    completion = json_object(candidate_metadata.get("completion_proof_v1"))
    if (
        set(source_import) != _SOURCE_IMPORT_FIELDS
        or not _has_valid_source_import_observed_at(
            source_import.get("observed_at")
        )
        or set(completion) != _COMPLETION_PROOF_FIELDS
        or set(diagnostics) != set(EXPECTED_RESOURCE_TYPES)
        or source_import.get("resources") != diagnostics
        or completion.get("resource_diagnostics") != diagnostics
        or clean_text(source_import.get("run_id"))
        != clean_text(candidate_row.get("import_run_id"))
        or clean_text(completion.get("acquisition_root_run_id"))
        != clean_text(candidate_row.get("acquisition_root_run_id"))
        or clean_text(completion.get("terminal_run_id"))
        != clean_text(candidate_row.get("import_run_id"))
        or json_text_tuple(completion.get("source_ids"))
        != (clean_text(source_row.get("source_id")) or "",)
        or json_text_tuple(completion.get("selected_resources"))
        != EXPECTED_RESOURCE_TYPES
        or completion.get("verification_campaign_id")
        != candidate_metadata.get("verification_campaign_id")
        or completion.get("verification_source_scope_hash")
        != candidate_metadata.get("verification_source_scope_hash")
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return diagnostics, source_import, candidate_metadata


def _validate_candidate_identity(
    source_row: Mapping[str, Any],
    candidate_row: Mapping[str, Any],
    expected_source_id: str,
    expected_endpoint_id: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    source_metadata = json_object(source_row.get("metadata_json"))
    candidate_metadata = json_object(
        candidate_row.get("publication_metadata_json")
    )
    disallowed_markers = (
        ABANDONMENT_METADATA_KEY,
        TERMINAL_DISPOSITION_METADATA_KEY,
    )
    if (
        clean_text(source_row.get("source_id")) != expected_source_id
        or clean_text(candidate_row.get("endpoint_id")) != expected_endpoint_id
        or candidate_row.get("is_current") is not False
        or clean_text(candidate_row.get("status"))
        not in {TERMINAL_DISPOSITION_PRIOR_STATUS, TERMINAL_DISPOSITION_STATUS}
        or candidate_row.get("completion_proof_required_version") != 3
        or candidate_row.get("completion_proof_json") is not None
        or candidate_row.get("completion_proof_sha256") is not None
        or candidate_row.get("validated_at") is not None
        or candidate_row.get("published_at") is not None
        or candidate_row.get("superseded_at") is not None
        or json_text_tuple(candidate_metadata.get("source_ids"))
        != (expected_source_id,)
        or json_text_tuple(candidate_metadata.get("selected_resources"))
        != EXPECTED_RESOURCE_TYPES
        or json_text_tuple(candidate_metadata.get("expected_resources"))
        != EXPECTED_RESOURCE_TYPES
        or clean_text(candidate_metadata.get("acquisition_root_run_id"))
        != clean_text(candidate_row.get("acquisition_root_run_id"))
        or not is_policy_one_pending(source_metadata)
        or not is_candidate_policy_one(candidate_metadata)
        or candidate_metadata.get(REVIEWED_ROOT_POLICY_METADATA_KEY)
        != source_metadata.get(REVIEWED_ROOT_POLICY_METADATA_KEY)
        or candidate_metadata.get("resource_hash_contract")
        != TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
        or candidate_metadata.get("reused_from_checkpoint") is not True
        or any(marker in candidate_metadata for marker in disallowed_markers)
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return _candidate_diagnostic_copies(source_row, candidate_row)


def _already_applied_selection(
    source_row: Mapping[str, Any],
    candidate_row: Mapping[str, Any],
    expected_source_id: str,
) -> tuple[ReviewedSubsetTerminalDispositionSelection, tuple[dict[str, Any], ...]]:
    candidate_metadata = json_object(candidate_row.get("publication_metadata_json"))
    if ABANDONMENT_METADATA_KEY in candidate_metadata:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    marker = validated_terminal_disposition_marker(
        json_object(candidate_metadata.get(TERMINAL_DISPOSITION_METADATA_KEY))
    )
    observed_metadata_by_field = dict(candidate_metadata)
    observed_metadata_by_field.pop(TERMINAL_DISPOSITION_METADATA_KEY)
    diagnostics = json_object(
        observed_metadata_by_field.get("resource_diagnostics")
    )
    completion = json_object(
        observed_metadata_by_field.get("completion_proof_v1")
    )
    if not _is_replay_candidate_valid(
        candidate_row,
        observed_metadata_by_field,
        diagnostics,
        completion,
        marker,
        expected_source_id,
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return (
        _selection(
            source_row,
            candidate_row,
            expected_source_id,
            marker,
            TERMINAL_DISPOSITION_STATUS,
        ),
        (),
    )


def _is_replay_candidate_valid(
    candidate_row: Mapping[str, Any],
    observed_metadata_by_field: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
    completion: Mapping[str, Any],
    marker: Mapping[str, Any],
    expected_source_id: str,
) -> bool:
    marker_resources = marker["resource_dispositions"]
    return bool(
        clean_text(candidate_row.get("status")) == TERMINAL_DISPOSITION_STATUS
        and candidate_row.get("is_current") is False
        and clean_text(observed_metadata_by_field.get("acquisition_root_run_id"))
        == clean_text(candidate_row.get("acquisition_root_run_id"))
        and json_text_tuple(observed_metadata_by_field.get("source_ids"))
        == (expected_source_id,)
        and json_text_tuple(observed_metadata_by_field.get("selected_resources"))
        == EXPECTED_RESOURCE_TYPES
        and json_text_tuple(observed_metadata_by_field.get("expected_resources"))
        == EXPECTED_RESOURCE_TYPES
        and is_candidate_policy_one(observed_metadata_by_field)
        and observed_metadata_by_field.get("resource_hash_contract")
        == TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
        and observed_metadata_by_field.get("reused_from_checkpoint") is True
        and set(diagnostics) == set(EXPECTED_RESOURCE_TYPES)
        and set(completion) == _COMPLETION_PROOF_FIELDS
        and completion.get("resource_diagnostics") == diagnostics
        and clean_text(completion.get("acquisition_root_run_id"))
        == clean_text(candidate_row.get("acquisition_root_run_id"))
        and clean_text(completion.get("terminal_run_id"))
        == clean_text(candidate_row.get("import_run_id"))
        and json_text_tuple(completion.get("source_ids"))
        == (expected_source_id,)
        and json_text_tuple(completion.get("selected_resources"))
        == EXPECTED_RESOURCE_TYPES
        and completion.get("verification_campaign_id")
        == observed_metadata_by_field.get("verification_campaign_id")
        and completion.get("verification_source_scope_hash")
        == observed_metadata_by_field.get("verification_source_scope_hash")
        and marker["candidate_metadata_sha256"]
        == canonical_evidence_sha256(observed_metadata_by_field)
        and marker["source_diagnostics_sha256"]
        == canonical_evidence_sha256(diagnostics)
        and all(
            marker_resources[resource_type]["diagnostic_sha256"]
            == canonical_evidence_sha256(diagnostics[resource_type])
            for resource_type in EXPECTED_RESOURCE_TYPES
        )
        and candidate_row.get("resource_count") == marker["resource_count"]
    )


def _selection(
    source_row: Mapping[str, Any],
    candidate_row: Mapping[str, Any],
    expected_source_id: str,
    marker: dict[str, Any],
    prior_status: str,
) -> ReviewedSubsetTerminalDispositionSelection:
    return ReviewedSubsetTerminalDispositionSelection(
        source_id=expected_source_id,
        endpoint_id=clean_text(candidate_row.get("endpoint_id")) or "",
        dataset_id=clean_text(candidate_row.get("dataset_id")) or "",
        acquisition_root_run_id=(
            clean_text(candidate_row.get("acquisition_root_run_id")) or ""
        ),
        owner_run_id=clean_text(candidate_row.get("import_run_id")) or "",
        canonical_api_base=clean_text(source_row.get("canonical_api_base")) or "",
        source_scope_sha256=marker["source_scope_sha256"],
        marker_by_field=marker,
        prior_status=prior_status,
        observed_resource_count=int(candidate_row.get("resource_count") or 0),
        observed_candidate_metadata=json_object(
            candidate_row.get("publication_metadata_json")
        ),
    )


async def _locked_disposition_rows(
    database: Any,
    expected_source_id: str,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    try:
        initial_source = await _initial_source_row(database, expected_source_id)
        await _lock_endpoint_scope(database, initial_source, None)
        configured_endpoint_id = _configured_endpoint_id(initial_source) or ""
        source_row = await _locked_source_row(
            database,
            expected_source_id,
            configured_endpoint_id,
            clean_text(initial_source.get("endpoint_id")) or "",
            clean_text(initial_source.get("canonical_api_base")) or "",
        )
        candidate_row = await _locked_candidate_row(
            database,
            expected_source_id,
            configured_endpoint_id,
        )
    except ReviewedSubsetAbandonmentError as error:
        _translate_evidence_error(error)
    return source_row, candidate_row, configured_endpoint_id


async def _new_terminal_selection(
    database: Any,
    source_row: Mapping[str, Any],
    candidate_row: Mapping[str, Any],
    expected_source_id: str,
    configured_endpoint_id: str,
    prior_status: str,
) -> tuple[ReviewedSubsetTerminalDispositionSelection, tuple[dict[str, Any], ...]]:
    diagnostics, source_import, candidate_metadata = _validate_candidate_identity(
        source_row,
        candidate_row,
        expected_source_id,
        configured_endpoint_id,
    )
    checkpoint_rows, source_scope, evidence_counts = (
        await _locked_terminal_evidence(database, candidate_row, source_row)
    )
    resource_count, proof_shard_count, proof_row_count = evidence_counts
    if (
        candidate_row.get("resource_count") != resource_count
        or resource_count != proof_row_count
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    expected_start_hash_by_type = expected_terminal_start_hashes(
        source_row,
        candidate_metadata,
        diagnostics,
    )
    resource_dispositions_by_type = validated_resource_dispositions(
        diagnostics,
        checkpoint_rows,
        candidate_metadata,
        expected_start_hash_by_type=expected_start_hash_by_type,
    )
    marker_by_field = terminal_disposition_marker(
        source_scope_sha256=source_scope,
        resource_dispositions=resource_dispositions_by_type,
        proof_shard_count=proof_shard_count,
        source_diagnostics=diagnostics,
        source_import=source_import,
        candidate_metadata=candidate_metadata,
    )
    return (
        _selection(
            source_row,
            candidate_row,
            expected_source_id,
            marker_by_field,
            prior_status,
        ),
        checkpoint_rows,
    )


async def _locked_terminal_evidence(
    database: Any,
    candidate_row: Mapping[str, Any],
    source_row: Mapping[str, Any],
) -> tuple[tuple[dict[str, Any], ...], str, tuple[int, int, int]]:
    try:
        checkpoint_rows = await _locked_checkpoint_rows(database, candidate_row)
        await _require_no_bulk_checkpoint(database, candidate_row)
        source_scope, _pages, _rows = validated_checkpoint_summary(
            checkpoint_rows,
            candidate_row,
            source_row,
            EXPECTED_RESOURCE_TYPES,
        )
        require_distinct_scope_domains(candidate_row, source_scope)
        evidence_counts = await retained_evidence_counts(
            database,
            candidate_row,
            checkpoint_rows,
        )
    except ReviewedSubsetAbandonmentError as error:
        _translate_evidence_error(error)
    return checkpoint_rows, source_scope, evidence_counts


async def selected_reviewed_subset_terminal_disposition(
    database: Any,
    expected_source_id: str,
) -> tuple[ReviewedSubsetTerminalDispositionSelection, tuple[dict[str, Any], ...]]:
    """Lock and validate the sole policy-one mixed-terminal retained root."""

    source_row, candidate_row, configured_endpoint_id = (
        await _locked_disposition_rows(database, expected_source_id)
    )
    source_metadata = json_object(source_row.get("metadata_json"))
    prior_status = clean_text(candidate_row.get("status")) or ""
    if prior_status == TERMINAL_DISPOSITION_STATUS:
        return _already_applied_selection(
            source_row,
            candidate_row,
            expected_source_id,
        )
    if not is_policy_one_pending(source_metadata):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return await _new_terminal_selection(
        database,
        source_row,
        candidate_row,
        expected_source_id,
        configured_endpoint_id,
        prior_status,
    )


__all__ = ("selected_reviewed_subset_terminal_disposition",)
