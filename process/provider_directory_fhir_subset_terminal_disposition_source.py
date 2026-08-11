# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-contract binding for a new reviewed terminal disposition."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Any, Mapping

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_execution import (
    current_version_census_proof_identity,
)
from process.provider_directory_fhir_root_policy import (
    POLICY_PENDING_STATUS,
    REVIEWED_ROOT_POLICY_METADATA_KEY,
    reviewed_root_policy_from_document,
)
from process.provider_directory_fhir_subset_activation_contract import (
    ACTIVATION_METADATA_KEY,
    ACTIVATION_METADATA_KEY_V2,
)
from process.provider_directory_fhir_subset_completion import canonical_sha256
from process.provider_directory_fhir_subset_identity import (
    SERVER_ISSUED_SUBSET_SEMANTICS,
    SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY,
    server_issued_subset_source_scope_payload,
    validated_subset_identity_values,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    EXPECTED_RESOURCE_TYPES,
    ReviewedSubsetTerminalDispositionError,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    DIRECT_V4_CAMPAIGN_ID,
    DIRECT_V4_LINEAGE_FIELDS,
    DIRECT_V5_CAMPAIGN_ID,
    SOURCE_PROFILE_RESOURCE_TYPES,
)
from process.provider_directory_fhir_subset_terminal_disposition_util import (
    clean_text,
    json_object,
    json_text_tuple,
    quoted_relation,
)


@dataclass(frozen=True, slots=True)
class _TransitionContractEvidence:
    source_metadata: dict[str, Any]
    source_identity: tuple[Any, ...]
    proof_identity: tuple[Any, ...]
    proof_contract_identity: Any
    source_contract: CurrentVersionCensusContract
    scope_sha256: str


def is_policy_one_pending(metadata: Mapping[str, Any]) -> bool:
    """Return whether a source is in the exact unactivated policy-one state."""

    if (
        metadata.get("provider_directory_candidate_status")
        != POLICY_PENDING_STATUS
        or ACTIVATION_METADATA_KEY in metadata
        or ACTIVATION_METADATA_KEY_V2 in metadata
    ):
        return False
    try:
        policy = reviewed_root_policy_from_document(
            metadata.get(REVIEWED_ROOT_POLICY_METADATA_KEY)
        )
    except (TypeError, ValueError):
        return False
    return policy.required_root_count == 1


def is_candidate_policy_one(metadata: Mapping[str, Any]) -> bool:
    """Return whether candidate metadata has the exact policy-one profile."""

    try:
        policy = reviewed_root_policy_from_document(
            metadata.get(REVIEWED_ROOT_POLICY_METADATA_KEY)
        )
    except (TypeError, ValueError):
        return False
    return bool(
        policy.required_root_count == 1
        and metadata.get("requires_twin_root_verification") is False
        and metadata.get("completion_proof_required_version") == 3
    )


async def _direct_lineage_counts(
    database: Any,
    candidate_by_field: Mapping[str, Any],
    campaign_id: str,
) -> dict[str, int]:
    root_run_id = str(candidate_by_field["acquisition_root_run_id"])
    owner_run_id = str(candidate_by_field["import_run_id"])
    query_by_name = {
        "import_run_row_count": f"""
            SELECT count(*) FROM {quoted_relation('import_run')}
             WHERE run_id IN (:root_run_id, :owner_run_id)
                OR retry_of_run_id IN (:root_run_id, :owner_run_id);
        """,
        "current_dataset_count": f"""
            SELECT count(*)
              FROM {quoted_relation('provider_directory_endpoint_dataset')}
             WHERE endpoint_id = :endpoint_id AND is_current = true;
        """,
        "competing_candidate_count": f"""
            SELECT count(*)
              FROM {quoted_relation('provider_directory_endpoint_dataset')}
             WHERE endpoint_id = :endpoint_id
               AND dataset_id <> :dataset_id
               AND status IN ('acquiring', 'failed')
               AND publication_metadata_json::jsonb
                       ->> 'verification_campaign_id' = :campaign_id;
        """,
        "previous_reference_count": f"""
            SELECT count(*)
              FROM {quoted_relation('provider_directory_endpoint_dataset')}
             WHERE previous_dataset_id = :dataset_id;
        """,
    }
    parameters_by_field = {
        "root_run_id": root_run_id,
        "owner_run_id": owner_run_id,
        "endpoint_id": candidate_by_field["endpoint_id"],
        "dataset_id": candidate_by_field["dataset_id"],
        "campaign_id": campaign_id,
    }
    return {
        count_name: int(
            await database.scalar(query, **parameters_by_field) or 0
        )
        for count_name, query in query_by_name.items()
    }


async def _direct_lineage_evidence(
    database: Any,
    candidate_by_field: Mapping[str, Any],
    checkpoint_rows: tuple[dict[str, Any], ...],
    campaign_id: str,
) -> dict[str, Any]:
    """Require the fresh direct root to have no retry or predecessor lineage."""

    count_by_name = await _direct_lineage_counts(
        database,
        candidate_by_field,
        campaign_id,
    )
    owner_run_id = clean_text(candidate_by_field.get("import_run_id"))
    root_run_id = clean_text(candidate_by_field.get("acquisition_root_run_id"))
    lineage_by_field = {
        **count_by_name,
        "checkpoint_retry_count": sum(
            int(checkpoint.get("retry_of_run_id") is not None)
            for checkpoint in checkpoint_rows
        ),
        "owner_equals_root": owner_run_id == root_run_id,
        "previous_dataset_present": (
            candidate_by_field.get("previous_dataset_id") is not None
        ),
    }
    if (
        set(lineage_by_field) != DIRECT_V4_LINEAGE_FIELDS
        or any(lineage_by_field[name] != 0 for name in count_by_name)
        or lineage_by_field["checkpoint_retry_count"] != 0
        or lineage_by_field["owner_equals_root"] is not True
        or lineage_by_field["previous_dataset_present"] is not False
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return lineage_by_field


async def direct_v4_lineage_evidence(
    database: Any,
    candidate_by_field: Mapping[str, Any],
    checkpoint_rows: tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    """Require the frozen v4 root to have no retry or predecessor lineage."""

    return await _direct_lineage_evidence(
        database,
        candidate_by_field,
        checkpoint_rows,
        DIRECT_V4_CAMPAIGN_ID,
    )


async def direct_v5_lineage_evidence(
    database: Any,
    candidate_by_field: Mapping[str, Any],
    checkpoint_rows: tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    """Require the frozen v5 root to have no retry or predecessor lineage."""

    return await _direct_lineage_evidence(
        database,
        candidate_by_field,
        checkpoint_rows,
        DIRECT_V5_CAMPAIGN_ID,
    )


def _source_contract(
    source_row: Mapping[str, Any],
    source_metadata: Mapping[str, Any],
    first_proof: Mapping[str, Any],
    source_identity: tuple[Any, ...],
) -> CurrentVersionCensusContract:
    start_urls = source_metadata.get(
        "provider_directory_current_version_census_start_urls"
    )
    if not isinstance(start_urls, Mapping):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return CurrentVersionCensusContract(
        source_id=clean_text(source_row.get("source_id")) or "",
        cutoff=clean_text(first_proof.get("cutoff")) or "",
        resources=SOURCE_PROFILE_RESOURCE_TYPES,
        expected_nonempty_resources=SOURCE_PROFILE_RESOURCE_TYPES,
        start_urls=tuple(
            (resource_type, start_urls.get(resource_type))
            for resource_type in SOURCE_PROFILE_RESOURCE_TYPES
        ),
        continuation_strategy=SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY,
        contract_version=source_identity[0],
        page_count=source_identity[1],
        strategy_version=source_identity[2],
        traversal_version=source_identity[3],
        canonicalization_version=source_identity[4],
        completion_scopes=source_identity[5],
        campaign_id=source_identity[6],
        semantics=SERVER_ISSUED_SUBSET_SEMANTICS,
    )


def _transition_contract_evidence(
    source_row: Mapping[str, Any],
    candidate_metadata: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
) -> _TransitionContractEvidence:
    source_metadata = json_object(source_row.get("metadata_json"))
    first_proof = json_object(
        json_object(diagnostics[EXPECTED_RESOURCE_TYPES[0]]).get(
            "server_issued_subset_completeness"
        )
    )
    try:
        source_identity = validated_subset_identity_values(source_metadata)
        source_contract = _source_contract(
            source_row,
            source_metadata,
            first_proof,
            source_identity,
        )
        scope_payload = server_issued_subset_source_scope_payload(
            source_row,
            json_text_tuple(candidate_metadata.get("source_ids")),
            clean_text(first_proof.get("cutoff")) or "",
            clean_text(source_row.get("canonical_api_base")) or "",
        )
    except ValueError:
        raise ReviewedSubsetTerminalDispositionError("evidence") from None
    proof_identity = (
        first_proof.get("contract_version"),
        first_proof.get("page_count"),
        first_proof.get("strategy_version"),
        first_proof.get("traversal_version"),
        first_proof.get("canonicalization_version"),
        tuple(first_proof.get("completion_scopes") or ()),
        first_proof.get("campaign_id"),
    )
    return _TransitionContractEvidence(
        source_metadata=source_metadata,
        source_identity=source_identity,
        proof_identity=proof_identity,
        proof_contract_identity=first_proof.get("contract_identity"),
        source_contract=source_contract,
        scope_sha256=canonical_sha256(scope_payload),
    )


def _has_exact_source_profile(
    evidence: _TransitionContractEvidence,
    source_row: Mapping[str, Any],
    candidate_metadata: Mapping[str, Any],
) -> bool:
    metadata = evidence.source_metadata
    contract = evidence.source_contract
    canonical_api_base = clean_text(source_row.get("canonical_api_base"))
    expected_start_url_by_type = {
        resource_type: f"{canonical_api_base}/{resource_type}"
        for resource_type in SOURCE_PROFILE_RESOURCE_TYPES
    }
    expected_page_cap_by_type = {
        resource_type: contract.page_count
        for resource_type in EXPECTED_RESOURCE_TYPES
    }
    resource_fields = (
        "provider_directory_supported_resources",
        "provider_directory_expected_nonempty_resources",
        "provider_directory_server_issued_subset_resources",
    )
    return bool(
        all(
            metadata.get(field_name) == list(SOURCE_PROFILE_RESOURCE_TYPES)
            for field_name in resource_fields
        )
        and dict(contract.start_urls) == expected_start_url_by_type
        and clean_text(source_row.get("endpoint_id")) is not None
        and source_row.get("requires_registration") is False
        and source_row.get("requires_api_key") is False
        and source_row.get("auth_type") == "none"
        and evidence.source_identity == evidence.proof_identity
        and candidate_metadata.get("verification_campaign_id")
        == evidence.source_identity[-1]
        and current_version_census_proof_identity(contract)
        == evidence.proof_contract_identity
        and evidence.scope_sha256
        == candidate_metadata.get("verification_source_scope_hash")
        and metadata.get("provider_directory_fully_enumerable_resources") == []
        and metadata.get("provider_directory_resource_page_count_caps")
        == expected_page_cap_by_type
        and metadata.get("provider_directory_acquisition_enabled") is True
        and metadata.get("provider_directory_manual_only") is True
        and metadata.get("provider_directory_coverage_mode")
        == SERVER_ISSUED_SUBSET_SEMANTICS
        and metadata.get(
            "provider_directory_current_version_census_continuation_strategy"
        )
        == SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY
    )


def expected_terminal_start_hashes(
    source_row: Mapping[str, Any],
    candidate_metadata: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
) -> dict[str, str]:
    """Bind the transition to the exact reviewed source contract and starts."""

    evidence = _transition_contract_evidence(
        source_row,
        candidate_metadata,
        diagnostics,
    )
    if not _has_exact_source_profile(
        evidence,
        source_row,
        candidate_metadata,
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    try:
        return {
            resource_type: hashlib.sha256(
                evidence.source_contract.start_url(
                    resource_type,
                    evidence.source_contract.page_count,
                ).encode("utf-8")
            ).hexdigest()
            for resource_type in EXPECTED_RESOURCE_TYPES
        }
    except ValueError:
        raise ReviewedSubsetTerminalDispositionError("evidence") from None


__all__ = (
    "direct_v4_lineage_evidence",
    "direct_v5_lineage_evidence",
    "expected_terminal_start_hashes",
    "is_candidate_policy_one",
    "is_policy_one_pending",
)
