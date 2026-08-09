# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure evidence selection for reviewed Provider Directory subset state."""

from __future__ import annotations

import importlib
from typing import Any, Mapping, Sequence

from process.provider_directory_fhir_subset_canonical import canonical_sha256
from process.provider_directory_fhir_subset_identity import (
    server_issued_subset_source_scope_payload,
)
from process.provider_directory_fhir_subset_activation_contract import (
    PENDING_STATUS,
    VERIFIED_STATUS,
    ReviewedSubsetActivationError,
    ReviewedSubsetActivationEvidence,
    ReviewedSubsetActivationSelection,
    _SHA256_RE,
    _text,
    reviewed_subset_source_contract_sha256,
)


def _metadata(row_by_field: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata_by_field = row_by_field.get("publication_metadata_json")
    if not isinstance(metadata_by_field, Mapping):
        raise ReviewedSubsetActivationError("evidence")
    return metadata_by_field


def _activation_source(
    source_rows: Sequence[Mapping[str, Any]],
    expected_source_id: str,
    evidence: ReviewedSubsetActivationEvidence,
) -> tuple[dict[str, Any], str, str]:
    if len(source_rows) != 1:
        raise ReviewedSubsetActivationError("evidence")
    source_by_field = dict(source_rows[0])
    source_id = _text(source_by_field.get("source_id"))
    endpoint_id = _text(source_by_field.get("endpoint_id"))
    canonical_api_base = _text(source_by_field.get("canonical_api_base"))
    source_metadata = source_by_field.get("metadata_json")
    if (
        source_id != expected_source_id
        or endpoint_id is None
        or canonical_api_base is None
        or source_by_field.get("requires_registration") is not False
        or source_by_field.get("requires_api_key") is not False
        or source_by_field.get("auth_type") != "none"
        or not isinstance(source_metadata, Mapping)
        or source_metadata.get("provider_directory_configured_endpoint_id")
        != endpoint_id
    ):
        raise ReviewedSubsetActivationError("evidence")
    try:
        importer = importlib.import_module("process.provider_directory_fhir")
        if not importer._is_reviewed_subset_source_metadata(
            dict(source_metadata)
        ):
            raise ValueError("source identity")
        source_contract_sha256 = reviewed_subset_source_contract_sha256(
            source_by_field
        )
        scope_sha256 = canonical_sha256(
            server_issued_subset_source_scope_payload(
                source_by_field,
                (source_id,),
                evidence.cutoff,
                canonical_api_base,
            )
        )
    except (AttributeError, TypeError, ValueError):
        raise ReviewedSubsetActivationError("evidence") from None
    if (
        source_contract_sha256 != evidence.source_contract_sha256
        or scope_sha256 != evidence.verification_source_scope_sha256
    ):
        raise ReviewedSubsetActivationError("evidence")
    return source_by_field, endpoint_id, source_contract_sha256


def _is_candidate_lifecycle_valid(candidate: Mapping[str, Any]) -> bool:
    """Accept active or retained lifecycle state for the matched candidate."""

    status = candidate.get("status")
    if candidate.get("validated_at") is None:
        return False
    if status == "validated":
        return (
            candidate.get("is_current") is False
            and candidate.get("published_at") is None
            and candidate.get("superseded_at") is None
        )
    if status == "published":
        return (
            candidate.get("is_current") is True
            and candidate.get("published_at") is not None
            and candidate.get("superseded_at") is None
        )
    if status == "superseded":
        return (
            candidate.get("is_current") is False
            and candidate.get("published_at") is not None
            and candidate.get("superseded_at") is not None
        )
    return False


def _activation_roots(
    dataset_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Select exactly one immutable baseline and one matched candidate."""

    if len(dataset_rows) != 2:
        raise ReviewedSubsetActivationError("evidence")
    baseline_rows = [
        dict(dataset_row)
        for dataset_row in dataset_rows
        if dataset_row.get("status") == "verification_baseline"
    ]
    candidate_rows = [
        dict(dataset_row)
        for dataset_row in dataset_rows
        if dataset_row.get("status")
        in ("validated", "published", "superseded")
    ]
    if len(baseline_rows) != 1 or len(candidate_rows) != 1:
        raise ReviewedSubsetActivationError("evidence")
    baseline = baseline_rows[0]
    candidate = candidate_rows[0]
    baseline_dataset_id = _text(baseline.get("dataset_id"))
    candidate_dataset_id = _text(candidate.get("dataset_id"))
    baseline_root_run_id = _text(baseline.get("acquisition_root_run_id"))
    candidate_root_run_id = _text(candidate.get("acquisition_root_run_id"))
    if (
        baseline.get("is_current") is not False
        or baseline.get("validated_at") is not None
        or baseline.get("published_at") is not None
        or baseline.get("superseded_at") is not None
        or not _is_candidate_lifecycle_valid(candidate)
        or baseline_dataset_id is None
        or candidate_dataset_id is None
        or baseline_dataset_id == candidate_dataset_id
        or baseline_root_run_id is None
        or candidate_root_run_id is None
        or baseline_root_run_id == candidate_root_run_id
    ):
        raise ReviewedSubsetActivationError("evidence")
    return baseline, candidate


def _validated_root_proofs(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    evidence: ReviewedSubsetActivationEvidence,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    try:
        importer = importlib.import_module("process.provider_directory_fhir")
        importer._twin_root_baseline_proof(baseline)
        importer._assert_matched_twin_root_dataset_proof(candidate)
        baseline_pair = importer._validated_parent_subset_completion_pair(
            baseline
        )
        candidate_pair = importer._validated_parent_subset_completion_pair(
            candidate
        )
    except (AttributeError, RuntimeError, TypeError, ValueError):
        raise ReviewedSubsetActivationError("evidence") from None
    if (
        baseline_pair is None
        or candidate_pair is None
        or baseline_pair != candidate_pair
        or baseline_pair[1] != evidence.completion_proof_sha256
        or baseline_pair[0].get("cutoff") != evidence.cutoff
    ):
        raise ReviewedSubsetActivationError("evidence")
    return _metadata(baseline), _metadata(candidate)


def _root_neutral_proof(verification_by_field: object) -> dict | None:
    if not isinstance(verification_by_field, Mapping):
        return None
    proof_by_field = verification_by_field.get("proof")
    if not isinstance(proof_by_field, Mapping):
        return None
    return {
        field_name: field_value
        for field_name, field_value in proof_by_field.items()
        if field_name != "acquisition_root_run_id"
    }


def _validate_candidate_baseline_binding(
    baseline: Mapping[str, Any],
    baseline_metadata: Mapping[str, Any],
    candidate_metadata: Mapping[str, Any],
) -> None:
    baseline_verification = baseline_metadata.get(
        "twin_root_verification_v1"
    )
    candidate_verification = candidate_metadata.get(
        "twin_root_verification_v1"
    )
    baseline_dataset_id = _text(baseline.get("dataset_id"))
    baseline_root_run_id = _text(baseline.get("acquisition_root_run_id"))
    if (
        not isinstance(candidate_verification, Mapping)
        or baseline_dataset_id is None
        or baseline_root_run_id is None
        or candidate_metadata.get("verification_baseline_dataset_id")
        != baseline_dataset_id
        or candidate_verification.get("baseline_dataset_id")
        != baseline_dataset_id
        or candidate_verification.get("baseline_acquisition_root_run_id")
        != baseline_root_run_id
        or _root_neutral_proof(baseline_verification) is None
        or _root_neutral_proof(baseline_verification)
        != _root_neutral_proof(candidate_verification)
    ):
        raise ReviewedSubsetActivationError("evidence")


def _selection_digest(
    metadata_by_field: Mapping[str, Any],
    field_name: str,
) -> str:
    digest = metadata_by_field.get(field_name)
    if type(digest) is not str or _SHA256_RE.fullmatch(digest) is None:
        raise ReviewedSubsetActivationError("evidence")
    return digest


def _selection_coverage_sha256(
    metadata_by_field: Mapping[str, Any],
) -> str:
    coverage_by_field = metadata_by_field.get("server_issued_subset_coverage")
    if not isinstance(coverage_by_field, Mapping):
        raise ReviewedSubsetActivationError("evidence")
    try:
        return canonical_sha256(dict(coverage_by_field))
    except (TypeError, ValueError):
        raise ReviewedSubsetActivationError("evidence") from None


def _selection_from_roots(
    source_by_field: Mapping[str, Any],
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
    baseline_metadata: Mapping[str, Any],
    candidate_metadata: Mapping[str, Any],
    evidence: ReviewedSubsetActivationEvidence,
    endpoint_id: str,
    source_contract_sha256: str,
) -> ReviewedSubsetActivationSelection:
    source_id = _text(source_by_field.get("source_id")) or ""
    source_metadata = source_by_field["metadata_json"]
    campaign_id = _text(candidate_metadata.get("verification_campaign_id"))
    scope_sha256 = _text(
        candidate_metadata.get("verification_source_scope_hash")
    )
    if (
        source_metadata.get("provider_directory_candidate_status")
        not in (PENDING_STATUS, VERIFIED_STATUS)
        or source_metadata.get("provider_directory_verification_campaign_id")
        != campaign_id
        or campaign_id != baseline_metadata.get("verification_campaign_id")
        or scope_sha256 != evidence.verification_source_scope_sha256
        or scope_sha256
        != baseline_metadata.get("verification_source_scope_hash")
        or baseline_metadata.get("source_ids") != [source_id]
        or candidate_metadata.get("source_ids") != [source_id]
        or _text(baseline.get("endpoint_id")) != endpoint_id
        or _text(candidate.get("endpoint_id")) != endpoint_id
    ):
        raise ReviewedSubsetActivationError("evidence")
    return ReviewedSubsetActivationSelection(
        source_id=source_id,
        endpoint_id=endpoint_id,
        campaign_id=campaign_id,
        baseline_dataset_id=_text(baseline.get("dataset_id")) or "",
        baseline_root_run_id=_text(baseline.get("acquisition_root_run_id")) or "",
        candidate_dataset_id=_text(candidate.get("dataset_id")) or "",
        candidate_root_run_id=_text(candidate.get("acquisition_root_run_id")) or "",
        source_contract_sha256=source_contract_sha256,
        verification_source_scope_sha256=scope_sha256,
        cutoff=evidence.cutoff,
        completion_proof_sha256=evidence.completion_proof_sha256,
        baseline_replay_evidence_sha256=_selection_digest(
            baseline_metadata, "server_issued_subset_replay_evidence_sha256"
        ),
        candidate_replay_evidence_sha256=_selection_digest(
            candidate_metadata, "server_issued_subset_replay_evidence_sha256"
        ),
        baseline_coverage_sha256=_selection_coverage_sha256(baseline_metadata),
        candidate_coverage_sha256=_selection_coverage_sha256(candidate_metadata),
    )


def validated_reviewed_subset_activation_selection(
    *,
    source_rows: Sequence[Mapping[str, Any]],
    dataset_rows: Sequence[Mapping[str, Any]],
    expected_source_id: str,
    evidence: ReviewedSubsetActivationEvidence,
) -> ReviewedSubsetActivationSelection:
    """Validate exact pending source and matching sealed twins for one sync."""

    source_by_field, endpoint_id, source_contract_sha256 = _activation_source(
        source_rows,
        expected_source_id,
        evidence,
    )
    baseline, candidate = _activation_roots(dataset_rows)
    baseline_metadata, candidate_metadata = _validated_root_proofs(
        baseline,
        candidate,
        evidence,
    )
    _validate_candidate_baseline_binding(
        baseline,
        baseline_metadata,
        candidate_metadata,
    )
    return _selection_from_roots(
        source_by_field,
        baseline,
        candidate,
        baseline_metadata,
        candidate_metadata,
        evidence,
        endpoint_id,
        source_contract_sha256,
    )
