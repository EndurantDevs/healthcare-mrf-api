"""Catalog binding for closed validated-publication candidates."""

from __future__ import annotations

from typing import Any

from process.provider_directory_validated_publication_contract import (
    AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY,
    AUTOMATIC_VALIDATED_PUBLICATION_POLICY,
    ProviderDirectoryDatasetIdentity,
    ValidatedPublicationCandidate,
    canonical_utc_timestamp,
    validated_publication_source_status,
)


def _candidate_map(
    source_id: str,
    candidate_dataset: Any,
    incumbent_identity: ProviderDirectoryDatasetIdentity | None,
    canonical_dataset: Any,
) -> dict[str, Any]:
    candidate_by_field: dict[str, Any] = {
        "source_id": source_id,
        "endpoint_id": candidate_dataset.endpoint_id,
        "dataset_id": candidate_dataset.dataset_id,
        "dataset_hash": candidate_dataset.dataset_hash,
        "acquisition_root_run_id": candidate_dataset.acquisition_root_run_id,
        "validated_at": canonical_utc_timestamp(candidate_dataset.validated_at),
        "expected_current": (
            incumbent_identity.to_payload() if incumbent_identity is not None else None
        ),
    }
    if canonical_dataset.verification_source_status is None:
        candidate_by_field.update(
            automatic_publication_policy=(
                AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY
            ),
            content_proof_admission_sha256=(
                canonical_dataset.content_proof_admission_sha256
            ),
        )
    else:
        candidate_by_field.update(
            automatic_publication_policy=AUTOMATIC_VALIDATED_PUBLICATION_POLICY,
            completion_proof_required_version=(
                canonical_dataset.completion_proof_required_version
            ),
            completion_proof_sha256=canonical_dataset.completion_proof_sha256,
            verification_campaign_id=canonical_dataset.verification_campaign_id,
            verification_source_scope_sha256=(
                canonical_dataset.verification_source_scope_hash
            ),
        )
    return candidate_by_field


def _candidate_from_outcomes(
    source_id: str,
    candidate_dataset: Any,
    incumbent_identity: ProviderDirectoryDatasetIdentity | None,
    canonical_dataset: Any,
) -> ValidatedPublicationCandidate | None:
    try:
        return ValidatedPublicationCandidate.from_payload(
            _candidate_map(
                source_id,
                candidate_dataset,
                incumbent_identity,
                canonical_dataset,
            )
        )
    except ValueError:
        return None


def _is_common_identity_exact(
    source_id: str,
    candidate_dataset: Any,
    canonical_dataset: Any,
    publication_candidate: ValidatedPublicationCandidate,
    expected_current_dataset_id: str | None,
) -> bool:
    return bool(
        candidate_dataset.source_ids == (source_id,)
        and candidate_dataset.status == "validated"
        and candidate_dataset.is_current is False
        and candidate_dataset.previous_dataset_id == expected_current_dataset_id
        and canonical_dataset.source_id == source_id
        and canonical_dataset.endpoint_id == publication_candidate.endpoint_id
        and canonical_dataset.dataset_id == publication_candidate.dataset_id
        and canonical_dataset.dataset_hash == publication_candidate.dataset_hash
        and canonical_dataset.evidence_run_id
        == publication_candidate.acquisition_root_run_id
        and canonical_utc_timestamp(canonical_dataset.validated_at)
        == publication_candidate.validated_at
        and canonical_dataset.status == "validated"
        and canonical_dataset.is_current is False
        and canonical_dataset.expected_incumbent_dataset_id
        == expected_current_dataset_id
        and canonical_dataset.verification_source_ids == (source_id,)
        and canonical_dataset.reviewed_root_policy is None
    )


def _is_twin_authority_exact(
    canonical_dataset: Any,
    publication_candidate: ValidatedPublicationCandidate,
) -> bool:
    return bool(
        canonical_dataset.completion_proof_required_version
        == publication_candidate.completion_proof_required_version
        and canonical_dataset.completion_proof_sha256
        == publication_candidate.completion_proof_sha256
        and canonical_dataset.verification_source_status
        == validated_publication_source_status(publication_candidate)
        and canonical_dataset.verification_campaign_id
        == publication_candidate.verification_campaign_id
        and canonical_dataset.verification_source_scope_hash
        == publication_candidate.verification_source_scope_sha256
    )


def _is_generic_authority_exact(
    canonical_dataset: Any,
    publication_candidate: ValidatedPublicationCandidate,
) -> bool:
    return bool(
        canonical_dataset.generic_admission_sealed is True
        and canonical_dataset.artifact_selection_receipt_present is True
        and canonical_dataset.content_proof_admission_sha256
        == publication_candidate.content_proof_admission_sha256
        and canonical_dataset.completion_proof_required_version is None
        and canonical_dataset.completion_proof_sha256 is None
        and canonical_dataset.completion_proof_cutoff is None
        and canonical_dataset.verification_source_status is None
        and canonical_dataset.verification_campaign_id is None
        and canonical_dataset.verification_source_scope_hash is None
    )


def validated_publication_candidate_payload(
    source_id: str,
    candidate_dataset: Any,
    incumbent_identity: ProviderDirectoryDatasetIdentity | None,
    canonical_dataset: Any,
) -> dict[str, Any] | None:
    """Return one exact policy-specific candidate and incumbent identity."""

    publication_candidate = _candidate_from_outcomes(
        source_id,
        candidate_dataset,
        incumbent_identity,
        canonical_dataset,
    )
    if publication_candidate is None:
        return None
    expected_current = publication_candidate.expected_current
    expected_dataset_id = (
        expected_current.dataset_id if expected_current is not None else None
    )
    if not _is_common_identity_exact(
        source_id,
        candidate_dataset,
        canonical_dataset,
        publication_candidate,
        expected_dataset_id,
    ):
        return None
    authority_is_exact = (
        _is_twin_authority_exact(canonical_dataset, publication_candidate)
        if publication_candidate.automatic_publication_policy
        == AUTOMATIC_VALIDATED_PUBLICATION_POLICY
        else _is_generic_authority_exact(
            canonical_dataset,
            publication_candidate,
        )
    )
    if not authority_is_exact or expected_current != incumbent_identity:
        return None
    return publication_candidate.to_payload()
