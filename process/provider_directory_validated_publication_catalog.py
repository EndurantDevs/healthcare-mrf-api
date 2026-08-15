"""Catalog binding for closed validated-publication candidates."""

from __future__ import annotations

from typing import Any

from process.provider_directory_fhir_root_policy import (
    LEGACY_VERIFIED_STATUS,
    POLICY_VERIFIED_STATUS,
    ReviewedRootPolicy,
)
from process.provider_directory_publication_catalog_authority import (
    bootstrap_catalog_authority,
)
from process.provider_directory_validated_publication_contract import (
    AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY,
    AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY,
    AUTOMATIC_REVIEWED_SINGLE_ROOT_PUBLICATION_POLICY,
    AUTOMATIC_REVIEWED_TWIN_ROOT_PUBLICATION_POLICY,
    AUTOMATIC_VALIDATED_PUBLICATION_POLICY,
    ProviderDirectoryDatasetIdentity,
    ValidatedPublicationCandidate,
    canonical_utc_timestamp,
    validated_publication_source_status,
)
from process.provider_directory_validated_publication_policies import (
    GENERIC_PUBLICATION_POLICIES,
    REVIEWED_PUBLICATION_POLICIES,
)


def _reviewed_candidate_values_by_field(
    canonical_dataset: Any,
) -> dict[str, Any]:
    reviewed_policy = (
        AUTOMATIC_REVIEWED_SINGLE_ROOT_PUBLICATION_POLICY
        if canonical_dataset.reviewed_root_policy == ReviewedRootPolicy(1)
        else AUTOMATIC_REVIEWED_TWIN_ROOT_PUBLICATION_POLICY
    )
    values_by_field = {
        "automatic_publication_policy": reviewed_policy,
        "completion_proof_required_version": (
            canonical_dataset.completion_proof_required_version
        ),
        "completion_proof_sha256": canonical_dataset.completion_proof_sha256,
        "verification_campaign_id": canonical_dataset.verification_campaign_id,
        "verification_source_scope_sha256": (
            canonical_dataset.verification_source_scope_hash
        ),
    }
    if reviewed_policy == AUTOMATIC_REVIEWED_SINGLE_ROOT_PUBLICATION_POLICY:
        values_by_field["content_proof_admission_sha256"] = (
            canonical_dataset.content_proof_admission_sha256
        )
    return values_by_field


def _generic_candidate_values_by_field(
    source_id: str,
    incumbent_identity: ProviderDirectoryDatasetIdentity | None,
    canonical_dataset: Any,
) -> dict[str, Any]:
    values_by_field = {
        "automatic_publication_policy": (
            AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY
            if incumbent_identity is None
            else AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY
        ),
        "content_proof_admission_sha256": (
            canonical_dataset.content_proof_admission_sha256
        ),
    }
    if incumbent_identity is None and (
        catalog_authority := bootstrap_catalog_authority(source_id)
    ) is not None:
        (
            values_by_field["source_catalog_entry_id"],
            values_by_field["source_catalog_digest_sha256"],
        ) = catalog_authority
    return values_by_field


def _proof_candidate_values_by_field(canonical_dataset: Any) -> dict[str, Any]:
    return {
        "automatic_publication_policy": AUTOMATIC_VALIDATED_PUBLICATION_POLICY,
        "completion_proof_required_version": (
            canonical_dataset.completion_proof_required_version
        ),
        "completion_proof_sha256": canonical_dataset.completion_proof_sha256,
        "verification_campaign_id": canonical_dataset.verification_campaign_id,
        "verification_source_scope_sha256": (
            canonical_dataset.verification_source_scope_hash
        ),
    }


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
    if canonical_dataset.reviewed_root_policy in {
        ReviewedRootPolicy(1),
        ReviewedRootPolicy(2),
    }:
        policy_values_by_field = _reviewed_candidate_values_by_field(
            canonical_dataset
        )
    elif canonical_dataset.verification_source_status is None:
        policy_values_by_field = _generic_candidate_values_by_field(
            source_id,
            incumbent_identity,
            canonical_dataset,
        )
    else:
        policy_values_by_field = _proof_candidate_values_by_field(
            canonical_dataset
        )
    return {**candidate_by_field, **policy_values_by_field}


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
        and canonical_dataset.reviewed_root_policy
        == {
            AUTOMATIC_REVIEWED_SINGLE_ROOT_PUBLICATION_POLICY: ReviewedRootPolicy(1),
            AUTOMATIC_REVIEWED_TWIN_ROOT_PUBLICATION_POLICY: ReviewedRootPolicy(2),
        }.get(publication_candidate.automatic_publication_policy)
    )


def _is_twin_authority_exact(
    canonical_dataset: Any,
    publication_candidate: ValidatedPublicationCandidate,
    *,
    manual_legacy_reviewed: bool,
) -> bool:
    expected_source_status = (
        LEGACY_VERIFIED_STATUS
        if manual_legacy_reviewed
        else validated_publication_source_status(publication_candidate)
    )
    return bool(
        canonical_dataset.completion_proof_required_version
        == publication_candidate.completion_proof_required_version
        and canonical_dataset.completion_proof_sha256
        == publication_candidate.completion_proof_sha256
        and canonical_dataset.verification_source_status == expected_source_status
        and canonical_dataset.verification_campaign_id
        == publication_candidate.verification_campaign_id
        and canonical_dataset.verification_source_scope_hash
        == publication_candidate.verification_source_scope_sha256
        and (
            not manual_legacy_reviewed
            or canonical_dataset.completion_proof_required_version == 3
        )
    )


def _is_generic_authority_exact(
    canonical_dataset: Any,
    publication_candidate: ValidatedPublicationCandidate,
) -> bool:
    catalog_authority = (
        bootstrap_catalog_authority(publication_candidate.source_id)
        if publication_candidate.automatic_publication_policy
        == AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY
        else None
    )
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
        and (
            publication_candidate.automatic_publication_policy
            == AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY
            and publication_candidate.source_catalog_entry_id is None
            and publication_candidate.source_catalog_digest_sha256 is None
            or publication_candidate.automatic_publication_policy
            == AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY
            and catalog_authority
            == (
                publication_candidate.source_catalog_entry_id,
                publication_candidate.source_catalog_digest_sha256,
            )
        )
    )


def _is_reviewed_authority_exact(
    canonical_dataset: Any,
    publication_candidate: ValidatedPublicationCandidate,
) -> bool:
    return bool(
        canonical_dataset.completion_proof_required_version
        == publication_candidate.completion_proof_required_version
        and canonical_dataset.completion_proof_sha256
        == publication_candidate.completion_proof_sha256
        and canonical_dataset.verification_source_status
        == POLICY_VERIFIED_STATUS
        and canonical_dataset.verification_campaign_id
        == publication_candidate.verification_campaign_id
        and canonical_dataset.verification_source_scope_hash
        == publication_candidate.verification_source_scope_sha256
        and (
            publication_candidate.automatic_publication_policy
            == AUTOMATIC_REVIEWED_TWIN_ROOT_PUBLICATION_POLICY
            and publication_candidate.content_proof_admission_sha256 is None
            or publication_candidate.automatic_publication_policy
            == AUTOMATIC_REVIEWED_SINGLE_ROOT_PUBLICATION_POLICY
            and canonical_dataset.generic_admission_sealed is True
            and canonical_dataset.artifact_selection_receipt_present is True
            and canonical_dataset.content_proof_admission_sha256
            == publication_candidate.content_proof_admission_sha256
        )
    )


def validated_publication_candidate_payload(
    source_id: str,
    candidate_dataset: Any,
    incumbent_identity: ProviderDirectoryDatasetIdentity | None,
    canonical_dataset: Any,
    *,
    manual_legacy_reviewed: bool = False,
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
    if publication_candidate.automatic_publication_policy == (
        AUTOMATIC_VALIDATED_PUBLICATION_POLICY
    ):
        is_authority_exact = _is_twin_authority_exact(
            canonical_dataset,
            publication_candidate,
            manual_legacy_reviewed=manual_legacy_reviewed,
        )
    elif publication_candidate.automatic_publication_policy in (
        REVIEWED_PUBLICATION_POLICIES
    ):
        is_authority_exact = _is_reviewed_authority_exact(
            canonical_dataset,
            publication_candidate,
        )
    elif publication_candidate.automatic_publication_policy in (
        GENERIC_PUBLICATION_POLICIES
    ):
        is_authority_exact = _is_generic_authority_exact(
            canonical_dataset,
            publication_candidate,
        )
    else:
        is_authority_exact = False
    if not is_authority_exact or expected_current != incumbent_identity:
        return None
    return publication_candidate.to_payload()
