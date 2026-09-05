# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed manual candidate projection for automatic publication."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from api import provider_directory_source_outcomes as outcomes
from api.provider_directory_sources import RUNNABLE_CLASSIFICATIONS
from process.provider_directory_fhir_root_policy import (
    LEGACY_VERIFIED_STATUS,
    ReviewedRootPolicy,
)
from process.provider_directory_validated_publication_contract import (
    AUTOMATIC_VALIDATED_PUBLICATION_ROLE,
    ProviderDirectoryDatasetIdentity,
)
from process.provider_directory_validated_publication_catalog import (
    validated_publication_candidate_payload,
)


def _is_automatic_publication_metadata_valid(
    dataset: outcomes._CurrentPublishedDataset,
) -> bool:
    is_twin_root_required = (
        dataset.publication_metadata.get("requires_twin_root_verification") is True
    )
    verification_role = dataset.publication_metadata.get("verification_role")
    return (
        is_twin_root_required
        and verification_role == AUTOMATIC_VALIDATED_PUBLICATION_ROLE
    ) or (not is_twin_root_required and verification_role is None)


def _is_reviewed_policy_authority(canonical_dataset: Any) -> bool:
    reviewed_root_policy = getattr(
        canonical_dataset,
        "reviewed_root_policy",
        None,
    )
    return bool(
        reviewed_root_policy in {ReviewedRootPolicy(1), ReviewedRootPolicy(2)}
        or (
            reviewed_root_policy is None
            and getattr(
                canonical_dataset,
                "verification_source_status",
                None,
            )
            == LEGACY_VERIFIED_STATUS
            and getattr(
                canonical_dataset,
                "completion_proof_required_version",
                None,
            )
            == 3
        )
    )


def _is_reviewed_manual_candidate(
    catalog_entry: Mapping[str, Any],
    source_ids: tuple[str, ...] | None,
    canonical_dataset: Any,
    validated_at: Any,
    resource_count: int | None,
) -> bool:
    return bool(
        catalog_entry.get("classification") == "manual_acquisition"
        and catalog_entry.get("runnable") is False
        and source_ids is not None
        and len(source_ids) == 1
        and getattr(canonical_dataset, "source_id", None) == source_ids[0]
        and getattr(canonical_dataset, "verification_source_ids", None)
        == source_ids
        and _is_reviewed_policy_authority(canonical_dataset)
        and getattr(canonical_dataset, "status", None) == "validated"
        and getattr(canonical_dataset, "is_current", None) is False
        and validated_at is not None
        and resource_count is not None
    )


def reviewed_publication_context(
    catalog_entry: Mapping[str, Any],
    source_ids: tuple[str, ...] | None,
    canonical_dataset: Any,
    dataset: outcomes._CurrentPublishedDataset | None,
    current_identity: ProviderDirectoryDatasetIdentity | None,
    legacy_identity: ProviderDirectoryDatasetIdentity | None,
) -> tuple[
    outcomes._CurrentPublishedDataset | None,
    ProviderDirectoryDatasetIdentity | None,
    outcomes._CurrentPublishedDataset | None,
]:
    """Return candidate, endpoint-local incumbent, and reviewed projection."""

    validated_at = outcomes._utc_datetime(
        getattr(canonical_dataset, "validated_at", None)
    )
    resource_count = outcomes._valid_nonnegative_count(
        getattr(canonical_dataset, "resource_count", None)
    )
    reviewed_dataset = None
    if _is_reviewed_manual_candidate(
        catalog_entry,
        source_ids,
        canonical_dataset,
        validated_at,
        resource_count,
    ):
        reviewed_dataset = outcomes._CurrentPublishedDataset(
            source_ids=source_ids,
            endpoint_id=canonical_dataset.endpoint_id,
            dataset_id=canonical_dataset.dataset_id,
            acquisition_root_run_id=canonical_dataset.evidence_run_id,
            previous_dataset_id=canonical_dataset.previous_dataset_id,
            dataset_hash=canonical_dataset.dataset_hash,
            status=canonical_dataset.status,
            is_current=canonical_dataset.is_current,
            sealed_at=validated_at,
            validated_at=validated_at,
            published_at=None,
            resource_count=resource_count,
            publication_metadata={},
        )
    candidate_dataset = reviewed_dataset or dataset
    incumbent_identity = current_identity or legacy_identity
    if (
        reviewed_dataset is not None
        and incumbent_identity is not None
        and incumbent_identity.endpoint_id != reviewed_dataset.endpoint_id
    ):
        incumbent_identity = None
    return candidate_dataset, incumbent_identity, reviewed_dataset


def _catalog_validated_publication_candidate(
    catalog_entry: Mapping[str, Any],
    source_ids: tuple[str, ...] | None,
    candidate_dataset: outcomes._CurrentPublishedDataset | None,
    incumbent_identity: ProviderDirectoryDatasetIdentity | None,
    canonical_dataset_by_source_id: Mapping[str, Any],
) -> dict[str, Any] | None:
    if (
        source_ids is None
        or len(source_ids) != 1
        or candidate_dataset is None
        or candidate_dataset.status != "validated"
        or candidate_dataset.is_current is not False
    ):
        return None
    canonical_dataset = canonical_dataset_by_source_id.get(source_ids[0])
    if canonical_dataset is None:
        return None
    is_runnable_acquisition = bool(
        catalog_entry.get("runnable") is True
        and catalog_entry.get("classification") in RUNNABLE_CLASSIFICATIONS
        and canonical_dataset.reviewed_root_policy is None
        and (
            canonical_dataset.verification_source_status is None
            or (
                candidate_dataset.publication_metadata.get(
                    "requires_twin_root_verification"
                )
                is True
                and _is_automatic_publication_metadata_valid(candidate_dataset)
            )
        )
    )
    is_manual_legacy_reviewed = bool(
        catalog_entry.get("runnable") is False
        and catalog_entry.get("classification") == "manual_acquisition"
        and canonical_dataset.reviewed_root_policy is None
        and canonical_dataset.verification_source_status == LEGACY_VERIFIED_STATUS
        and canonical_dataset.completion_proof_required_version == 3
    )
    is_reviewed_manual = bool(
        catalog_entry.get("runnable") is False
        and catalog_entry.get("classification") == "manual_acquisition"
        and (
            canonical_dataset.reviewed_root_policy
            in {ReviewedRootPolicy(1), ReviewedRootPolicy(2)}
            or is_manual_legacy_reviewed
        )
    )
    if not (is_runnable_acquisition or is_reviewed_manual):
        return None
    return validated_publication_candidate_payload(
        source_ids[0],
        candidate_dataset,
        incumbent_identity,
        canonical_dataset,
        manual_legacy_reviewed=is_manual_legacy_reviewed,
    )
