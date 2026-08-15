# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed manual candidate projection for automatic publication."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from api import provider_directory_source_outcomes as outcomes
from process.provider_directory_fhir_root_policy import (
    LEGACY_VERIFIED_STATUS,
    ReviewedRootPolicy,
)
from process.provider_directory_validated_publication_contract import (
    ProviderDirectoryDatasetIdentity,
)


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
