# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Policy-aware source state for reviewed subset terminal operations."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_fhir_root_policy import (
    LEGACY_PENDING_STATUS,
    LEGACY_VERIFIED_STATUS,
    POLICY_PENDING_STATUS,
    POLICY_VERIFIED_STATUS,
    REVIEWED_ROOT_POLICY_METADATA_KEY,
    reviewed_root_policy_for_status,
    reviewed_root_policy_from_document,
)
from process.provider_directory_fhir_subset_activation_contract import (
    ACTIVATION_METADATA_KEY,
    ACTIVATION_METADATA_KEY_V2,
)


def reviewed_abandonment_source_state(
    metadata_by_field: Mapping[str, Any],
) -> str | None:
    """Return one exact legacy or policy-bearing source lifecycle state."""

    status = metadata_by_field.get("provider_directory_candidate_status")
    has_legacy_activation = ACTIVATION_METADATA_KEY in metadata_by_field
    has_policy_activation = ACTIVATION_METADATA_KEY_V2 in metadata_by_field
    if has_legacy_activation and has_policy_activation:
        return None
    try:
        root_policy = reviewed_root_policy_for_status(
            metadata_by_field,
            status if type(status) is str else None,
        )
    except ValueError:
        return None
    if status in (LEGACY_PENDING_STATUS, POLICY_PENDING_STATUS):
        if not has_legacy_activation and not has_policy_activation:
            return "pending"
        return None
    if status == LEGACY_VERIFIED_STATUS:
        if (
            isinstance(metadata_by_field.get(ACTIVATION_METADATA_KEY), Mapping)
            and not has_policy_activation
        ):
            return "activated"
        return None
    if status == POLICY_VERIFIED_STATUS:
        marker_by_field = metadata_by_field.get(ACTIVATION_METADATA_KEY_V2)
        if (
            isinstance(marker_by_field, Mapping)
            and not has_legacy_activation
            and marker_by_field.get("root_policy") == root_policy.document()
        ):
            return "activated"
    return None


def has_matching_reviewed_root_policy(
    source_metadata_by_field: Mapping[str, Any],
    candidate_metadata_by_field: Mapping[str, Any],
) -> bool:
    """Require policy-key presence and exact documents to match."""

    has_source_policy = (
        REVIEWED_ROOT_POLICY_METADATA_KEY in source_metadata_by_field
    )
    has_candidate_policy = (
        REVIEWED_ROOT_POLICY_METADATA_KEY in candidate_metadata_by_field
    )
    if has_source_policy != has_candidate_policy:
        return False
    if not has_source_policy:
        return True
    try:
        return reviewed_root_policy_from_document(
            source_metadata_by_field.get(REVIEWED_ROOT_POLICY_METADATA_KEY)
        ) == reviewed_root_policy_from_document(
            candidate_metadata_by_field.get(REVIEWED_ROOT_POLICY_METADATA_KEY)
        )
    except ValueError:
        return False


__all__ = (
    "has_matching_reviewed_root_policy",
    "reviewed_abandonment_source_state",
)
