# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed policy identity for reviewed Provider Directory acquisitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


REVIEWED_ROOT_POLICY_METADATA_KEY = (
    "provider_directory_reviewed_root_policy_v1"
)
REVIEWED_ROOT_POLICY_VERSION = "provider-directory-reviewed-root-policy-v1"
DEFAULT_REQUIRED_ROOT_COUNT = 2
ALLOWED_REQUIRED_ROOT_COUNTS = frozenset({1, 2})

LEGACY_PENDING_STATUS = "pending_two_matching_reviewed_subset_acquisitions"
LEGACY_VERIFIED_STATUS = "verified_two_matching_reviewed_subset_acquisitions"
POLICY_PENDING_STATUS = "pending_reviewed_subset_acquisition"
POLICY_VERIFIED_STATUS = "verified_reviewed_subset_acquisition"

LEGACY_REVIEWED_STATUSES = frozenset(
    {LEGACY_PENDING_STATUS, LEGACY_VERIFIED_STATUS}
)
POLICY_REVIEWED_STATUSES = frozenset(
    {POLICY_PENDING_STATUS, POLICY_VERIFIED_STATUS}
)
REVIEWED_STATUSES = LEGACY_REVIEWED_STATUSES | POLICY_REVIEWED_STATUSES
PENDING_REVIEWED_STATUSES = frozenset(
    {LEGACY_PENDING_STATUS, POLICY_PENDING_STATUS}
)
VERIFIED_REVIEWED_STATUSES = frozenset(
    {LEGACY_VERIFIED_STATUS, POLICY_VERIFIED_STATUS}
)


@dataclass(frozen=True, slots=True)
class ReviewedRootPolicy:
    """Bind one fresh reviewed campaign to an exact root requirement."""

    required_root_count: int
    policy_version: str = REVIEWED_ROOT_POLICY_VERSION

    def __post_init__(self) -> None:
        if (
            self.policy_version != REVIEWED_ROOT_POLICY_VERSION
            or type(self.required_root_count) is not int
            or self.required_root_count not in ALLOWED_REQUIRED_ROOT_COUNTS
        ):
            raise ValueError("provider_directory_reviewed_root_policy_invalid")

    @property
    def is_twin_root_required(self) -> bool:
        """Return whether independent repeated acquisition is required."""

        return self.required_root_count == 2

    def document(self) -> dict[str, Any]:
        """Return the exact JSON object persisted into evidence."""

        return {
            "policy_version": self.policy_version,
            "required_root_count": self.required_root_count,
        }


def reviewed_root_policy_document(required_root_count: int) -> dict[str, Any]:
    """Build a validated closed policy document."""

    return ReviewedRootPolicy(required_root_count).document()


def reviewed_root_policy_from_document(raw_policy: object) -> ReviewedRootPolicy:
    """Parse one closed policy document supplied by a trusted caller."""

    if (
        type(raw_policy) is not dict
        or set(raw_policy) != {"policy_version", "required_root_count"}
    ):
        raise ValueError("provider_directory_reviewed_root_policy_invalid")
    return ReviewedRootPolicy(
        policy_version=raw_policy.get("policy_version"),
        required_root_count=raw_policy.get("required_root_count"),
    )


def reviewed_root_policy_from_metadata(
    metadata: Mapping[str, Any],
) -> ReviewedRootPolicy | None:
    """Parse an explicit policy without interpreting source status."""

    if REVIEWED_ROOT_POLICY_METADATA_KEY not in metadata:
        return None
    return reviewed_root_policy_from_document(
        metadata.get(REVIEWED_ROOT_POLICY_METADATA_KEY)
    )


def reviewed_root_policy_for_status(
    metadata: Mapping[str, Any],
    status: str | None,
) -> ReviewedRootPolicy | None:
    """Resolve legacy or policy-bearing reviewed status fail closed."""

    explicit_policy = reviewed_root_policy_from_metadata(metadata)
    if status is None:
        if explicit_policy is not None:
            raise ValueError(
                "provider_directory_reviewed_root_policy_status_required"
            )
        return None
    if status in LEGACY_REVIEWED_STATUSES:
        if explicit_policy is not None:
            raise ValueError(
                "provider_directory_reviewed_root_policy_status_mismatch"
            )
        return ReviewedRootPolicy(DEFAULT_REQUIRED_ROOT_COUNT)
    if status in POLICY_REVIEWED_STATUSES:
        if explicit_policy is None:
            raise ValueError(
                "provider_directory_reviewed_root_policy_required"
            )
        return explicit_policy
    raise ValueError("provider_directory_reviewed_root_policy_status_invalid")
