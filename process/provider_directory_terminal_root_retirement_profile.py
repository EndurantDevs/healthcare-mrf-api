# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed runtime profiles for terminal Provider Directory root retirement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


RESOURCE_HASH_CONTRACT_METADATA_KEY = "resource_hash_contract"


class TerminalRootRetirementProfileError(ValueError):
    """Reject metadata that cannot select one exact retirement profile."""


@dataclass(frozen=True)
class TerminalRootRetirementProfile:
    """Bind one persisted marker contract to its database validators."""

    metadata_key: str
    contract_version: str
    resource_hash_contract: str
    evidence_function: str
    valid_function: str


LEGACY_RETIREMENT_PROFILE = TerminalRootRetirementProfile(
    metadata_key="provider_directory_terminal_root_retirement_v1",
    contract_version="healthporta.provider-directory.terminal-root-retirement.v1",
    resource_hash_contract="transport_bound_v1",
    evidence_function="provider_directory_terminal_root_retirement_evidence",
    valid_function="provider_directory_terminal_root_retirement_valid",
)
SEMANTIC_V4_RETIREMENT_PROFILE = TerminalRootRetirementProfile(
    metadata_key="provider_directory_terminal_root_retirement_v2",
    contract_version="healthporta.provider-directory.terminal-root-retirement.v2",
    resource_hash_contract="semantic_content_v4",
    evidence_function="provider_directory_terminal_root_retirement_v2_evidence",
    valid_function="provider_directory_terminal_root_retirement_v2_valid",
)
RETIREMENT_PROFILES = (
    LEGACY_RETIREMENT_PROFILE,
    SEMANTIC_V4_RETIREMENT_PROFILE,
)
RETIREMENT_METADATA_KEYS = frozenset(
    profile.metadata_key for profile in RETIREMENT_PROFILES
)


def _hash_contract_profile(
    metadata_by_field: Mapping[str, Any],
) -> TerminalRootRetirementProfile:
    if RESOURCE_HASH_CONTRACT_METADATA_KEY not in metadata_by_field:
        return LEGACY_RETIREMENT_PROFILE
    hash_contract = metadata_by_field.get(RESOURCE_HASH_CONTRACT_METADATA_KEY)
    for profile in RETIREMENT_PROFILES:
        if hash_contract == profile.resource_hash_contract:
            return profile
    raise TerminalRootRetirementProfileError("resource_hash_contract_invalid")


def selected_retirement_profile(
    publication_metadata: Mapping[str, Any],
) -> TerminalRootRetirementProfile:
    """Resolve one exact profile and reject cross-version marker metadata."""

    metadata_by_field = dict(publication_metadata)
    profile = _hash_contract_profile(metadata_by_field)
    present_marker_keys = RETIREMENT_METADATA_KEYS.intersection(metadata_by_field)
    if len(present_marker_keys) > 1 or (
        present_marker_keys and profile.metadata_key not in present_marker_keys
    ):
        raise TerminalRootRetirementProfileError("retirement_marker_invalid")
    return profile


__all__ = (
    "LEGACY_RETIREMENT_PROFILE",
    "RETIREMENT_METADATA_KEYS",
    "RETIREMENT_PROFILES",
    "SEMANTIC_V4_RETIREMENT_PROFILE",
    "TerminalRootRetirementProfile",
    "TerminalRootRetirementProfileError",
    "selected_retirement_profile",
)
