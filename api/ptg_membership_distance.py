# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stable address-prefix state for bounded distance membership traversal."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Sequence

from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

if TYPE_CHECKING:
    from api.ptg2_serving import _GraphLocationCandidates


@dataclass
class _LocalDistanceGraphState:
    seen_npis: set[int] = field(default_factory=set)
    observed_prefix: tuple[tuple[int, str, str], ...] = ()
    matched_locations: list[dict[str, Any]] = field(default_factory=list)
    matching_sets_by_npi: dict[int, set[int]] = field(default_factory=dict)
    code_sets: set[int] = field(default_factory=set)
    noncode_sets: set[int] = field(default_factory=set)
    retained_memberships: int = 0
    membership_index_checked: bool = False
    membership_index: Any = None
    indexed_projection_members: int = 0

    def candidates(self) -> _GraphLocationCandidates:
        """Return the exact code-matched prefix accumulated so far."""

        from api.ptg2_serving import _GraphLocationCandidates

        return _GraphLocationCandidates(
            self.matched_locations,
            self.matching_sets_by_npi,
            taxonomy_filtered=True,
        )


def _new_local_taxonomy_locations(
    locations: Sequence[dict[str, Any]],
    state: _LocalDistanceGraphState,
) -> list[dict[str, Any]]:
    """Validate one stable ordered prefix and return its newly seen rows."""

    from api.ptg2_serving import _ptg2_row_address_key

    valid_locations = [location for location in locations if location.get("npi") not in (None, "")]
    current_prefix_rows = tuple(
        (
            int(location["npi"]),
            str(location.get("location_hash") or _ptg2_row_address_key(location) or ""),
            str(location.get("distance_miles") or ""),
        )
        for location in valid_locations
    )
    previous_count = len(state.observed_prefix)
    if current_prefix_rows[:previous_count] != state.observed_prefix:
        raise PTG2ManifestArtifactError("PTG2 nearby location prefix changed during bounded traversal")
    state.observed_prefix = current_prefix_rows
    return valid_locations[previous_count:]


def _has_exact_network_totals(network_responses):
    """A union has an exact total only when every contributing payload proves it."""
    return all(
        (network_response.get("pagination") or {}).get("total_is_exact") is True
        for _, _, network_response in network_responses
        if network_response is not None
    )
