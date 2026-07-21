# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure completeness rules for source-neutral retained campaign sealing."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from process.provider_directory_retained_artifact_contract import (
    FIXED_CATALOG,
    ORDERED_STREAMS,
    RetainedCampaignMismatch,
)


@dataclass(frozen=True)
class SealSnapshot:
    """Database-independent state required to calculate one campaign seal."""

    campaign_record_by_field: Mapping[str, Any]
    campaign_item_states: Sequence[Mapping[str, Any]]
    stream_states: Sequence[Mapping[str, Any]]
    artifact_states: Sequence[Mapping[str, Any]]
    layout_states: Sequence[Mapping[str, Any]]
    layout_range_states: Sequence[Mapping[str, Any]]
    terminal_stream_count: int

    @property
    def admitted_item_states(self) -> list[Mapping[str, Any]]:
        """Return campaign members backed by verified registry artifacts."""

        return [
            campaign_item_state
            for campaign_item_state in self.campaign_item_states
            if campaign_item_state["status"] == "admitted"
        ]


def assert_layout_range_census(
    layout_states: Sequence[Mapping[str, Any]],
    layout_range_states: Sequence[Mapping[str, Any]],
) -> None:
    """Require every layout to own its declared number of immutable ranges."""

    range_counts_by_layout = Counter(
        layout_range_state["layout_sha256"]
        for layout_range_state in layout_range_states
    )
    is_range_census_incomplete = any(
        range_counts_by_layout[layout_state["layout_sha256"]]
        != layout_state["layout_range_count"]
        for layout_state in layout_states
    )
    if is_range_census_incomplete:
        raise RetainedCampaignMismatch("campaign_layout_ranges_incomplete")


def assert_item_layout_artifact_bindings(
    admitted_item_states: Sequence[Mapping[str, Any]],
    layout_states: Sequence[Mapping[str, Any]],
) -> None:
    """Require every admitted item layout to belong to its exact artifact."""

    layout_artifact_by_hash = {
        layout_state["layout_sha256"]: layout_state["artifact_sha256"]
        for layout_state in layout_states
    }
    if any(
        layout_artifact_by_hash.get(item_state["layout_sha256"])
        != item_state["artifact_sha256"]
        for item_state in admitted_item_states
    ):
        raise RetainedCampaignMismatch("campaign_layout_artifact_mismatch")


def campaign_seal_counts(seal_snapshot: SealSnapshot) -> dict[str, int]:
    """Calculate persisted completeness counters from immutable state."""

    status_counts = Counter(
        campaign_item_state["status"]
        for campaign_item_state in seal_snapshot.campaign_item_states
    )
    artifact_registry = {
        artifact_state["artifact_sha256"]: artifact_state
        for artifact_state in seal_snapshot.artifact_states
    }
    layout_registry = {
        layout_state["layout_sha256"]: layout_state
        for layout_state in seal_snapshot.layout_states
    }
    return {
        "observed_item_count": sum(
            campaign_item_state.get("observed_byte_count") is not None
            for campaign_item_state in seal_snapshot.campaign_item_states
        ),
        "expected_byte_count": sum(
            int(campaign_item_state.get("observed_byte_count") or 0)
            for campaign_item_state in seal_snapshot.campaign_item_states
        ),
        "admitted_item_count": len(seal_snapshot.admitted_item_states),
        "admitted_byte_count": sum(
            int(
                artifact_registry[campaign_item_state["artifact_sha256"]][
                    "artifact_byte_count"
                ]
            )
            for campaign_item_state in seal_snapshot.admitted_item_states
        ),
        "admitted_record_count": sum(
            int(
                layout_registry[campaign_item_state["layout_sha256"]][
                    "artifact_record_count"
                ]
            )
            for campaign_item_state in seal_snapshot.admitted_item_states
        ),
        "terminal_zero_item_count": status_counts["terminal_zero"],
        "terminal_stream_count": seal_snapshot.terminal_stream_count,
        "failed_item_count": status_counts["failed"],
        "unaccounted_item_count": status_counts["unaccounted"],
    }


def is_complete_campaign(
    seal_snapshot: SealSnapshot,
    seal_counts: Mapping[str, int],
) -> bool:
    """Return whether fixed or streamed completeness evidence is sufficient."""

    campaign_record_by_field = seal_snapshot.campaign_record_by_field
    is_item_census_complete = (
        seal_counts["admitted_item_count"] + seal_counts["terminal_zero_item_count"]
        == campaign_record_by_field["expected_item_count"]
    )
    is_fixed_complete = (
        campaign_record_by_field["census_mode"] == FIXED_CATALOG
        and seal_counts["terminal_zero_item_count"] == 0
    )
    is_stream_complete = (
        campaign_record_by_field["census_mode"] == ORDERED_STREAMS
        and seal_counts["terminal_stream_count"]
        == campaign_record_by_field["expected_stream_count"]
    )
    return (
        seal_counts["failed_item_count"] == 0
        and seal_counts["unaccounted_item_count"] == 0
        and is_item_census_complete
        and (is_fixed_complete or is_stream_complete)
    )
