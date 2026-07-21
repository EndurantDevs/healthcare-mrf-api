# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable campaign membership and append-only item commitments."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from process.provider_directory_retained_artifact_base import (
    FIXED_CATALOG,
    ORDERED_STREAMS,
    RetainedCampaignMismatch,
    require_digest,
    sha256_json,
)


def planned_item_membership_digest(item_state: Mapping[str, Any]) -> str:
    """Hash one item's campaign-independent planned identity."""

    return sha256_json(
        {
            "source_item_id": require_digest(
                item_state.get("source_item_id"),
                "source_item_id",
            ),
            "campaign_ordinal": int(item_state["campaign_ordinal"]),
            "source_entry_sha256": require_digest(
                item_state.get("source_entry_sha256"),
                "source_entry_sha256",
            ),
            "artifact_kind": item_state["artifact_kind"],
            "family": item_state["family"],
            "collection_kind": item_state["collection_kind"],
            "partition_metadata_sha256": require_digest(
                item_state.get("partition_metadata_sha256"),
                "partition_metadata_sha256",
            ),
            "stream_identity_sha256": require_digest(
                item_state.get("stream_identity_sha256"),
                "stream_identity_sha256",
            ),
            "sequence_ordinal": int(item_state["sequence_ordinal"]),
            "item_role": item_state["item_role"],
            "declared_byte_count": item_state.get("declared_byte_count"),
            "terminal_proof_sha256": item_state.get("terminal_proof_sha256"),
        }
    )


def planned_item_identity_digest(item_state: Mapping[str, Any]) -> str:
    """Bind one immutable planned item to its exact campaign."""

    return sha256_json(
        {
            "campaign_id": require_digest(
                item_state.get("campaign_id"),
                "campaign_id",
            ),
            "item_membership_sha256": planned_item_membership_digest(item_state),
        }
    )


def planned_campaign_membership_digest(
    *,
    census_mode: str,
    item_states: Sequence[Mapping[str, Any]],
    stream_states: Sequence[Mapping[str, Any]],
) -> str:
    """Commit a fixed item census or an ordered stream-root census."""

    if census_mode == FIXED_CATALOG:
        membership_by_field = {
            "census_mode": FIXED_CATALOG,
            "items": [
                planned_item_membership_digest(item_state)
                for item_state in sorted(
                    item_states,
                    key=lambda state: int(state["campaign_ordinal"]),
                )
            ],
        }
    elif census_mode == ORDERED_STREAMS:
        membership_by_field = {
            "census_mode": ORDERED_STREAMS,
            "streams": [
                {
                    "stream_identity_sha256": require_digest(
                        stream_state.get("stream_identity_sha256"),
                        "stream_identity_sha256",
                    ),
                    "stream_ordinal": int(stream_state["stream_ordinal"]),
                }
                for stream_state in sorted(
                    stream_states,
                    key=lambda state: int(state["stream_ordinal"]),
                )
            ],
        }
    else:
        raise RetainedCampaignMismatch("retained_campaign_census_mode_mismatch")
    return sha256_json(membership_by_field)


def assert_campaign_membership(
    campaign_state: Mapping[str, Any],
    item_states: Sequence[Mapping[str, Any]],
    stream_states: Sequence[Mapping[str, Any]],
) -> None:
    """Verify the persisted fixed census or ordered stream roots."""

    census_mode = str(campaign_state.get("census_mode"))
    actual_count = (
        len(item_states) if census_mode == FIXED_CATALOG else len(stream_states)
    )
    if campaign_state.get(
        "identity_member_count"
    ) != actual_count or campaign_state.get(
        "identity_members_sha256"
    ) != planned_campaign_membership_digest(
        census_mode=census_mode,
        item_states=item_states,
        stream_states=stream_states,
    ):
        raise RetainedCampaignMismatch("retained_campaign_membership_mismatch")


def assert_planned_item_identities(
    item_states: Sequence[Mapping[str, Any]],
) -> None:
    """Reject any item whose planned immutable fields were replaced."""

    if any(
        item_state.get("planned_identity_sha256")
        != planned_item_identity_digest(item_state)
        for item_state in item_states
    ):
        raise RetainedCampaignMismatch("retained_item_planned_identity_mismatch")


def assert_planned_item_commitments(
    item_states: Sequence[Mapping[str, Any]],
    commitment_states: Sequence[Mapping[str, Any]],
) -> None:
    """Reject missing, added, or replaced append-only item commitments."""

    expected_commitments = sorted(
        (
            str(item_state["campaign_id"]),
            str(item_state["source_item_id"]),
            int(item_state["campaign_ordinal"]),
            str(item_state["planned_identity_sha256"]),
        )
        for item_state in item_states
    )
    actual_commitments = sorted(
        (
            str(commitment_state["campaign_id"]),
            str(commitment_state["source_item_id"]),
            int(commitment_state["campaign_ordinal"]),
            str(commitment_state["planned_identity_sha256"]),
        )
        for commitment_state in commitment_states
    )
    if actual_commitments != expected_commitments:
        raise RetainedCampaignMismatch("retained_item_commitment_mismatch")
