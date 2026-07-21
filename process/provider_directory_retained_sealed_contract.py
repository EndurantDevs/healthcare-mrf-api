# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable consumer handoff and campaign sealing digests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from process.provider_directory_retained_artifact_base import (
    require_digest,
    sha256_json,
)
from process.provider_directory_retained_layout_contract import ArtifactLayoutRange


_ITEM_DIGEST_FIELDS = (
    "source_item_id",
    "planned_identity_sha256",
    "source_entry_sha256",
    "artifact_kind",
    "family",
    "collection_kind",
    "partition_metadata_sha256",
    "stream_identity_sha256",
    "sequence_ordinal",
    "item_role",
    "status",
    "acquisition_mode",
    "observed_byte_count",
    "immutable_identity_sha256",
    "terminal_proof_sha256",
    "artifact_sha256",
    "layout_sha256",
    "safe_failure_code",
)
_ARTIFACT_DIGEST_FIELDS = (
    "artifact_sha256",
    "artifact_byte_count",
    "registry_status",
)
_LAYOUT_DIGEST_FIELDS = (
    "layout_sha256",
    "artifact_sha256",
    "artifact_record_count",
    "registry_status",
    "layout_contract_id",
    "layout_contract_version",
    "layout_range_count",
    "range_set_sha256",
    "canonical_byte_count",
    "manifest_sha256",
    "manifest_byte_count",
    "producer_build_id",
)
_RANGE_DIGEST_FIELDS = (
    "layout_sha256",
    "artifact_sha256",
    "layout_contract_version",
    "layout_range_count",
    "range_ordinal",
    "raw_byte_start",
    "raw_byte_end",
    "raw_byte_count",
    "raw_sha256",
    "record_start",
    "record_end",
    "record_count",
    "canonical_sha256",
    "canonical_byte_count",
)


@dataclass(frozen=True)
class SealedRetainedArtifact:
    """One claimed item binding and its sole authoritative artifact proof."""

    source_item_id: str
    source_entry_sha256: str
    artifact_sha256: str
    layout_sha256: str
    artifact_kind: str
    artifact_byte_count: int
    artifact_record_count: int
    family: str
    collection_kind: str
    partition_metadata_sha256: str
    stream_identity_sha256: str
    sequence_ordinal: int
    item_role: str
    acquisition_mode: str
    layout_contract_id: str
    layout_contract_version: int
    layout_range_count: int
    range_set_sha256: str
    canonical_byte_count: int
    manifest_sha256: str
    ranges: tuple[ArtifactLayoutRange, ...]


@dataclass(frozen=True)
class SealedTerminalStream:
    """Exact ordered-stream terminal-zero proof."""

    stream_identity_sha256: str
    terminal_sequence_ordinal: int
    terminal_proof_sha256: str


@dataclass(frozen=True)
class SealedRetainedCampaign:
    """Immutable physical handoff claimed by projection/materialization."""

    contract_id: str
    campaign_id: str
    campaign_sha256: str
    consumer_claim_generation: int
    adapter_id: str
    endpoint_id: str
    credential_descriptor_sha256: str
    source_census_sha256: str
    observation_set_sha256: str
    census_mode: str
    complete: bool
    expected_item_count: int
    admitted_item_count: int
    admitted_byte_count: int
    admitted_record_count: int
    terminal_zero_item_count: int
    family_kind_census: tuple[tuple[str, str, int], ...]
    terminal_streams: tuple[SealedTerminalStream, ...]
    artifacts: tuple[SealedRetainedArtifact, ...]

    @property
    def catalog_set_sha256(self) -> str:
        """Return the compatibility alias for the sealed source census."""

        return self.source_census_sha256

    @property
    def expected_file_count(self) -> int:
        """Return the compatibility alias for expected physical items."""

        return self.expected_item_count

    @property
    def admitted_file_count(self) -> int:
        """Return the compatibility alias for admitted physical items."""

        return self.admitted_item_count


def item_census_digest(item_rows: Sequence[Mapping[str, Any]]) -> str:
    """Seal every expected item, including terminal/failed/unaccounted outcomes."""

    terminal_statuses = {"admitted", "terminal_zero", "failed", "unaccounted"}
    return sha256_json(
        [
            {
                "campaign_ordinal": int(item_state["campaign_ordinal"]),
                "source_item_id": require_digest(
                    item_state.get("source_item_id"),
                    "source_item_id",
                ),
                "source_entry_sha256": require_digest(
                    item_state.get("source_entry_sha256"),
                    "source_entry_sha256",
                ),
                "artifact_kind": item_state["artifact_kind"],
                "family": item_state["family"],
                "collection_kind": item_state["collection_kind"],
                "partition_metadata_sha256": item_state["partition_metadata_sha256"],
                "stream_identity_sha256": item_state["stream_identity_sha256"],
                "sequence_ordinal": item_state["sequence_ordinal"],
                "item_role": item_state["item_role"],
                "status": (
                    item_state["status"]
                    if item_state.get("status") in terminal_statuses
                    else "unaccounted"
                ),
                "terminal_proof_sha256": item_state.get("terminal_proof_sha256"),
                "artifact_sha256": item_state.get("artifact_sha256"),
                "safe_failure_code": item_state.get("safe_failure_code"),
            }
            for item_state in sorted(
                item_rows,
                key=lambda item_state: int(item_state["campaign_ordinal"]),
            )
        ]
    )


def observation_set_digest(item_rows: Sequence[Mapping[str, Any]]) -> str:
    """Seal immutable observations without including private validators."""

    return sha256_json(
        [
            {
                "source_item_id": item_state["source_item_id"],
                "mode": item_state.get("acquisition_mode") or "unobserved",
                "byte_count": item_state.get("observed_byte_count"),
                "validator_kind": item_state.get("validator_kind"),
                "validator_sha256": item_state.get("validator_sha256"),
                "immutable_identity_sha256": item_state.get(
                    "immutable_identity_sha256"
                ),
                "terminal_proof_sha256": item_state.get("terminal_proof_sha256"),
            }
            for item_state in sorted(
                item_rows,
                key=lambda item_state: int(item_state["campaign_ordinal"]),
            )
        ]
    )


def _selected_digest_fields(
    source_state: Mapping[str, Any],
    field_names: Sequence[str],
) -> dict[str, Any]:
    """Select an ordered public field set from one immutable state record."""

    return {field_name: source_state.get(field_name) for field_name in field_names}


def _ordered_item_digest_records(
    item_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return item digest records in campaign census order."""

    ordered_states = sorted(
        item_rows,
        key=lambda item_state: int(item_state["campaign_ordinal"]),
    )
    return [
        _selected_digest_fields(item_state, _ITEM_DIGEST_FIELDS)
        for item_state in ordered_states
    ]


def _ordered_stream_digest_records(
    stream_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return the public terminal state of each ordered stream."""

    ordered_states = sorted(
        stream_rows,
        key=lambda stream_state: int(stream_state["stream_ordinal"]),
    )
    return [
        {
            "stream_identity_sha256": stream_state["stream_identity_sha256"],
            "stream_ordinal": stream_state["stream_ordinal"],
            "terminal_sequence_ordinal": stream_state.get("terminal_sequence_ordinal"),
            "terminal_proof_sha256": stream_state.get("terminal_proof_sha256"),
            "complete": bool(stream_state.get("complete")),
        }
        for stream_state in ordered_states
    ]


def _ordered_artifact_digest_records(
    artifact_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return physical artifacts ordered by content identity."""

    ordered_states = sorted(
        artifact_rows,
        key=lambda artifact_state: str(artifact_state["artifact_sha256"]),
    )
    return [
        _selected_digest_fields(artifact_state, _ARTIFACT_DIGEST_FIELDS)
        for artifact_state in ordered_states
    ]


def _ordered_layout_digest_records(
    layout_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return semantic layouts ordered by layout identity."""

    ordered_states = sorted(
        layout_rows,
        key=lambda layout_state: str(layout_state["layout_sha256"]),
    )
    return [
        _selected_digest_fields(layout_state, _LAYOUT_DIGEST_FIELDS)
        for layout_state in ordered_states
    ]


def _ordered_range_digest_records(
    range_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return canonical range proofs in layout and ordinal order."""

    ordered_states = sorted(
        range_rows,
        key=lambda range_state: (
            str(range_state["layout_sha256"]),
            int(range_state["range_ordinal"]),
        ),
    )
    return [
        _selected_digest_fields(range_state, _RANGE_DIGEST_FIELDS)
        for range_state in ordered_states
    ]


def sealed_campaign_digest(
    *,
    campaign_row: Mapping[str, Any],
    item_rows: Sequence[Mapping[str, Any]],
    stream_rows: Sequence[Mapping[str, Any]],
    artifact_rows: Sequence[Mapping[str, Any]],
    layout_rows: Sequence[Mapping[str, Any]],
    range_rows: Sequence[Mapping[str, Any]],
) -> str:
    """Hash public immutable proof; private and mutable locators are excluded."""

    campaign_fields = (
        "contract_id",
        "campaign_id",
        "adapter_id",
        "endpoint_id",
        "request_fence_id",
        "credential_descriptor_sha256",
        "source_census_sha256",
        "identity_members_sha256",
        "identity_member_count",
        "census_mode",
        "item_census_sha256",
        "observation_set_sha256",
        "expected_item_count",
        "expected_stream_count",
        "admitted_item_count",
        "admitted_byte_count",
        "admitted_record_count",
        "terminal_zero_item_count",
    )
    digest_state = _selected_digest_fields(campaign_row, campaign_fields)
    digest_state.update(
        {
            "items": _ordered_item_digest_records(item_rows),
            "streams": _ordered_stream_digest_records(stream_rows),
            "artifacts": _ordered_artifact_digest_records(artifact_rows),
            "layouts": _ordered_layout_digest_records(layout_rows),
            "ranges": _ordered_range_digest_records(range_rows),
        }
    )
    return sha256_json(digest_state)
