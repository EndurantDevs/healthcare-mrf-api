# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable consumer handoff construction from verified database state."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import asyncpg

from process.provider_directory_retained_artifact_contract import (
    ArtifactLayoutRange,
    RetainedArtifactError,
    RetainedCampaignIncomplete,
    RetainedCampaignMismatch,
    SealedRetainedArtifact,
    SealedRetainedCampaign,
    SealedTerminalStream,
)
from process.provider_directory_retained_seal_contract import (
    assert_item_layout_artifact_bindings,
)
from process.provider_directory_retained_identity_contract import (
    assert_campaign_membership,
    assert_planned_item_commitments,
    assert_planned_item_identities,
)
from process.provider_directory_retained_store_support import (
    assert_campaign_record_identity,
    database_record,
    database_record_list,
    database_table,
)


MAX_CLAIM_ITEMS = 100_000
MAX_CLAIM_STREAMS = 4096
MAX_CLAIM_LAYOUT_RANGES = 1_000_000


@dataclass(frozen=True)
class ClaimSnapshot:
    """All immutable rows covered by one sealed campaign digest."""

    campaign_record_by_field: Mapping[str, Any]
    campaign_item_states: tuple[Mapping[str, Any], ...]
    stream_states: tuple[Mapping[str, Any], ...]
    artifact_states: tuple[Mapping[str, Any], ...]
    layout_states: tuple[Mapping[str, Any], ...]
    layout_range_states: tuple[Mapping[str, Any], ...]
    admitted_item_states: tuple[Mapping[str, Any], ...] = field(init=False)

    def __post_init__(self) -> None:
        bounded_sequences = (
            (self.campaign_item_states, MAX_CLAIM_ITEMS),
            (self.stream_states, MAX_CLAIM_STREAMS),
            (self.artifact_states, MAX_CLAIM_ITEMS),
            (self.layout_states, MAX_CLAIM_ITEMS),
            (self.layout_range_states, MAX_CLAIM_LAYOUT_RANGES),
        )
        if any(
            type(sequence) is not tuple or len(sequence) > maximum
            for sequence, maximum in bounded_sequences
        ):
            raise RetainedArtifactError("consumer_claim_snapshot_unbounded")
        object.__setattr__(
            self,
            "admitted_item_states",
            tuple(
                campaign_item_state
                for campaign_item_state in self.campaign_item_states
                if campaign_item_state["status"] == "admitted"
            ),
        )


@dataclass(frozen=True)
class ClaimTables:
    campaign: str
    stream: str
    campaign_item: str
    campaign_item_identity: str
    artifact: str
    layout: str
    layout_range: str
    consumer: str
    consumer_reference: str


def claim_tables() -> ClaimTables:
    """Resolve the schema-qualified tables read during one consumer claim."""

    return ClaimTables(
        campaign=database_table("provider_directory_retained_artifact_campaign"),
        stream=database_table("provider_directory_retained_artifact_campaign_stream"),
        campaign_item=database_table(
            "provider_directory_retained_artifact_campaign_item"
        ),
        campaign_item_identity=database_table(
            "provider_directory_retained_artifact_campaign_item_identity"
        ),
        artifact=database_table("provider_directory_retained_artifact"),
        layout=database_table("provider_directory_retained_artifact_layout"),
        layout_range=database_table("provider_directory_retained_artifact_range"),
        consumer=database_table("provider_directory_retained_artifact_consumer"),
        consumer_reference=database_table(
            "provider_directory_retained_artifact_consumer_reference"
        ),
    )


async def _claim_identity_is_complete(
    connection: asyncpg.Connection,
    campaign_id: str,
    tables: ClaimTables,
    campaign: Mapping[str, Any],
    item_states: Sequence[Mapping[str, Any]],
    stream_states: Sequence[Mapping[str, Any]],
) -> None:
    """Verify the complete item ledger and campaign membership commitment."""

    assert_planned_item_identities(item_states)
    commitment_states = database_record_list(
        await connection.fetch(
            f"""SELECT campaign_id, source_item_id, campaign_ordinal,
                        planned_identity_sha256
                   FROM {tables.campaign_item_identity}
                  WHERE campaign_id=$1 ORDER BY campaign_ordinal
                  LIMIT {MAX_CLAIM_ITEMS + 1} FOR SHARE;""",
            campaign_id,
        )
    )
    assert_planned_item_commitments(item_states, commitment_states)
    assert_campaign_membership(campaign, item_states, stream_states)


async def _claim_census(
    connection: asyncpg.Connection,
    campaign_id: str,
    tables: ClaimTables,
    campaign: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Load one bounded campaign census while its root row remains locked."""

    expected_items = campaign.get("expected_item_count")
    expected_streams = campaign.get("expected_stream_count")
    if (
        type(expected_items) is not int
        or not 0 <= expected_items <= MAX_CLAIM_ITEMS
        or type(expected_streams) is not int
        or not 0 <= expected_streams <= MAX_CLAIM_STREAMS
    ):
        raise RetainedCampaignMismatch("consumer_claim_census_unbounded")
    item_states = database_record_list(
        await connection.fetch(
            f"""SELECT * FROM {tables.campaign_item} WHERE campaign_id=$1
                  ORDER BY campaign_ordinal LIMIT {MAX_CLAIM_ITEMS + 1}
                  FOR SHARE;""",
            campaign_id,
        )
    )
    stream_states = database_record_list(
        await connection.fetch(
            f"""SELECT * FROM {tables.stream} WHERE campaign_id=$1
                  ORDER BY stream_ordinal LIMIT {MAX_CLAIM_STREAMS + 1}
                  FOR SHARE;""",
            campaign_id,
        )
    )
    admitted_item_states = [
        state for state in item_states if state["status"] == "admitted"
    ]
    if len(item_states) > MAX_CLAIM_ITEMS or len(stream_states) > MAX_CLAIM_STREAMS:
        raise RetainedCampaignMismatch("consumer_claim_census_unbounded")
    if (
        len(item_states) != expected_items
        or len(stream_states) != expected_streams
        or len(admitted_item_states) != campaign["admitted_item_count"]
        or any(
            state.get("artifact_sha256") is None or state.get("layout_sha256") is None
            for state in admitted_item_states
        )
    ):
        raise RetainedCampaignIncomplete("retained_items_not_claimable")
    await _claim_identity_is_complete(
        connection,
        campaign_id,
        tables,
        campaign,
        item_states,
        stream_states,
    )
    return item_states, stream_states


async def _locked_registry_states(
    connection: asyncpg.Connection,
    table_name: str,
    identity_column: str,
    identity_hashes: Sequence[str],
) -> list[dict[str, Any]]:
    if not identity_hashes:
        return []
    return database_record_list(
        await connection.fetch(
            f"""SELECT * FROM {table_name}
                  WHERE {identity_column}=ANY($1::text[])
                  ORDER BY {identity_column} FOR SHARE;""",
            identity_hashes,
        )
    )


async def _claim_registry(
    connection: asyncpg.Connection,
    tables: ClaimTables,
    admitted_item_states: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    artifact_hashes = sorted(
        {str(state["artifact_sha256"]) for state in admitted_item_states}
    )
    layout_hashes = sorted(
        {str(state["layout_sha256"]) for state in admitted_item_states}
    )
    artifacts = await _locked_registry_states(
        connection, tables.artifact, "artifact_sha256", artifact_hashes
    )
    layouts = await _locked_registry_states(
        connection, tables.layout, "layout_sha256", layout_hashes
    )
    if (
        len(artifacts) != len(artifact_hashes)
        or len(layouts) != len(layout_hashes)
        or any(state["registry_status"] != "verified" for state in artifacts)
        or any(state["registry_status"] != "verified" for state in layouts)
    ):
        raise RetainedCampaignIncomplete("retained_artifacts_not_claimable")
    assert_item_layout_artifact_bindings(
        admitted_item_states,
        layouts,
    )
    if not layout_hashes:
        return artifacts, layouts, []
    ranges = database_record_list(
        await connection.fetch(
            f"""SELECT * FROM {tables.layout_range}
                  WHERE layout_sha256=ANY($1::text[])
                  ORDER BY layout_sha256, range_ordinal
                  LIMIT {MAX_CLAIM_LAYOUT_RANGES + 1} FOR SHARE;""",
            layout_hashes,
        )
    )
    if len(ranges) > MAX_CLAIM_LAYOUT_RANGES:
        raise RetainedCampaignIncomplete("retained_layout_range_census_unbounded")
    return artifacts, layouts, ranges


async def locked_claim_snapshot(
    connection: asyncpg.Connection,
    campaign_id: str,
    tables: ClaimTables,
) -> ClaimSnapshot:
    """Lock and capture one complete, bounded campaign proof."""

    campaign = database_record(
        await connection.fetchrow(
            f"SELECT * FROM {tables.campaign} WHERE campaign_id=$1 FOR SHARE;",
            campaign_id,
        )
    )
    if campaign:
        assert_campaign_record_identity(campaign)
    if (
        not campaign
        or campaign.get("state") != "complete"
        or campaign.get("complete") is not True
        or campaign.get("released_at") is not None
    ):
        raise RetainedCampaignIncomplete("retained_campaign_not_complete")
    item_states, stream_states = await _claim_census(
        connection, campaign_id, tables, campaign
    )
    admitted_item_states = tuple(
        state for state in item_states if state["status"] == "admitted"
    )
    artifacts, layouts, ranges = await _claim_registry(
        connection,
        tables,
        admitted_item_states,
    )
    return ClaimSnapshot(
        campaign_record_by_field=campaign,
        campaign_item_states=tuple(item_states),
        stream_states=tuple(stream_states),
        artifact_states=tuple(artifacts),
        layout_states=tuple(layouts),
        layout_range_states=tuple(ranges),
    )


def _layout_ranges_by_hash(
    layout_range_states: Sequence[Mapping[str, Any]],
) -> dict[str, list[ArtifactLayoutRange]]:
    layout_range_registry: dict[str, list[ArtifactLayoutRange]] = {}
    for layout_range_state in layout_range_states:
        layout_sha256 = str(layout_range_state["layout_sha256"])
        layout_range_registry.setdefault(layout_sha256, []).append(
            ArtifactLayoutRange(
                range_ordinal=int(layout_range_state["range_ordinal"]),
                raw_byte_start=int(layout_range_state["raw_byte_start"]),
                raw_byte_end=int(layout_range_state["raw_byte_end"]),
                raw_byte_count=int(layout_range_state["raw_byte_count"]),
                raw_sha256=str(layout_range_state["raw_sha256"]),
                record_start=int(layout_range_state["record_start"]),
                record_end=int(layout_range_state["record_end"]),
                record_count=int(layout_range_state["record_count"]),
                canonical_sha256=str(layout_range_state["canonical_sha256"]),
                canonical_byte_count=int(layout_range_state["canonical_byte_count"]),
            )
        )
    return layout_range_registry


def _sealed_artifact_from_state(
    campaign_item_state: Mapping[str, Any],
    artifact_registry: Mapping[str, Mapping[str, Any]],
    layout_registry: Mapping[str, Mapping[str, Any]],
    layout_range_registry: Mapping[str, Sequence[ArtifactLayoutRange]],
) -> SealedRetainedArtifact:
    artifact_sha256 = str(campaign_item_state["artifact_sha256"])
    layout_sha256 = str(campaign_item_state["layout_sha256"])
    artifact_state = artifact_registry[artifact_sha256]
    layout_state = layout_registry[layout_sha256]
    return SealedRetainedArtifact(
        source_item_id=str(campaign_item_state["source_item_id"]),
        source_entry_sha256=str(campaign_item_state["source_entry_sha256"]),
        artifact_sha256=artifact_sha256,
        layout_sha256=layout_sha256,
        artifact_kind=str(campaign_item_state["artifact_kind"]),
        artifact_byte_count=int(artifact_state["artifact_byte_count"]),
        artifact_record_count=int(layout_state["artifact_record_count"]),
        family=str(campaign_item_state["family"]),
        collection_kind=str(campaign_item_state["collection_kind"]),
        partition_metadata_sha256=str(campaign_item_state["partition_metadata_sha256"]),
        stream_identity_sha256=str(campaign_item_state["stream_identity_sha256"]),
        sequence_ordinal=int(campaign_item_state["sequence_ordinal"]),
        item_role=str(campaign_item_state["item_role"]),
        acquisition_mode=str(campaign_item_state["acquisition_mode"]),
        layout_contract_id=str(layout_state["layout_contract_id"]),
        layout_contract_version=int(layout_state["layout_contract_version"]),
        layout_range_count=int(layout_state["layout_range_count"]),
        range_set_sha256=str(layout_state["range_set_sha256"]),
        canonical_byte_count=int(layout_state["canonical_byte_count"]),
        manifest_sha256=str(layout_state["manifest_sha256"]),
        ranges=tuple(layout_range_registry[layout_sha256]),
    )


def _sealed_artifact_handoff(
    claim_snapshot: ClaimSnapshot,
) -> tuple[SealedRetainedArtifact, ...]:
    artifact_registry = {
        str(artifact_state["artifact_sha256"]): artifact_state
        for artifact_state in claim_snapshot.artifact_states
    }
    layout_registry = {
        str(layout_state["layout_sha256"]): layout_state
        for layout_state in claim_snapshot.layout_states
    }
    layout_range_registry = _layout_ranges_by_hash(claim_snapshot.layout_range_states)
    return tuple(
        _sealed_artifact_from_state(
            campaign_item_state,
            artifact_registry,
            layout_registry,
            layout_range_registry,
        )
        for campaign_item_state in claim_snapshot.admitted_item_states
    )


def _terminal_stream_handoff(
    stream_states: Sequence[Mapping[str, Any]],
) -> tuple[SealedTerminalStream, ...]:
    return tuple(
        SealedTerminalStream(
            stream_identity_sha256=str(stream_state["stream_identity_sha256"]),
            terminal_sequence_ordinal=int(stream_state["terminal_sequence_ordinal"]),
            terminal_proof_sha256=str(stream_state["terminal_proof_sha256"]),
        )
        for stream_state in stream_states
        if stream_state["complete"]
    )


def _family_kind_census(
    admitted_item_states: Sequence[Mapping[str, Any]],
) -> tuple[tuple[str, str, int], ...]:
    family_kind_counts = Counter(
        (
            str(campaign_item_state["family"]),
            str(campaign_item_state["collection_kind"]),
        )
        for campaign_item_state in admitted_item_states
    )
    return tuple(
        (family, collection_kind, count)
        for (family, collection_kind), count in sorted(family_kind_counts.items())
    )


def sealed_campaign_handoff(
    claim_snapshot: ClaimSnapshot,
    *,
    consumer_claim_generation: int,
) -> SealedRetainedCampaign:
    """Build the public claim response from a verified immutable snapshot."""

    campaign_record_by_field = claim_snapshot.campaign_record_by_field
    return SealedRetainedCampaign(
        contract_id=str(campaign_record_by_field["contract_id"]),
        campaign_id=str(campaign_record_by_field["campaign_id"]),
        campaign_sha256=str(campaign_record_by_field["campaign_sha256"]),
        consumer_claim_generation=consumer_claim_generation,
        adapter_id=str(campaign_record_by_field["adapter_id"]),
        endpoint_id=str(campaign_record_by_field["endpoint_id"]),
        credential_descriptor_sha256=str(
            campaign_record_by_field["credential_descriptor_sha256"]
        ),
        source_census_sha256=str(campaign_record_by_field["source_census_sha256"]),
        observation_set_sha256=str(campaign_record_by_field["observation_set_sha256"]),
        census_mode=str(campaign_record_by_field["census_mode"]),
        complete=True,
        expected_item_count=int(campaign_record_by_field["expected_item_count"]),
        admitted_item_count=int(campaign_record_by_field["admitted_item_count"]),
        admitted_byte_count=int(campaign_record_by_field["admitted_byte_count"]),
        admitted_record_count=int(campaign_record_by_field["admitted_record_count"]),
        terminal_zero_item_count=int(
            campaign_record_by_field["terminal_zero_item_count"]
        ),
        family_kind_census=_family_kind_census(claim_snapshot.admitted_item_states),
        terminal_streams=_terminal_stream_handoff(claim_snapshot.stream_states),
        artifacts=_sealed_artifact_handoff(claim_snapshot),
    )
