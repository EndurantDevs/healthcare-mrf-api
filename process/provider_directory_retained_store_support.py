# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared database naming and record conversion for retained acquisition."""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from operator import attrgetter
from typing import Any, NamedTuple

from process.provider_directory_retained_artifact_base import (
    ARTIFACT_KINDS,
    FIXED_CATALOG,
    ORDERED_STREAMS,
    PAYLOAD,
    RETAINED_ARTIFACT_CONTRACT_ID,
    TERMINAL_ZERO,
    RetainedArtifactError,
    RetainedCampaignMismatch,
    require_digest,
    require_nonnegative_int,
    require_positive_int,
    require_safe_id,
    sha256_json,
)
from process.provider_directory_retained_campaign_contract import (
    MAX_CAMPAIGN_ITEMS,
    MAX_EXPECTED_STREAM_IDENTITIES,
    RetainedCampaignItem,
    RetainedCampaignPlan,
    _LivePrivateSourceLocator,
    endpoint_request_fence_digest,
)
from process.provider_directory_retained_identity_contract import (
    planned_campaign_membership_digest,
)


_SCHEMA_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
MUTABLE_CAMPAIGN_STATES = frozenset(
    {"planned", "preflighting", "downloading", "sealed_incomplete"}
)


class CampaignItemSnapshot(NamedTuple):
    owner: RetainedCampaignItem
    source_item_id: str
    source_entry_sha256: str
    artifact_kind: str
    family: str
    collection_kind: str
    partition_metadata_sha256: str
    stream_identity_sha256: str
    sequence_ordinal: int
    item_role: str
    source_locator: _LivePrivateSourceLocator | None
    declared_byte_count: int | None
    terminal_proof_sha256: str | None


class CampaignPlanSnapshot(NamedTuple):
    adapter_id: str
    endpoint_id: str
    request_fence_id: str
    credential_descriptor_sha256: str
    source_census_sha256: str
    census_mode: str
    items: tuple[CampaignItemSnapshot, ...]
    per_item_byte_budget: int
    aggregate_byte_budget: int
    expected_stream_identities: tuple[str, ...]


def validated_campaign_item_snapshot(
    campaign_item: RetainedCampaignItem,
) -> CampaignItemSnapshot:
    """Capture and validate one item exactly once without private disclosure."""

    if type(campaign_item) is not RetainedCampaignItem:
        raise RetainedArtifactError("campaign_item_invalid")
    item_snapshot = CampaignItemSnapshot(
        campaign_item,
        campaign_item.source_item_id,
        campaign_item.source_entry_sha256,
        campaign_item.artifact_kind,
        campaign_item.family,
        campaign_item.collection_kind,
        campaign_item._partition_metadata_sha256,
        campaign_item.stream_identity_sha256,
        campaign_item.sequence_ordinal,
        campaign_item.item_role,
        campaign_item.source_locator,
        campaign_item.declared_byte_count,
        campaign_item.terminal_proof_sha256,
    )
    _validate_campaign_item_snapshot(item_snapshot, campaign_item)
    return item_snapshot


def _validate_campaign_item_snapshot(
    item_snapshot: CampaignItemSnapshot,
    campaign_item: RetainedCampaignItem,
) -> None:
    canonical_metadata = campaign_item._partition_metadata_canonical
    if (
        type(canonical_metadata) is not bytes
        or len(canonical_metadata) > 64 * 1024
        or hashlib.sha256(canonical_metadata).hexdigest()
        != item_snapshot.partition_metadata_sha256
    ):
        raise RetainedArtifactError("partition_metadata_binding_mismatch")
    require_digest(item_snapshot.source_item_id, "source_item_id")
    require_digest(item_snapshot.source_entry_sha256, "source_entry_sha256")
    require_digest(item_snapshot.stream_identity_sha256, "stream_identity_sha256")
    require_digest(item_snapshot.partition_metadata_sha256, "partition_metadata_sha256")
    if item_snapshot.artifact_kind not in ARTIFACT_KINDS:
        raise RetainedArtifactError("artifact_kind_invalid")
    require_safe_id(item_snapshot.family, "family", maximum=64)
    require_safe_id(item_snapshot.collection_kind, "collection_kind", maximum=64)
    require_nonnegative_int(item_snapshot.sequence_ordinal, "sequence_ordinal")
    if item_snapshot.declared_byte_count is not None:
        require_nonnegative_int(
            item_snapshot.declared_byte_count, "declared_byte_count"
        )
    if item_snapshot.item_role == PAYLOAD:
        if (
            type(item_snapshot.source_locator) is not _LivePrivateSourceLocator
            or item_snapshot.terminal_proof_sha256 is not None
        ):
            raise RetainedArtifactError("payload_item_invalid")
        item_snapshot.source_locator.validate(campaign_item)
    elif item_snapshot.item_role == TERMINAL_ZERO:
        require_digest(item_snapshot.terminal_proof_sha256, "terminal_proof_sha256")
        if (
            item_snapshot.source_locator is not None
            or item_snapshot.declared_byte_count
            not in {
                None,
                0,
            }
        ):
            raise RetainedArtifactError("terminal_zero_item_invalid")
    else:
        raise RetainedArtifactError("item_role_invalid")


def open_snapshot_source_locator(item_snapshot: CampaignItemSnapshot) -> str:
    """Open only the owner-bound locator captured in one validated snapshot."""

    source_locator = item_snapshot.source_locator
    if type(source_locator) is not _LivePrivateSourceLocator:
        raise RetainedArtifactError("source_locator_unavailable")
    return source_locator._open_for_owner(item_snapshot.owner)


def _validated_expected_streams(
    expected_streams: tuple[str, ...],
) -> tuple[str, ...]:
    previous_stream_id = None
    for stream_id in expected_streams:
        require_digest(stream_id, "expected_stream_identity")
        if previous_stream_id is not None and stream_id <= previous_stream_id:
            raise RetainedArtifactError("expected_streams_not_canonical")
        previous_stream_id = stream_id
    return expected_streams


def _ordered_item_snapshots(
    plan_values: CampaignPlanSnapshot,
) -> tuple[CampaignItemSnapshot, ...]:
    item_snapshots: list[CampaignItemSnapshot] = []
    item_ids: set[str] = set()
    stream_sequences: set[tuple[str, int]] = set()
    known_total = 0
    for campaign_item in plan_values.items:
        item_snapshot = validated_campaign_item_snapshot(campaign_item)
        if item_snapshot.source_item_id in item_ids:
            raise RetainedArtifactError("campaign_item_identity_duplicate")
        item_ids.add(item_snapshot.source_item_id)
        stream_sequence = (
            item_snapshot.stream_identity_sha256,
            item_snapshot.sequence_ordinal,
        )
        if stream_sequence in stream_sequences:
            raise RetainedArtifactError("campaign_stream_sequence_duplicate")
        stream_sequences.add(stream_sequence)
        if item_snapshot.declared_byte_count is not None:
            if item_snapshot.declared_byte_count > plan_values.per_item_byte_budget:
                raise RetainedArtifactError("per_item_byte_budget_exceeded")
            known_total += item_snapshot.declared_byte_count
        item_snapshots.append(item_snapshot)
    if known_total > plan_values.aggregate_byte_budget:
        raise RetainedArtifactError("aggregate_byte_budget_exceeded")
    if plan_values.census_mode == FIXED_CATALOG:
        order_key = attrgetter("family", "collection_kind", "source_item_id")
    else:
        order_key = attrgetter(
            "stream_identity_sha256",
            "sequence_ordinal",
            "source_item_id",
        )
    return tuple(sorted(item_snapshots, key=order_key))


def validated_campaign_plan_snapshot(
    campaign_plan: RetainedCampaignPlan,
) -> CampaignPlanSnapshot:
    """Capture one bounded plan once for identity and pre-await persistence."""

    if type(campaign_plan) is not RetainedCampaignPlan:
        raise RetainedArtifactError("campaign_plan_invalid")
    plan_values = CampaignPlanSnapshot(
        campaign_plan.adapter_id,
        campaign_plan.endpoint_id,
        campaign_plan.request_fence_id,
        campaign_plan.credential_descriptor_sha256,
        campaign_plan.source_census_sha256,
        campaign_plan.census_mode,
        campaign_plan.items,
        campaign_plan.per_item_byte_budget,
        campaign_plan.aggregate_byte_budget,
        campaign_plan.expected_stream_identities,
    )
    if (
        type(plan_values.items) is not tuple
        or len(plan_values.items) > MAX_CAMPAIGN_ITEMS
    ):
        raise RetainedArtifactError("campaign_items_invalid")
    if (
        type(plan_values.expected_stream_identities) is not tuple
        or len(plan_values.expected_stream_identities) > MAX_EXPECTED_STREAM_IDENTITIES
    ):
        raise RetainedArtifactError("expected_streams_invalid")
    require_safe_id(plan_values.adapter_id, "adapter_id", maximum=64)
    require_digest(plan_values.endpoint_id, "endpoint_id")
    require_digest(plan_values.request_fence_id, "request_fence_id")
    if plan_values.request_fence_id != endpoint_request_fence_digest(
        plan_values.endpoint_id
    ):
        raise RetainedArtifactError("request_fence_binding_mismatch")
    require_digest(
        plan_values.credential_descriptor_sha256,
        "credential_descriptor_sha256",
    )
    require_digest(plan_values.source_census_sha256, "source_census_sha256")
    require_positive_int(plan_values.per_item_byte_budget, "per_item_byte_budget")
    require_positive_int(plan_values.aggregate_byte_budget, "aggregate_byte_budget")
    expected_streams = _validated_expected_streams(
        plan_values.expected_stream_identities
    )
    item_snapshots = _ordered_item_snapshots(plan_values)
    if plan_values.census_mode == FIXED_CATALOG:
        if not item_snapshots or expected_streams:
            raise RetainedArtifactError("fixed_catalog_plan_invalid")
    elif plan_values.census_mode == ORDERED_STREAMS:
        if not expected_streams or any(
            campaign_item_snapshot.stream_identity_sha256 not in expected_streams
            for campaign_item_snapshot in item_snapshots
        ):
            raise RetainedArtifactError("ordered_stream_plan_invalid")
    else:
        raise RetainedArtifactError("census_mode_invalid")
    return plan_values._replace(items=item_snapshots)


def _snapshot_item_identity(
    campaign_item: CampaignItemSnapshot,
    campaign_ordinal: int,
) -> dict[str, Any]:
    return {
        "source_item_id": campaign_item.source_item_id,
        "campaign_ordinal": campaign_ordinal,
        "source_entry_sha256": campaign_item.source_entry_sha256,
        "artifact_kind": campaign_item.artifact_kind,
        "family": campaign_item.family,
        "collection_kind": campaign_item.collection_kind,
        "partition_metadata_sha256": campaign_item.partition_metadata_sha256,
        "stream_identity_sha256": campaign_item.stream_identity_sha256,
        "sequence_ordinal": campaign_item.sequence_ordinal,
        "item_role": campaign_item.item_role,
        "declared_byte_count": campaign_item.declared_byte_count,
        "terminal_proof_sha256": campaign_item.terminal_proof_sha256,
    }


def campaign_membership_digest_from_snapshot(
    campaign_snapshot: CampaignPlanSnapshot,
) -> str:
    """Commit the immutable membership represented by one validated plan."""

    item_states = [
        _snapshot_item_identity(campaign_item, campaign_ordinal)
        for campaign_ordinal, campaign_item in enumerate(campaign_snapshot.items)
    ]
    stream_states = [
        {
            "stream_identity_sha256": stream_identity,
            "stream_ordinal": stream_ordinal,
        }
        for stream_ordinal, stream_identity in enumerate(
            campaign_snapshot.expected_stream_identities
        )
    ]
    return planned_campaign_membership_digest(
        census_mode=campaign_snapshot.census_mode,
        item_states=item_states,
        stream_states=stream_states,
    )


def _campaign_identity_digest(identity_by_field: Mapping[str, Any]) -> str:
    return sha256_json(
        {
            "contract": require_safe_id(
                identity_by_field.get("contract_id"),
                "contract_id",
            ),
            "adapter_id": require_safe_id(
                identity_by_field.get("adapter_id"),
                "adapter_id",
                maximum=64,
            ),
            "endpoint_id": require_digest(
                identity_by_field.get("endpoint_id"),
                "endpoint_id",
            ),
            "request_fence_id": require_digest(
                identity_by_field.get("request_fence_id"),
                "request_fence_id",
            ),
            "credential_descriptor_sha256": require_digest(
                identity_by_field.get("credential_descriptor_sha256"),
                "credential_descriptor_sha256",
            ),
            "source_census_sha256": require_digest(
                identity_by_field.get("source_census_sha256"),
                "source_census_sha256",
            ),
            "census_mode": identity_by_field.get("census_mode"),
            "fixed_expected_item_count": (
                int(identity_by_field["expected_item_count"])
                if identity_by_field.get("census_mode") == FIXED_CATALOG
                else None
            ),
            "expected_stream_count": int(identity_by_field["expected_stream_count"]),
            "per_item_byte_budget": int(identity_by_field["per_item_byte_budget"]),
            "aggregate_byte_budget": int(identity_by_field["aggregate_byte_budget"]),
            "identity_members_sha256": require_digest(
                identity_by_field.get("identity_members_sha256"),
                "identity_members_sha256",
            ),
            "identity_member_count": int(identity_by_field["identity_member_count"]),
        }
    )


def campaign_id_from_snapshot(campaign_snapshot: CampaignPlanSnapshot) -> str:
    """Return one budget-bound identity from an already validated snapshot."""

    return _campaign_identity_digest(
        {
            "contract_id": RETAINED_ARTIFACT_CONTRACT_ID,
            "adapter_id": campaign_snapshot.adapter_id,
            "endpoint_id": campaign_snapshot.endpoint_id,
            "request_fence_id": campaign_snapshot.request_fence_id,
            "credential_descriptor_sha256": (
                campaign_snapshot.credential_descriptor_sha256
            ),
            "source_census_sha256": campaign_snapshot.source_census_sha256,
            "census_mode": campaign_snapshot.census_mode,
            "expected_item_count": len(campaign_snapshot.items),
            "expected_stream_count": len(campaign_snapshot.expected_stream_identities),
            "per_item_byte_budget": campaign_snapshot.per_item_byte_budget,
            "aggregate_byte_budget": campaign_snapshot.aggregate_byte_budget,
            "identity_members_sha256": campaign_membership_digest_from_snapshot(
                campaign_snapshot
            ),
            "identity_member_count": (
                len(campaign_snapshot.items)
                if campaign_snapshot.census_mode == FIXED_CATALOG
                else len(campaign_snapshot.expected_stream_identities)
            ),
        }
    )


def assert_campaign_record_identity(
    campaign_record_by_field: Mapping[str, Any],
) -> None:
    """Verify a persisted root against its deterministic public identity."""

    try:
        endpoint_id = require_digest(
            campaign_record_by_field.get("endpoint_id"),
            "endpoint_id",
        )
        if campaign_record_by_field.get(
            "request_fence_id"
        ) != endpoint_request_fence_digest(endpoint_id):
            raise RetainedCampaignMismatch("retained_request_fence_mismatch")
        if campaign_record_by_field.get("campaign_id") != _campaign_identity_digest(
            campaign_record_by_field
        ):
            raise RetainedCampaignMismatch("retained_campaign_identity_mismatch")
    except RetainedCampaignMismatch:
        raise
    except (KeyError, TypeError, ValueError, RetainedArtifactError) as error:
        raise RetainedCampaignMismatch("retained_campaign_identity_mismatch") from error


def database_schema() -> str:
    """Return the validated database schema used by retained acquisition."""

    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if not _SCHEMA_PATTERN.fullmatch(schema_name):
        raise RetainedArtifactError("retained_database_schema_invalid")
    return schema_name


def database_table(table_name: str) -> str:
    """Return one schema-qualified internal table name."""

    return f'"{database_schema()}"."{table_name}"'


@dataclass(frozen=True)
class SealTables:
    """Qualified table names used by one campaign seal transaction."""

    campaign: str
    stream: str
    campaign_item: str
    campaign_item_identity: str
    artifact: str
    layout: str
    layout_range: str


def seal_tables() -> SealTables:
    """Resolve campaign seal tables through the validated schema."""

    return SealTables(
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
    )


def database_record(database_value: Any) -> dict[str, Any]:
    """Convert one supported database record into a plain mapping."""

    if database_value is None:
        return {}
    record_mapping = (
        database_value._mapping
        if hasattr(database_value, "_mapping")
        else database_value
    )
    return dict(record_mapping)


def database_record_list(
    database_values: Sequence[Any],
) -> list[dict[str, Any]]:
    """Convert database records into plain mappings."""

    return [database_record(database_value) for database_value in database_values]
