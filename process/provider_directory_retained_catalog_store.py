# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed and ordered campaign census initialization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import asyncpg

from process.provider_directory_retained_artifact_contract import (
    FIXED_CATALOG,
    RETAINED_ARTIFACT_CONTRACT_ID,
    TERMINAL_ZERO,
    RetainedCampaignMismatch,
    RetainedCampaignPlan,
)
from process.provider_directory_retained_artifact_keys import (
    LOCATOR_PRIVATE_VALUE,
    active_private_key_id,
    private_item_binding,
    private_identity_hmac,
    seal_private_value,
)
from process.provider_directory_retained_store_support import (
    CampaignItemSnapshot,
    CampaignPlanSnapshot,
    assert_campaign_record_identity,
    campaign_id_from_snapshot,
    campaign_membership_digest_from_snapshot,
    database_record,
    database_record_list,
    database_table,
    open_snapshot_source_locator,
    validated_campaign_plan_snapshot,
)
from process.provider_directory_retained_identity_contract import (
    assert_planned_item_commitments,
    planned_item_identity_digest,
)


@dataclass(frozen=True)
class _PreparedCampaignItem:
    campaign_item: CampaignItemSnapshot
    locator_ciphertext: str | None
    locator_identity_hmac: str | None
    locator_key_id: str | None


def _prepare_campaign_items(
    campaign_plan: CampaignPlanSnapshot,
    campaign_id: str,
) -> tuple[_PreparedCampaignItem, ...]:
    prepared_items: list[_PreparedCampaignItem] = []
    for campaign_item in campaign_plan.items:
        has_locator = campaign_item.source_locator is not None
        source_locator = (
            open_snapshot_source_locator(campaign_item) if has_locator else None
        )
        binding_sha256 = private_item_binding(
            campaign_id=campaign_id,
            source_item_id=campaign_item.source_item_id,
            endpoint_id=campaign_plan.endpoint_id,
        )
        prepared_items.append(
            _PreparedCampaignItem(
                campaign_item=campaign_item,
                locator_ciphertext=(
                    seal_private_value(
                        source_locator,
                        purpose=LOCATOR_PRIVATE_VALUE,
                        binding_sha256=binding_sha256,
                    )
                    if has_locator
                    else None
                ),
                locator_identity_hmac=(
                    private_identity_hmac(
                        source_locator,
                        purpose=LOCATOR_PRIVATE_VALUE,
                        binding_sha256=binding_sha256,
                    )
                    if has_locator
                    else None
                ),
                locator_key_id=active_private_key_id() if has_locator else None,
            )
        )
    return tuple(prepared_items)


async def _insert_campaign_root(
    connection: asyncpg.Connection,
    campaign_plan: CampaignPlanSnapshot,
    campaign_id: str,
    prepared_items: tuple[_PreparedCampaignItem, ...],
) -> None:
    campaign_table = database_table("provider_directory_retained_artifact_campaign")
    await connection.execute(
        f"""INSERT INTO {campaign_table} (
                campaign_id, contract_id, adapter_id, endpoint_id,
                request_fence_id, credential_descriptor_sha256,
                source_census_sha256, identity_members_sha256,
                identity_member_count, census_mode, state,
                expected_item_count, expected_stream_count,
                per_item_byte_budget, aggregate_byte_budget,
                created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'planned', $11,
                $12, $13, $14, now(), now()
            ) ON CONFLICT (campaign_id) DO NOTHING;""",
        campaign_id,
        RETAINED_ARTIFACT_CONTRACT_ID,
        campaign_plan.adapter_id,
        campaign_plan.endpoint_id,
        campaign_plan.request_fence_id,
        campaign_plan.credential_descriptor_sha256,
        campaign_plan.source_census_sha256,
        campaign_membership_digest_from_snapshot(campaign_plan),
        (
            len(prepared_items)
            if campaign_plan.census_mode == FIXED_CATALOG
            else len(campaign_plan.expected_stream_identities)
        ),
        campaign_plan.census_mode,
        len(prepared_items),
        len(campaign_plan.expected_stream_identities),
        campaign_plan.per_item_byte_budget,
        campaign_plan.aggregate_byte_budget,
    )


def _expected_campaign_identity(
    campaign_plan: CampaignPlanSnapshot,
) -> dict[str, Any]:
    return {
        "contract_id": RETAINED_ARTIFACT_CONTRACT_ID,
        "adapter_id": campaign_plan.adapter_id,
        "endpoint_id": campaign_plan.endpoint_id,
        "request_fence_id": campaign_plan.request_fence_id,
        "credential_descriptor_sha256": campaign_plan.credential_descriptor_sha256,
        "source_census_sha256": campaign_plan.source_census_sha256,
        "identity_members_sha256": campaign_membership_digest_from_snapshot(
            campaign_plan
        ),
        "identity_member_count": (
            len(campaign_plan.items)
            if campaign_plan.census_mode == FIXED_CATALOG
            else len(campaign_plan.expected_stream_identities)
        ),
        "census_mode": campaign_plan.census_mode,
        "expected_stream_count": len(campaign_plan.expected_stream_identities),
        "per_item_byte_budget": campaign_plan.per_item_byte_budget,
        "aggregate_byte_budget": campaign_plan.aggregate_byte_budget,
    }


def _assert_campaign_identity(
    campaign_plan: CampaignPlanSnapshot,
    campaign_record_by_field: dict[str, Any],
    prepared_item_count: int,
) -> None:
    if campaign_plan.census_mode == FIXED_CATALOG and (
        campaign_record_by_field.get("expected_item_count") != prepared_item_count
    ):
        raise RetainedCampaignMismatch("fixed_catalog_count_mismatch")
    expected_identity_by_field = _expected_campaign_identity(campaign_plan)
    if any(
        campaign_record_by_field.get(field_name) != expected_field_value
        for field_name, expected_field_value in expected_identity_by_field.items()
    ):
        raise RetainedCampaignMismatch("retained_campaign_identity_mismatch")
    assert_campaign_record_identity(campaign_record_by_field)


async def _insert_expected_streams(
    connection: asyncpg.Connection,
    campaign_id: str,
    expected_stream_ids: tuple[str, ...],
) -> None:
    stream_table = database_table(
        "provider_directory_retained_artifact_campaign_stream"
    )
    for stream_ordinal, stream_identity in enumerate(expected_stream_ids):
        await connection.execute(
            f"""INSERT INTO {stream_table} (
                    campaign_id, stream_identity_sha256, stream_ordinal,
                    created_at
                ) VALUES ($1, $2, $3, now())
                ON CONFLICT (campaign_id, stream_identity_sha256) DO NOTHING;""",
            campaign_id,
            stream_identity,
            stream_ordinal,
        )


async def _assert_expected_streams(
    connection: asyncpg.Connection,
    campaign_id: str,
    expected_stream_ids: tuple[str, ...],
) -> None:
    persisted_streams = database_record_list(
        await connection.fetch(
            f"""SELECT stream_identity_sha256, stream_ordinal
                   FROM {database_table('provider_directory_retained_artifact_campaign_stream')}
                  WHERE campaign_id=$1 ORDER BY stream_ordinal;""",
            campaign_id,
        )
    )
    expected_streams = [
        {
            "stream_identity_sha256": stream_identity,
            "stream_ordinal": stream_ordinal,
        }
        for stream_ordinal, stream_identity in enumerate(expected_stream_ids)
    ]
    if persisted_streams != expected_streams:
        raise RetainedCampaignMismatch("expected_stream_identity_mismatch")


async def _insert_campaign_item(
    connection: asyncpg.Connection,
    campaign_id: str,
    campaign_ordinal: int,
    prepared_item: _PreparedCampaignItem,
) -> None:
    campaign_item = prepared_item.campaign_item
    immutable_item_by_field = _immutable_item_fields(
        prepared_item,
        campaign_ordinal,
        campaign_id,
    )
    await connection.execute(
        f"""INSERT INTO {database_table('provider_directory_retained_artifact_campaign_item_identity')} (
                campaign_id, source_item_id, campaign_ordinal,
                planned_identity_sha256, created_at
            ) VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (campaign_id, source_item_id) DO NOTHING;""",
        campaign_id,
        campaign_item.source_item_id,
        campaign_ordinal,
        immutable_item_by_field["planned_identity_sha256"],
    )
    await connection.execute(
        f"""INSERT INTO {database_table('provider_directory_retained_artifact_campaign_item')} (
                campaign_id, source_item_id, campaign_ordinal,
                source_entry_sha256, artifact_kind, family, collection_kind,
                partition_metadata_sha256, stream_identity_sha256,
                sequence_ordinal, item_role, locator_ciphertext,
                locator_identity_hmac, locator_key_id, declared_byte_count, status,
                terminal_proof_sha256, planned_identity_sha256,
                created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                $12, $13, $14, $15, $16, $17, $18, now(), now()
            ) ON CONFLICT (campaign_id, source_item_id) DO NOTHING;""",
        campaign_id,
        campaign_item.source_item_id,
        campaign_ordinal,
        campaign_item.source_entry_sha256,
        campaign_item.artifact_kind,
        campaign_item.family,
        campaign_item.collection_kind,
        campaign_item.partition_metadata_sha256,
        campaign_item.stream_identity_sha256,
        campaign_item.sequence_ordinal,
        campaign_item.item_role,
        prepared_item.locator_ciphertext,
        prepared_item.locator_identity_hmac,
        prepared_item.locator_key_id,
        campaign_item.declared_byte_count,
        "terminal_zero" if campaign_item.item_role == TERMINAL_ZERO else "expected",
        campaign_item.terminal_proof_sha256,
        immutable_item_by_field["planned_identity_sha256"],
    )


def _immutable_item_fields(
    prepared_item: _PreparedCampaignItem,
    campaign_ordinal: int,
    campaign_id: str,
) -> dict[str, Any]:
    campaign_item = prepared_item.campaign_item
    immutable_item_by_field = {
        "campaign_id": campaign_id,
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
        "locator_identity_hmac": prepared_item.locator_identity_hmac,
        "locator_key_id": prepared_item.locator_key_id,
        "declared_byte_count": campaign_item.declared_byte_count,
        "terminal_proof_sha256": campaign_item.terminal_proof_sha256,
    }
    return {
        **immutable_item_by_field,
        "planned_identity_sha256": planned_item_identity_digest(
            immutable_item_by_field
        ),
    }


async def _assert_item_census(
    connection: asyncpg.Connection,
    campaign_plan: CampaignPlanSnapshot,
    campaign_id: str,
    prepared_items: tuple[_PreparedCampaignItem, ...],
) -> None:
    persisted_items = database_record_list(
        await connection.fetch(
            f"""SELECT campaign_id, source_item_id, campaign_ordinal,
                        source_entry_sha256, artifact_kind, family,
                        collection_kind, partition_metadata_sha256,
                        stream_identity_sha256, sequence_ordinal, item_role,
                        locator_identity_hmac, locator_key_id, declared_byte_count,
                        terminal_proof_sha256, planned_identity_sha256
                   FROM {database_table('provider_directory_retained_artifact_campaign_item')}
                  WHERE campaign_id=$1 ORDER BY campaign_ordinal;""",
            campaign_id,
        )
    )
    expected_items = [
        _immutable_item_fields(prepared_item, campaign_ordinal, campaign_id)
        for campaign_ordinal, prepared_item in enumerate(prepared_items)
    ]
    commitment_states = database_record_list(
        await connection.fetch(
            f"""SELECT campaign_id, source_item_id, campaign_ordinal,
                        planned_identity_sha256
                   FROM {database_table('provider_directory_retained_artifact_campaign_item_identity')}
                  WHERE campaign_id=$1 ORDER BY campaign_ordinal;""",
            campaign_id,
        )
    )
    assert_planned_item_commitments(persisted_items, commitment_states)
    if campaign_plan.census_mode == FIXED_CATALOG:
        if persisted_items != expected_items:
            raise RetainedCampaignMismatch("fixed_catalog_census_mismatch")
        return
    persisted_by_id = {
        persisted_item["source_item_id"]: persisted_item
        for persisted_item in persisted_items
    }
    if any(
        persisted_by_id.get(expected_item["source_item_id"]) != expected_item
        for expected_item in expected_items
    ):
        raise RetainedCampaignMismatch("ordered_stream_item_mismatch")


async def initialize_retained_artifact_campaign(
    connection: asyncpg.Connection,
    *,
    plan: RetainedCampaignPlan,
) -> str:
    """Create or verify one exact fixed catalog or ordered-stream root."""

    campaign_plan = validated_campaign_plan_snapshot(plan)
    campaign_id = campaign_id_from_snapshot(campaign_plan)
    prepared_items = _prepare_campaign_items(campaign_plan, campaign_id)
    async with connection.transaction():
        await _insert_campaign_root(
            connection,
            campaign_plan,
            campaign_id,
            prepared_items,
        )
        campaign_record_by_field = database_record(
            await connection.fetchrow(
                f"""SELECT *
                       FROM {database_table('provider_directory_retained_artifact_campaign')}
                      WHERE campaign_id=$1 FOR UPDATE;""",
                campaign_id,
            )
        )
        _assert_campaign_identity(
            campaign_plan,
            campaign_record_by_field,
            len(prepared_items),
        )
        await _insert_expected_streams(
            connection,
            campaign_id,
            campaign_plan.expected_stream_identities,
        )
        await _assert_expected_streams(
            connection,
            campaign_id,
            campaign_plan.expected_stream_identities,
        )
        for campaign_ordinal, prepared_item in enumerate(prepared_items):
            await _insert_campaign_item(
                connection,
                campaign_id,
                campaign_ordinal,
                prepared_item,
            )
        await _assert_item_census(
            connection,
            campaign_plan,
            campaign_id,
            prepared_items,
        )
    return campaign_id
