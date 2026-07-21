# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Contiguous ordered Bundle and NDJSON stream growth."""

from __future__ import annotations

from typing import Any

import asyncpg

from process.provider_directory_retained_artifact_contract import (
    ORDERED_STREAMS,
    LeaseIdentity,
    RetainedArtifactError,
    RetainedCampaignItem,
    RetainedCampaignMismatch,
    require_digest,
)
from process.provider_directory_retained_artifact_keys import (
    LOCATOR_PRIVATE_VALUE,
    active_private_key_id,
    private_item_binding,
    private_identity_hmac,
    seal_private_value,
)
from process.provider_directory_retained_catalog_store import (
    _PreparedCampaignItem,
    _insert_campaign_item,
)
from process.provider_directory_retained_lease_store import _require_campaign_lease
from process.provider_directory_retained_store_support import (
    CampaignItemSnapshot,
    MUTABLE_CAMPAIGN_STATES,
    database_record,
    database_record_list,
    database_table,
    open_snapshot_source_locator,
    validated_campaign_item_snapshot,
)


def _prepare_stream_item(
    item_snapshot: CampaignItemSnapshot,
    *,
    campaign_id: str,
    endpoint_id: str,
) -> _PreparedCampaignItem:
    has_locator = item_snapshot.source_locator is not None
    source_locator = (
        open_snapshot_source_locator(item_snapshot) if has_locator else None
    )
    binding_sha256 = private_item_binding(
        campaign_id=campaign_id,
        source_item_id=item_snapshot.source_item_id,
        endpoint_id=endpoint_id,
    )
    return _PreparedCampaignItem(
        campaign_item=item_snapshot,
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


async def _require_appendable_stream(
    connection: asyncpg.Connection,
    campaign_id: str,
    campaign_lease: LeaseIdentity,
    stream_identity_sha256: str,
) -> dict[str, Any]:
    campaign_record_by_field = await _require_campaign_lease(
        connection,
        campaign_id,
        campaign_lease,
    )
    if (
        campaign_record_by_field["census_mode"] != ORDERED_STREAMS
        or campaign_record_by_field["state"] not in MUTABLE_CAMPAIGN_STATES
    ):
        raise RetainedArtifactError("ordered_stream_not_mutable")
    stream_record_by_field = database_record(
        await connection.fetchrow(
            f"""SELECT *
                   FROM {database_table('provider_directory_retained_artifact_campaign_stream')}
                  WHERE campaign_id=$1 AND stream_identity_sha256=$2
                  FOR UPDATE;""",
            campaign_id,
            stream_identity_sha256,
        )
    )
    if not stream_record_by_field or stream_record_by_field["complete"]:
        raise RetainedArtifactError("ordered_stream_not_appendable")
    return campaign_record_by_field


async def _next_stream_ordinals(
    connection: asyncpg.Connection,
    campaign_id: str,
    stream_identity_sha256: str,
) -> tuple[int, int]:
    item_table = database_table("provider_directory_retained_artifact_campaign_item")
    next_sequence = await connection.fetchval(
        f"""SELECT COALESCE(max(sequence_ordinal) + 1, 0)
              FROM {item_table}
             WHERE campaign_id=$1 AND stream_identity_sha256=$2;""",
        campaign_id,
        stream_identity_sha256,
    )
    next_campaign_ordinal = await connection.fetchval(
        f"""SELECT COALESCE(max(campaign_ordinal) + 1, 0)
              FROM {item_table} WHERE campaign_id=$1;""",
        campaign_id,
    )
    return int(next_sequence), int(next_campaign_ordinal)


async def _assert_inserted_stream_item(
    connection: asyncpg.Connection,
    campaign_id: str,
    campaign_item: CampaignItemSnapshot,
) -> None:
    inserted_by_field = database_record(
        await connection.fetchrow(
            f"""SELECT source_entry_sha256, stream_identity_sha256,
                        sequence_ordinal, item_role, terminal_proof_sha256
                   FROM {database_table('provider_directory_retained_artifact_campaign_item')}
                  WHERE campaign_id=$1 AND source_item_id=$2;""",
            campaign_id,
            campaign_item.source_item_id,
        )
    )
    expected_by_field = {
        "source_entry_sha256": campaign_item.source_entry_sha256,
        "stream_identity_sha256": campaign_item.stream_identity_sha256,
        "sequence_ordinal": campaign_item.sequence_ordinal,
        "item_role": campaign_item.item_role,
        "terminal_proof_sha256": campaign_item.terminal_proof_sha256,
    }
    if inserted_by_field != expected_by_field:
        raise RetainedCampaignMismatch("ordered_stream_item_mismatch")


async def _invalidate_campaign_seal(
    connection: asyncpg.Connection,
    campaign_id: str,
) -> None:
    item_table = database_table("provider_directory_retained_artifact_campaign_item")
    await connection.execute(
        f"""UPDATE {database_table('provider_directory_retained_artifact_campaign')}
               SET expected_item_count=(
                       SELECT count(*) FROM {item_table} WHERE campaign_id=$1
                   ),
                   state=CASE WHEN state='sealed_incomplete'
                              THEN 'preflighting' ELSE state END,
                   item_census_sha256=NULL,
                   observation_set_sha256=NULL,
                   campaign_sha256=NULL, complete=FALSE, sealed_at=NULL,
                   updated_at=now()
             WHERE campaign_id=$1;""",
        campaign_id,
    )


async def append_ordered_stream_item(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    campaign_lease: LeaseIdentity,
    item: RetainedCampaignItem,
) -> int:
    """Append one contiguous Bundle/NDJSON payload or terminal-zero proof."""

    item_snapshot = validated_campaign_item_snapshot(item)
    return await _append_prepared_stream_item(
        connection,
        campaign_id,
        campaign_lease,
        item_snapshot,
    )


async def _append_prepared_stream_item(
    connection: asyncpg.Connection,
    campaign_id: str,
    campaign_lease: LeaseIdentity,
    campaign_item: CampaignItemSnapshot,
) -> int:
    async with connection.transaction():
        campaign_record_by_field = await _require_appendable_stream(
            connection,
            campaign_id,
            campaign_lease,
            campaign_item.stream_identity_sha256,
        )
        prepared_item = _prepare_stream_item(
            campaign_item,
            campaign_id=campaign_id,
            endpoint_id=str(campaign_record_by_field["endpoint_id"]),
        )
        next_sequence, next_campaign_ordinal = await _next_stream_ordinals(
            connection,
            campaign_id,
            campaign_item.stream_identity_sha256,
        )
        if campaign_item.sequence_ordinal != next_sequence:
            raise RetainedCampaignMismatch("ordered_stream_sequence_gap")
        await _insert_campaign_item(
            connection,
            campaign_id,
            next_campaign_ordinal,
            prepared_item,
        )
        await _assert_inserted_stream_item(connection, campaign_id, campaign_item)
        await _invalidate_campaign_seal(connection, campaign_id)
    return next_campaign_ordinal


async def list_campaign_items(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
) -> list[dict[str, Any]]:
    """Read worker-private item state; never return this directly from an API."""

    require_digest(campaign_id, "campaign_id")
    return database_record_list(
        await connection.fetch(
            f"""SELECT *
                   FROM {database_table('provider_directory_retained_artifact_campaign_item')}
                  WHERE campaign_id=$1 ORDER BY campaign_ordinal;""",
            campaign_id,
        )
    )
