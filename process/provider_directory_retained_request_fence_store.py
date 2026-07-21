# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared upstream request fences across independent retained campaigns."""

from __future__ import annotations

import datetime as dt

import asyncpg

from process.provider_directory_retained_artifact_contract import (
    LeaseIdentity,
    RetainedArtifactError,
    RetainedCampaignMismatch,
    endpoint_request_fence_digest,
    require_nonnegative_int,
)
from process.provider_directory_retained_lease_store import (
    _require_campaign_lease,
    _require_item_lease,
)
from process.provider_directory_retained_store_support import (
    database_record,
    database_table,
)


MAX_RETRY_AFTER_SECONDS = 24 * 60 * 60
_COMPLETED_ITEM_STATUSES = frozenset({"admitted", "terminal_zero"})


def _require_aware_timestamp(
    timestamp: dt.datetime | None,
    *,
    allow_none: bool,
) -> None:
    if timestamp is None and allow_none:
        return
    if (
        not isinstance(timestamp, dt.datetime)
        or timestamp.tzinfo is None
        or timestamp.utcoffset() is None
    ):
        raise RetainedArtifactError("retry_not_before_invalid")


def _require_request_interval(request_interval_ms: int) -> None:
    require_nonnegative_int(request_interval_ms, "request_interval_ms")
    if request_interval_ms > 24 * 60 * 60 * 1000:
        raise RetainedArtifactError("request_interval_invalid")


def _require_bounded_retry_not_before(
    retry_not_before: dt.datetime | None,
    current_time: dt.datetime,
) -> None:
    if retry_not_before is not None and retry_not_before > current_time + dt.timedelta(
        seconds=MAX_RETRY_AFTER_SECONDS
    ):
        raise RetainedArtifactError("retry_not_before_too_far")


def _verified_request_fence_id(campaign_record_by_field: dict) -> str:
    expected_fence_id = endpoint_request_fence_digest(
        campaign_record_by_field.get("endpoint_id")
    )
    if campaign_record_by_field.get("request_fence_id") != expected_fence_id:
        raise RetainedCampaignMismatch("retained_request_fence_mismatch")
    return expected_fence_id


async def reserve_endpoint_request_slot(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    source_item_id: str,
    campaign_lease: LeaseIdentity,
    item_lease: LeaseIdentity,
    request_interval_ms: int,
    retry_not_before: dt.datetime | None = None,
) -> dt.datetime:
    """Atomically serialize one request slot across campaigns for an endpoint."""

    _require_request_interval(request_interval_ms)
    _require_aware_timestamp(retry_not_before, allow_none=True)
    fence_table = database_table("provider_directory_retained_artifact_endpoint_fence")
    async with connection.transaction():
        campaign_record_by_field = await _require_campaign_lease(
            connection,
            campaign_id,
            campaign_lease,
        )
        item_record_by_field = await _require_item_lease(
            connection,
            campaign_id,
            source_item_id,
            item_lease,
        )
        if item_record_by_field.get("status") in _COMPLETED_ITEM_STATUSES:
            raise RetainedArtifactError("retained_item_already_complete")
        request_fence_id = _verified_request_fence_id(campaign_record_by_field)
        await connection.execute(
            f"""INSERT INTO {fence_table} AS fence (
                    request_fence_id, next_request_at, fence_epoch,
                    created_at, updated_at
                ) VALUES ($1, clock_timestamp(), 0, now(), now())
                ON CONFLICT (request_fence_id) DO NOTHING;""",
            request_fence_id,
        )
        fence_record_by_field = database_record(
            await connection.fetchrow(
                f"""SELECT * FROM {fence_table}
                      WHERE request_fence_id=$1 FOR UPDATE;""",
                request_fence_id,
            )
        )
        current_time = await connection.fetchval("SELECT clock_timestamp();")
        _require_bounded_retry_not_before(retry_not_before, current_time)
        slot_candidates = [current_time, fence_record_by_field["next_request_at"]]
        if retry_not_before is not None:
            slot_candidates.append(retry_not_before)
        slot_at = max(slot_candidates)
        await connection.execute(
            f"""UPDATE {fence_table}
                   SET next_request_at=$2, fence_epoch=fence_epoch+1,
                       updated_at=now()
                 WHERE request_fence_id=$1;""",
            request_fence_id,
            slot_at + dt.timedelta(milliseconds=request_interval_ms),
        )
    return slot_at


async def defer_endpoint_request_fence(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    source_item_id: str,
    campaign_lease: LeaseIdentity,
    item_lease: LeaseIdentity,
    retry_not_before: dt.datetime,
) -> None:
    """Move an endpoint fence forward without consuming another request slot."""

    _require_aware_timestamp(retry_not_before, allow_none=False)
    fence_table = database_table("provider_directory_retained_artifact_endpoint_fence")
    async with connection.transaction():
        campaign = await _require_campaign_lease(
            connection,
            campaign_id,
            campaign_lease,
        )
        item_record_by_field = await _require_item_lease(
            connection,
            campaign_id,
            source_item_id,
            item_lease,
        )
        if item_record_by_field.get("status") in _COMPLETED_ITEM_STATUSES:
            raise RetainedArtifactError("retained_item_already_complete")
        request_fence_id = _verified_request_fence_id(campaign)
        current_time = await connection.fetchval("SELECT clock_timestamp();")
        _require_bounded_retry_not_before(retry_not_before, current_time)
        await connection.execute(
            f"""INSERT INTO {fence_table} AS fence (
                    request_fence_id, next_request_at, fence_epoch,
                    created_at, updated_at
                ) VALUES ($1, $2, 1, now(), now())
                ON CONFLICT (request_fence_id) DO UPDATE
                    SET next_request_at=GREATEST(
                            fence.next_request_at,
                            EXCLUDED.next_request_at
                        ),
                        fence_epoch=fence.fence_epoch+1,
                        updated_at=now();""",
            request_fence_id,
            retry_not_before,
        )
