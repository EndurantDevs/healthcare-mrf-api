# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime as dt

import asyncpg
import pytest

from process.provider_directory_retained_artifact_contract import (
    RetainedCampaignMismatch,
)
from process.provider_directory_retained_catalog_store import (
    initialize_retained_artifact_campaign,
)
from process.provider_directory_retained_lease_store import (
    acquire_campaign_lease,
    acquire_item_lease,
)
from process.provider_directory_retained_request_fence_store import (
    reserve_endpoint_request_slot,
)
from process.provider_directory_retained_seal_store import (
    seal_retained_artifact_campaign,
)
from process.provider_directory_retained_store_support import database_table
from tests.provider_directory_retained_core_postgres_support import (
    campaign_item,
    digest,
    fixed_campaign_plan,
    ordered_campaign_plan,
    retained_database,
    retained_peer_connection,
)


async def _fixed_root(connection, label: str):
    retained_item = campaign_item(label)
    campaign_id = await initialize_retained_artifact_campaign(
        connection,
        plan=fixed_campaign_plan(label, (retained_item,)),
    )
    campaign_lease = await acquire_campaign_lease(
        connection,
        campaign_id=campaign_id,
        owner=f"campaign-{label}",
    )
    return retained_item, campaign_id, campaign_lease


async def _wait_for_backend_lock(connection, backend_pid: int) -> None:
    wait_type = None
    for _attempt in range(100):
        wait_type = await connection.fetchval(
            "SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1",
            backend_pid,
        )
        if wait_type == "Lock":
            break
        await asyncio.sleep(0.01)
    assert wait_type == "Lock"


async def _assert_active_item_delete_rejected(
    connection,
    item_table: str,
    campaign_id: str,
    source_item_id: str,
) -> None:
    with pytest.raises(
        asyncpg.CheckViolationError,
        match="retained_item_delete_before_release",
    ):
        async with connection.transaction():
            await connection.execute(
                f"DELETE FROM {item_table} "
                "WHERE campaign_id=$1 AND source_item_id=$2",
                campaign_id,
                source_item_id,
            )


async def _remove_fixed_item_without_guard(
    connection,
    item_table: str,
    campaign_id: str,
    source_item_id: str,
) -> None:
    """Remove one fixed item only to exercise the runtime fallback guard."""

    await connection.execute(
        f"ALTER TABLE {item_table} DISABLE TRIGGER pd_retained_item_identity_immutable"
    )
    try:
        await connection.execute(
            f"DELETE FROM {item_table} " "WHERE campaign_id=$1 AND source_item_id=$2",
            campaign_id,
            source_item_id,
        )
    finally:
        await connection.execute(
            f"ALTER TABLE {item_table} ENABLE TRIGGER pd_retained_item_identity_immutable"
        )


async def _remove_stream_without_guard(
    connection,
    stream_table: str,
    campaign_id: str,
) -> None:
    """Remove one stream root only to exercise the runtime fallback guard."""

    await connection.execute(
        f"ALTER TABLE {stream_table} DISABLE TRIGGER pd_retained_stream_identity_guard"
    )
    try:
        await connection.execute(
            f"DELETE FROM {stream_table} WHERE campaign_id=$1",
            campaign_id,
        )
    finally:
        await connection.execute(
            f"ALTER TABLE {stream_table} ENABLE TRIGGER pd_retained_stream_identity_guard"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_status", ("unaccounted", "failed"))
async def test_retryable_item_states_can_reserve_requests(
    monkeypatch,
    retry_status: str,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, campaign_lease = await _fixed_root(
            connection,
            f"retry-{retry_status}",
        )
        if retry_status == "unaccounted":
            summary = await seal_retained_artifact_campaign(
                connection,
                campaign_id=campaign_id,
                campaign_lease=campaign_lease,
            )
            assert summary["state"] == "sealed_incomplete"
        else:
            await connection.execute(
                f"UPDATE {database_table('provider_directory_retained_artifact_campaign_item')} "
                "SET status='failed' WHERE campaign_id=$1",
                campaign_id,
            )
        item_lease = await acquire_item_lease(
            connection,
            campaign_id=campaign_id,
            source_item_id=retained_item.source_item_id,
            campaign_lease=campaign_lease,
            owner=f"retry-item-{retry_status}",
        )
        slot_at = await reserve_endpoint_request_slot(
            connection,
            campaign_id=campaign_id,
            source_item_id=retained_item.source_item_id,
            campaign_lease=campaign_lease,
            item_lease=item_lease,
            request_interval_ms=0,
        )
        assert isinstance(slot_at, dt.datetime)


@pytest.mark.asyncio
async def test_active_delete_fails_while_seal_waits(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, campaign_lease = await _fixed_root(
            connection,
            "concurrent-delete",
        )
        campaign_table = database_table("provider_directory_retained_artifact_campaign")
        item_table = database_table(
            "provider_directory_retained_artifact_campaign_item"
        )
        async with retained_peer_connection() as peer_connection:
            peer_pid = await peer_connection.fetchval("SELECT pg_backend_pid()")
            async with connection.transaction():
                await connection.execute(
                    f"SELECT 1 FROM {campaign_table} "
                    "WHERE campaign_id=$1 FOR UPDATE",
                    campaign_id,
                )
                seal_task = asyncio.create_task(
                    seal_retained_artifact_campaign(
                        peer_connection,
                        campaign_id=campaign_id,
                        campaign_lease=campaign_lease,
                    )
                )
                await _wait_for_backend_lock(connection, peer_pid)
                await _assert_active_item_delete_rejected(
                    connection,
                    item_table,
                    campaign_id,
                    retained_item.source_item_id,
                )
            summary = await asyncio.wait_for(seal_task, timeout=5)
            assert summary["state"] == "sealed_incomplete"


@pytest.mark.asyncio
async def test_seal_rejects_changed_item_and_stream_census(monkeypatch) -> None:
    """Seal fails closed when either fixed items or stream roots are removed."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, campaign_lease = await _fixed_root(
            connection, "changed-item-census"
        )
        item_table = database_table(
            "provider_directory_retained_artifact_campaign_item"
        )
        await _remove_fixed_item_without_guard(
            connection,
            item_table,
            campaign_id,
            retained_item.source_item_id,
        )
        with pytest.raises(RetainedCampaignMismatch, match="item_count_changed"):
            await seal_retained_artifact_campaign(
                connection, campaign_id=campaign_id, campaign_lease=campaign_lease
            )
        stream_identity = digest("missing-stream")
        stream_plan = ordered_campaign_plan("missing-stream", stream_identity)
        stream_id = await initialize_retained_artifact_campaign(
            connection, plan=stream_plan
        )
        stream_lease = await acquire_campaign_lease(
            connection, campaign_id=stream_id, owner="missing-stream-owner"
        )
        stream_table = database_table(
            "provider_directory_retained_artifact_campaign_stream"
        )
        await _remove_stream_without_guard(connection, stream_table, stream_id)
        with pytest.raises(
            RetainedCampaignMismatch,
            match="retained_campaign_membership_mismatch",
        ):
            await seal_retained_artifact_campaign(
                connection, campaign_id=stream_id, campaign_lease=stream_lease
            )


@pytest.mark.asyncio
async def test_released_campaign_allows_child_first_identity_cleanup(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("released-cleanup")
        campaign_id = await initialize_retained_artifact_campaign(
            connection,
            plan=fixed_campaign_plan("released-cleanup", (retained_item,)),
        )
        campaign_table = database_table("provider_directory_retained_artifact_campaign")
        item_table = database_table(
            "provider_directory_retained_artifact_campaign_item"
        )
        identity_table = database_table(
            "provider_directory_retained_artifact_campaign_item_identity"
        )
        await connection.execute(
            f"UPDATE {campaign_table} SET state='released', released_at=now() "
            "WHERE campaign_id=$1",
            campaign_id,
        )
        await connection.execute(
            f"DELETE FROM {item_table} WHERE campaign_id=$1",
            campaign_id,
        )
        await connection.execute(
            f"DELETE FROM {identity_table} WHERE campaign_id=$1",
            campaign_id,
        )
        await connection.execute(
            f"DELETE FROM {campaign_table} WHERE campaign_id=$1",
            campaign_id,
        )
        assert (
            await connection.fetchval(
                f"SELECT count(*) FROM {campaign_table} WHERE campaign_id=$1",
                campaign_id,
            )
            == 0
        )
