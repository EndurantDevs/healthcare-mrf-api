# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncpg
import pytest

from process.provider_directory_retained_artifact_contract import (
    TERMINAL_ZERO,
    RetainedCampaignIncomplete,
    RetainedCampaignMismatch,
)
from process.provider_directory_retained_catalog_store import (
    initialize_retained_artifact_campaign,
)
from process.provider_directory_retained_consumer_claim_store import (
    claim_sealed_retained_campaign,
)
from process.provider_directory_retained_lease_store import (
    acquire_campaign_lease,
    acquire_item_lease,
)
from process.provider_directory_retained_request_fence_store import (
    _verified_request_fence_id,
    reserve_endpoint_request_slot,
)
from process.provider_directory_retained_seal_store import (
    _seal_ordered_streams,
    seal_retained_artifact_campaign,
)
from process.provider_directory_retained_identity_contract import (
    planned_campaign_membership_digest,
    planned_item_identity_digest,
)
from process.provider_directory_retained_store_support import (
    assert_campaign_record_identity,
    database_table as table,
)
from tests.provider_directory_retained_core_postgres_support import (
    admit_campaign_item,
    campaign_item,
    digest,
    fixed_campaign_plan,
    ordered_campaign_plan,
    registry_artifact,
    retained_database,
)


async def _fixed_root(connection, label: str):
    retained_item = campaign_item(label)
    plan = fixed_campaign_plan(label, (retained_item,))
    campaign_id = await initialize_retained_artifact_campaign(connection, plan=plan)
    lease = await acquire_campaign_lease(
        connection, campaign_id=campaign_id, owner=f"campaign-{label}"
    )
    return retained_item, campaign_id, lease


async def _sealed_fixed_root(connection, label: str):
    retained_item, campaign_id, lease = await _fixed_root(connection, label)
    artifact = registry_artifact(label, retained_item.artifact_kind)
    await admit_campaign_item(connection, campaign_id, retained_item, artifact)
    summary = await seal_retained_artifact_campaign(
        connection,
        campaign_id=campaign_id,
        campaign_lease=lease,
    )
    assert summary["complete"] is True
    return retained_item, campaign_id


async def _disable_root_and_mutate(connection, campaign_id: str, update_sql: str):
    campaign_table = table("provider_directory_retained_artifact_campaign")
    await connection.execute(
        f"ALTER TABLE {campaign_table} "
        "DISABLE TRIGGER pd_retained_campaign_identity_guard"
    )
    try:
        await connection.execute(update_sql, campaign_id)
    finally:
        await connection.execute(
            f"ALTER TABLE {campaign_table} "
            "ENABLE TRIGGER pd_retained_campaign_identity_guard"
        )


async def _assert_runtime_root_fence_boundaries(
    connection,
    retained_item,
    campaign_id: str,
    campaign_lease,
) -> None:
    campaign_table = table("provider_directory_retained_artifact_campaign")
    item_lease = await acquire_item_lease(
        connection,
        campaign_id=campaign_id,
        source_item_id=retained_item.source_item_id,
        campaign_lease=campaign_lease,
        owner="item-root-boundaries",
    )
    original_epoch = await connection.fetchval(
        f"SELECT lease_epoch FROM {campaign_table} WHERE campaign_id=$1",
        campaign_id,
    )
    await _disable_root_and_mutate(
        connection,
        campaign_id,
        f"UPDATE {campaign_table} SET request_fence_id="
        f"'{digest('runtime-fence-change')}' WHERE campaign_id=$1",
    )
    with pytest.raises(RetainedCampaignMismatch, match="request_fence"):
        await acquire_campaign_lease(
            connection,
            campaign_id=campaign_id,
            owner=campaign_lease.owner,
        )
    current_epoch = await connection.fetchval(
        f"SELECT lease_epoch FROM {campaign_table} WHERE campaign_id=$1",
        campaign_id,
    )
    assert current_epoch == original_epoch
    with pytest.raises(RetainedCampaignMismatch, match="request_fence"):
        await reserve_endpoint_request_slot(
            connection,
            campaign_id=campaign_id,
            source_item_id=retained_item.source_item_id,
            campaign_lease=campaign_lease,
            item_lease=item_lease,
            request_interval_ms=0,
        )
    fence_count = await connection.fetchval(
        f"SELECT count(*) FROM "
        f"{table('provider_directory_retained_artifact_endpoint_fence')}"
    )
    assert fence_count == 0
    with pytest.raises(RetainedCampaignMismatch, match="request_fence"):
        await seal_retained_artifact_campaign(
            connection,
            campaign_id=campaign_id,
            campaign_lease=campaign_lease,
        )


async def _replace_item_through_ledger_guard(
    connection, item_table: str, campaign_id: str, source_item_id: str
) -> dict:
    original_item_by_field = dict(
        await connection.fetchrow(
            f"SELECT * FROM {item_table} " "WHERE campaign_id=$1 AND source_item_id=$2",
            campaign_id,
            source_item_id,
        )
    )
    await connection.execute(
        f"CREATE TEMP TABLE retained_saved_item AS SELECT * FROM {item_table} "
        "WHERE campaign_id=$1 AND source_item_id=$2",
        campaign_id,
        source_item_id,
    )
    await connection.execute(
        f"ALTER TABLE {item_table} "
        "DISABLE TRIGGER pd_retained_item_identity_immutable"
    )
    try:
        await connection.execute(
            f"DELETE FROM {item_table} " "WHERE campaign_id=$1 AND source_item_id=$2",
            campaign_id,
            source_item_id,
        )
        changed_item_by_field = dict(original_item_by_field)
        changed_item_by_field["family"] = "Organization"
        changed_digest = planned_item_identity_digest(changed_item_by_field)
        await connection.execute(
            "UPDATE retained_saved_item SET family='Organization', "
            "planned_identity_sha256=$1",
            changed_digest,
        )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await connection.execute(
                f"INSERT INTO {item_table} SELECT * FROM retained_saved_item"
            )
        await connection.execute(
            "UPDATE retained_saved_item SET family=$1, " "planned_identity_sha256=$2",
            original_item_by_field["family"],
            original_item_by_field["planned_identity_sha256"],
        )
        await connection.execute(
            f"INSERT INTO {item_table} SELECT * FROM retained_saved_item"
        )
    finally:
        await connection.execute(
            f"ALTER TABLE {item_table} "
            "ENABLE TRIGGER pd_retained_item_identity_immutable"
        )
    return original_item_by_field


def test_identity_helpers_reject_invalid_boundary_shapes() -> None:
    endpoint_id = digest("invalid-fence-boundary")
    with pytest.raises(RetainedCampaignMismatch, match="request_fence"):
        _verified_request_fence_id(
            {
                "endpoint_id": endpoint_id,
                "request_fence_id": digest("wrong-fence-boundary"),
            }
        )
    with pytest.raises(RetainedCampaignMismatch, match="census_mode"):
        planned_campaign_membership_digest(
            census_mode="invalid",
            item_states=(),
            stream_states=(),
        )
    with pytest.raises(RetainedCampaignMismatch, match="campaign_identity"):
        assert_campaign_record_identity({"endpoint_id": "invalid"})


@pytest.mark.asyncio
async def test_missing_claim_and_stream_count_guards(monkeypatch) -> None:
    with pytest.raises(RetainedCampaignMismatch, match="stream_count_changed"):
        await _seal_ordered_streams(
            None,
            None,
            digest("missing-stream-count"),
            1,
            (),
            (),
        )
    async with retained_database(monkeypatch) as (connection, _schema_name):
        with pytest.raises(RetainedCampaignIncomplete, match="not_complete"):
            await claim_sealed_retained_campaign(
                connection,
                campaign_id=digest("missing-claim-campaign"),
                consumer_recipe_id="missing_campaign_profile_v1",
            )


@pytest.mark.asyncio
async def test_campaign_root_guard_and_runtime_boundaries_fail_closed(
    monkeypatch,
) -> None:
    """Root identity mutations must fail at storage and every runtime boundary."""
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, campaign_lease = await _fixed_root(
            connection,
            "root-boundaries",
        )
        campaign_table = table("provider_directory_retained_artifact_campaign")
        with pytest.raises(
            asyncpg.CheckViolationError,
            match="retained_campaign_identity_immutable",
        ):
            await connection.execute(
                f"UPDATE {campaign_table} SET request_fence_id=$2 "
                "WHERE campaign_id=$1",
                campaign_id,
                digest("guarded-fence-change"),
            )
        await _assert_runtime_root_fence_boundaries(
            connection,
            retained_item,
            campaign_id,
            campaign_lease,
        )
        _claim_item, claim_campaign_id = await _sealed_fixed_root(
            connection,
            "root-claim-boundary",
        )
        await _disable_root_and_mutate(
            connection,
            claim_campaign_id,
            f"UPDATE {campaign_table} SET adapter_id='changed_adapter_v1' "
            "WHERE campaign_id=$1",
        )
        with pytest.raises(RetainedCampaignMismatch, match="campaign_identity"):
            await claim_sealed_retained_campaign(
                connection,
                campaign_id=claim_campaign_id,
                consumer_recipe_id="root_guard_profile_v1",
            )


@pytest.mark.asyncio
async def test_item_ledger_blocks_delete_insert_replacement(monkeypatch) -> None:
    """The append-only ledger must reject delete-and-replace identity changes."""
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, _campaign_lease = await _fixed_root(
            connection,
            "delete-insert-replacement",
        )
        item_table = table("provider_directory_retained_artifact_campaign_item")
        with pytest.raises(
            asyncpg.CheckViolationError,
            match="retained_item_delete_before_release",
        ):
            await connection.execute(
                f"DELETE FROM {item_table} "
                "WHERE campaign_id=$1 AND source_item_id=$2",
                campaign_id,
                retained_item.source_item_id,
            )
        original_item_by_field = await _replace_item_through_ledger_guard(
            connection,
            item_table,
            campaign_id,
            retained_item.source_item_id,
        )
        restored_identity = await connection.fetchval(
            f"SELECT planned_identity_sha256 FROM {item_table} "
            "WHERE campaign_id=$1 AND source_item_id=$2",
            campaign_id,
            retained_item.source_item_id,
        )
        assert restored_identity == original_item_by_field["planned_identity_sha256"]


async def _rewrite_item_beyond_live_guards(
    connection,
    campaign_id: str,
    source_item_id: str,
) -> None:
    item_table = table("provider_directory_retained_artifact_campaign_item")
    await connection.execute(
        f"UPDATE {item_table} SET family='Organization' "
        "WHERE campaign_id=$1 AND source_item_id=$2",
        campaign_id,
        source_item_id,
    )
    changed_item_by_field = dict(
        await connection.fetchrow(
            f"SELECT * FROM {item_table} " "WHERE campaign_id=$1 AND source_item_id=$2",
            campaign_id,
            source_item_id,
        )
    )
    await connection.execute(
        f"UPDATE {item_table} SET planned_identity_sha256=$3 "
        "WHERE campaign_id=$1 AND source_item_id=$2",
        campaign_id,
        source_item_id,
        planned_item_identity_digest(changed_item_by_field),
    )


@pytest.mark.asyncio
async def test_ledger_reconciliation_rejects_seal_and_claim_rewrites(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        item_table = table("provider_directory_retained_artifact_campaign_item")
        await connection.execute(
            f"ALTER TABLE {item_table} "
            "DISABLE TRIGGER pd_retained_item_identity_immutable"
        )
        await connection.execute(
            f"ALTER TABLE {item_table} "
            "DROP CONSTRAINT pd_retained_item_identity_ledger_fkey"
        )
        try:
            seal_item, seal_campaign_id, seal_lease = await _fixed_root(
                connection,
                "ledger-seal-fallback",
            )
            await _rewrite_item_beyond_live_guards(
                connection,
                seal_campaign_id,
                seal_item.source_item_id,
            )
            with pytest.raises(RetainedCampaignMismatch, match="commitment"):
                await seal_retained_artifact_campaign(
                    connection,
                    campaign_id=seal_campaign_id,
                    campaign_lease=seal_lease,
                )

            claim_item, claim_campaign_id = await _sealed_fixed_root(
                connection,
                "ledger-claim-fallback",
            )
            await _rewrite_item_beyond_live_guards(
                connection,
                claim_campaign_id,
                claim_item.source_item_id,
            )
            with pytest.raises(RetainedCampaignMismatch, match="commitment"):
                await claim_sealed_retained_campaign(
                    connection,
                    campaign_id=claim_campaign_id,
                    consumer_recipe_id="ledger_guard_profile_v1",
                )
        finally:
            await connection.execute(
                f"ALTER TABLE {item_table} "
                "ENABLE TRIGGER pd_retained_item_identity_immutable"
            )


async def _assert_claim_stream_membership_fallback(
    connection,
    stream_table: str,
) -> None:
    stream_identity = digest("claim-stream-membership")
    terminal_item = campaign_item(
        "claim-stream-terminal",
        stream_identity=stream_identity,
        item_role=TERMINAL_ZERO,
    )
    campaign_id = await initialize_retained_artifact_campaign(
        connection,
        plan=ordered_campaign_plan(
            "claim-stream-membership",
            stream_identity,
            (terminal_item,),
        ),
    )
    campaign_lease = await acquire_campaign_lease(
        connection,
        campaign_id=campaign_id,
        owner="claim-stream-owner",
    )
    claim_summary = await seal_retained_artifact_campaign(
        connection,
        campaign_id=campaign_id,
        campaign_lease=campaign_lease,
    )
    assert claim_summary["complete"] is True
    await connection.execute(
        f"ALTER TABLE {stream_table} "
        "DISABLE TRIGGER pd_retained_stream_identity_guard"
    )
    try:
        await connection.execute(
            f"UPDATE {stream_table} SET stream_identity_sha256=$2 "
            "WHERE campaign_id=$1",
            campaign_id,
            digest("claim-stream-replacement"),
        )
    finally:
        await connection.execute(
            f"ALTER TABLE {stream_table} "
            "ENABLE TRIGGER pd_retained_stream_identity_guard"
        )
    with pytest.raises(RetainedCampaignMismatch, match="membership"):
        await claim_sealed_retained_campaign(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id="stream_guard_profile_v1",
        )


@pytest.mark.asyncio
async def test_expected_stream_guard_and_membership_fallback(monkeypatch) -> None:
    """Stream identity changes must fail in triggers and runtime reconciliation."""
    async with retained_database(monkeypatch) as (connection, _schema_name):
        stream_table = table("provider_directory_retained_artifact_campaign_stream")
        stream_identity = digest("stream-membership-guard")
        campaign_id = await initialize_retained_artifact_campaign(
            connection,
            plan=ordered_campaign_plan("stream-membership-guard", stream_identity),
        )
        campaign_lease = await acquire_campaign_lease(
            connection,
            campaign_id=campaign_id,
            owner="stream-membership-owner",
        )
        with pytest.raises(
            asyncpg.CheckViolationError,
            match="retained_stream_identity_immutable",
        ):
            await connection.execute(
                f"UPDATE {stream_table} SET stream_identity_sha256=$2 "
                "WHERE campaign_id=$1",
                campaign_id,
                digest("guarded-stream-replacement"),
            )
        await connection.execute(
            f"ALTER TABLE {stream_table} "
            "DISABLE TRIGGER pd_retained_stream_identity_guard"
        )
        try:
            await connection.execute(
                f"UPDATE {stream_table} SET stream_identity_sha256=$2 "
                "WHERE campaign_id=$1",
                campaign_id,
                digest("runtime-stream-replacement"),
            )
        finally:
            await connection.execute(
                f"ALTER TABLE {stream_table} "
                "ENABLE TRIGGER pd_retained_stream_identity_guard"
            )
        with pytest.raises(RetainedCampaignMismatch, match="membership"):
            await seal_retained_artifact_campaign(
                connection,
                campaign_id=campaign_id,
                campaign_lease=campaign_lease,
            )
        await _assert_claim_stream_membership_fallback(connection, stream_table)
