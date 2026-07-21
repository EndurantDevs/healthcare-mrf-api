# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from process import provider_directory_retained_catalog_store as catalog_store

from process.provider_directory_retained_artifact_contract import (
    LeaseIdentity,
    RetainedArtifactError,
    RetainedCampaignMismatch,
)
from process.provider_directory_retained_catalog_store import (
    initialize_retained_artifact_campaign,
)
from process.provider_directory_retained_lease_store import (
    acquire_campaign_lease,
    acquire_item_lease,
    release_campaign_lease,
    release_item_lease,
    renew_campaign_lease,
    renew_item_lease,
)
from process.provider_directory_retained_request_fence_store import (
    reserve_endpoint_request_slot,
)
from process.provider_directory_retained_seal_store import (
    set_campaign_disk_reservation,
)
from process.provider_directory_retained_store_support import (
    database_table,
)
from process.provider_directory_retained_stream_store import (
    append_ordered_stream_item,
    list_campaign_items,
)
from tests.provider_directory_retained_core_postgres_support import (
    campaign_item,
    digest,
    fixed_campaign_plan,
    ordered_campaign_plan,
    retained_database,
)


async def _mutate_catalog_identity(
    connection,
    campaign_id: str,
    source_item_id: str,
    mutation_case: str,
) -> None:
    campaign_table = database_table("provider_directory_retained_artifact_campaign")
    stream_table = database_table(
        "provider_directory_retained_artifact_campaign_stream"
    )
    campaign_item_table = database_table(
        "provider_directory_retained_artifact_campaign_item"
    )
    mutation_sql_by_case = {
        "campaign": f"UPDATE {campaign_table} SET adapter_id='changed_adapter_v1' WHERE campaign_id=$1",
        "fixed_count": f"UPDATE {campaign_table} SET expected_item_count=2, identity_member_count=2 WHERE campaign_id=$1",
        "stream": f"UPDATE {stream_table} SET stream_ordinal=9 WHERE campaign_id=$1",
        "fixed_item": f"UPDATE {campaign_item_table} SET family='Organization' WHERE campaign_id=$1 AND source_item_id=$2",
        "stream_item": f"UPDATE {campaign_item_table} SET family='Organization' WHERE campaign_id=$1 AND source_item_id=$2",
    }
    mutation_sql = mutation_sql_by_case[mutation_case]
    mutation_arguments = (
        (campaign_id, source_item_id) if "$2" in mutation_sql else (campaign_id,)
    )
    await connection.execute(mutation_sql, *mutation_arguments)


def _identity_guard_for_case(mutation_case: str) -> tuple[str, str]:
    if mutation_case in {"campaign", "fixed_count"}:
        return (
            database_table("provider_directory_retained_artifact_campaign"),
            "pd_retained_campaign_identity_guard",
        )
    if mutation_case == "stream":
        return (
            database_table("provider_directory_retained_artifact_campaign_stream"),
            "pd_retained_stream_identity_guard",
        )
    return (
        database_table("provider_directory_retained_artifact_campaign_item"),
        "pd_retained_item_identity_immutable",
    )


async def _mutate_catalog_identity_without_guard(
    connection,
    campaign_id: str,
    source_item_id: str,
    mutation_case: str,
) -> None:
    guarded_table, trigger_name = _identity_guard_for_case(mutation_case)
    await connection.execute(
        f"ALTER TABLE {guarded_table} DISABLE TRIGGER {trigger_name}"
    )
    try:
        await _mutate_catalog_identity(
            connection,
            campaign_id,
            source_item_id,
            mutation_case,
        )
    finally:
        await connection.execute(
            f"ALTER TABLE {guarded_table} ENABLE TRIGGER {trigger_name}"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutation_case", "expected_code"),
    (
        ("campaign", "retained_campaign_identity_mismatch"),
        ("fixed_count", "fixed_catalog_count_mismatch"),
        ("fixed_item", "fixed_catalog_census_mismatch"),
        ("stream", "expected_stream_identity_mismatch"),
        ("stream_item", "ordered_stream_item_mismatch"),
    ),
)
async def test_catalog_reuse_detects_every_identity_drift(
    monkeypatch,
    mutation_case: str,
    expected_code: str,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        is_stream_case = mutation_case.startswith("stream")
        stream_identity = digest(f"stream:{mutation_case}")
        retained_item = campaign_item(
            mutation_case,
            stream_identity=stream_identity if is_stream_case else None,
        )
        campaign_plan = (
            ordered_campaign_plan(mutation_case, stream_identity, (retained_item,))
            if is_stream_case
            else fixed_campaign_plan(mutation_case, (retained_item,))
        )
        campaign_id = await initialize_retained_artifact_campaign(
            connection, plan=campaign_plan
        )
        await _mutate_catalog_identity_without_guard(
            connection,
            campaign_id,
            retained_item.source_item_id,
            mutation_case,
        )
        with pytest.raises(RetainedCampaignMismatch, match=expected_code):
            await initialize_retained_artifact_campaign(connection, plan=campaign_plan)


@pytest.mark.asyncio
async def test_catalog_reuse_compares_keyed_locator_identity_not_raw_text(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("locator-identity")
        campaign_plan = fixed_campaign_plan("locator-identity", (retained_item,))
        campaign_id = await initialize_retained_artifact_campaign(
            connection, plan=campaign_plan
        )
        changed_item = replace(
            retained_item,
            source_locator="fixture://changed-private-cursor",
        )
        changed_plan = replace(campaign_plan, items=(changed_item,)).validate()
        assert changed_plan.campaign_id == campaign_id
        with pytest.raises(RetainedCampaignMismatch, match="fixed_catalog_census"):
            await initialize_retained_artifact_campaign(
                connection,
                plan=changed_plan,
            )


@pytest.mark.asyncio
async def test_campaign_initialization_uses_only_one_preawait_snapshot(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("preawait-snapshot")
        campaign_plan = fixed_campaign_plan(
            "preawait-snapshot",
            (retained_item,),
        )
        expected_campaign_id = campaign_plan.campaign_id
        expected_partition_digest = retained_item.partition_metadata_sha256
        insert_started = asyncio.Event()
        allow_insert = asyncio.Event()
        original_insert = catalog_store._insert_campaign_root

        async def gated_insert(*arguments):
            insert_started.set()
            await allow_insert.wait()
            await original_insert(*arguments)

        monkeypatch.setattr(catalog_store, "_insert_campaign_root", gated_insert)
        initialization = asyncio.create_task(
            initialize_retained_artifact_campaign(
                connection,
                plan=campaign_plan,
            )
        )
        await insert_started.wait()
        retained_item.partition_metadata["late"] = "mutation"
        object.__setattr__(retained_item, "family", "Organization")
        object.__setattr__(
            retained_item,
            "_partition_metadata_sha256",
            digest("late-forged-partition"),
        )
        object.__setattr__(campaign_plan, "items", ())
        object.__setattr__(campaign_plan, "aggregate_byte_budget", 16384)
        allow_insert.set()
        assert await initialization == expected_campaign_id
        persisted = await connection.fetchrow(
            f"""SELECT campaign.expected_item_count,
                        campaign.aggregate_byte_budget,
                        item.family, item.partition_metadata_sha256
                   FROM {database_table('provider_directory_retained_artifact_campaign')} campaign
                   JOIN {database_table('provider_directory_retained_artifact_campaign_item')} item
                     ON item.campaign_id=campaign.campaign_id
                  WHERE campaign.campaign_id=$1""",
            expected_campaign_id,
        )
        assert dict(persisted) == {
            "expected_item_count": 1,
            "aggregate_byte_budget": 8192,
            "family": "PractitionerRole",
            "partition_metadata_sha256": expected_partition_digest,
        }


@pytest.mark.asyncio
async def test_campaign_lease_fences_competitors_and_stale_epochs(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("campaign-lease")
        campaign_plan = fixed_campaign_plan("campaign-lease", (retained_item,))
        campaign_id = await initialize_retained_artifact_campaign(
            connection, plan=campaign_plan
        )
        active_lease = await acquire_campaign_lease(
            connection, campaign_id=campaign_id, owner="campaign-owner-one"
        )
        with pytest.raises(RetainedArtifactError, match="campaign_lease_unavailable"):
            await acquire_campaign_lease(
                connection, campaign_id=campaign_id, owner="campaign-owner-two"
            )
        await renew_campaign_lease(
            connection, campaign_id=campaign_id, lease=active_lease
        )
        stale_lease = LeaseIdentity(active_lease.owner, active_lease.epoch + 1)
        with pytest.raises(RetainedArtifactError, match="campaign_lease_lost"):
            await renew_campaign_lease(
                connection, campaign_id=campaign_id, lease=stale_lease
            )
        with pytest.raises(RetainedArtifactError, match="campaign_lease_lost"):
            await set_campaign_disk_reservation(
                connection,
                campaign_id=campaign_id,
                campaign_lease=stale_lease,
                reserved_bytes=1,
            )
        await release_campaign_lease(
            connection, campaign_id=campaign_id, lease=active_lease
        )
        replacement_lease = await acquire_campaign_lease(
            connection, campaign_id=campaign_id, owner="campaign-owner-two"
        )
        assert replacement_lease.epoch > active_lease.epoch


@pytest.mark.asyncio
async def test_item_lease_fences_competitors_and_stale_epochs(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("item-lease")
        campaign_plan = fixed_campaign_plan("item-lease", (retained_item,))
        campaign_id = await initialize_retained_artifact_campaign(
            connection, plan=campaign_plan
        )
        campaign_lease = await acquire_campaign_lease(
            connection, campaign_id=campaign_id, owner="campaign-owner"
        )
        active_lease = await acquire_item_lease(
            connection,
            campaign_id=campaign_id,
            source_item_id=retained_item.source_item_id,
            campaign_lease=campaign_lease,
            owner="item-owner-one",
        )
        with pytest.raises(RetainedArtifactError, match="item_lease_unavailable"):
            await acquire_item_lease(
                connection,
                campaign_id=campaign_id,
                source_item_id=retained_item.source_item_id,
                campaign_lease=campaign_lease,
                owner="item-owner-two",
            )
        await renew_item_lease(
            connection,
            campaign_id=campaign_id,
            source_item_id=retained_item.source_item_id,
            lease=active_lease,
        )
        stale_lease = LeaseIdentity(active_lease.owner, active_lease.epoch + 1)
        with pytest.raises(RetainedArtifactError, match="item_lease_lost"):
            await renew_item_lease(
                connection,
                campaign_id=campaign_id,
                source_item_id=retained_item.source_item_id,
                lease=stale_lease,
            )
        with pytest.raises(RetainedArtifactError, match="item_lease_lost"):
            await reserve_endpoint_request_slot(
                connection,
                campaign_id=campaign_id,
                source_item_id=retained_item.source_item_id,
                campaign_lease=campaign_lease,
                item_lease=stale_lease,
                request_interval_ms=0,
            )
        await release_item_lease(
            connection,
            campaign_id=campaign_id,
            source_item_id=retained_item.source_item_id,
            lease=active_lease,
        )


@pytest.mark.asyncio
async def test_lease_ttl_guards_fail_before_database_mutation() -> None:
    valid_digest = digest("lease-ttl")
    valid_lease = LeaseIdentity("lease-owner", 1)
    with pytest.raises(RetainedArtifactError, match="lease_ttl_invalid"):
        await acquire_campaign_lease(
            None, campaign_id=valid_digest, owner="lease-owner", ttl_seconds=True
        )
    with pytest.raises(RetainedArtifactError, match="lease_ttl_invalid"):
        await renew_campaign_lease(
            None, campaign_id=valid_digest, lease=valid_lease, ttl_seconds=29
        )
    with pytest.raises(RetainedArtifactError, match="lease_ttl_invalid"):
        await acquire_item_lease(
            None,
            campaign_id=valid_digest,
            source_item_id=valid_digest,
            campaign_lease=valid_lease,
            owner="lease-owner",
            ttl_seconds=3601,
        )
    with pytest.raises(RetainedArtifactError, match="lease_ttl_invalid"):
        await renew_item_lease(
            None,
            campaign_id=valid_digest,
            source_item_id=valid_digest,
            lease=valid_lease,
            ttl_seconds=False,
        )


@pytest.mark.asyncio
async def test_stream_append_rolls_back_gaps_and_rejects_unknown_stream(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        stream_identity = digest("stream-guards")
        campaign_plan = ordered_campaign_plan("stream-guards", stream_identity)
        campaign_id = await initialize_retained_artifact_campaign(
            connection, plan=campaign_plan
        )
        campaign_lease = await acquire_campaign_lease(
            connection, campaign_id=campaign_id, owner="stream-owner"
        )
        gap_member = campaign_item(
            "stream-gap", stream_identity=stream_identity, sequence_ordinal=1
        )
        with pytest.raises(RetainedCampaignMismatch, match="sequence_gap"):
            await append_ordered_stream_item(
                connection,
                campaign_id=campaign_id,
                campaign_lease=campaign_lease,
                item=gap_member,
            )
        assert await list_campaign_items(connection, campaign_id=campaign_id) == []
        unknown_member = campaign_item(
            "stream-unknown",
            stream_identity=digest("unknown-stream"),
            sequence_ordinal=0,
        )
        with pytest.raises(RetainedArtifactError, match="not_appendable"):
            await append_ordered_stream_item(
                connection,
                campaign_id=campaign_id,
                campaign_lease=campaign_lease,
                item=unknown_member,
            )


@pytest.mark.asyncio
async def test_stream_append_rejects_collision_complete_and_fixed_roots(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        stream_identity = digest("stream-collision")
        initial_member = campaign_item(
            "stream-collision", stream_identity=stream_identity
        )
        campaign_plan = ordered_campaign_plan(
            "stream-collision", stream_identity, (initial_member,)
        )
        campaign_id = await initialize_retained_artifact_campaign(
            connection, plan=campaign_plan
        )
        campaign_lease = await acquire_campaign_lease(
            connection, campaign_id=campaign_id, owner="collision-owner"
        )
        collision_member = campaign_item(
            "stream-collision",
            stream_identity=stream_identity,
            sequence_ordinal=1,
        )
        with pytest.raises(RetainedCampaignMismatch, match="stream_item_mismatch"):
            await append_ordered_stream_item(
                connection,
                campaign_id=campaign_id,
                campaign_lease=campaign_lease,
                item=collision_member,
            )
        await connection.execute(
            f"""UPDATE {database_table('provider_directory_retained_artifact_campaign_stream')}
                   SET complete=TRUE, terminal_sequence_ordinal=0,
                       terminal_proof_sha256=$2, completed_at=now()
                 WHERE campaign_id=$1""",
            campaign_id,
            digest("forced-terminal-proof"),
        )
        with pytest.raises(RetainedArtifactError, match="not_appendable"):
            await append_ordered_stream_item(
                connection,
                campaign_id=campaign_id,
                campaign_lease=campaign_lease,
                item=collision_member,
            )
        fixed_item = campaign_item("fixed-append")
        fixed_plan = fixed_campaign_plan("fixed-append", (fixed_item,))
        fixed_id = await initialize_retained_artifact_campaign(
            connection, plan=fixed_plan
        )
        fixed_lease = await acquire_campaign_lease(
            connection, campaign_id=fixed_id, owner="fixed-owner"
        )
        with pytest.raises(RetainedArtifactError, match="not_mutable"):
            await append_ordered_stream_item(
                connection,
                campaign_id=fixed_id,
                campaign_lease=fixed_lease,
                item=campaign_item("fixed-extra"),
            )


@pytest.mark.asyncio
async def test_list_campaign_items_validates_campaign_identity() -> None:
    with pytest.raises(RetainedArtifactError, match="campaign_id_invalid"):
        await list_campaign_items(None, campaign_id="not-a-digest")


def test_database_schema_rejects_unsafe_identifiers(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "unsafe-schema")
    with pytest.raises(RetainedArtifactError, match="retained_database_schema_invalid"):
        database_table("provider_directory_retained_artifact_campaign")
