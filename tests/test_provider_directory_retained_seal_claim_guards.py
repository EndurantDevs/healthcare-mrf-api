# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from process.provider_directory_retained_artifact_contract import (
    TERMINAL_ZERO,
    RetainedArtifactError,
    RetainedCampaignIncomplete,
    RetainedCampaignMismatch,
)
from process.provider_directory_retained_catalog_store import (
    initialize_retained_artifact_campaign,
)
from process.provider_directory_retained_consumer_claim_store import (
    claim_sealed_retained_campaign,
    heartbeat_retained_campaign_consumer,
    release_retained_campaign_consumer,
)
from process.provider_directory_retained_lease_store import (
    acquire_campaign_lease,
)
from process.provider_directory_retained_seal_store import (
    seal_retained_artifact_campaign,
    set_campaign_disk_reservation,
)
from process.provider_directory_retained_identity_contract import (
    planned_item_identity_digest,
)
from process.provider_directory_retained_store_support import database_table
from process.provider_directory_retained_stream_store import (
    append_ordered_stream_item,
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
    campaign_plan = fixed_campaign_plan(label, (retained_item,))
    campaign_id = await initialize_retained_artifact_campaign(
        connection, plan=campaign_plan
    )
    campaign_lease = await acquire_campaign_lease(
        connection, campaign_id=campaign_id, owner=f"owner-{label}"
    )
    return retained_item, campaign_id, campaign_lease


async def _sealed_fixed_root(connection, label: str):
    retained_item, campaign_id, campaign_lease = await _fixed_root(connection, label)
    produced_artifact = registry_artifact(label, retained_item.artifact_kind)
    layout_sha256 = await admit_campaign_item(
        connection, campaign_id, retained_item, produced_artifact
    )
    sealed_summary = await seal_retained_artifact_campaign(
        connection, campaign_id=campaign_id, campaign_lease=campaign_lease
    )
    assert sealed_summary["complete"] is True
    return (
        retained_item,
        campaign_id,
        campaign_lease,
        produced_artifact,
        layout_sha256,
    )


@pytest.mark.asyncio
async def test_incomplete_fixed_seal_is_unclaimable_and_retryable(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        _retained_item, campaign_id, campaign_lease = await _fixed_root(
            connection, "incomplete-fixed"
        )
        first_summary = await seal_retained_artifact_campaign(
            connection, campaign_id=campaign_id, campaign_lease=campaign_lease
        )
        assert first_summary["state"] == "sealed_incomplete"
        assert first_summary["unaccounted_item_count"] == 1
        second_summary = await seal_retained_artifact_campaign(
            connection, campaign_id=campaign_id, campaign_lease=campaign_lease
        )
        assert second_summary["complete"] is False
        with pytest.raises(RetainedCampaignIncomplete, match="not_complete"):
            await claim_sealed_retained_campaign(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id="incomplete_profile_v1",
            )


@pytest.mark.asyncio
async def test_ordered_stream_without_terminal_persists_incomplete_state(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        stream_identity = digest("ordered-no-terminal")
        retained_item = campaign_item(
            "ordered-no-terminal",
            stream_identity=stream_identity,
        )
        campaign_plan = ordered_campaign_plan(
            "ordered-no-terminal",
            stream_identity,
            (retained_item,),
        )
        campaign_id = await initialize_retained_artifact_campaign(
            connection, plan=campaign_plan
        )
        campaign_lease = await acquire_campaign_lease(
            connection,
            campaign_id=campaign_id,
            owner="ordered-no-terminal-owner",
        )
        produced_artifact = registry_artifact(
            "ordered-no-terminal", retained_item.artifact_kind
        )
        await admit_campaign_item(
            connection,
            campaign_id,
            retained_item,
            produced_artifact,
        )
        sealed_summary = await seal_retained_artifact_campaign(
            connection,
            campaign_id=campaign_id,
            campaign_lease=campaign_lease,
        )
        assert sealed_summary["state"] == "sealed_incomplete"
        stream_state = await connection.fetchrow(
            f"""SELECT complete, terminal_sequence_ordinal, terminal_proof_sha256
                  FROM {database_table('provider_directory_retained_artifact_campaign_stream')}
                 WHERE campaign_id=$1 AND stream_identity_sha256=$2""",
            campaign_id,
            stream_identity,
        )
        assert dict(stream_state) == {
            "complete": False,
            "terminal_sequence_ordinal": None,
            "terminal_proof_sha256": None,
        }


async def _rewrite_stream_member_to_unknown_identity(
    connection,
    campaign_id: str,
) -> None:
    item_table = database_table("provider_directory_retained_artifact_campaign_item")
    identity_table = database_table(
        "provider_directory_retained_artifact_campaign_item_identity"
    )
    await connection.execute(
        f"ALTER TABLE {item_table} "
        "DISABLE TRIGGER pd_retained_item_identity_immutable"
    )
    await connection.execute(
        f"ALTER TABLE {item_table} "
        "DROP CONSTRAINT pd_retained_item_identity_ledger_fkey"
    )
    await connection.execute(
        f"ALTER TABLE {identity_table} " "DISABLE TRIGGER pd_retained_item_ledger_guard"
    )
    try:
        await connection.execute(
            f"UPDATE {item_table} SET stream_identity_sha256=$2 "
            "WHERE campaign_id=$1",
            campaign_id,
            digest("unknown-stream"),
        )
        changed_item_by_field = dict(
            await connection.fetchrow(
                f"SELECT * FROM {item_table} WHERE campaign_id=$1",
                campaign_id,
            )
        )
        changed_identity = planned_item_identity_digest(changed_item_by_field)
        await connection.execute(
            f"UPDATE {item_table} SET planned_identity_sha256=$2 "
            "WHERE campaign_id=$1",
            campaign_id,
            changed_identity,
        )
        await connection.execute(
            f"UPDATE {identity_table} SET planned_identity_sha256=$2 "
            "WHERE campaign_id=$1",
            campaign_id,
            changed_identity,
        )
    finally:
        await connection.execute(
            f"ALTER TABLE {identity_table} "
            "ENABLE TRIGGER pd_retained_item_ledger_guard"
        )
        await connection.execute(
            f"ALTER TABLE {item_table} "
            "ENABLE TRIGGER pd_retained_item_identity_immutable"
        )


async def _assert_ordered_stream_identity_rejected(connection) -> None:
    stream_identity = digest("wrong-stream")
    stream_member = campaign_item("wrong-stream", stream_identity=stream_identity)
    campaign_id = await initialize_retained_artifact_campaign(
        connection,
        plan=ordered_campaign_plan(
            "wrong-stream",
            stream_identity,
            (stream_member,),
        ),
    )
    campaign_lease = await acquire_campaign_lease(
        connection,
        campaign_id=campaign_id,
        owner="wrong-stream-owner",
    )
    await _rewrite_stream_member_to_unknown_identity(connection, campaign_id)
    with pytest.raises(RetainedCampaignMismatch, match="stream_identity_changed"):
        await seal_retained_artifact_campaign(
            connection,
            campaign_id=campaign_id,
            campaign_lease=campaign_lease,
        )


async def _assert_fixed_seal_rejects_stream_rows(connection) -> None:
    _fixed_member, campaign_id, campaign_lease = await _fixed_root(
        connection,
        "fixed-with-stream",
    )
    stream_table = database_table(
        "provider_directory_retained_artifact_campaign_stream"
    )
    await connection.execute(
        f"""INSERT INTO {stream_table} (
                campaign_id, stream_identity_sha256, stream_ordinal, created_at
            ) VALUES ($1, $2, 0, now())""",
        campaign_id,
        digest("fixed-extra-stream"),
    )
    with pytest.raises(RetainedCampaignMismatch, match="fixed_catalog_has_stream"):
        await seal_retained_artifact_campaign(
            connection,
            campaign_id=campaign_id,
            campaign_lease=campaign_lease,
        )


@pytest.mark.asyncio
async def test_seal_rejects_wrong_stream_identity_and_fixed_stream_rows(
    monkeypatch,
) -> None:
    """Sealing must reject rewritten ordered streams and streams on fixed roots."""
    async with retained_database(monkeypatch) as (connection, _schema_name):
        await _assert_ordered_stream_identity_rejected(connection)
        await _assert_fixed_seal_rejects_stream_rows(connection)


@pytest.mark.asyncio
@pytest.mark.parametrize("registry_kind", ("artifact", "layout", "range"))
async def test_seal_rejects_registry_and_range_tamper(
    monkeypatch,
    registry_kind: str,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, campaign_lease = await _fixed_root(
            connection, f"registry-{registry_kind}"
        )
        produced_artifact = registry_artifact(
            f"registry-{registry_kind}", retained_item.artifact_kind
        )
        layout_sha256 = await admit_campaign_item(
            connection, campaign_id, retained_item, produced_artifact
        )
        if registry_kind == "artifact":
            await connection.execute(
                f"UPDATE {database_table('provider_directory_retained_artifact')} SET registry_status='quarantined' WHERE artifact_sha256=$1",
                produced_artifact.artifact_sha256,
            )
        elif registry_kind == "layout":
            await connection.execute(
                f"UPDATE {database_table('provider_directory_retained_artifact_layout')} SET registry_status='quarantined' WHERE layout_sha256=$1",
                layout_sha256,
            )
        else:
            await connection.execute(
                f"DELETE FROM {database_table('provider_directory_retained_artifact_range')} WHERE layout_sha256=$1",
                layout_sha256,
            )
        expected_code = (
            "campaign_layout_ranges_incomplete"
            if registry_kind == "range"
            else "campaign_artifact_registry_incomplete"
        )
        with pytest.raises(RetainedCampaignMismatch, match=expected_code):
            await seal_retained_artifact_campaign(
                connection, campaign_id=campaign_id, campaign_lease=campaign_lease
            )


@pytest.mark.asyncio
async def test_terminal_only_stream_claim_covers_empty_registry(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        stream_identity = digest("terminal-only-stream")
        campaign_plan = ordered_campaign_plan("terminal-only", stream_identity)
        campaign_id = await initialize_retained_artifact_campaign(
            connection, plan=campaign_plan
        )
        campaign_lease = await acquire_campaign_lease(
            connection, campaign_id=campaign_id, owner="terminal-only-owner"
        )
        terminal_member = campaign_item(
            "terminal-only",
            stream_identity=stream_identity,
            item_role=TERMINAL_ZERO,
        )
        await append_ordered_stream_item(
            connection,
            campaign_id=campaign_id,
            campaign_lease=campaign_lease,
            item=terminal_member,
        )
        sealed_summary = await seal_retained_artifact_campaign(
            connection, campaign_id=campaign_id, campaign_lease=campaign_lease
        )
        assert sealed_summary["complete"] is True
        claimed_campaign = await claim_sealed_retained_campaign(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id="terminal_only_profile_v1",
        )
        assert claimed_campaign.artifacts == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("tamper_kind", ("census", "registry", "digest"))
async def test_claim_rejects_post_seal_tamper(
    monkeypatch,
    tamper_kind: str,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, _lease, produced_artifact, layout_sha256 = (
            await _sealed_fixed_root(connection, f"claim-{tamper_kind}")
        )
        if tamper_kind == "census":
            await connection.execute(
                f"UPDATE {database_table('provider_directory_retained_artifact_campaign_item')} SET status='failed' WHERE campaign_id=$1",
                campaign_id,
            )
        elif tamper_kind == "registry":
            await connection.execute(
                f"UPDATE {database_table('provider_directory_retained_artifact')} SET registry_status='quarantined' WHERE artifact_sha256=$1",
                produced_artifact.artifact_sha256,
            )
        else:
            await connection.execute(
                f"UPDATE {database_table('provider_directory_retained_artifact_range')} SET canonical_sha256=$2 WHERE layout_sha256=$1",
                layout_sha256,
                digest("tampered-canonical-range"),
            )
        expected_error = (
            RetainedCampaignMismatch
            if tamper_kind == "digest"
            else RetainedCampaignIncomplete
        )
        with pytest.raises(expected_error):
            await claim_sealed_retained_campaign(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id=f"tamper_{tamper_kind}_profile_v1",
            )
        assert retained_item.source_item_id


@pytest.mark.asyncio
async def test_consumer_identity_reference_heartbeat_and_release_guards(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        _member, campaign_id, _lease, _artifact, _layout = await _sealed_fixed_root(
            connection, "consumer-guards"
        )
        claimed_campaign = await claim_sealed_retained_campaign(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id="guarded_profile_v1",
        )
        wrong_digest = digest("wrong-consumer-digest")
        with pytest.raises(RetainedCampaignMismatch, match="consumer_claim_identity"):
            await heartbeat_retained_campaign_consumer(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id="guarded_profile_v1",
                claimed_campaign_sha256=wrong_digest,
                consumer_claim_generation=(claimed_campaign.consumer_claim_generation),
            )
        with pytest.raises(RetainedArtifactError, match="consumer_claim_not_found"):
            await release_retained_campaign_consumer(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id="missing_profile_v1",
                claimed_campaign_sha256=wrong_digest,
                consumer_claim_generation=1,
            )
        with pytest.raises(RetainedCampaignMismatch, match="consumer_claim_identity"):
            await release_retained_campaign_consumer(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id="guarded_profile_v1",
                claimed_campaign_sha256=wrong_digest,
                consumer_claim_generation=(claimed_campaign.consumer_claim_generation),
            )
        assert claimed_campaign.campaign_sha256 != wrong_digest


@pytest.mark.asyncio
@pytest.mark.parametrize("claim_tamper", ("consumer", "reference"))
async def test_repeat_claim_rejects_persisted_claim_tamper(
    monkeypatch,
    claim_tamper: str,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        _member, campaign_id, _lease, _artifact, _layout = await _sealed_fixed_root(
            connection, f"repeat-{claim_tamper}"
        )
        await claim_sealed_retained_campaign(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id="repeat_profile_v1",
        )
        if claim_tamper == "consumer":
            await connection.execute(
                f"""UPDATE {database_table('provider_directory_retained_artifact_consumer')}
                       SET claimed_campaign_sha256=$3
                     WHERE campaign_id=$1 AND consumer_recipe_id=$2""",
                campaign_id,
                "repeat_profile_v1",
                digest("wrong-claim-identity"),
            )
            expected_code = "consumer_claim_identity_mismatch"
        else:
            await connection.execute(
                f"""UPDATE {database_table('provider_directory_retained_artifact_consumer_reference')}
                       SET released_at=now()
                     WHERE campaign_id=$1 AND consumer_recipe_id=$2""",
                campaign_id,
                "repeat_profile_v1",
            )
            expected_code = "consumer_artifact_refs_mismatch"
        with pytest.raises(RetainedCampaignMismatch, match=expected_code):
            await claim_sealed_retained_campaign(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id="repeat_profile_v1",
            )


@pytest.mark.asyncio
async def test_seal_rejects_invalid_reservation_and_completed_campaign(
    monkeypatch,
) -> None:
    with pytest.raises(RetainedArtifactError, match="disk_reserved_bytes_invalid"):
        await set_campaign_disk_reservation(
            None,
            campaign_id=digest("invalid-reservation"),
            campaign_lease=None,
            reserved_bytes=-1,
        )
    async with retained_database(monkeypatch) as (connection, _schema_name):
        _member, campaign_id, campaign_lease, _artifact, _layout = (
            await _sealed_fixed_root(connection, "completed-seal")
        )
        with pytest.raises(RetainedArtifactError, match="not_sealable"):
            await seal_retained_artifact_campaign(
                connection,
                campaign_id=campaign_id,
                campaign_lease=campaign_lease,
            )
