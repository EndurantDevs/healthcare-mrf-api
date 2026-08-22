# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio

import pytest

from process import provider_directory_retained_producer_store as producer_store
from process.provider_directory_retained_artifact_contract import (
    FHIR_BUNDLE_PAGE,
    TERMINAL_ZERO,
    LeaseIdentity,
    RetainedArtifactError,
    RetainedCampaignMismatch,
)
from process.provider_directory_retained_catalog_store import (
    initialize_retained_artifact_campaign,
)
from process.provider_directory_retained_lease_store import acquire_campaign_lease
from process.provider_directory_retained_store_support import database_table
from process.provider_directory_retained_stream_store import append_ordered_stream_item
from tests.provider_directory_retained_core_postgres_support import (
    campaign_item,
    digest,
    ordered_campaign_plan,
    retained_database,
)
from tests.test_provider_directory_retained_producer_store_postgres import (
    _acquire_item,
    _admit,
    _initialize_campaign,
    _leased_item_campaign,
    _produced_artifact,
    _registry_counts,
)


@pytest.mark.asyncio
async def test_lease_and_item_state_fail_closed(monkeypatch) -> None:
    """Reject stale lease identities and non-admittable item state."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("producer-fences")
        produced_artifact = _produced_artifact(
            "producer-fences", retained_item.artifact_kind
        )
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, "producer-fences", retained_item
        )
        with pytest.raises(RetainedArtifactError, match="campaign_lease_lost"):
            await _admit(
                connection,
                campaign_id,
                retained_item,
                LeaseIdentity(campaign_lease.owner, campaign_lease.epoch + 1),
                item_lease,
                produced_artifact,
            )
        with pytest.raises(RetainedArtifactError, match="item_lease_lost"):
            await _admit(
                connection,
                campaign_id,
                retained_item,
                campaign_lease,
                LeaseIdentity(item_lease.owner, item_lease.epoch + 1),
                produced_artifact,
            )
        await connection.execute(
            f"""UPDATE {database_table('provider_directory_retained_artifact_campaign_item')}
                   SET status='downloading'
                 WHERE campaign_id=$1 AND source_item_id=$2""",
            campaign_id,
            retained_item.source_item_id,
        )
        with pytest.raises(
            RetainedCampaignMismatch,
            match="retained_producer_item_state_mismatch",
        ):
            await _admit(
                connection,
                campaign_id,
                retained_item,
                campaign_lease,
                item_lease,
                produced_artifact,
            )
        assert await _registry_counts(connection) == (0, 0, 0)


@pytest.mark.asyncio
async def test_per_item_budget_fails_before_registry_mutation(monkeypatch) -> None:
    """Reject an oversized item without retaining registry rows."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        budget_item = campaign_item(
            "producer-per-item-budget", declared_byte_count=None
        )
        budget_artifact = _produced_artifact(
            "producer-per-item-budget",
            budget_item.artifact_kind,
            byte_count=1025,
        )
        budget_campaign, budget_campaign_lease, budget_item_lease = (
            await _leased_item_campaign(
                connection, "producer-per-item-budget", budget_item
            )
        )
        with pytest.raises(RetainedArtifactError, match="per_item_byte_budget_exceeded"):
            await _admit(
                connection,
                budget_campaign,
                budget_item,
                budget_campaign_lease,
                budget_item_lease,
                budget_artifact,
            )
        assert await _registry_counts(connection) == (0, 0, 0)


@pytest.mark.asyncio
async def test_aggregate_budget_rejects_before_second_registry_mutation(
    monkeypatch,
) -> None:
    """Reject the second item before adding its registry identities."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        first_item = campaign_item(
            "producer-aggregate-first", declared_byte_count=None
        )
        second_item = campaign_item(
            "producer-aggregate-second", declared_byte_count=None
        )
        campaign_id, campaign_lease = await _initialize_campaign(
            connection,
            "producer-aggregate",
            (first_item, second_item),
            per_item_byte_budget=6000,
            aggregate_byte_budget=8192,
        )
        first_item_lease = await _acquire_item(
            connection, campaign_id, first_item, campaign_lease
        )
        await _admit(
            connection,
            campaign_id,
            first_item,
            campaign_lease,
            first_item_lease,
            _produced_artifact(
                "producer-aggregate-first", first_item.artifact_kind, byte_count=5000
            ),
        )
        second_item_lease = await _acquire_item(
            connection, campaign_id, second_item, campaign_lease
        )
        with pytest.raises(RetainedArtifactError, match="aggregate_byte_budget_exceeded"):
            await _admit(
                connection,
                campaign_id,
                second_item,
                campaign_lease,
                second_item_lease,
                _produced_artifact(
                    "producer-aggregate-second",
                    second_item.artifact_kind,
                    byte_count=5000,
                ),
            )
        assert await _registry_counts(connection) == (1, 1, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "expected_code"),
    (
        ("declared-size", "retained_producer_declared_size_mismatch"),
        ("artifact-kind", "retained_producer_item_mismatch"),
    ),
)
async def test_planned_item_contract_rejects_size_and_kind(
    monkeypatch,
    case: str,
    expected_code: str,
) -> None:
    """Reject producer proofs that disagree with the frozen campaign item."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item(f"producer-{case}")
        artifact_kind = (
            FHIR_BUNDLE_PAGE if case == "artifact-kind" else retained_item.artifact_kind
        )
        byte_count = 14 if case == "declared-size" else 13
        produced_artifact = _produced_artifact(
            f"producer-{case}", artifact_kind, byte_count=byte_count
        )
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, f"producer-{case}", retained_item
        )
        with pytest.raises(RetainedCampaignMismatch, match=expected_code):
            await _admit(
                connection,
                campaign_id,
                retained_item,
                campaign_lease,
                item_lease,
                produced_artifact,
            )
        assert await _registry_counts(connection) == (0, 0, 0)


@pytest.mark.asyncio
async def test_terminal_zero_item_rejects_artifact_admission(monkeypatch) -> None:
    """Keep terminal-zero proofs outside the producer artifact registry."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        stream_identity = digest("producer-terminal-stream")
        campaign_id = await initialize_retained_artifact_campaign(
            connection,
            plan=ordered_campaign_plan("producer-terminal", stream_identity),
        )
        campaign_lease = await acquire_campaign_lease(
            connection, campaign_id=campaign_id, owner="producer-terminal-owner"
        )
        retained_item = campaign_item(
            "producer-terminal",
            stream_identity=stream_identity,
            item_role=TERMINAL_ZERO,
        )
        await append_ordered_stream_item(
            connection,
            campaign_id=campaign_id,
            campaign_lease=campaign_lease,
            item=retained_item,
        )
        produced_artifact = _produced_artifact(
            "producer-terminal", retained_item.artifact_kind
        )
        with pytest.raises(
            RetainedCampaignMismatch, match="retained_producer_item_mismatch"
        ):
            await _admit(
                connection,
                campaign_id,
                retained_item,
                campaign_lease,
                LeaseIdentity("unused-terminal-item", 1),
                produced_artifact,
            )
        assert await _registry_counts(connection) == (0, 0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("lease_scope", "expected_code"),
    (("campaign", "campaign_lease_lost"), ("item", "item_lease_lost")),
)
async def test_lease_expiry_after_registry_insert_rolls_back(
    monkeypatch,
    lease_scope: str,
    expected_code: str,
) -> None:
    """Fence an expiry observed after registry insertion and roll back all rows."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        label = f"producer-{lease_scope}-expiry"
        retained_item = campaign_item(label)
        produced_artifact = _produced_artifact(label, retained_item.artifact_kind)
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, label, retained_item
        )
        original_insert = producer_store._insert_layout_and_ranges

        async def insert_then_expire(database_connection, prepared_artifact):
            await original_insert(database_connection, prepared_artifact)
            table_name = "provider_directory_retained_artifact_campaign"
            predicate = "campaign_id=$1"
            arguments = (campaign_id,)
            if lease_scope == "item":
                table_name += "_item"
                predicate += " AND source_item_id=$2"
                arguments += (retained_item.source_item_id,)
            await database_connection.execute(
                f"""UPDATE {database_table(table_name)}
                       SET lease_expires_at=clock_timestamp()
                     WHERE {predicate}""",
                *arguments,
            )

        monkeypatch.setattr(
            producer_store, "_insert_layout_and_ranges", insert_then_expire
        )
        with pytest.raises(RetainedArtifactError, match=expected_code):
            await _admit(
                connection,
                campaign_id,
                retained_item,
                campaign_lease,
                item_lease,
                produced_artifact,
            )
        assert await _registry_counts(connection) == (0, 0, 0)
        item_state = await connection.fetchrow(
            f"""SELECT status, artifact_sha256, layout_sha256
                   FROM {database_table('provider_directory_retained_artifact_campaign_item')}
                  WHERE campaign_id=$1 AND source_item_id=$2""",
            campaign_id,
            retained_item.source_item_id,
        )
        assert dict(item_state) == {
            "status": "expected",
            "artifact_sha256": None,
            "layout_sha256": None,
        }


@pytest.mark.asyncio
async def test_cancellation_rolls_back_partial_registry_and_admission(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("producer-cancel")
        produced_artifact = _produced_artifact(
            "producer-cancel", retained_item.artifact_kind, range_count=2
        )
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, "producer-cancel", retained_item
        )
        registry_inserted = asyncio.Event()
        original_insert = producer_store._insert_layout_and_ranges

        async def insert_then_block(database_connection, prepared_artifact):
            await original_insert(database_connection, prepared_artifact)
            registry_inserted.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(
            producer_store,
            "_insert_layout_and_ranges",
            insert_then_block,
        )
        admission = asyncio.create_task(
            _admit(
                connection,
                campaign_id,
                retained_item,
                campaign_lease,
                item_lease,
                produced_artifact,
            )
        )
        await asyncio.wait_for(registry_inserted.wait(), timeout=2)
        admission.cancel()
        with pytest.raises(asyncio.CancelledError):
            await admission
        assert await _registry_counts(connection) == (0, 0, 0)
        item_state = await connection.fetchrow(
            f"""SELECT status, artifact_sha256, layout_sha256, lease_owner
                   FROM {database_table('provider_directory_retained_artifact_campaign_item')}
                  WHERE campaign_id=$1 AND source_item_id=$2""",
            campaign_id,
            retained_item.source_item_id,
        )
        assert dict(item_state) == {
            "status": "expected",
            "artifact_sha256": None,
            "layout_sha256": None,
            "lease_owner": item_lease.owner,
        }


@pytest.mark.asyncio
async def test_injected_exception_rolls_back_registry_and_admission(monkeypatch) -> None:
    """Roll back registry rows when the terminal item update raises."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("producer-injected-exception")
        produced_artifact = _produced_artifact(
            "producer-injected-exception", retained_item.artifact_kind, range_count=2
        )
        campaign_id, campaign_lease, item_lease = await _leased_item_campaign(
            connection, "producer-injected-exception", retained_item
        )

        async def injected_failure(*_arguments):
            raise RuntimeError("synthetic_injected_failure")

        monkeypatch.setattr(producer_store, "_update_admitted_item", injected_failure)
        with pytest.raises(RuntimeError, match="synthetic_injected_failure"):
            await _admit(
                connection,
                campaign_id,
                retained_item,
                campaign_lease,
                item_lease,
                produced_artifact,
            )
        assert await _registry_counts(connection) == (0, 0, 0)
        item_state = await connection.fetchrow(
            f"""SELECT status, artifact_sha256, layout_sha256
                   FROM {database_table('provider_directory_retained_artifact_campaign_item')}
                  WHERE campaign_id=$1 AND source_item_id=$2""",
            campaign_id,
            retained_item.source_item_id,
        )
        assert dict(item_state) == {
            "status": "expected",
            "artifact_sha256": None,
            "layout_sha256": None,
        }
