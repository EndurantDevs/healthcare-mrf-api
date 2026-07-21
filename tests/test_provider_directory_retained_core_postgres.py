# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace

import pytest

from process.provider_directory_retained_artifact_contract import (
    TERMINAL_ZERO,
    RetainedArtifactError,
    RetainedCampaignMismatch,
)
from process.provider_directory_retained_artifact_keys import (
    LOCATOR_PRIVATE_VALUE,
    _consume_private_value,
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
    release_campaign_lease,
)
from process.provider_directory_retained_private_store import private_item_locator
from process.provider_directory_retained_seal_store import (
    seal_retained_artifact_campaign,
    set_campaign_disk_reservation,
)
from process.provider_directory_retained_status_store import (
    retained_artifact_campaign_status,
)
from process.provider_directory_retained_store_support import database_table
from process.provider_directory_retained_stream_store import (
    append_ordered_stream_item,
    list_campaign_items,
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


async def _sealed_fixed_campaign(connection):
    retained_item = campaign_item("fixed-happy")
    campaign_plan = fixed_campaign_plan("fixed-happy", (retained_item,))
    campaign_id = await initialize_retained_artifact_campaign(
        connection, plan=campaign_plan
    )
    assert (
        await initialize_retained_artifact_campaign(connection, plan=campaign_plan)
        == campaign_id
    )
    campaign_lease = await acquire_campaign_lease(
        connection, campaign_id=campaign_id, owner="fixed-owner"
    )
    locator = await private_item_locator(
        connection,
        campaign_id=campaign_id,
        source_item_id=retained_item.source_item_id,
    )
    assert (
        _consume_private_value(
            locator,
            purpose=LOCATOR_PRIVATE_VALUE,
        )
        == f"fixture://{retained_item.source_item_id}"
    )
    produced_artifact = registry_artifact("fixed-happy", retained_item.artifact_kind)
    await admit_campaign_item(connection, campaign_id, retained_item, produced_artifact)
    await set_campaign_disk_reservation(
        connection,
        campaign_id=campaign_id,
        campaign_lease=campaign_lease,
        reserved_bytes=4096,
    )
    sealed_summary = await seal_retained_artifact_campaign(
        connection, campaign_id=campaign_id, campaign_lease=campaign_lease
    )
    assert sealed_summary["complete"] is True
    return retained_item, campaign_id, campaign_lease


async def _claim_report_and_release(connection, campaign_id: str) -> None:
    claimed_campaign = await claim_sealed_retained_campaign(
        connection,
        campaign_id=campaign_id,
        consumer_recipe_id="profile_fixture_v1",
    )
    await heartbeat_retained_campaign_consumer(
        connection,
        campaign_id=campaign_id,
        consumer_recipe_id="profile_fixture_v1",
        claimed_campaign_sha256=claimed_campaign.campaign_sha256,
        consumer_claim_generation=claimed_campaign.consumer_claim_generation,
    )
    status_response = await retained_artifact_campaign_status(
        connection, campaign_id=campaign_id
    )
    assert status_response["items"][0]["admitted_record_count"] == 1
    assert "locator_ciphertext" not in status_response["items"][0]
    assert claimed_campaign.adapter_id == "retained_core_fixture_v1"
    assert claimed_campaign.credential_descriptor_sha256 == digest(
        "credential:public-fixture"
    )
    for _release_attempt in range(2):
        await release_retained_campaign_consumer(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id="profile_fixture_v1",
            claimed_campaign_sha256=claimed_campaign.campaign_sha256,
            consumer_claim_generation=claimed_campaign.consumer_claim_generation,
        )


@pytest.mark.asyncio
async def test_consumer_heartbeat_rejects_missing_campaign(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        with pytest.raises(
            RetainedArtifactError,
            match="consumer_campaign_not_found",
        ):
            await heartbeat_retained_campaign_consumer(
                connection,
                campaign_id=digest("missing-consumer-campaign"),
                consumer_recipe_id="missing_campaign_profile_v1",
                claimed_campaign_sha256=digest("missing-claim"),
                consumer_claim_generation=1,
            )


@pytest.mark.asyncio
async def test_migration_adoption_index_is_exact_and_credential_free(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, schema_name):
        index_definition = await connection.fetchval(
            """SELECT indexdef FROM pg_indexes
                 WHERE schemaname=$1 AND indexname=$2""",
            schema_name,
            "pd_retained_item_adoption_idx",
        )
        expected_columns = (
            "artifact_kind",
            "acquisition_mode",
            "validator_kind",
            "validator_sha256",
            "immutable_identity_sha256",
            "observed_byte_count",
            "downloaded_artifact_sha256",
            "artifact_sha256",
            "layout_sha256",
        )
        assert index_definition is not None
        assert all(column_name in index_definition for column_name in expected_columns)
        assert "locator" not in index_definition
        assert "ciphertext" not in index_definition
        assert "admitted" in index_definition
        assert "producer_verified" in index_definition
        assert "producer_proof" in index_definition
        assert index_definition.count("immutable_identity_sha256") == 3


@pytest.mark.asyncio
async def test_migration_persists_exact_transport_learning_scope(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, schema_name):
        descriptor_column = await connection.fetchrow(
            """SELECT data_type, is_nullable
                 FROM information_schema.columns
                WHERE table_schema=$1 AND table_name=$2 AND column_name=$3""",
            schema_name,
            "provider_directory_retained_artifact_campaign",
            "credential_descriptor_sha256",
        )
        constraint_definition = await connection.fetchval(
            """SELECT pg_get_constraintdef(constraint_row.oid)
                 FROM pg_constraint AS constraint_row
                 JOIN pg_namespace AS namespace_row
                   ON namespace_row.oid=constraint_row.connamespace
                WHERE namespace_row.nspname=$1 AND constraint_row.conname=$2""",
            schema_name,
            "pd_retained_campaign_source_census_key",
        )
        assert dict(descriptor_column) == {
            "data_type": "character varying",
            "is_nullable": "NO",
        }
        expected_scope = (
            "contract_id, adapter_id, endpoint_id, "
            "credential_descriptor_sha256, source_census_sha256, "
            "per_item_byte_budget, aggregate_byte_budget"
        )
        assert expected_scope in constraint_definition


@pytest.mark.asyncio
async def test_credential_scopes_are_distinct_but_share_request_fence(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("credential-scope")
        public_plan = fixed_campaign_plan("credential-scope", (retained_item,))
        private_plan = replace(
            public_plan,
            credential_descriptor_sha256=digest("credential:private-fixture"),
        ).validate()
        public_id = await initialize_retained_artifact_campaign(
            connection, plan=public_plan
        )
        private_id = await initialize_retained_artifact_campaign(
            connection, plan=private_plan
        )
        assert public_id != private_id
        assert public_plan.request_fence_id == private_plan.request_fence_id
        assert (
            public_plan.transport_experience_scope_sha256
            != private_plan.transport_experience_scope_sha256
        )


@pytest.mark.asyncio
async def test_corrected_budgets_create_a_fresh_retryable_campaign(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("budget-retry")
        undersized_plan = fixed_campaign_plan("budget-retry", (retained_item,))
        corrected_plan = replace(
            undersized_plan,
            per_item_byte_budget=2048,
            aggregate_byte_budget=16384,
        ).validate()
        undersized_id = await initialize_retained_artifact_campaign(
            connection,
            plan=undersized_plan,
        )
        corrected_id = await initialize_retained_artifact_campaign(
            connection,
            plan=corrected_plan,
        )
        assert corrected_id != undersized_id
        assert corrected_plan.request_fence_id == undersized_plan.request_fence_id


@pytest.mark.asyncio
async def test_fixed_campaign_seals_claims_reports_and_releases(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        _retained_item, campaign_id, campaign_lease = await _sealed_fixed_campaign(
            connection
        )
        await _claim_report_and_release(connection, campaign_id)
        await release_campaign_lease(
            connection, campaign_id=campaign_id, lease=campaign_lease
        )


@pytest.mark.asyncio
async def test_released_recipe_reclaims_with_a_new_fenced_generation(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        _item, campaign_id, _lease = await _sealed_fixed_campaign(connection)
        first_claim = await claim_sealed_retained_campaign(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id="reclaimable_profile_v1",
        )
        await release_retained_campaign_consumer(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id="reclaimable_profile_v1",
            claimed_campaign_sha256=first_claim.campaign_sha256,
            consumer_claim_generation=first_claim.consumer_claim_generation,
        )
        second_claim = await claim_sealed_retained_campaign(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id="reclaimable_profile_v1",
        )
        assert second_claim.consumer_claim_generation == (
            first_claim.consumer_claim_generation + 1
        )
        with pytest.raises(RetainedCampaignMismatch, match="consumer_claim_identity"):
            await heartbeat_retained_campaign_consumer(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id="reclaimable_profile_v1",
                claimed_campaign_sha256=first_claim.campaign_sha256,
                consumer_claim_generation=first_claim.consumer_claim_generation,
            )
        await heartbeat_retained_campaign_consumer(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id="reclaimable_profile_v1",
            claimed_campaign_sha256=second_claim.campaign_sha256,
            consumer_claim_generation=second_claim.consumer_claim_generation,
        )
        reference = await connection.fetchrow(
            f"""SELECT claim_generation, released_at
                   FROM {database_table('provider_directory_retained_artifact_consumer_reference')}
                  WHERE campaign_id=$1 AND consumer_recipe_id=$2""",
            campaign_id,
            "reclaimable_profile_v1",
        )
        assert dict(reference) == {
            "claim_generation": second_claim.consumer_claim_generation,
            "released_at": None,
        }


@pytest.mark.asyncio
async def test_ordered_stream_appends_terminal_proof_and_claims(monkeypatch) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        stream_identity = digest("ordered-happy-stream")
        campaign_plan = ordered_campaign_plan("ordered-happy", stream_identity)
        campaign_id = await initialize_retained_artifact_campaign(
            connection, plan=campaign_plan
        )
        campaign_lease = await acquire_campaign_lease(
            connection, campaign_id=campaign_id, owner="ordered-owner"
        )
        page_member = campaign_item(
            "ordered-page", stream_identity=stream_identity, sequence_ordinal=0
        )
        terminal_member = campaign_item(
            "ordered-terminal",
            stream_identity=stream_identity,
            sequence_ordinal=1,
            item_role=TERMINAL_ZERO,
        )
        assert (
            await append_ordered_stream_item(
                connection,
                campaign_id=campaign_id,
                campaign_lease=campaign_lease,
                item=page_member,
            )
            == 0
        )
        assert (
            await append_ordered_stream_item(
                connection,
                campaign_id=campaign_id,
                campaign_lease=campaign_lease,
                item=terminal_member,
            )
            == 1
        )
        produced_artifact = registry_artifact("ordered-page", page_member.artifact_kind)
        await admit_campaign_item(
            connection, campaign_id, page_member, produced_artifact
        )
        sealed_summary = await seal_retained_artifact_campaign(
            connection, campaign_id=campaign_id, campaign_lease=campaign_lease
        )
        assert sealed_summary["terminal_stream_count"] == 1
        claimed_campaign = await claim_sealed_retained_campaign(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id="ordered_profile_fixture_v1",
        )
        assert len(claimed_campaign.terminal_streams) == 1
        assert len(claimed_campaign.artifacts) == 1
        assert len(await list_campaign_items(connection, campaign_id=campaign_id)) == 2
