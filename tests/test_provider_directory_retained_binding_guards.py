# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncpg
import pytest

from process.provider_directory_retained_artifact_contract import (
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
)
from process.provider_directory_retained_seal_store import (
    seal_retained_artifact_campaign,
)
from process.provider_directory_retained_store_support import database_table
from tests.provider_directory_retained_core_postgres_support import (
    admit_campaign_item,
    campaign_item,
    digest,
    fixed_campaign_plan,
    registry_artifact,
    retained_database,
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
        owner=f"owner-{label}",
    )
    return retained_item, campaign_id, campaign_lease


async def _sealed_fixed_root(connection, label: str):
    retained_item, campaign_id, campaign_lease = await _fixed_root(connection, label)
    produced_artifact = registry_artifact(label, retained_item.artifact_kind)
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
    assert sealed_summary["complete"] is True
    return campaign_id


_PLANNED_IDENTITY_MUTATIONS = (
    ("campaign_id", digest("changed-campaign")),
    ("source_item_id", digest("changed-item")),
    ("campaign_ordinal", 1),
    ("source_entry_sha256", digest("changed-entry")),
    ("artifact_kind", "provider_file"),
    ("family", "changed-family"),
    ("collection_kind", "changed-collection"),
    ("partition_metadata_sha256", digest("changed-partition")),
    ("stream_identity_sha256", digest("changed-stream")),
    ("sequence_ordinal", 1),
    ("item_role", "terminal_zero"),
    ("declared_byte_count", 1),
    ("terminal_proof_sha256", digest("changed-terminal")),
    ("planned_identity_sha256", digest("changed-plan")),
)


async def _assert_planned_identity_guard(connection, campaign_id, source_item_id):
    item_table = database_table("provider_directory_retained_artifact_campaign_item")
    for field_name, changed_value in _PLANNED_IDENTITY_MUTATIONS:
        with pytest.raises(
            asyncpg.CheckViolationError,
            match="retained_item_planned_identity_immutable",
        ):
            async with connection.transaction():
                await connection.execute(
                    f"UPDATE {item_table} SET {field_name}=$3 "
                    "WHERE campaign_id=$1 AND source_item_id=$2",
                    campaign_id,
                    source_item_id,
                    changed_value,
                )


async def _tamper_item_family(connection, campaign_id):
    item_table = database_table("provider_directory_retained_artifact_campaign_item")
    await connection.execute(
        f"ALTER TABLE {item_table} "
        "DISABLE TRIGGER pd_retained_item_identity_immutable"
    )
    try:
        await connection.execute(
            f"UPDATE {item_table} " "SET family='changed-family' WHERE campaign_id=$1",
            campaign_id,
        )
    finally:
        await connection.execute(
            f"ALTER TABLE {item_table} "
            "ENABLE TRIGGER pd_retained_item_identity_immutable"
        )


async def _assert_seal_rejects_identity_tamper(connection):
    retained_item, campaign_id, campaign_lease = await _fixed_root(
        connection,
        "planned-identity-guard",
    )
    await _assert_planned_identity_guard(
        connection,
        campaign_id,
        retained_item.source_item_id,
    )
    await _tamper_item_family(connection, campaign_id)
    with pytest.raises(RetainedCampaignMismatch, match="planned_identity"):
        await seal_retained_artifact_campaign(
            connection,
            campaign_id=campaign_id,
            campaign_lease=campaign_lease,
        )


async def _assert_claim_rejects_identity_tamper(connection):
    sealed_id = await _sealed_fixed_root(connection, "planned-identity-claim")
    await _tamper_item_family(connection, sealed_id)
    with pytest.raises(RetainedCampaignMismatch, match="planned_identity"):
        await claim_sealed_retained_campaign(
            connection,
            campaign_id=sealed_id,
            consumer_recipe_id="planned_identity_profile_v1",
        )


@pytest.mark.asyncio
async def test_planned_item_identity_is_db_immutable_and_seal_claim_verified(
    monkeypatch,
) -> None:
    """Planned identity is immutable and rechecked at seal and claim boundaries."""
    async with retained_database(monkeypatch) as (connection, _schema_name):
        await _assert_seal_rejects_identity_tamper(connection)
        await _assert_claim_rejects_identity_tamper(connection)


async def _admitted_layout_pair(
    connection,
    *,
    plan_label,
    owner,
    first_label,
    second_label,
):
    first_item = campaign_item(first_label)
    second_item = campaign_item(second_label)
    campaign_id = await initialize_retained_artifact_campaign(
        connection,
        plan=fixed_campaign_plan(plan_label, (first_item, second_item)),
    )
    campaign_lease = await acquire_campaign_lease(
        connection,
        campaign_id=campaign_id,
        owner=owner,
    )
    await admit_campaign_item(
        connection,
        campaign_id,
        first_item,
        registry_artifact(first_label, first_item.artifact_kind),
    )
    second_layout = await admit_campaign_item(
        connection,
        campaign_id,
        second_item,
        registry_artifact(second_label, second_item.artifact_kind),
    )
    return first_item, campaign_id, campaign_lease, second_layout


async def _replace_first_layout(connection, campaign_id, first_item, layout):
    item_table = database_table("provider_directory_retained_artifact_campaign_item")
    await connection.execute(
        f"UPDATE {item_table} SET layout_sha256=$3 "
        "WHERE campaign_id=$1 AND source_item_id=$2",
        campaign_id,
        first_item.source_item_id,
        layout,
    )


async def _assert_layout_pair_seal_binding(connection):
    first_item, campaign_id, campaign_lease, second_layout = (
        await _admitted_layout_pair(
            connection,
            plan_label="layout-pair-seal",
            owner="layout-pair-seal-owner",
            first_label="layout-pair-first",
            second_label="layout-pair-second",
        )
    )
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await _replace_first_layout(
            connection,
            campaign_id,
            first_item,
            second_layout,
        )
    item_table = database_table("provider_directory_retained_artifact_campaign_item")
    await connection.execute(
        f"ALTER TABLE {item_table} "
        "DROP CONSTRAINT pd_retained_item_layout_artifact_fkey"
    )
    await _replace_first_layout(connection, campaign_id, first_item, second_layout)
    with pytest.raises(RetainedCampaignMismatch, match="layout_artifact"):
        await seal_retained_artifact_campaign(
            connection,
            campaign_id=campaign_id,
            campaign_lease=campaign_lease,
        )


async def _assert_layout_pair_claim_binding(connection):
    first_item, campaign_id, campaign_lease, second_layout = (
        await _admitted_layout_pair(
            connection,
            plan_label="layout-pair-claim",
            owner="layout-pair-claim-owner",
            first_label="layout-claim-first",
            second_label="layout-claim-second",
        )
    )
    sealed_summary = await seal_retained_artifact_campaign(
        connection,
        campaign_id=campaign_id,
        campaign_lease=campaign_lease,
    )
    assert sealed_summary["complete"] is True
    await _replace_first_layout(connection, campaign_id, first_item, second_layout)
    with pytest.raises(RetainedCampaignMismatch, match="layout_artifact"):
        await claim_sealed_retained_campaign(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id="layout_pair_profile_v1",
        )


@pytest.mark.asyncio
async def test_layout_artifact_pair_is_db_seal_and_claim_bound(monkeypatch) -> None:
    """Layout and artifact pairs remain bound in the DB, seal, and claim paths."""
    async with retained_database(monkeypatch) as (connection, _schema_name):
        await _assert_layout_pair_seal_binding(connection)
        await _assert_layout_pair_claim_binding(connection)
