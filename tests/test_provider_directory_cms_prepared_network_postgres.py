# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine native network encoding and private SQL; no canonical publication."""

import hashlib
import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from db.plan_release_storage import storage_relation
from process import provider_directory_cms_identity_candidate as common
from process import provider_directory_cms_prepared_identity as identity
from process import provider_directory_cms_prepared_network as network
from process import provider_directory_generic_source_policy as source
from tests.test_provider_directory_cms_prepared_identity_postgres import (
    identity_workspace,
    native_case,
    native_workspace,
    observe,
    private_count,
    publisher,
    scope,
)

STAMP = datetime(2026, 1, 1, tzinfo=timezone.utc)
ORGANIZATION = {"resourceType": "Organization", "id": "network-one", "type": [{"text": "ntwk"}]}
PAYER = {"resourceType": "Organization", "id": "payer-one", "name": "Example Payer"}
PLAN = {
    "resourceType": "InsurancePlan",
    "id": "plan-one",
    "network": [{"reference": "Organization/network-one"}],
    "ownedBy": {"reference": "Organization/payer-one"},
    "administeredBy": {"reference": "Organization/payer-one"},
}


async def canonical_snapshot(case):
    values = []
    for root in (*common.CMS_IDENTITY_TABLES, identity.RESOURCE_IDENTITY_ROOT):
        relation = storage_relation(case.storage.catalog_schema, root)
        values.append(
            await case.admin.fetchval(
                f"SELECT COALESCE(jsonb_agg(to_jsonb(entry) ORDER BY to_jsonb(entry)::text),'[]'::jsonb) "
                f"FROM {relation} entry"
            )
        )
    return values


async def temp_count(driver):
    return await driver.fetchval("SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema()")


async def seed_network_incumbent(case):
    """Retain the accepted organization/network identities and exact plan payload."""
    network_id, organization_id = uuid4(), uuid4()
    release = case.handoff.document["identity"]["metadata"]["source_release"]["vector_sha256"]
    await case.admin.execute(
        f"INSERT INTO {storage_relation(case.storage.catalog_schema, common.CMS_IDENTITY_TABLES[0])} "
        "VALUES('cms-npd','Organization','network-one',$1,NULL,$2)",
        organization_id,
        STAMP,
    )
    await case.admin.execute(
        f"INSERT INTO {storage_relation(case.storage.catalog_schema, common.CMS_IDENTITY_TABLES[2])} "
        "VALUES('cms-npd','Organization','network-one',$1,$2)",
        network_id,
        STAMP,
    )
    plan_json = json.dumps(PLAN, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    await case.admin.execute(
        f"INSERT INTO {storage_relation(case.storage.catalog_schema, common.CMS_IDENTITY_TABLES[3])} "
        "VALUES('cms-npd',$1,'Organization','network-one','plan-one',$2,$3,$3,$4,$5,$6)",
        release,
        json.dumps(["Organization/network-one"]),
        "Organization/payer-one",
        hashlib.sha256(plan_json.encode()).hexdigest(),
        plan_json,
        STAMP,
    )
    return network_id, organization_id


async def test_native_prepared_network_uses_encoder_reuses_ids_and_preserves_canonical(identity_workspace):
    case = identity_workspace
    network_id, organization_id = await seed_network_incumbent(case)
    before = await canonical_snapshot(case)
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            actual = await identity.stage_prepared_entity_batch(current, resources=[ORGANIZATION, PAYER, ORGANIZATION])
            assert actual[0] == actual[2] == organization_id and actual[1] != organization_id
            await identity.stage_prepared_resource_identity_batch(
                current, resource_type="InsurancePlan", resource_ids=["plan-one", "plan-one"]
            )
            organizations = await network.record_prepared_network_batch(
                current, resource_type="Organization", resources=[ORGANIZATION, PAYER, ORGANIZATION]
            )
            assert organizations["input_count"] == 3 and organizations["duplicate_count"] == 1
            assert organizations["organization_count"] == 1 and organizations["new_identity_count"] == 0
            plans = await network.record_prepared_network_batch(
                current, resource_type="InsurancePlan", resources=[PLAN, PLAN]
            )
            assert plans["plan_link_count"] == plans["new_plan_evidence_count"] == 1
            binding = storage_relation(current.policy.schema, current.receipts[common.CMS_IDENTITY_TABLES[2]]["name"])
            assert dict(await driver.fetchrow(f"SELECT network_id,created_at FROM {binding}")) == {
                "network_id": network_id,
                "created_at": STAMP,
            }
            evidence = storage_relation(current.policy.schema, current.receipts[common.CMS_IDENTITY_TABLES[3]]["name"])
            evidence_by_field = dict(
                await driver.fetchrow(f"SELECT owned_by_ref,administered_by_ref,observed_at FROM {evidence}")
            )
            assert evidence_by_field == {
                "owned_by_ref": "Organization/payer-one",
                "administered_by_ref": "Organization/payer-one",
                "observed_at": STAMP,
            }
            replay = await network.record_prepared_network_batch(
                current, resource_type="InsurancePlan", resources=[PLAN]
            )
            assert replay["new_identity_count"] == replay["new_plan_evidence_count"] == 0
            assert await private_count(current, common.CMS_IDENTITY_TABLES[2]) == 1
            assert await private_count(current, common.CMS_IDENTITY_TABLES[3]) == 1
            assert await temp_count(driver) == 0
            assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            await current.finish_indexes()
            with pytest.raises(ValueError, match="already_indexed"):
                await network.record_prepared_network_batch(current, resource_type="InsurancePlan", resources=[PLAN])
    assert await canonical_snapshot(case) == before


async def test_native_prepared_network_payload_conflict_rolls_back_temp_and_private_writes(identity_workspace):
    case = identity_workspace
    before = await canonical_snapshot(case)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await identity.stage_prepared_entity_batch(current, resources=[ORGANIZATION, PAYER])
            await network.record_prepared_network_batch(current, resource_type="Organization", resources=[ORGANIZATION])
            await network.record_prepared_network_batch(current, resource_type="InsurancePlan", resources=[PLAN])
            with pytest.raises(ValueError, match="release_plan_conflict"):
                await network.record_prepared_network_batch(
                    current, resource_type="InsurancePlan", resources=[{**PLAN, "name": "Changed"}]
                )
            assert await temp_count(driver) == 0
            assert await private_count(current, common.CMS_IDENTITY_TABLES[2]) == 1
            assert await private_count(current, common.CMS_IDENTITY_TABLES[3]) == 1
            replay = await network.record_prepared_network_batch(
                current, resource_type="InsurancePlan", resources=[PLAN]
            )
            assert replay["new_plan_evidence_count"] == 0
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
    assert await canonical_snapshot(case) == before
