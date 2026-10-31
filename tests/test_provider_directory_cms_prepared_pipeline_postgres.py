# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""All-eight-family private recipe with real native identity/network/graph work."""

import asyncio

import pytest

from db.plan_release_storage import storage_relation
from process import provider_directory_cms_prepared_identity_stream as identity
from process import provider_directory_cms_prepared_relationships as relationships
from process import provider_directory_generic_source_policy as source
from process import provider_directory_generic_source_storage as storage
from tests.test_provider_directory_cms_prepared_relationships_postgres import (
    input_snapshots,
    native_case,
    native_frozen_case,
    observe,
    publisher,
    scope,
)


async def output_counts(current):
    return {
        root: await current.connection.fetchval(
            f"SELECT count(*) FROM {storage_relation(current.policy.schema, receipt['name'])}"
        )
        for root, receipt in current.receipts.items()
    }


async def canonical_counts(case):
    return [
        await case.admin.fetchval(f"SELECT count(*) FROM {storage_relation(case.storage.catalog_schema, root)}")
        for root in storage.AUXILIARY_ROOTS
    ]


async def test_native_frozen_all8_to_private_identities_network_and_exact_graph(native_frozen_case):
    case = native_frozen_case
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    before = await input_snapshots(case)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            staged = await identity.stage_prepared_identities(current, case.handoff)
            assert staged["resource_counts"] == {family: len(case.raw[family]) for family in identity._FAMILIES}
            counts = await output_counts(current)
            assert [counts[root] for root in storage.CMS_IDENTITY_ROOTS] == [3, 3, 1, 1]
            assert counts[storage.CMS_RESOURCE_IDENTITY_ROOT] == 2
            evidence = storage_relation(current.policy.schema, current.receipts[storage.CMS_IDENTITY_ROOTS[3]]["name"])
            assert await driver.fetchval(f"SELECT owned_by_ref FROM {evidence}") == "Organization/payer-one"
            replay = await identity.stage_prepared_identities(current, case.handoff)
            assert replay == staged and await output_counts(current) == counts
            projected = await relationships.project_prepared_cms_relationships(current, case.handoff)
            assert projected["relationship_count"] > 20
            await current.finish_indexes()
            verified = await relationships.assert_prepared_cms_relationships(current, case.handoff)
            assert verified["relationship_count"] == projected["relationship_count"]
            assert verified == await relationships.assert_prepared_cms_relationships(current, case.handoff)
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient
            assert await input_snapshots(case) == before
    assert await canonical_counts(case) == [0] * 5


@pytest.mark.parametrize("failure", ["batch", "cancel"])
async def test_native_incomplete_identity_recipe_rolls_back_all_private_families_and_retries(
    native_frozen_case, monkeypatch, failure
):
    case = native_frozen_case
    before = await input_snapshots(case)
    original = identity._stage_batch
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:

            async def fail_after_prior_batches(workspace, family, resources):
                if family == "PractitionerRole":
                    raise asyncio.CancelledError() if failure == "cancel" else RuntimeError("synthetic recipe failure")
                return await original(workspace, family, resources)

            with monkeypatch.context() as patch:
                patch.setattr(identity, "_stage_batch", fail_after_prior_batches)
                with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
                    await identity.stage_prepared_identities(current, case.handoff)
            assert all(count == 0 for count in (await output_counts(current)).values())
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert await identity.stage_prepared_identities(current, case.handoff)
            assert await input_snapshots(case) == before
    assert await canonical_counts(case) == [0] * 5
