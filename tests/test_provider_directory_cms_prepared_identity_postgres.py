# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native prepared identity COPY, replay and rollback without canonical writes."""

from datetime import datetime, timezone
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from db.plan_release_storage import storage_relation
from process import provider_directory_cms_identity_candidate as common
from process import provider_directory_cms_prepared_identity as prepared
from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_generic_source_policy as source
from process.provider_directory_resource_identity import source_resource_uuid
from tests.test_provider_directory_cms_prepared_context_postgres import native_case, observe, publisher
from tests.test_provider_directory_cms_prepared_workspace_postgres import native_workspace, scope

STAMP = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest_asyncio.fixture
async def identity_workspace(native_workspace):
    """Real private workspace with five admin-seeded, publisher-read-only maps."""
    case = native_workspace
    layouts = workspace._layouts()
    for root in (*common.CMS_IDENTITY_TABLES, prepared.RESOURCE_IDENTITY_ROOT):
        definitions, _, checks = layouts[root]
        fields = [*definitions, *(f"CHECK ({check})" for check in checks)]
        relation = storage_relation(case.storage.catalog_schema, root)
        await case.admin.execute(f"CREATE TABLE {relation} ({','.join(fields)})")
        await case.admin.execute(f'GRANT SELECT ON {relation} TO "{case.policy.publisher_role}"')
        assert not await case.admin.fetchval(
            "SELECT has_table_privilege($1,$2,'INSERT,UPDATE,DELETE,TRUNCATE')", case.policy.publisher_role, relation
        )
    return case


async def private_count(current, root):
    receipt = current.receipts[root]
    return await current.connection.fetchval(
        f"SELECT count(*) FROM {storage_relation(current.policy.schema, receipt['name'])}"
    )


@pytest.mark.parametrize("kind", ["Organization", "Location"])
async def test_native_entity_opaque_uuid_timestamp_raw_hash_and_duplicate_order(identity_workspace, kind):
    case = identity_workspace
    entity_id = uuid4()
    resource_by_field = {"resourceType": kind, "id": "existing", "name": "Example é"}
    release = case.handoff.document["identity"]["metadata"]["source_release"]["vector_sha256"]
    _, _, payloads = prepared._validated_entity_batch("cms-npd", release, [resource_by_field])
    await case.admin.execute(
        f"INSERT INTO {storage_relation(case.storage.catalog_schema, common.CMS_IDENTITY_TABLES[0])} "
        "VALUES($1,$2,$3,$4,$5,$6)",
        "cms-npd",
        kind,
        "existing",
        entity_id if kind == "Organization" else None,
        entity_id if kind == "Location" else None,
        STAMP,
    )
    evidence = common._entity_evidence_record("cms-npd", kind, "existing", release, *payloads["existing"], STAMP)
    await case.admin.execute(
        f"INSERT INTO {storage_relation(case.storage.catalog_schema, common.CMS_IDENTITY_TABLES[1])} "
        "VALUES($1,$2,$3,$4,$5,$6,$7)",
        *evidence,
    )
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            actual = await prepared.stage_prepared_entity_batch(
                current, resources=[resource_by_field, resource_by_field]
            )
            assert actual == [entity_id, entity_id]
            assert await prepared.stage_prepared_entity_batch(current, resources=[resource_by_field]) == [entity_id]
            for root, timestamp in zip(common.CMS_IDENTITY_TABLES[:2], ("created_at", "observed_at"), strict=True):
                relation = storage_relation(current.policy.schema, current.receipts[root]["name"])
                assert await private_count(current, root) == 1
                assert await driver.fetchval(f"SELECT {timestamp} FROM {relation}") == STAMP
            relation = storage_relation(current.policy.schema, current.receipts[common.CMS_IDENTITY_TABLES[1]]["name"])
            assert await driver.fetchval(f"SELECT payload_sha256 FROM {relation}") == evidence[4]
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient


@pytest.mark.parametrize("kind", ["InsurancePlan", "PractitionerRole"])
async def test_native_resource_uuid5_replay_and_canonical_timestamp(identity_workspace, kind):
    case = identity_workspace
    entity_id = source_resource_uuid("cms-npd", kind, "existing")
    root = prepared.RESOURCE_IDENTITY_ROOT
    await case.admin.execute(
        f"INSERT INTO {storage_relation(case.storage.catalog_schema, root)} VALUES($1,$2,$3,$4,$5)",
        "cms-npd",
        kind,
        "existing",
        entity_id,
        STAMP,
    )
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            ids = ["new", "existing", "new"]
            expected_ids = [source_resource_uuid("cms-npd", kind, key) for key in ids]
            assert (
                await prepared.stage_prepared_resource_identity_batch(current, resource_type=kind, resource_ids=ids)
                == expected_ids
            )
            assert (
                await prepared.stage_prepared_resource_identity_batch(current, resource_type=kind, resource_ids=ids)
                == expected_ids
            )
            assert await private_count(current, root) == 2
            relation = storage_relation(current.policy.schema, current.receipts[root]["name"])
            assert await driver.fetchval(f"SELECT created_at FROM {relation} WHERE resource_id='existing'") == STAMP
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
    assert await case.admin.fetchval(f"SELECT count(*) FROM {storage_relation(case.storage.catalog_schema, root)}") == 1


async def test_native_second_copy_failure_rolls_back_binding_and_allows_retry(identity_workspace, monkeypatch):
    case = identity_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            original = current.copy_rows

            async def duplicate_evidence(root, columns, records):
                return await original(root, columns, records * 2 if root == common.CMS_IDENTITY_TABLES[1] else records)

            with monkeypatch.context() as patch:
                patch.setattr(current, "copy_rows", duplicate_evidence)
                with pytest.raises(asyncpg.UniqueViolationError):
                    await prepared.stage_prepared_entity_batch(
                        current, resources=[{"resourceType": "Organization", "id": "one"}]
                    )
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert all([await private_count(current, root) == 0 for root in common.CMS_IDENTITY_TABLES[:2]])
            assert (
                len(
                    await prepared.stage_prepared_entity_batch(
                        current, resources=[{"resourceType": "Organization", "id": "one"}]
                    )
                )
                == 1
            )
            assert all([await private_count(current, root) == 1 for root in common.CMS_IDENTITY_TABLES[:2]])


async def test_native_conflicting_same_release_is_immutable(identity_workspace):
    case = identity_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            payload = {"resourceType": "Location", "id": "one", "name": "original"}
            await prepared.stage_prepared_entity_batch(current, resources=[payload])
            with pytest.raises(common.GenericCandidateError, match="release_payload_conflict"):
                await prepared.stage_prepared_entity_batch(current, resources=[{**payload, "name": "changed"}])
            assert all([await private_count(current, root) == 1 for root in common.CMS_IDENTITY_TABLES[:2]])
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role


async def test_native_canonical_uuid_conflict_is_not_repaired(identity_workspace):
    case = identity_workspace
    root, wrong_id = prepared.RESOURCE_IDENTITY_ROOT, uuid4()
    await case.admin.execute(
        f"INSERT INTO {storage_relation(case.storage.catalog_schema, root)} VALUES($1,$2,$3,$4,$5)",
        "cms-npd",
        "InsurancePlan",
        "one",
        wrong_id,
        STAMP,
    )
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            with pytest.raises(common.CMSIdentityReprepareRequired, match="immutable_replay_conflict"):
                await prepared.stage_prepared_resource_identity_batch(
                    current, resource_type="InsurancePlan", resource_ids=["one"]
                )
            assert await private_count(current, root) == 0
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
    assert (
        await case.admin.fetchval(f"SELECT entity_id FROM {storage_relation(case.storage.catalog_schema, root)}")
        == wrong_id
    )
