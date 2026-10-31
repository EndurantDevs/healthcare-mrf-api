# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native five-map identity parity against genuine all-family frozen inputs."""

from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio

from db.plan_release_storage import storage_relation
from process import provider_directory_cms_prepared_identity_stream as stream
from process import provider_directory_cms_prepared_identity_validation as prepared
from process import provider_directory_generic_source_policy as source
from process.provider_directory_resource_identity import source_resource_uuid
from tests.test_provider_directory_cms_prepared_relationships_postgres import (
    input_snapshots,
    native_case,
    native_frozen_case,
    observe,
    publisher,
    scope,
)

MINT_ROOTS = (
    ("provider_directory_organization_identity", "organization_id", prepared.entities.CMS_IDENTITY_TABLES[0]),
    ("provider_directory_site_identity", "site_id", prepared.entities.CMS_IDENTITY_TABLES[0]),
    ("provider_directory_insurance_network_identity", "network_id", prepared.entities.CMS_IDENTITY_TABLES[2]),
)
IDENTITY_ROOTS = (*prepared.entities.CMS_IDENTITY_TABLES, prepared.resources.RESOURCE_IDENTITY_ROOT)


@pytest_asyncio.fixture
async def parity_case(native_frozen_case):
    """Add only SELECT-readable canonical UUID roots to the typed local fixture."""
    case = native_frozen_case
    for root, column, _ in MINT_ROOTS:
        relation = storage_relation(case.storage.catalog_schema, root)
        await case.admin.execute(f"CREATE TABLE {relation} ({column} uuid PRIMARY KEY,created_at timestamptz NOT NULL)")
        await case.admin.execute(f'GRANT SELECT ON {relation} TO "{case.policy.publisher_role}"')
        assert not await case.admin.fetchval(
            "SELECT has_table_privilege($1,$2,'INSERT,UPDATE,DELETE,TRUNCATE')", case.policy.publisher_role, relation
        )
    return case


def private_relations(current):
    return {root: storage_relation(current.policy.schema, current.receipts[root]["name"]) for root in IDENTITY_ROOTS}


async def snapshots(connection, relations):
    return [
        await connection.fetchval(
            f"SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb) FROM {relation} r"
        )
        for relation in relations
    ]


async def canonical_snapshots(case):
    return await snapshots(
        case.admin,
        [
            storage_relation(case.storage.catalog_schema, root)
            for root in (*IDENTITY_ROOTS, *(m[0] for m in MINT_ROOTS))
        ],
    )


async def assert_read_only_failure(current, handoff, message):
    before = await snapshots(current.connection, private_relations(current).values())
    with pytest.raises(ValueError, match=message):
        await prepared.assert_prepared_cms_identities(current, handoff)
    assert await snapshots(current.connection, private_relations(current).values()) == before
    assert await current.connection.fetchval("SELECT current_user") == current.policy.publisher_role


async def seed_canonical_maps(case, current):
    relations = private_relations(current)
    for root in IDENTITY_ROOTS:
        columns = prepared.entities.CMS_IDENTITY_COLUMNS.get(root, prepared.resources.RESOURCE_IDENTITY_COLUMNS)
        for row in await current.connection.fetch(f"SELECT {','.join(columns)} FROM {relations[root]}"):
            values = [
                prepared.storage._encoded(row[column])
                if column in prepared._JSON_COLUMNS and not isinstance(row[column], str)
                else row[column]
                for column in columns
            ]
            await case.admin.execute(
                f"INSERT INTO {storage_relation(case.storage.catalog_schema, root)} ({','.join(columns)}) "
                f"VALUES({','.join('$' + str(n) for n in range(1, len(columns) + 1))})",
                *values,
            )
    for root, column, source_root in MINT_ROOTS:
        rows = await current.connection.fetch(
            f"SELECT {column},created_at FROM {relations[source_root]} WHERE {column} IS NOT NULL"
        )
        for row in rows:
            await case.admin.execute(
                f"INSERT INTO {storage_relation(case.storage.catalog_schema, root)} VALUES($1,$2)", *row
            )


async def test_native_complete_identity5_is_read_only_replayable_and_closes_portals(parity_case):
    case = parity_case
    original = await canonical_snapshots(case)
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            staged = await stream.stage_prepared_identities(current, case.handoff)
            await current.finish_indexes()
            before = await snapshots(driver, private_relations(current).values())
            report = await prepared.assert_prepared_cms_identities(current, case.handoff)
            assert report["state"] == "verified" and "ready" not in report
            assert report["contract"] == prepared.CONTRACT
            assert list(report["row_counts"].values()) == [3, 3, 1, 1, 2]
            assert report["handoff_sha256"] == staged["handoff_sha256"]
            assert report["content_contract"] == prepared.CONTENT_CONTRACT
            assert len(report["content_sha256"]) == 64
            assert report == await prepared.assert_prepared_cms_identities(current, case.handoff)
            assert await snapshots(driver, private_relations(current).values()) == before
            assert await driver.fetchval("SELECT count(*) FROM pg_cursors WHERE name<>''") == 0
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient
            assert await input_snapshots(case) == case.input_snapshot
    assert await canonical_snapshots(case) == original


async def test_native_commitment_tracks_uuid_and_timestamp_across_batch_sizes(parity_case, monkeypatch):
    case = parity_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await stream.stage_prepared_identities(current, case.handoff)
            await current.finish_indexes()
            original = await prepared.assert_prepared_cms_identities(current, case.handoff)
            relations = private_relations(current)
            binding, evidence, networks, _, _ = relations.values()
            for query, arguments in (
                (f"UPDATE {binding} SET organization_id=$1 WHERE resource_id='payer-one'", (uuid4(),)),
                (f"UPDATE {networks} SET network_id=$1", (uuid4(),)),
                (f"UPDATE {evidence} SET observed_at=observed_at+interval '1 second' WHERE resource_id='site-one'", ()),
            ):
                transaction = driver.transaction()
                await transaction.start()
                try:
                    await driver.execute(query, *arguments)
                    changed = await prepared.assert_prepared_cms_identities(current, case.handoff)
                    assert changed["row_counts"] == original["row_counts"] and changed["tables"] == original["tables"]
                    assert changed["content_sha256"] != original["content_sha256"]
                    assert changed == await prepared.assert_prepared_cms_identities(current, case.handoff)
                finally:
                    await transaction.rollback()
            with monkeypatch.context() as patch:
                patch.setattr(prepared, "COPY_MAX_ROWS", 2)
                patch.setattr(prepared, "_PAGE_BYTES", 800)
                assert await prepared.assert_prepared_cms_identities(current, case.handoff) == original
            with monkeypatch.context() as patch:
                patch.setattr(prepared, "_ROW_BYTES", 50)
                await assert_read_only_failure(current, case.handoff, "commitment_row_unbounded")
            assert await driver.fetchval("SELECT count(*) FROM pg_cursors WHERE name<>''") == 0


async def test_native_full_accepted_map_replay_preserves_every_timestamp_and_uuid(parity_case):
    case = parity_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await stream.stage_prepared_identities(current, case.handoff)
            await current.finish_indexes()
            await seed_canonical_maps(case, current)
            before = await canonical_snapshots(case)
            assert (await prepared.assert_prepared_cms_identities(current, case.handoff))["state"] == "verified"
            assert await canonical_snapshots(case) == before
            assert await driver.fetchval("SELECT count(*) FROM pg_cursors WHERE name<>''") == 0


def _private_damage_cases(binding, evidence, networks, plans, resource, witness, raw):
    """Keep every original mutation, parameter and refusal in its original order."""
    return [
        (f"DELETE FROM {binding} WHERE resource_id='site-one'", (), "relationships_changed"),
        (f"DELETE FROM {evidence} WHERE resource_id='site-one'", (), "source_evidence_incomplete"),
        (
            f"INSERT INTO {binding} SELECT source_id,resource_type,'orphan',$1,NULL,created_at FROM {binding} WHERE resource_id='payer-one'",
            (uuid4(),),
            "source_evidence_incomplete",
        ),
        (
            f"INSERT INTO {evidence} SELECT source_id,resource_type,'orphan',release_id,payload_sha256,payload_json,observed_at FROM {evidence} WHERE resource_id='payer-one'",
            (),
            "relationships_changed",
        ),
        (
            f"UPDATE {evidence} SET payload_sha256=$1 WHERE resource_id='payer-one'",
            ("0" * 64,),
            "source_evidence_incomplete",
        ),
        (
            f"UPDATE {evidence} SET payload_json=payload_json || '{{\"name\":\"changed\"}}'::jsonb WHERE resource_id='payer-one'",
            (),
            "source_evidence_incomplete",
        ),
        (
            f"UPDATE {binding} SET organization_id=(SELECT organization_id FROM {binding} WHERE resource_id='payer-one') WHERE resource_id='network-one'",
            (),
            "canonical_identity_reuse_forbidden",
        ),
        (f"DELETE FROM {plans}", (), "network_evidence_incomplete"),
        (
            f"UPDATE {plans} SET network_refs='[\"Organization/other\"]'::jsonb",
            (),
            "network_evidence_incomplete",
        ),
        (f"UPDATE {plans} SET owned_by_ref='Organization/other'", (), "network_evidence_incomplete"),
        (f"UPDATE {plans} SET administered_by_ref='Organization/other'", (), "network_evidence_incomplete"),
        (f"UPDATE {plans} SET plan_payload_sha256=$1", ("0" * 64,), "network_evidence_incomplete"),
        (f"DELETE FROM {networks}", (), "relationships_changed"),
        (f"UPDATE {binding} SET source_id='other'", (), "source_coordinates_changed"),
        (f"UPDATE {evidence} SET release_id=$1", ("0" * 64,), "source_coordinates_changed"),
        (f"DELETE FROM {resource} WHERE resource_type='PractitionerRole'", (), "source_evidence_incomplete"),
        (
            f"UPDATE {resource} SET entity_id=$1 WHERE resource_type='InsurancePlan'",
            (uuid4(),),
            "identity_changed",
        ),
        (f"UPDATE {resource} SET source_id='other'", (), "source_evidence_incomplete"),
        (
            f"INSERT INTO {resource} SELECT source_id,resource_type,'extra',$1,created_at FROM {resource} WHERE resource_type='InsurancePlan'",
            (source_resource_uuid("cms-npd", "InsurancePlan", "extra"),),
            "source_evidence_incomplete",
        ),
        (f"UPDATE {witness} SET release_id=$1", ("0" * 64,), "frozen_source_coordinates_changed"),
        (f"UPDATE {witness} SET source_id='other'", (), "frozen_source_coordinates_changed"),
        (f"UPDATE {raw} SET dataset_id='other'", (), "frozen_source_coordinates_changed"),
    ]


async def test_native_private_map_and_raw_coordinate_damage_is_rejected_without_repair(parity_case):
    """Each damaged private input refuses read-only and rolls back before the next."""
    case = parity_case
    canonical = await canonical_snapshots(case)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await stream.stage_prepared_identities(current, case.handoff)
            await current.finish_indexes()
            binding, evidence, networks, plans, resource = private_relations(current).values()
            witness = storage_relation(
                current.policy.schema, case.handoff.document["tables"][prepared.storage.WITNESS]["name"]
            )
            raw = storage_relation(
                current.policy.schema, case.handoff.document["tables"][prepared.storage.RESOURCE]["name"]
            )
            mutations = _private_damage_cases(binding, evidence, networks, plans, resource, witness, raw)
            for query, arguments, message in mutations:
                transaction = driver.transaction()
                await transaction.start()
                try:
                    await driver.execute(query, *arguments)
                    await assert_read_only_failure(current, case.handoff, message)
                finally:
                    await transaction.rollback()
            assert (await prepared.assert_prepared_cms_identities(current, case.handoff))["state"] == "verified"
    assert await canonical_snapshots(case) == canonical


async def test_native_canonical_uuid_custody_timestamp_and_reverse_collision_damage(parity_case):
    case = parity_case
    original = await canonical_snapshots(case)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await stream.stage_prepared_identities(current, case.handoff)
            await current.finish_indexes()
            relations = private_relations(current)
            for root, column, source_root in MINT_ROOTS:
                mint_row = await driver.fetchrow(
                    f"SELECT {column},created_at FROM {relations[source_root]} WHERE {column} IS NOT NULL LIMIT 1"
                )
                target_relation = storage_relation(current.catalog_schema, root)
                await case.admin.execute(f"INSERT INTO {target_relation} VALUES($1,$2)", *mint_row)
                try:
                    before = await canonical_snapshots(case)
                    await assert_read_only_failure(current, case.handoff, "canonical_identity_reuse_forbidden")
                    assert await canonical_snapshots(case) == before
                finally:
                    await case.admin.execute(f"DELETE FROM {target_relation} WHERE {column}=$1", mint_row[column])
            resource_root = prepared.resources.RESOURCE_IDENTITY_ROOT
            resource_by_field = dict(
                await driver.fetchrow(f"SELECT * FROM {relations[resource_root]} WHERE resource_type='InsurancePlan'")
            )
            target_relation = storage_relation(current.catalog_schema, resource_root)
            columns = prepared.resources.RESOURCE_IDENTITY_COLUMNS
            for change, message in (
                ({"entity_id": uuid4()}, "immutable_replay_conflict"),
                ({"created_at": resource_by_field["created_at"] - timedelta(seconds=1)}, "resource_replay_conflict"),
                ({"resource_id": "other-key"}, "resource_replay_conflict"),
            ):
                damaged_by_field = {**resource_by_field, **change}
                await case.admin.execute(
                    f"INSERT INTO {target_relation} VALUES($1,$2,$3,$4,$5)",
                    *(damaged_by_field[column_name] for column_name in columns),
                )
                try:
                    before = await canonical_snapshots(case)
                    await assert_read_only_failure(current, case.handoff, message)
                    assert await canonical_snapshots(case) == before
                finally:
                    await case.admin.execute(
                        f"DELETE FROM {target_relation} WHERE resource_id=$1", damaged_by_field["resource_id"]
                    )
            assert (await prepared.assert_prepared_cms_identities(current, case.handoff))["state"] == "verified"
    assert await canonical_snapshots(case) == original
