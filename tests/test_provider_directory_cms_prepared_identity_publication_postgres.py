# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native identity-copy semantics; disposable fixtures issue no CMS authority."""

import asyncio
import json
from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_identity_publication as publication
from process import provider_directory_generic_source_policy as source
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_generic_source_policy import _savepoint
from tests.test_provider_directory_cms_prepared_identity_validation_postgres import MINT_ROOTS, snapshots
from tests.test_provider_directory_cms_prepared_outputs_postgres import (
    full_private_case,
    native_case,
    native_derived_case,
    native_frozen_case,
    observe,
    prepare,
    publisher,
)
from tests.test_provider_directory_cms_prepared_relationships_postgres import input_snapshots

IDENTITY_ROOTS = (*publication.entities.CMS_IDENTITY_TABLES, publication.resources.RESOURCE_IDENTITY_ROOT)
CANONICAL_ROOTS = (*IDENTITY_ROOTS, *(root for root, _, _ in MINT_ROOTS))


@pytest_asyncio.fixture
async def publication_case(full_private_case):
    """Grant only local INSERT/SELECT and fixed keys; no production guard changes."""
    case = full_private_case
    for root in CANONICAL_ROOTS:
        relation = storage_relation(case.storage.catalog_schema, root)
        if root in IDENTITY_ROOTS:
            keys = publication.entities.CMS_IDENTITY_KEYS.get(root, ("source_id", "resource_type", "resource_id"))
            await case.admin.execute(
                f"ALTER TABLE {relation} ADD PRIMARY KEY({','.join(storage_identifier(key) for key in keys)})"
            )
        if root == publication.resources.RESOURCE_IDENTITY_ROOT:
            await case.admin.execute(f"ALTER TABLE {relation} ADD UNIQUE(source_id,resource_type,entity_id)")
        await case.admin.execute(f'ALTER TABLE {relation} OWNER TO "{case.roles["namespace"]}"')
        await case.admin.execute(f'GRANT SELECT,INSERT ON {relation} TO "{case.policy.publisher_role}"')
        assert not await case.admin.fetchval(
            "SELECT pg_has_role($1,$2,'SET')", case.policy.publisher_role, case.roles["namespace"]
        )
        assert not await case.admin.fetchval(
            "SELECT has_table_privilege($1,$2,'UPDATE,DELETE,TRUNCATE')", case.policy.publisher_role, relation
        )
    return case


async def canonical_snapshot(case, driver):
    values = await snapshots(driver, [storage_relation(case.storage.catalog_schema, root) for root in CANONICAL_ROOTS])
    return [json.loads(value) if isinstance(value, str) else value for value in values]


async def private_snapshot(workspace):
    return await snapshots(
        workspace.connection,
        [storage_relation(workspace.policy.schema, receipt["name"]) for receipt in workspace.receipts.values()],
    )


async def assert_complete_copy(workspace):
    for root in IDENTITY_ROOTS:
        columns = publication.entities.CMS_IDENTITY_COLUMNS.get(root, publication.resources.RESOURCE_IDENTITY_COLUMNS)
        fields = ",".join(storage_identifier(column) for column in columns)
        stage = storage_relation(workspace.policy.schema, workspace.receipts[root]["name"])
        canonical = storage_relation(workspace.catalog_schema, root)
        assert await workspace.connection.fetchval(
            f"SELECT NOT EXISTS((SELECT {fields} FROM {stage} EXCEPT ALL SELECT {fields} FROM {canonical}) "
            f"UNION ALL (SELECT {fields} FROM {canonical} EXCEPT ALL SELECT {fields} FROM {stage}))"
        )
    for root, column, stage_root in MINT_ROOTS:
        stage = storage_relation(workspace.policy.schema, workspace.receipts[stage_root]["name"])
        canonical = storage_relation(workspace.catalog_schema, root)
        expected = f"SELECT {column},created_at FROM {stage} WHERE {column} IS NOT NULL"
        actual = f"SELECT {column},created_at FROM {canonical}"
        assert await workspace.connection.fetchval(
            f"SELECT NOT EXISTS(({expected} EXCEPT ALL {actual}) UNION ALL ({actual} EXCEPT ALL {expected}))"
        )


async def assert_no_copied_outputs(case, driver, *, winner=False):
    for root in CANONICAL_ROOTS:
        expected = int(winner and root == publication.resources.RESOURCE_IDENTITY_ROOT)
        assert (
            await driver.fetchval(f"SELECT count(*) FROM {storage_relation(case.storage.catalog_schema, root)}")
            == expected
        )
    assert (
        await driver.fetchval(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='private_source' AND c.relkind='r'"
        )
        == 8
    )
    assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role


async def test_native_eight_writes_replay_uuid_and_timestamps_without_private_mutation(publication_case):
    case = publication_case
    frozen = await input_snapshots(case)
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, evidence):
            private = await private_snapshot(workspace)
            result = await publication.publish_prepared_cms_identities(workspace, case.handoff)
            assert result == evidence["identity_parity"]
            await assert_complete_copy(workspace)
            accepted = await canonical_snapshot(case, driver)
            readback = await publication.read_prepared_cms_identity_publication(workspace, case.handoff)
            assert readback["identity_parity"] == result and len(readback["canonical_mints"]) == 3
            assert await publication.publish_prepared_cms_identities(workspace, case.handoff) == result
            assert await canonical_snapshot(case, driver) == accepted
            assert await publication.read_prepared_cms_identity_publication(workspace, case.handoff) == readback
            assert await private_snapshot(workspace) == private
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient
            assert await driver.fetchval("SELECT count(*) FROM pg_cursors WHERE name<>''") == 0
    assert await input_snapshots(case) == frozen


async def test_native_existing_mint_timestamps_are_preserved_and_actual_readback_detects_drift(publication_case):
    case = publication_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, _):
            await publication.publish_prepared_cms_identities(workspace, case.handoff)
    # Existing native creation timestamps need not equal later binding timestamps.
    for root, column, _ in MINT_ROOTS:
        relation = storage_relation(case.storage.catalog_schema, root)
        await case.admin.execute(f"UPDATE {relation} SET created_at=created_at-interval '1 day'")
    existing = await canonical_snapshot(case, case.admin)
    case.preparation_id = str(uuid4())
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, _):
            await publication.publish_prepared_cms_identities(workspace, case.handoff)
            assert await canonical_snapshot(case, driver) == existing
            before = await publication.read_prepared_cms_identity_publication(workspace, case.handoff)
            for root, column, _ in MINT_ROOTS:
                relation = storage_relation(case.storage.catalog_schema, root)
                mint_row = await driver.fetchrow(
                    f"SELECT {column},created_at FROM {relation} ORDER BY {column} LIMIT 1"
                )
                await case.admin.execute(
                    f"UPDATE {relation} SET created_at=$2 WHERE {column}=$1",
                    mint_row[column],
                    mint_row["created_at"] + timedelta(days=2),
                )
                after = await publication.read_prepared_cms_identity_publication(workspace, case.handoff)
                assert after["identity_parity"] == before["identity_parity"]
                assert after["canonical_mints"][root] != before["canonical_mints"][root]
                assert all(
                    after["canonical_mints"][other] == before["canonical_mints"][other]
                    for other in before["canonical_mints"]
                    if other != root
                )
                await case.admin.execute(
                    f"UPDATE {relation} SET created_at=$2 WHERE {column}=$1", mint_row[column], mint_row["created_at"]
                )
                assert await publication.read_prepared_cms_identity_publication(workspace, case.handoff) == before


async def test_native_legacy_resource_merge_keeps_uuid_only_replay_contract(publication_case):
    case = publication_case
    root = publication.resources.RESOURCE_IDENTITY_ROOT
    canonical = storage_relation(case.storage.catalog_schema, root)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, _):
            stage = storage_relation(workspace.policy.schema, workspace.receipts[root]["name"])
            winner_by_field = dict(await driver.fetchrow(f"SELECT * FROM {stage} ORDER BY resource_id LIMIT 1"))
            winner_by_field["created_at"] += timedelta(days=1)
            await case.admin.execute(
                f"INSERT INTO {canonical} VALUES($1,$2,$3,$4,$5)",
                *(winner_by_field[column] for column in publication.resources.RESOURCE_IDENTITY_COLUMNS),
            )
            with pytest.raises(RuntimeError, match="end legacy component probe"):
                async with _savepoint(driver):
                    await publication.resources.publish_resource_identity_relation(
                        driver, stage, case.storage.catalog_schema
                    )
                    assert (
                        await driver.fetchval(
                            f"SELECT created_at FROM {canonical} WHERE source_id=$1 AND resource_type=$2 AND resource_id=$3",
                            winner_by_field["source_id"],
                            winner_by_field["resource_type"],
                            winner_by_field["resource_id"],
                        )
                        == winner_by_field["created_at"]
                    )
                    raise RuntimeError("end legacy component probe")
            # Retire only the exact seeded fixture winner before private exit reproof.
            await case.admin.execute(
                f"DELETE FROM {canonical} WHERE source_id=$1 AND resource_type=$2 AND resource_id=$3 AND entity_id=$4",
                winner_by_field["source_id"],
                winner_by_field["resource_type"],
                winner_by_field["resource_id"],
                winner_by_field["entity_id"],
            )
            assert await driver.fetchval(f"SELECT count(*) FROM {canonical}") == 0


async def _interrupt_publication(case, original, winner_by_field, outcome, connection, relation, schema):
    """Interrupt after seven real writes or seed the exact conflicting fixture row."""
    assert all(
        [
            await connection.fetchval(f"SELECT EXISTS(SELECT FROM {storage_relation(schema, root)})")
            for root in CANONICAL_ROOTS
            if root != publication.resources.RESOURCE_IDENTITY_ROOT
        ]
    )
    if outcome == "cancel":
        raise asyncio.CancelledError()
    native_row = await connection.fetchrow(
        f"SELECT * FROM {relation} WHERE resource_type='InsurancePlan' ORDER BY resource_id LIMIT 1"
    )
    winner_by_field.update(dict(native_row))
    if outcome == "timestamp_winner":
        winner_by_field["created_at"] += timedelta(days=1)
    else:
        winner_by_field["resource_id"] = "other-winner"
    await case.admin.execute(
        f"INSERT INTO {storage_relation(schema, publication.resources.RESOURCE_IDENTITY_ROOT)} VALUES($1,$2,$3,$4,$5)",
        *(winner_by_field[column] for column in publication.resources.RESOURCE_IDENTITY_COLUMNS),
    )
    await original(connection, relation, schema)


@pytest.mark.parametrize("outcome", ["timestamp_winner", "reverse_uuid_winner", "cancel"])
async def test_native_late_winner_or_cancel_rolls_back_every_write_and_reprepares(
    publication_case, monkeypatch, outcome
):
    """Keep cancellation/conflict rollback and the same whole-candidate retry."""
    case = publication_case
    frozen = await input_snapshots(case)
    original = publication.resources.publish_resource_identity_relation
    winner_by_field = {}

    async def interrupt(connection, relation, schema):
        await _interrupt_publication(case, original, winner_by_field, outcome, connection, relation, schema)

    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        with monkeypatch.context() as patch:
            patch.setattr(publication.resources, "publish_resource_identity_relation", interrupt)
            error = asyncio.CancelledError if outcome == "cancel" else publication.entities.CMSIdentityReprepareRequired
            with pytest.raises(error):
                async with prepare(case, session, driver, inputs) as (workspace, _):
                    await publication.publish_prepared_cms_identities(workspace, case.handoff)
        await assert_no_copied_outputs(case, driver, winner=outcome != "cancel")
        assert await input_snapshots(case) == frozen
        if outcome == "reverse_uuid_winner":
            # Remove only this deliberate invalid fixture row before a clean retry.
            await case.admin.execute(
                f"DELETE FROM {storage_relation(case.storage.catalog_schema, publication.resources.RESOURCE_IDENTITY_ROOT)} "
                "WHERE source_id=$1 AND resource_type=$2 AND resource_id=$3 AND entity_id=$4",
                winner_by_field["source_id"],
                winner_by_field["resource_type"],
                winner_by_field["resource_id"],
                winner_by_field["entity_id"],
            )
        async with prepare(case, session, driver, inputs) as (workspace, _):
            await publication.publish_prepared_cms_identities(workspace, case.handoff)
            await assert_complete_copy(workspace)
            if outcome == "timestamp_winner":
                native_row = await driver.fetchrow(
                    f"SELECT * FROM {storage_relation(case.storage.catalog_schema, publication.resources.RESOURCE_IDENTITY_ROOT)} "
                    "WHERE source_id=$1 AND resource_type=$2 AND resource_id=$3",
                    winner_by_field["source_id"],
                    winner_by_field["resource_type"],
                    winner_by_field["resource_id"],
                )
                assert dict(native_row) == winner_by_field
    assert await input_snapshots(case) == frozen
