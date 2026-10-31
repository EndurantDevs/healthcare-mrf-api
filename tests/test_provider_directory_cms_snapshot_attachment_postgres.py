# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private payload attachment mechanics; no canonical custody or activation."""

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_workspace as workspace
from process.provider_directory_cms_snapshot_parents import cms_snapshot_parent_statements
from tests.test_provider_directory_cms_prepared_workspace_postgres import (
    native_case,
    native_workspace,
    observe,
    publisher,
    rows_by_root,
    scope,
)


async def _parent(connection, schema, root, ordinal):
    """Use fixed migration columns, named native CHECKs/keys and index semantics."""
    name = "snapshot_parent_" + str(ordinal)
    relation = storage_relation(schema, name)
    for statement in cms_snapshot_parent_statements(schema, root, name):
        await connection.execute(statement)
    return relation


async def _index_oids(connection, oid):
    return await connection.fetchval(
        "SELECT array_agg(indexrelid::bigint ORDER BY indexrelid) FROM pg_index WHERE indrelid=$1", oid
    )


async def test_six_payloads_attach_without_rebuilding_fixed_parent_indexes(native_workspace):
    case = native_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            for root, records in rows_by_root(current.identity).items():
                await current.copy_rows(root, workspace._layouts()[root][1], records)
            await current.finish_indexes()
            original_receipts = current.receipts
            savepoint = driver.transaction()
            await savepoint.start()
            try:
                await driver.execute("SET LOCAL ROLE " + storage_identifier(current.policy.owner_role))
                literal = await driver.fetchval("SELECT quote_literal($1::text)", current.identity["dataset_id"])
                for ordinal, root in enumerate(workspace.storage.RELATIONS):
                    parent = await _parent(driver, current.policy.schema, root, ordinal)
                    receipt = original_receipts[root]
                    child = storage_relation(current.policy.schema, receipt["name"])
                    before = await _index_oids(driver, receipt["oid"])
                    await driver.execute(f"ALTER TABLE {parent} ATTACH PARTITION {child} FOR VALUES IN ({literal})")
                    assert await _index_oids(driver, receipt["oid"]) == before
                    assert await driver.fetchval("SELECT count(*) FROM " + parent) == 1
            finally:
                await savepoint.rollback()
            assert await current.verify_current() == original_receipts
