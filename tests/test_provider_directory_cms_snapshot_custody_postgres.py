# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native private DML/owner closure only; fixture transfers issue no CMS authority."""

from copy import deepcopy

import asyncpg
import pytest
import pytest_asyncio

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_cms_snapshot_custody as custody
from process import provider_directory_cms_snapshot_seal as seal
from tests.test_provider_directory_cms_prepared_workspace_postgres import (
    native_case,
    native_workspace,
    observe,
    publisher,
    rows_by_root,
    scope,
)


@pytest_asyncio.fixture
async def private_payload_case(native_workspace):
    case = native_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            for root, records in rows_by_root(current.identity).items():
                await current.copy_rows(root, workspace._layouts()[root][1], records)
            await current.finish_indexes()
            case.snapshot_workspace, case.snapshot_inputs = current.binding, deepcopy(inputs)
    maintenance = "cms_snapshot_maintenance_" + case.preparation_id.replace("-", "")[:12]
    has_created = False
    try:
        await case.admin.execute(
            f"CREATE ROLE {storage_identifier(maintenance)} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
        )
        has_created = True
        await case.admin.execute(
            f"GRANT {storage_identifier(case.roles['namespace'])} TO {storage_identifier(maintenance)}"
        )
        case.snapshot_migration = maintenance
        yield case
    finally:
        if has_created:
            await case.admin.execute("DROP ROLE " + storage_identifier(maintenance))


@pytest_asyncio.fixture
async def closed_payload_case(private_payload_case):
    """Use the existing protected namespace owner only in this disposable database."""
    case = private_payload_case
    async with case.admin.transaction():
        await case.admin.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        await seal.seal_prepared_cms_payload_snapshots(case.admin, **seal_options(case))
    return case


def seal_options(case):
    return dict(
        source_binding=case.storage,
        prepared_inputs=case.snapshot_inputs,
        handoff=case.handoff,
        expected_workspace=case.snapshot_workspace,
        owner_role=case.roles["namespace"],
        migration_role=case.snapshot_migration,
    )


def _relation(case, root=workspace.storage.RESOURCE):
    return storage_relation(case.policy.schema, case.snapshot_workspace["tables"][root]["name"])


async def _check(case, driver, **changes):
    options_by_name = seal_options(case)
    options_by_name.update(changes)
    return await custody.require_cms_payload_snapshot_custody(driver, **options_by_name)


async def test_closed_payloads_pin_only_six_heaps_with_exact_layouts(closed_payload_case):
    case = closed_payload_case
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        evidence = await _check(case, driver)
        assert evidence["contract"] == custody.CONTRACT
        assert set(evidence["tables"]) == set(workspace.storage.RELATIONS)
        for root, proof in evidence["tables"].items():
            assert proof == case.snapshot_workspace["tables"][root]
        locked = await driver.fetchval(
            "SELECT array_agg(DISTINCT c.relname::text ORDER BY c.relname::text) "
            "FROM pg_locks l JOIN pg_class c ON c.oid=l.relation JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE l.pid=pg_backend_pid() AND l.mode='AccessShareLock' AND c.relkind='r' AND n.nspname=$1",
            case.policy.schema,
        )
        assert locked == sorted(proof["name"] for proof in evidence["tables"].values())
        assert evidence == await _check(case, driver)
        assert await driver.fetchval("SELECT session_user=current_user") is True


async def test_named_payload_read_is_not_a_freeze_or_mutation_operation(private_payload_case):
    case = private_payload_case
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        before = await driver.fetchval("SELECT count(*) FROM " + _relation(case))
        report = await _check(case, driver)
        assert set(report) == {"contract", "tables"}
        assert await driver.fetchval("SELECT count(*) FROM " + _relation(case)) == before
        assert await driver.execute("UPDATE " + _relation(case) + " SET payload_hash=payload_hash") == "UPDATE 1"


@pytest.mark.parametrize("operation", ["insert", "update", "delete", "truncate", "alter", "index", "owner_role"])
async def test_publisher_cannot_mutate_or_own_closed_payloads(closed_payload_case, operation):
    case = closed_payload_case
    relation = _relation(case)
    sql_by_operation = {
        "insert": f"INSERT INTO {relation} SELECT * FROM {relation}",
        "update": f"UPDATE {relation} SET payload_hash=repeat('b',64)",
        "delete": "DELETE FROM " + relation,
        "truncate": "TRUNCATE TABLE " + relation,
        "alter": "ALTER TABLE " + relation + " ADD COLUMN injected integer",
        "index": "CREATE INDEX injected ON " + relation + "(payload_hash)",
        "owner_role": "SET LOCAL ROLE " + storage_identifier(case.roles["namespace"]),
    }
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        await _check(case, driver)
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with driver.transaction():
                await driver.execute(sql_by_operation[operation])
        assert await driver.fetchval("SELECT count(*) FROM " + relation) == 1


async def test_busy_exact_payload_refuses_nowait_and_preserves_outer_transaction(closed_payload_case):
    case = closed_payload_case
    async with case.admin.transaction():
        await case.admin.execute("LOCK TABLE ONLY " + _relation(case) + " IN ACCESS EXCLUSIVE MODE")
        async with publisher(case) as (_session, driver):
            await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
            with pytest.raises(asyncpg.LockNotAvailableError):
                await _check(case, driver)
            assert driver.is_in_transaction()
            assert not await driver.fetchval(
                "SELECT EXISTS(SELECT FROM pg_locks l JOIN pg_class c ON c.oid=l.relation "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE l.pid=pg_backend_pid() AND n.nspname=$1)",
                case.policy.schema,
            )


async def test_frozen_input_oid_cannot_be_borrowed_as_a_payload(closed_payload_case):
    case = closed_payload_case
    changed = deepcopy(case.snapshot_workspace)
    changed["tables"][workspace.storage.RESOURCE]["name"] = case.snapshot_inputs["frozen_inventory"]["relations"][0][
        "relation_name"
    ]
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="retained_outputs_changed|input_output_alias"):
            await _check(case, driver, expected_workspace=changed)


async def test_unsafe_native_resolution_is_refused(closed_payload_case):
    case = closed_payload_case
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=public")
        with pytest.raises(ValueError, match="search_path"):
            await _check(case, driver)


async def test_prepared_input_changes_during_native_check_refuse(closed_payload_case, monkeypatch):
    case = closed_payload_case
    original = custody._observe

    async def change_inputs(*args):
        evidence = await original(*args)
        case.snapshot_inputs["prepared_policy"]["publisher_role"] = "changed_publisher"
        return evidence

    monkeypatch.setattr(custody, "_observe", change_inputs)
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="retained_binding_changed"):
            await _check(case, driver)
