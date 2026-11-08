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
        owner_oid=case.storage.namespace_owner_oid,
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
            assert proof == {**case.snapshot_workspace["tables"][root], "owner": case.storage.namespace_owner_oid}
        locked = await driver.fetchval(
            "SELECT array_agg(DISTINCT c.oid::bigint ORDER BY c.oid::bigint) "
            "FROM pg_locks l JOIN pg_class c ON c.oid=l.relation JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE l.pid=pg_backend_pid() AND l.mode='AccessShareLock' AND c.relkind='r' AND n.nspname=$1",
            case.policy.schema,
        )
        assert locked == sorted(proof["oid"] for proof in evidence["tables"].values())
        assert evidence == await _check(case, driver)
        assert await driver.fetchval("SELECT session_user=current_user") is True


async def test_mutable_preparation_owner_is_not_snapshot_custody(private_payload_case):
    case = private_payload_case
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="rights_changed"):
            await _check(case, driver)
        with pytest.raises(ValueError, match="principals_changed"):
            await _check(
                case,
                driver,
                owner_role=case.policy.owner_role,
                owner_oid=case.snapshot_workspace["tables"][workspace.storage.RESOURCE]["owner"],
            )


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


@pytest.mark.parametrize("grant", ["column", "public"])
async def test_direct_and_column_write_grants_refuse(closed_payload_case, grant):
    case = closed_payload_case
    principal = storage_identifier(case.policy.publisher_role) if grant == "column" else "PUBLIC"
    privilege = "UPDATE(payload_hash)" if grant == "column" else "UPDATE"
    await case.admin.execute(f"GRANT {privilege} ON {_relation(case)} TO {principal}")
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="storage_custody_changed|rights_changed"):
            await _check(case, driver)


@pytest.mark.parametrize("actor", ["prepared_publisher", "source_publisher"])
async def test_dormant_global_writer_membership_refuses_for_every_ordinary_login(closed_payload_case, actor):
    case = closed_payload_case
    await case.admin.execute(
        f"GRANT pg_write_all_data TO {storage_identifier(case.roles[actor])} WITH INHERIT FALSE,SET FALSE"
    )
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="writer_reachable"):
            await _check(case, driver)


async def test_dormant_owner_membership_refuses_even_without_set_or_inherit(closed_payload_case):
    case = closed_payload_case
    await case.admin.execute(
        f"GRANT {storage_identifier(case.roles['namespace'])} TO {storage_identifier(case.policy.publisher_role)} "
        "WITH INHERIT FALSE,SET FALSE"
    )
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="writer_reachable"):
            await _check(case, driver)


async def test_index_drift_refuses_against_original_retained_layout(closed_payload_case):
    case = closed_payload_case
    name = case.snapshot_workspace["tables"][workspace.storage.RESOURCE]["name"] + "_i0"
    await case.admin.execute("DROP INDEX " + storage_relation(case.policy.schema, name))
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="layout_changed"):
            await _check(case, driver)


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
    changed["tables"][workspace.storage.RESOURCE]["oid"] = case.snapshot_inputs["frozen_inventory"]["relations"][0][
        "relation_oid"
    ]
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="input_output_alias"):
            await _check(case, driver, expected_workspace=changed)


@pytest.mark.parametrize("changed", ["owner_oid", "active_role", "search_path"])
async def test_exact_owner_original_actor_and_native_resolution_are_required(closed_payload_case, changed):
    case = closed_payload_case
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        if changed == "active_role":
            await driver.execute("SET LOCAL ROLE " + storage_identifier(case.policy.owner_role))
        elif changed == "search_path":
            await driver.execute("SET LOCAL search_path=public")
        changes_by_name = {"owner_oid": case.storage.namespace_owner_oid + 1} if changed == "owner_oid" else {}
        with pytest.raises(ValueError, match="principals_changed|original_publisher_required|search_path"):
            await _check(case, driver, **changes_by_name)


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
