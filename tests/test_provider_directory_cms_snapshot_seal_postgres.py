# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original-admin private closure only; no CMS authority or attachment proof."""

import asyncio

import asyncpg
import pytest

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_snapshot_seal as seal
from tests.test_provider_directory_cms_snapshot_custody_postgres import (
    _check,
    _relation,
    native_case,
    native_workspace,
    private_payload_case,
    publisher,
    seal_options,
)


async def _owners(case):
    names = [
        storage_relation(case.policy.schema, value["name"]) for value in case.snapshot_workspace["tables"].values()
    ]
    names.extend(
        storage_relation(item["schema_name"], item["relation_name"])
        for item in case.snapshot_inputs["frozen_inventory"]["relations"]
    )
    return await case.admin.fetch(
        "SELECT oid::bigint,relowner::bigint,relacl::text FROM pg_class WHERE oid=ANY("
        "ARRAY(SELECT to_regclass(name)::oid FROM unnest($1::text[]) names(name))) ORDER BY oid",
        names,
    )


async def test_original_admin_closes_only_payloads_and_publisher_independently_rechecks(private_payload_case):
    case = private_payload_case
    original = await _owners(case)
    async with case.admin.transaction():
        await case.admin.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        evidence = await seal.seal_prepared_cms_payload_snapshots(case.admin, **seal_options(case))
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        assert await _check(case, driver) == evidence
    selected_oids = {item["oid"] for item in evidence["tables"].values()}
    for before, after in zip(original, await _owners(case), strict=True):
        assert before["oid"] == after["oid"]
        assert after["relowner"] == (
            case.storage.namespace_owner_oid if before["oid"] in selected_oids else before["relowner"]
        )


async def test_outer_rollback_restores_all_original_owners_and_acl(private_payload_case):
    case = private_payload_case
    before = await _owners(case)
    with pytest.raises(RuntimeError, match="outer rollback"):
        async with case.admin.transaction():
            await case.admin.execute("SET LOCAL search_path=pg_catalog,pg_temp")
            await seal.seal_prepared_cms_payload_snapshots(case.admin, **seal_options(case))
            raise RuntimeError("outer rollback")
    assert await _owners(case) == before


async def test_ordinary_publisher_cannot_invoke_admin_primitive(private_payload_case):
    case = private_payload_case
    before = await _owners(case)
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="independent_original_admin_required"):
            await seal.seal_prepared_cms_payload_snapshots(driver, **seal_options(case))
    assert await _owners(case) == before


@pytest.mark.parametrize("dependency", ["view", "sequence"])
async def test_external_owner_side_effects_refuse_without_changing_any_heap(private_payload_case, dependency):
    case = private_payload_case
    if dependency == "view":
        await case.admin.execute("CREATE VIEW private_source.dependent_view AS SELECT * FROM " + _relation(case))
    else:
        await case.admin.execute("CREATE SEQUENCE private_source.owned_sequence")
        await case.admin.execute(
            "ALTER SEQUENCE private_source.owned_sequence OWNER TO " + storage_identifier(case.policy.owner_role)
        )
        await case.admin.execute(
            "ALTER SEQUENCE private_source.owned_sequence OWNED BY " + _relation(case) + ".dataset_id"
        )
    before = await _owners(case)
    async with case.admin.transaction():
        await case.admin.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="unsupported_dependency"):
            await seal.seal_prepared_cms_payload_snapshots(case.admin, **seal_options(case))
        assert await _owners(case) == before


async def test_peer_writer_lock_refuses_nowait_before_any_owner_change(private_payload_case):
    case = private_payload_case
    before = await _owners(case)
    async with publisher(case) as (_session, driver):
        await driver.execute("LOCK TABLE ONLY " + _relation(case) + " IN ROW EXCLUSIVE MODE")
        async with case.admin.transaction():
            await case.admin.execute("SET LOCAL search_path=pg_catalog,pg_temp")
            with pytest.raises(asyncpg.LockNotAvailableError):
                await seal.seal_prepared_cms_payload_snapshots(case.admin, **seal_options(case))
            assert await _owners(case) == before


@pytest.mark.parametrize("failure", ["error", "cancel"])
async def test_late_failure_restores_every_owner_and_original_admin_role(private_payload_case, monkeypatch, failure):
    case = private_payload_case
    before = await _owners(case)
    original = seal._close

    async def interrupt(*args):
        await original(*args)
        raise asyncio.CancelledError() if failure == "cancel" else RuntimeError("late failure")

    monkeypatch.setattr(seal, "_close", interrupt)
    async with case.admin.transaction():
        await case.admin.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        error = asyncio.CancelledError if failure == "cancel" else RuntimeError
        with pytest.raises(error):
            await seal.seal_prepared_cms_payload_snapshots(case.admin, **seal_options(case))
        assert await _owners(case) == before
        assert await case.admin.fetchval("SELECT session_user=current_user") is True


@pytest.mark.parametrize("changed", ["grant", "index", "destination"])
async def test_drift_or_unprotected_destination_refuses_without_repair(private_payload_case, changed):
    case = private_payload_case
    changes_by_name = {}
    if changed == "grant":
        await case.admin.execute(
            f"GRANT UPDATE(payload_hash) ON {_relation(case)} TO {storage_identifier(case.policy.publisher_role)}"
        )
    elif changed == "index":
        name = case.snapshot_workspace["tables"][seal.workspace.storage.RESOURCE]["name"] + "_i0"
        await case.admin.execute("DROP INDEX " + storage_relation(case.policy.schema, name))
    else:
        changes_by_name = dict(
            owner_role=case.policy.publisher_role,
            owner_oid=await case.admin.fetchval(
                "SELECT oid::bigint FROM pg_roles WHERE rolname=$1", case.policy.publisher_role
            ),
        )
    before = await _owners(case)
    async with case.admin.transaction():
        await case.admin.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        options_by_name = {**seal_options(case), **changes_by_name}
        with pytest.raises(ValueError, match="storage_custody_changed|layout_changed|principals_changed"):
            await seal.seal_prepared_cms_payload_snapshots(case.admin, **options_by_name)
        assert await _owners(case) == before
