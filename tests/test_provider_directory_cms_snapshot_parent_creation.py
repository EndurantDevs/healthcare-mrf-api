# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed parent creation only; protected CMS issuance and the shared cut stay external."""

import asyncio
from uuid import uuid4

import asyncpg
import pytest

from process import provider_directory_cms_snapshot_parents as parents
from tests.test_provider_directory_cms_prepared_context_postgres import native_case


def _names(case, preparation):
    return [parents.cms_snapshot_parent_name(preparation, root) for root in parents.workspace.storage.RELATIONS[-2:]]


async def _objects(case, names):
    return await case.admin.fetch(
        "SELECT oid::bigint,relname,relowner::bigint,relkind FROM pg_catalog.pg_class "
        "WHERE relnamespace=$1::oid AND relname=ANY($2::text[]) ORDER BY oid",
        case.storage.catalog_schema_oid,
        names,
    )


async def test_native_create_keeps_fixed_owners_layout_and_outer_rollback(native_case):
    case, preparation = native_case, str(uuid4())
    names = _names(case, preparation)

    class CallerRollback(Exception):
        pass

    with pytest.raises(CallerRollback):
        async with case.admin.transaction():
            await case.admin.execute("SET LOCAL search_path=pg_catalog")
            before = await case.admin.fetchval("SELECT pg_catalog.pg_current_xact_id()::text")
            actual = await parents.create_cms_snapshot_parents(case.admin, case.storage, preparation)
            assert set(actual) == set(parents.workspace.storage.RELATIONS[-2:])
            assert {entry["relname"] for entry in actual.values()} == set(names)
            assert actual == await parents.read_cms_snapshot_parents(case.admin, case.storage, preparation)
            assert await case.admin.fetchval("SELECT pg_catalog.pg_current_xact_id()::text") == before
            assert case.admin.is_in_transaction()
            raise CallerRollback
    assert not await _objects(case, names)


async def test_native_existing_name_refuses_without_adoption_or_other_creation(native_case):
    case, preparation = native_case, str(uuid4())
    names = _names(case, preparation)
    async with case.admin.transaction():
        await case.admin.execute("SET LOCAL search_path=pg_catalog")
        relation = parents.storage_relation(case.storage.catalog_schema, names[0])
        await case.admin.execute(f"CREATE TABLE {relation}(marker integer)")
        await case.admin.execute(f"INSERT INTO {relation} VALUES(42)")
        before = await _objects(case, names)
        with pytest.raises(ValueError, match="parent_exists"):
            await parents.create_cms_snapshot_parents(case.admin, case.storage, preparation)
        assert await _objects(case, names) == before
        assert await case.admin.fetchval(f"SELECT marker FROM {relation}") == 42
        assert await case.admin.fetchval("SELECT 1") == 1


@pytest.mark.parametrize("failure", ["validation", "cancel"])
async def test_native_late_failure_rolls_back_both_parents_without_ending_caller(native_case, monkeypatch, failure):
    case, preparation = native_case, str(uuid4())
    names = _names(case, preparation)
    original = parents.read_cms_snapshot_parents
    error_type = RuntimeError if failure == "validation" else asyncio.CancelledError

    async def reject_after_native_readback(*arguments):
        actual = await original(*arguments)
        assert len(actual) == 2
        raise error_type("synthetic_late_refusal")

    monkeypatch.setattr(parents, "read_cms_snapshot_parents", reject_after_native_readback)
    async with case.admin.transaction():
        await case.admin.execute("SET LOCAL search_path=pg_catalog")
        with pytest.raises(error_type, match="synthetic_late_refusal"):
            await parents.create_cms_snapshot_parents(case.admin, case.storage, preparation)
        assert not await _objects(case, names)
        assert case.admin.is_in_transaction()
        assert await case.admin.fetchval("SELECT 1") == 1


@pytest.mark.parametrize("mode", ["read_only", "repeatable_read", "replica", "assumed_role"])
async def test_native_requires_original_writable_admin_session_before_creation(native_case, mode):
    case, preparation = native_case, str(uuid4())
    async with case.admin.transaction(
        readonly=mode == "read_only", isolation="repeatable_read" if mode == "repeatable_read" else "read_committed"
    ):
        await case.admin.execute("SET LOCAL search_path=pg_catalog")
        if mode == "replica":
            await case.admin.execute("SET LOCAL session_replication_role=replica")
        if mode == "assumed_role":
            await case.admin.execute(f'SET LOCAL ROLE "{case.roles["namespace"]}"')
        with pytest.raises(ValueError, match="original_admin_required"):
            await parents.create_cms_snapshot_parents(case.admin, case.storage, preparation)
        assert not await _objects(case, _names(case, preparation))


async def test_native_genuine_publisher_login_refuses_before_creation(native_case):
    case, preparation = native_case, str(uuid4())
    publisher = await asyncpg.connect(
        case.url.set(username=case.policy.publisher_role, password=case.password).render_as_string(hide_password=False)
    )
    try:
        async with publisher.transaction():
            await publisher.execute("SET LOCAL search_path=pg_catalog")
            with pytest.raises(ValueError, match="original_admin_required"):
                await parents.create_cms_snapshot_parents(publisher, case.storage, preparation)
    finally:
        await publisher.close()
    assert not await _objects(case, _names(case, preparation))
