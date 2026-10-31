# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Existing allocator branch only; disposable admin use issues no CMS authority."""

import asyncio
import json
from contextlib import asynccontextmanager

import pytest

from db import migration_cms_snapshot_allocator as migration
from db.migration_provider_directory_dataset_candidates import TABLES
from db.plan_release_storage import storage_relation
from process.provider_directory_cms_snapshot_allocation import cms_snapshot_allocation
from tests.test_provider_directory_cms_allocation_semantics_postgres import _native_allocations

_CMS_METADATA = {
    "source_ids": ["cms-npd"],
    "source_release": {"source_id": "cms-npd", "vector_sha256": "a" * 64},
}


@asynccontextmanager
async def _case(monkeypatch):
    async with _native_allocations(monkeypatch) as case:
        await case.catalog.connection.execute("SET search_path=pg_catalog,pg_temp")
        yield case


async def _identity(case):
    return dict(
        await case.catalog.connection.fetchrow(
            "SELECT p.oid::bigint,p.proowner::bigint AS owner_oid FROM pg_catalog.pg_proc p "
            "WHERE p.oid=$1::regprocedure",
            storage_relation(case.catalog.schema_name, "prepare_pd_generic_dataset_storage") + "()",
        )
    )


async def _install(case, expected):
    async with case.catalog.engine.begin() as transaction:
        await transaction.exec_driver_sql("SET LOCAL lock_timeout='100ms'")
        await transaction.exec_driver_sql("SET LOCAL search_path=pg_catalog,pg_temp")
        await transaction.run_sync(
            lambda connection: migration.upgrade_cms_snapshot_allocator(
                connection, case.catalog.schema_name, expected=expected
            )
        )


async def _header(case, dataset, *, connection=None, metadata=None):
    connection = connection or case.catalog.connection
    await connection.execute(
        "INSERT INTO "
        + storage_relation(case.catalog.schema_name, "provider_directory_endpoint_dataset")
        + "(dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json) "
        "VALUES($1,'endpoint-official','acquiring',false,0,$2::jsonb)",
        dataset,
        json.dumps(_CMS_METADATA if metadata is None else metadata),
    )


async def _children(case, dataset):
    return await case.catalog.connection.fetchval(
        "SELECT count(*) FROM pg_catalog.pg_class c WHERE c.relnamespace=pg_catalog.to_regnamespace($1) "
        "AND c.relname=ANY(ARRAY(SELECT 'pd_generic_'||md5(root||':'||$2) FROM unnest($3::text[]) root))",
        case.catalog.schema_name,
        dataset,
        list(TABLES[:4]),
    )


async def _metadata(case):
    return await case.catalog.connection.fetchrow(
        "SELECT to_jsonb(p)-'prosrc' AS identity,p.prosrc,obj_description(p.oid,'pg_proc') AS comment "
        "FROM pg_catalog.pg_proc p WHERE p.oid=$1::regprocedure",
        storage_relation(case.catalog.schema_name, "prepare_pd_generic_dataset_storage") + "()",
    )


@pytest.mark.asyncio
async def test_original_admin_selects_only_one_cms_dataset_and_keeps_other_allocation(monkeypatch):
    async with _case(monkeypatch) as case:
        expected = await _identity(case)
        before = await _metadata(case)
        await _install(case, expected)
        after = await _metadata(case)
        assert after["identity"] == before["identity"] and after["comment"] == before["comment"]
        assert after["prosrc"] == migration.bound_cms_allocator_body(case.catalog.schema_name)
        await _install(case, expected)
        assert await _metadata(case) == after
        driver = case.catalog.connection
        async with driver.transaction():
            async with cms_snapshot_allocation(
                driver, "selected", catalog_schema=case.catalog.schema_name, expected_allocator=expected
            ):
                await _header(case, "selected")
                await _header(case, "disjoint")
                await _header(case, "ordinary", metadata={})
            await _header(case, "restoration")
        assert await _children(case, "selected") == 0
        for dataset in ("disjoint", "ordinary", "restoration"):
            assert await _children(case, dataset) == 4


@pytest.mark.asyncio
async def test_original_publisher_cannot_select_branch_even_after_setting_same_variable(monkeypatch):
    async with _case(monkeypatch) as case:
        expected = await _identity(case)
        await _install(case, expected)
        role = await case.publisher.fetchval("SELECT session_user::text")
        await case.catalog.connection.execute(
            f'GRANT INSERT ON {case.catalog.schema}.provider_directory_endpoint_dataset TO "{role}"'
        )
        async with case.publisher.transaction():
            await case.publisher.execute("SET LOCAL search_path=pg_catalog,pg_temp")
            with pytest.raises(RuntimeError, match="original_admin_required"):
                async with cms_snapshot_allocation(
                    case.publisher, "untrusted", catalog_schema=case.catalog.schema_name, expected_allocator=expected
                ):
                    pytest.fail("ordinary caller selected the admin branch")
            await case.publisher.fetchval("SELECT set_config($1,'untrusted',true)", migration.SELECTOR)
            await _header(case, "untrusted", connection=case.publisher)
        assert await _children(case, "untrusted") == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_selector_and_header_roll_back_on_error_or_cancellation(monkeypatch, failure):
    async with _case(monkeypatch) as case:
        expected = await _identity(case)
        await _install(case, expected)
        driver = case.catalog.connection
        async with driver.transaction():
            with pytest.raises(failure):
                async with cms_snapshot_allocation(
                    driver, "interrupted", catalog_schema=case.catalog.schema_name, expected_allocator=expected
                ):
                    await _header(case, "interrupted")
                    raise failure("interrupted allocation")
            assert await driver.fetchval("SELECT current_setting($1,true)", migration.SELECTOR) in (None, "")
            assert not await driver.fetchval(
                f"SELECT EXISTS(SELECT FROM {case.catalog.schema}.provider_directory_endpoint_dataset WHERE dataset_id='interrupted')"
            )
            await _header(case, "subsequent")
        assert await _children(case, "subsequent") == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["source", "release", "vector", "selector"])
async def test_wrong_metadata_or_selector_keeps_existing_allocator(monkeypatch, changed):
    async with _case(monkeypatch) as case:
        expected = await _identity(case)
        await _install(case, expected)
        metadata = json.loads(json.dumps(_CMS_METADATA))
        if changed == "source":
            metadata["source_ids"] = ["example-source"]
        elif changed == "release":
            metadata["source_release"]["source_id"] = "example-source"
        elif changed == "vector":
            metadata["source_release"]["vector_sha256"] = "wrong"
        driver = case.catalog.connection
        async with driver.transaction():
            async with cms_snapshot_allocation(
                driver, "selected", catalog_schema=case.catalog.schema_name, expected_allocator=expected
            ):
                await _header(case, "other" if changed == "selector" else "selected", metadata=metadata)
        assert await _children(case, "other" if changed == "selector" else "selected") == 4


@pytest.mark.asyncio
async def test_selector_requires_installed_exact_body_and_exact_function_identity(monkeypatch):
    async with _case(monkeypatch) as case:
        expected = await _identity(case)
        driver = case.catalog.connection
        async with driver.transaction():
            with pytest.raises(RuntimeError, match="function_changed"):
                async with cms_snapshot_allocation(
                    driver, "missing", catalog_schema=case.catalog.schema_name, expected_allocator=expected
                ):
                    pytest.fail("legacy allocator accepted as snapshot variant")
            assert await driver.fetchval("SELECT 1") == 1
        await _install(case, expected)
        async with driver.transaction():
            with pytest.raises(RuntimeError, match="function_changed"):
                await migration.require_cms_snapshot_allocator(
                    driver, case.catalog.schema_name, expected={**expected, "oid": expected["oid"] + 1}
                )
        before = await _metadata(case)
        with pytest.raises(RuntimeError, match="function_changed"):
            await _install(case, {**expected, "owner_oid": expected["owner_oid"] + 1})
        assert await _metadata(case) == before


@pytest.mark.asyncio
async def test_untrusted_resolution_refuses_before_privileged_query(monkeypatch):
    async with _case(monkeypatch) as case:
        expected = await _identity(case)
        await _install(case, expected)
        driver = case.catalog.connection
        async with driver.transaction():
            await driver.execute("SET LOCAL search_path=public,pg_catalog")
            with pytest.raises(RuntimeError, match="search_path"):
                async with cms_snapshot_allocation(
                    driver, "untrusted-path", catalog_schema=case.catalog.schema_name, expected_allocator=expected
                ):
                    pytest.fail("untrusted path reached admin selection")
            assert await driver.fetchval("SELECT 1") == 1


@pytest.mark.asyncio
async def test_disjoint_ordinary_header_commits_while_admin_selector_is_active(monkeypatch):
    async with _case(monkeypatch) as case:
        expected = await _identity(case)
        await _install(case, expected)
        role = await case.publisher.fetchval("SELECT session_user::text")
        await case.catalog.connection.execute(
            f'GRANT INSERT ON {case.catalog.schema}.provider_directory_endpoint_dataset TO "{role}"'
        )
        driver = case.catalog.connection
        async with driver.transaction():
            async with cms_snapshot_allocation(
                driver, "admin-selected", catalog_schema=case.catalog.schema_name, expected_allocator=expected
            ):
                await _header(case, "admin-selected")
                async with case.publisher.transaction():
                    await case.publisher.execute("SET LOCAL statement_timeout='500ms'")
                    await _header(case, "parallel-ordinary", connection=case.publisher, metadata={})
                assert await _children(case, "parallel-ordinary") == 4
                assert await _children(case, "admin-selected") == 0


@pytest.mark.asyncio
async def test_upgrade_rolls_back_with_outer_transaction_and_refuses_body_drift(monkeypatch):
    async with _case(monkeypatch) as case:
        expected = await _identity(case)
        before = await _metadata(case)
        with pytest.raises(RuntimeError, match="outer rollback"):
            async with case.catalog.engine.begin() as transaction:
                await transaction.exec_driver_sql("SET LOCAL search_path=pg_catalog,pg_temp")
                await transaction.run_sync(
                    lambda connection: migration.upgrade_cms_snapshot_allocator(
                        connection, case.catalog.schema_name, expected=expected
                    )
                )
                raise RuntimeError("outer rollback")
        assert await _metadata(case) == before
        definition = await case.catalog.connection.fetchval(
            "SELECT pg_catalog.pg_get_functiondef($1::oid)", expected["oid"]
        )
        original = migration.allocator_bodies(case.catalog.schema_name)[0]
        await case.catalog.connection.execute(definition.replace(original, original + "\n-- changed body\n", 1))
        changed = await _metadata(case)
        with pytest.raises(RuntimeError, match="function_changed"):
            await _install(case, expected)
        assert await _metadata(case) == changed
