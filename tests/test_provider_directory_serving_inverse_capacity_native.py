# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native catalog/meter components; this is not an inverse admission or lifecycle proof."""

from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db import models
from process import provider_directory_artifact_scratch_inputs as scratch
from process import provider_directory_output_canonical_backfill as backfill
from process import provider_directory_profile as profile
from process import provider_directory_profile_native_copy as native_copy
from process import provider_directory_serving_inverse_capacity_inputs as inputs
from process import provider_directory_serving_inverse_commit as commit
from process.provider_directory_output_snapshot_catalog import OUTPUT_CATALOG_SQL, decoded
from process.provider_directory_projection_stage import _copy_driver
from tests.test_provider_directory_profile_selection_db import _profile_test_database


@pytest.mark.asyncio
async def test_native_inventory_preserves_fixed_files_and_resident_growth():
    database = await _profile_test_database()
    schema = "inverse_meter_" + uuid4().hex[:16]
    try:
        await database.status(f"CREATE SCHEMA {schema}")
        await database.status(profile.profile_table_sql(schema, logged=True))
        oid = await database.scalar(
            "SELECT to_regclass(:name)::oid::bigint", name=schema + ".provider_directory_profile"
        )
        async with database.transaction() as session:
            driver = await _copy_driver(session)
            before = (await inputs._physical_inventory(SimpleNamespace(driver=driver), [oid]))[oid]
            await database.status(
                f"INSERT INTO {schema}.provider_directory_profile SELECT 1234567890,jsonb_build_object('payload',(SELECT string_agg(md5(n::text),'') FROM generate_series(1,10000) n)),'{{}}'::jsonb,ARRAY['source']::varchar[],ARRAY['endpoint']::varchar[],ARRAY['dataset']::varchar[],1,1,1,'generation',clock_timestamp()"
            )
            grown = (await inputs._physical_inventory(SimpleNamespace(driver=driver), [oid]))[oid]
            assert grown["total_bytes"] > before["total_bytes"]
            assert grown["physical"] == before["physical"]
            assert len(grown["physical"]) >= 4  # native heap, PK index, TOAST and its native index
            await database.status(f"DELETE FROM {schema}.provider_directory_profile")
            deleted = (await inputs._physical_inventory(SimpleNamespace(driver=driver), [oid]))[oid]
            assert deleted["total_bytes"] >= grown["total_bytes"]
            await database.status(f"ALTER TABLE {schema}.provider_directory_profile RENAME TO retained_profile")
            renamed = (await inputs._physical_inventory(SimpleNamespace(driver=driver), [oid]))[oid]
            assert renamed == deleted
    finally:
        await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await database.disconnect()


async def _create_commit_roots(case):
    """Use the actual Profile/Evidence definitions and their complete ordinary indexes."""
    names = ["provider_directory_profile", "old_profile", "provider_directory_profile_evidence", "old_evidence"]
    oids = set()
    for name in names:
        is_evidence = "evidence" in name
        create = profile.profile_evidence_table_sql if is_evidence else profile.profile_table_sql
        await case.database.status(create(case.schema, name, logged=True))
        for statement in profile.profile_index_statements(case.schema, name, evidence=is_evidence):
            await case.database.status(statement)
        oids.add(await case.database.scalar("SELECT to_regclass(:name)::oid::bigint", name=case.schema + "." + name))
    return names, oids


async def _rename_commit_roots(case, names):
    """Exercise the actual retained-name helper on the same four locked heap/index families."""
    from process import provider_directory_serving_recovery_receipts as receipts

    for name in names:
        oid = await case.database.scalar("SELECT to_regclass(:name)::oid::bigint", name=case.schema + "." + name)
        indexes = await case.database.all(
            "SELECT c.oid::bigint,c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
            "WHERE i.indrelid=:oid ORDER BY c.oid",
            oid=oid,
        )
        for index_oid, index_name in indexes:
            await receipts.rename(
                case.database, case.schema, index_name, receipts.retained_name(index_oid, index=True), index=True
            )
        await receipts.rename(case.database, case.schema, name, receipts.retained_name(oid))


async def _occupied_array_name(case, target_oid):
    """Occupy the future automatic-array name through ordinary supported DDL."""
    from process import provider_directory_serving_recovery_receipts as receipts

    name = "_" + receipts.retained_name(target_oid)
    await case.database.status(f'CREATE TYPE "{case.schema}"."{name}" AS ENUM (\'synthetic\')')
    type_oid = await case.database.scalar("SELECT CAST(:name AS regtype)::oid::bigint", name=case.schema + "." + name)
    return target_oid, type_oid, name


async def _assert_array_name_preserved(case, collision):
    target_oid, type_oid, name = collision
    assert await case.database.scalar("SELECT typname FROM pg_type WHERE oid=:oid", oid=type_oid) == name
    array_name, array_element_oid, row_type_oid = await case.database.first(
        "SELECT a.typname,a.typelem::bigint,t.oid::bigint FROM pg_class c "
        "JOIN pg_type t ON t.oid=c.reltype JOIN pg_type a ON a.oid=t.typarray WHERE c.oid=:oid",
        oid=target_oid,
    )
    assert array_name != name and array_name.startswith(name)
    assert array_element_oid == row_type_oid


async def _assert_system_toast_refused(case):
    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError

    assert not await case.database.scalar("SELECT rolsuper FROM pg_roles WHERE rolname=current_user")
    toast_oid = await case.database.scalar(
        "SELECT reltoastrelid::bigint FROM pg_class WHERE oid='pg_catalog.pg_index'::regclass"
    )
    assert toast_oid > 0
    assert not await case.database.scalar("SELECT has_table_privilege(CAST(:oid AS oid),'SELECT')", oid=toast_oid)
    with pytest.raises(DBAPIError) as refused:
        async with case.database.transaction() as session:
            await session.execute(text("SELECT count(*) FROM pg_toast.pg_toast_2610"))
    assert getattr(refused.value.orig, "sqlstate", None) == "42501"


@pytest.mark.asyncio
@pytest.mark.parametrize("array_name_occupied", (False, True))
async def test_actual_profile_rename_catalog_census_reuses_existing_commit_envelope(monkeypatch, array_name_occupied):
    from process import provider_directory_profile_failed_cleanup as cleanup
    from tests.provider_directory_serving_recovery_postgres_support import ordinary_serving_case

    async with ordinary_serving_case(monkeypatch) as case:
        names, oids = await _create_commit_roots(case)
        collision = await _occupied_array_name(case, min(oids)) if array_name_occupied else None
        await _assert_system_toast_refused(case)
        async with case.database.transaction():
            await case.database.status(
                "LOCK TABLE " + ",".join(case.schema + "." + name for name in names) + " IN ACCESS EXCLUSIVE MODE"
            )
            dependencies = await commit.rename_dependencies(case.directory, oids)
            assert len(dependencies) == 4 and all(
                entry["catalog_tuple_count"] == entry["object_count"] + 1 and entry["relation_count"] >= 2
                for entry in dependencies
            )
            wal_block = await case.database.scalar("SELECT current_setting('wal_block_size')::bigint")
            bound = cleanup._commit_wal_bound(dependencies, SimpleNamespace(postgres_wal_block_size_bytes=wal_block))
            assert bound > wal_block and all(entry["fingerprint"] for entry in dependencies)
            await _rename_commit_roots(case, names)
            if collision is not None:
                await _assert_array_name_preserved(case, collision)
            precommit_lsn = await case.database.scalar("SELECT pg_current_wal_insert_lsn()::text")
        commit_wal = await case.database.scalar(
            "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),CAST(CAST(:lsn AS text) AS pg_lsn))::bigint",
            lsn=precommit_lsn,
        )
        assert 0 < commit_wal <= bound


@pytest.mark.asyncio
async def test_native_rename_census_refuses_external_dependency(monkeypatch):
    from tests.provider_directory_serving_recovery_postgres_support import ordinary_serving_case

    async with ordinary_serving_case(monkeypatch) as case:
        _names, oids = await _create_commit_roots(case)
        await case.database.status(
            f'CREATE VIEW "{case.schema}".external_profile AS SELECT npi FROM "{case.schema}".provider_directory_profile'
        )
        async with case.database.transaction():
            with pytest.raises(RuntimeError, match="drop_external_dependency"):
                await commit.rename_dependencies(case.directory, oids)


def test_catalog_consumers_share_constraint_scope():
    """All callers use the same complete catalog rather than narrower local patches."""
    assert scratch._SOURCE_CATALOG_SQL is OUTPUT_CATALOG_SQL
    assert backfill.CONTROL_CATALOG_SQL is OUTPUT_CATALOG_SQL
    assert native_copy._TARGET_CATALOG_SQL is OUTPUT_CATALOG_SQL


async def _existing_catalog_tables(database, schema):
    """Instantiate existing endpoint/child models, preserving their existing native FKs."""
    metadata = MetaData()
    for model in (
        models.ProviderDirectoryAPIEndpoint,
        models.ProviderDirectoryEndpointDataset,
        models.ProviderDirectorySource,
    ):
        model.__table__.to_metadata(metadata, schema=schema)
    for table in metadata.sorted_tables:
        await database.status(str(CreateTable(table).compile(dialect=postgresql.dialect())))
    endpoint = schema + ".provider_directory_api_endpoint"
    # This disposable-only self-reference exercises the same-table catalog case;
    # no production model, migration, relationship or write guard changes.
    await database.status(
        f"ALTER TABLE {endpoint} ADD CONSTRAINT catalog_self_reference "
        f"FOREIGN KEY(endpoint_id) REFERENCES {endpoint}(endpoint_id)"
    )
    await database.status(f"CREATE UNIQUE INDEX catalog_unowned_index ON {endpoint}(canonical_api_base)")


@pytest.mark.asyncio
async def test_native_catalog_index_constraint_owners():
    database = await _profile_test_database()
    schema = "catalog_owners_" + uuid4().hex[:16]
    try:
        await database.status(f"CREATE SCHEMA {schema}")
        await _existing_catalog_tables(database, schema)
        names = ["provider_directory_api_endpoint", "provider_directory_endpoint_dataset", "provider_directory_source"]
        unscoped = OUTPUT_CATALOG_SQL.replace("\n AND k.conrelid=c.oid AND k.contype IN ('p','u','x')", "", 1)
        with pytest.raises(asyncpg.CardinalityViolationError):
            async with database.transaction() as session:
                driver = await _copy_driver(session)
                await driver.fetch(unscoped, schema, names, 0, [])
        async with database.transaction() as session:
            driver = await _copy_driver(session)
            owner = await driver.fetchval("SELECT current_user::regrole::oid::bigint")
            catalog_entries = await driver.fetch(OUTPUT_CATALOG_SQL, schema, names, owner, [])
            assert len(catalog_entries) == len(names)
            endpoint = next(entry for entry in catalog_entries if entry["relname"] == names[0])
            constraints = decoded(endpoint["constraints"])
            primary = next(constraint for constraint in constraints if constraint["type"] == "p")
            self_reference = next(constraint for constraint in constraints if constraint["type"] == "f")
            indexes = decoded(endpoint["indexes"])
            owned = next(index for index in indexes if index["primary"])
            assert owned["constraint_oid"] == primary["oid"] != self_reference["oid"]
            assert (
                next(index for index in indexes if index["name"] == "catalog_unowned_index")["constraint_oid"] is None
            )
            for child in catalog_entries[1:]:
                assert any(constraint["type"] == "f" for constraint in decoded(child["constraints"]))
    finally:
        await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await database.disconnect()
