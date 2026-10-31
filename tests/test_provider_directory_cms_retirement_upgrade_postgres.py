# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original Admin replacement and partial-DDL rollback on real retained helper rows."""

import json
import os
from contextlib import asynccontextmanager

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from db import migration_cms_retirement_resolution as migration
from tests.test_provider_directory_cms_retirement_resolution_postgres import (
    _check_consumers,
)
from tests.test_provider_directory_cms_retirement_resolution_postgres import (
    _snapshots as _raw_snapshots,
)
from tests.test_provider_directory_cms_retirement_resolution_postgres import (
    retirement_corpus as retirement_corpus,
)
from tests.tin_npi_connector_postgres_support import POSTGRES_DSN_ENV

pytestmark = pytest.mark.asyncio(loop_scope="module")


async def _snapshots(driver, schema):
    """Decode only known JSON fields identically across raw and SQLAlchemy codecs."""
    rows_by_name = await _raw_snapshots(driver, schema)
    for row in rows_by_name.values():
        for field in ("identity", "hooks", "dependencies"):
            if isinstance(row[field], str):
                row[field] = json.loads(row[field])
    return rows_by_name


def _pins(snapshots):
    expected_by_name = {}
    for name, row in snapshots.items():
        identity = json.loads(row["identity"]) if isinstance(row["identity"], str) else row["identity"]
        expected_by_name[name] = dict(oid=int(identity["oid"]), owner_oid=int(identity["proowner"]))
    return expected_by_name


@asynccontextmanager
async def _operator(*, isolation="READ COMMITTED", readonly=False):
    """Use the same explicit disposable DB with a fresh genuine Admin transaction."""
    url = make_url(os.environ[POSTGRES_DSN_ENV]).set(drivername="postgresql+asyncpg")
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                await connection.exec_driver_sql(
                    "SET TRANSACTION ISOLATION LEVEL " + isolation + (" READ ONLY" if readonly else " READ WRITE")
                )
                await connection.exec_driver_sql("SET LOCAL search_path=pg_catalog,pg_temp")
                await connection.exec_driver_sql("SET LOCAL jit=off")
                driver = (await connection.get_raw_connection()).driver_connection
                yield connection, driver
            finally:
                if transaction.is_active:
                    await transaction.rollback()
    finally:
        await engine.dispose()


async def _upgrade(connection, schema, pins):
    await connection.run_sync(lambda sync: migration.upgrade_cms_retirement_resolution(sync, schema, expected=pins))


@pytest.mark.parametrize("isolation", ["READ COMMITTED", "SERIALIZABLE"])
async def test_native_installer_is_repeatable_and_preserves_complete_original_identity(retirement_corpus, isolation):
    """All six real replacements and their repeat leave rollback entirely caller-owned."""
    scenario = retirement_corpus[0]
    before = await _snapshots(scenario.connection, scenario.schema_name)
    async with _operator(isolation=isolation) as (connection, driver):
        assert await driver.fetchval("SHOW transaction_isolation") == isolation.lower()
        await _upgrade(connection, scenario.schema_name, _pins(before))
        first = await _snapshots(driver, scenario.schema_name)
        await _upgrade(connection, scenario.schema_name, _pins(before))
        assert await _snapshots(driver, scenario.schema_name) == first
        assert connection.in_transaction()
        for name, original in before.items():
            assert first[name]["prosrc"] == migration.retirement_resolution_bodies(scenario.schema_name)[name][1]
            for field in ("identity", "hooks", "dependencies", "description"):
                assert first[name][field] == original[field]
        await _check_consumers(driver, scenario.schema_name)
    assert await _snapshots(scenario.connection, scenario.schema_name) == before


@pytest.mark.parametrize("isolation", ["READ COMMITTED", "SERIALIZABLE"])
async def test_native_savepoint_restores_two_actual_replacements_without_ending_outer_transaction(
    retirement_corpus, isolation
):
    """A real post-DDL interruption rolls back the whole batch before normal retry."""
    scenario = retirement_corpus[0]
    before = await _snapshots(scenario.connection, scenario.schema_name)
    failure = RuntimeError("after two real retirement body replacements")
    async with _operator(isolation=isolation) as (connection, driver):
        assert await driver.fetchval("SHOW transaction_isolation") == isolation.lower()
        executed_statements = []

        def interrupted(sync):
            class InterruptedConnection:
                def __getattr__(self, name):
                    return getattr(sync, name)

                def exec_driver_sql(self, statement):
                    result = sync.exec_driver_sql(statement)
                    if statement.lstrip().startswith("CREATE OR REPLACE FUNCTION"):
                        executed_statements.append(statement)
                        if len(executed_statements) == 2:
                            raise failure
                    return result

            migration.upgrade_cms_retirement_resolution(
                InterruptedConnection(), scenario.schema_name, expected=_pins(before)
            )

        with pytest.raises(RuntimeError) as caught:
            await connection.run_sync(interrupted)
        assert caught.value is failure and len(executed_statements) == 2 and connection.in_transaction()
        assert await _snapshots(driver, scenario.schema_name) == before
        await _upgrade(connection, scenario.schema_name, _pins(before))
        await _check_consumers(driver, scenario.schema_name)
    assert await _snapshots(scenario.connection, scenario.schema_name) == before


@pytest.mark.parametrize(
    "isolation,readonly", [("REPEATABLE READ", False), ("READ COMMITTED", True), ("SERIALIZABLE", True)]
)
async def test_native_installer_refuses_other_admin_transaction_modes(retirement_corpus, isolation, readonly):
    scenario = retirement_corpus[0]
    before = await _snapshots(scenario.connection, scenario.schema_name)
    async with _operator(isolation=isolation, readonly=readonly) as (connection, driver):
        with pytest.raises(RuntimeError, match="cms_retirement_resolution_original_admin_required"):
            await _upgrade(connection, scenario.schema_name, _pins(before))
        assert connection.in_transaction()
        assert await _snapshots(driver, scenario.schema_name) == before
    assert await _snapshots(scenario.connection, scenario.schema_name) == before


async def test_native_import_run_successor_preserves_observation_and_ordinary_update(retirement_corpus):
    """The capacity observer must report actual typed fingerprints and retain hook checks."""
    from db.migration_provider_directory_terminal_root_retirement_guards import IMPORT_RUN_GUARD
    from process import provider_directory_import_run_guards as guards
    from tests.test_provider_directory_import_run_guards import (
        _assert_captured_guards,
        _install,
        _measure_ordinary_update,
    )
    from tests.test_provider_directory_profile_initial_migration import _CatalogDatabase

    schema = "import_guards"
    async with _operator() as (connection, driver):
        await connection.run_sync(_install)
        relation_by_field = dict(
            schema_name=schema,
            relation_oid=await driver.fetchval("SELECT 'import_guards.import_run'::regclass::oid::bigint"),
        )
        database = _CatalogDatabase(connection)
        original = await guards.assert_import_run_guards(database, relation_by_field)
        signature = schema + "." + IMPORT_RUN_GUARD + "()"
        definition = await driver.fetchval("SELECT pg_catalog.pg_get_functiondef($1::regprocedure)", signature)
        previous, typed = migration.retirement_resolution_bodies(schema)[IMPORT_RUN_GUARD]
        assert definition.count(previous) == 1
        await connection.exec_driver_sql(definition.replace(previous, typed, 1))
        current = await guards.assert_import_run_guards(database, relation_by_field)
        assert len(current) == len(original) == 25 and current != original
        await _assert_captured_guards(database, relation_by_field, current)
        await _measure_ordinary_update(connection)
        for before, after in zip(original, current, strict=True):
            if after.get("function_name") == IMPORT_RUN_GUARD and after["dependency_kind"] == "function":
                assert after["source_sha256"] == guards._source_hash(typed, schema)
                assert {
                    key: field for key, field in before.items() if key not in {"source_sha256", "raw_source_sha256"}
                } == {key: field for key, field in after.items() if key not in {"source_sha256", "raw_source_sha256"}}
            else:
                assert after == before
