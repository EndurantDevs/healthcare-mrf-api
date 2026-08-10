# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for immutable formulary source artifacts."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import uuid

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from tests.test_formulary_fhir_storage_postgres import _connect
from tests.test_formulary_fhir_storage_postgres import _database_url
from tests.test_formulary_fhir_storage_postgres import _drop_schema
from tests.test_formulary_fhir_storage_postgres import _load_migration
from tests.test_formulary_fhir_storage_postgres import _quoted
from tests.test_formulary_fhir_storage_postgres import _run_migration_action


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions" / (
    "20260810030000_fhir_formulary_source_artifact.py"
)
SOURCE_ID = "artifact-source"
SET_SHA256 = "a" * 64
OBSERVATION_SHA256 = "b" * 64
PROJECTION_SHA256 = "c" * 64


def _artifact_migration():
    module_spec = importlib.util.spec_from_file_location(
        "fhir_formulary_source_artifact_postgres_proof",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def _assert_sqlstate(
    connection: asyncpg.Connection,
    sqlstate: str,
    statement: str,
) -> None:
    with pytest.raises(asyncpg.PostgresError) as caught_error:
        async with connection.transaction():
            await connection.execute(statement)
    assert caught_error.value.sqlstate == sqlstate


def _artifact_insert_sql(schema_name: str, index: int, *, status: str) -> str:
    schema = _quoted(schema_name)
    artifact_sha256 = "NULL"
    artifact_byte_count = "NULL"
    verified_at = "NULL"
    if status == "verified":
        artifact_sha256 = f"'{index + 8:064x}'"
        artifact_byte_count = str(index + 7)
        verified_at = "transaction_timestamp()"
    return f"""
        INSERT INTO {schema}.fhir_formulary_source_artifact
          (source_id, source_file_set_sha256, source_file_id,
           raw_listing_projection_sha256, family, file_name, source_url,
           catalog_modified_at, catalog_entry_sha256, expected_byte_count,
           artifact_sha256, artifact_byte_count, status, verified_at)
        VALUES
          ('{SOURCE_ID}', '{SET_SHA256}', '{index + 1:064x}',
           '{PROJECTION_SHA256}', 'ifp', 'drug-{index}.json',
           'https://example.invalid/drug-{index}.json',
           '2026-08-10T00:00:00Z', '{index + 4:064x}', {index + 7},
           {artifact_sha256}, {artifact_byte_count}, '{status}', {verified_at})
    """


async def _seed_exact_artifact_set(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    schema = _quoted(schema_name)
    async with connection.transaction():
        await connection.execute(
            f"INSERT INTO {schema}.fhir_formulary_source "
            "(source_id, canonical_base, display_name) VALUES "
            f"('{SOURCE_ID}', 'https://example.invalid', 'Artifact source')"
        )
        await connection.execute(
            f"INSERT INTO {schema}.fhir_formulary_source_artifact_set "
            "(source_id, source_file_set_sha256, "
            "raw_listing_projection_sha256, expected_file_count) VALUES "
            f"('{SOURCE_ID}', '{SET_SHA256}', '{PROJECTION_SHA256}', 2)"
        )
        await connection.execute(
            f"INSERT INTO {schema}.fhir_formulary_source_artifact_observation "
            "(source_id, source_observation_sha256, source_file_set_sha256, "
            "raw_listing_projection_sha256) VALUES "
            f"('{SOURCE_ID}', '{OBSERVATION_SHA256}', '{SET_SHA256}', "
            f"'{PROJECTION_SHA256}')"
        )
        await connection.execute(_artifact_insert_sql(schema_name, 0, status="pending"))
        await connection.execute(_artifact_insert_sql(schema_name, 1, status="pending"))


async def _assert_trigger_catalog(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    trigger_rows = await connection.fetch(
        "SELECT trigger.tgname, trigger.tgenabled::text, "
        "trigger.tgdeferrable, trigger.tginitdeferred, "
        "has_function_privilege('public', routine.oid, 'EXECUTE') "
        "AS public_execute FROM pg_trigger AS trigger "
        "JOIN pg_class AS relation ON relation.oid = trigger.tgrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
        "JOIN pg_proc AS routine ON routine.oid = trigger.tgfoid "
        "WHERE namespace.nspname = $1 AND NOT trigger.tgisinternal",
        schema_name,
    )
    trigger_by_name = {trigger_row["tgname"]: trigger_row for trigger_row in trigger_rows}
    census_names = {
        "fhir_formulary_source_artifact_set_census",
        "fhir_formulary_source_artifact_census",
    }
    immutable_names = {
        "fhir_formulary_source_artifact_set_guard",
        "fhir_formulary_source_artifact_set_guard_truncate",
        "fhir_formulary_source_artifact_observation_guard",
        "fhir_formulary_source_artifact_observation_guard_truncate",
        "fhir_formulary_source_artifact_guard",
        "fhir_formulary_source_artifact_guard_truncate",
        *census_names,
    }
    assert immutable_names <= set(trigger_by_name)
    assert all(
        trigger_by_name[trigger_name]["tgenabled"] == "A"
        and trigger_by_name[trigger_name]["public_execute"] is False
        for trigger_name in immutable_names
    )
    assert all(
        trigger_by_name[trigger_name]["tgdeferrable"] is True
        and trigger_by_name[trigger_name]["tginitdeferred"] is True
        for trigger_name in census_names
    )


async def _assert_one_time_fill_and_immutability(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    schema = _quoted(schema_name)
    artifact = f"{schema}.fhir_formulary_source_artifact"
    observation = f"{schema}.fhir_formulary_source_artifact_observation"
    artifact_set = f"{schema}.fhir_formulary_source_artifact_set"
    await connection.execute(
        f"UPDATE {artifact} SET artifact_sha256 = '{8:064x}', "
        "artifact_byte_count = 7, status = 'verified', "
        "verified_at = transaction_timestamp() WHERE "
        f"source_id = '{SOURCE_ID}' AND source_file_id = '{1:064x}'"
    )
    await _assert_sqlstate(
        connection,
        "55000",
        f"UPDATE {artifact} SET artifact_sha256 = '{9:064x}' WHERE "
        f"source_id = '{SOURCE_ID}' AND source_file_id = '{1:064x}'",
    )
    await _assert_sqlstate(
        connection,
        "55000",
        f"UPDATE {observation} SET source_file_set_sha256 = '{9:064x}'",
    )
    await _assert_sqlstate(
        connection,
        "55000",
        f"DELETE FROM {artifact_set}",
    )
    await _assert_sqlstate(connection, "55000", f"TRUNCATE {artifact}")
    await _assert_sqlstate(
        connection,
        "55000",
        _artifact_insert_sql(schema_name, 2, status="verified"),
    )


async def _assert_exact_census_is_deferred(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    await _assert_sqlstate(
        connection,
        "23514",
        _artifact_insert_sql(schema_name, 2, status="pending"),
    )
    schema = _quoted(schema_name)
    stored_count = await connection.fetchval(
        f"SELECT count(*) FROM {schema}.fhir_formulary_source_artifact"
    )
    assert stored_count == 2


@pytest.mark.asyncio
async def test_source_artifact_postgres_census_and_immutability(monkeypatch):
    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    artifact_migration = _artifact_migration()
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    try:
        async with engine.begin() as engine_connection:
            await engine_connection.exec_driver_sql(
                f"CREATE SCHEMA {_quoted(schema_name)}"
            )
        await _run_migration_action(engine, _load_migration(), "upgrade")
        await _run_migration_action(engine, artifact_migration, "upgrade")
        connection = await _connect(database_url)
        try:
            await _seed_exact_artifact_set(connection, schema_name)
            await _assert_trigger_catalog(connection, schema_name)
            await _assert_one_time_fill_and_immutability(connection, schema_name)
            await _assert_exact_census_is_deferred(connection, schema_name)
            with pytest.raises(DBAPIError, match="downgrade_blocked"):
                await _run_migration_action(engine, artifact_migration, "downgrade")
        finally:
            await connection.close()
    finally:
        await _drop_schema(engine, schema_name)
        await engine.dispose()
