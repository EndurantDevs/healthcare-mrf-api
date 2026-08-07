# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for dormant FHIR formulary storage."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import re
from typing import Any
import uuid

import asyncpg
from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "20260807110000_fhir_formulary_storage_foundation.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN"
DISPOSABLE_DATABASE_RE = re.compile(
    r"(?:^test(?:[_-]|$)|(?:^|[_-])test(?:[_-]|$))",
    re.IGNORECASE,
)
DISPOSABLE_SCHEMA_RE = re.compile(r"^fhir_formulary_test_[0-9a-f]{32}$")
TABLE_NAMES = {
    "fhir_formulary_source",
    "fhir_formulary_dataset",
    "fhir_formulary_current",
    "fhir_formulary_coverage_plan",
    "fhir_formulary_coverage_plan_version",
    "fhir_formulary_dataset_coverage_plan",
    "fhir_formulary_drug_plan_alias",
    "fhir_formulary_drug_plan_alias_version",
    "fhir_formulary_dataset_alias",
    "fhir_formulary_medication",
    "fhir_formulary_alias_membership",
    "fhir_formulary_alternative",
    "fhir_formulary_checkpoint",
}
INDEX_NAMES = {
    "fhir_formulary_dataset_source_created_idx",
    "fhir_formulary_dataset_status_created_idx",
    "fhir_formulary_alias_source_plan_idx",
    "fhir_formulary_alias_version_created_idx",
    "fhir_formulary_dataset_alias_version_idx",
    "fhir_formulary_medication_rxnorm_idx",
    "fhir_formulary_medication_ndc11_idx",
    "fhir_formulary_membership_rxnorm_idx",
    "fhir_formulary_checkpoint_run_fence_idx",
}
OWNERSHIP_CONSTRAINTS = {
    "fhir_formulary_current_source_dataset_fkey",
    "fhir_formulary_dataset_previous_owner_fkey",
    "fhir_formulary_dataset_coverage_plan_owner_fkey",
    "fhir_formulary_dataset_alias_alias_owner_fkey",
    "fhir_formulary_membership_medication_owner_fkey",
    "fhir_formulary_alternative_target_owner_fkey",
    "fhir_formulary_checkpoint_dataset_owner_fkey",
}
PLAN_A = "fhir_" + "a" * 26
PLAN_B = "fhir_" + "b" * 26


def _load_migration() -> Any:
    module_spec = importlib.util.spec_from_file_location(
        "fhir_formulary_storage_postgres_proof",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _database_url() -> sa.URL:
    raw_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    database_url = make_url(raw_dsn)
    database_name = str(database_url.database or "")
    if (
        not database_url.drivername.startswith("postgresql")
        or not DISPOSABLE_DATABASE_RE.search(database_name)
        or not database_url.host
        or not database_url.username
    ):
        pytest.fail(
            f"{POSTGRES_DSN_ENV} must identify an explicit PostgreSQL test "
            "database; only a generated disposable schema is modified"
        )
    return database_url


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


async def _connect(database_url: sa.URL) -> asyncpg.Connection:
    return await asyncpg.connect(
        host=str(database_url.host),
        port=int(database_url.port or 5432),
        user=str(database_url.username),
        password=str(database_url.password or ""),
        database=str(database_url.database),
    )


async def _run_migration_action(
    engine: AsyncEngine,
    migration: Any,
    action: str,
) -> None:
    async with engine.connect() as async_connection:

        def run_action(sync_connection) -> None:
            context = MigrationContext.configure(sync_connection)
            migration.op = Operations(context)
            with context.begin_transaction():
                getattr(migration, action)()

        await async_connection.run_sync(run_action)


async def _drop_schema(engine: AsyncEngine, schema_name: str) -> None:
    if not DISPOSABLE_SCHEMA_RE.fullmatch(schema_name):
        raise RuntimeError(f"refusing to drop schema {schema_name!r}")
    async with engine.begin() as connection:
        await connection.exec_driver_sql(
            f"DROP SCHEMA IF EXISTS {_quoted(schema_name)} CASCADE"
        )
        remaining = await connection.scalar(
            sa.text("SELECT to_regnamespace(:schema_name)"),
            {"schema_name": schema_name},
        )
    assert remaining is None


async def _assert_catalog(connection, schema_name: str) -> None:
    tables = await connection.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname = $1",
        schema_name,
    )
    assert {table_row["tablename"] for table_row in tables} == TABLE_NAMES

    indexes = await connection.fetch(
        "SELECT indexname FROM pg_indexes WHERE schemaname = $1",
        schema_name,
    )
    available_indexes = {
        index_row["indexname"] for index_row in indexes
    }
    assert INDEX_NAMES <= available_indexes

    constraints = await connection.fetch(
        "SELECT constraint_name FROM information_schema.table_constraints "
        "WHERE constraint_schema = $1",
        schema_name,
    )
    available_constraints = {
        constraint_row["constraint_name"]
        for constraint_row in constraints
    }
    assert OWNERSHIP_CONSTRAINTS <= available_constraints

    function_shape = await connection.fetchrow(
        "SELECT routine.prosecdef AS security_definer, "
        "routine.proconfig AS function_config, "
        "has_function_privilege('public', routine.oid, 'EXECUTE') "
        "AS public_execute, "
        "trigger.tgenabled::text AS tgenabled "
        "FROM pg_proc AS routine "
        "JOIN pg_namespace AS namespace ON namespace.oid = routine.pronamespace "
        "JOIN pg_trigger AS trigger ON trigger.tgfoid = routine.oid "
        "WHERE namespace.nspname = $1 "
        "AND routine.proname = 'guard_fhir_formulary_checkpoint_fence' "
        "AND trigger.tgname = 'fhir_formulary_checkpoint_fence_guard'",
        schema_name,
    )
    assert function_shape is not None
    assert dict(function_shape) == {
        "security_definer": True,
        "function_config": ["search_path=pg_catalog"],
        "public_execute": False,
        "tgenabled": "O",
    }


async def _seed_synthetic_graph(connection, schema_name: str) -> None:
    """Create two isolated sources and their immutable content graph."""

    schema = _quoted(schema_name)
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_source
            (source_id, canonical_base, display_name)
        VALUES
            ('source-a', 'https://a.example.invalid/fhir', 'Synthetic A'),
            ('source-b', 'https://b.example.invalid/fhir', 'Synthetic B')"""
    )
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_dataset
            (dataset_id, source_id, run_id, previous_dataset_id, cutoff_at, status)
        VALUES
            ('dataset-a1', 'source-a', 'run-a1', NULL, '2026-08-01Z', 'building'),
            ('dataset-a2', 'source-a', 'run-a2', 'dataset-a1', '2026-08-02Z', 'verified'),
            ('dataset-b1', 'source-b', 'run-b1', NULL, '2026-08-01Z', 'building')"""
    )
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_coverage_plan
            (public_id, source_id, upstream_list_id, canonical_identity)
        VALUES ($1, 'source-a', 'list-a', 'identity-a'),
               ($2, 'source-b', 'list-b', 'identity-b')""",
        PLAN_A,
        PLAN_B,
    )
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_coverage_plan_version
            (coverage_version_id, public_id, content_hash)
        VALUES ('coverage-version-a', $1, 'coverage-hash-a'),
               ('coverage-version-b', $2, 'coverage-hash-b')""",
        PLAN_A,
        PLAN_B,
    )
    await _seed_synthetic_alias_content(connection, schema)


async def _seed_synthetic_alias_content(connection, schema: str) -> None:
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_drug_plan_alias
            (alias_id, source_id, public_id, source_plan_identifier)
        VALUES ('alias-a', 'source-a', $1, 'source-plan-a'),
               ('alias-b', 'source-b', $2, 'source-plan-b')""",
        PLAN_A,
        PLAN_B,
    )
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_drug_plan_alias_version
            (alias_version_id, source_id, alias_id, expected_count,
             membership_count, membership_hash, cutoff_at, acquisition_mode)
        VALUES
            ('alias-version-a', 'source-a', 'alias-a', 2, 2,
             'membership-hash-a', '2026-08-01Z', 'full'),
            ('alias-version-b', 'source-b', 'alias-b', 1, 1,
             'membership-hash-b', '2026-08-01Z', 'full')"""
    )
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_medication
            (medication_version_id, source_id, upstream_medication_id,
             codings_json, content_hash)
        VALUES
            ('medication-version-a', 'source-a', 'medication-a', '[]', 'med-hash-a'),
            ('medication-version-a2', 'source-a', 'medication-a2', '[]', 'med-hash-a2'),
            ('medication-version-b', 'source-b', 'medication-b', '[]', 'med-hash-b')"""
    )


async def _assert_sqlstate(
    connection,
    expected_sqlstate: str,
    statement: str,
    *arguments: object,
) -> None:
    with pytest.raises(asyncpg.PostgresError) as error:
        async with connection.transaction():
            await connection.execute(statement, *arguments)
    assert error.value.sqlstate == expected_sqlstate


async def _assert_ownership_and_reuse(connection, schema_name: str) -> None:
    """Prove owner-qualified bindings and immutable-version reuse."""

    schema = _quoted(schema_name)
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_dataset_coverage_plan
            (source_id, dataset_id, public_id, coverage_version_id)
        VALUES ('source-a', 'dataset-a1', $1, 'coverage-version-a')""",
        PLAN_A,
    )
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_dataset_alias
            (source_id, dataset_id, alias_id, alias_version_id)
        VALUES
            ('source-a', 'dataset-a1', 'alias-a', 'alias-version-a'),
            ('source-a', 'dataset-a2', 'alias-a', 'alias-version-a')"""
    )
    version_count = await connection.fetchval(
        f"SELECT count(*) FROM {schema}.fhir_formulary_drug_plan_alias_version "
        "WHERE alias_id = 'alias-a'"
    )
    binding_count = await connection.fetchval(
        f"SELECT count(*) FROM {schema}.fhir_formulary_dataset_alias "
        "WHERE alias_version_id = 'alias-version-a'"
    )
    assert (version_count, binding_count) == (1, 2)
    await _assert_medication_and_owner_rejections(connection, schema)


async def _assert_medication_and_owner_rejections(
    connection,
    schema: str,
) -> None:
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_alias_membership
            (source_id, alias_version_id, upstream_medication_id,
             medication_version_id, variant_hash)
        VALUES
            ('source-a', 'alias-version-a', 'medication-a',
             'medication-version-a', 'variant-a'),
            ('source-a', 'alias-version-a', 'medication-a2',
             'medication-version-a2', 'variant-a2'),
            ('source-b', 'alias-version-b', 'medication-b',
             'medication-version-b', 'variant-b')"""
    )
    await _assert_alternative_target_ownership(connection, schema)
    await _assert_sqlstate(
        connection,
        "23503",
        f"""INSERT INTO {schema}.fhir_formulary_alias_membership
            (source_id, alias_version_id, upstream_medication_id,
             medication_version_id, variant_hash)
        VALUES ('source-a', 'alias-version-a', 'wrong-medication-id',
                'medication-version-a', 'variant-wrong')""",
    )
    await _assert_sqlstate(
        connection,
        "23503",
        f"""INSERT INTO {schema}.fhir_formulary_current
            (source_id, dataset_id)
        VALUES ('source-b', 'dataset-a1')""",
    )
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_current
            (source_id, dataset_id)
        VALUES ('source-a', 'dataset-a2')"""
    )
    await _assert_sqlstate(
        connection,
        "23503",
        f"""INSERT INTO {schema}.fhir_formulary_dataset_alias
            (source_id, dataset_id, alias_id, alias_version_id)
        VALUES ('source-a', 'dataset-a1', 'alias-b', 'alias-version-b')""",
    )


async def _assert_alternative_target_ownership(
    connection,
    schema: str,
) -> None:
    await connection.execute(
        f"""INSERT INTO {schema}.fhir_formulary_alternative
            (alias_version_id, upstream_medication_id, raw_reference,
             resolved_medication_id, resolved)
        VALUES ('alias-version-a', 'medication-a', '#same-alias',
                'medication-a2', true)"""
    )
    await _assert_sqlstate(
        connection,
        "23503",
        f"""INSERT INTO {schema}.fhir_formulary_alternative
            (alias_version_id, upstream_medication_id, raw_reference,
             resolved_medication_id, resolved)
        VALUES ('alias-version-a', 'medication-a', '#cross-alias',
                'medication-b', true)""",
    )
    alternative_count = await connection.fetchval(
        f"SELECT count(*) FROM {schema}.fhir_formulary_alternative"
    )
    assert alternative_count == 1


async def _assert_checkpoint_fences(connection, schema_name: str) -> None:
    checkpoint = f'{_quoted(schema_name)}.fhir_formulary_checkpoint'
    await connection.execute(
        f"""INSERT INTO {checkpoint}
            (source_id, alias_id, source_plan_identifier, run_id, dataset_id,
             fence_token, cutoff_at, acquisition_mode, expected_count)
        VALUES ('source-a', 'alias-a', 'source-plan-a', 'run-a1', 'dataset-a1',
                1, '2026-08-01Z', 'full', 1)"""
    )
    await connection.execute(
        f"UPDATE {checkpoint} SET fence_token = 2, processed_count = 1 "
        "WHERE source_id = 'source-a' AND alias_id = 'alias-a' "
        "AND run_id = 'run-a1'"
    )
    await _assert_sqlstate(
        connection,
        "40001",
        f"UPDATE {checkpoint} SET fence_token = 2 "
        "WHERE source_id = 'source-a' AND alias_id = 'alias-a' "
        "AND run_id = 'run-a1'",
    )
    await _assert_sqlstate(
        connection,
        "55000",
        f"UPDATE {checkpoint} SET fence_token = 3, acquisition_mode = 'delta' "
        "WHERE source_id = 'source-a' AND alias_id = 'alias-a' "
        "AND run_id = 'run-a1'",
    )
    await _assert_checkpoint_completion_guards(connection, checkpoint)


async def _assert_checkpoint_completion_guards(connection, checkpoint: str) -> None:
    await _assert_sqlstate(
        connection,
        "23514",
        f"""INSERT INTO {checkpoint}
            (source_id, alias_id, source_plan_identifier, run_id, dataset_id,
             fence_token, cutoff_at, acquisition_mode, completed)
        VALUES ('source-a', 'alias-a', 'source-plan-a', 'invalid-complete',
                'dataset-a1', 1, '2026-08-01Z', 'full', true)""",
    )
    await _assert_sqlstate(
        connection,
        "23514",
        f"""INSERT INTO {checkpoint}
            (source_id, alias_id, source_plan_identifier, run_id, dataset_id,
             fence_token, cutoff_at, acquisition_mode, expected_count,
             processed_count, completed)
        VALUES ('source-a', 'alias-a', 'source-plan-a', 'null-hash-complete',
                'dataset-a1', 1, '2026-08-01Z', 'full', 1, 1, true)""",
    )
    await connection.execute(
        f"""INSERT INTO {checkpoint}
            (source_id, alias_id, source_plan_identifier, run_id, dataset_id,
             fence_token, cutoff_at, acquisition_mode, expected_count,
             processed_count, membership_hash, completed)
        VALUES ('source-a', 'alias-a', 'source-plan-a', 'run-a2', 'dataset-a2',
                1, '2026-08-02Z', 'reuse', 1, 1, $1, true)""",
        "a" * 64,
    )
    await _assert_sqlstate(
        connection,
        "55000",
        f"UPDATE {checkpoint} SET fence_token = 2, completed = false "
        "WHERE source_id = 'source-a' AND alias_id = 'alias-a' "
        "AND run_id = 'run-a2'",
    )
    await _assert_sqlstate(
        connection,
        "55000",
        f"DELETE FROM {checkpoint} WHERE source_id = 'source-a' "
        "AND alias_id = 'alias-a' AND run_id = 'run-a2'",
    )


async def _assert_downgraded(connection, schema_name: str) -> None:
    relations = await connection.fetchval(
        "SELECT count(*) FROM pg_class AS relation "
        "JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
        "WHERE namespace.nspname = $1 AND relation.relkind IN ('r', 'i')",
        schema_name,
    )
    function_count = await connection.fetchval(
        "SELECT count(*) FROM pg_proc AS routine "
        "JOIN pg_namespace AS namespace ON namespace.oid = routine.pronamespace "
        "WHERE namespace.nspname = $1",
        schema_name,
    )
    assert (relations, function_count) == (0, 0)


@pytest.mark.asyncio
async def test_formulary_storage_postgres_lifecycle_and_integrity(monkeypatch):
    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    migration = _load_migration()
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
        await _run_migration_action(engine, migration, "upgrade")
        connection = await _connect(database_url)
        try:
            await _assert_catalog(connection, schema_name)
            await _seed_synthetic_graph(connection, schema_name)
            await _assert_ownership_and_reuse(connection, schema_name)
            await _assert_checkpoint_fences(connection, schema_name)
            await _run_migration_action(engine, migration, "downgrade")
            await _assert_downgraded(connection, schema_name)
            await _run_migration_action(engine, migration, "upgrade")
            await _assert_catalog(connection, schema_name)
        finally:
            await connection.close()
    finally:
        await _drop_schema(engine, schema_name)
        await engine.dispose()
