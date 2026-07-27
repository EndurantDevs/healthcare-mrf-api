# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL support for the legacy PTG orphan sweeper."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import importlib.util
import os
from pathlib import Path
import uuid

from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest

from db.connection import Database
from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LegacySweepLimits,
)
from tests.ptg2_legacy_orphan_table_templates import MRF_TABLE_TEMPLATES


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260727110000_ptg2_legacy_orphan_sweep_audit.py"
)
SUFFIX_EMPTY = "1" * 32
SUFFIX_OWNED = "2" * 32
SUFFIX_BUILDING = "3" * 32
SUFFIX_DRIFT = "4" * 32
SUFFIX_LOCKED = "5" * 32
SUFFIX_FOREIGN_OWNER = "6" * 32
SUFFIX_FOREIGN_FENCE = "7" * 32
SUFFIX_SERVING_RESIDUE = "8" * 32
SUFFIX_EXTERNAL_DEPENDENCY = "9" * 32
FOREIGN_SUFFIX = "a" * 32
LIMITS = LegacySweepLimits(
    max_suffixes=20,
    max_tables=200,
    max_relations=800,
    max_bytes=2 * 1024 * 1024 * 1024,
)


@dataclass(frozen=True)
class _PostgresCase:
    database: Database
    mrf_schema: str
    control_schema: str
    migration: object


@asynccontextmanager
async def _prepared_case(monkeypatch):
    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip("requires the disposable PostgreSQL GC job")
    database = Database()
    mrf_schema = f"ptg2_legacy_sweep_test_{uuid.uuid4().hex}"
    control_schema = f"ptg2_legacy_control_test_{uuid.uuid4().hex}"
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", mrf_schema)
    monkeypatch.setenv("DB_SCHEMA", mrf_schema)
    await database.connect()
    try:
        async with database.acquire() as connection:
            await _create_schemas(connection, mrf_schema, control_schema)
            await _create_lifecycle_tables(
                connection,
                mrf_schema,
                control_schema,
            )
        await _run_migration(database, migration, "upgrade")
        yield _PostgresCase(
            database=database,
            mrf_schema=mrf_schema,
            control_schema=control_schema,
            migration=migration,
        )
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(
                    f"DROP SCHEMA IF EXISTS {_q(control_schema)} CASCADE"
                )
                await connection.status(
                    f"DROP SCHEMA IF EXISTS {_q(mrf_schema)} CASCADE"
                )
        finally:
            await database.disconnect()


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "ptg2_legacy_orphan_sweep_migration_test",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


async def _run_migration(
    database: Database,
    migration,
    action: str,
) -> None:
    assert database.engine is not None
    async with database.engine.begin() as connection:

        def run(sync_connection) -> None:
            context = MigrationContext.configure(sync_connection)
            migration.op = Operations(context)
            getattr(migration, action)()

        await connection.run_sync(run)


async def _create_schemas(
    connection,
    mrf_schema: str,
    control_schema: str,
) -> None:
    await connection.status(f"CREATE SCHEMA {_q(mrf_schema)}")
    await connection.status(f"CREATE SCHEMA {_q(control_schema)}")


def _mrf_table_statements(schema_name: str) -> tuple[str, ...]:
    schema = _q(schema_name)
    return tuple(
        statement.format(schema=schema)
        for statement in MRF_TABLE_TEMPLATES
    )


def _control_table_statements(schema_name: str) -> tuple[str, ...]:
    schema = _q(schema_name)
    return (
        f"""
        CREATE TABLE {schema}.source_file_import (
            source_file_import_id text PRIMARY KEY,
            status text NOT NULL,
            snapshot_id text
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg_file_placement (
            placement_id text PRIMARY KEY,
            source_file_import_id text NOT NULL,
            status text NOT NULL,
            snapshot_id text
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg_route_index (
            snapshot_id text NOT NULL,
            status text NOT NULL
        )
        """,
        f"""
        CREATE TABLE {schema}.hp_plan_release_binding (
            snapshot_id text NOT NULL
        )
        """,
        f"CREATE TABLE {schema}.hp_snapshot_pin (snapshot_id text NOT NULL)",
    )


async def _create_lifecycle_tables(
    connection,
    mrf_schema: str,
    control_schema: str,
) -> None:
    for statement in (
        *_mrf_table_statements(mrf_schema),
        *_control_table_statements(control_schema),
    ):
        await connection.status(statement)


async def _create_root(
    connection,
    schema_name: str,
    suffix: str,
    *,
    populated: bool,
) -> str:
    table_name = f"ptg_file_{suffix}"
    table = f"{_q(schema_name)}.{_q(table_name)}"
    await connection.status(
        f"CREATE TABLE {table} (ordinal bigint PRIMARY KEY)"
    )
    if populated:
        await connection.status(f"INSERT INTO {table} VALUES (1)")
    return table_name


async def _seed_candidate_roots(
    connection,
    mrf_schema: str,
) -> None:
    await _create_root(
        connection,
        mrf_schema,
        SUFFIX_EMPTY,
        populated=False,
    )
    await _create_root(
        connection,
        mrf_schema,
        SUFFIX_OWNED,
        populated=True,
    )
    await _create_root(
        connection,
        mrf_schema,
        SUFFIX_BUILDING,
        populated=False,
    )


async def _seed_candidate_snapshots(
    connection,
    mrf_schema: str,
) -> None:
    mrf = _q(mrf_schema)
    await connection.status(
        f"""
        INSERT INTO {mrf}.ptg2_snapshot (
            snapshot_id, import_run_id, status, manifest
        ) VALUES
            (
                'snapshot-owned', 'ptg2:{SUFFIX_OWNED}', 'failed',
                jsonb_build_object(
                    'legacy_table_suffix', '{SUFFIX_OWNED}'
                )
            ),
            (
                'snapshot-building', 'ptg2:{SUFFIX_BUILDING}', 'building',
                jsonb_build_object(
                    'legacy_table_suffix', '{SUFFIX_BUILDING}'
                )
            )
        """
    )
    await connection.status(
        f"""
        INSERT INTO {mrf}.ptg2_import_run (import_run_id, status)
        VALUES
            ('ptg2:{SUFFIX_OWNED}', 'failed'),
            ('ptg2:{SUFFIX_BUILDING}', 'building')
        """
    )


async def _seed_candidate_control_owners(
    connection,
    mrf_schema: str,
    control_schema: str,
) -> None:
    mrf = _q(mrf_schema)
    control = _q(control_schema)
    await connection.status(
        f"""
        INSERT INTO {mrf}.import_run (
            run_id, source_file_import_id, status, snapshot_id
        ) VALUES (
            'mirror-owned', '{SUFFIX_OWNED}', 'failed', 'snapshot-owned'
        )
        """
    )
    await connection.status(
        f"""
        INSERT INTO {control}.source_file_import (
            source_file_import_id, status, snapshot_id
        ) VALUES
            ('{SUFFIX_OWNED}', 'failed', 'snapshot-owned'),
            ('{SUFFIX_BUILDING}', 'failed', 'snapshot-building')
        """
    )
    await connection.status(
        f"""
        INSERT INTO {control}.ptg_file_placement (
            placement_id, source_file_import_id, status, snapshot_id
        ) VALUES (
            'placement-owned', '{SUFFIX_OWNED}', 'inactive', 'snapshot-owned'
        )
        """
    )


async def _seed_candidates(
    connection,
    mrf_schema: str,
    control_schema: str,
) -> None:
    await _seed_candidate_roots(connection, mrf_schema)
    await _seed_candidate_snapshots(connection, mrf_schema)
    await _seed_candidate_control_owners(
        connection,
        mrf_schema,
        control_schema,
    )


async def _seed_terminal_owner(
    connection,
    mrf_schema: str,
    control_schema: str,
    suffix: str,
) -> str:
    snapshot_id = f"snapshot-{suffix[0]}"
    mrf = _q(mrf_schema)
    control = _q(control_schema)
    await _create_root(
        connection,
        mrf_schema,
        suffix,
        populated=False,
    )
    await connection.status(
        f"""
        INSERT INTO {mrf}.ptg2_snapshot (
            snapshot_id, import_run_id, status, manifest
        ) VALUES (
            :snapshot_id,
            :internal_run_id,
            'failed',
            jsonb_build_object(
                'legacy_table_suffix',
                CAST(:suffix AS text)
            )
        )
        """,
        snapshot_id=snapshot_id,
        internal_run_id=f"ptg2:{suffix}",
        suffix=suffix,
    )
    await connection.status(
        f"""
        INSERT INTO {mrf}.ptg2_import_run (import_run_id, status)
        VALUES (:internal_run_id, 'failed')
        """,
        internal_run_id=f"ptg2:{suffix}",
    )
    await connection.status(
        f"""
        INSERT INTO {control}.source_file_import (
            source_file_import_id, status, snapshot_id
        ) VALUES (:suffix, 'failed', :snapshot_id)
        """,
        suffix=suffix,
        snapshot_id=snapshot_id,
    )
    return snapshot_id


async def _has_relation(
    connection,
    schema_name: str,
    relation_name: str,
) -> bool:
    return bool(
        await connection.scalar(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM pg_class AS relation_record
                  JOIN pg_namespace AS namespace_record
                    ON namespace_record.oid = relation_record.relnamespace
                 WHERE namespace_record.nspname = :schema_name
                   AND relation_record.relname = :relation_name
            )
            """,
            schema_name=schema_name,
            relation_name=relation_name,
        )
    )
