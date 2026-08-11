"""Typed PostgreSQL fixtures for legacy V3 metadata-repair tests."""

from __future__ import annotations

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator

import asyncpg

from db.migration_ptg2_legacy_v3_metadata_reconcile import (
    ATTEMPT_AUTHORITY_SERVICE_NAME,
)
from process.ptg_parts import ptg2_legacy_v3_metadata_reconcile as reconcile
from process.ptg_parts import ptg_source_attempt_actions as source_actions
from tests.ptg2_v4_stale_metadata_postgres_support import (
    _load_attempt_migration,
    _migration_sql,
    create_stale_schema,
    database_for_dsn,
    postgres_dsn,
    quoted,
)


SNAPSHOT_ID = "ptg2:202607:synthetic-legacy-v3"
INTERNAL_RUN_ID = "ptg2:" + "d" * 32
OUTER_RUN_ID = "run_synthetic_legacy_v3"
SOURCE_IMPORT_ID = "synthetic-source-import-v3"


@dataclass(frozen=True)
class LegacyV3PostgresContext:
    connection: asyncpg.Connection
    test_database: Any
    dsn: str
    schema_name: str
    control_schema: str

    @property
    def schema(self) -> str:
        return quoted(self.schema_name)

    @property
    def control(self) -> str:
        return quoted(self.control_schema)


class _SqlRecorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(str(statement))


async def apply_reconcile_migration(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    migration = _load_attempt_migration(
        "20260801140000_ptg2_legacy_v3_metadata_reconcile.py"
    )
    async with connection.transaction():
        for statement in _migration_sql(migration, schema_name):
            await connection.execute(statement)


def reconcile_downgrade_sql(schema_name: str) -> tuple[str, ...]:
    migration = _load_attempt_migration(
        "20260801140000_ptg2_legacy_v3_metadata_reconcile.py"
    )
    predecessor = _load_attempt_migration(
        "20260724100000_ptg2_v4_attempt_fence.py"
    )
    recorder = _SqlRecorder()
    original_op = migration.op
    original_schema = migration._schema
    original_restore = migration._restore_predecessor_guard
    predecessor_op = predecessor.op
    migration.op = recorder
    migration._schema = lambda: schema_name

    def restore_guard(restored_schema: str) -> None:
        predecessor.op = recorder
        try:
            predecessor._create_guard_function(restored_schema)
        finally:
            predecessor.op = predecessor_op

    migration._restore_predecessor_guard = restore_guard
    try:
        migration.downgrade()
    finally:
        migration.op = original_op
        migration._schema = original_schema
        migration._restore_predecessor_guard = original_restore
    return tuple(recorder.statements)


async def apply_reconcile_downgrade(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    async with connection.transaction():
        for statement in reconcile_downgrade_sql(schema_name):
            await connection.execute(statement)


async def create_control_tables(
    connection: asyncpg.Connection,
    control_schema: str,
) -> None:
    control = quoted(control_schema)
    await connection.execute(f"CREATE SCHEMA {control}")
    await connection.execute(
        f"""
        CREATE TABLE {control}.run_mirror (
            run_id varchar(64) PRIMARY KEY, importer varchar(64),
            status varchar(32), params jsonb, snapshot_id varchar(96),
            heartbeat_at timestamp, finished_at timestamp,
            synced_at timestamp, created_at timestamp, metrics jsonb
        );
        CREATE TABLE {control}.source_file_import (
            source_file_import_id varchar(64) PRIMARY KEY,
            status varchar(32), engine_run_id varchar(64),
            snapshot_id varchar(96), metrics jsonb
        );
        CREATE TABLE {control}.ptg_file_placement (
            placement_id varchar(64) PRIMARY KEY,
            source_file_import_id varchar(64), snapshot_id varchar(96),
            status varchar(32)
        );
        """
    )


async def create_outer_run_table(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    await connection.execute(
        f"""
        CREATE TABLE {schema}.import_run (
            run_id varchar(64) PRIMARY KEY, importer varchar(64),
            status varchar(32), params json,
            source_file_import_id varchar(64), import_id varchar(64),
            retry_of_run_id varchar(64), phase_detail varchar(128),
            created_at timestamp, started_at timestamp,
            finished_at timestamp, heartbeat_at timestamp,
            progress json, snapshot_id varchar(96), metrics json
        )
        """
    )


def source_options() -> str:
    return json.dumps(
        {
            "storage_generation": "shared_blocks_v3",
            "snapshot_arch": "postgres_binary_v3",
            "source_file_import_id": SOURCE_IMPORT_ID,
        }
    )


def source_params() -> str:
    return json.dumps(
        {
            "source_file_import_id": SOURCE_IMPORT_ID,
            "import_id": SOURCE_IMPORT_ID,
        }
    )


async def seed_internal_pair(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    await connection.execute(
        f"""
        INSERT INTO {schema}.ptg2_import_run (
            import_run_id, status, started_at, heartbeat_at,
            options, report, error
        ) VALUES (
            $1, 'running', timezone('UTC', now()) - INTERVAL '8 hours',
            timezone('UTC', now()) - INTERVAL '8 hours', $2::json,
            '{{"synthetic":"preserved"}}'::json,
            'synthetic-preserved-error'
        )
        """,
        INTERNAL_RUN_ID,
        source_options(),
    )
    await connection.execute(
        f"""
        INSERT INTO {schema}.ptg2_snapshot (
            snapshot_id, import_run_id, status, created_at, manifest
        ) VALUES (
            $1, $2, 'building',
            timezone('UTC', now()) - INTERVAL '8 hours', '{{}}'::json
        )
        """,
        SNAPSHOT_ID,
        INTERNAL_RUN_ID,
    )


async def seed_retained_attachments(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    for table_name in (
        "ptg2_v3_snapshot_scope",
        "ptg2_v3_snapshot_plan_scope",
        "ptg2_v3_snapshot_source",
    ):
        await connection.execute(
            f"INSERT INTO {schema}.{table_name} (snapshot_id) VALUES ($1)",
            SNAPSHOT_ID,
        )
    await connection.execute(
        f"INSERT INTO {schema}.ptg2_plan_month "
        "(plan_month_id, snapshot_id) VALUES ('synthetic-plan-month', $1)",
        SNAPSHOT_ID,
    )


async def seed_outer_run(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    await connection.execute(
        f"""
        INSERT INTO {schema}.import_run (
            run_id, importer, status, params, source_file_import_id,
            import_id, created_at, finished_at, heartbeat_at, metrics
        ) VALUES (
            $1, 'ptg', 'failed', $2::json, $3, $3,
            timezone('UTC', now()) - INTERVAL '8 hours',
            timezone('UTC', now()) - INTERVAL '7 hours',
            timezone('UTC', now()) - INTERVAL '7 hours', '{{}}'::json
        )
        """,
        OUTER_RUN_ID,
        source_params(),
        SOURCE_IMPORT_ID,
    )


async def seed_control_lineage(
    connection: asyncpg.Connection,
    control_schema: str,
) -> None:
    control = quoted(control_schema)
    await connection.execute(
        f"""
        INSERT INTO {control}.run_mirror (
            run_id, importer, status, params, finished_at,
            synced_at, created_at, metrics
        ) VALUES (
            $1, 'ptg', 'failed', $2::jsonb,
            timezone('UTC', now()) - INTERVAL '7 hours',
            timezone('UTC', now()) - INTERVAL '7 hours',
            timezone('UTC', now()) - INTERVAL '8 hours', '{{}}'::jsonb
        )
        """,
        OUTER_RUN_ID,
        source_params(),
    )
    await connection.execute(
        f"INSERT INTO {control}.source_file_import "
        "(source_file_import_id, status, engine_run_id, metrics) "
        "VALUES ($1, 'failed', $2, '{}'::jsonb)",
        SOURCE_IMPORT_ID,
        OUTER_RUN_ID,
    )


async def seed_attempt_authority_capability(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    await connection.execute(
        f"""
        INSERT INTO {schema}.ptg_source_attempt_guard_capability (
            service_name, protocol_version, lock_namespace,
            hash_seed, database_name
        ) VALUES (
            '{ATTEMPT_AUTHORITY_SERVICE_NAME}',
            'ptg_source_attempt_fence_v1',
            'ptg-source-import:-1-attempt', 0, current_database()
        ) ON CONFLICT (service_name) DO NOTHING
        """
    )


async def seed_source_event(
    connection: asyncpg.Connection,
    schema_name: str,
    *,
    outer_run_id: str = OUTER_RUN_ID,
) -> None:
    schema = quoted(schema_name)
    await connection.execute(
        f"""
        INSERT INTO {schema}.ptg_source_attempt_event (
            protocol_version, source_file_import_id, event_kind,
            outer_run_id, state_digest
        ) VALUES (
            'ptg_source_attempt_fence_v1', $1,
            'start_admitted', $2, repeat('1', 64)
        )
        """,
        SOURCE_IMPORT_ID,
        outer_run_id,
    )


async def seed_ready_v3_target(
    context: LegacyV3PostgresContext,
    *,
    include_source_event: bool = True,
) -> None:
    await create_outer_run_table(context.connection, context.schema_name)
    await seed_internal_pair(context.connection, context.schema_name)
    await seed_retained_attachments(context.connection, context.schema_name)
    await seed_outer_run(context.connection, context.schema_name)
    await seed_control_lineage(context.connection, context.control_schema)
    await seed_attempt_authority_capability(
        context.connection,
        context.schema_name,
    )
    if include_source_event:
        await seed_source_event(context.connection, context.schema_name)


async def row_versions(
    context: LegacyV3PostgresContext,
) -> tuple[Any, ...]:
    return tuple(
        await context.connection.fetchrow(
            f"""
            SELECT
              (SELECT xmin::text FROM {context.schema}.ptg2_snapshot
                WHERE snapshot_id = $1),
              (SELECT xmin::text FROM {context.schema}.ptg2_import_run
                WHERE import_run_id = $2),
              (SELECT xmin::text FROM {context.schema}.ptg2_v3_snapshot_scope
                WHERE snapshot_id = $1),
              (SELECT xmin::text FROM {context.schema}.ptg2_v3_snapshot_plan_scope
                WHERE snapshot_id = $1),
              (SELECT xmin::text FROM {context.schema}.ptg2_v3_snapshot_source
                WHERE snapshot_id = $1),
              (SELECT xmin::text FROM {context.schema}.ptg2_plan_month
                WHERE snapshot_id = $1)
            """,
            SNAPSHOT_ID,
            INTERNAL_RUN_ID,
        )
    )


def operational_absence(_outer_runs: Any) -> dict[str, Any]:
    """Return the exact synthetic proof that no external work exists."""

    return {
        "contract": "ptg_source_attempt_external_absence_v1",
        "job_identity_count": 1,
        "queue_count": 6,
        "queue_memberships": 0,
        "redis_exact_key_count": 0,
        "worker_running_count": 0,
        "worker_present_count": 0,
        "exact_external_absence": True,
    }


def patch_operational_absence(monkeypatch: Any) -> None:
    """Use the synthetic external-absence proof in the reconciler."""

    async def exact_absence(outer_runs, _event_rows=None):
        return operational_absence(outer_runs)

    monkeypatch.setattr(reconcile, "load_exact_operational_absence", exact_absence)


async def create_test_context(monkeypatch: Any) -> LegacyV3PostgresContext:
    dsn = postgres_dsn()
    schema_name = "ptg_v3_reconcile_" + uuid.uuid4().hex[:12]
    control_schema = "ptg_v3_control_" + uuid.uuid4().hex[:12]
    connection = await asyncpg.connect(dsn)
    context = LegacyV3PostgresContext(
        connection=connection,
        test_database=database_for_dsn(dsn),
        dsn=dsn,
        schema_name=schema_name,
        control_schema=control_schema,
    )
    await create_stale_schema(connection, schema_name)
    await apply_reconcile_migration(connection, schema_name)
    await create_control_tables(connection, control_schema)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    monkeypatch.setenv(
        reconcile._ATTEMPT_AUTHORITY_SCHEMA_ENV,
        control_schema,
    )
    monkeypatch.setattr(reconcile, "db", context.test_database)
    monkeypatch.setattr(source_actions, "db", context.test_database)
    return context


async def close_test_context(context: LegacyV3PostgresContext) -> None:
    await context.test_database.disconnect()
    await context.connection.execute(
        f"DROP SCHEMA IF EXISTS {context.control} CASCADE"
    )
    await context.connection.execute(
        f"DROP SCHEMA IF EXISTS {context.schema} CASCADE"
    )
    await context.connection.close()


@asynccontextmanager
async def legacy_v3_postgres_context(
    monkeypatch: Any,
) -> AsyncIterator[LegacyV3PostgresContext]:
    context = await create_test_context(monkeypatch)
    try:
        yield context
    finally:
        await close_test_context(context)


__all__ = [
    "INTERNAL_RUN_ID",
    "OUTER_RUN_ID",
    "SNAPSHOT_ID",
    "SOURCE_IMPORT_ID",
    "LegacyV3PostgresContext",
    "apply_reconcile_migration",
    "legacy_v3_postgres_context",
    "operational_absence",
    "patch_operational_absence",
    "quoted",
    "apply_reconcile_downgrade",
    "reconcile_downgrade_sql",
    "row_versions",
    "seed_attempt_authority_capability",
    "seed_ready_v3_target",
    "seed_source_event",
    "source_options",
    "source_params",
]
