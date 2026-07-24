# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL schema and evidence helpers for stale V4 metadata tests."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import sys

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process.ptg_parts.ptg2_v4_attempt_registry import ATTEMPT_ATTACHMENTS
from process.ptg_parts.ptg2_v4_stale_metadata_fence import (
    StaleMetadataFenceError,
    guard_attempt_rows,
)


SNAPSHOT_ID = "ptg2:202607:synthetic-stale"
INTERNAL_RUN_ID = "ptg2:synthetic-stale-run"

_SCHEMA_DDL = (
    """
    CREATE TABLE {schema}.ptg2_snapshot (
        snapshot_id varchar(96) PRIMARY KEY,
        import_run_id varchar(96),
        import_month date,
        status varchar(32),
        created_at timestamp without time zone,
        validated_at timestamp without time zone,
        published_at timestamp without time zone,
        previous_snapshot_id varchar(96),
        manifest json
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_import_run (
        import_run_id varchar(96) PRIMARY KEY,
        import_month date,
        status varchar(32),
        started_at timestamp without time zone,
        finished_at timestamp without time zone,
        heartbeat_at timestamp without time zone,
        options json,
        report json,
        error text
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
        snapshot_id varchar(96),
        snapshot_key bigint
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_scope (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_plan_scope (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_source (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_candidate_audit_attestation (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_snapshot_pin (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.plan_release_snapshot_binding (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_plan_month (
        plan_month_id varchar(96) PRIMARY KEY,
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_artifact_manifest (
        artifact_id varchar(96) PRIMARY KEY,
        snapshot_id varchar(96),
        import_run_id varchar(96),
        artifact_kind varchar(64),
        storage_uri text,
        sha256 varchar(64),
        byte_count bigint,
        payload json,
        created_at timestamp without time zone
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_allowed_amount_plan (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_allowed_amount_item (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_allowed_amount_payment (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_allowed_amount_provider_payment (
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_current_snapshot (
        slot varchar(32),
        snapshot_id varchar(96),
        previous_snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_current_source_snapshot (
        source_key varchar(96),
        snapshot_id varchar(96),
        previous_snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_current_plan_source (
        plan_source_key varchar(96),
        snapshot_id varchar(96),
        previous_snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_import_job (
        import_job_id varchar(96),
        import_run_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_source_catalog (
        source_catalog_id varchar(96),
        import_run_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_serving_rate (
        serving_rate_id varchar(96),
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_serving_rate_compact (
        serving_rate_id varchar(96),
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_price_set_stage (
        snapshot_id varchar(96),
        price_set_hash varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_serving_rate_stage (
        serving_rate_id varchar(96),
        snapshot_id varchar(96)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
        snapshot_key bigint PRIMARY KEY,
        state varchar(16)
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_block (
        block_hash bytea PRIMARY KEY,
        payload bytea
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_snapshot_block (
        snapshot_key bigint,
        block_hash bytea
    )
    """,
    """
    CREATE TABLE {schema}.ptg2_v3_gc_candidate (
        block_hash bytea PRIMARY KEY,
        eligible_at timestamptz
    )
    """,
)


class _OperationRecorder:
    """Capture Alembic SQL so a synthetic schema can run the real migration."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(str(statement))


def _load_attempt_migration(file_name: str):
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / file_name
    )
    module_spec = importlib.util.spec_from_file_location(
        f"test_{migration_path.stem}",
        migration_path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(f"could not load migration {file_name}")
    migration = importlib.util.module_from_spec(module_spec)
    sys.modules[module_spec.name] = migration
    try:
        module_spec.loader.exec_module(migration)
    finally:
        sys.modules.pop(module_spec.name, None)
    return migration


def load_attempt_fence_migration():
    """Load the immutable fence revision for registry parity checks."""

    return _load_attempt_migration(
        "20260724100000_ptg2_v4_attempt_fence.py"
    )


def _migration_sql(migration, schema_name: str) -> tuple[str, ...]:
    recorder = _OperationRecorder()
    original_op = migration.op
    original_schema = migration._schema
    migration.op = recorder
    migration._schema = lambda: schema_name
    try:
        migration.upgrade()
    finally:
        migration.op = original_op
        migration._schema = original_schema
    return tuple(recorder.statements)


def _attempt_migration_sql(schema_name: str) -> tuple[str, ...]:
    statements: list[str] = []
    for file_name in (
        "20260724100000_ptg2_v4_attempt_fence.py",
        "20260724104500_ptg2_v4_attempt_lock_order.py",
        "20260724103000_ptg2_v4_attempt_indexes.py",
    ):
        statements.extend(
            _migration_sql(_load_attempt_migration(file_name), schema_name)
        )
    return tuple(statements)
_PHYSICAL_STATE_SQL = (
    """
    INSERT INTO {schema}.ptg2_v3_snapshot_layout
        (snapshot_key, state)
    VALUES (7, 'sealed')
    """,
    """
    INSERT INTO {schema}.ptg2_v3_block (block_hash, payload)
    VALUES (decode(repeat('11', 32), 'hex'), decode('abcd', 'hex'))
    """,
    """
    INSERT INTO {schema}.ptg2_v3_snapshot_block
        (snapshot_key, block_hash)
    VALUES (7, decode(repeat('11', 32), 'hex'))
    """,
    """
    INSERT INTO {schema}.ptg2_v3_gc_candidate
        (block_hash, eligible_at)
    VALUES
        (decode(repeat('11', 32), 'hex'), now() + INTERVAL '1 day')
    """,
    """
    INSERT INTO {schema}.ptg2_current_source_snapshot
        (source_key, snapshot_id, previous_snapshot_id)
    VALUES ('unrelated', 'snapshot-current', 'snapshot-previous')
    """,
)


def quoted(identifier: str) -> str:
    """Quote one synthetic PostgreSQL identifier."""

    return '"' + str(identifier).replace('"', '""') + '"'


def decoded_json(value):
    """Return an asyncpg JSON value as a mapping-compatible object."""

    return json.loads(value) if isinstance(value, str) else value


def database_for_dsn(dsn: str) -> Database:
    """Build an isolated SQLAlchemy database handle for one test DSN."""

    sqlalchemy_dsn = (
        dsn
        if dsn.startswith("postgresql+asyncpg://")
        else "postgresql+asyncpg://" + dsn.removeprefix("postgresql://")
    )
    engine = create_async_engine(sqlalchemy_dsn)
    return Database(
        engine=engine,
        session_factory=async_sessionmaker(
            engine,
            expire_on_commit=False,
            autoflush=False,
        ),
    )


async def create_unmigrated_stale_schema(
    connection,
    schema_name: str,
) -> None:
    """Create the minimal catalog as it exists before attempt fencing."""

    quoted_schema = quoted(schema_name)
    await connection.execute(f"CREATE SCHEMA {quoted_schema}")
    for statement in _SCHEMA_DDL:
        await connection.execute(statement.format(schema=quoted_schema))


async def apply_attempt_migrations(connection, schema_name: str) -> None:
    """Apply the real attempt-fence migration chain to a test schema."""

    for statement in _attempt_migration_sql(schema_name):
        await connection.execute(statement)


async def create_stale_schema(connection, schema_name: str) -> None:
    """Create the current catalog touched by the reconciler."""

    await create_unmigrated_stale_schema(connection, schema_name)
    await apply_attempt_migrations(connection, schema_name)


async def seed_ready_pair(connection, schema_name: str) -> None:
    """Insert one stale V4 run and its exact empty-manifest snapshot."""

    schema = quoted(schema_name)
    await connection.execute(
        f"""
        INSERT INTO {schema}.ptg2_import_run
            (import_run_id, status, started_at, heartbeat_at, options, report)
        VALUES
            ($1, 'running',
             timezone('UTC', now()) - INTERVAL '8 hours',
             timezone('UTC', now()) - INTERVAL '8 hours',
             $2::json, '{{}}'::json)
        """,
        INTERNAL_RUN_ID,
        json.dumps(
            {
                "source_key": "synthetic-source",
                "storage_generation": "shared_blocks_v4",
            }
        ),
    )
    await connection.execute(
        f"""
        INSERT INTO {schema}.ptg2_snapshot
            (snapshot_id, import_run_id, status, created_at, manifest)
        VALUES
            ($1, $2, 'building',
             timezone('UTC', now()) - INTERVAL '8 hours',
             '{{}}'::json)
        """,
        SNAPSHOT_ID,
        INTERNAL_RUN_ID,
    )


async def seed_physical_state(connection, schema_name: str) -> None:
    """Insert unrelated layout, block, GC, and pointer sentinels."""

    quoted_schema = quoted(schema_name)
    for statement in _PHYSICAL_STATE_SQL:
        await connection.execute(statement.format(schema=quoted_schema))


async def external_state(connection, schema_name: str) -> dict:
    """Capture every sentinel that metadata-only repair must preserve."""

    schema = quoted(schema_name)
    return dict(
        await connection.fetchrow(
            f"""
            SELECT
              (SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_layout)
                AS layouts,
              (SELECT COUNT(*) FROM {schema}.ptg2_v3_block)
                AS blocks,
              (SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_block)
                AS block_mappings,
              (SELECT COUNT(*) FROM {schema}.ptg2_v3_gc_candidate)
                AS gc_candidates,
              (SELECT COUNT(*) FROM {schema}.ptg2_artifact_manifest)
                AS artifacts,
              (SELECT jsonb_agg(to_jsonb(pointer) ORDER BY source_key)
                 FROM {schema}.ptg2_current_source_snapshot AS pointer)
                AS pointers
            """
        )
    )


async def row_versions(connection, schema_name: str) -> tuple:
    """Return row versions proving an idempotent retry made no writes."""

    schema = quoted(schema_name)
    version_row = await connection.fetchrow(
        f"""
        SELECT
          (SELECT xmin::text FROM {schema}.ptg2_snapshot
            WHERE snapshot_id = $1) AS snapshot_xmin,
          (SELECT xmin::text FROM {schema}.ptg2_import_run
            WHERE import_run_id = $2) AS run_xmin
        """,
        SNAPSHOT_ID,
        INTERNAL_RUN_ID,
    )
    return tuple(version_row)


async def assert_registered_writers_are_fenced(
    test_database,
    schema_name: str,
) -> None:
    """Prove every authoritative attachment writer rejects the pair."""

    for attachment in ATTEMPT_ATTACHMENTS:
        row_by_field = {
            column: SNAPSHOT_ID
            for column in attachment.snapshot_columns
        }
        row_by_field.update(
            {
                column: INTERNAL_RUN_ID
                for column in attachment.run_columns
            }
        )
        with pytest.raises(
            StaleMetadataFenceError,
            match="metadata-reconciled",
        ):
            async with test_database.transaction() as session:
                await guard_attempt_rows(
                    session,
                    test_database,
                    schema_name=schema_name,
                    table_name=attachment.table_name,
                    attempt_rows=[row_by_field],
                )


async def drop_stale_schema(dsn: str, schema_name: str) -> None:
    """Drop only the synthetic per-test schema."""

    connection = await asyncpg.connect(dsn)
    try:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {quoted(schema_name)} CASCADE"
        )
    finally:
        await connection.close()

def postgres_dsn() -> str:
    """Return the guarded V4 PostgreSQL test DSN or skip."""

    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST=1 for PostgreSQL E2E")
    return os.environ["HLTHPRT_PTG2_V4_MIGRATION_POSTGRES_DSN"]
