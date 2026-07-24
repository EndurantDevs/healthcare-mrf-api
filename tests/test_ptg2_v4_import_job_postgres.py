# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL catalog and writer-guard proofs for PTG V4 import jobs."""

from __future__ import annotations

import uuid

import asyncpg
import pytest

from tests.ptg2_v4_attempt_migration_postgres_support import (
    FENCE_MIGRATION,
    attempt_migration_database,
    migration,
    run_migration_action,
)
from tests.ptg2_v4_stale_metadata_postgres_support import (
    INTERNAL_RUN_ID,
    SNAPSHOT_ID,
    create_stale_schema,
    drop_stale_schema,
    postgres_dsn,
    quoted,
    seed_ready_pair,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dirty_sql",
    (
        "ALTER TABLE {table} ALTER COLUMN payload "
        "TYPE jsonb USING payload::jsonb",
        "ALTER TABLE {table} ALTER COLUMN created_at "
        "TYPE timestamp with time zone",
        "ALTER TABLE {table} ALTER COLUMN status "
        "SET DEFAULT 'pending'",
        "ALTER TABLE {table} ADD COLUMN unexpected text",
        "ALTER TABLE {table} ADD CONSTRAINT "
        "ptg2_import_job_attempts_check CHECK (attempts >= 0)",
        "ALTER TABLE {table} DROP COLUMN updated_at",
        "DROP INDEX {schema}.ptg2_import_job_status_idx; "
        "CREATE INDEX ptg2_import_job_status_idx "
        "ON {table} (source_type)",
        "DROP INDEX {schema}.ptg2_import_job_status_idx; "
        "CREATE TABLE {schema}.import_job_index_collision "
        "(status varchar(32)); "
        "CREATE INDEX ptg2_import_job_status_idx "
        "ON {schema}.import_job_index_collision (status)",
    ),
)
async def test_dirty_or_partial_import_job_shape_is_refused(
    monkeypatch,
    dirty_sql,
):
    async with attempt_migration_database(monkeypatch) as (
        dsn,
        schema_name,
        connection,
    ):
        schema = quoted(schema_name)
        table = f"{schema}.ptg2_import_job"
        await connection.execute(
            dirty_sql.format(schema=schema, table=table)
        )
        before_oid = await connection.fetchval(
            "SELECT to_regclass($1)::oid",
            f"{schema_name}.ptg2_import_job",
        )

        with pytest.raises(RuntimeError, match="import_job|existing_schema"):
            await run_migration_action(
                dsn,
                migration(FENCE_MIGRATION),
                "upgrade",
            )

        assert await connection.fetchval(
            "SELECT to_regclass($1)::oid",
            f"{schema_name}.ptg2_import_job",
        ) == before_oid


async def _seed_reconciled_import_jobs(connection, schema: str) -> None:
    await connection.execute(
        f"""
        INSERT INTO {schema}.ptg2_import_job
            (import_job_id, import_run_id, status)
        VALUES
            ('blocked-update', $1, 'pending'),
            ('blocked-delete', $1, 'pending')
        """,
        INTERNAL_RUN_ID,
    )
    await connection.execute(
        f"""
        UPDATE {schema}.ptg2_v4_attempt_fence
           SET state = 'reconciled',
               target_digest = repeat('1', 64),
               plan_digest = repeat('2', 64),
               marker_digest = repeat('3', 64),
               marker = '{{}}'::jsonb,
               reconciled_at = now()
         WHERE snapshot_id = $1
        """,
        SNAPSHOT_ID,
    )


def _import_job_statement(
    schema: str,
    operation: str,
) -> tuple[str, tuple[str, ...]]:
    statement_by_operation = {
        "insert": (
            f"INSERT INTO {schema}.ptg2_import_job "
            "(import_job_id, import_run_id, status) "
            "VALUES ('blocked-insert', $1, 'pending')"
        ),
        "update": (
            f"UPDATE {schema}.ptg2_import_job "
            "SET status = 'running' "
            "WHERE import_job_id = 'blocked-update'"
        ),
        "delete": (
            f"DELETE FROM {schema}.ptg2_import_job "
            "WHERE import_job_id = 'blocked-delete'"
        ),
    }
    parameters_by_operation = {
        "insert": (INTERNAL_RUN_ID,),
        "update": (),
        "delete": (),
    }
    return (
        statement_by_operation[operation],
        parameters_by_operation[operation],
    )


async def _assert_import_job_operation_is_fenced(
    connection,
    schema: str,
    operation: str,
) -> None:
    statement, parameters = _import_job_statement(schema, operation)
    with pytest.raises(
        asyncpg.PostgresError,
        match="PTG2_STALE_METADATA_FENCE",
    ):
        await connection.execute(statement, *parameters)


async def _assert_import_jobs_unchanged(connection, schema: str) -> None:
    job_rows = await connection.fetch(
        f"""
        SELECT import_job_id, status
          FROM {schema}.ptg2_import_job
         ORDER BY import_job_id
        """
    )
    assert [tuple(job_row) for job_row in job_rows] == [
        ("blocked-delete", "pending"),
        ("blocked-update", "pending"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("insert", "update", "delete"))
async def test_reconciled_attempt_rejects_direct_import_job_writes(
    operation,
):
    dsn = postgres_dsn()
    schema_name = "ptg_v4_import_job_fence_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        await _seed_reconciled_import_jobs(connection, schema)
        await _assert_import_job_operation_is_fenced(
            connection,
            schema,
            operation,
        )
        await _assert_import_jobs_unchanged(connection, schema)
    finally:
        await connection.close()
        await drop_stale_schema(dsn, schema_name)
