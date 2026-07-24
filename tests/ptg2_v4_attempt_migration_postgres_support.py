# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL helpers for V4 attempt migration lifecycle tests."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator
import uuid

import asyncpg
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.ext.asyncio import create_async_engine

from tests.ptg2_v4_stale_metadata_postgres_support import (
    _load_attempt_migration,
    create_unmigrated_stale_schema,
    drop_stale_schema,
    postgres_dsn,
    quoted,
)


FENCE_MIGRATION = "20260724100000_ptg2_v4_attempt_fence.py"
LOCK_MIGRATION = "20260724104500_ptg2_v4_attempt_lock_order.py"
HARDENING_MIGRATION = (
    "20260724110000_ptg2_v4_attempt_fence_hardening.py"
)

_LEGACY_AUDIT_BODY = """
BEGIN
    IF TG_OP = 'DELETE' AND OLD.state = 'reconciled' THEN
        RAISE EXCEPTION
            'PTG2_RECONCILIATION_AUDIT_IMMUTABLE'
            USING ERRCODE = 'P0001';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF OLD.state = 'reconciled' THEN
            RAISE EXCEPTION
                'PTG2_RECONCILIATION_AUDIT_IMMUTABLE'
                USING ERRCODE = 'P0001';
        END IF;
        IF NEW.state NOT IN ('active', 'reconciled') THEN
            RAISE EXCEPTION
                'PTG2_RECONCILIATION_AUDIT_STATE'
                USING ERRCODE = 'P0001';
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
""".strip()


class _AsyncpgMigrationOperations(Operations):
    """Let real migration SQL batches use asyncpg's batch executor."""

    def execute(self, sqltext, *, execution_options=None):
        """Execute raw migration batches without prepared-statement splitting."""

        if isinstance(sqltext, str):
            adapted_connection = self.get_bind().connection.dbapi_connection
            return adapted_connection.run_async(
                lambda raw_connection: raw_connection.execute(sqltext)
            )
        return super().execute(
            sqltext,
            execution_options=execution_options,
        )


def migration(file_name: str):
    """Load one real attempt migration module."""

    return _load_attempt_migration(file_name)


async def run_migration_action(
    dsn: str,
    migration_module: Any,
    action: str,
) -> None:
    """Run one revision action through real Alembic PostgreSQL operations."""

    async_engine = create_async_engine(
        dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    )
    try:
        async with async_engine.begin() as async_connection:

            def apply_action(sync_connection) -> None:
                migration_context = MigrationContext.configure(sync_connection)
                migration_module.op = _AsyncpgMigrationOperations(
                    migration_context
                )
                getattr(migration_module, action)()

            await async_connection.run_sync(apply_action)
    finally:
        await async_engine.dispose()


async def _add_downgrade_prerequisites(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    await connection.execute(
        f"""
        ALTER TABLE {schema}.ptg2_v3_snapshot_layout
            ADD COLUMN generation varchar(32);
        CREATE TABLE {schema}.ptg2_v4_snapshot_map_root (
            snapshot_key bigint PRIMARY KEY
        );
        """
    )


@asynccontextmanager
async def attempt_migration_database(
    monkeypatch,
) -> AsyncIterator[tuple[str, str, asyncpg.Connection]]:
    """Create and remove one isolated attempt-migration schema."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_attempt_migration_" + uuid.uuid4().hex[:10]
    connection = await asyncpg.connect(dsn)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    try:
        await create_unmigrated_stale_schema(connection, schema_name)
        await _add_downgrade_prerequisites(connection, schema_name)
        yield dsn, schema_name, connection
    finally:
        await connection.close()
        await drop_stale_schema(dsn, schema_name)


async def _create_legacy_fence(
    connection: asyncpg.Connection,
    schema: str,
) -> None:
    """Create the exact pre-nonce fence relation."""

    await connection.execute(
        f"""
        CREATE TABLE {schema}.ptg2_v4_attempt_fence (
            snapshot_id varchar(96) NOT NULL,
            internal_run_id varchar(96) NOT NULL,
            state varchar(16) NOT NULL DEFAULT 'active',
            target_digest varchar(64),
            plan_digest varchar(64),
            marker_digest varchar(64),
            marker jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            reconciled_at timestamptz,
            CONSTRAINT ptg2_v4_attempt_fence_pkey
                PRIMARY KEY (snapshot_id),
            CONSTRAINT ptg2_v4_attempt_fence_internal_run_id_key
                UNIQUE (internal_run_id),
            CONSTRAINT ptg2_v4_attempt_fence_snapshot_fkey
                FOREIGN KEY (snapshot_id)
                REFERENCES {schema}.ptg2_snapshot(snapshot_id)
                ON DELETE CASCADE,
            CONSTRAINT ptg2_v4_attempt_fence_run_fkey
                FOREIGN KEY (internal_run_id)
                REFERENCES {schema}.ptg2_import_run(import_run_id)
                ON DELETE CASCADE,
            CONSTRAINT ptg2_v4_attempt_fence_pair_key
                UNIQUE (snapshot_id, internal_run_id),
            CONSTRAINT ptg2_v4_attempt_fence_state_check
                CHECK (state IN ('active', 'reconciled')),
            CONSTRAINT ptg2_v4_attempt_fence_reconciled_check
                CHECK (
                    state = 'active'
                    OR (
                        target_digest ~ '^[0-9a-f]{{64}}$'
                        AND plan_digest ~ '^[0-9a-f]{{64}}$'
                        AND marker_digest ~ '^[0-9a-f]{{64}}$'
                        AND marker IS NOT NULL
                        AND reconciled_at IS NOT NULL
                    )
                )
        )
        """
    )


async def _create_legacy_stage_and_trigger(
    connection: asyncpg.Connection,
    schema: str,
) -> None:
    """Create the exact legacy stage relation and audit trigger."""

    await connection.execute(
        f"""
        CREATE TABLE {schema}.ptg2_v4_attempt_stage (
            snapshot_id varchar(96) NOT NULL,
            internal_run_id varchar(96) NOT NULL,
            table_name varchar(63) NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT ptg2_v4_attempt_stage_pkey
                PRIMARY KEY (snapshot_id, table_name),
            CONSTRAINT ptg2_v4_attempt_stage_fence_fkey
                FOREIGN KEY (snapshot_id, internal_run_id)
                REFERENCES {schema}.ptg2_v4_attempt_fence(
                    snapshot_id, internal_run_id
                ) ON DELETE CASCADE
        );
        CREATE FUNCTION {schema}.guard_ptg2_v4_attempt_audit()
        RETURNS trigger LANGUAGE plpgsql AS $$
        {_LEGACY_AUDIT_BODY}
        $$;
        CREATE TRIGGER ptg2_v4_attempt_fence_audit_guard
        BEFORE UPDATE OR DELETE
        ON {schema}.ptg2_v4_attempt_fence
        FOR EACH ROW
        EXECUTE FUNCTION {schema}.guard_ptg2_v4_attempt_audit();
        """
    )


async def create_legacy_attempt_tables(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Create the only pre-nonce fence/stage shape eligible for adoption."""

    schema = quoted(schema_name)
    await _create_legacy_fence(connection, schema)
    await _create_legacy_stage_and_trigger(connection, schema)


async def seed_v4_attempt(
    connection: asyncpg.Connection,
    schema_name: str,
    *,
    insert_fence: bool = False,
) -> None:
    """Insert one V4 run/snapshot and optionally its legacy fence row."""

    schema = quoted(schema_name)
    await connection.execute(
        f"""
        INSERT INTO {schema}.ptg2_import_run
            (import_run_id, status, options, report)
        VALUES (
            'migration-run', 'running',
            '{{"storage_generation":"shared_blocks_v4"}}'::json,
            '{{}}'::json
        );
        INSERT INTO {schema}.ptg2_snapshot
            (snapshot_id, import_run_id, status, manifest)
        VALUES (
            'migration-snapshot', 'migration-run', 'building', '{{}}'::json
        );
        """
    )
    if insert_fence:
        await connection.execute(
            f"""
            INSERT INTO {schema}.ptg2_v4_attempt_fence
                (snapshot_id, internal_run_id)
            VALUES ('migration-snapshot', 'migration-run')
            """
        )


async def table_oids(
    connection: asyncpg.Connection,
    schema_name: str,
) -> tuple[int, int]:
    """Return stable fence/stage relation identities."""

    return tuple(
        await connection.fetchrow(
            "SELECT to_regclass($1)::oid, to_regclass($2)::oid",
            f"{schema_name}.ptg2_v4_attempt_fence",
            f"{schema_name}.ptg2_v4_attempt_stage",
        )
    )
