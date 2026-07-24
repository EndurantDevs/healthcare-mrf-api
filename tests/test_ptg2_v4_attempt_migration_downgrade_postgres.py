# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for fail-closed V4 attempt migration downgrade."""

from __future__ import annotations

import asyncpg
import pytest

from tests.ptg2_v4_attempt_migration_postgres_support import (
    FENCE_MIGRATION,
    HARDENING_MIGRATION,
    LOCK_MIGRATION,
    attempt_migration_database,
    migration,
    run_migration_action,
    seed_v4_attempt,
    table_oids,
)
from tests.ptg2_v4_stale_metadata_postgres_support import quoted


async def _attempt_catalog_state(connection, schema_name: str) -> tuple:
    schema = quoted(schema_name)
    return tuple(
        await connection.fetchrow(
            f"""
            SELECT
              to_regclass($1)::oid AS fence_oid,
              to_regclass($2)::oid AS stage_oid,
              (SELECT COUNT(*)
                 FROM {schema}.ptg2_v4_attempt_fence) AS fence_count,
              (SELECT COUNT(*)
                 FROM {schema}.ptg2_v4_attempt_stage) AS stage_count,
              (SELECT string_agg(
                    snapshot_id || ':' || internal_run_id || ':' || state,
                    ',' ORDER BY snapshot_id
               ) FROM {schema}.ptg2_v4_attempt_fence) AS fence_rows
            """,
            f"{schema_name}.ptg2_v4_attempt_fence",
            f"{schema_name}.ptg2_v4_attempt_stage",
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fence_state", ("active", "reconciled"))
async def test_live_attempt_authority_refuses_downgrade_without_changes(
    monkeypatch,
    fence_state,
):
    async with attempt_migration_database(monkeypatch) as (
        dsn,
        schema_name,
        connection,
    ):
        fence_migration = migration(FENCE_MIGRATION)
        await run_migration_action(dsn, fence_migration, "upgrade")
        await seed_v4_attempt(connection, schema_name)
        schema = quoted(schema_name)
        if fence_state == "reconciled":
            await connection.execute(
                f"""
                UPDATE {schema}.ptg2_v4_attempt_fence
                   SET state = 'reconciled',
                       target_digest = repeat('1', 64),
                       plan_digest = repeat('2', 64),
                       marker_digest = repeat('3', 64),
                       marker = '{{}}'::jsonb,
                       reconciled_at = now()
                """
            )
        before_state = await _attempt_catalog_state(
            connection,
            schema_name,
        )

        with pytest.raises(
            asyncpg.ObjectNotInPrerequisiteStateError,
            match="ptg2_v4_attempt_downgrade_requires_empty_authority",
        ):
            await run_migration_action(
                dsn,
                fence_migration,
                "downgrade",
            )

        assert await _attempt_catalog_state(
            connection,
            schema_name,
        ) == before_state


@pytest.mark.asyncio
async def test_empty_authority_downgrades_and_reupgrades_cleanly(
    monkeypatch,
):
    async with attempt_migration_database(monkeypatch) as (
        dsn,
        schema_name,
        connection,
    ):
        fence_migration = migration(FENCE_MIGRATION)
        await run_migration_action(dsn, fence_migration, "upgrade")
        first_oids = await table_oids(connection, schema_name)

        await run_migration_action(dsn, fence_migration, "downgrade")
        assert await connection.fetchval(
            "SELECT to_regclass($1)",
            f"{schema_name}.ptg2_v4_attempt_fence",
        ) is None
        assert await connection.fetchval(
            "SELECT to_regprocedure($1)",
            f"{schema_name}.guard_ptg2_v4_attempt(text,text,boolean)",
        ) is None

        await run_migration_action(dsn, fence_migration, "upgrade")
        second_oids = await table_oids(connection, schema_name)
        assert all(second_oids)
        assert second_oids != first_oids


@pytest.mark.asyncio
async def test_lock_order_downgrade_restores_predecessor_lifecycle_layer(
    monkeypatch,
):
    async with attempt_migration_database(monkeypatch) as (
        dsn,
        schema_name,
        connection,
    ):
        await run_migration_action(
            dsn,
            migration(FENCE_MIGRATION),
            "upgrade",
        )
        lock_migration = migration(LOCK_MIGRATION)
        await run_migration_action(dsn, lock_migration, "upgrade")
        await run_migration_action(dsn, lock_migration, "downgrade")

        lifecycle_function_exists = await connection.fetchval(
            "SELECT to_regprocedure($1) IS NOT NULL",
            f"{schema_name}.lock_ptg2_v4_attempt_lifecycle()",
        )
        trigger_count = await connection.fetchval(
            """
            SELECT COUNT(*)
              FROM pg_trigger AS trigger_record
              JOIN pg_class AS relation_record
                ON relation_record.oid = trigger_record.tgrelid
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = relation_record.relnamespace
             WHERE namespace_record.nspname = $1
               AND NOT trigger_record.tgisinternal
               AND trigger_record.tgname LIKE
                   '%_attempt_lifecycle_lock'
            """,
            schema_name,
        )
        assert lifecycle_function_exists
        assert trigger_count == len(lock_migration._GUARDED_TABLES)


@pytest.mark.asyncio
async def test_hardening_downgrade_preserves_nonce_and_audit(
    monkeypatch,
):
    async with attempt_migration_database(monkeypatch) as (
        dsn,
        schema_name,
        connection,
    ):
        await run_migration_action(
            dsn,
            migration(FENCE_MIGRATION),
            "upgrade",
        )
        hardening_migration = migration(HARDENING_MIGRATION)
        await run_migration_action(dsn, hardening_migration, "upgrade")
        before_oids = await table_oids(connection, schema_name)

        await run_migration_action(
            dsn,
            hardening_migration,
            "downgrade",
        )

        assert await table_oids(connection, schema_name) == before_oids
        assert await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                 WHERE table_schema = $1
                   AND table_name = 'ptg2_v4_attempt_fence'
                   AND column_name = 'fence_nonce'
            )
            """,
            schema_name,
        )
