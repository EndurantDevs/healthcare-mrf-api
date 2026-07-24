# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for strict V4 attempt-table adoption."""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError

from db.ptg2_v4_attempt_schema import (
    FENCE_FINAL_COLUMNS,
    FENCE_FINAL_CONSTRAINTS,
    LIFECYCLE_GUARDED_TABLES,
    STAGE_FINAL_COLUMNS,
    STAGE_FINAL_CONSTRAINTS,
)
from tests.ptg2_v4_attempt_migration_postgres_support import (
    FENCE_MIGRATION,
    HARDENING_MIGRATION,
    attempt_migration_database,
    create_legacy_attempt_tables,
    migration,
    run_migration_action,
    seed_v4_attempt,
    table_oids,
)
from tests.ptg2_v4_stale_metadata_postgres_support import quoted


async def _column_names(connection, schema_name: str, table_name: str) -> set[str]:
    column_result = await connection.fetch(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = $2
        """,
        schema_name,
        table_name,
    )
    return {str(column_record["column_name"]) for column_record in column_result}


async def _constraint_catalog(
    connection,
    schema_name: str,
    table_name: str,
) -> dict[str, bool]:
    constraint_result = await connection.fetch(
        """
        SELECT constraint_record.conname, constraint_record.convalidated
          FROM pg_constraint AS constraint_record
          JOIN pg_class AS relation_record
            ON relation_record.oid = constraint_record.conrelid
          JOIN pg_namespace AS namespace_record
            ON namespace_record.oid = relation_record.relnamespace
         WHERE namespace_record.nspname = $1
           AND relation_record.relname = $2
           AND constraint_record.contype <> 'n'
        """,
        schema_name,
        table_name,
    )
    return {
        str(constraint_record["conname"]): bool(
            constraint_record["convalidated"]
        )
        for constraint_record in constraint_result
    }


async def _assert_final_catalog(connection, schema_name: str) -> None:
    assert await _column_names(
        connection,
        schema_name,
        "ptg2_v4_attempt_fence",
    ) == FENCE_FINAL_COLUMNS
    assert await _column_names(
        connection,
        schema_name,
        "ptg2_v4_attempt_stage",
    ) == STAGE_FINAL_COLUMNS
    fence_constraints = await _constraint_catalog(
        connection,
        schema_name,
        "ptg2_v4_attempt_fence",
    )
    stage_constraints = await _constraint_catalog(
        connection,
        schema_name,
        "ptg2_v4_attempt_stage",
    )
    assert set(fence_constraints) == FENCE_FINAL_CONSTRAINTS
    assert set(stage_constraints) == STAGE_FINAL_CONSTRAINTS
    assert all(fence_constraints.values())
    assert all(stage_constraints.values())


@pytest.mark.asyncio
async def test_fresh_upgrade_installs_exact_final_attempt_catalog(
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

        await _assert_final_catalog(connection, schema_name)
        trigger_body = await connection.fetchval(
            """
            SELECT function_record.prosrc
              FROM pg_trigger AS trigger_record
              JOIN pg_class AS relation_record
                ON relation_record.oid = trigger_record.tgrelid
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = relation_record.relnamespace
              JOIN pg_proc AS function_record
                ON function_record.oid = trigger_record.tgfoid
             WHERE namespace_record.nspname = $1
               AND relation_record.relname = 'ptg2_v4_attempt_fence'
               AND trigger_record.tgname =
                   'ptg2_v4_attempt_fence_audit_guard'
            """,
            schema_name,
        )
        assert "OLD.fence_nonce IS DISTINCT FROM NEW.fence_nonce" in trigger_body


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "migration_file",
    (FENCE_MIGRATION, HARDENING_MIGRATION),
)
@pytest.mark.parametrize("fence_state", ("active", "reconciled"))
async def test_known_legacy_shape_is_adopted_in_place(
    monkeypatch,
    migration_file,
    fence_state,
):
    async with attempt_migration_database(monkeypatch) as (
        dsn,
        schema_name,
        connection,
    ):
        await create_legacy_attempt_tables(connection, schema_name)
        await seed_v4_attempt(connection, schema_name, insert_fence=True)
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
        before_oids = await table_oids(connection, schema_name)

        await run_migration_action(
            dsn,
            migration(migration_file),
            "upgrade",
        )

        assert await table_oids(connection, schema_name) == before_oids
        await _assert_final_catalog(connection, schema_name)
        adopted_row = await connection.fetchrow(
            f"""
            SELECT snapshot_id, internal_run_id, fence_nonce, state
              FROM {schema}.ptg2_v4_attempt_fence
            """
        )
        assert tuple(adopted_row)[:2] == (
            "migration-snapshot",
            "migration-run",
        )
        assert adopted_row["fence_nonce"] is not None
        assert adopted_row["state"] == fence_state


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown_kind", ("extra_column", "partial"))
async def test_unknown_or_partial_attempt_shape_is_refused_atomically(
    monkeypatch,
    unknown_kind,
):
    async with attempt_migration_database(monkeypatch) as (
        dsn,
        schema_name,
        connection,
    ):
        await create_legacy_attempt_tables(connection, schema_name)
        schema = quoted(schema_name)
        if unknown_kind == "extra_column":
            await connection.execute(
                f"ALTER TABLE {schema}.ptg2_v4_attempt_fence "
                "ADD COLUMN unexpected text"
            )
        else:
            await connection.execute(
                f"DROP TABLE {schema}.ptg2_v4_attempt_stage"
            )
        before_oid = await connection.fetchval(
            "SELECT to_regclass($1)::oid",
            f"{schema_name}.ptg2_v4_attempt_fence",
        )
        before_columns = await _column_names(
            connection,
            schema_name,
            "ptg2_v4_attempt_fence",
        )

        with pytest.raises(
            RuntimeError,
            match="partial_shape|shape_unknown",
        ):
            await run_migration_action(
                dsn,
                migration(FENCE_MIGRATION),
                "upgrade",
            )

        assert await connection.fetchval(
            "SELECT to_regclass($1)::oid",
            f"{schema_name}.ptg2_v4_attempt_fence",
        ) == before_oid
        assert await _column_names(
            connection,
            schema_name,
            "ptg2_v4_attempt_fence",
        ) == before_columns


@pytest.mark.asyncio
async def test_legacy_dirty_active_row_refuses_nonce_adoption(
    monkeypatch,
):
    async with attempt_migration_database(monkeypatch) as (
        dsn,
        schema_name,
        connection,
    ):
        await create_legacy_attempt_tables(connection, schema_name)
        await seed_v4_attempt(connection, schema_name, insert_fence=True)
        schema = quoted(schema_name)
        await connection.execute(
            f"""
            UPDATE {schema}.ptg2_v4_attempt_fence
               SET target_digest = repeat('a', 64)
            """
        )

        with pytest.raises(
            DBAPIError,
            match="ptg2_v4_attempt_legacy_rows_not_adoptable",
        ):
            await run_migration_action(
                dsn,
                migration(FENCE_MIGRATION),
                "upgrade",
            )

        assert "fence_nonce" not in await _column_names(
            connection,
            schema_name,
            "ptg2_v4_attempt_fence",
        )


@pytest.mark.asyncio
async def test_hardening_only_repairs_legacy_lifecycle_layer(
    monkeypatch,
):
    async with attempt_migration_database(monkeypatch) as (
        dsn,
        schema_name,
        connection,
    ):
        await create_legacy_attempt_tables(connection, schema_name)
        assert not await connection.fetchval(
            "SELECT to_regprocedure($1) IS NOT NULL",
            f"{schema_name}.lock_ptg2_v4_attempt_lifecycle()",
        )

        await run_migration_action(
            dsn,
            migration(HARDENING_MIGRATION),
            "upgrade",
        )

        assert await connection.fetchval(
            "SELECT to_regprocedure($1) IS NOT NULL",
            f"{schema_name}.lock_ptg2_v4_attempt_lifecycle()",
        )
        lifecycle_trigger_count = await connection.fetchval(
            """
            SELECT COUNT(*)
              FROM pg_trigger AS trigger_record
              JOIN pg_class AS relation_record
                ON relation_record.oid = trigger_record.tgrelid
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = relation_record.relnamespace
             WHERE namespace_record.nspname = $1
               AND NOT trigger_record.tgisinternal
               AND right(
                    trigger_record.tgname,
                    length('_attempt_lifecycle_lock')
               ) = '_attempt_lifecycle_lock'
            """,
            schema_name,
        )
        assert lifecycle_trigger_count == len(LIFECYCLE_GUARDED_TABLES)
