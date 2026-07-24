# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for reviewed attempt-fence identity and effects."""

from __future__ import annotations

import uuid

import asyncpg
import pytest

from process.ptg_parts import ptg2_v4_stale_metadata_reconcile as reconcile
from tests.ptg2_v4_stale_metadata_postgres_support import (
    INTERNAL_RUN_ID,
    SNAPSHOT_ID,
    create_stale_schema,
    database_for_dsn,
    drop_stale_schema,
    postgres_dsn,
    quoted,
    seed_ready_pair,
)


def _configure_reconciler(monkeypatch, schema_name, test_database) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv(
        reconcile.PTG2_V4_STALE_METADATA_SECONDS_ENV,
        "3600",
    )
    monkeypatch.setattr(reconcile, "db", test_database)


async def _ready_plan(monkeypatch, schema_name, test_database) -> dict:
    _configure_reconciler(monkeypatch, schema_name, test_database)
    plan_by_field = await reconcile.plan_v4_stale_metadata(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
    )
    assert plan_by_field["status"] == "ready"
    return plan_by_field


async def _attempt_state(connection, schema: str):
    return await connection.fetchrow(
        f"""
        SELECT snapshot.status AS snapshot_status,
               snapshot.xmin::text AS snapshot_xmin,
               internal_run.status AS run_status,
               internal_run.xmin::text AS run_xmin,
               fence.state AS fence_state,
               fence.fence_nonce,
               fence.xmin::text AS fence_xmin,
               (SELECT COUNT(*)
                  FROM {schema}.ptg2_v4_attempt_fence) AS fence_count
          FROM {schema}.ptg2_snapshot AS snapshot
          JOIN {schema}.ptg2_import_run AS internal_run
            ON internal_run.import_run_id = snapshot.import_run_id
          LEFT JOIN {schema}.ptg2_v4_attempt_fence AS fence
            ON fence.snapshot_id = snapshot.snapshot_id
         WHERE snapshot.snapshot_id = $1
        """,
        SNAPSHOT_ID,
    )


async def _replace_active_fence(connection, schema: str):
    """Replace one active authority row and return both immutable ids."""

    original_nonce = await connection.fetchval(
        f"SELECT fence_nonce FROM {schema}.ptg2_v4_attempt_fence "
        "WHERE snapshot_id = $1",
        SNAPSHOT_ID,
    )
    await connection.execute(
        f"DELETE FROM {schema}.ptg2_v4_attempt_fence "
        "WHERE snapshot_id = $1",
        SNAPSHOT_ID,
    )
    await connection.execute(
        f"""
        INSERT INTO {schema}.ptg2_v4_attempt_fence
            (snapshot_id, internal_run_id)
        VALUES ($1, $2)
        """,
        SNAPSHOT_ID,
        INTERNAL_RUN_ID,
    )
    replacement_nonce = await connection.fetchval(
        f"SELECT fence_nonce FROM {schema}.ptg2_v4_attempt_fence "
        "WHERE snapshot_id = $1",
        SNAPSHOT_ID,
    )
    return original_nonce, replacement_nonce


@pytest.mark.parametrize(
    "dirty_assignment",
    (
        "marker = '{}'::jsonb",
        "marker = 'null'::jsonb",
        "target_digest = repeat('1', 64)",
        "reconciled_at = now()",
    ),
)
@pytest.mark.asyncio
async def test_database_rejects_dirty_active_fence_audit(
    dirty_assignment,
):
    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_fence_shape_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(connection, schema_name)
        await seed_ready_pair(connection, schema_name)

        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"""
                UPDATE {schema}.ptg2_v4_attempt_fence
                   SET {dirty_assignment}
                 WHERE snapshot_id = $1
                """,
                SNAPSHOT_ID,
            )

        fence = await connection.fetchrow(
            f"""
            SELECT state, target_digest, marker, reconciled_at
              FROM {schema}.ptg2_v4_attempt_fence
             WHERE snapshot_id = $1
            """,
            SNAPSHOT_ID,
        )
        assert tuple(fence) == ("active", None, None, None)
    finally:
        await connection.close()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_deleted_reviewed_fence_is_not_recreated_on_execute(
    monkeypatch,
):
    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_fence_delete_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        reviewed_plan = await _ready_plan(
            monkeypatch,
            schema_name,
            test_database,
        )
        await connection.execute(
            f"DELETE FROM {schema}.ptg2_v4_attempt_fence "
            "WHERE snapshot_id = $1",
            SNAPSHOT_ID,
        )

        with pytest.raises(
            reconcile.PTG2V4StaleMetadataConflict,
            match="not eligible.*attempt_fence_missing",
        ):
            await reconcile.reconcile_v4_stale_metadata(
                snapshot_id=SNAPSHOT_ID,
                internal_run_id=INTERNAL_RUN_ID,
                expected_plan_digest=reviewed_plan["plan_digest"],
            )

        state = await _attempt_state(connection, schema)
        assert state["snapshot_status"] == "building"
        assert state["run_status"] == "running"
        assert state["fence_state"] is None
        assert state["fence_count"] == 0
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_replaced_pristine_fence_changes_reviewed_identity(
    monkeypatch,
):
    """Reject a new pristine authority row under the reviewed digest."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_fence_replace_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        reviewed_plan = await _ready_plan(
            monkeypatch,
            schema_name,
            test_database,
        )
        original_nonce, replacement_nonce = await _replace_active_fence(
            connection,
            schema,
        )
        assert replacement_nonce != original_nonce

        with pytest.raises(
            reconcile.PTG2V4StaleMetadataConflict,
            match="state changed after plan review",
        ):
            await reconcile.reconcile_v4_stale_metadata(
                snapshot_id=SNAPSHOT_ID,
                internal_run_id=INTERNAL_RUN_ID,
                expected_plan_digest=reviewed_plan["plan_digest"],
            )

        state = await _attempt_state(connection, schema)
        assert state["snapshot_status"] == "building"
        assert state["run_status"] == "running"
        assert state["fence_state"] == "active"
        assert state["fence_nonce"] == replacement_nonce
        assert state["fence_count"] == 1
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_sealed_fence_mutation_blocks_the_reviewed_plan(
    monkeypatch,
):
    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_fence_mutate_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        reviewed_plan = await _ready_plan(
            monkeypatch,
            schema_name,
            test_database,
        )
        await connection.execute(
            f"""
            UPDATE {schema}.ptg2_v4_attempt_fence
               SET state = 'reconciled',
                   target_digest = repeat('1', 64),
                   plan_digest = repeat('2', 64),
                   marker_digest = repeat('3', 64),
                   marker = jsonb_build_object('synthetic', true),
                   reconciled_at = now()
             WHERE snapshot_id = $1
            """,
            SNAPSHOT_ID,
        )

        with pytest.raises(
            reconcile.PTG2V4StaleMetadataConflict,
            match="not eligible.*attempt_fence_not_active",
        ):
            await reconcile.reconcile_v4_stale_metadata(
                snapshot_id=SNAPSHOT_ID,
                internal_run_id=INTERNAL_RUN_ID,
                expected_plan_digest=reviewed_plan["plan_digest"],
            )

        state = await _attempt_state(connection, schema)
        assert state["snapshot_status"] == "building"
        assert state["run_status"] == "running"
        assert state["fence_state"] == "reconciled"
        assert state["fence_count"] == 1
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_success_and_retry_report_exact_fence_effects(monkeypatch):
    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_fence_effect_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        reviewed_plan = await _ready_plan(
            monkeypatch,
            schema_name,
            test_database,
        )
        before = await _attempt_state(connection, schema)

        report = await reconcile.reconcile_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            expected_plan_digest=reviewed_plan["plan_digest"],
        )
        after = await _attempt_state(connection, schema)
        assert report["metadata_updates"] == {
            "snapshot_rows": 1,
            "internal_run_rows": 1,
            "attempt_fence_rows": 1,
        }
        assert after["snapshot_status"] == "failed"
        assert after["run_status"] == "failed"
        assert after["fence_state"] == "reconciled"
        assert after["fence_nonce"] == before["fence_nonce"]
        assert after["fence_count"] == 1

        retry_report = await reconcile.reconcile_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            expected_plan_digest=reviewed_plan["plan_digest"],
        )
        retried = await _attempt_state(connection, schema)
        assert retry_report["metadata_updates"] == {
            "snapshot_rows": 0,
            "internal_run_rows": 0,
            "attempt_fence_rows": 0,
        }
        assert tuple(retried) == tuple(after)
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)
