# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL regressions for V4 stage ownership and run lookup."""

from __future__ import annotations

import uuid

import asyncpg
import pytest

from process.ptg_parts import ptg2_manifest_publish
from process.ptg_parts import ptg2_v4_stale_metadata_reconcile as reconcile
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_GENERATION,
    PTG2_V4_SHARED_GENERATION,
)
from process.ptg_parts.ptg2_v4_attempt_registry import (
    manifest_stage_table_names,
)
from process.ptg_parts.ptg2_v4_stale_metadata_fence import (
    StaleMetadataFenceError,
    drop_attempt_stage_tables,
    lock_writable_snapshot,
    register_attempt_stage_tables,
)
from tests.ptg2_v4_stale_metadata_postgres_support import (
    INTERNAL_RUN_ID,
    SNAPSHOT_ID,
    configure_test_schema,
    create_stale_schema,
    database_for_dsn,
    drop_stale_schema,
    postgres_dsn,
    quoted,
    seed_ready_pair,
)


async def _assert_wrong_run_cannot_drop_stage(
    connection,
    test_database,
    schema_name: str,
    stage_table: str,
) -> None:
    schema = quoted(schema_name)
    await connection.execute(
        f"INSERT INTO {schema}.ptg2_import_run "
        "(import_run_id, status, options, report) VALUES "
        "('wrong-run', 'running', "
        "json_build_object('storage_generation', 'shared_blocks_v3'), "
        "'{}'::json)"
    )
    with pytest.raises(StaleMetadataFenceError, match="durably fenced"):
        await drop_attempt_stage_tables(
            test_database,
            schema_name=schema_name,
            snapshot_id=SNAPSHOT_ID,
            internal_run_id="wrong-run",
            table_names=[stage_table],
        )
    assert await connection.fetchval(
        "SELECT to_regclass($1)",
        f"{schema}.{quoted(stage_table)}",
    ) is not None
    assert await connection.fetchval(
        f"SELECT COUNT(*) FROM {schema}.ptg2_v4_attempt_stage"
    ) == 1


async def _drop_stage_but_retain_attachment(
    connection,
    test_database,
    schema_name: str,
    stage_table: str,
) -> None:
    await register_attempt_stage_tables(
        test_database,
        schema_name=schema_name,
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        table_names=[stage_table],
    )
    await connection.execute(
        f"CREATE TABLE {quoted(schema_name)}.{quoted(stage_table)} "
        "(entry_id bigint)"
    )
    await _assert_wrong_run_cannot_drop_stage(
        connection, test_database, schema_name, stage_table
    )
    await drop_attempt_stage_tables(
        test_database,
        schema_name=schema_name,
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        table_names=[stage_table],
        retain_registration=True,
    )
    assert await connection.fetchval(
        "SELECT to_regclass($1)",
        f"{quoted(schema_name)}.{quoted(stage_table)}",
    ) is None


async def _assert_crash_gap_safety(
    monkeypatch,
    test_database,
    schema_name: str,
) -> None:
    configure_test_schema(monkeypatch, schema_name)
    monkeypatch.setenv(
        reconcile.PTG2_V4_STALE_METADATA_SECONDS_ENV, "3600"
    )
    monkeypatch.setattr(reconcile, "db", test_database)
    crash_gap_plan = await reconcile.plan_v4_stale_metadata(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
    )
    assert crash_gap_plan["status"] == "ineligible"
    assert "attached_state_present" in crash_gap_plan["reason_codes"]
    async with test_database.transaction() as recovery_session:
        await lock_writable_snapshot(
            recovery_session,
            test_database,
            schema_name=schema_name,
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
        )


@pytest.mark.asyncio
async def test_run_only_guard_lookup_uses_the_snapshot_attempt_index():
    """Keep heartbeat fencing off a full snapshot-table scan."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_run_index_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        index_state = await connection.fetchrow(
            """
            SELECT index_meta.indisvalid, index_meta.indisready,
                   pg_get_indexdef(index_meta.indexrelid) AS definition
              FROM pg_index AS index_meta
             WHERE index_meta.indexrelid = to_regclass($1)
            """,
            f"{schema_name}.ptg2_snapshot_attempt_run_idx",
        )
        assert tuple(index_state)[:2] == (True, True)
        assert "(import_run_id)" in index_state["definition"]

        await connection.execute("SET enable_seqscan = off")
        plan_records = await connection.fetch(
            f"""
            EXPLAIN (COSTS OFF)
            SELECT COUNT(*), MIN(snapshot_id)
              FROM {schema}.ptg2_snapshot
             WHERE import_run_id = $1
            """,
            INTERNAL_RUN_ID,
        )
        query_plan = "\n".join(
            str(plan_record[0]) for plan_record in plan_records
        )
        assert "ptg2_snapshot_attempt_run_idx" in query_plan
    finally:
        await connection.close()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_manifest_stage_attachment_spans_the_crash_gap(monkeypatch):
    """Block metadata repair while preserving exact recovery authority."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_stage_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    stage_table = manifest_stage_table_names(
        "synthetic-source",
        SNAPSHOT_ID,
    )[0]
    try:
        await create_stale_schema(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        await _drop_stage_but_retain_attachment(
            connection, test_database, schema_name, stage_table
        )
        await _assert_crash_gap_safety(
            monkeypatch, test_database, schema_name
        )
        await drop_attempt_stage_tables(
            test_database,
            schema_name=schema_name,
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            table_names=[stage_table],
        )
        assert await connection.fetchval(
            f"SELECT COUNT(*) FROM "
            f"{quoted(schema_name)}.ptg2_v4_attempt_stage"
        ) == 0
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_serving_stage_registration_is_v4_only(monkeypatch):
    """Let V3 stage normally while retaining exact V4 attachment ownership."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_stage_gate_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        monkeypatch.delenv("DB_SCHEMA", raising=False)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
        monkeypatch.setenv(
            "HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3"
        )
        monkeypatch.setattr(ptg2_manifest_publish, "db", test_database)

        await ptg2_manifest_publish._create_serving_stage_table(
            "legacy-stage",
            snapshot_id="legacy-snapshot",
            internal_run_id="legacy-run",
            storage_generation=PTG2_V3_SHARED_GENERATION,
        )
        assert await connection.fetchval(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v4_attempt_stage"
        ) == 0

        v4_stage = await ptg2_manifest_publish._create_serving_stage_table(
            "fenced-stage",
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            storage_generation=PTG2_V4_SHARED_GENERATION,
        )
        registered_names = {
            stage_record["table_name"]
            for stage_record in await connection.fetch(
                f"SELECT table_name FROM {schema}.ptg2_v4_attempt_stage"
            )
        }
        assert registered_names == {
            v4_stage,
            *(
                ptg2_manifest_publish._ptg2_manifest_support_stage_table(
                    v4_stage, stage_kind
                )
                for stage_kind in (
                    "price_atom",
                    "price_set_atom",
                    "price_set_summary",
                )
            ),
        }
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)
