# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL crash-boundary proof for V4 terminal stage release."""

from __future__ import annotations

import datetime
import importlib
import json
import uuid

import asyncpg
import pytest

from process.ptg_parts import snapshot_cleanup
from process.ptg_parts.ptg2_shared_blocks import PTG2_V4_SHARED_GENERATION
from process.ptg_parts.ptg2_v4_attempt_registry import (
    manifest_stage_table_names,
)
from process.ptg_parts.ptg2_v4_stale_metadata_fence import (
    register_attempt_stage_tables,
)
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


process_ptg = importlib.import_module("process.ptg")


async def _drop_test_stages(
    test_database,
    stage_table_names,
    **attempt_coordinate_by_name,
) -> None:
    await snapshot_cleanup._drop_ptg2_snapshot_table_names(
        stage_table_names,
        executor=test_database,
        **attempt_coordinate_by_name,
    )


async def _register_test_stage(
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


async def _prepare_terminal_crash_pair(
    connection,
    test_database,
    schema_name: str,
    stage_table: str,
) -> None:
    schema = quoted(schema_name)
    await seed_ready_pair(connection, schema_name)
    await connection.execute(
        f"UPDATE {schema}.ptg2_snapshot "
        "SET status = 'validated', manifest = '{\"ready\": true}'::json "
        "WHERE snapshot_id = $1",
        SNAPSHOT_ID,
    )
    await _register_test_stage(
        connection,
        test_database,
        schema_name,
        stage_table,
    )


async def _assert_terminal_pair_state(
    connection,
    schema_name: str,
    stage_table: str,
    *,
    run_status: str,
    attachment_count: int,
    stage_exists: bool,
) -> None:
    schema = quoted(schema_name)
    stored_run_status = await connection.fetchval(
        f"SELECT status FROM {schema}.ptg2_import_run "
        "WHERE import_run_id = $1",
        INTERNAL_RUN_ID,
    )
    assert stored_run_status == run_status
    stored_attachment_count = await connection.fetchval(
        f"SELECT COUNT(*) FROM {schema}.ptg2_v4_attempt_stage"
    )
    assert stored_attachment_count == attachment_count
    stored_stage_name = await connection.fetchval(
        "SELECT to_regclass($1)",
        f"{schema}.{quoted(stage_table)}",
    )
    assert (stored_stage_name is not None) is stage_exists


async def _seed_nonempty_failed_run(connection, schema_name: str) -> None:
    schema = quoted(schema_name)
    await connection.execute(
        f"""
        UPDATE {schema}.ptg2_import_run
           SET status = 'failed',
               finished_at = '2000-01-01 00:00:00',
               heartbeat_at = '2000-01-01 00:00:00',
               report = '{{"old_failure": true}}'::json,
               error = 'old failure'
         WHERE import_run_id = $1
        """,
        INTERNAL_RUN_ID,
    )


async def _assert_failed_run_recovered(
    connection,
    schema_name: str,
) -> None:
    stored_run = await connection.fetchrow(
        f"""
        SELECT status, finished_at, heartbeat_at, report, error
          FROM {quoted(schema_name)}.ptg2_import_run
         WHERE import_run_id = $1
        """,
        INTERNAL_RUN_ID,
    )
    assert stored_run["status"] == "validated"
    assert stored_run["error"] is None
    assert json.loads(stored_run["report"]) == {"ready": True}
    assert stored_run["finished_at"].year > 2000
    assert stored_run["heartbeat_at"].year > 2000


def _raw_terminal_push(
    test_database,
    schema_name: str,
    failure_state_by_name: dict[str, bool],
):
    schema = quoted(schema_name)

    async def push(attempt_rows, model, **_kwargs):
        attempt_row = attempt_rows[0]
        if model is process_ptg.PTG2Snapshot:
            await test_database.status(
                f"""
                UPDATE {schema}.ptg2_snapshot
                   SET status = :status,
                       validated_at = :validated_at,
                       published_at = :published_at,
                       manifest = CAST(:manifest AS json)
                 WHERE snapshot_id = :snapshot_id
                """,
                **{
                    **attempt_row,
                    "manifest": json.dumps(attempt_row["manifest"]),
                },
            )
            return
        if model is not process_ptg.PTG2ImportRun:
            raise AssertionError(f"unexpected terminal model: {model}")
        if failure_state_by_name["fail_run_write"]:
            raise RuntimeError("simulated crash before run terminal state")
        await test_database.status(
            f"""
            UPDATE {schema}.ptg2_import_run
               SET status = :status,
                   finished_at = :finished_at,
                   heartbeat_at = :heartbeat_at,
                   report = CAST(:report AS json),
                   error = :error
             WHERE import_run_id = :import_run_id
            """,
            **{
                **attempt_row,
                "report": json.dumps(attempt_row["report"]),
            },
        )

    return push


async def _crash_terminal_retry(
    monkeypatch,
    test_database,
) -> None:
    async def crash_after_drop(
        stage_table_names,
        **attempt_coordinate_by_name,
    ):
        await _drop_test_stages(
            test_database,
            stage_table_names,
            **attempt_coordinate_by_name,
        )
        raise RuntimeError("simulated crash before terminal commit")

    monkeypatch.setattr(
        process_ptg,
        "_drop_ptg2_snapshot_table_names",
        crash_after_drop,
    )
    with pytest.raises(RuntimeError, match="before terminal commit"):
        await process_ptg._finalize_resumed_terminal_attempt(
            {
                "snapshot_id": SNAPSHOT_ID,
                "import_run_id": INTERNAL_RUN_ID,
            },
            internal_run_id=INTERNAL_RUN_ID,
        )


def _install_stage_drop(monkeypatch, test_database) -> None:
    async def drop_stage_tables(
        stage_table_names,
        **attempt_coordinate_by_name,
    ):
        await _drop_test_stages(
            test_database,
            stage_table_names,
            **attempt_coordinate_by_name,
        )

    monkeypatch.setattr(
        process_ptg,
        "_drop_ptg2_snapshot_table_names",
        drop_stage_tables,
    )


async def _finish_terminal_retry(monkeypatch, test_database) -> None:
    _install_stage_drop(monkeypatch, test_database)
    await process_ptg._finalize_resumed_terminal_attempt(
        {
            "snapshot_id": SNAPSHOT_ID,
            "import_run_id": INTERNAL_RUN_ID,
        },
        internal_run_id=INTERNAL_RUN_ID,
    )


@pytest.mark.asyncio
async def test_terminal_retry_release_is_atomic_and_recoverable(monkeypatch):
    """Roll back a crashed release, then finish the exact retry."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_terminal_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    stage_table = manifest_stage_table_names("source", SNAPSHOT_ID)[0]
    try:
        await create_stale_schema(connection, schema_name)
        await _prepare_terminal_crash_pair(
            connection,
            test_database,
            schema_name,
            stage_table,
        )
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
        monkeypatch.setattr(process_ptg, "db", test_database)
        await _crash_terminal_retry(monkeypatch, test_database)
        await _assert_terminal_pair_state(
            connection,
            schema_name,
            stage_table,
            run_status="running",
            attachment_count=1,
            stage_exists=True,
        )
        await _seed_nonempty_failed_run(connection, schema_name)
        await _finish_terminal_retry(monkeypatch, test_database)
        await _assert_terminal_pair_state(
            connection,
            schema_name,
            stage_table,
            run_status="validated",
            attachment_count=0,
            stage_exists=False,
        )
        await _assert_failed_run_recovered(connection, schema_name)
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


async def _mark_failed_attempt(
    *,
    manifest_stage_table: str,
) -> dict | None:
    return await process_ptg._mark_ptg2_import_failed(
        INTERNAL_RUN_ID,
        SNAPSHOT_ID,
        datetime.date(2026, 7, 1),
        datetime.datetime(2026, 7, 1),
        "synthetic failure",
        options={"storage_generation": PTG2_V4_SHARED_GENERATION},
        manifest_stage_table=manifest_stage_table,
    )


async def _assert_snapshot_status(
    connection,
    schema_name: str,
    expected_status: str,
) -> None:
    stored_snapshot_status = await connection.fetchval(
        f"SELECT status FROM {quoted(schema_name)}.ptg2_snapshot "
        "WHERE snapshot_id = $1",
        SNAPSHOT_ID,
    )
    assert stored_snapshot_status == expected_status


async def _resume_failed_building_attempt() -> dict:
    """Re-enter through the production claim path after terminal rollback."""

    started_at = datetime.datetime(2026, 7, 1)
    return await process_ptg._push_ptg2_snapshot_preserving_publication(
        {
            "snapshot_id": SNAPSHOT_ID,
            "import_run_id": INTERNAL_RUN_ID,
            "import_month": datetime.date(2026, 7, 1),
            "status": "building",
            "created_at": started_at,
            "validated_at": None,
            "published_at": None,
            "previous_snapshot_id": None,
            "manifest": {},
        },
        initial_import_run_by_field={
            "import_run_id": INTERNAL_RUN_ID,
            "import_month": datetime.date(2026, 7, 1),
            "status": "running",
            "started_at": started_at,
            "finished_at": None,
            "heartbeat_at": started_at,
            "options": {"storage_generation": PTG2_V4_SHARED_GENERATION},
            "report": {},
            "error": None,
        },
    )


async def _exercise_failed_pair_atomicity(
    connection,
    schema_name: str,
    stage_table: str,
    failure_state_by_name: dict[str, bool],
) -> None:
    failure_report = await _mark_failed_attempt(
        manifest_stage_table=stage_table,
    )
    assert failure_report is None
    await _assert_terminal_pair_state(
        connection,
        schema_name,
        stage_table,
        run_status="running",
        attachment_count=1,
        stage_exists=True,
    )
    await _assert_snapshot_status(connection, schema_name, "building")
    retry_state = await _resume_failed_building_attempt()
    assert retry_state["snapshot_claim_status"] == "existing"
    await _assert_terminal_pair_state(
        connection,
        schema_name,
        stage_table,
        run_status="running",
        attachment_count=1,
        stage_exists=True,
    )

    failure_state_by_name["fail_run_write"] = False
    failure_report = await _mark_failed_attempt(
        manifest_stage_table=stage_table,
    )
    assert failure_report is not None
    await _assert_terminal_pair_state(
        connection,
        schema_name,
        stage_table,
        run_status="failed",
        attachment_count=0,
        stage_exists=False,
    )
    await _assert_snapshot_status(connection, schema_name, "failed")


@pytest.mark.asyncio
async def test_failed_terminal_commit_rolls_back_then_retry_recovers(
    monkeypatch,
):
    """Re-enter the exact attempt after failed terminal persistence."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_failed_atomic_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    stage_table = manifest_stage_table_names("source", SNAPSHOT_ID)[0]
    failure_state_by_name = {"fail_run_write": True}
    try:
        await create_stale_schema(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        await _register_test_stage(
            connection,
            test_database,
            schema_name,
            stage_table,
        )
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
        monkeypatch.setattr(process_ptg, "db", test_database)
        monkeypatch.setattr(
            process_ptg.PTG2Snapshot.__table__, "schema", schema_name
        )
        monkeypatch.setattr(
            process_ptg.PTG2ImportRun.__table__, "schema", schema_name
        )
        monkeypatch.setattr(
            process_ptg,
            "_push_ptg2_objects",
            _raw_terminal_push(
                test_database,
                schema_name,
                failure_state_by_name,
            ),
        )
        _install_stage_drop(monkeypatch, test_database)
        await _exercise_failed_pair_atomicity(
            connection,
            schema_name,
            stage_table,
            failure_state_by_name,
        )
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)
