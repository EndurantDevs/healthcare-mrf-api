# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for exact, write-bounded V4 stale metadata repair."""

from __future__ import annotations

import asyncio
import json
import uuid
import asyncpg
import pytest

from process.ptg_parts import ptg2_v4_stale_metadata_reconcile as reconcile
from process.ptg_parts import ptg2_artifact_blobs as artifact_blobs
from process.ptg_parts.ptg2_v4_attempt_registry import (
    manifest_stage_table_names,
)
from process.ptg_parts.ptg2_v4_stale_metadata_fence import (
    StaleMetadataFenceError,
    lock_writable_snapshot,
)
from tests.ptg2_v4_stale_metadata_postgres_support import (
    INTERNAL_RUN_ID,
    SNAPSHOT_ID,
    assert_registered_writers_are_fenced, configure_test_schema,
    create_stale_schema,
    database_for_dsn,
    decoded_json,
    drop_stale_schema,
    external_state,
    postgres_dsn,
    quoted,
    row_versions,
    seed_physical_state,
    seed_ready_pair,
)

def _configure_reconciler(monkeypatch, schema_name, test_database) -> None:
    configure_test_schema(monkeypatch, schema_name)
    monkeypatch.setenv(
        reconcile.PTG2_V4_STALE_METADATA_SECONDS_ENV,
        "3600",
    )
    monkeypatch.setattr(reconcile, "db", test_database)

async def _ready_plan() -> dict:
    plan_by_field = await reconcile.plan_v4_stale_metadata(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
    )
    serialized_plan = json.dumps(plan_by_field, sort_keys=True)
    assert plan_by_field["status"] == "ready"
    assert SNAPSHOT_ID not in serialized_plan
    assert INTERNAL_RUN_ID not in serialized_plan
    return plan_by_field

async def _concurrent_reports(plan_by_field: dict) -> None:
    reports = await asyncio.gather(
        *(
            reconcile.reconcile_v4_stale_metadata(
                snapshot_id=SNAPSHOT_ID,
                internal_run_id=INTERNAL_RUN_ID,
                expected_plan_digest=plan_by_field["plan_digest"],
            )
            for _ordinal in range(2)
        )
    )
    assert {report["status"] for report in reports} == {
        "reconciled",
        "already_reconciled",
    }
    assert all(
        set(report["external_effects"].values()) == {0}
        for report in reports
    )
    update_counts = [
        tuple(report["metadata_updates"].values())
        for report in reports
    ]
    assert sorted(update_counts) == [(0, 0, 0), (1, 1, 1)]
async def _assert_persisted_marker(
    connection,
    schema_name: str,
    plan_by_field: dict,
) -> None:
    persisted_state = await connection.fetchrow(
        f"""
        SELECT snapshot.status AS snapshot_status,
               snapshot.manifest AS snapshot_manifest,
               internal_run.status AS run_status,
               internal_run.report AS run_report,
               internal_run.error AS run_error
          FROM {quoted(schema_name)}.ptg2_snapshot AS snapshot
          JOIN {quoted(schema_name)}.ptg2_import_run AS internal_run
            ON internal_run.import_run_id = snapshot.import_run_id
         WHERE snapshot.snapshot_id = $1
        """,
        SNAPSHOT_ID,
    )
    snapshot_marker = decoded_json(persisted_state["snapshot_manifest"])[
        reconcile.PTG2_V4_STALE_METADATA_MARKER
    ]
    run_marker = decoded_json(persisted_state["run_report"])[
        reconcile.PTG2_V4_STALE_METADATA_MARKER
    ]
    serialized_marker = json.dumps(snapshot_marker, sort_keys=True)
    assert tuple(persisted_state)[:3:2] == ("failed", "failed")
    assert snapshot_marker == run_marker
    assert snapshot_marker["target_digest"] == plan_by_field["target_digest"]
    assert SNAPSHOT_ID not in serialized_marker
    assert INTERNAL_RUN_ID not in serialized_marker
    assert persisted_state["run_error"] == (
        "authenticated metadata-only stale PTG V4 build reconciliation"
    )


async def _assert_write_free_retry(
    connection,
    schema_name: str,
    plan_by_field: dict,
) -> None:
    versions_before = await row_versions(connection, schema_name)
    retry_report = await reconcile.reconcile_v4_stale_metadata(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        expected_plan_digest=plan_by_field["plan_digest"],
    )
    assert retry_report["status"] == "already_reconciled"
    assert retry_report["metadata_updates"] == {
        "snapshot_rows": 0,
        "internal_run_rows": 0,
        "attempt_fence_rows": 0,
    }
    assert await row_versions(connection, schema_name) == versions_before


async def _set_heartbeat(
    connection,
    schema_name: str,
    *,
    is_stale: bool,
) -> None:
    heartbeat_expression = (
        "timezone('UTC', now()) - INTERVAL '8 hours'"
        if is_stale
        else "timezone('UTC', now())"
    )
    await connection.execute(
        f"""
        UPDATE {quoted(schema_name)}.ptg2_import_run
           SET heartbeat_at = {heartbeat_expression}
         WHERE import_run_id = $1
        """,
        INTERNAL_RUN_ID,
    )


async def _assert_execute_conflict(
    plan_by_field: dict,
    *,
    message_pattern: str,
) -> None:
    with pytest.raises(
        reconcile.PTG2V4StaleMetadataConflict,
        match=message_pattern,
    ):
        await reconcile.reconcile_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            expected_plan_digest=plan_by_field["plan_digest"],
        )


async def _artifact_count(connection, schema_name: str) -> int:
    return int(
        await connection.fetchval(
            f"""
            SELECT COUNT(*)
              FROM {quoted(schema_name)}.ptg2_artifact_manifest
             WHERE snapshot_id = $1
            """,
            SNAPSHOT_ID,
        )
    )


async def _assert_reconciled_attempt_rejects_writes(
    connection,
    test_database,
    schema_name: str,
    tmp_path,
) -> None:
    await _assert_recovery_fence_rejected(test_database, schema_name)
    resumed_artifact = tmp_path / "resumed-worker.bin"
    resumed_artifact.write_bytes(b"reconciled worker must stay fenced")
    with pytest.raises(
        StaleMetadataFenceError,
        match="metadata-reconciled",
    ):
        await artifact_blobs.store_ptg2_artifact_file(
            resumed_artifact,
            snapshot_id=SNAPSHOT_ID,
            import_run_id=INTERNAL_RUN_ID,
            artifact_kind="test",
            schema_name=schema_name,
        )
    assert await _artifact_count(connection, schema_name) == 0
    with pytest.raises(
        asyncpg.PostgresError,
        match="PTG2_STALE_METADATA_FENCE",
    ):
        await connection.execute(
            f"""
            INSERT INTO {quoted(schema_name)}.ptg2_plan_month
                (plan_month_id, snapshot_id)
            VALUES ('direct-trigger-proof', $1)
            """,
            SNAPSHOT_ID,
        )
    with pytest.raises(
        asyncpg.PostgresError,
        match="PTG2_ATTEMPT_FENCE_GENERATION_CHANGED",
    ):
        await connection.execute(
            f"""
            UPDATE {quoted(schema_name)}.ptg2_import_run
               SET options = $2::json
             WHERE import_run_id = $1
            """,
            INTERNAL_RUN_ID,
            json.dumps({"storage_generation": "shared_blocks_v3"}),
        )
    await assert_registered_writers_are_fenced(
        test_database,
        schema_name,
    )


async def _assert_recovery_fence_rejected(test_database, schema_name: str) -> None:
    """Prove the physical-recovery guard rejects a reconciled attempt."""

    async with test_database.transaction() as session:
        with pytest.raises(
            StaleMetadataFenceError,
            match="metadata-reconciled",
        ):
            await lock_writable_snapshot(
                session,
                test_database,
                schema_name=schema_name,
                snapshot_id=SNAPSHOT_ID, internal_run_id=INTERNAL_RUN_ID,
            )


async def _assert_manifest_stage_existence_blocks_execute(
    connection,
    schema_name: str,
    reviewed_plan: dict,
) -> None:
    stage_table = manifest_stage_table_names(
        "synthetic-source",
        SNAPSHOT_ID,
    )[0]
    await connection.execute(
        f"CREATE TABLE {quoted(schema_name)}.{quoted(stage_table)} "
        "(sentinel integer)"
    )
    try:
        await _assert_execute_conflict(
            reviewed_plan,
            message_pattern="attached_state_present",
        )
    finally:
        await connection.execute(
            f"DROP TABLE {quoted(schema_name)}.{quoted(stage_table)}"
        )


async def _set_plan_month_attachment(
    connection,
    schema_name: str,
    *,
    is_present: bool,
) -> None:
    if is_present:
        await connection.execute(
            f"""
            INSERT INTO {quoted(schema_name)}.ptg2_plan_month
                (plan_month_id, snapshot_id)
            VALUES ('plan-month-one', $1)
            """,
            SNAPSHOT_ID,
        )
        return
    await connection.execute(
        f"""
        DELETE FROM {quoted(schema_name)}.ptg2_plan_month
         WHERE snapshot_id = $1
        """,
        SNAPSHOT_ID,
    )


async def _status_pair(connection, schema_name: str) -> tuple:
    status_row = await connection.fetchrow(
        f"""
        SELECT snapshot.status AS snapshot_status,
               internal_run.status AS run_status
          FROM {quoted(schema_name)}.ptg2_snapshot AS snapshot
          JOIN {quoted(schema_name)}.ptg2_import_run AS internal_run
            ON internal_run.import_run_id = snapshot.import_run_id
         WHERE snapshot.snapshot_id = $1
        """,
        SNAPSHOT_ID,
    )
    return tuple(status_row)


async def _run_paused_artifact_race(
    monkeypatch,
    *,
    test_database,
    schema_name: str,
    artifact_path,
    plan_by_field: dict,
) -> None:
    monkeypatch.setattr(artifact_blobs, "db", test_database)
    artifact_path.write_bytes(b"writer wins before reconcile lock")
    writer_has_lock = asyncio.Event()
    release_writer = asyncio.Event()
    real_lock = artifact_blobs.lock_writable_snapshot

    async def pause_after_writer_lock(*args, **kwargs):
        await real_lock(*args, **kwargs)
        writer_has_lock.set()
        await release_writer.wait()

    monkeypatch.setattr(
        artifact_blobs,
        "lock_writable_snapshot",
        pause_after_writer_lock,
    )
    writer_task = asyncio.create_task(
        artifact_blobs.store_ptg2_artifact_file(
            artifact_path,
            snapshot_id=SNAPSHOT_ID,
            import_run_id=INTERNAL_RUN_ID,
            artifact_kind="test",
            schema_name=schema_name,
        )
    )
    await writer_has_lock.wait()
    reconcile_task = asyncio.create_task(
        reconcile.reconcile_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            expected_plan_digest=plan_by_field["plan_digest"],
        )
    )
    await asyncio.sleep(0)
    release_writer.set()
    await writer_task
    with pytest.raises(
        reconcile.PTG2V4StaleMetadataConflict,
        match="attached_state_present",
    ):
        await reconcile_task


async def _assert_plan_drift_guards(connection, schema_name: str) -> dict:
    reviewed_plan = await _ready_plan()
    with pytest.raises(
        reconcile.PTG2V4StaleMetadataConflict,
        match="state changed after plan review",
    ):
        await reconcile.reconcile_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            expected_plan_digest="0" * 64,
        )
    assert await _status_pair(
        connection,
        schema_name,
    ) == ("building", "running")
    await _set_heartbeat(connection, schema_name, is_stale=False)
    await _assert_execute_conflict(
        reviewed_plan,
        message_pattern="not eligible",
    )
    await _set_heartbeat(connection, schema_name, is_stale=True)
    reviewed_plan = await _ready_plan()
    await _set_plan_month_attachment(
        connection,
        schema_name,
        is_present=True,
    )
    await _assert_execute_conflict(
        reviewed_plan,
        message_pattern="attached_state_present",
    )
    await _set_plan_month_attachment(
        connection,
        schema_name,
        is_present=False,
    )
    reviewed_plan = await _ready_plan()
    await _assert_manifest_stage_existence_blocks_execute(
        connection,
        schema_name,
        reviewed_plan,
    )
    return await _ready_plan()


@pytest.mark.asyncio
async def test_exact_stale_pair_reconciles_once_without_physical_side_effects(
    monkeypatch,
    tmp_path,
):
    """Reconcile once under contention and preserve every physical sentinel."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_" + uuid.uuid4().hex[:12]
    setup_connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    try:
        await create_stale_schema(setup_connection, schema_name)
        await seed_ready_pair(setup_connection, schema_name)
        await seed_physical_state(setup_connection, schema_name)
        external_state_before = await external_state(
            setup_connection,
            schema_name,
        )

        _configure_reconciler(monkeypatch, schema_name, test_database)
        plan_by_field = await _ready_plan()
        await _concurrent_reports(plan_by_field)
        await _assert_persisted_marker(
            setup_connection,
            schema_name,
            plan_by_field,
        )
        external_state_after = await external_state(
            setup_connection,
            schema_name,
        )
        assert external_state_after == external_state_before
        await _assert_write_free_retry(
            setup_connection,
            schema_name,
            plan_by_field,
        )
        monkeypatch.setattr(artifact_blobs, "db", test_database)
        await _assert_reconciled_attempt_rejects_writes(
            setup_connection,
            test_database,
            schema_name,
            tmp_path,
        )
    finally:
        await setup_connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_execute_rechecks_staleness_and_attached_state_under_lock(
    monkeypatch,
    tmp_path,
):
    """Reject heartbeat and attachment drift after plan review."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_cas_" + uuid.uuid4().hex[:10]
    setup_connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    try:
        await create_stale_schema(setup_connection, schema_name)
        await seed_ready_pair(setup_connection, schema_name)
        _configure_reconciler(monkeypatch, schema_name, test_database)
        refreshed_plan = await _assert_plan_drift_guards(
            setup_connection,
            schema_name,
        )
        artifact_path = tmp_path / "paused-worker.bin"
        await _run_paused_artifact_race(
            monkeypatch,
            test_database=test_database,
            schema_name=schema_name,
            artifact_path=artifact_path,
            plan_by_field=refreshed_plan,
        )
        assert await _artifact_count(setup_connection, schema_name) == 1
        assert await _status_pair(
            setup_connection,
            schema_name,
        ) == ("building", "running")
    finally:
        await setup_connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)
