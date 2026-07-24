# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL coverage for the authoritative V4 attempt registry."""

from __future__ import annotations

import asyncio
import uuid

import asyncpg
import pytest

from process.ptg_parts import ptg2_artifact_blobs as artifact_blobs
from process.ptg_parts import ptg2_v4_stale_metadata_reconcile as reconcile
from process.ptg_parts.ptg2_v4_attempt_registry import (
    ATTEMPT_ATTACHMENTS,
)
from process.ptg_parts.ptg2_v4_stale_metadata_fence import (
    StaleMetadataFenceError,
    drop_attempt_stage_tables,
    register_attempt_stage_tables,
)
from tests.ptg2_v4_stale_metadata_postgres_support import (
    INTERNAL_RUN_ID,
    SNAPSHOT_ID,
    apply_attempt_migrations,
    create_stale_schema,
    create_unmigrated_stale_schema,
    database_for_dsn,
    drop_stale_schema,
    postgres_dsn,
    quoted,
    seed_ready_pair,
)


def _attachment_row(attachment) -> dict[str, str]:
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
    extra_value_by_table = {
        "ptg2_plan_month": ("plan_month_id", "registry-plan-month"),
        "ptg2_artifact_manifest": ("artifact_id", "registry-artifact"),
        "ptg2_import_job": ("import_job_id", "registry-import-job"),
        "ptg2_source_catalog": (
            "source_catalog_id",
            "registry-source-catalog",
        ),
        "ptg2_serving_rate": ("serving_rate_id", "registry-rate"),
        "ptg2_serving_rate_compact": (
            "serving_rate_id",
            "registry-compact-rate",
        ),
        "ptg2_price_set_stage": (
            "price_set_hash",
            "registry-price-set",
        ),
        "ptg2_serving_rate_stage": (
            "serving_rate_id",
            "registry-stage-rate",
        ),
        "ptg2_v4_attempt_stage": (
            "table_name",
            "registry_manifest_stage",
        ),
    }
    extra_value = extra_value_by_table.get(attachment.table_name)
    if extra_value is not None:
        row_by_field[extra_value[0]] = extra_value[1]
    return row_by_field


async def _wait_until_advisory_blocked(connection, backend_pid: int) -> None:
    for _attempt in range(100):
        activity = await connection.fetchrow(
            "SELECT wait_event_type, wait_event "
            "FROM pg_stat_activity WHERE pid = $1",
            backend_pid,
        )
        if (
            activity is not None
            and activity["wait_event_type"] == "Lock"
            and str(activity["wait_event"]).lower() in {
                "advisory",
                "advisorylock",
            }
        ):
            return
        await asyncio.sleep(0.01)
    raise AssertionError("direct writer did not reach the lifecycle lock")


async def _set_registry_attachment(
    connection,
    schema_name: str,
    attachment,
    *,
    is_present: bool,
) -> None:
    row_by_field = _attachment_row(attachment)
    columns = tuple(row_by_field)
    qualified_table = (
        f"{quoted(schema_name)}.{quoted(attachment.table_name)}"
    )
    if is_present:
        placeholders = ", ".join(
            f"${parameter_index}"
            for parameter_index in range(1, len(columns) + 1)
        )
        await connection.execute(
            f"INSERT INTO {qualified_table} "
            f"({', '.join(quoted(column) for column in columns)}) "
            f"VALUES ({placeholders})",
            *(row_by_field[column] for column in columns),
        )
        return
    predicates = " AND ".join(
        f"{quoted(column)} IS NOT DISTINCT FROM ${parameter_index}"
        for parameter_index, column in enumerate(columns, start=1)
    )
    await connection.execute(
        f"DELETE FROM {qualified_table} WHERE {predicates}",
        *(row_by_field[column] for column in columns),
    )


@pytest.mark.asyncio
async def test_migration_rejects_multiple_v4_snapshots_for_one_run():
    """Fail clearly before installing a fence with ambiguous run identity."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_preflight_" + uuid.uuid4().hex[:8]
    setup_connection = await asyncpg.connect(dsn)
    schema = quoted(schema_name)
    try:
        await create_unmigrated_stale_schema(
            setup_connection,
            schema_name,
        )
        await setup_connection.execute(
            f"""
            INSERT INTO {schema}.ptg2_import_run
                (import_run_id, status, options, report)
            VALUES
                ('duplicate-run', 'running',
                 json_build_object(
                     'storage_generation',
                     'shared_blocks_v4'
                 ),
                 '{{}}'::json);
            INSERT INTO {schema}.ptg2_snapshot
                (snapshot_id, import_run_id, status, manifest)
            VALUES
                ('duplicate-one', 'duplicate-run', 'building', '{{}}'::json),
                ('duplicate-two', 'duplicate-run', 'building', '{{}}'::json);
            """
        )
        with pytest.raises(
            asyncpg.PostgresError,
            match="PTG2_ATTEMPT_FENCE_V4_RUN_MULTIPLE_SNAPSHOTS",
        ):
            await apply_attempt_migrations(
                setup_connection,
                schema_name,
            )
    finally:
        await setup_connection.close()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_non_v4_multi_snapshot_run_remains_outside_the_fence():
    """Keep the V4 invariant from restricting earlier storage generations."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_legacy_" + uuid.uuid4().hex[:8]
    setup_connection = await asyncpg.connect(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(setup_connection, schema_name)
        await setup_connection.execute(
            f"""
            INSERT INTO {schema}.ptg2_import_run
                (import_run_id, status, options, report)
            VALUES
                ('legacy-run', 'running',
                 json_build_object(
                     'storage_generation',
                     'shared_blocks_v3'
                 ),
                 '{{}}'::json);
            INSERT INTO {schema}.ptg2_snapshot
                (snapshot_id, import_run_id, status, manifest)
            VALUES
                ('legacy-one', 'legacy-run', 'building', '{{}}'::json),
                ('legacy-two', 'legacy-run', 'building', '{{}}'::json);
            """
        )
        update_status = await setup_connection.execute(
            f"UPDATE {schema}.ptg2_import_run "
            "SET status = status WHERE import_run_id = 'legacy-run'"
        )
        assert update_status == "UPDATE 1"
    finally:
        await setup_connection.close()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_legacy_schema_alias_reaches_a_real_fenced_writer(
    monkeypatch,
    tmp_path,
):
    """Use DB_SCHEMA alone for the fence and the writer transaction."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_alias_" + uuid.uuid4().hex[:8]
    setup_connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    artifact_path = tmp_path / "legacy-schema-proof.bin"
    artifact_path.write_bytes(b"legacy schema writer proof")
    try:
        await create_stale_schema(setup_connection, schema_name)
        await seed_ready_pair(setup_connection, schema_name)
        monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
        monkeypatch.setenv("DB_SCHEMA", schema_name)
        monkeypatch.setattr(artifact_blobs, "db", test_database)

        await artifact_blobs.store_ptg2_artifact_file(
            artifact_path,
            snapshot_id=SNAPSHOT_ID,
            import_run_id=INTERNAL_RUN_ID,
            artifact_kind="schema_alias_proof",
            retain_local_cache=True,
        )

        artifact_count = await setup_connection.fetchval(
            f"SELECT COUNT(*) FROM "
            f"{quoted(schema_name)}.ptg2_artifact_manifest "
            "WHERE snapshot_id = $1",
            SNAPSHOT_ID,
        )
        assert artifact_count == 1
    finally:
        await setup_connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_every_registry_attachment_is_an_eligibility_blocker(
    monkeypatch,
):
    """Prove the authoritative registry fails closed one row at a time."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_registry_" + uuid.uuid4().hex[:8]
    setup_connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    try:
        await create_stale_schema(setup_connection, schema_name)
        await seed_ready_pair(setup_connection, schema_name)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
        monkeypatch.setenv(
            reconcile.PTG2_V4_STALE_METADATA_SECONDS_ENV,
            "3600",
        )
        monkeypatch.setattr(reconcile, "db", test_database)
        for attachment in ATTEMPT_ATTACHMENTS:
            await _set_registry_attachment(
                setup_connection,
                schema_name,
                attachment,
                is_present=True,
            )
            blocked_plan = await reconcile.plan_v4_stale_metadata(
                snapshot_id=SNAPSHOT_ID,
                internal_run_id=INTERNAL_RUN_ID,
            )
            assert blocked_plan["status"] == "ineligible"
            assert "attached_state_present" in blocked_plan["reason_codes"]
            assert (
                blocked_plan["observations"]["attachment_counts"][
                    attachment.name
                ]
                == 1
            )
            await _set_registry_attachment(
                setup_connection,
                schema_name,
                attachment,
                is_present=False,
            )
        ready_plan = await reconcile.plan_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
        )
        assert ready_plan["status"] == "ready"
    finally:
        await setup_connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_raw_high_volume_attachment_invalidates_idempotent_retry(
    monkeypatch,
):
    """Fail closed even if an unguarded bulk table changes after repair."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_raw_" + uuid.uuid4().hex[:8]
    setup_connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    try:
        await create_stale_schema(setup_connection, schema_name)
        await seed_ready_pair(setup_connection, schema_name)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
        monkeypatch.setenv(
            reconcile.PTG2_V4_STALE_METADATA_SECONDS_ENV,
            "3600",
        )
        monkeypatch.setattr(reconcile, "db", test_database)
        ready_plan = await reconcile.plan_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
        )
        await reconcile.reconcile_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            expected_plan_digest=ready_plan["plan_digest"],
        )
        await setup_connection.execute(
            f"INSERT INTO {quoted(schema_name)}.ptg2_serving_rate "
            "(serving_rate_id, snapshot_id) VALUES ('raw-rate', $1)",
            SNAPSHOT_ID,
        )

        retry_plan = await reconcile.plan_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
        )

        assert retry_plan["status"] == "ineligible"
        assert retry_plan["idempotent"] is False
        assert "attached_state_present" in retry_plan["reason_codes"]
        assert retry_plan["observations"]["attachment_counts"][
            "serving_rate"
        ] == 1
    finally:
        await setup_connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_direct_writer_takes_lifecycle_lock_before_snapshot_row_lock():
    """Prevent the direct-writer lock inversion guarded by DB triggers."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_order_" + uuid.uuid4().hex[:8]
    setup_connection = await asyncpg.connect(dsn)
    lifecycle_owner = await asyncpg.connect(dsn)
    direct_writer = await asyncpg.connect(dsn)
    writer_task = None
    is_owner_in_transaction = False
    try:
        await create_stale_schema(setup_connection, schema_name)
        await seed_ready_pair(setup_connection, schema_name)
        writer_pid = await direct_writer.fetchval("SELECT pg_backend_pid()")
        await lifecycle_owner.execute("BEGIN")
        is_owner_in_transaction = True
        await lifecycle_owner.execute(
            "SELECT pg_advisory_xact_lock("
            "hashtext('ptg2_source_pointer_gc_v1'))"
        )
        writer_task = asyncio.create_task(
            direct_writer.execute(
                f"UPDATE {quoted(schema_name)}.ptg2_snapshot "
                "SET status = status WHERE snapshot_id = $1",
                SNAPSHOT_ID,
            )
        )
        await _wait_until_advisory_blocked(setup_connection, writer_pid)
        await lifecycle_owner.execute("SET LOCAL lock_timeout = '500ms'")
        locked_snapshot_id = await lifecycle_owner.fetchval(
            f"SELECT snapshot_id FROM {quoted(schema_name)}.ptg2_snapshot "
            "WHERE snapshot_id = $1 FOR UPDATE",
            SNAPSHOT_ID,
        )
        assert locked_snapshot_id == SNAPSHOT_ID
        await lifecycle_owner.execute("COMMIT")
        is_owner_in_transaction = False
        await asyncio.wait_for(writer_task, timeout=2)
    finally:
        if is_owner_in_transaction:
            await lifecycle_owner.execute("ROLLBACK")
        if writer_task is not None and not writer_task.done():
            writer_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await writer_task
        await lifecycle_owner.close()
        await direct_writer.close()
        await setup_connection.close()
        await drop_stale_schema(dsn, schema_name)
