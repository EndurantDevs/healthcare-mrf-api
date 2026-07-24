# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof that stage identity is bound to reviewed V4 plans."""

from __future__ import annotations

import uuid

import asyncpg
import pytest

from process.ptg_parts import ptg2_v4_stale_metadata_reconcile as reconcile
from process.ptg_parts.ptg2_v4_attempt_registry import (
    manifest_stage_table_names,
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


def _configure_reconciler(monkeypatch, schema_name, test_database) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv(
        reconcile.PTG2_V4_STALE_METADATA_SECONDS_ENV,
        "3600",
    )
    monkeypatch.setattr(reconcile, "db", test_database)


async def _change_source_after_creating_stage(
    connection,
    schema: str,
    stage_table: str,
) -> None:
    await connection.execute(
        f"CREATE TABLE {schema}.{quoted(stage_table)} (entry_id bigint)"
    )
    await connection.execute(
        f"""
        UPDATE {schema}.ptg2_import_run
           SET options = jsonb_set(
               options::jsonb,
               '{{source_key}}',
               to_jsonb('changed-source'::text)
           )::json
         WHERE import_run_id = $1
        """,
        INTERNAL_RUN_ID,
    )


async def _assert_attempt_and_stage_remain(
    connection,
    schema: str,
    stage_table: str,
) -> None:
    statuses = await connection.fetchrow(
        f"""
        SELECT snapshot.status AS snapshot_status,
               internal_run.status AS run_status
          FROM {schema}.ptg2_snapshot AS snapshot
          JOIN {schema}.ptg2_import_run AS internal_run
            ON internal_run.import_run_id = snapshot.import_run_id
         WHERE snapshot.snapshot_id = $1
        """,
        SNAPSHOT_ID,
    )
    assert tuple(statuses) == ("building", "running")
    assert await connection.fetchval(
        "SELECT to_regclass($1)",
        f"{schema}.{quoted(stage_table)}",
    ) is not None


async def _change_report_after_plan(
    connection,
    schema: str,
) -> None:
    await connection.execute(
        f"""
        UPDATE {schema}.ptg2_import_run
           SET report = jsonb_build_object(
               'progress_seq',
               COALESCE((report::jsonb->>'progress_seq')::integer, 0) + 1
           )::json
         WHERE import_run_id = $1
        """,
        INTERNAL_RUN_ID,
    )


@pytest.mark.asyncio
async def test_source_key_drift_cannot_hide_an_unregistered_stage(
    monkeypatch,
):
    """Reject reviewed execution when the stage-name input changes."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_source_" + uuid.uuid4().hex[:8]
    setup_connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    old_stage_table = manifest_stage_table_names(
        "synthetic-source",
        SNAPSHOT_ID,
    )[0]
    schema = quoted(schema_name)
    try:
        await create_stale_schema(setup_connection, schema_name)
        await seed_ready_pair(setup_connection, schema_name)
        _configure_reconciler(monkeypatch, schema_name, test_database)
        reviewed_plan = await reconcile.plan_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
        )
        await _change_source_after_creating_stage(
            setup_connection,
            schema,
            old_stage_table,
        )
        with pytest.raises(
            reconcile.PTG2V4StaleMetadataConflict,
            match="state changed after plan review",
        ):
            await reconcile.reconcile_v4_stale_metadata(
                snapshot_id=SNAPSHOT_ID,
                internal_run_id=INTERNAL_RUN_ID,
                expected_plan_digest=reviewed_plan["plan_digest"],
            )
        await _assert_attempt_and_stage_remain(
            setup_connection,
            schema,
            old_stage_table,
        )
    finally:
        await setup_connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_run_report_drift_invalidates_the_reviewed_plan(
    monkeypatch,
):
    """Bind benign report movement even when eligibility is unchanged."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_report_" + uuid.uuid4().hex[:8]
    setup_connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(setup_connection, schema_name)
        await seed_ready_pair(setup_connection, schema_name)
        _configure_reconciler(monkeypatch, schema_name, test_database)
        reviewed_plan = await reconcile.plan_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
        )
        await _change_report_after_plan(setup_connection, schema)

        with pytest.raises(
            reconcile.PTG2V4StaleMetadataConflict,
            match="state changed after plan review",
        ):
            await reconcile.reconcile_v4_stale_metadata(
                snapshot_id=SNAPSHOT_ID,
                internal_run_id=INTERNAL_RUN_ID,
                expected_plan_digest=reviewed_plan["plan_digest"],
            )

        assert (
            await setup_connection.fetchval(
                f"""
                SELECT status
                  FROM {schema}.ptg2_import_run
                 WHERE import_run_id = $1
                """,
                INTERNAL_RUN_ID,
            )
            == "running"
        )
    finally:
        await setup_connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_physical_layout_report_is_ineligible_in_postgres(
    monkeypatch,
):
    """Route failed physical ownership to exact layout recovery."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_physical_" + uuid.uuid4().hex[:8]
    setup_connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(setup_connection, schema_name)
        await seed_ready_pair(setup_connection, schema_name)
        await setup_connection.execute(
            f"""
            UPDATE {schema}.ptg2_import_run
               SET report = jsonb_build_object(
                   'shared_snapshot_key',
                   491,
                   'shared_layout_abandonment_deferred',
                   true
               )::json
             WHERE import_run_id = $1
            """,
            INTERNAL_RUN_ID,
        )
        _configure_reconciler(monkeypatch, schema_name, test_database)

        plan_by_field = await reconcile.plan_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
        )

        assert plan_by_field["status"] == "ineligible"
        assert (
            "physical_layout_or_recovery_report_present"
            in plan_by_field["reason_codes"]
        )
        assert plan_by_field["observations"][
            "physical_layout_report_keys"
        ] == [
            "shared_layout_abandonment_deferred",
            "shared_snapshot_key",
        ]
    finally:
        await setup_connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_terminal_run_evidence_blocks_reviewed_execution(
    monkeypatch,
):
    """Preserve a late terminal diagnostic instead of overwriting it."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_terminal_" + uuid.uuid4().hex[:8]
    setup_connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(setup_connection, schema_name)
        await seed_ready_pair(setup_connection, schema_name)
        _configure_reconciler(monkeypatch, schema_name, test_database)
        reviewed_plan = await reconcile.plan_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
        )
        await setup_connection.execute(
            f"""
            UPDATE {schema}.ptg2_import_run
               SET finished_at = timezone('UTC', now()),
                   error = 'late diagnostic'
             WHERE import_run_id = $1
            """,
            INTERNAL_RUN_ID,
        )

        with pytest.raises(
            reconcile.PTG2V4StaleMetadataConflict,
            match="not eligible",
        ):
            await reconcile.reconcile_v4_stale_metadata(
                snapshot_id=SNAPSHOT_ID,
                internal_run_id=INTERNAL_RUN_ID,
                expected_plan_digest=reviewed_plan["plan_digest"],
            )

        terminal_state = await setup_connection.fetchrow(
            f"""
            SELECT status, finished_at, error
              FROM {schema}.ptg2_import_run
             WHERE import_run_id = $1
            """,
            INTERNAL_RUN_ID,
        )
        assert terminal_state["status"] == "running"
        assert terminal_state["finished_at"] is not None
        assert terminal_state["error"] == "late diagnostic"
    finally:
        await setup_connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_previous_snapshot_drift_invalidates_reviewed_plan(
    monkeypatch,
):
    """Bind predecessor identity without disclosing it in the plan."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_previous_" + uuid.uuid4().hex[:8]
    setup_connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(setup_connection, schema_name)
        await seed_ready_pair(setup_connection, schema_name)
        _configure_reconciler(monkeypatch, schema_name, test_database)
        reviewed_plan = await reconcile.plan_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
        )
        await setup_connection.execute(
            f"""
            UPDATE {schema}.ptg2_snapshot
               SET previous_snapshot_id = 'ptg2:previous'
             WHERE snapshot_id = $1
            """,
            SNAPSHOT_ID,
        )

        with pytest.raises(
            reconcile.PTG2V4StaleMetadataConflict,
            match="state changed after plan review",
        ):
            await reconcile.reconcile_v4_stale_metadata(
                snapshot_id=SNAPSHOT_ID,
                internal_run_id=INTERNAL_RUN_ID,
                expected_plan_digest=reviewed_plan["plan_digest"],
            )

        snapshot_state = await setup_connection.fetchrow(
            f"""
            SELECT status, previous_snapshot_id
              FROM {schema}.ptg2_snapshot
             WHERE snapshot_id = $1
            """,
            SNAPSHOT_ID,
        )
        assert tuple(snapshot_state) == ("building", "ptg2:previous")
    finally:
        await setup_connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_publication_evidence_is_ineligible_in_postgres(
    monkeypatch,
):
    """Reject a building row carrying validation or publication evidence."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_publish_" + uuid.uuid4().hex[:8]
    setup_connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(setup_connection, schema_name)
        await seed_ready_pair(setup_connection, schema_name)
        await setup_connection.execute(
            f"""
            UPDATE {schema}.ptg2_snapshot
               SET validated_at = timezone('UTC', now()),
                   published_at = timezone('UTC', now())
             WHERE snapshot_id = $1
            """,
            SNAPSHOT_ID,
        )
        _configure_reconciler(monkeypatch, schema_name, test_database)

        plan_by_field = await reconcile.plan_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
        )

        assert plan_by_field["status"] == "ineligible"
        assert {
            "snapshot_validation_evidence_present",
            "snapshot_publication_evidence_present",
        }.issubset(plan_by_field["reason_codes"])
    finally:
        await setup_connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)

