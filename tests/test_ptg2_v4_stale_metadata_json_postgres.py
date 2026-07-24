# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof that JSON values retain their database-decoded types."""

from __future__ import annotations

import uuid

import asyncpg
import pytest

from process.ptg_parts import ptg2_v4_stale_metadata_reconcile as reconcile
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


_REPORT_SHAPES = (
    ("object", "'{}'::json", "object", False, "ready"),
    ("json_string", "to_json('{}'::text)", "string", False, "ineligible"),
    ("array", "'[]'::json", "array", False, "ineligible"),
    ("scalar", "'7'::json", "number", False, "ineligible"),
    ("json_null", "'null'::json", "null", False, "ineligible"),
    ("sql_null", "NULL", None, True, "ready"),
)


def _configure_reconciler(monkeypatch, schema_name, test_database) -> None:
    configure_test_schema(monkeypatch, schema_name)
    monkeypatch.setenv(
        reconcile.PTG2_V4_STALE_METADATA_SECONDS_ENV,
        "3600",
    )
    monkeypatch.setattr(reconcile, "db", test_database)


async def _set_report_shape(connection, schema: str, expression: str) -> None:
    await connection.execute(
        f"""
        UPDATE {schema}.ptg2_import_run
           SET report = {expression}
         WHERE import_run_id = $1
        """,
        INTERNAL_RUN_ID,
    )


@pytest.mark.asyncio
async def test_database_json_shape_matrix_is_typed_and_digest_distinct(
    monkeypatch,
):
    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_json_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        _configure_reconciler(monkeypatch, schema_name, test_database)

        plan_by_shape: dict[str, dict] = {}
        for (
            shape_name,
            expression,
            expected_json_type,
            expected_sql_null,
            expected_status,
        ) in _REPORT_SHAPES:
            await _set_report_shape(connection, schema, expression)
            stored_shape = await connection.fetchrow(
                f"""
                SELECT json_typeof(report), report IS NULL
                  FROM {schema}.ptg2_import_run
                 WHERE import_run_id = $1
                """,
                INTERNAL_RUN_ID,
            )
            assert tuple(stored_shape) == (
                expected_json_type,
                expected_sql_null,
            )
            plan_by_field = await reconcile.plan_v4_stale_metadata(
                snapshot_id=SNAPSHOT_ID,
                internal_run_id=INTERNAL_RUN_ID,
            )
            plan_by_shape[shape_name] = plan_by_field
            assert plan_by_field["status"] == expected_status
            if expected_status == "ineligible":
                assert (
                    "internal_run_report_not_object"
                    in plan_by_field["reason_codes"]
                )

        assert len(
            {
                plan_by_field["plan_digest"]
                for plan_by_field in plan_by_shape.values()
            }
        ) == len(_REPORT_SHAPES)
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_object_to_json_string_drift_fails_closed_before_write(
    monkeypatch,
):
    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_json_drift_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    schema = quoted(schema_name)
    try:
        await create_stale_schema(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        _configure_reconciler(monkeypatch, schema_name, test_database)
        reviewed_plan = await reconcile.plan_v4_stale_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
        )
        assert reviewed_plan["status"] == "ready"

        await _set_report_shape(connection, schema, "to_json('{}'::text)")
        with pytest.raises(
            reconcile.PTG2V4StaleMetadataConflict,
            match="not eligible.*internal_run_report_not_object",
        ):
            await reconcile.reconcile_v4_stale_metadata(
                snapshot_id=SNAPSHOT_ID,
                internal_run_id=INTERNAL_RUN_ID,
                expected_plan_digest=reviewed_plan["plan_digest"],
            )

        persisted_state = await connection.fetchrow(
            f"""
            SELECT snapshot.status,
                   internal_run.status,
                   json_typeof(internal_run.report),
                   fence.state
              FROM {schema}.ptg2_snapshot AS snapshot
              JOIN {schema}.ptg2_import_run AS internal_run
                ON internal_run.import_run_id = snapshot.import_run_id
              JOIN {schema}.ptg2_v4_attempt_fence AS fence
                ON fence.snapshot_id = snapshot.snapshot_id
             WHERE snapshot.snapshot_id = $1
            """,
            SNAPSHOT_ID,
        )
        assert tuple(persisted_state) == (
            "building",
            "running",
            "string",
            "active",
        )
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)
