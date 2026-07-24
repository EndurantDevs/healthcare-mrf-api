# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for optional V4 reconciliation stage relations."""

from __future__ import annotations

import uuid

import asyncpg
import pytest
import sqlalchemy as sa

from process.ptg_parts import ptg2_v4_stale_metadata_reconcile as reconcile
from tests.ptg2_v4_stale_metadata_postgres_support import (
    INTERNAL_RUN_ID,
    SNAPSHOT_ID,
    apply_attempt_migrations,
    configure_test_schema,
    create_stale_schema,
    create_unmigrated_stale_schema,
    database_for_dsn,
    drop_stale_schema,
    postgres_dsn,
    quoted,
    row_versions,
    seed_ready_pair,
)


OPTIONAL_TABLES = (
    "ptg2_price_set_stage",
    "ptg2_serving_rate_stage",
)


def _configure_reconciler(monkeypatch, schema_name, test_database) -> None:
    configure_test_schema(monkeypatch, schema_name)
    monkeypatch.setenv(
        reconcile.PTG2_V4_STALE_METADATA_SECONDS_ENV,
        "3600",
    )
    monkeypatch.setattr(reconcile, "db", test_database)


async def _drop_tables(connection, schema_name: str, table_names) -> None:
    for table_name in table_names:
        await connection.execute(
            f"DROP TABLE {quoted(schema_name)}.{quoted(table_name)}"
        )


async def _create_without_optional_stages(connection, schema_name: str) -> None:
    await create_unmigrated_stale_schema(connection, schema_name)
    await _drop_tables(connection, schema_name, OPTIONAL_TABLES)
    await apply_attempt_migrations(connection, schema_name)


async def _plan() -> dict:
    return await reconcile.plan_v4_stale_metadata(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
    )


async def _execute(plan_by_field: dict) -> dict:
    return await reconcile.reconcile_v4_stale_metadata(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        expected_plan_digest=plan_by_field["plan_digest"],
    )


@pytest.mark.asyncio
async def test_absent_optional_stage_relations_reconcile_idempotently(
    monkeypatch,
):
    dsn = postgres_dsn()
    schema_name = "ptg_v4_optional_" + uuid.uuid4().hex[:12]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    try:
        await _create_without_optional_stages(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        _configure_reconciler(monkeypatch, schema_name, test_database)

        plan_by_field = await _plan()
        assert plan_by_field["status"] == "ready"
        counts = plan_by_field["observations"]["attachment_counts"]
        assert counts["price_set_stage"] == 0
        assert counts["serving_rate_stage"] == 0

        report = await _execute(plan_by_field)
        assert report["status"] == "reconciled"
        versions = await row_versions(connection, schema_name)
        retry_report = await _execute(plan_by_field)
        assert retry_report["status"] == "already_reconciled"
        assert await row_versions(connection, schema_name) == versions
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_execute_reprobes_optional_relation_after_review(monkeypatch):
    dsn = postgres_dsn()
    schema_name = "ptg_v4_optional_drift_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    try:
        await _create_without_optional_stages(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        _configure_reconciler(monkeypatch, schema_name, test_database)
        plan_by_field = await _plan()
        await connection.execute(
            f"""
            CREATE TABLE {quoted(schema_name)}.ptg2_price_set_stage (
                snapshot_id varchar(96),
                price_set_hash varchar(96)
            )
            """
        )
        await connection.execute(
            f"""
            INSERT INTO {quoted(schema_name)}.ptg2_price_set_stage
                (snapshot_id, price_set_hash)
            VALUES ($1, 'late-stage-row')
            """,
            SNAPSHOT_ID,
        )

        with pytest.raises(
            reconcile.PTG2V4StaleMetadataConflict,
            match="attached_state_present",
        ):
            await _execute(plan_by_field)
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)


@pytest.mark.asyncio
async def test_missing_required_attachment_relation_still_fails_closed(
    monkeypatch,
):
    dsn = postgres_dsn()
    schema_name = "ptg_v4_required_" + uuid.uuid4().hex[:12]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    try:
        await create_stale_schema(connection, schema_name)
        await seed_ready_pair(connection, schema_name)
        await _drop_tables(connection, schema_name, ("ptg2_plan_month",))
        _configure_reconciler(monkeypatch, schema_name, test_database)

        with pytest.raises(sa.exc.ProgrammingError):
            await _plan()
        statuses = await connection.fetchrow(
            f"""
            SELECT snapshot.status, internal_run.status
              FROM {quoted(schema_name)}.ptg2_snapshot AS snapshot
              JOIN {quoted(schema_name)}.ptg2_import_run AS internal_run
                ON internal_run.import_run_id = snapshot.import_run_id
             WHERE snapshot.snapshot_id = $1
            """,
            SNAPSHOT_ID,
        )
        assert tuple(statuses) == ("building", "running")
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)
