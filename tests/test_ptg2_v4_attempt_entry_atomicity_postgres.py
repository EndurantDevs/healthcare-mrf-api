# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL crash proof for atomic V4 attempt entry."""

from __future__ import annotations

import datetime
import importlib
import uuid
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process.ptg_parts.ptg2_shared_blocks import PTG2_V4_SHARED_GENERATION
from tests.ptg2_v4_stale_metadata_postgres_support import (
    INTERNAL_RUN_ID,
    SNAPSHOT_ID,
    configure_test_schema,
    create_stale_schema,
    database_for_dsn,
    drop_stale_schema,
    postgres_dsn,
    quoted,
)


process_ptg = importlib.import_module("process.ptg")


def _attempt_entry_rows() -> tuple[dict, dict]:
    started_at = datetime.datetime(2026, 7, 1)
    snapshot_by_field = {
        "snapshot_id": SNAPSHOT_ID,
        "import_run_id": INTERNAL_RUN_ID,
        "import_month": datetime.date(2026, 7, 1),
        "status": "building",
        "created_at": started_at,
        "validated_at": None,
        "published_at": None,
        "previous_snapshot_id": None,
        "manifest": {},
    }
    run_by_field = {
        "import_run_id": INTERNAL_RUN_ID,
        "import_month": datetime.date(2026, 7, 1),
        "status": "running",
        "started_at": started_at,
        "finished_at": None,
        "heartbeat_at": started_at,
        "options": {"storage_generation": PTG2_V4_SHARED_GENERATION},
        "report": {},
        "error": None,
    }
    return snapshot_by_field, run_by_field


async def _assert_attempt_row_counts(connection, schema: str, count: int):
    for table_name in (
        "ptg2_snapshot",
        "ptg2_import_run",
        "ptg2_v4_attempt_fence",
    ):
        assert await connection.fetchval(
            f"SELECT COUNT(*) FROM {schema}.{table_name}"
        ) == count


@pytest.mark.asyncio
async def test_attempt_entry_crash_rolls_back_and_exact_retry_recovers(
    monkeypatch,
):
    """Commit snapshot, run, and fence together across an entry crash."""

    dsn = postgres_dsn()
    schema_name = "ptg_v4_stale_entry_" + uuid.uuid4().hex[:8]
    connection = await asyncpg.connect(dsn)
    test_database = database_for_dsn(dsn)
    snapshot_by_field, run_by_field = _attempt_entry_rows()
    original_run_push = process_ptg._push_fenced_import_run
    try:
        await create_stale_schema(connection, schema_name)
        configure_test_schema(monkeypatch, schema_name)
        monkeypatch.setattr(process_ptg, "db", test_database)
        monkeypatch.setattr(
            process_ptg.PTG2Snapshot.__table__, "schema", schema_name
        )
        monkeypatch.setattr(
            process_ptg.PTG2ImportRun.__table__, "schema", schema_name
        )
        monkeypatch.setattr(
            process_ptg,
            "_push_fenced_import_run",
            AsyncMock(side_effect=RuntimeError("entry crash")),
        )
        with pytest.raises(RuntimeError, match="entry crash"):
            await process_ptg._push_ptg2_snapshot_preserving_publication(
                snapshot_by_field,
                initial_import_run_by_field=run_by_field,
            )
        schema = quoted(schema_name)
        await _assert_attempt_row_counts(connection, schema, 0)

        monkeypatch.setattr(
            process_ptg,
            "_push_fenced_import_run",
            original_run_push,
        )
        first_state = (
            await process_ptg._push_ptg2_snapshot_preserving_publication(
                snapshot_by_field,
                initial_import_run_by_field=run_by_field,
            )
        )
        retry_state = (
            await process_ptg._push_ptg2_snapshot_preserving_publication(
                snapshot_by_field,
                initial_import_run_by_field=run_by_field,
            )
        )
        assert first_state["snapshot_claim_status"] == "acquired"
        assert retry_state["snapshot_claim_status"] == "existing"
        await _assert_attempt_row_counts(connection, schema, 1)
    finally:
        await connection.close()
        await test_database.disconnect()
        await drop_stale_schema(dsn, schema_name)
