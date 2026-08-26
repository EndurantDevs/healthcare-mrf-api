# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL replay proof for durable plan-pricing imports."""

from __future__ import annotations

import asyncio
import os

import pytest
from sqlalchemy import text

from api import control_imports
from db.models import ImportRun, db
from tests.plan_pricing_idempotency_test_support import (
    assert_plan_pricing_replay_durable,
)


pytestmark = pytest.mark.asyncio(loop_scope="module")


def _require_disposable_database() -> None:
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database_name.rsplit("/", 1)[-1].lower():
        pytest.skip(
            "DB-backed control tests require HLTHPRT_DB_DATABASE to contain 'test'"
        )


def _import_run_schema() -> str:
    return ImportRun.__table__.schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


async def _reset_import_run_schema() -> None:
    _require_disposable_database()
    await db.disconnect()
    await asyncio.sleep(0)
    try:
        await db.connect()
    except Exception as exc:
        pytest.skip(f"Postgres is unavailable for DB-backed control tests: {exc}")
    schema = _import_run_schema()
    if not schema.replace("_", "").isalnum():
        raise AssertionError(f"unsafe schema name for test cleanup: {schema!r}")
    assert db.engine is not None
    async with db.engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        await connection.execute(
            text(
                f'DROP TABLE IF EXISTS "{schema}".'
                f'"{ImportRun.__tablename__}" CASCADE'
            )
        )
    control_imports._IMPORT_RUN_ENSURE_STATE.ensured = False
    await control_imports.ensure_import_run_table()


async def _drop_import_run_table() -> None:
    schema = _import_run_schema()
    if db.engine is not None:
        async with db.engine.begin() as connection:
            await connection.execute(
                text(
                    f'DROP TABLE IF EXISTS "{schema}".'
                    f'"{ImportRun.__tablename__}" CASCADE'
                )
            )
    await db.disconnect()
    await asyncio.sleep(0)
    control_imports._IMPORT_RUN_ENSURE_STATE.ensured = False


async def _fake_enqueue(run_by_field: dict) -> dict:
    return {
        "status": "queued",
        "phase_detail": "enqueued",
        "heartbeat_at": control_imports.utc_now(),
        "progress": {
            "unit": "run",
            "total": 1,
            "done": 0,
            "pct": 0,
            "message": "queued",
        },
        "metrics": {
            "enqueue_adapter": "arq_single_job",
            "queue": f"arq:{run_by_field['importer'].upper()}",
        },
        "error": None,
    }


async def test_plan_pricing_replay_is_concurrent_and_terminal_durable(
    monkeypatch,
) -> None:
    await assert_plan_pricing_replay_durable(
        monkeypatch,
        reset_schema=_reset_import_run_schema,
        drop_schema=_drop_import_run_table,
        fake_enqueue=_fake_enqueue,
    )
