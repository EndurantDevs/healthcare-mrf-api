# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL replay proof for durable plan-pricing imports."""

from __future__ import annotations

import asyncio
import os

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import insert, select, text
from sqlalchemy.exc import IntegrityError

from api import control_imports
from db.models import ImportRun, db
from tests.plan_pricing_idempotency_test_support import (
    assert_plan_pricing_replay_durable,
)
from tests.test_plan_pricing_idempotency import _projection_migration
from tests.test_registry_source_check import _idempotency_migration as _source_check_migration

pytestmark = pytest.mark.asyncio(loop_scope="module")


def _require_disposable_database() -> None:
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database_name.rsplit("/", 1)[-1].lower():
        pytest.skip("DB-backed control tests require HLTHPRT_DB_DATABASE to contain 'test'")


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
        await connection.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{ImportRun.__tablename__}" CASCADE'))
    control_imports._IMPORT_RUN_ENSURE_STATE.ensured = False
    await control_imports.ensure_import_run_table()


async def _drop_import_run_table() -> None:
    schema = _import_run_schema()
    if db.engine is not None:
        async with db.engine.begin() as connection:
            await connection.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{ImportRun.__tablename__}" CASCADE'))
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


async def _apply_idempotency_migration(method, *, migration=None):
    migration = migration or _projection_migration()

    def apply(connection):
        with Operations.context(MigrationContext.configure(connection)):
            getattr(migration, method)()

    assert db.engine is not None
    async with db.engine.begin() as connection:
        await connection.run_sync(apply)


@pytest.mark.parametrize("index_source", ("model", "migration"))
async def test_attribution_terminal_before_second_insert_replays_once(monkeypatch, index_source):
    """The active-only index cannot protect a winner that has already finished."""

    async def reset_schema():
        await _reset_import_run_schema()
        if index_source == "migration":
            await _apply_idempotency_migration("downgrade")
            await _apply_idempotency_migration("upgrade")

    await assert_plan_pricing_replay_durable(
        monkeypatch,
        reset_schema=reset_schema,
        drop_schema=_drop_import_run_table,
        fake_enqueue=_fake_enqueue,
        importer="plan-pricing-em-attribution",
    )


@pytest.mark.parametrize("index_source", ("model", "migration", "startup-then-migration"))
async def test_source_check_terminal_before_second_insert_replays_once(monkeypatch, index_source):
    """A source-check winner finishing before a competing insert retains its only job."""

    async def reset_schema():
        await _reset_import_run_schema()
        if index_source != "model":
            migration = _source_check_migration()
            if index_source == "migration":
                await _apply_idempotency_migration("downgrade", migration=migration)
            await _apply_idempotency_migration("upgrade", migration=migration)

    await assert_plan_pricing_replay_durable(
        monkeypatch,
        reset_schema=reset_schema,
        drop_schema=_drop_import_run_table,
        fake_enqueue=_fake_enqueue,
        importer="registry-source-check",
    )


async def _index_and_runs():
    schema = _import_run_schema()
    index = (
        await db.execute(
            text("SELECT oid, pg_get_indexdef(oid) FROM pg_class WHERE oid = to_regclass(:name)"),
            name=f"{schema}.import_run_plan_pricing_idempotency_idx",
        )
    ).one()
    rows = (await db.execute(select(ImportRun).order_by(ImportRun.run_id))).scalars().all()
    return tuple(index), [control_imports.normalize_run(row) for row in rows]


async def test_attribution_duplicate_history_refuses_migration_atomically():
    """An expanded unique index must not delete historical duplicate owners."""
    await _reset_import_run_schema()
    try:
        await _apply_idempotency_migration("downgrade")
        await db.execute(
            insert(ImportRun).values(
                [
                    {
                        "run_id": f"run_duplicate_{ordinal}",
                        "engine": control_imports.ENGINE_NAME,
                        "importer": "plan-pricing-em-attribution",
                        "status": "succeeded",
                        "idempotency_key": "duplicate-terminal-attribution",
                    }
                    for ordinal in (1, 2)
                ]
            ),
        )
        before = await _index_and_runs()
        with pytest.raises(IntegrityError, match="import_run_plan_pricing_idempotency_idx"):
            await _apply_idempotency_migration("upgrade")
        assert await _index_and_runs() == before
        assert len(before[1]) == 2
    finally:
        await _drop_import_run_table()


async def test_source_check_duplicate_history_refuses_migration_atomically():
    """Conflicting terminal owners survive a refused source-check migration unchanged."""
    await _reset_import_run_schema()
    migration = _source_check_migration()
    try:
        await _apply_idempotency_migration("downgrade", migration=migration)
        await db.execute(
            insert(ImportRun).values(
                [
                    {
                        "run_id": f"run_source_check_duplicate_{ordinal}",
                        "engine": control_imports.ENGINE_NAME,
                        "importer": "registry-source-check",
                        "status": "succeeded",
                        "idempotency_key": "duplicate-terminal-source-check",
                    }
                    for ordinal in (1, 2)
                ]
            ),
        )
        before = await _index_and_runs()
        with pytest.raises(IntegrityError, match=migration.INDEX_NAME):
            await _apply_idempotency_migration("upgrade", migration=migration)
        assert await _index_and_runs() == before
        index = await db.scalar(
            text("SELECT to_regclass(:name)"),
            name=f"{_import_run_schema()}.{migration.INDEX_NAME}",
        )
        assert index is None and len(before[1]) == 2
    finally:
        await _drop_import_run_table()
