# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable-PostgreSQL proofs for PTG source-local lifecycle fencing."""

from __future__ import annotations

import asyncio
import time

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process.ptg_parts import ptg2_lifecycle_lock as lifecycle
from process.ptg_parts import source_pointers
from process.ptg_parts import table_setup
from tests.test_ptg_wave_recovery_storage_postgres import _dsn, _quote


def _database(*, pool_size: int = 30) -> tuple[object, Database, object]:
    engine = create_async_engine(
        _dsn().replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=pool_size,
        max_overflow=0,
        pool_timeout=2,
    )
    sessions = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autoflush=False,
    )
    return engine, Database(engine=engine, session_factory=sessions), sessions


async def _create_source_event_schema(engine, schema: str) -> None:
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await connection.execute(text(f"CREATE SCHEMA {schema}"))
        await connection.execute(
            text(
                f"CREATE TABLE {schema}.source_events "
                "(source_no integer PRIMARY KEY)"
            )
        )


async def _publish_source_event(sessions, schema: str, source_no: int) -> None:
    async with sessions.begin() as session:
        await lifecycle.acquire_ptg2_source_lifecycle_lock(
            session,
            source_key=f"source_{source_no}",
            lock_timeout="1s",
            statement_timeout="3s",
        )
        await session.execute(
            text(
                f"INSERT INTO {schema}.source_events (source_no) "
                "VALUES (:source_no)"
            ),
            {"source_no": source_no},
        )


async def _start_waiting_global_gc(sessions):
    gc_entered = asyncio.Event()

    async def run_gc() -> None:
        async with sessions.begin() as session:
            gc_entered.set()
            await lifecycle.acquire_ptg2_lifecycle_lock(
                session,
                lock_timeout="2s",
                statement_timeout="3s",
            )

    gc_task = asyncio.create_task(run_gc())
    await gc_entered.wait()
    await asyncio.sleep(0.1)
    assert not gc_task.done()
    return gc_task


async def _cancel_source_zero_waiters(sessions, engine) -> None:
    async def wait_on_source_zero() -> None:
        async with sessions.begin() as session:
            await lifecycle.acquire_ptg2_source_lifecycle_lock(
                session,
                source_key="source_0",
                lock_timeout="5s",
                statement_timeout="5s",
            )

    blocked_tasks = [
        asyncio.create_task(wait_on_source_zero()) for _ in range(4)
    ]
    await asyncio.sleep(0.1)
    for task in blocked_tasks:
        task.cancel()
    await asyncio.gather(*blocked_tasks, return_exceptions=True)
    assert engine.pool.checkedout() == 2


@pytest.mark.asyncio
async def test_source_zero_does_not_block_peers_and_gc_waits(monkeypatch):
    """Prove one held source permits peers while exclusive GC waits."""
    schema_name = "ptg_lifecycle_source_local_hol"
    schema = _quote(schema_name)
    engine, local_db, sessions = _database()
    monkeypatch.setattr(lifecycle, "db", local_db)
    await _create_source_event_schema(engine, schema)

    holder = sessions()
    holder_transaction = await holder.begin()
    await lifecycle.acquire_ptg2_source_lifecycle_lock(
        holder,
        source_key="source_0",
        lock_timeout="2s",
        statement_timeout="5s",
    )

    await asyncio.wait_for(
        asyncio.gather(
            *(
                _publish_source_event(sessions, schema, source_no)
                for source_no in range(1, 24)
            )
        ),
        timeout=3,
    )
    assert (
        await local_db.scalar(f"SELECT COUNT(*) FROM {schema}.source_events")
        == 23
    )

    gc_task = await _start_waiting_global_gc(sessions)
    await _cancel_source_zero_waiters(sessions, engine)

    await holder_transaction.commit()
    await holder.close()
    await asyncio.wait_for(gc_task, timeout=2)
    assert engine.pool.checkedout() == 0

    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    await engine.dispose()


async def _create_global_projection_schema(engine, schema: str) -> None:
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await connection.execute(text(f"CREATE SCHEMA {schema}"))
        for statement in (
            (
                f"CREATE TABLE {schema}.ptg2_snapshot ("
                "snapshot_id text PRIMARY KEY, status text NOT NULL, "
                "published_at timestamptz NOT NULL)"
            ),
            (
                f"CREATE TABLE {schema}.ptg2_current_snapshot ("
                "slot text PRIMARY KEY, snapshot_id text, "
                "previous_snapshot_id text, updated_at timestamptz)"
            ),
            (
                f"CREATE TABLE {schema}.source_publication "
                "(source_no integer PRIMARY KEY, snapshot_id text NOT NULL)"
            ),
            (
                f"INSERT INTO {schema}.ptg2_snapshot "
                "(snapshot_id, status, published_at) "
                "SELECT 'snapshot_' || ordinal, 'published', now() "
                "FROM generate_series(0, 23) AS ordinal"
            ),
            (
                f"INSERT INTO {schema}.ptg2_current_snapshot "
                "(slot, snapshot_id, updated_at) "
                "VALUES ('current', 'snapshot_0', now())"
            ),
        ):
            await connection.execute(text(statement))


async def _publish_source_and_project(
    sessions,
    schema_name: str,
    schema: str,
    source_no: int,
) -> str:
    async with sessions.begin() as session:
        await lifecycle.acquire_ptg2_source_lifecycle_lock(
            session,
            source_key=f"publication_source_{source_no}",
        )
        await session.execute(
            text(
                f"INSERT INTO {schema}.source_publication "
                "(source_no, snapshot_id) VALUES (:source_no, :snapshot_id)"
            ),
            {"source_no": source_no, "snapshot_id": f"snapshot_{source_no}"},
        )
    return await source_pointers._attempt_global_snapshot_pointer_reconciliation(
        schema_name=schema_name,
        snapshot_id=f"snapshot_{source_no}",
        updated_at=source_pointers.datetime.datetime.now(
            source_pointers.datetime.UTC
        ),
    )


@pytest.mark.asyncio
async def test_held_legacy_singleton_does_not_gate_24_source_commits(monkeypatch):
    """Prove a locked legacy singleton cannot gate source-local commits."""
    schema_name = "ptg_lifecycle_global_projection_hol"
    schema = _quote(schema_name)
    engine, local_db, sessions = _database()
    monkeypatch.setattr(lifecycle, "db", local_db)
    monkeypatch.setattr(source_pointers, "db", local_db)
    await _create_global_projection_schema(engine, schema)

    holder = sessions()
    holder_transaction = await holder.begin()
    await holder.execute(
        text(
            f"UPDATE {schema}.ptg2_current_snapshot "
            "SET updated_at = now() WHERE slot = 'current'"
        )
    )

    started = time.monotonic()
    statuses = await asyncio.wait_for(
        asyncio.gather(
            *(
                _publish_source_and_project(
                    sessions,
                    schema_name,
                    schema,
                    source_no,
                )
                for source_no in range(24)
            )
        ),
        timeout=3,
    )
    elapsed = time.monotonic() - started
    assert statuses == ["deferred"] * 24
    assert elapsed < 2
    assert (
        await local_db.scalar(f"SELECT COUNT(*) FROM {schema}.source_publication")
        == 24
    )

    await holder_transaction.rollback()
    await holder.close()
    assert engine.pool.checkedout() == 0
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    await engine.dispose()


class _RequiredProbeTable:
    __tablename__ = "ptg2_probe"


def _patch_runtime_schema_probe_models(
    monkeypatch,
    local_db,
    schema_name: str,
) -> None:
    monkeypatch.setattr(table_setup, "db", local_db)
    monkeypatch.setattr(table_setup, "resolve_ptg2_schema", lambda: schema_name)
    monkeypatch.setattr(table_setup, "PTG2_MODEL_CLASSES", (_RequiredProbeTable,))
    monkeypatch.setattr(
        table_setup,
        "PTG2_ALLOWED_AMOUNT_MIGRATION_TABLE_NAMES",
        (),
    )
    monkeypatch.setattr(table_setup, "PTG2_V4_ATTEMPT_MIGRATION_TABLE_NAMES", ())
    monkeypatch.setattr(
        table_setup,
        "PTG2_LAYOUT_BUILD_CANDIDATE_TABLE",
        "ptg2_probe",
    )
    monkeypatch.setattr(table_setup, "PTG2_BLOCK_BUILD_PIN_TABLE", "ptg2_probe")
    monkeypatch.setattr(
        table_setup,
        "PTG2_PLAN_CATALOG_OUTBOX_TABLE",
        "ptg2_probe",
    )
    monkeypatch.setattr(
        table_setup,
        "PTG2_ARTIFACT_BLOB_TABLE",
        "ptg2_probe",
    )
    monkeypatch.setattr(
        table_setup,
        "PTG2_LEGACY_GLOBAL_PROJECTION_QUEUE_TABLE",
        "ptg2_probe",
    )
    monkeypatch.setattr(lifecycle, "db", local_db)


async def _assert_bounded_schema_probes(statements: list[str]) -> None:
    probe_results = await asyncio.wait_for(
        asyncio.gather(
            *(table_setup.require_ptg2_runtime_schema_ready() for _ in range(24)),
            return_exceptions=True,
        ),
        timeout=3,
    )
    assert all(probe_result is None for probe_result in probe_results), probe_results
    assert not any(
        statement.lstrip().upper().startswith(("CREATE ", "ALTER ", "DROP "))
        for statement in statements
    )


async def _assert_missing_schema_probe(statements: list[str]) -> None:
    missing_results = await asyncio.wait_for(
        asyncio.gather(
            *(table_setup.require_ptg2_runtime_schema_ready() for _ in range(24)),
            return_exceptions=True,
        ),
        timeout=3,
    )
    assert all(
        isinstance(probe_result, table_setup.PTG2RuntimeSchemaUnavailable)
        and "missing migration-owned capabilities: table:ptg2_probe"
        in str(probe_result)
        for probe_result in missing_results
    ), missing_results
    assert not any(
        statement.lstrip().upper().startswith(("CREATE ", "ALTER ", "DROP "))
        for statement in statements
    )


@pytest.mark.asyncio
async def test_runtime_schema_probe_never_runs_ddl_or_hangs_on_relation_lock(
    monkeypatch,
):
    """Prove hot-path readiness fails boundedly without runtime DDL."""
    schema_name = "ptg_runtime_schema_hol"
    schema = _quote(schema_name)
    engine, local_db, sessions = _database()
    statements: list[str] = []

    _patch_runtime_schema_probe_models(monkeypatch, local_db, schema_name)

    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await connection.execute(text(f"CREATE SCHEMA {schema}"))
        await connection.execute(
            text(f"CREATE TABLE {schema}.ptg2_probe (probe_id integer PRIMARY KEY)")
        )

    holder = sessions()
    holder_transaction = await holder.begin()
    await holder.execute(
        text(f"LOCK TABLE {schema}.ptg2_probe IN ACCESS EXCLUSIVE MODE")
    )

    original_execute = local_db.session_factory.class_.execute

    async def recording_execute(session, statement, *args, **kwargs):
        statements.append(str(statement))
        return await original_execute(session, statement, *args, **kwargs)

    monkeypatch.setattr(
        local_db.session_factory.class_,
        "execute",
        recording_execute,
    )
    await _assert_bounded_schema_probes(statements)

    await holder_transaction.rollback()
    await holder.close()
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP TABLE {schema}.ptg2_probe"))
    await _assert_missing_schema_probe(statements)
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    await engine.dispose()
