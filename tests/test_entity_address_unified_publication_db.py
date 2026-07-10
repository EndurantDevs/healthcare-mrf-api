# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import importlib
import os
import time
import uuid

import pytest

from db.connection import Database


entity_address_unified = importlib.import_module("process.entity_address_unified")


class _LiveTable:
    __main_table__ = "entity_address_unified"
    __my_additional_indexes__ = []


class _StageTable:
    __tablename__ = "entity_address_unified_stage"
    __my_additional_indexes__ = []


class _MissingStageTable:
    __tablename__ = "entity_address_unified_missing_stage"
    __my_additional_indexes__ = []


class _SupportLiveTable:
    __main_table__ = "entity_address_evidence"
    __my_additional_indexes__ = []


class _SupportStageTable:
    __tablename__ = "entity_address_evidence_stage"
    __my_additional_indexes__ = []


def _require_disposable_postgres() -> None:
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database_name.lower():
        pytest.skip("EAU publication integration tests require a disposable test database")


@asynccontextmanager
async def _temporary_schema():
    _require_disposable_postgres()
    database = Database()
    await database.connect()
    schema = f"eau_cutover_{uuid.uuid4().hex[:12]}"
    await database.status(f"CREATE SCHEMA {schema};")
    try:
        yield database, schema
    finally:
        await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


async def _prepare_live_and_stage(database: Database, schema: str) -> None:
    await database.status(
        f"CREATE TABLE {schema}.entity_address_unified "
        "(marker text NOT NULL);"
    )
    await database.status(
        f"INSERT INTO {schema}.entity_address_unified (marker) VALUES ('old');"
    )
    await database.status(
        f"CREATE UNLOGGED TABLE {schema}.entity_address_unified_stage "
        "(marker text NOT NULL);"
    )
    await database.status(
        f"INSERT INTO {schema}.entity_address_unified_stage (marker) VALUES ('new');"
    )


async def _relation_persistence(database: Database, schema: str, table_name: str) -> str | None:
    return await database.scalar(
        """
        SELECT c.relpersistence::text
          FROM pg_class AS c
          JOIN pg_namespace AS n ON n.oid = c.relnamespace
         WHERE n.nspname = :schema
           AND c.relname = :table_name;
        """,
        schema=schema,
        table_name=table_name,
    )


async def _hold_access_share_lock(
    database: Database,
    schema: str,
    reader_ready: asyncio.Event,
    release_reader: asyncio.Event,
) -> None:
    async with database.transaction():
        await database.status(
            f"LOCK TABLE {schema}.entity_address_unified IN ACCESS SHARE MODE;"
        )
        reader_ready.set()
        await release_reader.wait()


async def _observe_live_relation(
    database: Database,
    schema: str,
    observed_marker_list: list[str],
    observed_error_list: list[Exception],
    stop_observer: asyncio.Event,
) -> None:
    while not stop_observer.is_set():
        try:
            observed_marker_list.append(
                await database.scalar(
                    f"SELECT marker FROM {schema}.entity_address_unified;"
                )
            )
        except Exception as exc:  # pragma: no cover - asserted empty
            observed_error_list.append(exc)
        await asyncio.sleep(0.002)


async def _start_reader_blocked_cutover(
    reader_database: Database,
    schema: str,
) -> tuple[asyncio.Event, asyncio.Task, asyncio.Task]:
    reader_ready = asyncio.Event()
    release_reader = asyncio.Event()
    reader_task = asyncio.create_task(
        _hold_access_share_lock(reader_database, schema, reader_ready, release_reader)
    )
    await reader_ready.wait()
    publish_task = asyncio.create_task(
        entity_address_unified._publish_staged_entity_address_tables(
            schema,
            _StageTable,
            {},
            partial_support_patch=False,
            affected_group_table="",
            context={},
        )
    )
    return release_reader, reader_task, publish_task


async def _observe_completed_cutover(
    observer_database: Database,
    schema: str,
    release_reader: asyncio.Event,
    reader_task: asyncio.Task,
    publish_task: asyncio.Task,
) -> tuple[list[str], list[Exception]]:
    observed_marker_list: list[str] = []
    observed_error_list: list[Exception] = []
    stop_observer = asyncio.Event()
    observer_task = asyncio.create_task(
        _observe_live_relation(
            observer_database,
            schema,
            observed_marker_list,
            observed_error_list,
            stop_observer,
        )
    )
    try:
        release_reader.set()
        await reader_task
        await asyncio.wait_for(publish_task, timeout=3)
        for _ in range(10):
            if "new" in observed_marker_list:
                break
            await asyncio.sleep(0.01)
    finally:
        stop_observer.set()
        await observer_task
    return observed_marker_list, observed_error_list


@pytest.mark.asyncio
async def test_real_postgres_mid_swap_failure_restores_live_relation(monkeypatch):
    async with _temporary_schema() as (database, schema):
        monkeypatch.setattr(entity_address_unified, "db", database)
        await database.status(
            f"CREATE TABLE {schema}.entity_address_unified "
            "(marker text NOT NULL);"
        )
        await database.status(
            f"INSERT INTO {schema}.entity_address_unified (marker) VALUES ('old');"
        )

        with pytest.raises(Exception, match="entity_address_unified_missing_stage"):
            async with database.transaction():
                await entity_address_unified._swap_stage_table(
                    schema,
                    _LiveTable,
                    _MissingStageTable,
                )

        assert await database.scalar(
            f"SELECT marker FROM {schema}.entity_address_unified;"
        ) == "old"
        assert await database.scalar(
            "SELECT to_regclass(:relation_name) IS NULL;",
            relation_name=f"{schema}.entity_address_unified_old",
        ) is True


@pytest.mark.asyncio
async def test_real_postgres_cutover_promotes_logged_relation(monkeypatch):
    async with _temporary_schema() as (database, schema):
        monkeypatch.setattr(entity_address_unified, "db", database)
        await _prepare_live_and_stage(database, schema)

        cutover_context_map = {}
        await entity_address_unified._publish_staged_entity_address_tables(
            schema,
            _StageTable,
            {},
            partial_support_patch=False,
            affected_group_table="",
            context=cutover_context_map,
        )

        assert await database.scalar(
            f"SELECT marker FROM {schema}.entity_address_unified;"
        ) == "new"
        assert await database.scalar(
            f"SELECT marker FROM {schema}.entity_address_unified_old;"
        ) == "old"
        assert await _relation_persistence(
            database,
            schema,
            "entity_address_unified",
        ) == "p"


@pytest.mark.asyncio
async def test_real_postgres_cutover_promotes_logged_support_relation(monkeypatch):
    async with _temporary_schema() as (database, schema):
        monkeypatch.setattr(entity_address_unified, "db", database)
        await _prepare_live_and_stage(database, schema)
        await database.status(
            f"CREATE TABLE {schema}.entity_address_evidence (marker text NOT NULL);"
        )
        await database.status(
            f"INSERT INTO {schema}.entity_address_evidence (marker) VALUES ('old-support');"
        )
        await database.status(
            f"CREATE UNLOGGED TABLE {schema}.entity_address_evidence_stage "
            "(marker text NOT NULL);"
        )
        await database.status(
            f"INSERT INTO {schema}.entity_address_evidence_stage (marker) "
            "VALUES ('new-support');"
        )

        await entity_address_unified._publish_staged_entity_address_tables(
            schema,
            _StageTable,
            {_SupportLiveTable: _SupportStageTable},
            partial_support_patch=False,
            affected_group_table="",
            context={},
        )

        assert await database.scalar(
            f"SELECT marker FROM {schema}.entity_address_evidence;"
        ) == "new-support"
        assert await database.scalar(
            f"SELECT marker FROM {schema}.entity_address_evidence_old;"
        ) == "old-support"
        assert await _relation_persistence(
            database,
            schema,
            "entity_address_evidence",
        ) == "p"


@pytest.mark.asyncio
async def test_real_postgres_reader_does_not_create_missing_relation_window(
    monkeypatch,
):
    async with _temporary_schema() as (publisher_database, schema):
        reader_database = Database()
        observer_database = Database()
        await reader_database.connect()
        await observer_database.connect()
        monkeypatch.setattr(entity_address_unified, "db", publisher_database)
        monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_CUTOVER_LOCK_TIMEOUT", "20ms")
        monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_CUTOVER_RETRY_ATTEMPTS", "20")
        monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_CUTOVER_RETRY_BACKOFF_MS", "20")
        monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_CUTOVER_RETRY_MAX_BACKOFF_MS", "50")
        await _prepare_live_and_stage(publisher_database, schema)
        release_reader, reader_task, publish_task = await _start_reader_blocked_cutover(
            reader_database,
            schema,
        )

        try:
            await asyncio.sleep(0.05)
            assert not publish_task.done()
            started = time.monotonic()
            assert await observer_database.scalar(
                f"SELECT marker FROM {schema}.entity_address_unified;"
            ) == "old"
            assert time.monotonic() - started < 0.5
            observed_marker_list, observed_error_list = await _observe_completed_cutover(
                observer_database,
                schema,
                release_reader,
                reader_task,
                publish_task,
            )

            assert observed_error_list == []
            assert set(observed_marker_list) <= {"old", "new"}
            assert "new" in observed_marker_list
            assert await _relation_persistence(
                publisher_database,
                schema,
                "entity_address_unified",
            ) == "p"
        finally:
            release_reader.set()
            if not reader_task.done():
                await reader_task
            if not publish_task.done():
                publish_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await publish_task
            await reader_database.disconnect()
            await observer_database.disconnect()
