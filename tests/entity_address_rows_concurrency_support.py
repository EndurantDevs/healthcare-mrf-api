# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real backend probes for selective row scratch and common stage locks."""

import asyncio
from contextlib import asynccontextmanager

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import entity_address_preparation_admission as admitted
from process import entity_address_rows_preparation as preparation
from process.provider_directory_projection_stage import _copy_driver
from tests.entity_address_rows_preparation_support import preparation_admission, preparation_database, source_select_sql


@asynccontextmanager
async def parallel_preparation(monkeypatch):
    async with preparation_database(monkeypatch) as (sql, database, schema):
        await database.status(f"DROP TABLE {schema}.raw_actual")
        inventories = []
        async with preparation_admission(sql, database, schema, monkeypatch, inventories, workers=2) as (
            authority,
            scope,
        ):
            await admitted.create_stage_sql(schema, "raw_actual", sql._prepare_raw_stage_sql(schema, "raw_actual"))
            await admitted.create_stage_sql(
                schema, "shared_dependency", f"CREATE TABLE {schema}.shared_dependency(marker text)"
            )
            yield sql, database, schema, authority, scope


async def assert_exclusive_refused(database, schema, name):
    with pytest.raises(DBAPIError) as error:
        async with database.engine.begin() as connection:
            await connection.execute(text(f"LOCK TABLE {schema}.{name} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    assert getattr(error.value.orig, "sqlstate", None) == "55P03"


async def held_consumer(sql, database, schema, held, release, observed):
    async def consume(select_sql, *, native_prepared):
        """Hold one actual consumer transaction while another runner retires its scratch."""
        capability = admitted._ROW_SCRATCH.get()
        observed["capability"] = capability
        observed["scratch"] = dict(admitted._row_scratch_state(capability).owned_oids)
        driver = await _copy_driver(database._transaction_binding().session)
        observed["pid"] = await driver.fetchval("SELECT pg_backend_pid()")
        held.set()
        await release.wait()
        await database.status(
            sql._insert_raw_from_source_sql(schema, "raw_actual", select_sql, native_prepared=native_prepared)
        )

    await preparation.run_prepared_source(sql, schema, source_select_sql(schema), consume)


async def drain_tasks(tasks, release):
    release.set()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(result is None or isinstance(result, asyncio.CancelledError) for result in results), results


def staged_runner_barrier(monkeypatch, held, task_by_name):
    ready = asyncio.Event()
    ready_tasks = set()
    original = preparation._consume_candidate

    async def consume(*args):
        """Register both scratch pairs before the held runner acquires any row locks."""
        task = asyncio.current_task()
        ready_tasks.add(task)
        if len(ready_tasks) == 2:
            ready.set()
        await ready.wait()
        if task is not task_by_name["held"]:
            await held.wait()
        return await original(*args)

    monkeypatch.setattr(preparation, "_consume_candidate", consume)


def creation_cancel_gate(monkeypatch, task_by_name, captured_by_name):
    entered, release = asyncio.Event(), asyncio.Event()
    original = admitted._create_owned_relation

    async def create(schema, name, operation, row_scratch=None, row_state=None):
        """Hold a successfully registered creation until its original waiter is cancelled."""
        await original(schema, name, operation, row_scratch, row_state)
        if row_state is not None and row_state.task is task_by_name.get("cancelled"):
            captured_by_name.update(row_state.owned_oids)
            entered.set()
            await release.wait()

    monkeypatch.setattr(admitted, "_create_owned_relation", create)
    return entered, release


def copy_cancel_gate(monkeypatch, task_by_name, captured_by_name):
    entered, release = asyncio.Event(), asyncio.Event()
    original = asyncpg.Connection.copy_to_table

    async def copy(connection, *args, **kwargs):
        """Pause a real PostgreSQL COPY after its first partial frame was sent."""
        if asyncio.current_task() is task_by_name.get("cancelled"):
            source = kwargs["source"]
            captured_by_name.update(admitted._row_scratch_state(admitted._ROW_SCRATCH.get()).owned_oids)

            async def chunks():
                """Leave the server waiting for the remainder of this bounded native frame."""
                yield source.read(64)
                entered.set()
                await release.wait()
                yield source.read()

            kwargs["source"] = chunks()
        return await original(connection, *args, **kwargs)

    monkeypatch.setattr(asyncpg.Connection, "copy_to_table", copy)
    return entered, release
