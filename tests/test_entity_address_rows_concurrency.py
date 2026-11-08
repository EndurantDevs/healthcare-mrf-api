# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio

import pytest

from process import entity_address_preparation_admission as admitted
from process import entity_address_rows_preparation as preparation
from tests.entity_address_rows_concurrency_support import (
    assert_exclusive_refused,
    copy_cancel_gate,
    creation_cancel_gate,
    drain_tasks,
    held_consumer,
    parallel_preparation,
    staged_runner_barrier,
)
from tests.entity_address_rows_preparation_support import assert_no_row_scratch, source_select_sql


@pytest.mark.asyncio
async def test_two_workers_retire_private_scratch_while_other_consumer_stays_held(monkeypatch):
    pytest.importorskip("ptg2_address_canon", exc_type=ModuleNotFoundError)
    async with parallel_preparation(monkeypatch) as (sql, database, schema, authority, scope):
        held, release = asyncio.Event(), asyncio.Event()
        held_by_field, finished_by_field, task_by_name = {}, {}, {}
        staged_runner_barrier(monkeypatch, held, task_by_name)
        tasks = []
        try:

            async def consume(select_sql, *, native_prepared):
                """Record exact OIDs before this runner commits and cleans its private heaps."""
                finished_by_field.update(admitted._row_scratch_state(admitted._ROW_SCRATCH.get()).owned_oids)
                await sql._run_sql_phase(
                    sql._insert_raw_from_source_sql(schema, "raw_actual", select_sql, native_prepared=native_prepared),
                    context={},
                    phase="parallel synthetic rows",
                )

            tasks.append(asyncio.create_task(held_consumer(sql, database, schema, held, release, held_by_field)))
            task_by_name["held"] = tasks[0]
            tasks.append(
                asyncio.create_task(preparation.run_prepared_source(sql, schema, source_select_sql(schema), consume))
            )
            await asyncio.wait_for(held.wait(), 10)
            await assert_exclusive_refused(database, schema, "raw_actual")
            await assert_exclusive_refused(database, schema, "shared_dependency")
            await asyncio.wait_for(asyncio.shield(tasks[1]), 10)
            assert not tasks[0].done() and not release.is_set()
            assert (
                await database.scalar(
                    "SELECT count(*) FROM pg_locks WHERE pid=:pid AND relation=ANY(CAST(:oids AS oid[]))",
                    pid=held_by_field["pid"],
                    oids=list(finished_by_field.values()),
                )
                == 0
            )
            assert all(name not in scope.owned_oids for name in finished_by_field)
            assert scope.workers._value == 1
        finally:
            await drain_tasks(tasks, release)
        assert scope.workers._value == 2 and not scope.worker_tasks and not scope.row_scratch_owners
        assert authority._external_relations == {(schema, "raw_actual"), (schema, "shared_dependency")}
        assert await database.scalar(f"SELECT count(*) FROM {schema}.raw_actual") == 8
        await assert_no_row_scratch(database, schema)


@pytest.mark.asyncio
@pytest.mark.parametrize("order", ["measure_first", "drop_first"])
async def test_inventory_measurement_and_drop_retirement_never_observe_a_gap(monkeypatch, order):
    async with parallel_preparation(monkeypatch) as (sql, database, schema, authority, scope):
        capability = admitted.issue_row_scratch()
        await admitted.create_stage_sql(
            schema, "measured_private", f"CREATE TABLE {schema}.measured_private(marker text)", row_scratch=capability
        )
        entered, release = asyncio.Event(), asyncio.Event()
        fhir = admitted._fhir()
        tasks = []
        if order == "measure_first":
            original = database.all

            async def measured(statement, **params):
                """Pause only the physical catalog read after the complete snapshot was chosen."""
                if "pg_total_relation_size(relation.oid)" in statement:
                    entered.set()
                    await release.wait()
                return await original(statement, **params)

            monkeypatch.setattr(database, "all", measured)
            tasks.append(asyncio.create_task(authority.measure(fhir, schema)))
            await asyncio.wait_for(entered.wait(), 10)
            tasks.append(asyncio.create_task(admitted.drop_stage(schema, "measured_private")))
        else:
            original = authority.retire_external_relation

            async def retired(*args, **kwargs):
                """Pause after DROP committed while its captured classification is still present."""
                entered.set()
                await release.wait()
                return await original(*args, **kwargs)

            monkeypatch.setattr(authority, "retire_external_relation", retired)
            tasks.append(asyncio.create_task(admitted.drop_stage(schema, "measured_private")))
            await asyncio.wait_for(entered.wait(), 10)
            tasks.append(asyncio.create_task(authority.measure(fhir, schema)))
        try:
            await asyncio.sleep(0)
            assert not tasks[1].done()
        finally:
            release.set()
            task_results = await asyncio.gather(*tasks)
        observed = task_results[0 if order == "measure_first" else 1]
        assert ("measured_private" in {relation.relation for relation in observed}) is (order == "measure_first")
        assert {relation.relation for relation in await authority.measure(fhir, schema)} == {
            "raw_actual",
            "shared_dependency",
        }
        assert not scope.row_scratch_owners
        admitted.finish_row_scratch(capability)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["creation", "copy"])
async def test_cancelled_runner_cleans_exact_scratch_while_other_consumer_is_safe(monkeypatch, phase):
    pytest.importorskip("ptg2_address_canon", exc_type=ModuleNotFoundError)
    async with parallel_preparation(monkeypatch) as (sql, database, schema, _authority, scope):
        held, release = asyncio.Event(), asyncio.Event()
        held_by_field, captured_by_name, task_by_name = {}, {}, {}
        tasks = []
        gate_release = None
        try:
            install = creation_cancel_gate if phase == "creation" else copy_cancel_gate
            entered, gate_release = install(monkeypatch, task_by_name, captured_by_name)
            if phase == "copy":
                staged_runner_barrier(monkeypatch, held, task_by_name)

            async def consume(_select_sql, *, native_prepared):
                """Cancellation must stop this producer before it reaches its consumer."""
                pytest.fail("cancelled producer reached the consumer")

            task_by_name["cancelled"] = asyncio.create_task(
                preparation.run_prepared_source(sql, schema, source_select_sql(schema), consume)
            )
            tasks.append(task_by_name["cancelled"])
            if phase == "creation":
                await asyncio.wait_for(entered.wait(), 10)
            held_task = asyncio.create_task(held_consumer(sql, database, schema, held, release, held_by_field))
            task_by_name["held"] = held_task
            tasks.insert(0, held_task)
            await asyncio.wait_for(held.wait(), 10)
            await asyncio.wait_for(entered.wait(), 10)
            tasks[1].cancel()
            gate_release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(tasks[1], 10)
            assert captured_by_name and all(name not in scope.owned_oids for name in captured_by_name)
            assert not tasks[0].done() and scope.workers._value == 1 and scope.worker_tasks == {tasks[0]}
            assert set(scope.row_scratch_owners) == set(held_by_field["scratch"])
            assert await database.scalar(
                "SELECT xact_start IS NOT NULL FROM pg_stat_activity WHERE pid=:pid", pid=held_by_field["pid"]
            )
        finally:
            if gate_release is not None:
                gate_release.set()
            await drain_tasks(tasks, release)
        assert scope.workers._value == 2 and not scope.worker_tasks and not scope.row_scratch_owners
        assert await database.scalar(f"SELECT count(*) FROM {schema}.raw_actual") == 4
        await assert_no_row_scratch(database, schema)
