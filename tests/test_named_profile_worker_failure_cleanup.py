# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partial Profile worker launches close unissued work and drain started children."""

import asyncio
import inspect

import pytest

from process import provider_directory_profile_scope_copy as scope_copy
from process import provider_directory_profile_scope_reads as reads
from process import provider_directory_profile_source_inputs as inputs
from tests import test_provider_directory_profile_scope_reads as read_cases
from tests import test_provider_directory_profile_source_inputs as input_cases


@pytest.fixture
def source_case(monkeypatch):
    yield from input_cases.case.__wrapped__(monkeypatch)


@pytest.fixture
async def read_case(monkeypatch):
    async for case in read_cases.case.__wrapped__(monkeypatch):
        yield case


async def _blocked_worker(started, drained):
    try:
        started.set()
        await asyncio.Future()
    finally:
        drained.set()


async def _unissued_worker():
    raise AssertionError("unissued worker ran")


def _reject_launch(monkeypatch, failure, pending):
    def reject(operation, **_options):
        pending.append(operation)
        raise failure

    monkeypatch.setattr(asyncio, "create_task", reject)


@pytest.mark.asyncio
async def test_source_launch_failure_closes_both_coroutines_and_drains_started_worker(source_case, monkeypatch):
    case = source_case
    model, _batch, table, _admission = input_cases._worker(case, monkeypatch)
    options_by_field = dict(model=model, relation="typed_model_run", table_projection=table)
    started, drained, pending = asyncio.Event(), asyncio.Event(), []
    failure = RuntimeError("worker launch failed")
    unopened = _unissued_worker()
    with pytest.raises(RuntimeError) as caught:
        async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):
            task = inputs.create_materialization_task(
                case.directory, _blocked_worker(started, drained), **options_by_field
            )
            await asyncio.wait_for(started.wait(), 1)
            with monkeypatch.context() as changed:
                _reject_launch(changed, failure, pending)
                inputs.create_materialization_task(case.directory, unopened, **options_by_field)
    assert caught.value is failure
    assert task.cancelled() and drained.is_set()
    assert inspect.getcoroutinestate(unopened) == inspect.CORO_CLOSED
    assert len(pending) == 1 and inspect.getcoroutinestate(pending[0]) == inspect.CORO_CLOSED


@pytest.mark.asyncio
async def test_read_launch_failure_returns_started_child_permit_and_closes_pending_work(read_case, monkeypatch):
    slots = asyncio.Semaphore(1)
    started, drained, pending = asyncio.Event(), asyncio.Event(), []
    failure = RuntimeError("read worker launch failed")
    unopened = _unissued_worker()
    with pytest.raises(RuntimeError) as caught:
        async with read_cases.prepared(read_case):
            task = reads.create_read_task(read_case.directory, _blocked_worker(started, drained), reader_slots=slots)
            await asyncio.wait_for(started.wait(), 1)
            assert slots.locked()
            with monkeypatch.context() as changed:
                _reject_launch(changed, failure, pending)
                reads.create_read_task(read_case.directory, unopened, reader_slots=slots)
    assert caught.value is failure and task.cancelled() and drained.is_set()
    assert inspect.getcoroutinestate(unopened) == inspect.CORO_CLOSED
    assert len(pending) == 1 and inspect.getcoroutinestate(pending[0]) == inspect.CORO_CLOSED
    async with asyncio.timeout(1), reads.projection_slot(slots):
        assert slots.locked()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("read failed"), asyncio.CancelledError("read canceled")])
async def test_projection_slot_releases_permit_after_body_failure(failure):
    slots = asyncio.Semaphore(1)
    with pytest.raises(type(failure)) as caught:
        async with reads.projection_slot(slots):
            assert slots.locked()
            raise failure
    assert caught.value is failure
    async with asyncio.timeout(1), reads.projection_slot(slots):
        assert slots.locked()


@pytest.mark.asyncio
@pytest.mark.parametrize("drain_fails", [False, True])
async def test_materialization_launch_failure_retains_primary_and_closes_remaining_work(monkeypatch, drain_fails):
    from process import provider_directory_fhir as fhir

    failure, cleanup_failure = RuntimeError("second launch failed"), RuntimeError("drain failed")
    original_create, original_cancel = asyncio.create_task, scope_copy._cancel_workers
    tasks, issued = [], []

    def launch(operation, **options_by_field):
        issued.append(operation)
        if len(issued) == 2:
            raise failure
        task = original_create(operation, **options_by_field)
        tasks.append(task)
        return task

    async def cancel_workers(selected):
        await original_cancel(selected)
        if drain_fails:
            raise cleanup_failure

    monkeypatch.setattr(scope_copy.source_policy, "is_source_storage_required", lambda: False)
    monkeypatch.setattr(asyncio, "create_task", launch)
    monkeypatch.setattr(scope_copy, "_cancel_workers", cancel_workers)
    started, drained = asyncio.Event(), asyncio.Event()
    options_by_field = dict(model=None, relation=None, table_projection=None)
    operations = [
        (_blocked_worker(started, drained), options_by_field),
        *[(_unissued_worker(), options_by_field) for _ in range(2)],
    ]
    expected = BaseExceptionGroup if drain_fails else RuntimeError
    with pytest.raises(expected) as caught:
        await scope_copy.gather_materialization_tasks(fhir, operations)
    if drain_fails:
        assert caught.value.exceptions == (failure, cleanup_failure)
    else:
        assert caught.value is failure
    assert tasks and all(task.done() for task in tasks)
    assert started.is_set() and drained.is_set()
    assert all(inspect.getcoroutinestate(operation) == inspect.CORO_CLOSED for operation, _options in operations)
