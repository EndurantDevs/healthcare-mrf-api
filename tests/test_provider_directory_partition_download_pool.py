# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import weakref
from types import SimpleNamespace

import pytest

from process.provider_directory_time_partition import CountObservation, PartitionPlan, PlanStatus, run_partition_passes
from tests.test_provider_directory_time_partition import HOUR, _DownloadedWindow, instant, resource


def counted_plan():
    plan = PartitionPlan.create(instant(0), instant(16), ceiling=1, minimum_width=HOUR)
    while plan.pending_count_windows():
        for window in plan.pending_count_windows():
            plan.observe_count(window.window_id, CountObservation.exact((window.end - window.start) // HOUR))
    return plan


async def assert_all_permits_available(slots, count):
    async with asyncio.timeout(1):
        for _ in range(count):
            await slots.acquire()
    assert slots.locked()
    for _ in range(count):
        slots.release()


@pytest.mark.asyncio
async def test_remaining_resource_claims_all_freed_slots_before_current_download_finishes():
    slots = asyncio.BoundedSemaphore(8)
    for _ in range(7):
        await slots.acquire()
    plan = counted_plan()
    first_started, all_started, finish_downloads = asyncio.Event(), asyncio.Event(), asyncio.Event()
    active_windows = set()
    parent = None

    async def fetch(window):
        active_windows.add(window.window_id)
        first_started.set()
        if len(active_windows) == 8:
            all_started.set()
        try:
            await finish_downloads.wait()
            return [resource(window.window_id)]
        finally:
            active_windows.remove(window.window_id)

    async def persist(window_pass, rows):
        assert asyncio.current_task() is parent
        window, number = window_pass
        plan.record_pass(window.window_id, number, rows, complete=True)

    parent = asyncio.create_task(run_partition_passes(plan, fetch, persist, concurrency=8, window_slots=slots))
    try:
        await asyncio.wait_for(first_started.wait(), 1)
        assert len(active_windows) == 1
        for _ in range(7):
            slots.release()
        await asyncio.wait_for(all_started.wait(), 1)
        assert len(active_windows) == 8
        assert all(not window.passes for window in plan.leaf_windows())
        finish_downloads.set()
        assert await asyncio.wait_for(parent, 2) is None
        assert plan.status is PlanStatus.SUCCEEDED
    finally:
        finish_downloads.set()
        parent.cancel()
        await asyncio.gather(parent, return_exceptions=True)
    await assert_all_permits_available(slots, 8)


@pytest.mark.asyncio
async def test_two_resources_bound_all_completed_buffers_until_their_parent_commits():
    slots = asyncio.BoundedSemaphore(8)
    buffers = weakref.WeakSet()
    second_downloaded = asyncio.Event()
    tracking = SimpleNamespace(maximum_buffers=0)

    async def run_resource(name):
        plan = counted_plan()
        parent = asyncio.current_task()
        writer = SimpleNamespace(commits=0, is_persisting=False)

        async def fetch(window):
            await asyncio.sleep(0)
            downloaded = _DownloadedWindow(window.window_id)
            buffers.add(downloaded)
            tracking.maximum_buffers = max(tracking.maximum_buffers, len(buffers))
            if name == "second":
                second_downloaded.set()
            return downloaded

        async def persist(window_pass, downloaded):
            assert asyncio.current_task() is parent
            assert not writer.is_persisting
            writer.is_persisting = True
            if name == "first" and writer.commits == 1:
                await second_downloaded.wait()
            await asyncio.sleep(0)
            window, number = window_pass
            plan.record_pass(window.window_id, number, downloaded.resources, complete=True)
            writer.commits += 1
            writer.is_persisting = False

        assert await run_partition_passes(plan, fetch, persist, concurrency=8, window_slots=slots) is None
        assert plan.status is PlanStatus.SUCCEEDED

    await asyncio.wait_for(asyncio.gather(run_resource("first"), run_resource("second")), 3)
    assert tracking.maximum_buffers == 8
    assert not buffers
    await assert_all_permits_available(slots, 8)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["retry", "fetch", "persist", "cancel"])
async def test_shared_small_budget_drains_failures_without_leaking_admission(failure):
    slots = asyncio.BoundedSemaphore(2)
    active_windows = set()
    started = asyncio.Event()

    async def fetch(window):
        active_windows.add(window.window_id)
        started.set()
        try:
            await asyncio.sleep(0)
            if failure == "fetch":
                raise RuntimeError("download failed")
            if failure == "cancel":
                await asyncio.Future()
            return []
        finally:
            active_windows.remove(window.window_id)

    async def persist(_window_pass, _rows):
        if failure == "persist":
            raise RuntimeError("commit failed")
        return "retry"

    operation = asyncio.create_task(
        run_partition_passes(counted_plan(), fetch, persist, concurrency=8, window_slots=slots)
    )
    if failure == "cancel":
        await asyncio.wait_for(started.wait(), 1)
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
    elif failure in {"fetch", "persist"}:
        with pytest.raises(RuntimeError, match="failed"):
            await asyncio.wait_for(operation, 2)
    else:
        assert await asyncio.wait_for(operation, 2) == "retry"
    assert not active_windows
    await assert_all_permits_available(slots, 2)


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_release_another_resources_permits():
    slots = asyncio.BoundedSemaphore(1)
    await slots.acquire()

    async def unexpected(*_args):
        pytest.fail("admission was not granted")

    operation = asyncio.create_task(
        run_partition_passes(counted_plan(), unexpected, unexpected, concurrency=8, window_slots=slots)
    )
    await asyncio.sleep(0)
    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert slots.locked()
    slots.release()
    await assert_all_permits_available(slots, 1)


@pytest.mark.asyncio
async def test_download_setup_failure_refunds_the_acquired_window_permit():
    slots = asyncio.BoundedSemaphore(1)

    def failed_fetch(_window):
        raise RuntimeError("download setup failed")

    with pytest.raises(RuntimeError, match="download setup failed"):
        await run_partition_passes(counted_plan(), failed_fetch, None, concurrency=8, window_slots=slots)
    await assert_all_permits_available(slots, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["retry", "persist", "cancel"])
async def test_admission_acquired_during_persistence_is_refunded_on_failure(failure):
    admitted, persisting, finish_persist = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class ObservedSlots(asyncio.BoundedSemaphore):
        async def acquire(self):
            result = await super().acquire()
            if asyncio.current_task().get_name() == "provider-directory-partition-admission":
                admitted.set()
            return result

    slots = ObservedSlots(2)
    await slots.acquire()

    async def fetch(_window):
        return []

    async def persist(_window_pass, _rows):
        persisting.set()
        await finish_persist.wait()
        if failure == "persist":
            raise RuntimeError("commit failed")
        return "retry"

    operation = asyncio.create_task(
        run_partition_passes(counted_plan(), fetch, persist, concurrency=8, window_slots=slots)
    )
    try:
        await asyncio.wait_for(persisting.wait(), 1)
        slots.release()
        await asyncio.wait_for(admitted.wait(), 1)
        if failure == "cancel":
            operation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await operation
        else:
            finish_persist.set()
            if failure == "persist":
                with pytest.raises(RuntimeError, match="commit failed"):
                    await asyncio.wait_for(operation, 1)
            else:
                assert await asyncio.wait_for(operation, 1) == "retry"
    finally:
        finish_persist.set()
        operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)
    await assert_all_permits_available(slots, 2)
