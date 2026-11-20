# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime

import pytest

from process.provider_directory_time_partition import (
    CountObservation,
    PartitionPlan,
    PartitionPlanError,
    PlanStatus,
    WindowState,
    run_partition_passes,
)

HOUR = datetime.timedelta(hours=1)


def _instant(hour):
    return datetime.datetime(2026, 1, 1, hour, tzinfo=datetime.UTC)


def _plan(count=8, *, hours=8, ceiling=8):
    plan = PartitionPlan.create(_instant(0), _instant(hours), ceiling=ceiling, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(count))
    return plan


def _resources(window):
    return [{"resourceType": "Practitioner", "id": f"{window.window_id}-{index}"} for index in range(window.count)]


def _complete(plan, window_id):
    resources = _resources(plan.windows[window_id])
    for pass_number in (1, 2):
        plan.record_pass(window_id, pass_number, resources, complete=True)


def _serialized_window(plan, window_id):
    return next(window for window in plan.to_dict()["windows"] if window["window_id"] == window_id)


def _counted_pool_plan():
    plan = _plan(7, ceiling=3)
    plan.observe_count("root.0", CountObservation.exact(4))
    plan.observe_count("root.1", CountObservation.exact(3))
    plan.observe_count("root.0.0", CountObservation.exact(2))
    plan.observe_count("root.0.1", CountObservation.exact(2))
    return plan


async def _assert_permits_returned(slots, capacity):
    for _ in range(capacity):
        await asyncio.wait_for(slots.acquire(), timeout=2)
    assert slots.locked()
    for _ in range(capacity):
        slots.release()


def test_ready_leaf_split_preserves_parent_and_recursively_reconciles_counts():
    plan = _plan()
    original_config = plan.to_dict()["config"]

    assert plan.split_acquisition_window("root") is plan.windows["root"]
    assert plan.status is PlanStatus.COUNTING
    parent = plan.windows["root"]
    assert parent.state is WindowState.SPLIT
    assert parent.count == 8
    assert parent.split_reason is None
    assert parent.overflow_lower_bound is None
    assert parent.passes == {}
    assert [(window.start, window.end) for window in plan.leaf_windows()] == [
        (_instant(0), _instant(4)),
        (_instant(4), _instant(8)),
    ]

    plan.observe_count("root.0", CountObservation.exact(5))
    plan.observe_count("root.1", CountObservation.exact(3))
    assert plan.split_acquisition_window("root.0") is plan.windows["root.0"]
    plan.observe_count("root.0.0", CountObservation.exact(2))
    assert plan.status is PlanStatus.COUNTING
    plan.observe_count("root.0.1", CountObservation.exact(3))

    assert plan.failure is None
    assert plan.status is PlanStatus.ACQUIRING
    assert plan.windows["root.0"].count == 5
    assert sum(window.count for window in plan.leaf_windows()) == 8
    assert plan.to_dict()["config"] == original_config
    restored = PartitionPlan.from_dict(plan.to_dict())
    assert restored.to_dict() == plan.to_dict()


@pytest.mark.parametrize("second_count", [3, 5])
def test_ready_split_rejects_child_sum_below_or_above_exact_parent(second_count):
    plan = _plan()
    assert plan.split_acquisition_window("root") is plan.windows["root"]
    plan.observe_count("root.0", CountObservation.exact(4))
    assert plan.failure is None

    plan.observe_count("root.1", CountObservation.exact(second_count))

    assert plan.status is PlanStatus.FAILED
    assert plan.failure.code == "split_count_mismatch"


def test_nested_split_mismatch_cannot_resume_or_acquire_despite_balanced_root():
    plan = _plan(10, ceiling=10)
    assert plan.split_acquisition_window("root") is plan.windows["root"]
    plan.observe_count("root.0", CountObservation.exact(6))
    plan.observe_count("root.1", CountObservation.exact(4))
    assert plan.split_acquisition_window("root.0") is plan.windows["root.0"]
    assert plan.split_acquisition_window("root.1") is plan.windows["root.1"]
    plan.observe_count("root.0.0", CountObservation.exact(2))
    plan.observe_count("root.0.1", CountObservation.exact(3))

    assert plan.status is PlanStatus.COUNTING
    assert plan.windows["root.1.0"].count is None
    assert plan.windows["root.1.1"].count is None
    with pytest.raises(PartitionPlanError, match="split|count"):
        PartitionPlan.from_dict(plan.to_dict())

    plan.observe_count("root.1.0", CountObservation.exact(2))
    plan.observe_count("root.1.1", CountObservation.exact(3))

    assert sum(window.count for window in plan.leaf_windows()) == 10
    assert plan.status is PlanStatus.FAILED
    assert plan.failure.code == "split_count_mismatch"


def test_restore_rejects_balanced_root_with_inconsistent_split_descendants():
    plan = _plan()
    assert plan.split_acquisition_window("root") is plan.windows["root"]
    for side in ("root.0", "root.1"):
        plan.observe_count(side, CountObservation.exact(4))
        assert plan.split_acquisition_window(side) is plan.windows[side]
        plan.observe_count(side + ".0", CountObservation.exact(2))
        plan.observe_count(side + ".1", CountObservation.exact(2))
    checkpoint = plan.to_dict()
    for window in checkpoint["windows"]:
        if window["window_id"] == "root.0.0":
            window["count"] = 1
        elif window["window_id"] == "root.1.0":
            window["count"] = 3

    with pytest.raises(PartitionPlanError, match="split|count"):
        PartitionPlan.from_dict(checkpoint)


def test_restore_rejects_contiguous_exact_split_with_wrong_midpoint():
    plan = _plan()
    assert plan.split_acquisition_window("root") is plan.windows["root"]
    plan.observe_count("root.0", CountObservation.exact(4))
    plan.observe_count("root.1", CountObservation.exact(4))
    checkpoint = plan.to_dict()
    for window in checkpoint["windows"]:
        if window["window_id"] == "root.0":
            window["end"] = _instant(3).isoformat()
        elif window["window_id"] == "root.1":
            window["start"] = _instant(3).isoformat()

    with pytest.raises(PartitionPlanError, match="split|geometry"):
        PartitionPlan.from_dict(checkpoint)


@pytest.mark.parametrize("case", ["uncounted", "zero", "too_narrow", "pass_one", "complete", "split"])
def test_ineligible_acquisition_split_is_a_noop(case):
    plan = _plan(0 if case == "zero" else 2, hours=1 if case == "too_narrow" else 8)
    if case == "uncounted":
        plan = PartitionPlan.create(_instant(0), _instant(8), ceiling=8, minimum_width=HOUR)
    elif case == "pass_one":
        plan.record_pass("root", 1, _resources(plan.windows["root"]), complete=True)
    elif case == "complete":
        _complete(plan, "root")
    elif case == "split":
        assert plan.split_acquisition_window("root") is plan.windows["root"]
    before = plan.to_dict()

    assert plan.split_acquisition_window("root") is None

    assert plan.to_dict() == before


def test_acquisition_split_accepts_exactly_two_minimum_widths():
    plan = _plan(1, hours=2)

    assert plan.split_acquisition_window("root") is plan.windows["root"]

    assert [(window.start, window.end) for window in plan.leaf_windows()] == [
        (_instant(0), _instant(1)),
        (_instant(1), _instant(2)),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("complete_first_child", [False, True])
async def test_split_restore_and_resume_preserve_completed_sibling_passes(complete_first_child):
    plan = _plan(6, ceiling=4)
    plan.observe_count("root.0", CountObservation.exact(4))
    plan.observe_count("root.1", CountObservation.exact(2))
    _complete(plan, "root.1")
    sibling = _serialized_window(plan, "root.1")
    assert plan.split_acquisition_window("root.0") is plan.windows["root.0"]
    plan = PartitionPlan.from_dict(plan.to_dict())
    assert plan.status is PlanStatus.COUNTING
    assert _serialized_window(plan, "root.1") == sibling
    for window_id in ("root.0.0", "root.0.1"):
        plan.observe_count(window_id, CountObservation.exact(2))
    if complete_first_child:
        _complete(plan, "root.0.0")
    plan = PartitionPlan.from_dict(plan.to_dict())
    started_window_ids = []

    async def fetch(window):
        started_window_ids.append(window.window_id)
        return _resources(window)

    async def persist(window_pass, resources):
        window, pass_number = window_pass
        plan.record_pass(window.window_id, pass_number, resources, complete=True)

    assert await run_partition_passes(plan, fetch, persist) is None

    expected_window_ids = ["root.0.1"] if complete_first_child else ["root.0.0", "root.0.1"]
    assert started_window_ids == [window_id for window_id in expected_window_ids for _ in (1, 2)]
    assert plan.status is PlanStatus.SUCCEEDED
    assert _serialized_window(plan, "root.1") == sibling
    assert plan.windows["root"].count == 6
    assert plan.windows["root.0"].count == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency,capacity", [(1, 1), (3, 2)])
async def test_split_callback_stops_pool_and_drains_inflight_and_admission(concurrency, capacity):
    """Pause for child counts, drain downloads and return all shared permits."""
    plan = _counted_pool_plan()
    slots = asyncio.BoundedSemaphore(capacity)
    wave_ready = asyncio.Event()
    blocked = asyncio.Event()
    started, finished, cancelled, active, tasks, persisted = [], set(), set(), set(), [], []

    async def fetch(window):
        started.append(window.window_id)
        tasks.append(asyncio.current_task())
        active.add(window.window_id)
        try:
            if len(active) == capacity:
                wave_ready.set()
            await wave_ready.wait()
            if window.window_id == "root.1":
                return "timeout"
            try:
                await blocked.wait()
            except asyncio.CancelledError:
                cancelled.add(window.window_id)
                raise
            pytest.fail("blocked download should have been cancelled")
        finally:
            active.remove(window.window_id)
            finished.add(window.window_id)

    async def persist(window_pass, result):
        window, pass_number = window_pass
        persisted.append((window.window_id, pass_number))
        assert result == "timeout"
        assert plan.split_acquisition_window(window.window_id) is plan.windows[window.window_id]
        return None

    outcome = await asyncio.wait_for(
        run_partition_passes(plan, fetch, persist, concurrency=concurrency, window_slots=slots),
        timeout=2,
    )

    assert outcome is None
    assert plan.status is PlanStatus.COUNTING
    assert plan.failure is None
    expected_started = ["root.1"] if capacity == 1 else ["root.1", "root.0.0"]
    assert started == expected_started
    assert persisted == [("root.1", 1)]
    assert finished == set(expected_started)
    assert cancelled == set(expected_started) - {"root.1"}
    assert not active
    assert all(task.done() for task in tasks)
    assert plan.windows["root.1"].count == 3
    assert plan.windows["root.1"].passes == {}
    await _assert_permits_returned(slots, capacity)
