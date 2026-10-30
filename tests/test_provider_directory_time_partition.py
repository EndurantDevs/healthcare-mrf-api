# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import json
import weakref

import pytest

from process.provider_directory_time_partition import (
    CountObservation,
    PartitionPlan,
    PartitionPlanError,
    PlanStatus,
    fingerprint_resource,
    parse_utc_instant,
    run_partition_passes,
)

UTC = datetime.UTC
HOUR = datetime.timedelta(hours=1)


def instant(hour: int) -> datetime.datetime:
    return datetime.datetime(2026, 1, 1, hour, tzinfo=UTC)


def resource(resource_id: str, *, updated: str = "first", active: bool = True) -> dict:
    return {
        "resourceType": "Practitioner",
        "id": resource_id,
        "active": active,
        "meta": {"lastUpdated": updated, "versionId": updated},
    }


def plan_for_two_windows() -> PartitionPlan:
    plan = PartitionPlan.create(instant(0), instant(4), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(3))
    plan.observe_count("root.0", CountObservation.exact(1))
    plan.observe_count("root.1", CountObservation.exact(1))
    return plan


def record_twin_passes(plan: PartitionPlan, window_id: str, resources: list[dict]) -> None:
    plan.record_pass(window_id, 1, resources, complete=True)
    plan.record_pass(window_id, 2, resources, complete=True)


@pytest.mark.parametrize(
    "start,end",
    [
        (datetime.datetime(2026, 1, 1), instant(1)),
        (instant(0), datetime.datetime(2026, 1, 1, 2, tzinfo=datetime.timezone(HOUR))),
        (instant(1), instant(1)),
        (instant(2), instant(1)),
    ],
)
def test_plan_requires_ordered_explicit_utc_instants(start, end):
    with pytest.raises(PartitionPlanError):
        PartitionPlan.create(start, end, ceiling=10, minimum_width=HOUR)


def test_parse_utc_instant_rejects_non_utc_offset():
    with pytest.raises(PartitionPlanError, match="explicitly UTC"):
        parse_utc_instant("2026-01-01T01:00:00+01:00")


def test_recursive_split_produces_ordered_non_overlapping_half_open_windows():
    plan = PartitionPlan.create(instant(0), instant(4), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(8))
    plan.observe_count("root.0", CountObservation.exact(4))
    plan.observe_count("root.1", CountObservation.exact(2))
    plan.observe_count("root.0.0", CountObservation.exact(2))
    plan.observe_count("root.0.1", CountObservation.exact(2))

    leaves = plan.leaf_windows()

    assert [(entry.start, entry.end) for entry in leaves] == [
        (instant(0), instant(1)),
        (instant(1), instant(2)),
        (instant(2), instant(4)),
    ]
    assert all(left.end <= right.start for left, right in zip(leaves, leaves[1:]))
    assert plan.status is PlanStatus.ACQUIRING


def test_boundary_precision_aligns_root_and_odd_recursive_splits():
    precision = datetime.timedelta(seconds=1)
    plan = PartitionPlan.create(
        datetime.datetime(2025, 2, 10, 8, 0, 24, 623858, tzinfo=UTC),
        datetime.datetime(2025, 2, 10, 8, 0, 28, 100000, tzinfo=UTC),
        ceiling=1,
        minimum_width=precision,
        boundary_precision=precision,
    )

    root = plan.windows["root"]
    assert root.start == datetime.datetime(2025, 2, 10, 8, 0, 24, tzinfo=UTC)
    assert root.end == datetime.datetime(2025, 2, 10, 8, 0, 29, tzinfo=UTC)

    plan.observe_count("root", CountObservation.exact(4))
    plan.observe_count("root.0", CountObservation.exact(2))
    plan.observe_count("root.1", CountObservation.exact(2))

    assert [(window.start.second, window.end.second) for window in plan.leaf_windows()] == [
        (24, 25),
        (25, 26),
        (26, 27),
        (27, 29),
    ]
    assert all(window.start.microsecond == window.end.microsecond == 0 for window in plan.windows.values())


def test_boundary_precision_must_divide_minimum_width():
    with pytest.raises(PartitionPlanError, match="exact multiple"):
        PartitionPlan.create(
            instant(0),
            instant(1),
            ceiling=2,
            minimum_width=datetime.timedelta(milliseconds=1500),
            boundary_precision=datetime.timedelta(seconds=1),
        )


@pytest.mark.parametrize(
    "observation,code",
    [
        (CountObservation.error("timeout"), "count_error"),
        (CountObservation.unknown(), "count_unknown"),
    ],
)
def test_non_exact_count_terminally_fails(observation, code):
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)

    plan.observe_count("root", observation)

    assert plan.status is PlanStatus.FAILED
    assert plan.failure.code == code


def test_contradictory_count_terminally_fails_after_idempotent_replay():
    plan = PartitionPlan.create(instant(0), instant(2), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(2))
    plan.observe_count("root", CountObservation.exact(2))

    plan.observe_count("root", CountObservation.exact(1))

    assert plan.failure.code == "contradictory_count"


@pytest.mark.parametrize("bad_count", [-1, True, 1.5])
def test_exact_count_rejects_invalid_values(bad_count):
    with pytest.raises(PartitionPlanError):
        CountObservation.exact(bad_count)


def test_over_ceiling_unsplittable_window_terminally_fails():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)

    plan.observe_count("root", CountObservation.exact(3))

    assert plan.status is PlanStatus.FAILED
    assert plan.failure.code == "minimum_width_overflow"


def test_window_below_two_minimum_widths_cannot_create_invalid_children():
    plan = PartitionPlan.create(
        instant(0),
        instant(0) + datetime.timedelta(minutes=90),
        ceiling=2,
        minimum_width=HOUR,
    )

    plan.observe_count("root", CountObservation.exact(3))

    assert plan.failure.code == "minimum_width_overflow"
    assert list(plan.windows) == ["root"]


def test_checkpoint_is_deterministic_json_and_round_trips():
    plan = plan_for_two_windows()
    plan.record_pass("root.1", 1, [resource("right")], complete=True)
    checkpoint = plan.to_json()

    restored = PartitionPlan.from_json(checkpoint)

    assert restored.to_json() == checkpoint
    assert json.loads(checkpoint)["windows"][0]["start"].endswith("Z")
    assert restored.status is PlanStatus.ACQUIRING


def test_checkpoint_rejects_noncontiguous_leaf_windows():
    checkpoint = plan_for_two_windows().to_dict()
    checkpoint["windows"][1]["end"] = "2026-01-01T01:00:00.000000Z"

    with pytest.raises(PartitionPlanError, match="contiguous"):
        PartitionPlan.from_dict(checkpoint)


def test_checkpoint_rejects_window_outside_configured_boundary_precision():
    plan = PartitionPlan.create(
        instant(0),
        instant(2),
        ceiling=2,
        minimum_width=datetime.timedelta(seconds=1),
        boundary_precision=datetime.timedelta(seconds=1),
    )
    checkpoint = plan.to_dict()
    checkpoint["windows"][0]["start"] = "2026-01-01T00:00:00.500000Z"

    with pytest.raises(PartitionPlanError, match="not aligned"):
        PartitionPlan.from_dict(checkpoint)


def test_checkpoint_rejects_pass_numbers_that_could_manufacture_success():
    checkpoint = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    checkpoint.observe_count("root", CountObservation.exact(0))
    checkpoint_fields = checkpoint.to_dict()
    checkpoint_fields["windows"][0]["passes"] = {"3": {}, "4": {}}

    with pytest.raises(PartitionPlanError, match="pass sequence"):
        PartitionPlan.from_dict(checkpoint_fields)


def test_success_requires_two_complete_passes_for_every_leaf():
    plan = plan_for_two_windows()
    record_twin_passes(plan, "root.0", [resource("left")])
    plan.record_pass("root.1", 1, [resource("right")], complete=True)

    assert plan.status is PlanStatus.ACQUIRING

    plan.record_pass("root.1", 2, [resource("right")], complete=True)

    assert plan.status is PlanStatus.SUCCEEDED


@pytest.mark.parametrize(
    "complete,bounded,code",
    [
        (False, False, "incomplete_window"),
        (True, True, "bounded_window"),
    ],
)
def test_incomplete_or_bounded_window_terminally_fails(complete, bounded, code):
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(0))

    plan.record_pass("root", 1, [], complete=complete, bounded=bounded)

    assert plan.status is PlanStatus.FAILED
    assert plan.failure.code == code


def test_exact_count_must_reconcile_with_unique_resources():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(2))

    plan.record_pass("root", 1, [resource("only-one")], complete=True)

    assert plan.failure.code == "count_mismatch"


def test_duplicate_resource_id_within_window_terminally_fails():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(2))

    plan.record_pass("root", 1, [resource("same"), resource("same")], complete=True)

    assert plan.failure.code == "duplicate_resource_id"


def test_duplicate_resource_id_across_windows_terminally_fails():
    plan = plan_for_two_windows()
    plan.record_pass("root.0", 1, [resource("same")], complete=True)

    plan.record_pass("root.1", 1, [resource("same")], complete=True)

    assert plan.failure.code == "duplicate_resource_id"


def test_twin_pass_fingerprint_ignores_configured_volatile_paths():
    plan = PartitionPlan.create(
        instant(0),
        instant(1),
        ceiling=2,
        minimum_width=HOUR,
        volatile_metadata_paths=("/meta/lastUpdated", "/meta/versionId"),
    )
    plan.observe_count("root", CountObservation.exact(1))
    plan.record_pass("root", 1, [resource("stable", updated="first")], complete=True)

    plan.record_pass("root", 2, [resource("stable", updated="second")], complete=True)

    assert plan.status is PlanStatus.SUCCEEDED


def test_twin_pass_fingerprint_detects_nonvolatile_change():
    plan = PartitionPlan.create(
        instant(0),
        instant(1),
        ceiling=2,
        minimum_width=HOUR,
        volatile_metadata_paths=("/meta/lastUpdated", "/meta/versionId"),
    )
    plan.observe_count("root", CountObservation.exact(1))
    plan.record_pass("root", 1, [resource("changed", active=True)], complete=True)

    plan.record_pass("root", 2, [resource("changed", active=False)], complete=True)

    assert plan.failure.code == "twin_pass_mismatch"


def test_twin_pass_detects_resource_id_set_change_even_when_counts_match():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(1))
    plan.record_pass("root", 1, [resource("first")], complete=True)

    plan.record_pass("root", 2, [resource("second")], complete=True)

    assert plan.failure.code == "twin_pass_mismatch"


def test_fingerprint_is_key_order_independent_and_supports_escaped_pointer():
    first_resource_dict = {"id": "one", "meta": {"a/b": "volatile"}, "active": True}
    second_resource_dict = {"active": True, "meta": {"a/b": "changed"}, "id": "one"}

    assert fingerprint_resource(first_resource_dict, ["/meta/a~1b"]) == fingerprint_resource(
        second_resource_dict,
        ["/meta/a~1b"],
    )


def test_invalid_resource_id_terminally_fails():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(1))

    plan.record_pass("root", 1, [{"resourceType": "Practitioner"}], complete=True)

    assert plan.failure.code == "invalid_resource_id"


def test_second_pass_cannot_precede_first_pass():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(0))

    with pytest.raises(PartitionPlanError, match="pass 1"):
        plan.record_pass("root", 2, [], complete=True)


def test_terminal_plan_rejects_additional_operations():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=2, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.unknown())

    with pytest.raises(PartitionPlanError, match="terminally failed"):
        plan.observe_count("root", CountObservation.exact(0))


def _counted_parallel_plan():
    plan = PartitionPlan.create(instant(0), instant(4), ceiling=1, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(4))
    for side in ("root.0", "root.1"):
        plan.observe_count(side, CountObservation.exact(2))
        for suffix in ("0", "1"):
            plan.observe_count(f"{side}.{suffix}", CountObservation.exact(1))
    return plan


class _DownloadedWindow:
    def __init__(self, resource_id):
        self.resources = (resource(resource_id),)


class _PartitionDownloadRecorder:
    def __init__(self, concurrency):
        self.concurrency = concurrency
        self.first_wave_ready = asyncio.Event()
        self.active_windows = set()
        self.live_buffers = weakref.WeakSet()
        self.maximum_active = 0
        self.maximum_buffers = 0
        self.started_passes = []
        self.committed_passes = []
        self.is_persisting = False

    async def fetch(self, window):
        """Overlap one bounded wave and track retained download buffers."""
        pass_number = 2 if 1 in window.passes else 1
        self.started_passes.append((window.window_id, pass_number))
        self.active_windows.add(window.window_id)
        self.maximum_active = max(self.maximum_active, len(self.active_windows))
        try:
            if len(self.active_windows) == self.concurrency:
                self.first_wave_ready.set()
            await self.first_wave_ready.wait()
            await asyncio.sleep(0)
            downloaded = _DownloadedWindow(window.window_id)
            self.live_buffers.add(downloaded)
            self.maximum_buffers = max(self.maximum_buffers, len(self.live_buffers))
            return downloaded
        finally:
            self.active_windows.remove(window.window_id)

    async def persist(self, plan, window_pass, downloaded):
        """Commit serially, rejecting any second pass issued before the first."""
        window, pass_number = window_pass
        assert self.is_persisting is False
        assert pass_number == 1 or (window.window_id, 1) in self.committed_passes
        self.is_persisting = True
        await asyncio.sleep(0)
        plan.record_pass(window.window_id, pass_number, downloaded.resources, complete=True)
        window_pass_key = (window.window_id, pass_number)
        self.committed_passes.append(window_pass_key)
        assert self.started_passes.count(window_pass_key) == 1
        self.is_persisting = False


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [1, 2])
async def test_partition_downloads_are_bounded_and_committed_serially(concurrency):
    plan = _counted_parallel_plan()
    recorder = _PartitionDownloadRecorder(concurrency)

    async def persist(window_pass, downloaded):
        await recorder.persist(plan, window_pass, downloaded)

    outcome = await asyncio.wait_for(
        run_partition_passes(plan, recorder.fetch, persist, concurrency=concurrency),
        timeout=2,
    )

    assert outcome is None
    assert plan.status is PlanStatus.SUCCEEDED
    assert recorder.maximum_active == concurrency
    assert recorder.maximum_buffers <= concurrency
    assert len(recorder.started_passes) == len(recorder.committed_passes) == 8
    assert not recorder.active_windows
    assert not recorder.live_buffers


@pytest.mark.asyncio
async def test_partition_resume_skips_completed_windows_and_first_passes():
    plan = _counted_parallel_plan()
    for window in plan.leaf_windows()[:3]:
        record_twin_passes(plan, window.window_id, [resource(window.window_id)])
    remaining = plan.leaf_windows()[-1]
    plan.record_pass(remaining.window_id, 1, [resource(remaining.window_id)], complete=True)
    resumed = PartitionPlan.from_dict(plan.to_dict())
    started_windows = []

    async def fetch(window):
        started_windows.append(window.window_id)
        return [resource(window.window_id)]

    async def persist(window_pass, resources):
        window, pass_number = window_pass
        assert pass_number == 2
        resumed.record_pass(window.window_id, pass_number, resources, complete=True)

    assert await run_partition_passes(resumed, fetch, persist, concurrency=2) is None
    assert started_windows == [remaining.window_id]
    assert resumed.status is PlanStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_partition_downloads_larger_counted_windows_first_without_reordering_boundaries():
    plan = PartitionPlan.create(instant(0), instant(4), ceiling=3, minimum_width=HOUR)
    plan.observe_count("root", CountObservation.exact(4))
    plan.observe_count("root.0", CountObservation.exact(1))
    plan.observe_count("root.1", CountObservation.exact(3))
    started_windows = []

    async def fetch(window):
        started_windows.append(window.window_id)
        return [resource(f"{window.window_id}-{index}") for index in range(window.count)]

    async def persist(window_pass, resources):
        window, pass_number = window_pass
        plan.record_pass(window.window_id, pass_number, resources, complete=True)

    assert await run_partition_passes(plan, fetch, persist) is None
    assert started_windows == ["root.1", "root.1", "root.0", "root.0"]
    assert [window.window_id for window in plan.leaf_windows()] == ["root.0", "root.1"]
    assert plan.status is PlanStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_partition_retry_retains_window_committed_during_the_same_run():
    plan = _counted_parallel_plan()
    first, failed = plan.leaf_windows()[:2]
    committed = asyncio.Event()

    async def fetch(window):
        if window is first:
            return [resource(window.window_id)]
        if window is failed:
            await committed.wait()
            return None
        await asyncio.Future()

    async def persist(window_pass, resources):
        if resources is None:
            return "retry"
        window, pass_number = window_pass
        plan.record_pass(window.window_id, pass_number, resources, complete=True)
        if pass_number == 2:
            committed.set()
        return None

    assert await asyncio.wait_for(run_partition_passes(plan, fetch, persist, concurrency=2), timeout=2) == "retry"
    restored = PartitionPlan.from_dict(plan.to_dict())
    assert set(restored.windows[first.window_id].passes) == {1, 2}
    assert all(not window.passes for window in restored.leaf_windows()[1:])


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_mode", ["retry", "fetch_exception", "persist_exception", "cancel"])
async def test_partition_failure_drains_downloads_and_retains_committed_windows(failure_mode):
    plan = _counted_parallel_plan()
    finished = plan.leaf_windows()[0]
    record_twin_passes(plan, finished.window_id, [resource(finished.window_id)])
    first_pending = plan.leaf_windows()[1].window_id
    active_windows = set()
    both_started = asyncio.Event()
    persistence_started = asyncio.Event()

    async def fetch(window):
        active_windows.add(window.window_id)
        try:
            if len(active_windows) == 2:
                both_started.set()
            await both_started.wait()
            if window.window_id == first_pending:
                if failure_mode == "fetch_exception":
                    raise RuntimeError("download failed")
                return [resource(window.window_id)]
            await asyncio.Future()
        finally:
            active_windows.remove(window.window_id)

    async def persist(_window_pass, _resources):
        persistence_started.set()
        if failure_mode == "persist_exception":
            raise RuntimeError("commit failed")
        if failure_mode == "cancel":
            await asyncio.Future()
        return "retry"

    operation = asyncio.create_task(run_partition_passes(plan, fetch, persist, concurrency=2))
    if failure_mode == "cancel":
        await asyncio.wait_for(persistence_started.wait(), timeout=2)
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
    elif failure_mode.endswith("exception"):
        with pytest.raises(RuntimeError, match="failed"):
            await asyncio.wait_for(operation, timeout=2)
    else:
        assert await asyncio.wait_for(operation, timeout=2) == "retry"

    assert not active_windows
    restored = PartitionPlan.from_dict(plan.to_dict())
    assert set(restored.windows[finished.window_id].passes) == {1, 2}
    assert all(not window.passes for window in restored.leaf_windows()[1:])


@pytest.mark.asyncio
async def test_partition_does_not_start_second_pass_without_committed_first_pass():
    plan = _counted_parallel_plan()
    started_windows = []

    async def fetch(window):
        started_windows.append(window.window_id)
        return []

    async def persist(_window_pass, _resources):
        return None

    with pytest.raises(PartitionPlanError, match="was not committed"):
        await run_partition_passes(plan, fetch, persist)

    assert len(started_windows) == 1
    assert all(not window.passes for window in plan.leaf_windows())


@pytest.mark.asyncio
async def test_partition_retry_drains_a_sibling_that_already_failed():
    """Preserve the primary retry outcome while joining a settled download exception."""
    plan = _counted_parallel_plan()
    first, second = plan.leaf_windows()[:2]
    sibling_finished = asyncio.Event()
    download_tasks = []

    async def fetch(window):
        download_tasks.append(asyncio.current_task())
        if window is second:
            await asyncio.sleep(0)
            sibling_finished.set()
            raise RuntimeError("sibling download failed")
        assert window is first
        return []

    async def persist(window_pass, _resources):
        assert window_pass == (first, 1)
        await sibling_finished.wait()
        return "retry"

    assert await asyncio.wait_for(run_partition_passes(plan, fetch, persist, concurrency=2), 1) == "retry"
    assert len(download_tasks) == 2
    assert all(task.done() for task in download_tasks)
    assert all(not window.passes for window in PartitionPlan.from_dict(plan.to_dict()).leaf_windows())


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [0, 3, True, 1.0, "2"])
async def test_partition_rejects_invalid_concurrency_before_fetch(concurrency):
    async def unexpected_fetch(_window):
        pytest.fail("invalid concurrency reached download")

    with pytest.raises(PartitionPlanError, match="concurrency"):
        await run_partition_passes(_counted_parallel_plan(), unexpected_fetch, None, concurrency=concurrency)


@pytest.mark.asyncio
async def test_partition_rejects_uncounted_plan_before_fetch():
    plan = PartitionPlan.create(instant(0), instant(1), ceiling=1, minimum_width=HOUR)

    async def unexpected_fetch(_window):
        pytest.fail("uncounted plan reached download")

    with pytest.raises(PartitionPlanError, match="counted, active"):
        await run_partition_passes(plan, unexpected_fetch, None)
