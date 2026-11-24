# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Planning, bounded downloads and verification for FHIR time partitions."""

from __future__ import annotations

import asyncio
import datetime
import json
import re
import urllib.parse
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from enum import Enum
from functools import lru_cache
from typing import Any, Iterable, Mapping

from process import provider_directory_bcbs_minnesota as bcbs_minnesota
from process import provider_directory_highmark as highmark
from process import provider_directory_time_partition_primitives as _primitives
from process.provider_directory_fetch_types import PartitionFetchState
from process.provider_directory_time_partition_primitives import (
    PartitionPlanError,
    fingerprint_resource,
    parse_utc_instant,
)


class CountKind(str, Enum):
    EXACT = "exact"
    OVERFLOW = "overflow"
    SPLIT = "split"
    ERROR = "error"
    UNKNOWN = "unknown"


class WindowState(str, Enum):
    NEEDS_COUNT = "needs_count"
    SPLIT = "split"
    READY = "ready"


class PlanStatus(str, Enum):
    COUNTING = "counting"
    ACQUIRING = "acquiring"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True)
class CountObservation:
    kind: CountKind
    count: int | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, CountKind):
            raise PartitionPlanError("count observation kind is invalid")
        if self.kind is CountKind.EXACT:
            if isinstance(self.count, bool) or not isinstance(self.count, int):
                raise PartitionPlanError("an exact count must be an integer")
            if self.count < 0:
                raise PartitionPlanError("an exact count cannot be negative")
            if self.detail is not None:
                raise PartitionPlanError("an exact count cannot include an error detail")
        elif self.kind is CountKind.OVERFLOW:
            if isinstance(self.count, bool) or not isinstance(self.count, int) or self.count <= 0:
                raise PartitionPlanError("an overflow lower bound must be a positive integer")
            if self.detail is not None:
                raise PartitionPlanError("an overflow observation cannot include an error detail")
        elif self.kind is CountKind.SPLIT:
            if self.count is not None or self.detail not in ("timeout", "planned"):
                raise PartitionPlanError("an uncounted split requires a timeout reason or planned reason and no count")
        elif self.count is not None:
            raise PartitionPlanError("an error or unknown count cannot include a count")

    @classmethod
    def exact(cls, count: int) -> CountObservation:
        """Create a successful authoritative count observation."""
        return cls(CountKind.EXACT, count=count)

    @classmethod
    def overflow(cls, lower_bound: int) -> CountObservation:
        """Retain a certified lower bound without inventing an exact count."""
        return cls(CountKind.OVERFLOW, count=lower_bound)

    @classmethod
    def split(cls, reason: str = "timeout") -> CountObservation:
        """Subdivide without a count only under a caller's exhaustive contract."""
        return cls(CountKind.SPLIT, detail=reason)

    @classmethod
    def error(cls, detail: str) -> CountObservation:
        """Create an observation for a failed authoritative count request."""
        return cls(CountKind.ERROR, detail=detail)

    @classmethod
    def unknown(cls, detail: str | None = None) -> CountObservation:
        """Create an observation for a non-authoritative or absent count."""
        return cls(CountKind.UNKNOWN, detail=detail)


@dataclass(frozen=True)
class PlanFailure:
    code: str
    message: str
    window_id: str | None = None


@dataclass(frozen=True)
class WindowPass:
    resource_fingerprints: dict[str, str]


@dataclass
class TimeWindow:
    window_id: str
    start: datetime.datetime
    end: datetime.datetime
    state: WindowState = WindowState.NEEDS_COUNT
    count: int | None = None
    passes: dict[int, WindowPass] = field(default_factory=dict)
    overflow_lower_bound: int | None = None
    split_reason: str | None = None

    def __post_init__(self) -> None:
        self.start = _primitives.validate_utc_instant(self.start, "window start")
        self.end = _primitives.validate_utc_instant(self.end, "window end")
        if self.start >= self.end:
            raise PartitionPlanError("a window must have start < end")


def partition_window_concurrency(directory, source_record, request_url):
    """Share the bounded transport without enabling credentialed windows."""
    session = directory._rest_page_prefetch_session(source_record, request_url)
    if session is None or session.connector is None:
        return 1
    return max(1, min(8, session.connector.limit_per_host, directory._source_resource_scan_concurrency(source_record)))


def window_hard_page_limit(directory, source_record, resource_type, expected_count):
    """Apply the declared bound to each counted window."""
    default_limit = min(directory._max_page_count(), max(1, expected_count))
    config, _ = directory._last_updated_partition_config(dict(source_record), resource_type)
    if config is not None:
        return min(default_limit, config.maximum_pages_per_window or default_limit)
    base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    if settings := directory.highmark.companion_partition_settings(base, resource_type):
        return min(default_limit, settings["maximum_pages_per_window"])
    return default_limit


async def resource_partition_config(directory, source_record, resource_type, context):
    """Consult a retained companion only when no ordinary seed declares a partition plan."""
    config, error = directory._last_updated_partition_config(source_record, resource_type)
    if error == "resource_not_opted_in":
        config, progress = await directory.highmark.admitted_companion_partition(directory, context, resource_type)
        return config, error, progress
    return config, error, (0, 0)


def partition_window_slots(directory, source_record, request_url):
    """Bound fetched and uncommitted windows across one source's resource scans."""
    concurrency = partition_window_concurrency(directory, source_record, request_url)
    if concurrency == 1:
        return None
    slots = directory.SOURCE_HTTP_REQUEST_SLOTS.get()
    if slots is None:
        slots = source_record.setdefault("_provider_directory_request_pacing_state", {})
    source_ids = source_record.get("_partition_checkpoint_source_ids") or (source_record.get("source_id"),)
    if isinstance(source_ids, str):
        source_ids = (source_ids,)
    base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    key = ("partition_windows", base, tuple(sorted(source_ids)))
    if key not in slots:
        slots[key] = asyncio.BoundedSemaphore(concurrency)
    return slots[key]


async def _fetch_partition_count_batch(directory, source_record, config, state, options, windows, slots):
    """Read disjoint windows under the shared HTTP pacing and window permits."""

    async def fetch(window):
        """Hold one shared permit through the normal count transport."""
        async with slots:
            return await directory._fetch_last_updated_partition_count(
                source_record,
                directory._last_updated_partition_count_url(state, config, window),
                timeout=options.timeout,
                deadline_at=state.deadline_at,
            )

    observations = await asyncio.gather(*(fetch(window) for window in windows), return_exceptions=True)
    for observation in observations:
        if isinstance(observation, BaseException):
            raise observation
    retry_not_before = max((item.retry_not_before for item in observations if item.retry_not_before), default=None)
    # Commit successful siblings before yielding the latest source retry fence.
    return sorted(
        (
            (window, replace(item, retry_not_before=retry_not_before) if item.transient else item)
            for window, item in zip(windows, observations, strict=True)
        ),
        key=lambda pair: pair[1].transient,
    )


async def observe_partition_counts(directory, source_record, resource_type, model, config, state, options):
    """Resolve exact leaf counts while committing all plan changes through one checkpoint owner."""
    slots = None
    if state.context.canonical_api_base == bcbs_minnesota.API_BASE and options.window_concurrency > 1:
        slots = partition_window_slots(directory, source_record, state.start_url)
    batch_width = min(4, options.window_concurrency)
    while pending := state.plan.pending_count_windows():
        if state.deadline_at is not None and directory._last_updated_partition_monotonic() >= state.deadline_at:
            return await directory._saved_partition_retry_result(
                model, resource_type, config, state, "deadline_reached", deadline_reached=True
            )
        counts = (
            await _fetch_partition_count_batch(
                directory, source_record, config, state, options, pending[:batch_width], slots
            )
            if slots is not None and len(pending) >= 2
            else (None,)
        )
        for fetched in counts:
            failure = await directory._observe_next_partition_count(
                source_record, resource_type, model, config, state, options, fetched=fetched
            )
            if failure is not None:
                return failure
    return await directory._reconcile_partition_leaf_counts(resource_type, model, config, state)


def _pending_partition_windows(plan):
    return deque(
        sorted(
            (window for window in plan.leaf_windows() if len(window.passes) < 2),
            key=lambda window: window.count or 0,
            reverse=True,
        )
    )


def _start_partition_window(windows, fetch_window, passes_by_task, slots):
    try:
        window = windows.popleft()
        pass_number = next(number for number in (1, 2) if number not in window.passes)
        fetch_task = asyncio.create_task(fetch_window(window), name="provider-directory-partition-window")
        passes_by_task[fetch_task] = (window, pass_number)
    except BaseException:
        slots.release()
        raise


async def _wait_partition_downloads(passes_by_task, admission):
    waiting_tasks = set(passes_by_task)
    if admission is not None:
        waiting_tasks.add(admission)
    settled_tasks, _ = await asyncio.wait(waiting_tasks, return_when=asyncio.FIRST_COMPLETED)
    waiting_tasks.clear()
    return settled_tasks


async def _persist_partition_downloads(passes_by_task, settled_tasks, persist_pass, windows, slots):
    while settled_tasks:
        fetch_task = next(task for task in passes_by_task if task in settled_tasks)
        settled_tasks.remove(fetch_task)
        window, pass_number = passes_by_task.pop(fetch_task)
        try:
            failure = await persist_pass((window, pass_number), fetch_task.result())
        finally:
            # Free the completed buffer before another resource can spend its permit.
            del fetch_task
            slots.release()
        if failure is not None:
            return failure
        if window.state is WindowState.SPLIT:
            continue
        if pass_number not in window.passes:
            raise PartitionPlanError("partition pass was not committed")
        if pass_number == 1:
            windows.appendleft(window)
    return None


async def _close_partition_downloads(passes_by_task, admission, slots):
    remaining_tasks = list(passes_by_task)
    if admission is not None:
        remaining_tasks.append(admission)
    for fetch_task in remaining_tasks:
        if not fetch_task.done():
            fetch_task.cancel()
    if remaining_tasks:
        try:
            await asyncio.gather(*remaining_tasks, return_exceptions=True)
        finally:
            for _task in passes_by_task:
                slots.release()
            if admission is not None and not admission.cancelled() and admission.exception() is None:
                slots.release()
            passes_by_task.clear()


async def run_partition_passes[FetchResult, FailureResult](
    plan: PartitionPlan,
    fetch_window: Callable[[TimeWindow], Awaitable[FetchResult]],
    persist_pass: Callable[[tuple[TimeWindow, int], FetchResult], Awaitable[FailureResult | None]],
    *,
    concurrency: int = 1,
    window_slots: asyncio.Semaphore | None = None,
) -> FailureResult | None:
    """Overlap downloads while one caller commits each window's ordered passes."""

    if isinstance(concurrency, bool) or not isinstance(concurrency, int) or not 1 <= concurrency <= 8:
        raise PartitionPlanError("partition concurrency must be between 1 and 8")
    if plan.status not in (PlanStatus.ACQUIRING, PlanStatus.SUCCEEDED):
        raise PartitionPlanError("partition downloads require a counted, active plan")
    windows = _pending_partition_windows(plan)
    passes_by_task: dict[asyncio.Task[FetchResult], tuple[TimeWindow, int]] = {}
    slots = window_slots if window_slots is not None else asyncio.BoundedSemaphore(concurrency)
    admission: asyncio.Task[bool] | None = None

    def start_window():
        """Transfer an acquired permit to one tracked download."""
        _start_partition_window(windows, fetch_window, passes_by_task, slots)

    try:
        while windows or passes_by_task:
            if admission is None:
                if windows and not passes_by_task:
                    await slots.acquire()
                    start_window()
                while windows and len(passes_by_task) < concurrency and not slots.locked():
                    await slots.acquire()
                    start_window()
                if windows and len(passes_by_task) < concurrency:
                    admission = asyncio.create_task(slots.acquire(), name="provider-directory-partition-admission")
            settled_tasks = await _wait_partition_downloads(passes_by_task, admission)
            if admission in settled_tasks:
                settled_tasks.remove(admission)
                admission.result()
                admission = None
                start_window()
            failure = await _persist_partition_downloads(passes_by_task, settled_tasks, persist_pass, windows, slots)
            if failure is not None or plan.status is PlanStatus.COUNTING:
                return failure
        return None
    finally:
        await _close_partition_downloads(passes_by_task, admission, slots)


async def fetch_window_for_pass(directory, source_record, resource_type, config, state, fetch_options, window):
    """Fetch a partition window with the carrier-specific recovery if applicable."""
    request_url = last_updated_partition_url(
        directory,
        state.start_url,
        window,
        page_count=config.page_count,
        count_only=False,
    )
    window_fetch = await directory._fetch_last_updated_partition_window(
        source_record,
        resource_type,
        request_url,
        window,
        timeout=fetch_options.timeout,
        cancel_ctx=fetch_options.cancel_ctx,
        cancel_task=fetch_options.cancel_task,
        deadline_at=state.deadline_at,
    )
    recovery = (
        bcbs_minnesota.verify_timed_out_organization_window
        if resource_type == "Organization"
        else bcbs_minnesota.verify_timed_out_practitioner_window
    )
    return request_url, await recovery(
        directory, source_record, resource_type, config, state, fetch_options, window, window_fetch
    )


async def fetch_partition_resource_rows(directory, source_record, resource_type, model, config, fetch_options):
    """Retain preflight and census checks while recounting subdivided data windows."""
    preflight_failure = directory._partition_preflight_failure(model, fetch_options)
    if preflight_failure is not None:
        return preflight_failure
    prepared_state = await directory._prepare_partition_state(
        source_record, resource_type, model, config, fetch_options
    )
    if isinstance(prepared_state, directory.ResourceFetchResult):
        return prepared_state
    census_failure = await directory._observe_partition_pre_census(
        source_record, resource_type, model, config, prepared_state, fetch_options
    )
    if census_failure is not None:
        return census_failure
    while True:
        count_failure = await directory._observe_partition_counts(
            source_record, resource_type, model, config, prepared_state, fetch_options
        )
        if count_failure is not None:
            return count_failure
        pass_failure = await directory._fetch_partition_plan_passes(
            source_record, resource_type, model, config, prepared_state, fetch_options
        )
        if pass_failure is not None:
            return pass_failure
        if prepared_state.plan.status is not PlanStatus.COUNTING:
            break
    return await directory._finish_partition_fetch(
        source_record, resource_type, model, config, prepared_state, fetch_options
    )


def new_partition_plan(config, *, canonical_api_base: str | None = None) -> PartitionPlan:
    """Create a durable, bounded plan for the ordinary opening and closing censuses."""
    root_end = config.root_end()
    if (
        canonical_api_base == bcbs_minnesota.API_BASE
        and config.end is None
        and config.census_mode is None
        and config.versioned_search_limit is None
    ):
        # Keep versions updated during the scan inside the census range.
        root_end += datetime.timedelta(days=1)
    if config.start >= root_end:
        raise RuntimeError("provider_directory_last_updated_partition_root_cutoff_invalid")
    return PartitionPlan.create(
        config.start,
        root_end,
        ceiling=config.ceiling,
        minimum_width=config.minimum_width,
        volatile_metadata_paths=config.volatile_metadata_paths,
        boundary_precision=config.boundary_precision,
    )


def partition_checkpoint_windows(
    control_dict: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, tuple[int, ...]]]:
    """Decode persisted geometry and its external pass markers."""
    control_windows = control_dict.get("windows")
    if not isinstance(control_windows, list):
        raise ValueError("checkpoint windows missing")
    planner_windows: list[dict[str, Any]] = []
    completed_passes_by_window: dict[str, tuple[int, ...]] = {}
    for control_window in control_windows:
        if not isinstance(control_window, dict):
            raise ValueError("checkpoint window invalid")
        window_id = control_window.get("window_id")
        completed_passes = control_window.get("completed_passes")
        if (
            not isinstance(window_id, str)
            or not isinstance(completed_passes, list)
            or any(isinstance(pass_number, bool) or pass_number not in (1, 2) for pass_number in completed_passes)
            or completed_passes not in ([], [1], [1, 2])
        ):
            raise ValueError("checkpoint pass markers invalid")
        completed_passes_by_window[window_id] = tuple(completed_passes)
        planner_windows.append(
            {
                **{key: field_value for key, field_value in control_window.items() if key != "completed_passes"},
                "passes": {},
            }
        )
    return planner_windows, completed_passes_by_window


def partition_plan_from_control(
    control_dict: dict[str, Any],
    planner_windows: list[dict[str, Any]],
    config,
) -> PartitionPlan:
    """Restore the planner using the current immutable configuration."""
    config_identity = config.identity()
    return PartitionPlan.from_dict(
        {
            "version": 2,
            "config": {
                "ceiling": config.ceiling,
                "minimum_width_microseconds": config_identity["minimum_width_microseconds"],
                "boundary_precision_microseconds": config_identity["boundary_precision_microseconds"],
                "volatile_metadata_paths": list(config.volatile_metadata_paths),
            },
            "windows": planner_windows,
            "failure": control_dict.get("failure"),
        }
    )


def restore_partition_pass_markers(
    plan: PartitionPlan,
    completed_passes_by_window: dict[str, tuple[int, ...]],
) -> PartitionPlan:
    """Restore committed pass markers without loading fingerprint rows."""
    if len(completed_passes_by_window) != len(plan.windows):
        raise ValueError("checkpoint repeats a window ID")
    for window_id, completed_passes in completed_passes_by_window.items():
        window = plan.windows[window_id]
        if completed_passes and window.count is None:
            raise ValueError("checkpoint completed an uncounted window")
        window.passes = {pass_number: WindowPass({}) for pass_number in completed_passes}
    return plan


@lru_cache(maxsize=16)
def validate_planned_splits(split_ids: tuple[str, ...]) -> tuple[str, ...]:
    """Validate a bounded set of parent-first branches from a previous partition tree."""
    selected_ids = set(split_ids)
    parents = selected_ids | {"root"}
    if len(selected_ids) != len(split_ids) or any(
        re.fullmatch(r"root(?:\.[01]){1,48}", window_id) is None or window_id.rpartition(".")[0] not in parents
        for window_id in split_ids
    ):
        raise ValueError("planned_splits_invalid")
    return tuple(sorted(split_ids, key=lambda window_id: (window_id.count("."), window_id)))


def seed_planned_splits(plan: PartitionPlan, split_ids: tuple[str, ...]) -> None:
    """Use only prior geometry after opening a new exact root census."""
    if plan.status is not PlanStatus.COUNTING:
        return
    for window_id in split_ids:
        plan.observe_count(window_id, CountObservation.split("planned"))
        if plan.failure is not None:
            raise ValueError("planned_splits_outside_partition_bounds")


def verify_planned_splits(plan: PartitionPlan, split_ids: tuple[str, ...]) -> None:
    """Require the exact configured geometry once the new root count justifies splitting."""
    root_count = plan.windows["root"].count
    expected = set(split_ids) if root_count is not None and root_count > plan.config.ceiling else set()
    if {window.window_id for window in plan.windows.values() if window.split_reason == "planned"} != expected:
        raise ValueError("checkpoint planned split geometry mismatch")


@dataclass(frozen=True)
class PartitionConfig:
    ceiling: int
    minimum_width: datetime.timedelta
    volatile_metadata_paths: tuple[str, ...] = ()
    boundary_precision: datetime.timedelta = datetime.timedelta(microseconds=1)

    def __post_init__(self) -> None:
        if isinstance(self.ceiling, bool) or not isinstance(self.ceiling, int) or self.ceiling < 0:
            raise PartitionPlanError("ceiling must be a non-negative integer")
        if not isinstance(self.minimum_width, datetime.timedelta):
            raise PartitionPlanError("minimum_width must be a timedelta")
        if self.minimum_width <= datetime.timedelta(0):
            raise PartitionPlanError("minimum_width must be positive")
        if not isinstance(self.boundary_precision, datetime.timedelta):
            raise PartitionPlanError("boundary_precision must be a timedelta")
        if self.boundary_precision <= datetime.timedelta(0):
            raise PartitionPlanError("boundary_precision must be positive")
        minimum_width_microseconds = _primitives.timedelta_microseconds(self.minimum_width)
        boundary_precision_microseconds = _primitives.timedelta_microseconds(self.boundary_precision)
        if minimum_width_microseconds % boundary_precision_microseconds:
            raise PartitionPlanError("minimum_width must be an exact multiple of boundary_precision")
        normalized_paths = tuple(sorted(set(self.volatile_metadata_paths)))
        for path in normalized_paths:
            _primitives.decode_json_pointer(path)
        object.__setattr__(self, "volatile_metadata_paths", normalized_paths)


@dataclass
class PartitionPlan:
    """Deterministic state machine for planning and verifying two complete passes."""

    config: PartitionConfig
    windows: dict[str, TimeWindow]
    failure: PlanFailure | None = None

    @classmethod
    def create(
        cls,
        start: datetime.datetime,
        end: datetime.datetime,
        *,
        ceiling: int,
        minimum_width: datetime.timedelta,
        volatile_metadata_paths: Iterable[str] = (),
        boundary_precision: datetime.timedelta = datetime.timedelta(microseconds=1),
    ) -> PartitionPlan:
        """Create a new plan containing one uncounted root window."""
        config = PartitionConfig(
            ceiling=ceiling,
            minimum_width=minimum_width,
            volatile_metadata_paths=tuple(volatile_metadata_paths),
            boundary_precision=boundary_precision,
        )
        normalized_start = _primitives.validate_utc_instant(start, "window start")
        normalized_end = _primitives.validate_utc_instant(end, "window end")
        if normalized_start >= normalized_end:
            raise PartitionPlanError("a window must have start < end")
        root = TimeWindow(
            "root",
            _primitives.align_partition_boundary(
                normalized_start,
                config.boundary_precision,
                round_up=False,
            ),
            _primitives.align_partition_boundary(
                normalized_end,
                config.boundary_precision,
                round_up=True,
            ),
        )
        return cls(config=config, windows={root.window_id: root})

    @property
    def status(self) -> PlanStatus:
        """Return the state derived from all windows and any terminal failure."""
        if self.failure is not None:
            return PlanStatus.FAILED
        leaves = self.leaf_windows()
        if any(window.state is WindowState.NEEDS_COUNT for window in leaves):
            return PlanStatus.COUNTING
        if any(len(window.passes) < 2 for window in leaves):
            return PlanStatus.ACQUIRING
        return PlanStatus.SUCCEEDED

    def leaf_windows(self) -> tuple[TimeWindow, ...]:
        """Return active windows in deterministic chronological order."""
        leaves = [window for window in self.windows.values() if window.state is not WindowState.SPLIT]
        return tuple(sorted(leaves, key=lambda window: (window.start, window.end, window.window_id)))

    def pending_count_windows(self) -> tuple[TimeWindow, ...]:
        """Return leaf windows that still require authoritative counts."""
        return tuple(window for window in self.leaf_windows() if window.state is WindowState.NEEDS_COUNT)

    def observe_count(self, window_id: str, observation: CountObservation) -> None:
        """Apply one authoritative count outcome and split when necessary."""
        self._require_active()
        window = self._window(window_id)
        if window.overflow_lower_bound is not None or window.split_reason is not None:
            previous = (
                CountObservation.overflow(window.overflow_lower_bound)
                if window.overflow_lower_bound is not None
                else CountObservation.split(window.split_reason)
            )
            if observation == previous:
                return
            self._fail("contradictory_count", "count observation contradicts the recorded split", window_id)
            return
        if window.count is not None:
            if observation == CountObservation.exact(window.count):
                return
            self._fail("contradictory_count", "count observation contradicts the recorded count", window_id)
            return
        if window.state is not WindowState.NEEDS_COUNT:
            raise PartitionPlanError(f"window {window_id!r} does not accept a count")
        if observation.kind is CountKind.OVERFLOW:
            if observation.count <= self.config.ceiling:
                raise PartitionPlanError("overflow lower bound must exceed the ceiling")
            window.overflow_lower_bound = observation.count
            self._split_or_fail(window)
            return
        if observation.kind is CountKind.SPLIT:
            window.split_reason = observation.detail
            self._split_or_fail(window)
            return
        if observation.kind is CountKind.ERROR:
            detail = observation.detail or "authoritative count request failed"
            self._fail("count_error", detail, window_id)
            return
        if observation.kind is CountKind.UNKNOWN:
            detail = observation.detail or "authoritative count is unknown"
            self._fail("count_unknown", detail, window_id)
            return
        window.count = observation.count
        if observation.count <= self.config.ceiling:
            window.state = WindowState.READY
            overflow_failure = self._overflow_count_failure()
            if overflow_failure is not None:
                self._fail("overflow_count_mismatch", "leaf counts contradict the certified overflow", overflow_failure)
            elif self.status is PlanStatus.ACQUIRING and (count_failure := self._split_count_failure()) is not None:
                self._fail("split_count_mismatch", "leaf counts contradict the exact parent count", count_failure)
            return
        self._split_or_fail(window)

    def split_acquisition_window(self, window_id: str) -> TimeWindow | None:
        """Return the split parent, or None when committed proof prevents splitting."""
        window = self._window(window_id)
        if (
            window.state is not WindowState.READY
            or not window.count
            or window.passes
            or window.end - window.start < 2 * self.config.minimum_width
        ):
            return None
        self._require_active()
        self._split_or_fail(window)
        return window

    def record_pass(
        self,
        window_id: str,
        pass_number: int,
        resources: Iterable[Mapping[str, Any]],
        *,
        complete: bool,
        bounded: bool = False,
    ) -> None:
        """Record and verify one complete, unbounded acquisition pass."""
        self._require_active()
        window = self._window(window_id)
        if window.state is not WindowState.READY:
            raise PartitionPlanError(f"window {window_id!r} is not ready for acquisition")
        if pass_number not in (1, 2):
            raise PartitionPlanError("pass_number must be 1 or 2")
        if bounded:
            self._fail("bounded_window", "window acquisition hit a source bound", window_id)
            return
        if not complete:
            self._fail("incomplete_window", "window acquisition was incomplete", window_id)
            return
        if pass_number == 2 and 1 not in window.passes:
            raise PartitionPlanError("pass 1 must be recorded before pass 2")
        fingerprints = self._fingerprints(resources, window_id)
        if self.failure is not None:
            return
        if len(fingerprints) != window.count:
            self._fail(
                "count_mismatch",
                f"expected {window.count} resources but received {len(fingerprints)}",
                window_id,
            )
            return
        new_pass = WindowPass(fingerprints)
        existing_pass = window.passes.get(pass_number)
        if existing_pass is not None:
            if existing_pass != new_pass:
                self._fail("contradictory_pass", "replayed pass has different content", window_id)
            return
        if self._has_cross_window_duplicate(window_id, pass_number, fingerprints):
            return
        if pass_number == 2 and window.passes[1] != new_pass:
            self._fail("twin_pass_mismatch", "twin pass IDs or fingerprints differ", window_id)
            return
        window.passes[pass_number] = new_pass

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic checkpoint containing JSON-compatible types."""
        ordered_windows = sorted(self.windows.values(), key=lambda entry: entry.window_id)
        windows_list = [self._window_to_dict(window) for window in ordered_windows]
        failure_metadata_dict = None
        if self.failure is not None:
            failure_metadata_dict = {
                "code": self.failure.code,
                "message": self.failure.message,
                "window_id": self.failure.window_id,
            }
        return {
            "version": 2,
            "config": {
                "ceiling": self.config.ceiling,
                "minimum_width_microseconds": _primitives.timedelta_microseconds(self.config.minimum_width),
                "boundary_precision_microseconds": _primitives.timedelta_microseconds(self.config.boundary_precision),
                "volatile_metadata_paths": list(self.config.volatile_metadata_paths),
            },
            "windows": windows_list,
            "failure": failure_metadata_dict,
        }

    def to_json(self) -> str:
        """Return the canonical JSON checkpoint representation."""
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, checkpoint: Mapping[str, Any]) -> PartitionPlan:
        """Restore and validate a plan from a checkpoint mapping."""
        if checkpoint.get("version") != 2:
            raise PartitionPlanError("unsupported partition checkpoint version")
        try:
            config_fields = checkpoint["config"]
            config = PartitionConfig(
                ceiling=config_fields["ceiling"],
                minimum_width=datetime.timedelta(microseconds=config_fields["minimum_width_microseconds"]),
                volatile_metadata_paths=tuple(config_fields["volatile_metadata_paths"]),
                boundary_precision=datetime.timedelta(microseconds=config_fields["boundary_precision_microseconds"]),
            )
            window_fields_list = checkpoint["windows"]
            windows_by_id = {fields["window_id"]: cls._window_from_dict(fields) for fields in window_fields_list}
            if len(windows_by_id) != len(window_fields_list):
                raise PartitionPlanError("checkpoint contains duplicate window IDs")
            failure_fields = checkpoint.get("failure")
            failure = PlanFailure(**failure_fields) if failure_fields else None
        except (KeyError, TypeError, ValueError) as exc:
            raise PartitionPlanError("invalid partition checkpoint") from exc
        plan = cls(config=config, windows=windows_by_id, failure=failure)
        plan._validate_checkpoint()
        return plan

    @classmethod
    def from_json(cls, checkpoint_json: str) -> PartitionPlan:
        """Restore and validate a plan from checkpoint JSON."""
        try:
            checkpoint = json.loads(checkpoint_json)
        except json.JSONDecodeError as exc:
            raise PartitionPlanError("invalid partition checkpoint JSON") from exc
        if not isinstance(checkpoint, dict):
            raise PartitionPlanError("partition checkpoint must be an object")
        return cls.from_dict(checkpoint)

    def _split_or_fail(self, window: TimeWindow) -> None:
        if window.end - window.start < 2 * self.config.minimum_width:
            self._fail(
                "minimum_width_planned_split" if window.split_reason == "planned" else "minimum_width_overflow",
                (
                    "uncounted window cannot be split above the minimum width"
                    if window.split_reason == "planned"
                    else "timed-out count cannot be split above the minimum width"
                    if window.split_reason is not None
                    else "window exceeds the ceiling and cannot be split above the minimum width"
                ),
                window.window_id,
            )
            return
        midpoint = self._split_midpoint(window)
        left = TimeWindow(f"{window.window_id}.0", window.start, midpoint)
        right = TimeWindow(f"{window.window_id}.1", midpoint, window.end)
        window.state = WindowState.SPLIT
        self.windows[left.window_id] = left
        self.windows[right.window_id] = right

    def _split_midpoint(self, window: TimeWindow) -> datetime.datetime:
        duration_microseconds = _primitives.timedelta_microseconds(window.end - window.start)
        boundary_microseconds = _primitives.timedelta_microseconds(self.config.boundary_precision)
        midpoint_units = (duration_microseconds // boundary_microseconds) // 2
        return window.start + datetime.timedelta(microseconds=midpoint_units * boundary_microseconds)

    def _overflow_count_failure(self) -> str | None:
        leaves = self.leaf_windows()
        for window in self.windows.values():
            if window.overflow_lower_bound is None or window.state is not WindowState.SPLIT:
                continue
            descendants = [leaf for leaf in leaves if leaf.window_id.startswith(window.window_id + ".")]
            if descendants and all(leaf.count is not None for leaf in descendants):
                if sum(leaf.count for leaf in descendants) < window.overflow_lower_bound:
                    return window.window_id
        return None

    def _split_count_failure(self) -> str | None:
        descendant_count_by_window: dict[str, int | None] = {}
        for window in sorted(self.windows.values(), key=lambda entry: entry.window_id.count("."), reverse=True):
            if window.state is not WindowState.SPLIT:
                descendant_count_by_window[window.window_id] = window.count
                continue
            left = descendant_count_by_window.get(window.window_id + ".0")
            right = descendant_count_by_window.get(window.window_id + ".1")
            total = None if left is None or right is None else left + right
            if window.count is not None and total is not None and total != window.count:
                return window.window_id
            descendant_count_by_window[window.window_id] = total
        return None

    def _fingerprints(
        self,
        resources: Iterable[Mapping[str, Any]],
        window_id: str,
    ) -> dict[str, str]:
        fingerprints_by_id: dict[str, str] = {}
        for resource in resources:
            if not isinstance(resource, Mapping):
                self._fail("invalid_resource", "FHIR resource must be a mapping", window_id)
                return {}
            resource_id = resource.get("id")
            if not isinstance(resource_id, str) or not resource_id:
                self._fail("invalid_resource_id", "FHIR resource id must be a non-empty string", window_id)
                return {}
            if resource_id in fingerprints_by_id:
                self._fail("duplicate_resource_id", f"duplicate resource id {resource_id!r}", window_id)
                return {}
            fingerprints_by_id[resource_id] = fingerprint_resource(resource, self.config.volatile_metadata_paths)
        return dict(sorted(fingerprints_by_id.items()))

    def _has_cross_window_duplicate(
        self,
        window_id: str,
        pass_number: int,
        fingerprints: Mapping[str, str],
    ) -> bool:
        resource_ids = set(fingerprints)
        for other in self.leaf_windows():
            if other.window_id == window_id or pass_number not in other.passes:
                continue
            duplicates = resource_ids.intersection(other.passes[pass_number].resource_fingerprints)
            if duplicates:
                duplicate = sorted(duplicates)[0]
                self._fail(
                    "duplicate_resource_id",
                    f"resource id {duplicate!r} occurs in multiple windows",
                    window_id,
                )
                return True
        return False

    def _validate_checkpoint(self) -> None:
        if "root" not in self.windows or not self.windows:
            raise PartitionPlanError("checkpoint is missing the root window")
        leaves = self.leaf_windows()
        if not leaves:
            raise PartitionPlanError("checkpoint has no leaf windows")
        root = self.windows["root"]
        if leaves[0].start != root.start or leaves[-1].end != root.end:
            raise PartitionPlanError("checkpoint leaves do not cover the root window")
        for previous, current in zip(leaves, leaves[1:]):
            if previous.end != current.start:
                raise PartitionPlanError("checkpoint leaf windows are not contiguous")
        for window in self.windows.values():
            if window.count is not None:
                CountObservation.exact(window.count)
            self._validate_split_evidence(window)
            if not _primitives.is_partition_boundary_aligned(
                window.start,
                self.config.boundary_precision,
            ) or not _primitives.is_partition_boundary_aligned(
                window.end,
                self.config.boundary_precision,
            ):
                raise PartitionPlanError("checkpoint window is not aligned to boundary_precision")
            pass_numbers = set(window.passes)
            if not pass_numbers.issubset({1, 2}) or (2 in pass_numbers and 1 not in pass_numbers):
                raise PartitionPlanError("checkpoint contains an invalid pass sequence")
            if window.passes and window.state is not WindowState.READY:
                raise PartitionPlanError("only ready checkpoint windows can contain passes")
            if window.state is WindowState.NEEDS_COUNT and window.count is not None:
                raise PartitionPlanError("uncounted checkpoint window has a count")
            if window.state is WindowState.READY and window.count is None:
                raise PartitionPlanError("ready checkpoint window is missing its count")
            if window.state is WindowState.READY and window.count > self.config.ceiling:
                raise PartitionPlanError("ready checkpoint window exceeds the ceiling")
            if any(len(window_pass.resource_fingerprints) != window.count for window_pass in window.passes.values()):
                raise PartitionPlanError("checkpoint pass does not reconcile to its count")
            if 2 in window.passes and window.passes.get(1) != window.passes[2]:
                raise PartitionPlanError("checkpoint twin passes differ")
        self._validate_checkpoint_duplicates(leaves)
        if self._overflow_count_failure() is not None and (
            self.failure is None or self.failure.code != "overflow_count_mismatch"
        ):
            raise PartitionPlanError("checkpoint leaf counts contradict the certified overflow")
        if self._split_count_failure() is not None and (
            self.failure is None or self.failure.code != "split_count_mismatch"
        ):
            raise PartitionPlanError("checkpoint leaf counts contradict the exact parent count")

    def _validate_split_evidence(self, window: TimeWindow) -> None:
        lower_bound, reason = window.overflow_lower_bound, window.split_reason
        if lower_bound is None and reason is None:
            if window.state is WindowState.SPLIT:
                self._validate_split_geometry(window)
            return
        if lower_bound is not None and (
            isinstance(lower_bound, bool)
            or not isinstance(lower_bound, int)
            or lower_bound <= self.config.ceiling
            or reason is not None
        ):
            raise PartitionPlanError("checkpoint overflow lower bound is invalid")
        if reason is not None and reason not in ("timeout", "planned"):
            raise PartitionPlanError("checkpoint uncounted split reason is invalid")
        if window.count is not None or window.passes:
            raise PartitionPlanError("checkpoint uncounted split contains an exact count or passes")
        if (
            self.failure is not None
            and self.failure.code in {"minimum_width_overflow", "minimum_width_planned_split"}
            and self.failure.window_id == window.window_id
            and window.state is WindowState.NEEDS_COUNT
            and window.end - window.start < 2 * self.config.minimum_width
        ):
            return
        self._validate_split_geometry(window)

    def _validate_split_geometry(self, window: TimeWindow) -> None:
        left = self.windows.get(window.window_id + ".0")
        right = self.windows.get(window.window_id + ".1")
        if (
            window.state is not WindowState.SPLIT
            or left is None
            or right is None
            or left.start != window.start
            or left.end != self._split_midpoint(window)
            or left.end != right.start
            or right.end != window.end
        ):
            raise PartitionPlanError("checkpoint split geometry is invalid")

    @staticmethod
    def _validate_checkpoint_duplicates(leaves: tuple[TimeWindow, ...]) -> None:
        for pass_number in (1, 2):
            owner_by_resource_id: dict[str, str] = {}
            for window in leaves:
                if pass_number not in window.passes:
                    continue
                for resource_id in window.passes[pass_number].resource_fingerprints:
                    if resource_id in owner_by_resource_id:
                        raise PartitionPlanError("checkpoint repeats a resource ID across windows")
                    owner_by_resource_id[resource_id] = window.window_id

    def _require_active(self) -> None:
        if self.failure is not None:
            raise PartitionPlanError("partition plan has terminally failed")
        if self.status is PlanStatus.SUCCEEDED:
            raise PartitionPlanError("partition plan has already succeeded")

    def _window(self, window_id: str) -> TimeWindow:
        try:
            return self.windows[window_id]
        except KeyError as exc:
            raise PartitionPlanError(f"unknown window {window_id!r}") from exc

    def _fail(self, code: str, message: str, window_id: str | None) -> None:
        self.failure = PlanFailure(code=code, message=message, window_id=window_id)

    @staticmethod
    def _window_to_dict(window: TimeWindow) -> dict[str, Any]:
        return {
            "window_id": window.window_id,
            "start": _primitives.format_utc_instant(window.start),
            "end": _primitives.format_utc_instant(window.end),
            "state": window.state.value,
            "count": window.count,
            **(
                {"overflow_lower_bound": window.overflow_lower_bound} if window.overflow_lower_bound is not None else {}
            ),
            **({"split_reason": window.split_reason} if window.split_reason is not None else {}),
            "passes": {
                str(pass_number): window_pass.resource_fingerprints
                for pass_number, window_pass in sorted(window.passes.items())
            },
        }

    @staticmethod
    def _window_from_dict(fields: Mapping[str, Any]) -> TimeWindow:
        passes_by_number = {
            int(pass_number): WindowPass(dict(fingerprints)) for pass_number, fingerprints in fields["passes"].items()
        }
        return TimeWindow(
            window_id=fields["window_id"],
            start=parse_utc_instant(fields["start"]),
            end=parse_utc_instant(fields["end"]),
            state=WindowState(fields["state"]),
            count=fields["count"],
            overflow_lower_bound=fields.get("overflow_lower_bound"),
            split_reason=fields.get("split_reason"),
            passes=passes_by_number,
        )


async def mark_partition_page_fetched(state: PartitionFetchState) -> None:
    """Account for a fetched page under the existing partition state lock."""
    async with state.state_lock:
        state.page_count += 1


@dataclass(frozen=True)
class LastUpdatedWindowPage:
    resources: tuple[dict[str, Any], ...]
    next_url: str | None
    error: str | None = None
    transient: bool = False
    retry_not_before: str | None = None


def validate_window_page(directory, source_record, resource_type, request_url, window, response_payload):
    """Check the window contents and resolve a source-bound continuation."""
    api_base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    is_highmark = api_base in highmark.API_BASES
    if not isinstance(response_payload, dict) or (
        not is_highmark and not directory._is_fhir_searchset_bundle(response_payload)
    ):
        return LastUpdatedWindowPage((), None, "non_searchset_bundle")
    page_resources, validation_error = directory._validated_last_updated_page_resources(
        response_payload, resource_type, window
    )
    if validation_error:
        return LastUpdatedWindowPage(page_resources, None, validation_error)
    try:
        next_url = (
            highmark.search_next_link(directory, source_record, response_payload, resource_type, request_url)
            if is_highmark
            else directory._resolved_fhir_next_url(source_record, request_url, directory._next_link(response_payload))
        )
    except ValueError as exc:
        return LastUpdatedWindowPage(page_resources, None, str(exc))
    return LastUpdatedWindowPage(page_resources, next_url)


def _needs_zero_count(directory, start_url: str) -> bool:
    base = directory._resource_or_metadata_parent_base(start_url)
    return base == bcbs_minnesota.API_BASE or base in highmark.CHUNKED_API_BASES


def last_updated_partition_url(
    directory,
    start_url: str,
    window: TimeWindow,
    *,
    page_count: int,
    count_only: bool,
) -> str:
    """Build a deterministic count or data URL before checkpointing it."""
    parsed = urllib.parse.urlsplit(start_url)
    replaced_names = {
        "_count",
        "_lastUpdated",
        "_sort",
        "_summary",
        "_total",
    }
    query_items = [
        (key, query_value)
        for key, query_value in urllib.parse.parse_qsl(
            parsed.query,
            keep_blank_values=True,
        )
        if key not in replaced_names
    ]
    query_items.extend(
        (
            (
                "_lastUpdated",
                f"ge{directory._format_last_updated_partition_instant(window.start)}",
            ),
            (
                "_lastUpdated",
                f"lt{directory._format_last_updated_partition_instant(window.end)}",
            ),
        )
    )
    if count_only:
        query_items.extend(
            (
                ("_summary", "count"),
                ("_total", "accurate"),
            )
        )
        if _needs_zero_count(directory, start_url):
            query_items.append(("_count", "0"))
    else:
        if directory._resource_or_metadata_parent_base(start_url) not in highmark.CHUNKED_API_BASES:
            query_items.append(("_sort", "_lastUpdated,_id"))
        query_items.append(("_count", str(page_count)))
    return urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(query_items, doseq=True)))


def last_updated_partition_unfiltered_count_url(directory, start_url: str) -> str:
    """Build a deterministic count or data URL before checkpointing it."""
    parsed = urllib.parse.urlsplit(start_url)
    replaced_names = {
        "_count",
        "_lastUpdated",
        "_sort",
        "_summary",
        "_total",
    }
    query_items = [
        (key, query_value)
        for key, query_value in urllib.parse.parse_qsl(
            parsed.query,
            keep_blank_values=True,
        )
        if key not in replaced_names
    ]
    query_items.extend(
        (
            ("_summary", "count"),
            ("_total", "accurate"),
        )
    )
    if _needs_zero_count(directory, start_url):
        query_items.append(("_count", "0"))
    return urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(query_items, doseq=True)))
