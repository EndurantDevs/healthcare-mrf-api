# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure planning and verification for ceiling-limited FHIR time partitions."""

from __future__ import annotations

import datetime
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Mapping

from process import provider_directory_time_partition_primitives as _primitives
from process.provider_directory_time_partition_primitives import (
    PartitionPlanError,
    fingerprint_resource,
    parse_utc_instant,
)


class CountKind(str, Enum):
    EXACT = "exact"
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
        elif self.count is not None:
            raise PartitionPlanError("an error or unknown count cannot include a count")

    @classmethod
    def exact(cls, count: int) -> CountObservation:
        """Create a successful authoritative count observation."""
        return cls(CountKind.EXACT, count=count)

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

    def __post_init__(self) -> None:
        self.start = _primitives.validate_utc_instant(self.start, "window start")
        self.end = _primitives.validate_utc_instant(self.end, "window end")
        if self.start >= self.end:
            raise PartitionPlanError("a window must have start < end")

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
        boundary_precision_microseconds = _primitives.timedelta_microseconds(
            self.boundary_precision
        )
        if minimum_width_microseconds % boundary_precision_microseconds:
            raise PartitionPlanError(
                "minimum_width must be an exact multiple of boundary_precision"
            )
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
        if window.count is not None:
            if observation == CountObservation.exact(window.count):
                return
            self._fail("contradictory_count", "count observation contradicts the recorded count", window_id)
            return
        if window.state is not WindowState.NEEDS_COUNT:
            raise PartitionPlanError(f"window {window_id!r} does not accept a count")
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
            return
        self._split_or_fail(window)

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
                "minimum_width_microseconds": _primitives.timedelta_microseconds(
                    self.config.minimum_width
                ),
                "boundary_precision_microseconds": _primitives.timedelta_microseconds(
                    self.config.boundary_precision
                ),
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
                boundary_precision=datetime.timedelta(
                    microseconds=config_fields["boundary_precision_microseconds"]
                ),
            )
            window_fields_list = checkpoint["windows"]
            windows_by_id = {
                fields["window_id"]: cls._window_from_dict(fields)
                for fields in window_fields_list
            }
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
                "minimum_width_overflow",
                "window exceeds the ceiling and cannot be split above the minimum width",
                window.window_id,
            )
            return
        duration_microseconds = _primitives.timedelta_microseconds(
            window.end - window.start
        )
        boundary_microseconds = _primitives.timedelta_microseconds(
            self.config.boundary_precision
        )
        midpoint_units = (duration_microseconds // boundary_microseconds) // 2
        midpoint = window.start + datetime.timedelta(
            microseconds=midpoint_units * boundary_microseconds
        )
        left = TimeWindow(f"{window.window_id}.0", window.start, midpoint)
        right = TimeWindow(f"{window.window_id}.1", midpoint, window.end)
        window.state = WindowState.SPLIT
        self.windows[left.window_id] = left
        self.windows[right.window_id] = right

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
        root = self.windows["root"]
        if leaves[0].start != root.start or leaves[-1].end != root.end:
            raise PartitionPlanError("checkpoint leaves do not cover the root window")
        for previous, current in zip(leaves, leaves[1:]):
            if previous.end != current.start:
                raise PartitionPlanError("checkpoint leaf windows are not contiguous")
        for window in self.windows.values():
            if not _primitives.is_partition_boundary_aligned(
                window.start,
                self.config.boundary_precision,
            ) or not _primitives.is_partition_boundary_aligned(
                window.end,
                self.config.boundary_precision,
            ):
                raise PartitionPlanError(
                    "checkpoint window is not aligned to boundary_precision"
                )
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
            "passes": {
                str(pass_number): window_pass.resource_fingerprints
                for pass_number, window_pass in sorted(window.passes.items())
            },
        }

    @staticmethod
    def _window_from_dict(fields: Mapping[str, Any]) -> TimeWindow:
        passes_by_number = {
            int(pass_number): WindowPass(dict(fingerprints))
            for pass_number, fingerprints in fields["passes"].items()
        }
        return TimeWindow(
            window_id=fields["window_id"],
            start=parse_utc_instant(fields["start"]),
            end=parse_utc_instant(fields["end"]),
            state=WindowState(fields["state"]),
            count=fields["count"],
            passes=passes_by_number,
        )
