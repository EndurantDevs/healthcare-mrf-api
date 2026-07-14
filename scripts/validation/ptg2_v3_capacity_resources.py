#!/usr/bin/env python3
"""Strict parser and deterministic derivation for PTG V3 resource telemetry.

The accepted object is deliberately fixed and bounded.  It contains no host
paths or collector-defined labels, and every accepted identifier is a redacted
SHA-256 digest.  This module does not import the capacity gate so collectors and
the gate can both depend on it without creating a cycle.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_CEILING
from typing import Any, Mapping, NamedTuple, Sequence


INTERVAL_SECONDS = 5
MIN_CONTENTION_SECONDS = 30 * 60
MAX_CONTENTION_SECONDS = 120 * 60
MAX_SCRATCH_CLEANUP_EVENTS = 256
MAX_CHECKPOINT_EVENTS = 64
MAX_AUTOVACUUM_EVENTS = 512
MAX_STORAGE_DELTAS = 256
MAX_PREEXISTING_LAYOUTS = 256
GC_CYCLE_COUNT = 24
GC_CYCLE_SECONDS = 60 * 60
MONTH_HOURS = Decimal(30 * 24)

# Counts are exclusive buckets.  The first bucket is exactly zero wait; the
# overflow count is strictly greater than the last finite upper bound.
POOL_WAIT_BUCKET_UPPER_BOUNDS_MS = (
    Decimal("0"),
    Decimal("1"),
    Decimal("5"),
    Decimal("10"),
    Decimal("25"),
    Decimal("50"),
    Decimal("100"),
)

MAX_BYTES = (1 << 63) - 1
MAX_COUNT = (1 << 31) - 1
MAX_CONNECTIONS = 1_000_000
MAX_AUTOVACUUM_WORKERS = 4_096
MAX_RETENTION_MONTHS = Decimal("120")
MAX_CLEARANCE_HOURS = Decimal(10 * 365 * 24)
MAX_WAIT_MS = Decimal(60 * 60 * 1_000)
MAX_PENDING_SECONDS = 365 * 24 * 60 * 60

_DIGEST_RE = re.compile(r"[0-9a-f]{64}\Z")
_UTC_SECONDS_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\Z")
_UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_VALIDATED_TELEMETRY_TOKEN = object()


class ResourceTelemetryError(ValueError):
    """A stable validation error that never includes input-derived text."""

    def __init__(self, code: str, field: str):
        super().__init__(code)
        self.code = code
        self.field = field


@dataclass(frozen=True)
class CategoryCounters:
    epoch_sha256: str
    total_bytes: int
    import_bytes: int
    api_bytes: int
    other_bytes: int


@dataclass(frozen=True)
class ResourceConfig:
    observed_at: datetime
    config_revision_sha256: str
    max_connections: int
    reserved_connections: int
    autovacuum_max_workers: int
    write_enforced_limit_bytes_per_second: Decimal
    wal_enforced_limit_bytes_per_second: Decimal
    scratch_capacity_bytes: int
    scratch_reserve_bytes: int
    temp_capacity_bytes: int
    temp_reserve_bytes: int
    storage_capacity_bytes: int
    storage_reserve_bytes: int
    physical_retention_months: Decimal
    logical_retention_months: Decimal
    required_connection_headroom: int
    required_autovacuum_headroom: int
    gc_max_backlog_bytes: int
    gc_max_clearance_hours: Decimal


@dataclass(frozen=True)
class ResourceBaseline:
    observed_at: datetime
    scratch_used_bytes: int
    scratch_available_bytes: int
    temp_used_bytes: int
    temp_available_bytes: int
    storage_used_bytes: int
    storage_available_bytes: int
    physical_layout_bytes: int
    physical_layout_count: int
    logical_mapping_bytes: int
    logical_mapping_count: int
    write_counters: CategoryCounters
    wal_counters: CategoryCounters


@dataclass(frozen=True)
class ConnectionPeaks:
    api_connections: int
    import_connections: int
    other_connections: int

    @property
    def total(self) -> int:
        """Return the aggregate API, import, and other connection peak."""
        return (
            self.api_connections
            + self.import_connections
            + self.other_connections
        )


@dataclass(frozen=True)
class PoolWaitHistogram:
    bucket_counts: tuple[int, ...]
    overflow_count: int
    max_ms: Decimal
    timeout_errors: int

    @property
    def observations(self) -> int:
        """Return all histogram observations, including the overflow bucket."""
        return sum(self.bucket_counts) + self.overflow_count


@dataclass(frozen=True)
class AutovacuumIntervalPeak:
    workers: int
    oldest_pending_seconds: int


@dataclass(frozen=True)
class ResourceInterval:
    started_at: datetime
    ended_at: datetime
    config_revision_sha256: str
    scratch_peak_used_bytes: int
    scratch_min_available_bytes: int
    temp_peak_used_bytes: int
    temp_min_available_bytes: int
    write_counters: CategoryCounters
    wal_counters: CategoryCounters
    connections: ConnectionPeaks
    pool_wait: PoolWaitHistogram
    autovacuum: AutovacuumIntervalPeak


@dataclass(frozen=True)
class ScratchCleanupEvent:
    import_id_sha256: str
    started_at: datetime
    ended_at: datetime
    outcome: str


@dataclass(frozen=True)
class CheckpointEvent:
    started_at: datetime
    ended_at: datetime
    trigger: str
    write_seconds: Decimal
    sync_seconds: Decimal


@dataclass(frozen=True)
class AutovacuumEvent:
    relation_id_sha256: str
    started_at: datetime
    ended_at: datetime
    outcome: str


@dataclass(frozen=True)
class StorageEndpoint:
    measured_at: datetime
    used_bytes: int
    available_bytes: int
    physical_layout_bytes: int
    physical_layout_count: int
    logical_mapping_bytes: int
    logical_mapping_count: int


@dataclass(frozen=True)
class PreexistingLayout:
    physical_layout_id_sha256: str


@dataclass(frozen=True)
class StorageDelta:
    import_id_sha256: str
    kind: str
    physical_layout_id_sha256: str
    physical_net_new_bytes: int
    logical_net_new_bytes: int


@dataclass(frozen=True)
class EventCounterSnapshot:
    observed_at: datetime
    checkpoint_epoch_sha256: str
    checkpoint_completed: int
    checkpoint_requested: int
    autovacuum_epoch_sha256: str
    autovacuum_completed: int
    autovacuum_cancelled: int


@dataclass(frozen=True)
class GcCycle:
    started_at: datetime
    ended_at: datetime
    config_revision_sha256: str
    executed: bool
    overlap_detected: bool
    reference_recheck_failures: int
    starting_backlog_bytes: int
    starting_backlog_layouts: int
    newly_eligible_bytes: int
    eligible_layouts: int
    deleted_bytes: int
    deleted_layouts: int
    ending_backlog_bytes: int
    ending_backlog_layouts: int


@dataclass(frozen=True)
class ResourceTelemetry:
    _validation_token: object = field(repr=False, compare=False)
    contention_run_id: str
    contention_started_at: datetime
    contention_ended_at: datetime
    config_start: ResourceConfig
    config_end: ResourceConfig
    baseline: ResourceBaseline
    intervals: tuple[ResourceInterval, ...]
    scratch_cleanup_events: tuple[ScratchCleanupEvent, ...]
    checkpoint_events: tuple[CheckpointEvent, ...]
    autovacuum_events: tuple[AutovacuumEvent, ...]
    event_counters_start: EventCounterSnapshot
    event_counters_end: EventCounterSnapshot
    storage_endpoint: StorageEndpoint
    preexisting_layouts: tuple[PreexistingLayout, ...]
    storage_deltas: tuple[StorageDelta, ...]
    gc_cycles: tuple[GcCycle, ...]

    def __post_init__(self) -> None:
        if self._validation_token is not _VALIDATED_TELEMETRY_TOKEN:
            raise ResourceTelemetryError("unvalidated_telemetry", "telemetry")


@dataclass(frozen=True)
class ScratchSummary:
    capacity_bytes: int
    baseline_used_bytes: int
    reserve_bytes: int
    measured_peak_used_bytes: int
    measured_peak_incremental_bytes: int
    cleanup_cycles: int
    cleanup_failures: int
    cleanup_imports_observed: int


@dataclass(frozen=True)
class CounterRateSummary:
    total_bytes_per_second: Decimal
    import_bytes_per_second: Decimal
    api_bytes_per_second: Decimal
    other_bytes_per_second: Decimal
    sustainable_bytes_per_second: Decimal
    peak_interval_total_bytes_per_second: Decimal
    peak_interval_import_bytes_per_second: Decimal
    peak_interval_api_bytes_per_second: Decimal
    peak_interval_other_bytes_per_second: Decimal


@dataclass(frozen=True)
class PostgresConnectionSummary:
    max_connections: int
    reserved_connections: int
    peak_api_connections: int
    peak_import_connections: int
    peak_other_connections: int
    peak_connections: int
    peak_interval_started_at: datetime
    required_headroom_connections: int


@dataclass(frozen=True)
class PostgresPoolWaitSummary:
    observations: int
    waited_acquisitions: int
    p95_ms: Decimal
    max_ms: Decimal
    timeout_errors: int


@dataclass(frozen=True)
class PostgresCheckpointSummary:
    sample_seconds: Decimal
    completed: int
    requested: int
    write_seconds: Decimal
    sync_seconds: Decimal


@dataclass(frozen=True)
class PostgresTempSummary:
    capacity_bytes: int
    baseline_used_bytes: int
    reserve_bytes: int
    measured_peak_used_bytes: int
    measured_peak_incremental_bytes: int


@dataclass(frozen=True)
class PostgresAutovacuumSummary:
    sample_seconds: Decimal
    max_workers: int
    peak_workers: int
    required_headroom_workers: int
    completed_cycles: int
    cancelled_cycles: int
    oldest_pending_minutes: Decimal


@dataclass(frozen=True)
class PostgresSummary:
    connections: PostgresConnectionSummary
    write: CounterRateSummary
    wal: CounterRateSummary
    pool_wait: PostgresPoolWaitSummary
    checkpoint: PostgresCheckpointSummary
    temp: PostgresTempSummary
    autovacuum: PostgresAutovacuumSummary


@dataclass(frozen=True)
class StorageSummary:
    physical_unique_builds_observed: int
    physical_net_new_bytes_observed: int
    physical_max_bytes_per_unique_build: int
    logical_imports_observed: int
    logical_net_new_bytes_observed: int
    logical_max_bytes_per_import: int
    current_used_bytes: int
    capacity_bytes: int
    reserve_bytes: int
    physical_retention_months: Decimal
    logical_retention_months: Decimal


@dataclass(frozen=True)
class GcSummary:
    window_hours: Decimal
    cycles_observed: int
    executed_cycles: int
    overlap_count: int
    reference_recheck_failures: int
    starting_backlog_bytes: int
    starting_backlog_layouts: int
    newly_eligible_bytes: int
    deleted_bytes: int
    ending_backlog_bytes: int
    ending_backlog_layouts: int
    observed_peak_backlog_bytes: int
    max_backlog_bytes: int
    max_clearance_hours: Decimal
    eligible_layouts: int
    deleted_layouts: int
    eligible_bytes_per_hour: Decimal
    deleted_bytes_per_hour: Decimal
    net_drain_bytes_per_hour: Decimal
    headroom_fraction: Decimal | None
    clearance_hours: Decimal | None
    projected_30_day_backlog_bytes: int


@dataclass(frozen=True)
class ResourceSummaries:
    contention_run_id: str
    contention_started_at: datetime
    contention_ended_at: datetime
    sample_seconds: Decimal
    scratch: ScratchSummary
    postgresql: PostgresSummary
    storage: StorageSummary
    gc: GcSummary


class ParsedResourceTelemetry(NamedTuple):
    """One atomic result produced only after validation and derivation succeed."""

    telemetry: ResourceTelemetry
    summaries: ResourceSummaries


class _ResourceTelemetryContext(NamedTuple):
    telemetry_object: Mapping[str, Any]
    contention_run_id: str
    contention_started_at: datetime
    contention_ended_at: datetime
    duration_seconds: int
    config_start: ResourceConfig
    config_end: ResourceConfig
    baseline: ResourceBaseline


class _StorageEvidence(NamedTuple):
    endpoint: StorageEndpoint
    preexisting_layouts: tuple[PreexistingLayout, ...]
    storage_deltas: tuple[StorageDelta, ...]


class _EventEvidence(NamedTuple):
    cleanup_events: tuple[ScratchCleanupEvent, ...]
    checkpoint_events: tuple[CheckpointEvent, ...]
    autovacuum_events: tuple[AutovacuumEvent, ...]
    counters_start: EventCounterSnapshot
    counters_end: EventCounterSnapshot


class _GcDerivedMetrics(NamedTuple):
    eligible_bytes: int
    deleted_bytes: int
    eligible_layouts: int
    deleted_layouts: int
    eligible_rate: Decimal
    deleted_rate: Decimal
    net_drain: Decimal
    clearance_hours: Decimal | None
    headroom_fraction: Decimal | None
    projected_backlog: int


_ROOT_FIELDS = (
    "contention_run_id",
    "contention_started_at",
    "contention_ended_at",
    "config_start",
    "config_end",
    "baseline",
    "intervals",
    "scratch_cleanup_events",
    "checkpoint_events",
    "autovacuum_events",
    "event_counters_start",
    "event_counters_end",
    "storage_endpoint",
    "preexisting_layouts",
    "storage_deltas",
    "gc_cycles",
)
_CONFIG_FIELDS = (
    "observed_at",
    "config_revision_sha256",
    "max_connections",
    "reserved_connections",
    "autovacuum_max_workers",
    "write_enforced_limit_bytes_per_second",
    "wal_enforced_limit_bytes_per_second",
    "scratch_capacity_bytes",
    "scratch_reserve_bytes",
    "temp_capacity_bytes",
    "temp_reserve_bytes",
    "storage_capacity_bytes",
    "storage_reserve_bytes",
    "physical_retention_months",
    "logical_retention_months",
    "required_connection_headroom",
    "required_autovacuum_headroom",
    "gc_max_backlog_bytes",
    "gc_max_clearance_hours",
)
_CONFIG_STABLE_FIELDS = _CONFIG_FIELDS[1:]
_BASELINE_FIELDS = (
    "observed_at",
    "scratch_used_bytes",
    "scratch_available_bytes",
    "temp_used_bytes",
    "temp_available_bytes",
    "storage_used_bytes",
    "storage_available_bytes",
    "physical_layout_bytes",
    "physical_layout_count",
    "logical_mapping_bytes",
    "logical_mapping_count",
    "write_counters",
    "wal_counters",
)
_COUNTER_FIELDS = (
    "epoch_sha256",
    "total_bytes",
    "import_bytes",
    "api_bytes",
    "other_bytes",
)
_COUNTER_CATEGORY_FIELDS = ("import_bytes", "api_bytes", "other_bytes")
_INTERVAL_FIELDS = (
    "started_at",
    "ended_at",
    "config_revision_sha256",
    "scratch_peak_used_bytes",
    "scratch_min_available_bytes",
    "temp_peak_used_bytes",
    "temp_min_available_bytes",
    "write_counters",
    "wal_counters",
    "connections",
    "pool_wait",
    "autovacuum",
)


def _field(parent: str, name: str) -> str:
    return f"{parent}.{name}" if parent else name


def _object(value: Any, path: str, fields: Sequence[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ResourceTelemetryError("invalid_type", path or "root")
    for name in fields:
        if name not in value:
            raise ResourceTelemetryError("missing_field", _field(path, name))
    allowed_field_names = set(fields)
    if any(name not in allowed_field_names for name in value):
        raise ResourceTelemetryError("unexpected_field", path or "root")
    return value


def _array(
    value: Any,
    path: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
    exact: int | None = None,
) -> Sequence[Any]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, (list, tuple)
    ):
        raise ResourceTelemetryError("invalid_type", path)
    if exact is not None and len(value) != exact:
        raise ResourceTelemetryError("invalid_count", path)
    if minimum is not None and len(value) < minimum:
        raise ResourceTelemetryError("invalid_count", path)
    if maximum is not None and len(value) > maximum:
        raise ResourceTelemetryError("sample_limit", path)
    return value


def _integer_value(
    value: Any,
    field: str,
    *,
    minimum: int = 0,
    maximum: int = MAX_COUNT,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ResourceTelemetryError("invalid_type", field)
    if value < minimum or value > maximum:
        raise ResourceTelemetryError("invalid_value", field)
    return value


def _integer(
    obj: Mapping[str, Any],
    name: str,
    path: str,
    *,
    minimum: int = 0,
    maximum: int = MAX_COUNT,
) -> int:
    return _integer_value(
        obj[name], _field(path, name), minimum=minimum, maximum=maximum
    )


def _decimal_value(
    value: Any,
    field: str,
    *,
    minimum: Decimal = Decimal(0),
    maximum: Decimal,
    minimum_inclusive: bool = True,
) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ResourceTelemetryError("invalid_type", field)
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except Exception:
        raise ResourceTelemetryError("invalid_value", field)
    if not result.is_finite():
        raise ResourceTelemetryError("invalid_value", field)
    below = result < minimum if minimum_inclusive else result <= minimum
    if below or result > maximum:
        raise ResourceTelemetryError("invalid_value", field)
    return result


def _decimal(
    obj: Mapping[str, Any],
    name: str,
    path: str,
    *,
    minimum: Decimal = Decimal(0),
    maximum: Decimal,
    minimum_inclusive: bool = True,
) -> Decimal:
    return _decimal_value(
        obj[name],
        _field(path, name),
        minimum=minimum,
        maximum=maximum,
        minimum_inclusive=minimum_inclusive,
    )


def _is_boolean(obj: Mapping[str, Any], name: str, path: str) -> bool:
    value = obj[name]
    if not isinstance(value, bool):
        raise ResourceTelemetryError("invalid_type", _field(path, name))
    return value


def _timestamp(obj: Mapping[str, Any], name: str, path: str) -> datetime:
    value = obj[name]
    field = _field(path, name)
    if not isinstance(value, str) or _UTC_SECONDS_RE.fullmatch(value) is None:
        raise ResourceTelemetryError("invalid_timestamp", field)
    try:
        return datetime.strptime(value, _UTC_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        raise ResourceTelemetryError("invalid_timestamp", field)


def _digest(obj: Mapping[str, Any], name: str, path: str) -> str:
    value = obj[name]
    field = _field(path, name)
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise ResourceTelemetryError("invalid_digest", field)
    return value


def _enum(
    obj: Mapping[str, Any],
    name: str,
    path: str,
    allowed: Sequence[str],
) -> str:
    value = obj[name]
    field = _field(path, name)
    if not isinstance(value, str):
        raise ResourceTelemetryError("invalid_type", field)
    if value not in allowed:
        raise ResourceTelemetryError("invalid_enum", field)
    return value


def _bounded_sum(values: Sequence[int], field: str) -> int:
    total = sum(values)
    if total > MAX_BYTES:
        raise ResourceTelemetryError("numeric_overflow", field)
    return total


def _bounded_count_sum(values: Sequence[int], field: str) -> int:
    total = sum(values)
    if total > MAX_COUNT:
        raise ResourceTelemetryError("numeric_overflow", field)
    return total


def _parse_counters(raw_counters: Any, path: str) -> CategoryCounters:
    counters_object = _object(raw_counters, path, _COUNTER_FIELDS)
    category_values = tuple(
        _integer(counters_object, name, path, maximum=MAX_BYTES)
        for name in _COUNTER_CATEGORY_FIELDS
    )
    category_total = _bounded_sum(category_values, f"{path}.total_bytes")
    total = _integer(counters_object, "total_bytes", path, maximum=MAX_BYTES)
    if total != category_total:
        raise ResourceTelemetryError("counter_total_mismatch", f"{path}.total_bytes")
    return CategoryCounters(
        epoch_sha256=_digest(counters_object, "epoch_sha256", path),
        total_bytes=total,
        import_bytes=category_values[0],
        api_bytes=category_values[1],
        other_bytes=category_values[2],
    )


def _connection_config_fields(
    config_object: Mapping[str, Any], path: str
) -> dict[str, int]:
    max_connections = _integer(
        config_object, "max_connections", path, minimum=1, maximum=MAX_CONNECTIONS
    )
    reserved_connections = _integer(
        config_object,
        "reserved_connections",
        path,
        maximum=max_connections - 1,
    )
    max_workers = _integer(
        config_object,
        "autovacuum_max_workers",
        path,
        minimum=1,
        maximum=MAX_AUTOVACUUM_WORKERS,
    )
    required_connections = _integer(
        config_object,
        "required_connection_headroom",
        path,
        minimum=1,
        maximum=max_connections - reserved_connections,
    )
    required_workers = _integer(
        config_object,
        "required_autovacuum_headroom",
        path,
        minimum=1,
        maximum=max_workers,
    )
    return {
        "max_connections": max_connections,
        "reserved_connections": reserved_connections,
        "autovacuum_max_workers": max_workers,
        "required_connection_headroom": required_connections,
        "required_autovacuum_headroom": required_workers,
    }


def _capacity_config_fields(
    config_object: Mapping[str, Any], path: str
) -> dict[str, int]:
    scratch_capacity = _integer(
        config_object, "scratch_capacity_bytes", path, minimum=1, maximum=MAX_BYTES
    )
    temp_capacity = _integer(
        config_object, "temp_capacity_bytes", path, minimum=1, maximum=MAX_BYTES
    )
    storage_capacity = _integer(
        config_object, "storage_capacity_bytes", path, minimum=1, maximum=MAX_BYTES
    )
    return {
        "scratch_capacity_bytes": scratch_capacity,
        "scratch_reserve_bytes": _integer(
            config_object, "scratch_reserve_bytes", path, maximum=scratch_capacity - 1
        ),
        "temp_capacity_bytes": temp_capacity,
        "temp_reserve_bytes": _integer(
            config_object, "temp_reserve_bytes", path, maximum=temp_capacity - 1
        ),
        "storage_capacity_bytes": storage_capacity,
        "storage_reserve_bytes": _integer(
            config_object,
            "storage_reserve_bytes",
            path,
            maximum=storage_capacity - 1,
        ),
    }


def _policy_config_fields(
    config_object: Mapping[str, Any], path: str
) -> dict[str, Decimal | int]:
    return {
        "write_enforced_limit_bytes_per_second": _decimal(
            config_object,
            "write_enforced_limit_bytes_per_second",
            path,
            maximum=Decimal(MAX_BYTES),
            minimum_inclusive=False,
        ),
        "wal_enforced_limit_bytes_per_second": _decimal(
            config_object,
            "wal_enforced_limit_bytes_per_second",
            path,
            maximum=Decimal(MAX_BYTES),
            minimum_inclusive=False,
        ),
        "physical_retention_months": _decimal(
            config_object,
            "physical_retention_months",
            path,
            maximum=MAX_RETENTION_MONTHS,
            minimum_inclusive=False,
        ),
        "logical_retention_months": _decimal(
            config_object,
            "logical_retention_months",
            path,
            maximum=MAX_RETENTION_MONTHS,
            minimum_inclusive=False,
        ),
        "gc_max_backlog_bytes": _integer(
            config_object, "gc_max_backlog_bytes", path, maximum=MAX_BYTES
        ),
        "gc_max_clearance_hours": _decimal(
            config_object,
            "gc_max_clearance_hours",
            path,
            maximum=MAX_CLEARANCE_HOURS,
            minimum_inclusive=False,
        ),
    }


def _parse_config(raw_config: Any, path: str) -> ResourceConfig:
    """Validate a resource configuration and return its immutable form."""

    config_object = _object(raw_config, path, _CONFIG_FIELDS)
    return ResourceConfig(
        observed_at=_timestamp(config_object, "observed_at", path),
        config_revision_sha256=_digest(config_object, "config_revision_sha256", path),
        **_connection_config_fields(config_object, path),
        **_capacity_config_fields(config_object, path),
        **_policy_config_fields(config_object, path),
    )


def _validate_usage(
    used: int,
    available: int,
    capacity: int,
    used_field: str,
    available_field: str,
) -> None:
    if used > capacity:
        raise ResourceTelemetryError("inconsistent_capacity", used_field)
    if available > capacity or used + available > capacity:
        raise ResourceTelemetryError("inconsistent_capacity", available_field)


def _validate_storage_domains(
    *,
    physical_bytes: int,
    physical_count: int,
    logical_bytes: int,
    logical_count: int,
    used: int,
    available: int,
    capacity: int,
    path: str,
) -> None:
    if (physical_bytes == 0) != (physical_count == 0):
        raise ResourceTelemetryError(
            "inconsistent_storage_domain", f"{path}.physical_layout_count"
        )
    if (logical_bytes == 0) != (logical_count == 0):
        raise ResourceTelemetryError(
            "inconsistent_storage_domain", f"{path}.logical_mapping_count"
        )
    domain_bytes = _bounded_sum(
        (physical_bytes, logical_bytes), f"{path}.logical_mapping_bytes"
    )
    if domain_bytes > _conservative_used(used, available, capacity):
        raise ResourceTelemetryError(
            "inconsistent_storage_domain", f"{path}.logical_mapping_bytes"
        )


def _baseline_fields(
    baseline_object: Mapping[str, Any], path: str
) -> dict[str, int | CategoryCounters]:
    return {
        "scratch_used_bytes": _integer(
            baseline_object, "scratch_used_bytes", path, maximum=MAX_BYTES
        ),
        "scratch_available_bytes": _integer(
            baseline_object, "scratch_available_bytes", path, maximum=MAX_BYTES
        ),
        "temp_used_bytes": _integer(
            baseline_object, "temp_used_bytes", path, maximum=MAX_BYTES
        ),
        "temp_available_bytes": _integer(
            baseline_object, "temp_available_bytes", path, maximum=MAX_BYTES
        ),
        "storage_used_bytes": _integer(
            baseline_object, "storage_used_bytes", path, maximum=MAX_BYTES
        ),
        "storage_available_bytes": _integer(
            baseline_object, "storage_available_bytes", path, maximum=MAX_BYTES
        ),
        "physical_layout_bytes": _integer(
            baseline_object, "physical_layout_bytes", path, maximum=MAX_BYTES
        ),
        "physical_layout_count": _integer(
            baseline_object, "physical_layout_count", path, maximum=MAX_COUNT
        ),
        "logical_mapping_bytes": _integer(
            baseline_object, "logical_mapping_bytes", path, maximum=MAX_BYTES
        ),
        "logical_mapping_count": _integer(
            baseline_object, "logical_mapping_count", path, maximum=MAX_COUNT
        ),
        "write_counters": _parse_counters(
            baseline_object["write_counters"], f"{path}.write_counters"
        ),
        "wal_counters": _parse_counters(
            baseline_object["wal_counters"], f"{path}.wal_counters"
        ),
    }


def _parse_baseline(
    raw_baseline: Any, path: str, config: ResourceConfig
) -> ResourceBaseline:
    """Validate a baseline resource snapshot against its configuration."""

    baseline_object = _object(raw_baseline, path, _BASELINE_FIELDS)
    baseline = ResourceBaseline(
        observed_at=_timestamp(baseline_object, "observed_at", path),
        **_baseline_fields(baseline_object, path),
    )
    _validate_usage(
        baseline.scratch_used_bytes,
        baseline.scratch_available_bytes,
        config.scratch_capacity_bytes,
        f"{path}.scratch_used_bytes",
        f"{path}.scratch_available_bytes",
    )
    _validate_usage(
        baseline.temp_used_bytes,
        baseline.temp_available_bytes,
        config.temp_capacity_bytes,
        f"{path}.temp_used_bytes",
        f"{path}.temp_available_bytes",
    )
    _validate_usage(
        baseline.storage_used_bytes,
        baseline.storage_available_bytes,
        config.storage_capacity_bytes,
        f"{path}.storage_used_bytes",
        f"{path}.storage_available_bytes",
    )
    _validate_storage_domains(
        physical_bytes=baseline.physical_layout_bytes,
        physical_count=baseline.physical_layout_count,
        logical_bytes=baseline.logical_mapping_bytes,
        logical_count=baseline.logical_mapping_count,
        used=baseline.storage_used_bytes,
        available=baseline.storage_available_bytes,
        capacity=config.storage_capacity_bytes,
        path=path,
    )
    return baseline


def _parse_pool_wait(raw_pool_wait: Any, path: str) -> PoolWaitHistogram:
    pool_wait_object = _object(
        raw_pool_wait,
        path,
        ("bucket_counts", "overflow_count", "max_ms", "timeout_errors"),
    )
    raw_counts = _array(
        pool_wait_object["bucket_counts"],
        f"{path}.bucket_counts",
        exact=len(POOL_WAIT_BUCKET_UPPER_BOUNDS_MS),
    )
    counts = tuple(
        _integer_value(
            bucket_count, f"{path}.bucket_counts[{index}]", maximum=MAX_COUNT
        )
        for index, bucket_count in enumerate(raw_counts)
    )
    overflow = _integer(pool_wait_object, "overflow_count", path, maximum=MAX_COUNT)
    observations = sum(counts) + overflow
    if observations > MAX_COUNT:
        raise ResourceTelemetryError("numeric_overflow", path)
    max_ms = _decimal(pool_wait_object, "max_ms", path, maximum=MAX_WAIT_MS)
    timeouts = _integer(pool_wait_object, "timeout_errors", path, maximum=MAX_COUNT)
    if timeouts > observations:
        raise ResourceTelemetryError("inconsistent_histogram", f"{path}.timeout_errors")
    if observations == 0:
        if max_ms != 0:
            raise ResourceTelemetryError("inconsistent_histogram", f"{path}.max_ms")
    elif overflow:
        if max_ms <= POOL_WAIT_BUCKET_UPPER_BOUNDS_MS[-1]:
            raise ResourceTelemetryError("inconsistent_histogram", f"{path}.max_ms")
    else:
        highest = max(index for index, count in enumerate(counts) if count)
        upper = POOL_WAIT_BUCKET_UPPER_BOUNDS_MS[highest]
        lower = (
            Decimal(-1)
            if highest == 0
            else POOL_WAIT_BUCKET_UPPER_BOUNDS_MS[highest - 1]
        )
        if max_ms > upper or max_ms <= lower:
            raise ResourceTelemetryError("inconsistent_histogram", f"{path}.max_ms")
    return PoolWaitHistogram(counts, overflow, max_ms, timeouts)


def _parse_interval_connections(
    interval_object: Mapping[str, Any], path: str, config: ResourceConfig
) -> ConnectionPeaks:
    connection_path = f"{path}.connections"
    connection_obj = _object(
        interval_object["connections"],
        connection_path,
        ("api_connections", "import_connections", "other_connections"),
    )
    connections = ConnectionPeaks(
        api_connections=_integer(
            connection_obj,
            "api_connections",
            connection_path,
            maximum=config.max_connections,
        ),
        import_connections=_integer(
            connection_obj,
            "import_connections",
            connection_path,
            maximum=config.max_connections,
        ),
        other_connections=_integer(
            connection_obj,
            "other_connections",
            connection_path,
            maximum=config.max_connections,
        ),
    )
    if connections.total > config.max_connections:
        raise ResourceTelemetryError("inconsistent_connections", connection_path)
    return connections


def _parse_interval_autovacuum(
    interval_object: Mapping[str, Any], path: str, config: ResourceConfig
) -> AutovacuumIntervalPeak:
    autovacuum_path = f"{path}.autovacuum"
    autovacuum_object = _object(
        interval_object["autovacuum"],
        autovacuum_path,
        ("workers", "oldest_pending_seconds"),
    )
    return AutovacuumIntervalPeak(
        workers=_integer(
            autovacuum_object,
            "workers",
            autovacuum_path,
            maximum=config.autovacuum_max_workers,
        ),
        oldest_pending_seconds=_integer(
            autovacuum_object,
            "oldest_pending_seconds",
            autovacuum_path,
            maximum=MAX_PENDING_SECONDS,
        ),
    )


def _parse_interval(
    raw_interval: Any, path: str, config: ResourceConfig
) -> ResourceInterval:
    """Validate one fixed telemetry interval against its resource limits."""

    interval_object = _object(raw_interval, path, _INTERVAL_FIELDS)
    interval = ResourceInterval(
        started_at=_timestamp(interval_object, "started_at", path),
        ended_at=_timestamp(interval_object, "ended_at", path),
        config_revision_sha256=_digest(
            interval_object, "config_revision_sha256", path
        ),
        scratch_peak_used_bytes=_integer(
            interval_object, "scratch_peak_used_bytes", path, maximum=MAX_BYTES
        ),
        scratch_min_available_bytes=_integer(
            interval_object, "scratch_min_available_bytes", path, maximum=MAX_BYTES
        ),
        temp_peak_used_bytes=_integer(
            interval_object, "temp_peak_used_bytes", path, maximum=MAX_BYTES
        ),
        temp_min_available_bytes=_integer(
            interval_object, "temp_min_available_bytes", path, maximum=MAX_BYTES
        ),
        write_counters=_parse_counters(
            interval_object["write_counters"], f"{path}.write_counters"
        ),
        wal_counters=_parse_counters(
            interval_object["wal_counters"], f"{path}.wal_counters"
        ),
        connections=_parse_interval_connections(interval_object, path, config),
        pool_wait=_parse_pool_wait(interval_object["pool_wait"], f"{path}.pool_wait"),
        autovacuum=_parse_interval_autovacuum(interval_object, path, config),
    )
    _validate_usage(
        interval.scratch_peak_used_bytes,
        interval.scratch_min_available_bytes,
        config.scratch_capacity_bytes,
        f"{path}.scratch_peak_used_bytes",
        f"{path}.scratch_min_available_bytes",
    )
    _validate_usage(
        interval.temp_peak_used_bytes,
        interval.temp_min_available_bytes,
        config.temp_capacity_bytes,
        f"{path}.temp_peak_used_bytes",
        f"{path}.temp_min_available_bytes",
    )
    return interval


def _counter_values(counters: CategoryCounters) -> tuple[int, int, int]:
    return counters.import_bytes, counters.api_bytes, counters.other_bytes


def _validate_counter_progression(
    previous: CategoryCounters,
    current: CategoryCounters,
    path: str,
    *,
    elapsed_seconds: int,
    enforced_limit: Decimal,
) -> None:
    if current.epoch_sha256 != previous.epoch_sha256:
        raise ResourceTelemetryError(
            "counter_epoch_changed", f"{path}.epoch_sha256"
        )
    for name, before, after in zip(
        ("total_bytes", *_COUNTER_CATEGORY_FIELDS),
        (previous.total_bytes, *_counter_values(previous)),
        (current.total_bytes, *_counter_values(current)),
    ):
        if after < before:
            raise ResourceTelemetryError("counter_regression", f"{path}.{name}")
    total_delta = current.total_bytes - previous.total_bytes
    if Decimal(total_delta) > enforced_limit * Decimal(elapsed_seconds):
        raise ResourceTelemetryError("rate_limit_exceeded", f"{path}.total_bytes")


def _parse_storage_endpoint(
    raw_endpoint: Any, path: str, config: ResourceConfig
) -> StorageEndpoint:
    endpoint_object = _object(
        raw_endpoint,
        path,
        (
            "measured_at",
            "used_bytes",
            "available_bytes",
            "physical_layout_bytes",
            "physical_layout_count",
            "logical_mapping_bytes",
            "logical_mapping_count",
        ),
    )
    endpoint = StorageEndpoint(
        measured_at=_timestamp(endpoint_object, "measured_at", path),
        used_bytes=_integer(endpoint_object, "used_bytes", path, maximum=MAX_BYTES),
        available_bytes=_integer(endpoint_object, "available_bytes", path, maximum=MAX_BYTES),
        physical_layout_bytes=_integer(
            endpoint_object, "physical_layout_bytes", path, maximum=MAX_BYTES
        ),
        physical_layout_count=_integer(
            endpoint_object, "physical_layout_count", path, maximum=MAX_COUNT
        ),
        logical_mapping_bytes=_integer(
            endpoint_object, "logical_mapping_bytes", path, maximum=MAX_BYTES
        ),
        logical_mapping_count=_integer(
            endpoint_object, "logical_mapping_count", path, maximum=MAX_COUNT
        ),
    )
    _validate_usage(
        endpoint.used_bytes,
        endpoint.available_bytes,
        config.storage_capacity_bytes,
        f"{path}.used_bytes",
        f"{path}.available_bytes",
    )
    _validate_storage_domains(
        physical_bytes=endpoint.physical_layout_bytes,
        physical_count=endpoint.physical_layout_count,
        logical_bytes=endpoint.logical_mapping_bytes,
        logical_count=endpoint.logical_mapping_count,
        used=endpoint.used_bytes,
        available=endpoint.available_bytes,
        capacity=config.storage_capacity_bytes,
        path=path,
    )
    return endpoint


def _parse_preexisting_layouts(
    raw_layouts: Any, path: str
) -> tuple[PreexistingLayout, ...]:
    layout_rows = _array(raw_layouts, path, maximum=MAX_PREEXISTING_LAYOUTS)
    parsed_layouts: list[PreexistingLayout] = []
    seen_layout_ids: set[str] = set()
    for index, raw_layout in enumerate(layout_rows):
        item_path = f"{path}[{index}]"
        layout_object = _object(raw_layout, item_path, ("physical_layout_id_sha256",))
        layout_id = _digest(layout_object, "physical_layout_id_sha256", item_path)
        if layout_id in seen_layout_ids:
            raise ResourceTelemetryError(
                "duplicate_id", f"{item_path}.physical_layout_id_sha256"
            )
        seen_layout_ids.add(layout_id)
        parsed_layouts.append(PreexistingLayout(layout_id))
    return tuple(parsed_layouts)


def _parse_storage_deltas(raw_deltas: Any, path: str) -> tuple[StorageDelta, ...]:
    delta_rows = _array(raw_deltas, path, minimum=1, maximum=MAX_STORAGE_DELTAS)
    parsed_deltas: list[StorageDelta] = []
    seen_import_ids: set[str] = set()
    unique_layouts: set[str] = set()
    fields = (
        "import_id_sha256",
        "kind",
        "physical_layout_id_sha256",
        "physical_net_new_bytes",
        "logical_net_new_bytes",
    )
    for index, raw_delta in enumerate(delta_rows):
        item_path = f"{path}[{index}]"
        delta_object = _object(raw_delta, item_path, fields)
        import_id = _digest(delta_object, "import_id_sha256", item_path)
        if import_id in seen_import_ids:
            raise ResourceTelemetryError("duplicate_id", f"{item_path}.import_id_sha256")
        seen_import_ids.add(import_id)
        kind = _enum(delta_object, "kind", item_path, ("unique_build", "reuse"))
        layout_id = _digest(delta_object, "physical_layout_id_sha256", item_path)
        physical = _integer(
            delta_object, "physical_net_new_bytes", item_path, maximum=MAX_BYTES
        )
        logical = _integer(
            delta_object,
            "logical_net_new_bytes",
            item_path,
            minimum=1,
            maximum=MAX_BYTES,
        )
        if kind == "reuse" and physical != 0:
            raise ResourceTelemetryError(
                "inconsistent_storage_delta",
                f"{item_path}.physical_net_new_bytes",
            )
        if kind == "unique_build":
            if physical == 0:
                raise ResourceTelemetryError(
                    "inconsistent_storage_delta",
                    f"{item_path}.physical_net_new_bytes",
                )
            if layout_id in unique_layouts:
                raise ResourceTelemetryError(
                    "duplicate_id", f"{item_path}.physical_layout_id_sha256"
                )
            unique_layouts.add(layout_id)
        parsed_deltas.append(StorageDelta(import_id, kind, layout_id, physical, logical))
    if not unique_layouts:
        raise ResourceTelemetryError("invalid_count", path)
    return tuple(parsed_deltas)


def _terminal_event_window(
    started_at: datetime,
    ended_at: datetime,
    contention_started_at: datetime,
    contention_ended_at: datetime,
    path: str,
) -> None:
    if started_at < contention_started_at or started_at >= contention_ended_at:
        raise ResourceTelemetryError("event_outside_window", f"{path}.started_at")
    if ended_at <= started_at or ended_at > contention_ended_at:
        raise ResourceTelemetryError("event_outside_window", f"{path}.ended_at")


def _completion_event_window(
    started_at: datetime,
    ended_at: datetime,
    contention_started_at: datetime,
    contention_ended_at: datetime,
    path: str,
) -> None:
    if started_at >= contention_ended_at:
        raise ResourceTelemetryError("event_outside_window", f"{path}.started_at")
    if (
        ended_at <= started_at
        or ended_at <= contention_started_at
        or ended_at > contention_ended_at
    ):
        raise ResourceTelemetryError("event_outside_window", f"{path}.ended_at")


def _parse_cleanup_events(
    raw_cleanup_events: Any,
    path: str,
    contention_started_at: datetime,
    contention_ended_at: datetime,
    storage_deltas: tuple[StorageDelta, ...],
) -> tuple[ScratchCleanupEvent, ...]:
    cleanup_event_rows = _array(
        raw_cleanup_events, path, minimum=1, maximum=MAX_SCRATCH_CLEANUP_EVENTS
    )
    unique_imports = {
        storage_delta.import_id_sha256
        for storage_delta in storage_deltas
        if storage_delta.kind == "unique_build"
    }
    observed_import_ids: set[str] = set()
    parsed_cleanup_events: list[ScratchCleanupEvent] = []
    fields = ("import_id_sha256", "started_at", "ended_at", "outcome")
    for index, raw_cleanup_event in enumerate(cleanup_event_rows):
        item_path = f"{path}[{index}]"
        cleanup_event_object = _object(raw_cleanup_event, item_path, fields)
        import_id = _digest(cleanup_event_object, "import_id_sha256", item_path)
        if import_id not in unique_imports:
            raise ResourceTelemetryError("missing_join", f"{item_path}.import_id_sha256")
        if import_id in observed_import_ids:
            raise ResourceTelemetryError(
                "duplicate_terminal_cleanup", f"{item_path}.import_id_sha256"
            )
        started = _timestamp(cleanup_event_object, "started_at", item_path)
        ended = _timestamp(cleanup_event_object, "ended_at", item_path)
        _terminal_event_window(
            started,
            ended,
            contention_started_at,
            contention_ended_at,
            item_path,
        )
        outcome = _enum(cleanup_event_object, "outcome", item_path, ("completed", "failed"))
        parsed_cleanup_events.append(
            ScratchCleanupEvent(import_id, started, ended, outcome)
        )
        observed_import_ids.add(import_id)
    if observed_import_ids != unique_imports:
        raise ResourceTelemetryError("missing_join", path)
    return tuple(parsed_cleanup_events)


def _parse_checkpoint_events(
    raw_checkpoint_events: Any,
    path: str,
    contention_started_at: datetime,
    contention_ended_at: datetime,
) -> tuple[CheckpointEvent, ...]:
    checkpoint_event_rows = _array(raw_checkpoint_events, path, maximum=MAX_CHECKPOINT_EVENTS)
    parsed_checkpoint_events: list[CheckpointEvent] = []
    seen_events: set[tuple[datetime, datetime, str, Decimal, Decimal]] = set()
    fields = (
        "started_at",
        "ended_at",
        "trigger",
        "write_seconds",
        "sync_seconds",
    )
    for index, raw_checkpoint_event in enumerate(checkpoint_event_rows):
        item_path = f"{path}[{index}]"
        checkpoint_event_object = _object(raw_checkpoint_event, item_path, fields)
        started = _timestamp(checkpoint_event_object, "started_at", item_path)
        ended = _timestamp(checkpoint_event_object, "ended_at", item_path)
        _completion_event_window(
            started,
            ended,
            contention_started_at,
            contention_ended_at,
            item_path,
        )
        duration = Decimal((ended - started).total_seconds())
        write_seconds = _decimal(
            checkpoint_event_object, "write_seconds", item_path, maximum=Decimal(MAX_CONTENTION_SECONDS)
        )
        sync_seconds = _decimal(
            checkpoint_event_object, "sync_seconds", item_path, maximum=Decimal(MAX_CONTENTION_SECONDS)
        )
        if write_seconds + sync_seconds > duration:
            raise ResourceTelemetryError(
                "inconsistent_duration", f"{item_path}.write_seconds"
            )
        trigger = _enum(checkpoint_event_object, "trigger", item_path, ("scheduled", "requested"))
        event_key = (started, ended, trigger, write_seconds, sync_seconds)
        if event_key in seen_events:
            raise ResourceTelemetryError("duplicate_event", item_path)
        seen_events.add(event_key)
        parsed_checkpoint_events.append(
            CheckpointEvent(started, ended, trigger, write_seconds, sync_seconds)
        )
    previous_end: datetime | None = None
    for original_index, event in sorted(
        enumerate(parsed_checkpoint_events),
        key=lambda indexed_event: (indexed_event[1].started_at, indexed_event[1].ended_at),
    ):
        if previous_end is not None and event.started_at < previous_end:
            raise ResourceTelemetryError(
                "overlapping_checkpoint",
                f"{path}[{original_index}].started_at",
            )
        previous_end = event.ended_at
    return tuple(parsed_checkpoint_events)


def _parse_autovacuum_events(
    raw_autovacuum_events: Any,
    path: str,
    contention_started_at: datetime,
    contention_ended_at: datetime,
) -> tuple[AutovacuumEvent, ...]:
    autovacuum_event_rows = _array(raw_autovacuum_events, path, maximum=MAX_AUTOVACUUM_EVENTS)
    parsed_autovacuum_events: list[AutovacuumEvent] = []
    seen_events: set[tuple[str, datetime, datetime]] = set()
    fields = ("relation_id_sha256", "started_at", "ended_at", "outcome")
    for index, raw_autovacuum_event in enumerate(autovacuum_event_rows):
        item_path = f"{path}[{index}]"
        autovacuum_event_object = _object(raw_autovacuum_event, item_path, fields)
        started = _timestamp(autovacuum_event_object, "started_at", item_path)
        ended = _timestamp(autovacuum_event_object, "ended_at", item_path)
        _completion_event_window(
            started,
            ended,
            contention_started_at,
            contention_ended_at,
            item_path,
        )
        relation_id = _digest(autovacuum_event_object, "relation_id_sha256", item_path)
        outcome = _enum(autovacuum_event_object, "outcome", item_path, ("completed", "cancelled"))
        event_key = (relation_id, started, ended)
        if event_key in seen_events:
            raise ResourceTelemetryError("duplicate_event", item_path)
        seen_events.add(event_key)
        parsed_autovacuum_events.append(
            AutovacuumEvent(relation_id, started, ended, outcome)
        )
    return tuple(parsed_autovacuum_events)


def _parse_event_counter_snapshot(
    raw_event_counters: Any, path: str
) -> EventCounterSnapshot:
    fields = (
        "observed_at",
        "checkpoint_epoch_sha256",
        "checkpoint_completed",
        "checkpoint_requested",
        "autovacuum_epoch_sha256",
        "autovacuum_completed",
        "autovacuum_cancelled",
    )
    event_counter_object = _object(raw_event_counters, path, fields)
    checkpoint_completed = _integer(
        event_counter_object, "checkpoint_completed", path, maximum=MAX_COUNT
    )
    checkpoint_requested = _integer(
        event_counter_object, "checkpoint_requested", path, maximum=MAX_COUNT
    )
    if checkpoint_requested > checkpoint_completed:
        raise ResourceTelemetryError(
            "inconsistent_event_counter", f"{path}.checkpoint_requested"
        )
    return EventCounterSnapshot(
        observed_at=_timestamp(event_counter_object, "observed_at", path),
        checkpoint_epoch_sha256=_digest(
            event_counter_object, "checkpoint_epoch_sha256", path
        ),
        checkpoint_completed=checkpoint_completed,
        checkpoint_requested=checkpoint_requested,
        autovacuum_epoch_sha256=_digest(
            event_counter_object, "autovacuum_epoch_sha256", path
        ),
        autovacuum_completed=_integer(
            event_counter_object, "autovacuum_completed", path, maximum=MAX_COUNT
        ),
        autovacuum_cancelled=_integer(
            event_counter_object, "autovacuum_cancelled", path, maximum=MAX_COUNT
        ),
    )


def _parse_gc_cycle_backlog_fields(
    cycle_object: Mapping[str, Any], item_path: str
) -> dict[str, int | bool]:
    return {
        "executed": _is_boolean(cycle_object, "executed", item_path),
        "starting_backlog_bytes": _integer(
            cycle_object, "starting_backlog_bytes", item_path, maximum=MAX_BYTES
        ),
        "starting_backlog_layouts": _integer(
            cycle_object, "starting_backlog_layouts", item_path, maximum=MAX_COUNT
        ),
        "newly_eligible_bytes": _integer(
            cycle_object, "newly_eligible_bytes", item_path, maximum=MAX_BYTES
        ),
        "eligible_layouts": _integer(
            cycle_object, "eligible_layouts", item_path, maximum=MAX_COUNT
        ),
        "deleted_bytes": _integer(
            cycle_object, "deleted_bytes", item_path, maximum=MAX_BYTES
        ),
        "deleted_layouts": _integer(
            cycle_object, "deleted_layouts", item_path, maximum=MAX_COUNT
        ),
        "ending_backlog_bytes": _integer(
            cycle_object, "ending_backlog_bytes", item_path, maximum=MAX_BYTES
        ),
        "ending_backlog_layouts": _integer(
            cycle_object, "ending_backlog_layouts", item_path, maximum=MAX_COUNT
        ),
    }


def _validate_gc_cycle_backlog(
    backlog_fields_by_name: Mapping[str, int | bool], item_path: str
) -> None:
    start_bytes = int(backlog_fields_by_name["starting_backlog_bytes"])
    start_layouts = int(backlog_fields_by_name["starting_backlog_layouts"])
    eligible_bytes = int(backlog_fields_by_name["newly_eligible_bytes"])
    eligible_layouts = int(backlog_fields_by_name["eligible_layouts"])
    deleted_bytes = int(backlog_fields_by_name["deleted_bytes"])
    deleted_layouts = int(backlog_fields_by_name["deleted_layouts"])
    end_bytes = int(backlog_fields_by_name["ending_backlog_bytes"])
    end_layouts = int(backlog_fields_by_name["ending_backlog_layouts"])
    for bytes_value, layouts_value, field_name in (
        (start_bytes, start_layouts, "starting_backlog_layouts"),
        (eligible_bytes, eligible_layouts, "eligible_layouts"),
        (end_bytes, end_layouts, "ending_backlog_layouts"),
    ):
        if (bytes_value == 0) != (layouts_value == 0):
            raise ResourceTelemetryError(
                "gc_layout_bytes_mismatch", f"{item_path}.{field_name}"
            )
    if (deleted_bytes == 0) != (deleted_layouts == 0):
        raise ResourceTelemetryError(
            "gc_deletion_mismatch", f"{item_path}.deleted_layouts"
        )
    if not backlog_fields_by_name["executed"] and (deleted_bytes or deleted_layouts):
        raise ResourceTelemetryError("gc_not_executed", f"{item_path}.deleted_bytes")
    if start_bytes + eligible_bytes > MAX_BYTES:
        raise ResourceTelemetryError("numeric_overflow", f"{item_path}.newly_eligible_bytes")
    if start_layouts + eligible_layouts > MAX_COUNT:
        raise ResourceTelemetryError("numeric_overflow", f"{item_path}.eligible_layouts")
    if end_bytes != start_bytes + eligible_bytes - deleted_bytes:
        raise ResourceTelemetryError("gc_conservation", f"{item_path}.ending_backlog_bytes")
    if end_layouts != start_layouts + eligible_layouts - deleted_layouts:
        raise ResourceTelemetryError("gc_conservation", f"{item_path}.ending_backlog_layouts")


def _parse_gc_cycle(
    raw_cycle: Any,
    item_path: str,
    expected_start: datetime,
    config_revision_sha256: str,
) -> GcCycle:
    fields = (
        "started_at",
        "ended_at",
        "config_revision_sha256",
        "executed",
        "overlap_detected",
        "reference_recheck_failures",
        "starting_backlog_bytes",
        "starting_backlog_layouts",
        "newly_eligible_bytes",
        "eligible_layouts",
        "deleted_bytes",
        "deleted_layouts",
        "ending_backlog_bytes",
        "ending_backlog_layouts",
    )
    cycle_object = _object(raw_cycle, item_path, fields)
    started = _timestamp(cycle_object, "started_at", item_path)
    ended = _timestamp(cycle_object, "ended_at", item_path)
    if started != expected_start:
        raise ResourceTelemetryError("gc_discontinuity", f"{item_path}.started_at")
    if ended - started != timedelta(seconds=GC_CYCLE_SECONDS):
        raise ResourceTelemetryError("invalid_interval", f"{item_path}.ended_at")
    revision = _digest(cycle_object, "config_revision_sha256", item_path)
    if revision != config_revision_sha256:
        raise ResourceTelemetryError("config_changed", f"{item_path}.config_revision_sha256")
    backlog_fields_by_name = _parse_gc_cycle_backlog_fields(cycle_object, item_path)
    _validate_gc_cycle_backlog(backlog_fields_by_name, item_path)
    return GcCycle(
        started_at=started,
        ended_at=ended,
        config_revision_sha256=revision,
        overlap_detected=_is_boolean(cycle_object, "overlap_detected", item_path),
        reference_recheck_failures=_integer(
            cycle_object, "reference_recheck_failures", item_path, maximum=MAX_COUNT
        ),
        **backlog_fields_by_name,
    )


def _validate_gc_cycle_continuity(
    previous_cycle: GcCycle | None, current_cycle: GcCycle, item_path: str
) -> None:
    if previous_cycle is None:
        return
    if current_cycle.starting_backlog_bytes != previous_cycle.ending_backlog_bytes:
        raise ResourceTelemetryError("gc_discontinuity", f"{item_path}.starting_backlog_bytes")
    if current_cycle.starting_backlog_layouts != previous_cycle.ending_backlog_layouts:
        raise ResourceTelemetryError("gc_discontinuity", f"{item_path}.starting_backlog_layouts")


def _parse_gc_cycles(
    raw_cycles: Any,
    path: str,
    contention_started_at: datetime,
    config_revision_sha256: str,
) -> tuple[GcCycle, ...]:
    """Validate the fixed hourly GC history preceding the contention window."""

    cycle_rows = _array(raw_cycles, path, exact=GC_CYCLE_COUNT)
    parsed_cycles: list[GcCycle] = []
    expected_start = contention_started_at - timedelta(hours=GC_CYCLE_COUNT)
    previous_cycle: GcCycle | None = None
    for index, raw_cycle in enumerate(cycle_rows):
        item_path = f"{path}[{index}]"
        current_cycle = _parse_gc_cycle(
            raw_cycle, item_path, expected_start, config_revision_sha256
        )
        _validate_gc_cycle_continuity(previous_cycle, current_cycle, item_path)
        parsed_cycles.append(current_cycle)
        expected_start = current_cycle.ended_at
        previous_cycle = current_cycle
    if expected_start != contention_started_at:
        raise ResourceTelemetryError("gc_discontinuity", path)
    return tuple(parsed_cycles)


def _validate_layout_references(
    baseline: ResourceBaseline,
    preexisting_layouts: tuple[PreexistingLayout, ...],
    storage_deltas: tuple[StorageDelta, ...],
) -> set[str]:
    preexisting_ids = {
        layout.physical_layout_id_sha256 for layout in preexisting_layouts
    }
    if len(preexisting_ids) > baseline.physical_layout_count:
        raise ResourceTelemetryError("invalid_count", "preexisting_layouts")

    unique_layout_ids = {
        storage_delta.physical_layout_id_sha256
        for storage_delta in storage_deltas
        if storage_delta.kind == "unique_build"
    }
    if preexisting_ids & unique_layout_ids:
        raise ResourceTelemetryError(
            "layout_already_exists", "preexisting_layouts"
        )

    referenced_preexisting_ids: set[str] = set()
    for index, storage_delta in enumerate(storage_deltas):
        if storage_delta.kind != "reuse":
            continue
        if storage_delta.physical_layout_id_sha256 in unique_layout_ids:
            continue
        if storage_delta.physical_layout_id_sha256 not in preexisting_ids:
            raise ResourceTelemetryError(
                "unknown_layout",
                f"storage_deltas[{index}].physical_layout_id_sha256",
            )
        referenced_preexisting_ids.add(storage_delta.physical_layout_id_sha256)
    if referenced_preexisting_ids != preexisting_ids:
        raise ResourceTelemetryError("unused_layout_proof", "preexisting_layouts")
    return unique_layout_ids


def _validate_storage_endpoint_totals(
    baseline: ResourceBaseline,
    endpoint: StorageEndpoint,
    storage_deltas: tuple[StorageDelta, ...],
    unique_layout_ids: set[str],
) -> None:
    physical_delta = _bounded_sum(
        [storage_delta.physical_net_new_bytes for storage_delta in storage_deltas],
        "storage_deltas.physical_net_new_bytes",
    )
    logical_delta = _bounded_sum(
        [storage_delta.logical_net_new_bytes for storage_delta in storage_deltas],
        "storage_deltas.logical_net_new_bytes",
    )
    expected_physical_bytes = baseline.physical_layout_bytes + physical_delta
    expected_logical_bytes = baseline.logical_mapping_bytes + logical_delta
    expected_physical_count = baseline.physical_layout_count + len(unique_layout_ids)
    expected_logical_count = baseline.logical_mapping_count + len(storage_deltas)
    if expected_physical_bytes > MAX_BYTES:
        raise ResourceTelemetryError(
            "numeric_overflow", "storage_endpoint.physical_layout_bytes"
        )
    if expected_logical_bytes > MAX_BYTES:
        raise ResourceTelemetryError(
            "numeric_overflow", "storage_endpoint.logical_mapping_bytes"
        )
    if expected_physical_count > MAX_COUNT:
        raise ResourceTelemetryError(
            "numeric_overflow", "storage_endpoint.physical_layout_count"
        )
    if expected_logical_count > MAX_COUNT:
        raise ResourceTelemetryError(
            "numeric_overflow", "storage_endpoint.logical_mapping_count"
        )
    for observed, expected, field_name in (
        (endpoint.physical_layout_bytes, expected_physical_bytes, "physical_layout_bytes"),
        (endpoint.logical_mapping_bytes, expected_logical_bytes, "logical_mapping_bytes"),
        (endpoint.physical_layout_count, expected_physical_count, "physical_layout_count"),
        (endpoint.logical_mapping_count, expected_logical_count, "logical_mapping_count"),
    ):
        if observed != expected:
            raise ResourceTelemetryError(
                "storage_conservation", f"storage_endpoint.{field_name}"
            )


def _validate_storage_conservation(
    baseline: ResourceBaseline,
    endpoint: StorageEndpoint,
    preexisting_layouts: tuple[PreexistingLayout, ...],
    storage_deltas: tuple[StorageDelta, ...],
) -> None:
    """Require layout references and endpoint totals to match the delta log."""

    unique_layout_ids = _validate_layout_references(
        baseline, preexisting_layouts, storage_deltas
    )
    _validate_storage_endpoint_totals(
        baseline, endpoint, storage_deltas, unique_layout_ids
    )


def _validate_event_counter_reconciliation(
    start: EventCounterSnapshot,
    end: EventCounterSnapshot,
    checkpoint_events: tuple[CheckpointEvent, ...],
    autovacuum_events: tuple[AutovacuumEvent, ...],
) -> None:
    for name in ("checkpoint_epoch_sha256", "autovacuum_epoch_sha256"):
        if getattr(start, name) != getattr(end, name):
            raise ResourceTelemetryError(
                "counter_epoch_changed", f"event_counters_end.{name}"
            )
    expected_delta_by_counter = {
        "checkpoint_completed": len(checkpoint_events),
        "checkpoint_requested": sum(
            event.trigger == "requested" for event in checkpoint_events
        ),
        "autovacuum_completed": sum(
            event.outcome == "completed" for event in autovacuum_events
        ),
        "autovacuum_cancelled": sum(
            event.outcome == "cancelled" for event in autovacuum_events
        ),
    }
    for name, expected_delta in expected_delta_by_counter.items():
        before = getattr(start, name)
        after = getattr(end, name)
        if after < before:
            raise ResourceTelemetryError(
                "counter_regression", f"event_counters_end.{name}"
            )
        if after - before != expected_delta:
            raise ResourceTelemetryError(
                "event_counter_mismatch", f"event_counters_end.{name}"
            )


def _observed_autovacuum_concurrency(
    events: tuple[AutovacuumEvent, ...], interval: ResourceInterval
) -> int:
    points: list[tuple[datetime, int]] = []
    for event in events:
        overlap_start = max(event.started_at, interval.started_at)
        overlap_end = min(event.ended_at, interval.ended_at)
        if overlap_start < overlap_end:
            points.append((overlap_start, 1))
            points.append((overlap_end, -1))
    active = 0
    peak = 0
    for _, delta in sorted(points, key=lambda item: (item[0], item[1])):
        active += delta
        peak = max(peak, active)
    return peak


def _validate_first_interval_covers_baseline(
    baseline: ResourceBaseline, first: ResourceInterval
) -> None:
    checks = (
        (first.scratch_peak_used_bytes >= baseline.scratch_used_bytes, "scratch_peak_used_bytes"),
        (
            first.scratch_min_available_bytes <= baseline.scratch_available_bytes,
            "scratch_min_available_bytes",
        ),
        (first.temp_peak_used_bytes >= baseline.temp_used_bytes, "temp_peak_used_bytes"),
        (
            first.temp_min_available_bytes <= baseline.temp_available_bytes,
            "temp_min_available_bytes",
        ),
    )
    for covered, field_name in checks:
        if not covered:
            raise ResourceTelemetryError(
                "baseline_not_covered", f"intervals[0].{field_name}"
            )


def _parse_resource_context(raw_telemetry: Any) -> _ResourceTelemetryContext:
    telemetry_object = _object(raw_telemetry, "", _ROOT_FIELDS)
    contention_run_id = _digest(telemetry_object, "contention_run_id", "")
    started_at = _timestamp(telemetry_object, "contention_started_at", "")
    ended_at = _timestamp(telemetry_object, "contention_ended_at", "")
    duration_seconds = int((ended_at - started_at).total_seconds())
    if (
        duration_seconds < MIN_CONTENTION_SECONDS
        or duration_seconds > MAX_CONTENTION_SECONDS
        or duration_seconds % INTERVAL_SECONDS
    ):
        raise ResourceTelemetryError("invalid_window", "contention_ended_at")

    config_start = _parse_config(telemetry_object["config_start"], "config_start")
    config_end = _parse_config(telemetry_object["config_end"], "config_end")
    if config_start.observed_at != started_at:
        raise ResourceTelemetryError("timestamp_mismatch", "config_start.observed_at")
    if config_end.observed_at != ended_at:
        raise ResourceTelemetryError("timestamp_mismatch", "config_end.observed_at")
    for name in _CONFIG_STABLE_FIELDS:
        if getattr(config_start, name) != getattr(config_end, name):
            raise ResourceTelemetryError("config_changed", f"config_end.{name}")

    baseline = _parse_baseline(telemetry_object["baseline"], "baseline", config_start)
    if baseline.observed_at != started_at:
        raise ResourceTelemetryError("timestamp_mismatch", "baseline.observed_at")

    return _ResourceTelemetryContext(
        telemetry_object,
        contention_run_id,
        started_at,
        ended_at,
        duration_seconds,
        config_start,
        config_end,
        baseline,
    )


def _validate_interval_sequence(
    interval: ResourceInterval,
    path: str,
    expected_start: datetime,
    previous_write: CategoryCounters,
    previous_wal: CategoryCounters,
    config: ResourceConfig,
) -> None:
    if interval.config_revision_sha256 != config.config_revision_sha256:
        raise ResourceTelemetryError("config_changed", f"{path}.config_revision_sha256")
    if interval.started_at != expected_start:
        raise ResourceTelemetryError("interval_gap", f"{path}.started_at")
    if interval.ended_at - interval.started_at != timedelta(seconds=INTERVAL_SECONDS):
        raise ResourceTelemetryError("invalid_interval", f"{path}.ended_at")
    _validate_counter_progression(
        previous_write,
        interval.write_counters,
        f"{path}.write_counters",
        elapsed_seconds=INTERVAL_SECONDS,
        enforced_limit=config.write_enforced_limit_bytes_per_second,
    )
    _validate_counter_progression(
        previous_wal,
        interval.wal_counters,
        f"{path}.wal_counters",
        elapsed_seconds=INTERVAL_SECONDS,
        enforced_limit=config.wal_enforced_limit_bytes_per_second,
    )


def _parse_resource_intervals(
    context: _ResourceTelemetryContext,
) -> tuple[ResourceInterval, ...]:
    expected_interval_count = context.duration_seconds // INTERVAL_SECONDS
    interval_rows = _array(
        context.telemetry_object["intervals"],
        "intervals",
        exact=expected_interval_count,
    )
    intervals: list[ResourceInterval] = []
    expected_start = context.contention_started_at
    previous_write = context.baseline.write_counters
    previous_wal = context.baseline.wal_counters
    for index, raw_interval in enumerate(interval_rows):
        path = f"intervals[{index}]"
        interval = _parse_interval(raw_interval, path, context.config_start)
        _validate_interval_sequence(
            interval,
            path,
            expected_start,
            previous_write,
            previous_wal,
            context.config_start,
        )
        intervals.append(interval)
        expected_start = interval.ended_at
        previous_write = interval.write_counters
        previous_wal = interval.wal_counters
    if expected_start != context.contention_ended_at:
        raise ResourceTelemetryError("interval_gap", "contention_ended_at")
    last_interval_path = f"intervals[{len(intervals) - 1}]"
    _validate_counter_progression(
        context.baseline.write_counters,
        previous_write,
        f"{last_interval_path}.write_counters",
        elapsed_seconds=context.duration_seconds,
        enforced_limit=context.config_start.write_enforced_limit_bytes_per_second,
    )
    _validate_counter_progression(
        context.baseline.wal_counters,
        previous_wal,
        f"{last_interval_path}.wal_counters",
        elapsed_seconds=context.duration_seconds,
        enforced_limit=context.config_start.wal_enforced_limit_bytes_per_second,
    )
    _validate_first_interval_covers_baseline(context.baseline, intervals[0])
    return tuple(intervals)


def _parse_storage_evidence(context: _ResourceTelemetryContext) -> _StorageEvidence:
    storage_endpoint = _parse_storage_endpoint(
        context.telemetry_object["storage_endpoint"], "storage_endpoint", context.config_start
    )
    if storage_endpoint.measured_at != context.contention_ended_at:
        raise ResourceTelemetryError(
            "timestamp_mismatch", "storage_endpoint.measured_at"
        )
    preexisting_layouts = _parse_preexisting_layouts(
        context.telemetry_object["preexisting_layouts"], "preexisting_layouts"
    )
    storage_deltas = _parse_storage_deltas(
        context.telemetry_object["storage_deltas"], "storage_deltas"
    )
    _validate_storage_conservation(
        context.baseline, storage_endpoint, preexisting_layouts, storage_deltas
    )
    return _StorageEvidence(storage_endpoint, preexisting_layouts, storage_deltas)


def _parse_event_evidence(
    context: _ResourceTelemetryContext, storage_evidence: _StorageEvidence
) -> _EventEvidence:
    cleanup_events = _parse_cleanup_events(
        context.telemetry_object["scratch_cleanup_events"],
        "scratch_cleanup_events",
        context.contention_started_at,
        context.contention_ended_at,
        storage_evidence.storage_deltas,
    )
    checkpoint_events = _parse_checkpoint_events(
        context.telemetry_object["checkpoint_events"],
        "checkpoint_events",
        context.contention_started_at,
        context.contention_ended_at,
    )
    autovacuum_events = _parse_autovacuum_events(
        context.telemetry_object["autovacuum_events"],
        "autovacuum_events",
        context.contention_started_at,
        context.contention_ended_at,
    )
    event_counters_start = _parse_event_counter_snapshot(
        context.telemetry_object["event_counters_start"], "event_counters_start"
    )
    event_counters_end = _parse_event_counter_snapshot(
        context.telemetry_object["event_counters_end"], "event_counters_end"
    )
    if event_counters_start.observed_at != context.contention_started_at:
        raise ResourceTelemetryError(
            "timestamp_mismatch", "event_counters_start.observed_at"
        )
    if event_counters_end.observed_at != context.contention_ended_at:
        raise ResourceTelemetryError(
            "timestamp_mismatch", "event_counters_end.observed_at"
        )
    _validate_event_counter_reconciliation(
        event_counters_start,
        event_counters_end,
        checkpoint_events,
        autovacuum_events,
    )
    return _EventEvidence(
        cleanup_events,
        checkpoint_events,
        autovacuum_events,
        event_counters_start,
        event_counters_end,
    )


def _validate_autovacuum_concurrency(
    intervals: tuple[ResourceInterval, ...], autovacuum_events: tuple[AutovacuumEvent, ...]
) -> None:
    for index, interval in enumerate(intervals):
        if (
            _observed_autovacuum_concurrency(autovacuum_events, interval)
            > interval.autovacuum.workers
        ):
            raise ResourceTelemetryError(
                "autovacuum_concurrency_exceeded",
                f"intervals[{index}].autovacuum.workers",
            )
def _validate_aggregate_bounds(
    intervals: tuple[ResourceInterval, ...], gc_cycles: tuple[GcCycle, ...]
) -> None:
    _bounded_count_sum(
        [interval.pool_wait.observations for interval in intervals],
        "intervals.pool_wait",
    )
    _bounded_count_sum(
        [interval.pool_wait.timeout_errors for interval in intervals],
        "intervals.pool_wait.timeout_errors",
    )
    _bounded_sum(
        [cycle.newly_eligible_bytes for cycle in gc_cycles],
        "gc_cycles.newly_eligible_bytes",
    )
    _bounded_sum([cycle.deleted_bytes for cycle in gc_cycles], "gc_cycles.deleted_bytes")
    _bounded_count_sum(
        [cycle.eligible_layouts for cycle in gc_cycles], "gc_cycles.eligible_layouts"
    )
    _bounded_count_sum(
        [cycle.deleted_layouts for cycle in gc_cycles], "gc_cycles.deleted_layouts"
    )
    _bounded_count_sum(
        [cycle.reference_recheck_failures for cycle in gc_cycles],
        "gc_cycles.reference_recheck_failures",
    )


def _parse_resource_telemetry(raw_telemetry: Any) -> ResourceTelemetry:
    """Validate one fixed raw telemetry object and return immutable rows."""

    context = _parse_resource_context(raw_telemetry)
    intervals = _parse_resource_intervals(context)
    storage_evidence = _parse_storage_evidence(context)
    event_evidence = _parse_event_evidence(context, storage_evidence)
    _validate_autovacuum_concurrency(intervals, event_evidence.autovacuum_events)
    gc_cycles = _parse_gc_cycles(
        context.telemetry_object["gc_cycles"],
        "gc_cycles",
        context.contention_started_at,
        context.config_start.config_revision_sha256,
    )
    _validate_aggregate_bounds(intervals, gc_cycles)
    return ResourceTelemetry(
        _validation_token=_VALIDATED_TELEMETRY_TOKEN,
        contention_run_id=context.contention_run_id,
        contention_started_at=context.contention_started_at,
        contention_ended_at=context.contention_ended_at,
        config_start=context.config_start,
        config_end=context.config_end,
        baseline=context.baseline,
        intervals=intervals,
        scratch_cleanup_events=event_evidence.cleanup_events,
        checkpoint_events=event_evidence.checkpoint_events,
        autovacuum_events=event_evidence.autovacuum_events,
        event_counters_start=event_evidence.counters_start,
        event_counters_end=event_evidence.counters_end,
        storage_endpoint=storage_evidence.endpoint,
        preexisting_layouts=storage_evidence.preexisting_layouts,
        storage_deltas=storage_evidence.storage_deltas,
        gc_cycles=gc_cycles,
    )


def _conservative_used(used: int, available: int, capacity: int) -> int:
    return max(used, capacity - available)


def _counter_rate_summary(
    baseline: CategoryCounters,
    intervals: tuple[ResourceInterval, ...],
    *,
    attribute: str,
    duration_seconds: int,
    sustainable: Decimal,
) -> CounterRateSummary:
    previous = baseline
    peaks = [0, 0, 0]
    peak_total = 0
    for interval in intervals:
        current = getattr(interval, attribute)
        deltas = tuple(
            after - before
            for before, after in zip(
                _counter_values(previous), _counter_values(current)
            )
        )
        peaks = [max(existing, delta) for existing, delta in zip(peaks, deltas)]
        peak_total = max(peak_total, current.total_bytes - previous.total_bytes)
        previous = current
    totals = tuple(
        after - before
        for before, after in zip(
            _counter_values(baseline), _counter_values(previous)
        )
    )
    duration = Decimal(duration_seconds)
    interval_duration = Decimal(INTERVAL_SECONDS)
    return CounterRateSummary(
        total_bytes_per_second=Decimal(
            previous.total_bytes - baseline.total_bytes
        )
        / duration,
        import_bytes_per_second=Decimal(totals[0]) / duration,
        api_bytes_per_second=Decimal(totals[1]) / duration,
        other_bytes_per_second=Decimal(totals[2]) / duration,
        sustainable_bytes_per_second=sustainable,
        peak_interval_total_bytes_per_second=Decimal(peak_total)
        / interval_duration,
        peak_interval_import_bytes_per_second=Decimal(peaks[0]) / interval_duration,
        peak_interval_api_bytes_per_second=Decimal(peaks[1]) / interval_duration,
        peak_interval_other_bytes_per_second=Decimal(peaks[2]) / interval_duration,
    )


def _pool_wait_summary(
    intervals: tuple[ResourceInterval, ...]
) -> PostgresPoolWaitSummary:
    bucket_counts = [0] * len(POOL_WAIT_BUCKET_UPPER_BOUNDS_MS)
    overflow = 0
    timeout_errors = 0
    maximum = Decimal(0)
    for interval in intervals:
        histogram = interval.pool_wait
        bucket_counts = [
            left + right for left, right in zip(bucket_counts, histogram.bucket_counts)
        ]
        overflow += histogram.overflow_count
        timeout_errors += histogram.timeout_errors
        maximum = max(maximum, histogram.max_ms)
    observations = sum(bucket_counts) + overflow
    if observations == 0:
        p95 = Decimal(0)
    else:
        rank = (95 * observations + 99) // 100
        cumulative = 0
        p95 = maximum
        for upper, count in zip(POOL_WAIT_BUCKET_UPPER_BOUNDS_MS, bucket_counts):
            cumulative += count
            if cumulative >= rank:
                p95 = min(upper, maximum)
                break
    return PostgresPoolWaitSummary(
        observations=observations,
        waited_acquisitions=observations - bucket_counts[0],
        p95_ms=p95,
        max_ms=maximum,
        timeout_errors=timeout_errors,
    )


def _derive_scratch_summary(
    telemetry: ResourceTelemetry, config: ResourceConfig, baseline: ResourceBaseline
) -> ScratchSummary:
    scratch_baseline = _conservative_used(
        baseline.scratch_used_bytes,
        baseline.scratch_available_bytes,
        config.scratch_capacity_bytes,
    )
    scratch_peak = max(
        scratch_baseline,
        max(
            _conservative_used(
                interval.scratch_peak_used_bytes,
                interval.scratch_min_available_bytes,
                config.scratch_capacity_bytes,
            )
            for interval in telemetry.intervals
        )
    )
    cleanup_failures = sum(
        event.outcome == "failed" for event in telemetry.scratch_cleanup_events
    )
    return ScratchSummary(
        capacity_bytes=config.scratch_capacity_bytes,
        baseline_used_bytes=scratch_baseline,
        reserve_bytes=config.scratch_reserve_bytes,
        measured_peak_used_bytes=scratch_peak,
        measured_peak_incremental_bytes=max(0, scratch_peak - scratch_baseline),
        cleanup_cycles=len(telemetry.scratch_cleanup_events),
        cleanup_failures=cleanup_failures,
        cleanup_imports_observed=len(
            {event.import_id_sha256 for event in telemetry.scratch_cleanup_events}
        ),
    )


def _derive_connection_summary(
    intervals: tuple[ResourceInterval, ...], config: ResourceConfig
) -> PostgresConnectionSummary:
    peak_interval = max(intervals, key=lambda interval: interval.connections.total)
    return PostgresConnectionSummary(
        max_connections=config.max_connections,
        reserved_connections=config.reserved_connections,
        peak_api_connections=peak_interval.connections.api_connections,
        peak_import_connections=peak_interval.connections.import_connections,
        peak_other_connections=peak_interval.connections.other_connections,
        peak_connections=peak_interval.connections.total,
        peak_interval_started_at=peak_interval.started_at,
        required_headroom_connections=config.required_connection_headroom,
    )


def _derive_checkpoint_summary(
    telemetry: ResourceTelemetry, duration_seconds: int
) -> PostgresCheckpointSummary:
    checkpoint_write = sum(
        (event.write_seconds for event in telemetry.checkpoint_events), Decimal(0)
    )
    checkpoint_sync = sum(
        (event.sync_seconds for event in telemetry.checkpoint_events), Decimal(0)
    )
    return PostgresCheckpointSummary(
        sample_seconds=Decimal(duration_seconds),
        completed=len(telemetry.checkpoint_events),
        requested=sum(
            event.trigger == "requested" for event in telemetry.checkpoint_events
        ),
        write_seconds=checkpoint_write,
        sync_seconds=checkpoint_sync,
    )


def _derive_temp_summary(
    baseline: ResourceBaseline,
    intervals: tuple[ResourceInterval, ...],
    config: ResourceConfig,
) -> PostgresTempSummary:
    temp_baseline = _conservative_used(
        baseline.temp_used_bytes,
        baseline.temp_available_bytes,
        config.temp_capacity_bytes,
    )
    temp_peak = max(
        temp_baseline,
        max(
            _conservative_used(
                interval.temp_peak_used_bytes,
                interval.temp_min_available_bytes,
                config.temp_capacity_bytes,
            )
            for interval in intervals
        )
    )
    return PostgresTempSummary(
        capacity_bytes=config.temp_capacity_bytes,
        baseline_used_bytes=temp_baseline,
        reserve_bytes=config.temp_reserve_bytes,
        measured_peak_used_bytes=temp_peak,
        measured_peak_incremental_bytes=max(0, temp_peak - temp_baseline),
    )


def _derive_autovacuum_summary(
    telemetry: ResourceTelemetry, config: ResourceConfig, duration_seconds: int
) -> PostgresAutovacuumSummary:
    return PostgresAutovacuumSummary(
        sample_seconds=Decimal(duration_seconds),
        max_workers=config.autovacuum_max_workers,
        peak_workers=max(interval.autovacuum.workers for interval in telemetry.intervals),
        required_headroom_workers=config.required_autovacuum_headroom,
        completed_cycles=sum(
            event.outcome == "completed" for event in telemetry.autovacuum_events
        ),
        cancelled_cycles=sum(
            event.outcome == "cancelled" for event in telemetry.autovacuum_events
        ),
        oldest_pending_minutes=Decimal(
            max(interval.autovacuum.oldest_pending_seconds for interval in telemetry.intervals)
        )
        / Decimal(60),
    )


def _derive_postgres_summary(
    telemetry: ResourceTelemetry,
    config: ResourceConfig,
    baseline: ResourceBaseline,
    duration_seconds: int,
) -> PostgresSummary:
    intervals = telemetry.intervals
    return PostgresSummary(
        connections=_derive_connection_summary(intervals, config),
        write=_counter_rate_summary(
            baseline.write_counters,
            intervals,
            attribute="write_counters",
            duration_seconds=duration_seconds,
            sustainable=config.write_enforced_limit_bytes_per_second,
        ),
        wal=_counter_rate_summary(
            baseline.wal_counters,
            intervals,
            attribute="wal_counters",
            duration_seconds=duration_seconds,
            sustainable=config.wal_enforced_limit_bytes_per_second,
        ),
        pool_wait=_pool_wait_summary(intervals),
        checkpoint=_derive_checkpoint_summary(telemetry, duration_seconds),
        temp=_derive_temp_summary(baseline, intervals, config),
        autovacuum=_derive_autovacuum_summary(telemetry, config, duration_seconds),
    )

def _derive_storage_summary(
    telemetry: ResourceTelemetry, config: ResourceConfig
) -> StorageSummary:
    physical_deltas = tuple(
        storage_delta
        for storage_delta in telemetry.storage_deltas
        if storage_delta.kind == "unique_build"
    )
    physical_total = _bounded_sum(
        [storage_delta.physical_net_new_bytes for storage_delta in physical_deltas],
        "storage_deltas.physical_net_new_bytes",
    )
    logical_total = _bounded_sum(
        [storage_delta.logical_net_new_bytes for storage_delta in telemetry.storage_deltas],
        "storage_deltas.logical_net_new_bytes",
    )
    return StorageSummary(
        physical_unique_builds_observed=len(physical_deltas),
        physical_net_new_bytes_observed=physical_total,
        physical_max_bytes_per_unique_build=max(
            (storage_delta.physical_net_new_bytes for storage_delta in physical_deltas),
            default=0,
        ),
        logical_imports_observed=len(telemetry.storage_deltas),
        logical_net_new_bytes_observed=logical_total,
        logical_max_bytes_per_import=max(
            (
                storage_delta.logical_net_new_bytes
                for storage_delta in telemetry.storage_deltas
            ),
            default=0,
        ),
        current_used_bytes=_conservative_used(
            telemetry.storage_endpoint.used_bytes,
            telemetry.storage_endpoint.available_bytes,
            config.storage_capacity_bytes,
        ),
        capacity_bytes=config.storage_capacity_bytes,
        reserve_bytes=config.storage_reserve_bytes,
        physical_retention_months=config.physical_retention_months,
        logical_retention_months=config.logical_retention_months,
    )

def _derive_gc_metrics(gc_cycles: tuple[GcCycle, ...]) -> _GcDerivedMetrics:
    eligible_bytes = _bounded_sum(
        [cycle.newly_eligible_bytes for cycle in gc_cycles],
        "gc_cycles.newly_eligible_bytes",
    )
    deleted_bytes = _bounded_sum(
        [cycle.deleted_bytes for cycle in gc_cycles], "gc_cycles.deleted_bytes"
    )
    eligible_layouts = sum(cycle.eligible_layouts for cycle in gc_cycles)
    deleted_layouts = sum(cycle.deleted_layouts for cycle in gc_cycles)
    if eligible_layouts > MAX_COUNT or deleted_layouts > MAX_COUNT:
        raise ResourceTelemetryError("numeric_overflow", "gc_cycles")
    window_hours = Decimal(GC_CYCLE_COUNT)
    eligible_rate = Decimal(eligible_bytes) / window_hours
    deleted_rate = Decimal(deleted_bytes) / window_hours
    net_drain = deleted_rate - eligible_rate
    ending_backlog = gc_cycles[-1].ending_backlog_bytes
    if ending_backlog == 0:
        clearance: Decimal | None = Decimal(0)
    elif net_drain > 0:
        clearance = Decimal(ending_backlog) / net_drain
    else:
        clearance = None
    headroom = (
        (Decimal(deleted_bytes) - Decimal(eligible_bytes)) / Decimal(deleted_bytes)
        if deleted_bytes
        else None
    )
    monthly_growth = max(Decimal(0), eligible_rate - deleted_rate) * MONTH_HOURS
    projected_backlog = ending_backlog + int(
        monthly_growth.to_integral_value(rounding=ROUND_CEILING)
    )
    if projected_backlog > MAX_BYTES:
        raise ResourceTelemetryError("numeric_overflow", "gc_cycles")
    return _GcDerivedMetrics(
        eligible_bytes,
        deleted_bytes,
        eligible_layouts,
        deleted_layouts,
        eligible_rate,
        deleted_rate,
        net_drain,
        clearance,
        headroom,
        projected_backlog,
    )


def _derive_gc_summary(
    telemetry: ResourceTelemetry, config: ResourceConfig
) -> GcSummary:
    gc_cycles = telemetry.gc_cycles
    gc_metrics = _derive_gc_metrics(gc_cycles)
    window_hours = Decimal(GC_CYCLE_COUNT)
    ending_backlog = gc_cycles[-1].ending_backlog_bytes
    return GcSummary(
        window_hours=window_hours,
        cycles_observed=len(gc_cycles),
        executed_cycles=sum(cycle.executed for cycle in gc_cycles),
        overlap_count=sum(cycle.overlap_detected for cycle in gc_cycles),
        reference_recheck_failures=sum(
            cycle.reference_recheck_failures for cycle in gc_cycles
        ),
        starting_backlog_bytes=gc_cycles[0].starting_backlog_bytes,
        starting_backlog_layouts=gc_cycles[0].starting_backlog_layouts,
        newly_eligible_bytes=gc_metrics.eligible_bytes,
        deleted_bytes=gc_metrics.deleted_bytes,
        ending_backlog_bytes=ending_backlog,
        ending_backlog_layouts=gc_cycles[-1].ending_backlog_layouts,
        observed_peak_backlog_bytes=max(
            max(
                cycle.starting_backlog_bytes + cycle.newly_eligible_bytes,
                cycle.ending_backlog_bytes,
            )
            for cycle in gc_cycles
        ),
        max_backlog_bytes=config.gc_max_backlog_bytes,
        max_clearance_hours=config.gc_max_clearance_hours,
        eligible_layouts=gc_metrics.eligible_layouts,
        deleted_layouts=gc_metrics.deleted_layouts,
        eligible_bytes_per_hour=gc_metrics.eligible_rate,
        deleted_bytes_per_hour=gc_metrics.deleted_rate,
        net_drain_bytes_per_hour=gc_metrics.net_drain,
        headroom_fraction=gc_metrics.headroom_fraction,
        clearance_hours=gc_metrics.clearance_hours,
        projected_30_day_backlog_bytes=gc_metrics.projected_backlog,
    )


def _derive_resource_summaries(telemetry: ResourceTelemetry) -> ResourceSummaries:
    """Derive gate-ready aggregates exclusively from validated raw telemetry."""

    if (
        not isinstance(telemetry, ResourceTelemetry)
        or telemetry._validation_token is not _VALIDATED_TELEMETRY_TOKEN
    ):
        raise ResourceTelemetryError("invalid_type", "telemetry")
    config = telemetry.config_start
    duration_seconds = int(
        (telemetry.contention_ended_at - telemetry.contention_started_at).total_seconds()
    )
    return ResourceSummaries(
        contention_run_id=telemetry.contention_run_id,
        contention_started_at=telemetry.contention_started_at,
        contention_ended_at=telemetry.contention_ended_at,
        sample_seconds=Decimal(duration_seconds),
        scratch=_derive_scratch_summary(telemetry, config, telemetry.baseline),
        postgresql=_derive_postgres_summary(
            telemetry, config, telemetry.baseline, duration_seconds
        ),
        storage=_derive_storage_summary(telemetry, config),
        gc=_derive_gc_summary(telemetry, config),
    )


def parse_and_derive_resource_telemetry(value: Any) -> ParsedResourceTelemetry:
    """Atomically validate raw input and return telemetry plus its summaries."""

    telemetry = _parse_resource_telemetry(value)
    return ParsedResourceTelemetry(
        telemetry=telemetry,
        summaries=_derive_resource_summaries(telemetry),
    )


__all__ = (
    "GC_CYCLE_COUNT",
    "INTERVAL_SECONDS",
    "MAX_AUTOVACUUM_EVENTS",
    "MAX_CHECKPOINT_EVENTS",
    "MAX_PREEXISTING_LAYOUTS",
    "MAX_SCRATCH_CLEANUP_EVENTS",
    "MAX_STORAGE_DELTAS",
    "POOL_WAIT_BUCKET_UPPER_BOUNDS_MS",
    "ParsedResourceTelemetry",
    "ResourceSummaries",
    "ResourceTelemetryError",
    "parse_and_derive_resource_telemetry",
)
