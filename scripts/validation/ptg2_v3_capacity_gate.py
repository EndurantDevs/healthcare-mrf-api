#!/usr/bin/env python3
"""Strict aggregate capacity release gate for PTG2 V3.

The input contract intentionally accepts only fixed-schema numeric, boolean,
and enumerated evidence. Reports never echo input paths, unknown keys, JSON
values, or exception text. Exit codes are stable: 0 passes, 1 is a completed
release-gate failure, and 2 is invalid or unavailable evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import suppress
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any, Mapping, Sequence


SCRIPT_VERSION = "1.0.0"
SCHEMA_VERSION = 1
DEFAULT_TARGET_LOGICAL_IMPORTS_PER_MONTH = 2_000
MIN_RELEASE_TARGET_LOGICAL_IMPORTS_PER_MONTH = 2_000
MONTH_DAYS = 30
MONTH_HOURS = Decimal(MONTH_DAYS * 24)
MAX_UNIQUE_BUILD_MINUTES = Decimal("15")
MAX_LANE_UTILIZATION = Decimal("0.70")
MIN_POSTGRES_HEADROOM_FRACTION = Decimal("0.20")
MAX_COLD_FIRST_PAGE_P95_MS = Decimal("40")
MAX_API_ERROR_RATE = Decimal("0")
MIN_API_REQUESTS = 3_000
MIN_API_COLD_FIRST_PAGE_SAMPLES = 2_500
MAX_INPUT_BYTES = 1024 * 1024
EVIDENCE_PROFILE = "deployed_release_production_like"
REUSE_EVIDENCE_TYPE = "observed_complete_fingerprint"

EXIT_PASS = 0
EXIT_GATE_FAILURE = 1
EXIT_INVALID_EVIDENCE = 2

_ROOT_FIELDS = (
    "schema_version",
    "evidence_profile",
    "unique_build",
    "reuse",
    "retry",
    "lanes",
    "peak_arrival",
    "scratch",
    "postgresql",
    "storage",
    "gc",
    "api",
)


class EvidenceError(ValueError):
    """A report-safe validation error with no input-derived message."""

    def __init__(self, code: str, field: str | None = None):
        super().__init__(code)
        self.code = code
        self.field = field


class _DuplicateFieldError(ValueError):
    pass


def _field(parent: str, name: str) -> str:
    return f"{parent}.{name}" if parent else name


def _object(
    value: Any,
    path: str,
    required: Sequence[str],
    optional: Sequence[str] = (),
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EvidenceError("invalid_type", path or "root")
    for name in required:
        if name not in value:
            raise EvidenceError("missing_field", _field(path, name))
    allowed = set(required) | set(optional)
    if any(name not in allowed for name in value):
        # Never echo an unknown key: it may itself contain sensitive text.
        raise EvidenceError("unexpected_field", path or "root")
    return value


def _integer(
    obj: Mapping[str, Any],
    name: str,
    path: str,
    *,
    minimum: int | None = None,
) -> int:
    value = obj[name]
    field = _field(path, name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise EvidenceError("invalid_type", field)
    if minimum is not None and value < minimum:
        raise EvidenceError("invalid_value", field)
    return value


def _decimal(
    obj: Mapping[str, Any],
    name: str,
    path: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
    minimum_inclusive: bool = True,
) -> Decimal:
    raw = obj[name]
    field = _field(path, name)
    if isinstance(raw, bool) or not isinstance(raw, (int, float, Decimal)):
        raise EvidenceError("invalid_type", field)
    try:
        value = raw if isinstance(raw, Decimal) else Decimal(str(raw))
    except Exception:
        raise EvidenceError("invalid_value", field)
    if not value.is_finite():
        raise EvidenceError("invalid_value", field)
    if minimum is not None:
        below = value < minimum if minimum_inclusive else value <= minimum
        if below:
            raise EvidenceError("invalid_value", field)
    if maximum is not None and value > maximum:
        raise EvidenceError("invalid_value", field)
    return value


def _boolean(obj: Mapping[str, Any], name: str, path: str) -> bool:
    value = obj[name]
    if not isinstance(value, bool):
        raise EvidenceError("invalid_type", _field(path, name))
    return value


def _fixed_string(
    obj: Mapping[str, Any],
    name: str,
    path: str,
    expected: str,
) -> str:
    value = obj[name]
    field = _field(path, name)
    if not isinstance(value, str):
        raise EvidenceError("invalid_type", field)
    if value != expected:
        raise EvidenceError("invalid_enum", field)
    return value


def _at_most(value: int, maximum: int, field: str) -> None:
    if value > maximum:
        raise EvidenceError("inconsistent_evidence", field)


@dataclass(frozen=True)
class UniqueBuildEvidence:
    sample_count: int
    p95_minutes: Decimal
    max_minutes: Decimal
    fresh_fingerprint_builds: int
    complete_source_coverage_builds: int
    durable_publication_builds: int
    persisted_audit_builds: int
    release_scanner_builds: int


@dataclass(frozen=True)
class ReuseEvidence:
    observed_logical_imports: int
    observed_unique_builds: int
    observed_reuse_hits: int
    complete_fingerprint_verified_hits: int
    logical_publication_verified_hits: int
    production_like_observed_hits: int

    @property
    def qualified_hits(self) -> int:
        """Return the reuse-hit count satisfying every qualification bound."""

        return min(
            self.complete_fingerprint_verified_hits,
            self.logical_publication_verified_hits,
            self.production_like_observed_hits,
        )


@dataclass(frozen=True)
class RetryEvidence:
    successful_unique_builds: int
    failed_attempts: int
    failed_attempt_worker_minutes: Decimal


@dataclass(frozen=True)
class LaneEvidence:
    count: int
    availability_factor: Decimal


@dataclass(frozen=True)
class PeakArrivalEvidence:
    sample_windows: int
    window_minutes: Decimal
    max_queue_delay_minutes: Decimal
    observed_peak_logical_imports: int
    observed_peak_unique_builds: int


@dataclass(frozen=True)
class ScratchEvidence:
    configured_concurrent_unique_builds: int
    measured_concurrent_unique_builds: int
    capacity_bytes: int
    baseline_used_bytes: int
    reserve_bytes: int
    measured_peak_incremental_bytes: int
    cleanup_cycles: int
    cleanup_failures: int


@dataclass(frozen=True)
class PostgresLoadEvidence:
    concurrent_unique_builds: int
    api_requests_per_second: Decimal
    sample_seconds: Decimal


@dataclass(frozen=True)
class PostgresConnectionEvidence:
    max_connections: int
    reserved_connections: int
    peak_api_connections: int
    peak_import_connections: int
    peak_other_connections: int
    required_headroom_connections: int


@dataclass(frozen=True)
class PostgresRateEvidence:
    import_bytes_per_second: Decimal
    api_bytes_per_second: Decimal
    other_bytes_per_second: Decimal
    sustainable_bytes_per_second: Decimal


@dataclass(frozen=True)
class PostgresEvidence:
    load: PostgresLoadEvidence
    connections: PostgresConnectionEvidence
    write: PostgresRateEvidence
    wal: PostgresRateEvidence


@dataclass(frozen=True)
class StorageEvidence:
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
class GcEvidence:
    window_hours: Decimal
    cycles_observed: int
    executed_cycles: int
    overlap_count: int
    reference_recheck_failures: int
    starting_backlog_bytes: int
    newly_eligible_bytes: int
    deleted_bytes: int
    ending_backlog_bytes: int
    max_backlog_bytes: int
    max_clearance_hours: Decimal


@dataclass(frozen=True)
class ApiEvidence:
    concurrent_unique_builds: int
    fresh_processes: bool
    requests: int
    requests_while_imports_running: int
    errors: int
    cold_first_page_samples: int
    distinct_query_keys: int
    cold_first_page_p95_ms: Decimal


@dataclass(frozen=True)
class CapacityEvidence:
    target_logical_imports_per_month: int
    unique_build: UniqueBuildEvidence
    reuse: ReuseEvidence
    retry: RetryEvidence
    lanes: LaneEvidence
    peak_arrival: PeakArrivalEvidence
    scratch: ScratchEvidence
    postgresql: PostgresEvidence
    storage: StorageEvidence
    gc: GcEvidence
    api: ApiEvidence


def _validate_unique_build(root: Mapping[str, Any]) -> UniqueBuildEvidence:
    path = "unique_build"
    fields = (
        "sample_count",
        "p95_minutes",
        "max_minutes",
        "fresh_fingerprint_builds",
        "complete_source_coverage_builds",
        "durable_publication_builds",
        "persisted_audit_builds",
        "release_scanner_builds",
    )
    obj = _object(root[path], path, fields)
    sample_count = _integer(obj, "sample_count", path, minimum=1)
    p95_minutes = _decimal(
        obj,
        "p95_minutes",
        path,
        minimum=Decimal(0),
        minimum_inclusive=False,
    )
    max_minutes = _decimal(
        obj,
        "max_minutes",
        path,
        minimum=Decimal(0),
        minimum_inclusive=False,
    )
    if p95_minutes > max_minutes:
        raise EvidenceError("inconsistent_evidence", f"{path}.p95_minutes")
    counts = {
        name: _integer(obj, name, path, minimum=0)
        for name in fields[3:]
    }
    for name, value in counts.items():
        _at_most(value, sample_count, f"{path}.{name}")
    return UniqueBuildEvidence(
        sample_count=sample_count,
        p95_minutes=p95_minutes,
        max_minutes=max_minutes,
        **counts,
    )


def _validate_reuse(root: Mapping[str, Any]) -> ReuseEvidence:
    path = "reuse"
    fields = (
        "evidence_type",
        "observed_logical_imports",
        "observed_unique_builds",
        "observed_reuse_hits",
        "complete_fingerprint_verified_hits",
        "logical_publication_verified_hits",
        "production_like_observed_hits",
    )
    obj = _object(root[path], path, fields)
    _fixed_string(obj, "evidence_type", path, REUSE_EVIDENCE_TYPE)
    logical = _integer(obj, "observed_logical_imports", path, minimum=1)
    unique = _integer(obj, "observed_unique_builds", path, minimum=0)
    hits = _integer(obj, "observed_reuse_hits", path, minimum=0)
    if unique + hits != logical:
        raise EvidenceError("inconsistent_evidence", f"{path}.observed_reuse_hits")
    verified = {
        name: _integer(obj, name, path, minimum=0)
        for name in fields[4:]
    }
    for name, value in verified.items():
        _at_most(value, hits, f"{path}.{name}")
    return ReuseEvidence(
        observed_logical_imports=logical,
        observed_unique_builds=unique,
        observed_reuse_hits=hits,
        **verified,
    )


def _validate_retry(root: Mapping[str, Any]) -> RetryEvidence:
    path = "retry"
    fields = (
        "successful_unique_builds",
        "failed_attempts",
        "failed_attempt_worker_minutes",
    )
    obj = _object(root[path], path, fields)
    successful = _integer(obj, "successful_unique_builds", path, minimum=1)
    failed = _integer(obj, "failed_attempts", path, minimum=0)
    failed_minutes = _decimal(
        obj,
        "failed_attempt_worker_minutes",
        path,
        minimum=Decimal(0),
    )
    if failed == 0 and failed_minutes != 0:
        raise EvidenceError(
            "inconsistent_evidence",
            f"{path}.failed_attempt_worker_minutes",
        )
    return RetryEvidence(successful, failed, failed_minutes)


def _validate_lanes(root: Mapping[str, Any]) -> LaneEvidence:
    path = "lanes"
    obj = _object(root[path], path, ("count", "availability_factor"))
    return LaneEvidence(
        count=_integer(obj, "count", path, minimum=1),
        availability_factor=_decimal(
            obj,
            "availability_factor",
            path,
            minimum=Decimal(0),
            maximum=Decimal(1),
            minimum_inclusive=False,
        ),
    )


def _validate_peak(root: Mapping[str, Any]) -> PeakArrivalEvidence:
    path = "peak_arrival"
    fields = (
        "sample_windows",
        "window_minutes",
        "max_queue_delay_minutes",
        "observed_peak_logical_imports",
        "observed_peak_unique_builds",
    )
    obj = _object(root[path], path, fields)
    logical = _integer(obj, "observed_peak_logical_imports", path, minimum=1)
    unique = _integer(obj, "observed_peak_unique_builds", path, minimum=0)
    _at_most(unique, logical, f"{path}.observed_peak_unique_builds")
    return PeakArrivalEvidence(
        sample_windows=_integer(obj, "sample_windows", path, minimum=1),
        window_minutes=_decimal(
            obj,
            "window_minutes",
            path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
        max_queue_delay_minutes=_decimal(
            obj,
            "max_queue_delay_minutes",
            path,
            minimum=Decimal(0),
        ),
        observed_peak_logical_imports=logical,
        observed_peak_unique_builds=unique,
    )


def _validate_scratch(root: Mapping[str, Any]) -> ScratchEvidence:
    path = "scratch"
    fields = (
        "configured_concurrent_unique_builds",
        "measured_concurrent_unique_builds",
        "capacity_bytes",
        "baseline_used_bytes",
        "reserve_bytes",
        "measured_peak_incremental_bytes",
        "cleanup_cycles",
        "cleanup_failures",
    )
    obj = _object(root[path], path, fields)
    cycles = _integer(obj, "cleanup_cycles", path, minimum=1)
    failures = _integer(obj, "cleanup_failures", path, minimum=0)
    _at_most(failures, cycles, f"{path}.cleanup_failures")
    return ScratchEvidence(
        configured_concurrent_unique_builds=_integer(
            obj,
            "configured_concurrent_unique_builds",
            path,
            minimum=1,
        ),
        measured_concurrent_unique_builds=_integer(
            obj,
            "measured_concurrent_unique_builds",
            path,
            minimum=1,
        ),
        capacity_bytes=_integer(obj, "capacity_bytes", path, minimum=1),
        baseline_used_bytes=_integer(obj, "baseline_used_bytes", path, minimum=0),
        reserve_bytes=_integer(obj, "reserve_bytes", path, minimum=0),
        measured_peak_incremental_bytes=_integer(
            obj,
            "measured_peak_incremental_bytes",
            path,
            minimum=1,
        ),
        cleanup_cycles=cycles,
        cleanup_failures=failures,
    )


def _validate_rate(obj: Mapping[str, Any], path: str) -> PostgresRateEvidence:
    fields = (
        "import_bytes_per_second",
        "api_bytes_per_second",
        "other_bytes_per_second",
        "sustainable_bytes_per_second",
    )
    obj = _object(obj, path, fields)
    return PostgresRateEvidence(
        import_bytes_per_second=_decimal(
            obj,
            "import_bytes_per_second",
            path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
        api_bytes_per_second=_decimal(
            obj,
            "api_bytes_per_second",
            path,
            minimum=Decimal(0),
        ),
        other_bytes_per_second=_decimal(
            obj,
            "other_bytes_per_second",
            path,
            minimum=Decimal(0),
        ),
        sustainable_bytes_per_second=_decimal(
            obj,
            "sustainable_bytes_per_second",
            path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
    )


def _validate_postgresql(root: Mapping[str, Any]) -> PostgresEvidence:
    """Validate and materialize the PostgreSQL load evidence section."""

    path = "postgresql"
    obj = _object(root[path], path, ("load", "connections", "write", "wal"))
    load_path = f"{path}.load"
    load_obj = _object(
        obj["load"],
        load_path,
        ("concurrent_unique_builds", "api_requests_per_second", "sample_seconds"),
    )
    connection_path = f"{path}.connections"
    connection_fields = (
        "max_connections",
        "reserved_connections",
        "peak_api_connections",
        "peak_import_connections",
        "peak_other_connections",
        "required_headroom_connections",
    )
    connection_obj = _object(obj["connections"], connection_path, connection_fields)
    load = PostgresLoadEvidence(
        concurrent_unique_builds=_integer(
            load_obj,
            "concurrent_unique_builds",
            load_path,
            minimum=1,
        ),
        api_requests_per_second=_decimal(
            load_obj,
            "api_requests_per_second",
            load_path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
        sample_seconds=_decimal(
            load_obj,
            "sample_seconds",
            load_path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
    )
    connections = PostgresConnectionEvidence(
        max_connections=_integer(
            connection_obj,
            "max_connections",
            connection_path,
            minimum=1,
        ),
        reserved_connections=_integer(
            connection_obj,
            "reserved_connections",
            connection_path,
            minimum=0,
        ),
        peak_api_connections=_integer(
            connection_obj,
            "peak_api_connections",
            connection_path,
            minimum=1,
        ),
        peak_import_connections=_integer(
            connection_obj,
            "peak_import_connections",
            connection_path,
            minimum=1,
        ),
        peak_other_connections=_integer(
            connection_obj,
            "peak_other_connections",
            connection_path,
            minimum=0,
        ),
        required_headroom_connections=_integer(
            connection_obj,
            "required_headroom_connections",
            connection_path,
            minimum=1,
        ),
    )
    return PostgresEvidence(
        load=load,
        connections=connections,
        write=_validate_rate(obj["write"], f"{path}.write"),
        wal=_validate_rate(obj["wal"], f"{path}.wal"),
    )


def _validate_storage(root: Mapping[str, Any]) -> StorageEvidence:
    """Validate storage evidence, including observed count and byte consistency."""

    path = "storage"
    fields = (
        "physical_unique_builds_observed",
        "physical_net_new_bytes_observed",
        "physical_max_bytes_per_unique_build",
        "logical_imports_observed",
        "logical_net_new_bytes_observed",
        "logical_max_bytes_per_import",
        "current_used_bytes",
        "capacity_bytes",
        "reserve_bytes",
        "physical_retention_months",
        "logical_retention_months",
    )
    obj = _object(root[path], path, fields)
    physical_count = _integer(
        obj,
        "physical_unique_builds_observed",
        path,
        minimum=1,
    )
    physical_total = _integer(
        obj,
        "physical_net_new_bytes_observed",
        path,
        minimum=0,
    )
    physical_max = _integer(
        obj,
        "physical_max_bytes_per_unique_build",
        path,
        minimum=1,
    )
    logical_count = _integer(obj, "logical_imports_observed", path, minimum=1)
    logical_total = _integer(
        obj,
        "logical_net_new_bytes_observed",
        path,
        minimum=0,
    )
    logical_max = _integer(
        obj,
        "logical_max_bytes_per_import",
        path,
        minimum=1,
    )
    if physical_max * physical_count < physical_total:
        raise EvidenceError(
            "inconsistent_evidence",
            f"{path}.physical_max_bytes_per_unique_build",
        )
    if logical_max * logical_count < logical_total:
        raise EvidenceError(
            "inconsistent_evidence",
            f"{path}.logical_max_bytes_per_import",
        )
    return StorageEvidence(
        physical_unique_builds_observed=physical_count,
        physical_net_new_bytes_observed=physical_total,
        physical_max_bytes_per_unique_build=physical_max,
        logical_imports_observed=logical_count,
        logical_net_new_bytes_observed=logical_total,
        logical_max_bytes_per_import=logical_max,
        current_used_bytes=_integer(obj, "current_used_bytes", path, minimum=0),
        capacity_bytes=_integer(obj, "capacity_bytes", path, minimum=1),
        reserve_bytes=_integer(obj, "reserve_bytes", path, minimum=0),
        physical_retention_months=_decimal(
            obj,
            "physical_retention_months",
            path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
        logical_retention_months=_decimal(
            obj,
            "logical_retention_months",
            path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
    )


def _validate_gc(root: Mapping[str, Any]) -> GcEvidence:
    path = "gc"
    fields = (
        "window_hours",
        "cycles_observed",
        "executed_cycles",
        "overlap_count",
        "reference_recheck_failures",
        "starting_backlog_bytes",
        "newly_eligible_bytes",
        "deleted_bytes",
        "ending_backlog_bytes",
        "max_backlog_bytes",
        "max_clearance_hours",
    )
    obj = _object(root[path], path, fields)
    cycles = _integer(obj, "cycles_observed", path, minimum=1)
    executed = _integer(obj, "executed_cycles", path, minimum=0)
    _at_most(executed, cycles, f"{path}.executed_cycles")
    start = _integer(obj, "starting_backlog_bytes", path, minimum=0)
    eligible = _integer(obj, "newly_eligible_bytes", path, minimum=0)
    deleted = _integer(obj, "deleted_bytes", path, minimum=0)
    end = _integer(obj, "ending_backlog_bytes", path, minimum=0)
    if deleted > start + eligible or end != start + eligible - deleted:
        raise EvidenceError("inconsistent_evidence", f"{path}.ending_backlog_bytes")
    return GcEvidence(
        window_hours=_decimal(
            obj,
            "window_hours",
            path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
        cycles_observed=cycles,
        executed_cycles=executed,
        overlap_count=_integer(obj, "overlap_count", path, minimum=0),
        reference_recheck_failures=_integer(
            obj,
            "reference_recheck_failures",
            path,
            minimum=0,
        ),
        starting_backlog_bytes=start,
        newly_eligible_bytes=eligible,
        deleted_bytes=deleted,
        ending_backlog_bytes=end,
        max_backlog_bytes=_integer(obj, "max_backlog_bytes", path, minimum=0),
        max_clearance_hours=_decimal(
            obj,
            "max_clearance_hours",
            path,
            minimum=Decimal(0),
            minimum_inclusive=False,
        ),
    )


def _validate_api(root: Mapping[str, Any]) -> ApiEvidence:
    path = "api"
    fields = (
        "concurrent_unique_builds",
        "fresh_processes",
        "requests",
        "requests_while_imports_running",
        "errors",
        "cold_first_page_samples",
        "distinct_query_keys",
        "cold_first_page_p95_ms",
    )
    obj = _object(root[path], path, fields)
    requests = _integer(obj, "requests", path, minimum=1)
    overlap = _integer(obj, "requests_while_imports_running", path, minimum=0)
    errors = _integer(obj, "errors", path, minimum=0)
    samples = _integer(obj, "cold_first_page_samples", path, minimum=0)
    distinct = _integer(obj, "distinct_query_keys", path, minimum=0)
    for name, value in (
        ("requests_while_imports_running", overlap),
        ("errors", errors),
        ("cold_first_page_samples", samples),
    ):
        _at_most(value, requests, f"{path}.{name}")
    _at_most(distinct, samples, f"{path}.distinct_query_keys")
    return ApiEvidence(
        concurrent_unique_builds=_integer(
            obj,
            "concurrent_unique_builds",
            path,
            minimum=1,
        ),
        fresh_processes=_boolean(obj, "fresh_processes", path),
        requests=requests,
        requests_while_imports_running=overlap,
        errors=errors,
        cold_first_page_samples=samples,
        distinct_query_keys=distinct,
        cold_first_page_p95_ms=_decimal(
            obj,
            "cold_first_page_p95_ms",
            path,
            minimum=Decimal(0),
        ),
    )


def validate_measurement(
    record: Any,
    *,
    target_override: int | None = None,
) -> CapacityEvidence:
    """Validate and normalize one aggregate measurement record."""

    root = _object(record, "", _ROOT_FIELDS, ("objective",))
    schema_version = _integer(root, "schema_version", "", minimum=1)
    if schema_version != SCHEMA_VERSION:
        raise EvidenceError("unsupported_schema_version", "schema_version")
    _fixed_string(root, "evidence_profile", "", EVIDENCE_PROFILE)

    objective_target = DEFAULT_TARGET_LOGICAL_IMPORTS_PER_MONTH
    if "objective" in root:
        objective = _object(
            root["objective"],
            "objective",
            ("target_logical_imports_per_month",),
        )
        objective_target = _integer(
            objective,
            "target_logical_imports_per_month",
            "objective",
            minimum=1,
        )
    if target_override is not None:
        if isinstance(target_override, bool) or not isinstance(target_override, int):
            raise EvidenceError("invalid_type", "target_override")
        if target_override < 1:
            raise EvidenceError("invalid_value", "target_override")
        objective_target = target_override

    evidence = CapacityEvidence(
        target_logical_imports_per_month=objective_target,
        unique_build=_validate_unique_build(root),
        reuse=_validate_reuse(root),
        retry=_validate_retry(root),
        lanes=_validate_lanes(root),
        peak_arrival=_validate_peak(root),
        scratch=_validate_scratch(root),
        postgresql=_validate_postgresql(root),
        storage=_validate_storage(root),
        gc=_validate_gc(root),
        api=_validate_api(root),
    )
    if evidence.scratch.configured_concurrent_unique_builds > evidence.lanes.count:
        raise EvidenceError(
            "inconsistent_evidence",
            "scratch.configured_concurrent_unique_builds",
        )
    return evidence


def _json_number(value: Decimal | int | None) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if value == value.to_integral_value():
        return int(value)
    return float(value.quantize(Decimal("0.000001")))


def _ceil_decimal(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_CEILING))


def _floor_decimal(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_FLOOR))


def _ceil_ratio(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


def _safe_fraction(numerator: Decimal | int, denominator: Decimal | int) -> Decimal | None:
    denominator = Decimal(denominator)
    if denominator <= 0:
        return None
    return Decimal(numerator) / denominator


def _rate_metrics(rate: PostgresRateEvidence) -> dict[str, Any]:
    total = (
        rate.import_bytes_per_second
        + rate.api_bytes_per_second
        + rate.other_bytes_per_second
    )
    headroom = rate.sustainable_bytes_per_second - total
    return {
        "combined_bytes_per_second": _json_number(total),
        "sustainable_bytes_per_second": _json_number(
            rate.sustainable_bytes_per_second
        ),
        "headroom_bytes_per_second": _json_number(headroom),
        "headroom_fraction": _json_number(
            headroom / rate.sustainable_bytes_per_second
        ),
        "utilization_fraction": _json_number(
            total / rate.sustainable_bytes_per_second
        ),
    }


def _gate(gate_id: str, passed: bool) -> dict[str, Any]:
    return {"id": gate_id, "passed": bool(passed)}


def evaluate_evidence(evidence: CapacityEvidence) -> dict[str, Any]:
    """Calculate all aggregate metrics and return a deterministic gate report."""

    target = evidence.target_logical_imports_per_month
    unique = evidence.unique_build
    reuse = evidence.reuse
    retry = evidence.retry
    lanes = evidence.lanes
    scratch = evidence.scratch
    active_lane_count = min(
        lanes.count,
        scratch.configured_concurrent_unique_builds,
    )

    qualified_reuse_hits = reuse.qualified_hits
    reuse_adjusted_unique_builds = _ceil_ratio(
        target * (reuse.observed_logical_imports - qualified_reuse_hits),
        reuse.observed_logical_imports,
    )
    retry_overhead_minutes = (
        retry.failed_attempt_worker_minutes / retry.successful_unique_builds
    )
    retry_adjusted_minutes = unique.max_minutes + retry_overhead_minutes
    worst_worker_hours = Decimal(target) * retry_adjusted_minutes / Decimal(60)
    reuse_worker_hours = (
        Decimal(reuse_adjusted_unique_builds)
        * retry_adjusted_minutes
        / Decimal(60)
    )
    available_lane_hours = (
        Decimal(active_lane_count) * MONTH_HOURS * lanes.availability_factor
    )
    worst_lane_utilization = worst_worker_hours / available_lane_hours
    reuse_lane_utilization = reuse_worker_hours / available_lane_hours

    peak = evidence.peak_arrival
    peak_service_minutes = (
        Decimal(active_lane_count)
        * lanes.availability_factor
        * (peak.window_minutes + peak.max_queue_delay_minutes)
    )
    peak_capacity = peak_service_minutes / retry_adjusted_minutes
    guaranteed_peak_capacity = _floor_decimal(peak_capacity)

    scratch_usable = (
        scratch.capacity_bytes - scratch.baseline_used_bytes - scratch.reserve_bytes
    )
    scratch_headroom = scratch_usable - scratch.measured_peak_incremental_bytes
    scratch_capacity_per_build = _floor_decimal(
        Decimal(scratch_usable) / scratch.configured_concurrent_unique_builds
    )

    postgres = evidence.postgresql
    connections = postgres.connections
    usable_connections = connections.max_connections - connections.reserved_connections
    peak_connections = (
        connections.peak_api_connections
        + connections.peak_import_connections
        + connections.peak_other_connections
    )
    connection_headroom = usable_connections - peak_connections
    connection_headroom_fraction = _safe_fraction(
        connection_headroom,
        usable_connections,
    )
    write_metrics = _rate_metrics(postgres.write)
    wal_metrics = _rate_metrics(postgres.wal)
    write_total = (
        postgres.write.import_bytes_per_second
        + postgres.write.api_bytes_per_second
        + postgres.write.other_bytes_per_second
    )
    wal_total = (
        postgres.wal.import_bytes_per_second
        + postgres.wal.api_bytes_per_second
        + postgres.wal.other_bytes_per_second
    )
    write_headroom_fraction = (
        postgres.write.sustainable_bytes_per_second - write_total
    ) / postgres.write.sustainable_bytes_per_second
    wal_headroom_fraction = (
        postgres.wal.sustainable_bytes_per_second - wal_total
    ) / postgres.wal.sustainable_bytes_per_second

    storage = evidence.storage
    monthly_physical_worst = target * storage.physical_max_bytes_per_unique_build
    monthly_physical_reuse = (
        reuse_adjusted_unique_builds
        * storage.physical_max_bytes_per_unique_build
    )
    monthly_logical = target * storage.logical_max_bytes_per_import
    retained_physical_worst = _ceil_decimal(
        Decimal(monthly_physical_worst) * storage.physical_retention_months
    )
    retained_physical_reuse = _ceil_decimal(
        Decimal(monthly_physical_reuse) * storage.physical_retention_months
    )
    retained_logical = _ceil_decimal(
        Decimal(monthly_logical) * storage.logical_retention_months
    )
    projected_storage_worst = (
        storage.current_used_bytes + retained_physical_worst + retained_logical
    )
    projected_storage_reuse = (
        storage.current_used_bytes + retained_physical_reuse + retained_logical
    )
    storage_headroom_worst = (
        storage.capacity_bytes - storage.reserve_bytes - projected_storage_worst
    )
    storage_headroom_reuse = (
        storage.capacity_bytes - storage.reserve_bytes - projected_storage_reuse
    )

    gc = evidence.gc
    gc_delete_rate = Decimal(gc.deleted_bytes) / gc.window_hours
    gc_eligible_rate = Decimal(gc.newly_eligible_bytes) / gc.window_hours
    gc_net_drain_rate = gc_delete_rate - gc_eligible_rate
    if gc.ending_backlog_bytes == 0:
        gc_clearance_hours: Decimal | None = Decimal(0)
    elif gc_net_drain_rate > 0:
        gc_clearance_hours = Decimal(gc.ending_backlog_bytes) / gc_net_drain_rate
    else:
        gc_clearance_hours = None
    monthly_gc_growth = _ceil_decimal(
        max(Decimal(0), gc_eligible_rate - gc_delete_rate) * MONTH_HOURS
    )
    projected_gc_backlog = gc.ending_backlog_bytes + monthly_gc_growth

    api = evidence.api
    api_error_rate = Decimal(api.errors) / api.requests

    qualifying_unique_builds = min(
        unique.fresh_fingerprint_builds,
        unique.complete_source_coverage_builds,
        unique.durable_publication_builds,
        unique.persisted_audit_builds,
        unique.release_scanner_builds,
    )
    gates = [
        _gate(
            "logical_import_target",
            target >= MIN_RELEASE_TARGET_LOGICAL_IMPORTS_PER_MONTH,
        ),
        _gate("unique_build_duration", unique.max_minutes <= MAX_UNIQUE_BUILD_MINUTES),
        _gate(
            "unique_build_qualifying_evidence",
            qualifying_unique_builds == unique.sample_count,
        ),
        _gate(
            "reuse_complete_fingerprint_evidence",
            qualified_reuse_hits == reuse.observed_reuse_hits,
        ),
        _gate(
            "worst_case_lane_utilization",
            worst_lane_utilization <= MAX_LANE_UTILIZATION,
        ),
        _gate(
            "worst_case_peak_arrival",
            peak.observed_peak_logical_imports <= guaranteed_peak_capacity,
        ),
        _gate(
            "scratch_concurrency_coverage",
            scratch.measured_concurrent_unique_builds
            >= scratch.configured_concurrent_unique_builds,
        ),
        _gate("scratch_capacity", scratch_headroom >= 0),
        _gate("scratch_cleanup", scratch.cleanup_failures == 0),
        _gate(
            "postgres_load_coverage",
            postgres.load.concurrent_unique_builds
            >= scratch.configured_concurrent_unique_builds,
        ),
        _gate(
            "postgres_connection_headroom",
            connection_headroom >= connections.required_headroom_connections
            and connection_headroom_fraction is not None
            and connection_headroom_fraction >= MIN_POSTGRES_HEADROOM_FRACTION,
        ),
        _gate(
            "postgres_write_headroom",
            write_headroom_fraction >= MIN_POSTGRES_HEADROOM_FRACTION,
        ),
        _gate(
            "postgres_wal_headroom",
            wal_headroom_fraction >= MIN_POSTGRES_HEADROOM_FRACTION,
        ),
        _gate("storage_retention_capacity", storage_headroom_worst >= 0),
        _gate(
            "gc_execution",
            gc.executed_cycles == gc.cycles_observed
            and gc.overlap_count == 0
            and gc.reference_recheck_failures == 0,
        ),
        _gate("gc_throughput", gc_delete_rate >= gc_eligible_rate),
        _gate(
            "gc_backlog",
            gc.ending_backlog_bytes <= gc.max_backlog_bytes
            and projected_gc_backlog <= gc.max_backlog_bytes
            and gc_clearance_hours is not None
            and gc_clearance_hours <= gc.max_clearance_hours,
        ),
        _gate(
            "api_import_overlap",
            api.requests_while_imports_running == api.requests
            and api.concurrent_unique_builds
            >= scratch.configured_concurrent_unique_builds,
        ),
        _gate(
            "api_measurement_volume",
            api.requests >= MIN_API_REQUESTS
            and api.cold_first_page_samples >= MIN_API_COLD_FIRST_PAGE_SAMPLES,
        ),
        _gate(
            "api_cold_sample_coverage",
            api.cold_first_page_samples == api.requests - api.errors
            and api.distinct_query_keys > 0,
        ),
        _gate("api_fresh_processes", api.fresh_processes),
        _gate(
            "api_cold_first_page_p95",
            api.cold_first_page_p95_ms <= MAX_COLD_FIRST_PAGE_P95_MS,
        ),
        _gate("api_error_rate", api_error_rate <= MAX_API_ERROR_RATE),
    ]
    failed_gate_ids = [gate["id"] for gate in gates if not gate["passed"]]
    status = "pass" if not failed_gate_ids else "fail"
    exit_code = EXIT_PASS if status == "pass" else EXIT_GATE_FAILURE

    return {
        "schema_version": SCHEMA_VERSION,
        "gate": {"name": "ptg2_v3_capacity", "version": SCRIPT_VERSION},
        "status": status,
        "release": status == "pass",
        "exit_code": exit_code,
        "objective": {
            "target_logical_imports_per_month": target,
            "month_days": MONTH_DAYS,
        },
        "limits": {
            "minimum_target_logical_imports_per_month": (
                MIN_RELEASE_TARGET_LOGICAL_IMPORTS_PER_MONTH
            ),
            "unique_build_max_minutes": _json_number(MAX_UNIQUE_BUILD_MINUTES),
            "lane_utilization_max_fraction": _json_number(MAX_LANE_UTILIZATION),
            "postgres_minimum_headroom_fraction": _json_number(
                MIN_POSTGRES_HEADROOM_FRACTION
            ),
            "api_cold_first_page_p95_max_ms": _json_number(
                MAX_COLD_FIRST_PAGE_P95_MS
            ),
            "api_error_rate_max_fraction": _json_number(MAX_API_ERROR_RATE),
            "api_minimum_requests": MIN_API_REQUESTS,
            "api_minimum_cold_first_page_samples": (
                MIN_API_COLD_FIRST_PAGE_SAMPLES
            ),
        },
        "metrics": {
            "unique_build": {
                "sample_count": unique.sample_count,
                "qualifying_builds": qualifying_unique_builds,
                "p95_minutes": _json_number(unique.p95_minutes),
                "worst_case_minutes": _json_number(unique.max_minutes),
            },
            "monthly_capacity": {
                "worst_case_unique_builds": target,
                "reuse_adjusted_unique_builds": reuse_adjusted_unique_builds,
                "observed_reuse_hits": reuse.observed_reuse_hits,
                "qualified_reuse_hits": qualified_reuse_hits,
                "qualified_reuse_fraction": _json_number(
                    Decimal(qualified_reuse_hits) / reuse.observed_logical_imports
                ),
                "retry_attempt_rate": _json_number(
                    Decimal(retry.failed_attempts)
                    / (retry.successful_unique_builds + retry.failed_attempts)
                ),
                "retry_overhead_minutes_per_unique_build": _json_number(
                    retry_overhead_minutes
                ),
                "retry_adjusted_minutes_per_unique_build": _json_number(
                    retry_adjusted_minutes
                ),
                "worst_case_worker_hours": _json_number(worst_worker_hours),
                "reuse_adjusted_worker_hours": _json_number(reuse_worker_hours),
                "available_lane_count": lanes.count,
                "configured_unique_build_lane_count": active_lane_count,
                "availability_factor": _json_number(lanes.availability_factor),
                "available_lane_hours": _json_number(available_lane_hours),
                "worst_case_lane_utilization_fraction": _json_number(
                    worst_lane_utilization
                ),
                "reuse_adjusted_lane_utilization_fraction": _json_number(
                    reuse_lane_utilization
                ),
            },
            "peak_arrival": {
                "sample_windows": peak.sample_windows,
                "window_minutes": _json_number(peak.window_minutes),
                "max_queue_delay_minutes": _json_number(
                    peak.max_queue_delay_minutes
                ),
                "observed_peak_logical_imports": peak.observed_peak_logical_imports,
                "observed_peak_unique_builds": peak.observed_peak_unique_builds,
                "calculated_unique_build_capacity": _json_number(peak_capacity),
                "guaranteed_unique_build_capacity": guaranteed_peak_capacity,
            },
            "scratch": {
                "configured_concurrent_unique_builds": (
                    scratch.configured_concurrent_unique_builds
                ),
                "measured_concurrent_unique_builds": (
                    scratch.measured_concurrent_unique_builds
                ),
                "usable_bytes": scratch_usable,
                "capacity_per_configured_build_bytes": scratch_capacity_per_build,
                "measured_peak_incremental_bytes": (
                    scratch.measured_peak_incremental_bytes
                ),
                "headroom_bytes": scratch_headroom,
                "cleanup_cycles": scratch.cleanup_cycles,
                "cleanup_failures": scratch.cleanup_failures,
            },
            "postgresql": {
                "load": {
                    "concurrent_unique_builds": postgres.load.concurrent_unique_builds,
                    "api_requests_per_second": _json_number(
                        postgres.load.api_requests_per_second
                    ),
                    "sample_seconds": _json_number(postgres.load.sample_seconds),
                },
                "connections": {
                    "usable_connections": usable_connections,
                    "peak_connections": peak_connections,
                    "headroom_connections": connection_headroom,
                    "required_headroom_connections": (
                        connections.required_headroom_connections
                    ),
                    "headroom_fraction": _json_number(
                        connection_headroom_fraction
                    ),
                },
                "write": write_metrics,
                "wal": wal_metrics,
            },
            "storage": {
                "monthly_physical_bytes_worst_case": monthly_physical_worst,
                "monthly_physical_bytes_reuse_adjusted": monthly_physical_reuse,
                "monthly_logical_bytes": monthly_logical,
                "physical_retention_months": _json_number(
                    storage.physical_retention_months
                ),
                "logical_retention_months": _json_number(
                    storage.logical_retention_months
                ),
                "projected_retained_bytes_worst_case": projected_storage_worst,
                "projected_retained_bytes_reuse_adjusted": projected_storage_reuse,
                "headroom_bytes_worst_case": storage_headroom_worst,
                "headroom_bytes_reuse_adjusted": storage_headroom_reuse,
            },
            "gc": {
                "cycles_observed": gc.cycles_observed,
                "executed_cycles": gc.executed_cycles,
                "eligible_bytes_per_hour": _json_number(gc_eligible_rate),
                "deleted_bytes_per_hour": _json_number(gc_delete_rate),
                "net_drain_bytes_per_hour": _json_number(gc_net_drain_rate),
                "ending_backlog_bytes": gc.ending_backlog_bytes,
                "projected_30_day_backlog_bytes": projected_gc_backlog,
                "max_backlog_bytes": gc.max_backlog_bytes,
                "clearance_hours": _json_number(gc_clearance_hours),
                "max_clearance_hours": _json_number(gc.max_clearance_hours),
            },
            "api": {
                "concurrent_unique_builds": api.concurrent_unique_builds,
                "requests": api.requests,
                "requests_while_imports_running": (
                    api.requests_while_imports_running
                ),
                "cold_first_page_samples": api.cold_first_page_samples,
                "distinct_query_keys": api.distinct_query_keys,
                "cold_first_page_p95_ms": _json_number(
                    api.cold_first_page_p95_ms
                ),
                "errors": api.errors,
                "error_rate_fraction": _json_number(api_error_rate),
                "fresh_processes": api.fresh_processes,
            },
        },
        "gates": gates,
        "failures": failed_gate_ids,
        "redaction": {
            "policy": "fixed_schema_aggregate_metrics_only",
            "input_values_echoed": False,
            "sensitive_identifiers_emitted": False,
        },
    }


def evaluate_measurement(
    record: Any,
    *,
    target_override: int | None = None,
) -> dict[str, Any]:
    """Validate and evaluate one JSON-compatible measurement record."""

    return evaluate_evidence(validate_measurement(record, target_override=target_override))


def invalid_report(exc: EvidenceError) -> dict[str, Any]:
    """Return a redacted fail-closed report for invalid measurement evidence."""

    error: dict[str, Any] = {"code": exc.code}
    if exc.field is not None:
        error["field"] = exc.field
    return {
        "schema_version": SCHEMA_VERSION,
        "gate": {"name": "ptg2_v3_capacity", "version": SCRIPT_VERSION},
        "status": "invalid",
        "release": False,
        "exit_code": EXIT_INVALID_EVIDENCE,
        "errors": [error],
        "redaction": {
            "policy": "fixed_schema_aggregate_metrics_only",
            "input_values_echoed": False,
            "sensitive_identifiers_emitted": False,
        },
    }


def example_measurement() -> dict[str, Any]:
    """Return a passing example containing aggregate evidence only."""

    gigabyte = 1_000_000_000
    return {
        "schema_version": SCHEMA_VERSION,
        "evidence_profile": EVIDENCE_PROFILE,
        "objective": {"target_logical_imports_per_month": 2_000},
        "unique_build": {
            "sample_count": 40,
            "p95_minutes": 9.5,
            "max_minutes": 10,
            "fresh_fingerprint_builds": 40,
            "complete_source_coverage_builds": 40,
            "durable_publication_builds": 40,
            "persisted_audit_builds": 40,
            "release_scanner_builds": 40,
        },
        "reuse": {
            "evidence_type": REUSE_EVIDENCE_TYPE,
            "observed_logical_imports": 1_000,
            "observed_unique_builds": 800,
            "observed_reuse_hits": 200,
            "complete_fingerprint_verified_hits": 200,
            "logical_publication_verified_hits": 200,
            "production_like_observed_hits": 200,
        },
        "retry": {
            "successful_unique_builds": 100,
            "failed_attempts": 5,
            "failed_attempt_worker_minutes": 25,
        },
        "lanes": {"count": 2, "availability_factor": 0.9},
        "peak_arrival": {
            "sample_windows": 720,
            "window_minutes": 60,
            "max_queue_delay_minutes": 60,
            "observed_peak_logical_imports": 8,
            "observed_peak_unique_builds": 7,
        },
        "scratch": {
            "configured_concurrent_unique_builds": 2,
            "measured_concurrent_unique_builds": 2,
            "capacity_bytes": 1_000 * gigabyte,
            "baseline_used_bytes": 100 * gigabyte,
            "reserve_bytes": 100 * gigabyte,
            "measured_peak_incremental_bytes": 300 * gigabyte,
            "cleanup_cycles": 40,
            "cleanup_failures": 0,
        },
        "postgresql": {
            "load": {
                "concurrent_unique_builds": 2,
                "api_requests_per_second": 100,
                "sample_seconds": 3_600,
            },
            "connections": {
                "max_connections": 200,
                "reserved_connections": 10,
                "peak_api_connections": 30,
                "peak_import_connections": 12,
                "peak_other_connections": 20,
                "required_headroom_connections": 50,
            },
            "write": {
                "import_bytes_per_second": 200_000_000,
                "api_bytes_per_second": 5_000_000,
                "other_bytes_per_second": 15_000_000,
                "sustainable_bytes_per_second": 500_000_000,
            },
            "wal": {
                "import_bytes_per_second": 100_000_000,
                "api_bytes_per_second": 2_000_000,
                "other_bytes_per_second": 18_000_000,
                "sustainable_bytes_per_second": 300_000_000,
            },
        },
        "storage": {
            "physical_unique_builds_observed": 40,
            "physical_net_new_bytes_observed": 720 * gigabyte,
            "physical_max_bytes_per_unique_build": 20 * gigabyte,
            "logical_imports_observed": 1_000,
            "logical_net_new_bytes_observed": 8 * gigabyte,
            "logical_max_bytes_per_import": 10_000_000,
            "current_used_bytes": 100_000 * gigabyte,
            "capacity_bytes": 1_000_000 * gigabyte,
            "reserve_bytes": 100_000 * gigabyte,
            "physical_retention_months": 3,
            "logical_retention_months": 12,
        },
        "gc": {
            "window_hours": 24,
            "cycles_observed": 24,
            "executed_cycles": 24,
            "overlap_count": 0,
            "reference_recheck_failures": 0,
            "starting_backlog_bytes": 100 * gigabyte,
            "newly_eligible_bytes": 400 * gigabyte,
            "deleted_bytes": 450 * gigabyte,
            "ending_backlog_bytes": 50 * gigabyte,
            "max_backlog_bytes": 500 * gigabyte,
            "max_clearance_hours": 48,
        },
        "api": {
            "concurrent_unique_builds": 2,
            "fresh_processes": True,
            "requests": 5_000,
            "requests_while_imports_running": 5_000,
            "errors": 0,
            "cold_first_page_samples": 5_000,
            "distinct_query_keys": 4_000,
            "cold_first_page_p95_ms": 32,
        },
    }


def _reject_constant(_value: str) -> None:
    raise ValueError("non_finite_number")


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateFieldError()
        result[key] = value
    return result


def parse_measurement_bytes(payload: bytes) -> Any:
    """Parse bounded UTF-8 JSON while rejecting duplicates and non-finite values."""

    if len(payload) > MAX_INPUT_BYTES:
        raise EvidenceError("input_too_large")
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise EvidenceError("invalid_encoding")
    try:
        return json.loads(
            text,
            parse_float=Decimal,
            parse_int=int,
            parse_constant=_reject_constant,
            object_pairs_hook=_strict_object,
        )
    except _DuplicateFieldError:
        raise EvidenceError("duplicate_field")
    except (ValueError, RecursionError):
        raise EvidenceError("invalid_json")


def load_measurement(path: str) -> Any:
    """Read one bounded measurement from a file or stdin and parse it strictly."""

    try:
        if path == "-":
            payload = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
        else:
            with Path(path).open("rb") as input_file:
                payload = input_file.read(MAX_INPUT_BYTES + 1)
    except OSError:
        raise EvidenceError("input_unreadable")
    return parse_measurement_bytes(payload)


def _serialized(value: Mapping[str, Any], *, pretty: bool) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        indent=2 if pretty else None,
        separators=None if pretty else (",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ) + "\n"


def _write_json(value: Mapping[str, Any], destination: str, *, pretty: bool) -> None:
    serialized = _serialized(value, pretty=pretty)
    if destination == "-":
        sys.stdout.write(serialized)
        return
    path = Path(destination)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(serialized, encoding="utf-8")
        os.replace(temporary, path)
    except OSError:
        with suppress(OSError):
            temporary.unlink()
        raise EvidenceError("output_unwritable")


class RedactedArgumentParser(argparse.ArgumentParser):
    """Suppress argparse messages that can echo sensitive argument values."""

    def error(self, _message: str) -> None:
        """Exit with a redacted invalid-arguments report instead of echoing input."""

        report = invalid_report(EvidenceError("invalid_arguments"))
        self.exit(EXIT_INVALID_EVIDENCE, _serialized(report, pretty=False))


def _positive_target(value: str) -> int:
    try:
        target = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("invalid")
    if target < 1:
        raise argparse.ArgumentTypeError("invalid")
    return target


def build_argument_parser() -> argparse.ArgumentParser:
    """Build the redacting command-line parser for the capacity gate."""

    parser = RedactedArgumentParser(
        description="Evaluate an aggregate PTG2 V3 capacity measurement record.",
    )
    parser.add_argument("measurement", nargs="?", help="JSON record path, or - for stdin")
    parser.add_argument("--input", dest="input_path", help="JSON record path, or - for stdin")
    parser.add_argument("--output", default="-", help="Report path, or - for stdout")
    parser.add_argument(
        "--target-logical-imports-per-month",
        type=_positive_target,
        default=None,
    )
    parser.add_argument(
        "--write-example",
        nargs="?",
        const="-",
        metavar="PATH",
        help="Write a redacted passing example to PATH, or stdout when omitted",
    )
    return parser


def run_cli(argv: Sequence[str] | None = None) -> int:
    """Execute the capacity gate CLI and return its documented exit code."""

    args = build_argument_parser().parse_args(argv)
    if args.write_example is not None:
        if args.measurement is not None or args.input_path is not None:
            report = invalid_report(EvidenceError("conflicting_arguments"))
            _write_json(report, args.output, pretty=False)
            return EXIT_INVALID_EVIDENCE
        try:
            _write_json(example_measurement(), args.write_example, pretty=True)
        except EvidenceError as exc:
            sys.stderr.write(_serialized(invalid_report(exc), pretty=False))
            return EXIT_INVALID_EVIDENCE
        return EXIT_PASS

    if (args.measurement is None) == (args.input_path is None):
        report = invalid_report(EvidenceError("measurement_input_required"))
        try:
            _write_json(report, args.output, pretty=False)
        except EvidenceError as exc:
            sys.stderr.write(_serialized(invalid_report(exc), pretty=False))
        return EXIT_INVALID_EVIDENCE

    input_path = args.input_path if args.input_path is not None else args.measurement
    try:
        record = load_measurement(input_path)
        report = evaluate_measurement(
            record,
            target_override=args.target_logical_imports_per_month,
        )
    except EvidenceError as exc:
        report = invalid_report(exc)

    try:
        _write_json(report, args.output, pretty=False)
    except EvidenceError as exc:
        sys.stderr.write(_serialized(invalid_report(exc), pretty=False))
        return EXIT_INVALID_EVIDENCE
    return int(report["exit_code"])


def main(argv: Sequence[str] | None = None) -> int:
    """Run the capacity-gate command-line entry point."""

    return run_cli(argv)


if __name__ == "__main__":
    raise SystemExit(main())
