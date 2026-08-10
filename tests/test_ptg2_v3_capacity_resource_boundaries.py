# See LICENSE.

from __future__ import annotations

import copy

import hashlib

from dataclasses import fields

from datetime import UTC, datetime, timedelta

from decimal import Decimal

import pytest

from scripts.validation import ptg2_v3_capacity_resources as resources

from tests.test_ptg2_v3_capacity_resources import (
    AUTOVACUUM_EPOCH,
    CHECKPOINT_EPOCH,
    CONFIG_REVISION,
    END,
    START,
    WAL_EPOCH,
    WRITE_EPOCH,
    _config,
    _counters,
    _digest,
    _error,
    _gc_cycles,
    _interval,
    _maintenance_event_counters,
    _maintenance_evidence,
    _parse,
    _record,
    _resource_baseline,
    _storage_evidence,
    _timestamp,
)

def test_interval_and_gc_config_revisions_cover_the_full_window():
    interval_record = _record()
    interval_record["intervals"][10]["config_revision_sha256"] = _digest(
        "changed"
    )
    interval_error = _error(interval_record)
    assert (interval_error.code, interval_error.field) == (
        "config_changed",
        "intervals[10].config_revision_sha256",
    )

    gc_record = _record()
    gc_record["gc_cycles"][0]["config_revision_sha256"] = _digest("old")
    gc_error = _error(gc_record)
    assert (gc_error.code, gc_error.field) == (
        "config_changed",
        "gc_cycles[0].config_revision_sha256",
    )

def test_configuration_values_must_remain_stable_for_the_window():
    record = _record()
    record["config_end"]["wal_enforced_limit_bytes_per_second"] = 1_999

    error = _error(record)

    assert (error.code, error.field) == (
        "config_changed",
        "config_end.wal_enforced_limit_bytes_per_second",
    )

@pytest.mark.parametrize("counter_name", ("write_counters", "wal_counters"))
def test_counter_categories_must_sum_to_independent_total(counter_name: str):
    record = _record()
    record["intervals"][0][counter_name]["total_bytes"] += 1

    error = _error(record)

    assert (error.code, error.field) == (
        "counter_total_mismatch",
        f"intervals[0].{counter_name}.total_bytes",
    )

@pytest.mark.parametrize("counter_name", ("write_counters", "wal_counters"))
def test_counter_epochs_cannot_change(counter_name: str):
    record = _record()
    record["intervals"][5][counter_name]["epoch_sha256"] = _digest("new-epoch")

    error = _error(record)

    assert (error.code, error.field) == (
        "counter_epoch_changed",
        f"intervals[5].{counter_name}.epoch_sha256",
    )

@pytest.mark.parametrize(
    ("counter_name", "limit"), (("write_counters", 1_000), ("wal_counters", 2_000))
)
def test_every_interval_total_rate_is_enforced(counter_name: str, limit: int):
    record = _record()
    epoch = WRITE_EPOCH if counter_name == "write_counters" else WAL_EPOCH
    record["intervals"][0][counter_name] = _counters(
        epoch, limit * resources.INTERVAL_SECONDS + 1, 0, 0
    )

    error = _error(record)

    assert (error.code, error.field) == (
        "rate_limit_exceeded",
        f"intervals[0].{counter_name}.total_bytes",
    )

def test_interval_and_full_window_rates_accept_exact_enforced_limits():
    record = _record()
    for config_name in ("config_start", "config_end"):
        record[config_name]["write_enforced_limit_bytes_per_second"] = Decimal(
            "3.4"
        )
        record[config_name]["wal_enforced_limit_bytes_per_second"] = Decimal(
            "4.8"
        )

    summary = _parse(record).summaries

    assert summary.postgresql.write.total_bytes_per_second == Decimal("3.4")
    assert summary.postgresql.write.peak_interval_total_bytes_per_second == Decimal(
        "3.4"
    )
    assert summary.postgresql.wal.total_bytes_per_second == Decimal("4.8")
    assert summary.postgresql.wal.peak_interval_total_bytes_per_second == Decimal(
        "4.8"
    )

def test_cumulative_counter_regression_is_rejected():
    record = _record()
    counters = record["intervals"][10]["write_counters"]
    counters["import_bytes"] = 99
    counters["total_bytes"] = (
        counters["import_bytes"]
        + counters["api_bytes"]
        + counters["other_bytes"]
    )

    error = _error(record)

    assert (error.code, error.field) == (
        "counter_regression",
        "intervals[10].write_counters.import_bytes",
    )

def test_boundary_spanning_checkpoint_and_autovacuum_completions_are_valid():
    record = _record()
    record["checkpoint_events"][0]["started_at"] = _timestamp(
        START - timedelta(minutes=1)
    )
    record["autovacuum_events"][0]["started_at"] = _timestamp(
        START - timedelta(minutes=1)
    )

    summary = _parse(record).summaries

    assert summary.postgresql.checkpoint.completed == 2
    assert summary.postgresql.autovacuum.completed_cycles == 1

def test_checkpoint_event_rows_reconcile_trusted_cumulative_counters():
    record = _record()
    record["event_counters_end"]["checkpoint_completed"] += 1

    error = _error(record)

    assert (error.code, error.field) == (
        "event_counter_mismatch",
        "event_counters_end.checkpoint_completed",
    )

def test_autovacuum_event_rows_reconcile_trusted_cumulative_counters():
    record = _record()
    record["event_counters_end"]["autovacuum_completed"] += 1

    error = _error(record)

    assert (error.code, error.field) == (
        "event_counter_mismatch",
        "event_counters_end.autovacuum_completed",
    )

def test_event_counter_epochs_must_remain_stable():
    record = _record()
    record["event_counters_end"]["autovacuum_epoch_sha256"] = _digest(
        "new-autovacuum-epoch"
    )

    error = _error(record)

    assert (error.code, error.field) == (
        "counter_epoch_changed",
        "event_counters_end.autovacuum_epoch_sha256",
    )

def test_overlapping_checkpoints_are_rejected():
    record = _record()
    record["checkpoint_events"][1]["started_at"] = _timestamp(
        START + timedelta(minutes=2, seconds=5)
    )
    record["checkpoint_events"][1]["ended_at"] = _timestamp(
        START + timedelta(minutes=2, seconds=15)
    )

    error = _error(record)

    assert (error.code, error.field) == (
        "overlapping_checkpoint",
        "checkpoint_events[1].started_at",
    )

def test_autovacuum_event_concurrency_cannot_exceed_interval_peak():
    record = _record()
    for index in range(36, 48):
        record["intervals"][index]["autovacuum"]["workers"] = 0

    error = _error(record)

    assert (error.code, error.field) == (
        "autovacuum_concurrency_exceeded",
        "intervals[36].autovacuum.workers",
    )

def test_autovacuum_interval_peak_cannot_exceed_config():
    record = _record()
    record["intervals"][0]["autovacuum"]["workers"] = 6

    error = _error(record)

    assert (error.code, error.field) == (
        "invalid_value",
        "intervals[0].autovacuum.workers",
    )

def test_duplicate_event_rows_cannot_inflate_derived_counts():
    record = _record()
    record["checkpoint_events"].append(copy.deepcopy(record["checkpoint_events"][0]))

    error = _error(record)

    assert (error.code, error.field) == (
        "duplicate_event",
        "checkpoint_events[2]",
    )

def test_gc_byte_and_layout_conservation_are_exact():
    bytes_record = _record()
    bytes_record["gc_cycles"][4]["ending_backlog_bytes"] += 1
    bytes_error = _error(bytes_record)
    assert (bytes_error.code, bytes_error.field) == (
        "gc_conservation",
        "gc_cycles[4].ending_backlog_bytes",
    )

    layouts_record = _record()
    layouts_record["gc_cycles"][4]["ending_backlog_layouts"] += 1
    layouts_error = _error(layouts_record)
    assert (layouts_error.code, layouts_error.field) == (
        "gc_conservation",
        "gc_cycles[4].ending_backlog_layouts",
    )

def test_gc_backlog_byte_and_layout_continuity_are_exact():
    bytes_record = _record()
    bytes_row = bytes_record["gc_cycles"][5]
    bytes_row["starting_backlog_bytes"] += 1
    bytes_row["ending_backlog_bytes"] += 1
    bytes_error = _error(bytes_record)
    assert (bytes_error.code, bytes_error.field) == (
        "gc_discontinuity",
        "gc_cycles[5].starting_backlog_bytes",
    )

    layouts_record = _record()
    layouts_row = layouts_record["gc_cycles"][5]
    layouts_row["starting_backlog_layouts"] += 1
    layouts_row["ending_backlog_layouts"] += 1
    layouts_error = _error(layouts_record)
    assert (layouts_error.code, layouts_error.field) == (
        "gc_discontinuity",
        "gc_cycles[5].starting_backlog_layouts",
    )

def test_nonexecuted_gc_cycle_cannot_delete_bytes_or_layouts():
    record = _record()
    record["gc_cycles"][0]["executed"] = False

    error = _error(record)

    assert (error.code, error.field) == (
        "gc_not_executed",
        "gc_cycles[0].deleted_bytes",
    )

def test_gc_deletion_bytes_and_layouts_must_be_jointly_zero_or_nonzero():
    record = _record()
    record["gc_cycles"][0]["deleted_layouts"] = 0

    error = _error(record)

    assert (error.code, error.field) == (
        "gc_deletion_mismatch",
        "gc_cycles[0].deleted_layouts",
    )

def test_gc_deletion_can_drain_layouts_from_starting_backlog():
    record = _record()
    first = record["gc_cycles"][0]
    assert first["deleted_layouts"] > first["eligible_layouts"]

    result = _parse(record)

    assert result.summaries.gc.ending_backlog_layouts == 24

@pytest.mark.parametrize(
    ("baseline_used", "baseline_available", "peak_field", "expected_field"),
    (
        (1_600, 8_400, "scratch_peak_used_bytes", "scratch_peak_used_bytes"),
        (2_600, 17_400, "temp_peak_used_bytes", "temp_peak_used_bytes"),
    ),
)
def test_first_interval_must_cover_scratch_and_temp_baselines(
    baseline_used: int,
    baseline_available: int,
    peak_field: str,
    expected_field: str,
):
    record = _record()
    prefix = "scratch" if peak_field.startswith("scratch") else "temp"
    record["baseline"][f"{prefix}_used_bytes"] = baseline_used
    record["baseline"][f"{prefix}_available_bytes"] = baseline_available

    error = _error(record)

    assert (error.code, error.field) == (
        "baseline_not_covered",
        f"intervals[0].{expected_field}",
    )

@pytest.mark.parametrize(
    ("prefix", "baseline_available", "expected_field"),
    (
        ("scratch", 8_000, "scratch_min_available_bytes"),
        ("temp", 17_000, "temp_min_available_bytes"),
    ),
)
def test_first_interval_minimum_available_covers_baseline(
    prefix: str, baseline_available: int, expected_field: str
):
    record = _record()
    record["baseline"][f"{prefix}_available_bytes"] = baseline_available

    error = _error(record)

    assert (error.code, error.field) == (
        "baseline_not_covered",
        f"intervals[0].{expected_field}",
    )

def test_interval_gap_is_rejected():
    record = _record()
    record["intervals"][10]["started_at"] = _timestamp(
        START + timedelta(seconds=51)
    )

    error = _error(record)

    assert (error.code, error.field) == (
        "interval_gap",
        "intervals[10].started_at",
    )

def test_histogram_p95_uses_conservative_bucket_ceiling():
    record = _record()
    for interval in record["intervals"]:
        interval["pool_wait"].update(
            bucket_counts=[0, 0, 0, 0, 0, 0, 0],
            overflow_count=0,
            max_ms=0,
        )
    record["intervals"][0]["pool_wait"].update(
        bucket_counts=[0, 94, 0, 0, 0, 0, 0], max_ms=1
    )
    record["intervals"][1]["pool_wait"].update(
        bucket_counts=[0, 0, 0, 6, 0, 0, 0], max_ms=10
    )

    pool_wait = _parse(record).summaries.postgresql.pool_wait

    assert pool_wait.observations == 100
    assert pool_wait.waited_acquisitions == 100
    assert pool_wait.p95_ms == Decimal(10)
    assert pool_wait.max_ms == Decimal(10)

def test_connection_categories_come_from_same_peak_interval():
    record = _record()
    record["intervals"][0]["connections"] = {
        "api_connections": 80,
        "import_connections": 0,
        "other_connections": 0,
    }
    record["intervals"][1]["connections"] = {
        "api_connections": 20,
        "import_connections": 50,
        "other_connections": 20,
    }

    connections = _parse(record).summaries.postgresql.connections

    assert connections.peak_connections == 90
    assert connections.peak_api_connections == 20
    assert connections.peak_import_connections == 50
    assert connections.peak_other_connections == 20
    assert connections.peak_interval_started_at == START + timedelta(seconds=5)

@pytest.mark.parametrize(
    ("field", "limit"),
    (
        ("scratch_cleanup_events", resources.MAX_SCRATCH_CLEANUP_EVENTS),
        ("checkpoint_events", resources.MAX_CHECKPOINT_EVENTS),
        ("autovacuum_events", resources.MAX_AUTOVACUUM_EVENTS),
        ("preexisting_layouts", resources.MAX_PREEXISTING_LAYOUTS),
        ("storage_deltas", resources.MAX_STORAGE_DELTAS),
    ),
)
def test_all_raw_arrays_are_bounded(field: str, limit: int):
    record = _record()
    seed = record[field][0]
    record[field] = [copy.deepcopy(seed) for _ in range(limit + 1)]

    error = _error(record)

    assert (error.code, error.field) == ("sample_limit", field)

def test_exact_fields_utc_seconds_and_safe_errors():
    record = _record()
    record["collector_path"] = "/secret/path"

    error = _error(record)

    assert (error.code, error.field, str(error)) == (
        "unexpected_field",
        "root",
        "unexpected_field",
    )

    timestamp_record = _record()
    timestamp_record["contention_started_at"] = "2026-07-13T09:00:00.000Z"
    timestamp_error = _error(timestamp_record)
    assert (timestamp_error.code, timestamp_error.field) == (
        "invalid_timestamp",
        "contention_started_at",
    )
