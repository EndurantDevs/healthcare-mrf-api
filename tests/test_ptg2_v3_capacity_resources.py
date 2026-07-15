# See LICENSE.

from __future__ import annotations

import copy
import hashlib
from dataclasses import fields
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from scripts.validation import ptg2_v3_capacity_resources as resources


START = datetime(2026, 7, 13, 9, 0, tzinfo=UTC)
END = START + timedelta(minutes=30)
CONFIG_REVISION = hashlib.sha256(b"resource-config-v1").hexdigest()
WRITE_EPOCH = hashlib.sha256(b"write-epoch").hexdigest()
WAL_EPOCH = hashlib.sha256(b"wal-epoch").hexdigest()
CHECKPOINT_EPOCH = hashlib.sha256(b"checkpoint-epoch").hexdigest()
AUTOVACUUM_EPOCH = hashlib.sha256(b"autovacuum-epoch").hexdigest()


def _timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _counters(
    epoch_sha256: str, import_bytes: int, api_bytes: int, other_bytes: int
) -> dict[str, object]:
    return {
        "epoch_sha256": epoch_sha256,
        "total_bytes": import_bytes + api_bytes + other_bytes,
        "import_bytes": import_bytes,
        "api_bytes": api_bytes,
        "other_bytes": other_bytes,
    }


def _config(observed_at: datetime) -> dict[str, object]:
    return {
        "observed_at": _timestamp(observed_at),
        "config_revision_sha256": CONFIG_REVISION,
        "max_connections": 100,
        "reserved_connections": 10,
        "autovacuum_max_workers": 5,
        "write_enforced_limit_bytes_per_second": 1_000,
        "wal_enforced_limit_bytes_per_second": 2_000,
        "scratch_capacity_bytes": 10_000,
        "scratch_reserve_bytes": 1_000,
        "temp_capacity_bytes": 20_000,
        "temp_reserve_bytes": 2_000,
        "storage_capacity_bytes": 1_000_000,
        "storage_reserve_bytes": 100_000,
        "physical_retention_months": 2,
        "logical_retention_months": 3,
        "required_connection_headroom": 10,
        "required_autovacuum_headroom": 1,
        "gc_max_backlog_bytes": 100_000,
        "gc_max_clearance_hours": 48,
    }


def _interval(index: int) -> dict[str, object]:
    started_at = START + timedelta(seconds=index * resources.INTERVAL_SECONDS)
    ended_at = started_at + timedelta(seconds=resources.INTERVAL_SECONDS)
    step = index + 1
    return {
        "started_at": _timestamp(started_at),
        "ended_at": _timestamp(ended_at),
        "config_revision_sha256": CONFIG_REVISION,
        "scratch_peak_used_bytes": 1_500,
        "scratch_min_available_bytes": 8_500,
        "temp_peak_used_bytes": 2_500,
        "temp_min_available_bytes": 17_500,
        "write_counters": _counters(
            WRITE_EPOCH, step * 10, step * 5, step * 2
        ),
        "wal_counters": _counters(WAL_EPOCH, step * 20, step * 3, step),
        "connections": {
            "api_connections": 10,
            "import_connections": 20,
            "other_connections": 5,
        },
        "pool_wait": {
            "bucket_counts": [1, 0, 0, 0, 0, 0, 0],
            "overflow_count": 0,
            "max_ms": 0,
            "timeout_errors": 0,
        },
        "autovacuum": {"workers": 2, "oldest_pending_seconds": 120},
    }


def _gc_cycles() -> list[dict[str, object]]:
    cycles = []
    cycle_start = START - timedelta(hours=resources.GC_CYCLE_COUNT)
    backlog_bytes = 24_000
    backlog_layouts = 48
    for _ in range(resources.GC_CYCLE_COUNT):
        cycle_end = cycle_start + timedelta(hours=1)
        ending_bytes = backlog_bytes + 1_000 - 1_500
        ending_layouts = backlog_layouts + 2 - 3
        cycles.append(
            {
                "started_at": _timestamp(cycle_start),
                "ended_at": _timestamp(cycle_end),
                "config_revision_sha256": CONFIG_REVISION,
                "executed": True,
                "overlap_detected": False,
                "reference_recheck_failures": 0,
                "starting_backlog_bytes": backlog_bytes,
                "starting_backlog_layouts": backlog_layouts,
                "newly_eligible_bytes": 1_000,
                "eligible_layouts": 2,
                "deleted_bytes": 1_500,
                "deleted_layouts": 3,
                "ending_backlog_bytes": ending_bytes,
                "ending_backlog_layouts": ending_layouts,
            }
        )
        cycle_start = cycle_end
        backlog_bytes = ending_bytes
        backlog_layouts = ending_layouts
    return cycles


def _resource_baseline() -> dict[str, object]:
    return {
        "observed_at": _timestamp(START),
        "scratch_used_bytes": 1_000,
        "scratch_available_bytes": 9_000,
        "temp_used_bytes": 2_000,
        "temp_available_bytes": 18_000,
        "storage_used_bytes": 100_000,
        "storage_available_bytes": 900_000,
        "physical_layout_bytes": 50_000,
        "physical_layout_count": 50,
        "logical_mapping_bytes": 10_000,
        "logical_mapping_count": 100,
        "write_counters": _counters(WRITE_EPOCH, 0, 0, 0),
        "wal_counters": _counters(WAL_EPOCH, 0, 0, 0),
    }


def _maintenance_evidence(
    first_import_sha256: str, second_import_sha256: str
) -> dict[str, object]:
    """Build deterministic maintenance evidence for capacity-gate tests."""

    return {
        "scratch_cleanup_events": [
            {
                "import_id_sha256": first_import_sha256,
                "started_at": _timestamp(START + timedelta(minutes=5)),
                "ended_at": _timestamp(START + timedelta(minutes=5, seconds=10)),
                "outcome": "completed",
            },
            {
                "import_id_sha256": second_import_sha256,
                "started_at": _timestamp(START + timedelta(minutes=10)),
                "ended_at": _timestamp(START + timedelta(minutes=10, seconds=10)),
                "outcome": "completed",
            },
        ],
        "checkpoint_events": [
            {
                "started_at": _timestamp(START + timedelta(minutes=2)),
                "ended_at": _timestamp(START + timedelta(minutes=2, seconds=10)),
                "trigger": "scheduled",
                "write_seconds": 4,
                "sync_seconds": 1,
            },
            {
                "started_at": _timestamp(START + timedelta(minutes=20)),
                "ended_at": _timestamp(START + timedelta(minutes=20, seconds=10)),
                "trigger": "scheduled",
                "write_seconds": 3,
                "sync_seconds": 1,
            },
        ],
        "autovacuum_events": [
            {
                "relation_id_sha256": _digest("relation-1"),
                "started_at": _timestamp(START + timedelta(minutes=3)),
                "ended_at": _timestamp(START + timedelta(minutes=4)),
                "outcome": "completed",
            },
        ],
        "event_counters_start": {
            "observed_at": _timestamp(START),
            "checkpoint_epoch_sha256": CHECKPOINT_EPOCH,
            "checkpoint_completed": 100,
            "checkpoint_requested": 10,
            "autovacuum_epoch_sha256": AUTOVACUUM_EPOCH,
            "autovacuum_completed": 200,
            "autovacuum_cancelled": 5,
        },
        "event_counters_end": {
            "observed_at": _timestamp(END),
            "checkpoint_epoch_sha256": CHECKPOINT_EPOCH,
            "checkpoint_completed": 102,
            "checkpoint_requested": 10,
            "autovacuum_epoch_sha256": AUTOVACUUM_EPOCH,
            "autovacuum_completed": 201,
            "autovacuum_cancelled": 5,
        },
    }


def _storage_evidence(
    first_import_sha256: str,
    second_import_sha256: str,
    preexisting_layout_sha256: str,
) -> dict[str, object]:
    return {
        "storage_endpoint": {
            "measured_at": _timestamp(END),
            "used_bytes": 100_360,
            "available_bytes": 899_640,
            "physical_layout_bytes": 50_300,
            "physical_layout_count": 52,
            "logical_mapping_bytes": 10_060,
            "logical_mapping_count": 103,
        },
        "preexisting_layouts": [
            {"physical_layout_id_sha256": preexisting_layout_sha256}
        ],
        "storage_deltas": [
            {
                "import_id_sha256": first_import_sha256,
                "kind": "unique_build",
                "physical_layout_id_sha256": _digest("layout-1"),
                "physical_net_new_bytes": 100,
                "logical_net_new_bytes": 10,
            },
            {
                "import_id_sha256": second_import_sha256,
                "kind": "unique_build",
                "physical_layout_id_sha256": _digest("layout-2"),
                "physical_net_new_bytes": 200,
                "logical_net_new_bytes": 20,
            },
            {
                "import_id_sha256": _digest("import-3"),
                "kind": "reuse",
                "physical_layout_id_sha256": preexisting_layout_sha256,
                "physical_net_new_bytes": 0,
                "logical_net_new_bytes": 30,
            },
        ],
        "gc_cycles": _gc_cycles(),
    }


def _record() -> dict[str, object]:
    first_import_sha256 = _digest("import-1")
    second_import_sha256 = _digest("import-2")
    preexisting_layout_sha256 = _digest("layout-preexisting")
    return {
        "contention_run_id": _digest("contention-run"),
        "contention_started_at": _timestamp(START),
        "contention_ended_at": _timestamp(END),
        "config_start": _config(START),
        "config_end": _config(END),
        "baseline": _resource_baseline(),
        "intervals": [_interval(index) for index in range(360)],
        **_maintenance_evidence(first_import_sha256, second_import_sha256),
        **_storage_evidence(
            first_import_sha256,
            second_import_sha256,
            preexisting_layout_sha256,
        ),
    }


def _parse(record: dict[str, object]) -> resources.ParsedResourceTelemetry:
    return resources.parse_and_derive_resource_telemetry(record)


def _error(record: dict[str, object]) -> resources.ResourceTelemetryError:
    with pytest.raises(resources.ResourceTelemetryError) as caught:
        _parse(record)
    return caught.value


def test_valid_derivation_reproduces_resource_gate_inputs():
    derived_resources = _parse(_record())
    summary = derived_resources.summaries

    assert derived_resources.telemetry.contention_run_id == _digest("contention-run")
    assert summary.sample_seconds == Decimal(1_800)
    assert summary.scratch.baseline_used_bytes == 1_000
    assert summary.scratch.measured_peak_used_bytes == 1_500
    assert summary.scratch.measured_peak_incremental_bytes == 500
    assert summary.scratch.cleanup_cycles == 2
    assert summary.scratch.cleanup_failures == 0
    assert summary.postgresql.write.total_bytes_per_second == Decimal("3.4")
    assert summary.postgresql.write.import_bytes_per_second == Decimal(2)
    assert summary.postgresql.write.api_bytes_per_second == Decimal(1)
    assert summary.postgresql.write.other_bytes_per_second == Decimal("0.4")
    assert summary.postgresql.write.peak_interval_total_bytes_per_second == Decimal(
        "3.4"
    )
    assert summary.postgresql.wal.total_bytes_per_second == Decimal("4.8")
    assert summary.postgresql.pool_wait.observations == 360
    assert summary.postgresql.pool_wait.waited_acquisitions == 0
    assert summary.postgresql.checkpoint.completed == 2
    assert summary.postgresql.checkpoint.requested == 0
    assert summary.postgresql.autovacuum.completed_cycles == 1
    assert summary.postgresql.autovacuum.oldest_pending_minutes == Decimal(2)
    assert summary.storage.physical_unique_builds_observed == 2
    assert summary.storage.physical_net_new_bytes_observed == 300
    assert summary.storage.physical_max_bytes_per_unique_build == 200
    assert summary.storage.logical_imports_observed == 3
    assert summary.storage.logical_net_new_bytes_observed == 60
    assert summary.storage.logical_max_bytes_per_import == 30
    assert summary.gc.cycles_observed == 24
    assert summary.gc.newly_eligible_bytes == 24_000
    assert summary.gc.deleted_bytes == 36_000
    assert summary.gc.starting_backlog_layouts == 48
    assert summary.gc.eligible_layouts == 48
    assert summary.gc.deleted_layouts == 72
    assert summary.gc.ending_backlog_layouts == 24
    assert summary.gc.ending_backlog_bytes == 12_000
    assert summary.gc.net_drain_bytes_per_hour == Decimal(500)
    assert summary.gc.clearance_hours == Decimal(24)
    assert summary.gc.headroom_fraction == Decimal(1) / Decimal(3)


def test_only_atomic_parse_and_derive_is_public_and_direct_construction_fails():
    assert "parse_resource_telemetry" not in resources.__all__
    assert "derive_resource_summaries" not in resources.__all__
    assert not hasattr(resources, "parse_resource_telemetry")
    assert not hasattr(resources, "derive_resource_summaries")

    derived_resources = _parse(_record())
    telemetry, summaries = derived_resources
    assert telemetry is derived_resources.telemetry
    assert summaries is derived_resources.summaries
    constructor_field_values = {
        resource_field.name: getattr(telemetry, resource_field.name)
        for resource_field in fields(resources.ResourceTelemetry)
        if resource_field.name != "_validation_token"
    }
    with pytest.raises(resources.ResourceTelemetryError) as caught:
        resources.ResourceTelemetry(
            _validation_token=object(), **constructor_field_values
        )
    assert (caught.value.code, caught.value.field) == (
        "unvalidated_telemetry",
        "telemetry",
    )

    with pytest.raises(resources.ResourceTelemetryError) as parse_error:
        resources.parse_and_derive_resource_telemetry(telemetry)
    assert (parse_error.value.code, parse_error.value.field) == (
        "invalid_type",
        "root",
    )


@pytest.mark.parametrize("field", ("storage_deltas", "scratch_cleanup_events"))
def test_storage_and_cleanup_streams_must_be_nonempty(field: str):
    record = _record()
    record[field] = []

    error = _error(record)

    assert (error.code, error.field) == ("invalid_count", field)


def test_every_unique_build_has_exactly_one_terminal_cleanup():
    record = _record()
    duplicate = copy.deepcopy(record["scratch_cleanup_events"][0])
    duplicate["started_at"] = _timestamp(START + timedelta(minutes=6))
    duplicate["ended_at"] = _timestamp(
        START + timedelta(minutes=6, seconds=10)
    )
    record["scratch_cleanup_events"].append(duplicate)

    error = _error(record)

    assert (error.code, error.field) == (
        "duplicate_terminal_cleanup",
        "scratch_cleanup_events[2].import_id_sha256",
    )


def test_missing_unique_build_cleanup_is_rejected():
    record = _record()
    record["scratch_cleanup_events"].pop()

    error = _error(record)

    assert (error.code, error.field) == ("missing_join", "scratch_cleanup_events")


def test_cleanup_events_must_join_unique_build_storage_hashes():
    record = _record()
    record["scratch_cleanup_events"][0]["import_id_sha256"] = _digest("unknown")

    error = _error(record)

    assert (error.code, error.field) == (
        "missing_join",
        "scratch_cleanup_events[0].import_id_sha256",
    )


@pytest.mark.parametrize(
    "field",
    (
        "physical_layout_bytes",
        "physical_layout_count",
        "logical_mapping_bytes",
        "logical_mapping_count",
    ),
)
def test_storage_domains_require_exact_before_after_conservation(field: str):
    record = _record()
    record["storage_endpoint"][field] += 1

    error = _error(record)

    assert (error.code, error.field) == (
        "storage_conservation",
        f"storage_endpoint.{field}",
    )


def test_reuse_must_reference_proven_preexisting_or_same_run_layout():
    record = _record()
    record["storage_deltas"][2]["physical_layout_id_sha256"] = _digest(
        "unknown-layout"
    )

    error = _error(record)

    assert (error.code, error.field) == (
        "unknown_layout",
        "storage_deltas[2].physical_layout_id_sha256",
    )


def test_same_run_layout_is_a_valid_reuse_reference_without_preexisting_proof():
    record = _record()
    record["preexisting_layouts"] = []
    record["storage_deltas"][2]["physical_layout_id_sha256"] = record[
        "storage_deltas"
    ][0]["physical_layout_id_sha256"]

    result = _parse(record)

    assert result.summaries.storage.logical_imports_observed == 3


def test_preexisting_layout_proof_list_must_be_exactly_referenced():
    record = _record()
    record["preexisting_layouts"].append(
        {"physical_layout_id_sha256": _digest("unused-layout")}
    )

    error = _error(record)

    assert (error.code, error.field) == (
        "unused_layout_proof",
        "preexisting_layouts",
    )


def test_unique_build_layouts_cannot_alias_each_other():
    record = _record()
    record["storage_deltas"][1]["physical_layout_id_sha256"] = record[
        "storage_deltas"
    ][0]["physical_layout_id_sha256"]

    error = _error(record)

    assert (error.code, error.field) == (
        "duplicate_id",
        "storage_deltas[1].physical_layout_id_sha256",
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
