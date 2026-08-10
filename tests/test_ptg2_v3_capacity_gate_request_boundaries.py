# See LICENSE.

from __future__ import annotations

import hashlib

import json

from dataclasses import replace

from datetime import UTC, datetime, timedelta

from decimal import Decimal

from types import SimpleNamespace

import pytest

from scripts.validation import ptg2_v3_capacity_gate as gate

from tests.test_ptg2_v3_capacity_gate import (
    ATTACKER_API_PRIVATE_KEY,
    ATTACKER_API_PUBLIC_KEY,
    TEST_API_PRIVATE_KEY,
    TEST_TRUST,
    _evaluate,
    _evaluate_signed,
    _evaluation_error,
    _gate_map,
    _record,
    _resign_api_row,
    _set_reuse_end_to_end_minutes,
    _signed_record,
)

from tests.test_ptg2_v3_capacity_gate_adversarial import (
    _duplicate_query,
    _http_distinctness_cases,
    _mutate_first_api_sample,
    _overlap_query,
)

def test_burst_plus_one_request_per_five_seconds_fails_bounded_rate():
    measurement = _record()
    contention_started_at = datetime(2026, 7, 13, 9, tzinfo=UTC)
    all_samples = [
        sample
        for class_name in ("matched_positive", "negative", "random")
        for sample in measurement["raw_samples"][f"http_{class_name}"]
    ]
    sparse_sample_count = 60 * 60 // 5
    burst_sample_count = len(all_samples) - sparse_sample_count
    for index, http_sample in enumerate(all_samples):
        if index < burst_sample_count:
            observation_offset = index % 5
        else:
            observation_offset = (index - burst_sample_count) * 5
        observed_at = contention_started_at + timedelta(seconds=observation_offset)
        timestamp_text = gate._timestamp_text(observed_at)
        http_sample.update(
            process_started_at=timestamp_text,
            server_received_at=timestamp_text,
            server_observed_at=timestamp_text,
            collector_received_at=timestamp_text,
        )
        _resign_api_row(http_sample)
    for class_name in ("matched_positive", "negative", "random"):
        class_samples = measurement["raw_samples"][f"http_{class_name}"]
        measurement["api"][class_name].update(
            sample_started_at=min(
                sample["server_received_at"] for sample in class_samples
            ),
            sample_ended_at=max(
                sample["server_observed_at"] for sample in class_samples
            ),
        )

    report = _evaluate(measurement)
    api_metrics = report["metrics"]["api"]
    rate_buckets = api_metrics["request_rate_buckets"]
    gates = _gate_map(report)

    assert api_metrics["requests_per_second"] == 1
    assert api_metrics["http_observation_span"]["coverage_ratio"] >= Decimal(
        "0.99"
    )
    assert api_metrics["http_observation_span"]["max_gap_seconds"] == 5
    assert rate_buckets["bucket_seconds"] == 5
    assert rate_buckets["minimum_observations"] == 1
    assert rate_buckets["minimum_requests_per_second"] == 0.2
    assert rate_buckets["underfilled_buckets"] == 719
    assert gates["api_continuous_contention_coverage"] is True
    assert gates["api_measurement_volume"] is True
    assert gates["api_sustained_request_rate"] is False

def test_short_http_burst_cannot_prove_sustained_contention_load():
    measurement = _record()
    contention_started_at = datetime(2026, 7, 13, 9, tzinfo=UTC)
    all_samples = [
        sample
        for class_name in ("matched_positive", "negative", "random")
        for sample in measurement["raw_samples"][f"http_{class_name}"]
    ]
    for index, http_sample in enumerate(all_samples):
        observed_at = contention_started_at + timedelta(
            seconds=index * 239 // max(1, len(all_samples) - 1)
        )
        timestamp_text = gate._timestamp_text(observed_at)
        http_sample.update(
            process_started_at=timestamp_text,
            server_received_at=timestamp_text,
            server_observed_at=timestamp_text,
            collector_received_at=timestamp_text,
        )
        _resign_api_row(http_sample)
    for class_name in ("matched_positive", "negative", "random"):
        class_samples = measurement["raw_samples"][f"http_{class_name}"]
        measurement["api"][class_name].update(
            sample_started_at=class_samples[0]["server_received_at"],
            sample_ended_at=class_samples[-1]["server_observed_at"],
        )

    report = _evaluate(measurement)

    assert report["metrics"]["api"]["contention_coverage_ratio"] < Decimal("0.99")
    assert report["metrics"]["api"]["contention_max_gap_seconds"] > 5
    assert _gate_map(report)["api_continuous_contention_coverage"] is False

def test_cold_samples_require_distinct_fresh_process_identity():
    stale_process = _record()
    stale_sample = stale_process["raw_samples"]["http_random"][-1]
    stale_sample["process_started_at"] = "2026-07-13T08:00:00Z"
    _resign_api_row(stale_sample)
    assert _evaluation_error(stale_process).code == "stale_process"

    reused_process = _record()
    process_samples = reused_process["raw_samples"]["http_random"]
    process_samples[1]["process_instance_digest"] = process_samples[0][
        "process_instance_digest"
    ]
    _resign_api_row(process_samples[1])
    assert _evaluation_error(reused_process).code == "reused_api_process"

def test_peak_capacity_uses_each_signed_window_composition():
    measurement = _record()
    _set_reuse_end_to_end_minutes(measurement, 15)
    measurement["lanes"]["availability_factor"] = Decimal("0.50")
    first_window = measurement["peak_arrival"]["windows"][0]
    first_window["unique_builds"] = 0
    for event in measurement["raw_samples"]["peak_import_events"]:
        if (
            first_window["started_at"]
            <= event["enqueued_at"]
            < first_window["ended_at"]
        ):
            event["kind"] = "reuse"

    report = _evaluate(measurement)
    peak_metrics = report["metrics"]["peak_arrival"]

    assert measurement["peak_arrival"]["observed_peak_unique_builds"] == 7
    assert peak_metrics["import_service_demand_minutes"] == 120
    assert peak_metrics["import_service_capacity_minutes"] == 90
    assert _gate_map(report)["worst_case_peak_arrival"] is False

def test_peak_window_counts_are_recomputed_from_signed_import_events():
    measurement = _record()
    measurement["peak_arrival"]["windows"][0]["logical_imports"] = 9
    measurement["peak_arrival"]["observed_peak_logical_imports"] = 9

    error = _evaluation_error(measurement)

    assert (error.code, error.field) == (
        "raw_aggregate_mismatch",
        "peak_arrival.windows.logical_imports",
    )

def test_peak_audit_arrivals_are_reconciled_independently_from_imports():
    measurement = _record()
    first_window = measurement["peak_arrival"]["windows"][0]
    second_window = measurement["peak_arrival"]["windows"][1]
    moved_events = measurement["raw_samples"]["peak_audit_events"][:8]
    for event_index, event in enumerate(moved_events):
        queued_at = datetime.strptime(
            second_window["started_at"], gate._UTC_TIMESTAMP_FORMAT
        ).replace(tzinfo=UTC) + timedelta(minutes=40 + event_index)
        event["queued_at"] = gate._timestamp_text(queued_at)
        event["started_at"] = gate._timestamp_text(queued_at + timedelta(minutes=5))
    first_window["candidate_audits"] = 0
    first_window["max_audit_queue_age_minutes"] = 0
    second_window["candidate_audits"] = 16
    measurement["peak_arrival"]["observed_peak_candidate_audits"] = 16

    report = _evaluate(measurement)

    assert report["metrics"]["peak_arrival"]["observed_peak_candidate_audits"] == 16
    assert (
        report["metrics"]["peak_arrival"]["candidate_audit_service_demand_minutes"]
        == 80
    )

def test_resource_measurements_are_bound_to_fresh_observation_windows():
    stale_storage = _record()
    stale_storage["resource_observation"][
        "storage_measured_at"
    ] = "2026-07-13T08:00:00Z"
    assert _evaluation_error(stale_storage).code == "sample_outside_contention"

    stale_gc = _record()
    stale_gc["resource_observation"]["gc_started_at"] = "2026-07-11T09:00:00Z"
    error = _evaluation_error(stale_gc)
    assert (error.code, error.field) == (
        "raw_aggregate_mismatch",
        "resource_observation.gc_started_at",
    )

    other_run = _record()
    other_run["resource_observation"]["contention_run_id"] = hashlib.sha256(
        b"other-resource-run"
    ).hexdigest()
    assert _evaluation_error(other_run).code == "raw_aggregate_mismatch"

def test_retry_cost_is_nonzero_and_includes_reuse_attempts():
    contradictory = _record()
    reuse_sample = next(
        lifecycle_sample
        for lifecycle_sample in contradictory["raw_samples"]["import_lifecycle"]
        if lifecycle_sample["kind"] == "reuse"
    )
    reuse_sample["failed_attempts"] = 1
    assert _evaluation_error(contradictory).code == "inconsistent_evidence"

    measured = _record()
    reuse_sample = next(
        lifecycle_sample
        for lifecycle_sample in measured["raw_samples"]["import_lifecycle"]
        if lifecycle_sample["kind"] == "reuse"
    )
    reuse_sample.update(failed_attempts=1, failed_attempt_worker_seconds=3_600)
    measured["retry"].update(
        failed_attempts=1,
        failed_attempt_worker_minutes=60,
    )
    report = _evaluate(measured)
    capacity = report["metrics"]["monthly_capacity"]

    assert capacity["retry_overhead_minutes_per_logical_import"] == 1
    assert capacity["retry_adjusted_minutes_per_reuse"] == 8

def test_unique_retry_cost_is_projected_only_across_unique_builds():
    measurement = _record()
    unique_sample = next(
        lifecycle_sample
        for lifecycle_sample in measurement["raw_samples"]["import_lifecycle"]
        if lifecycle_sample["kind"] == "unique_build"
    )
    unique_sample.update(
        failed_attempts=1,
        failed_attempt_worker_seconds=48_000,
    )
    measurement["retry"].update(
        failed_attempts=1,
        failed_attempt_worker_minutes=800,
    )

    report = _evaluate(measurement)
    capacity = report["metrics"]["monthly_capacity"]

    assert capacity["retry_overhead_minutes_per_unique_build"] > 26
    assert capacity["retry_overhead_minutes_per_reuse"] == 0
    assert _gate_map(report)["worst_case_lane_utilization"] is False

@pytest.mark.parametrize(
    ("field_path", "measured_value", "error_code"),
    (
        (("scratch", "measured_peak_incremental_bytes"), 650_000_000_000, "raw_aggregate_mismatch"),
        (("postgresql", "connections", "max_connections"), 120, "raw_aggregate_mismatch"),
        (("postgresql", "pool_wait", "p95_ms"), 11, "inconsistent_evidence"),
        (("storage", "capacity_bytes"), 300_000_000_000_000, "raw_aggregate_mismatch"),
    ),
)
def test_resource_aggregates_cannot_override_signed_samples(
    field_path, measured_value, error_code
):
    measurement = _record()
    nested_fields = measurement
    for field_name in field_path[:-1]:
        nested_fields = nested_fields[field_name]
    nested_fields[field_path[-1]] = measured_value

    assert _evaluation_error(measurement).code == error_code

def test_scratch_and_connection_capacity_gates_remain_fail_closed():
    scratch = _record()
    for interval in scratch["resource_telemetry"]["intervals"]:
        interval["scratch_peak_used_bytes"] = 750_000_000_000
        interval["scratch_min_available_bytes"] = 250_000_000_000
    scratch["scratch"]["measured_peak_incremental_bytes"] = 650_000_000_000
    assert _gate_map(_evaluate(scratch))["scratch_capacity"] is False

    connections = _record()
    for key in ("config_start", "config_end"):
        connections["resource_telemetry"][key]["max_connections"] = 120
    connections["postgresql"]["connections"]["max_connections"] = 120
    assert _gate_map(_evaluate(connections))["postgres_connection_headroom"] is False

def test_pool_wait_and_storage_capacity_gates_remain_fail_closed():
    pool_wait = _record()
    waited = 0
    for interval in pool_wait["resource_telemetry"]["intervals"]:
        histogram = interval["pool_wait"]
        moved = max(1, histogram["bucket_counts"][0] // 10)
        histogram["bucket_counts"][0] -= moved
        histogram["bucket_counts"][4] = moved
        histogram["max_ms"] = 11
        waited += moved
    pool_wait["postgresql"]["pool_wait"].update(
        waited_acquisitions=waited,
        p95_ms=11,
        max_ms=11,
    )
    assert _gate_map(_evaluate(pool_wait))["postgres_pool_wait_slo"] is False

    storage = _record()
    capacity_bytes = 300_000_000_000_000
    for key in ("config_start", "config_end"):
        storage["resource_telemetry"][key]["storage_capacity_bytes"] = capacity_bytes
    baseline = storage["resource_telemetry"]["baseline"]
    baseline["storage_available_bytes"] = (
        capacity_bytes - baseline["storage_used_bytes"]
    )
    endpoint = storage["resource_telemetry"]["storage_endpoint"]
    endpoint["available_bytes"] = capacity_bytes - endpoint["used_bytes"]
    storage["storage"]["capacity_bytes"] = capacity_bytes
    assert _gate_map(_evaluate(storage))["storage_retention_capacity"] is False

def test_monthly_target_and_objective_contracts_remain_fail_closed():
    with pytest.raises(gate.EvidenceError, match="target_override_mismatch"):
        _evaluate(_record(), target_override=1_999)
    assert _evaluate(_record(), target_override=2_000)["status"] == "pass"

    missing_objective = _record()
    missing_objective.pop("objective")
    error = _evaluation_error(missing_objective)
    assert (error.code, error.field) == ("missing_field", "objective")

def test_monthly_capacity_uses_explicit_30_day_contract():
    measurement = _record()
    measurement["lanes"]["availability_factor"] = Decimal("0.13")

    report = _evaluate(measurement)
    capacity = report["metrics"]["monthly_capacity"]
    worst_case_worker_hours = Decimal(2_000 * 4) / Decimal(60)
    contracted_utilization = worst_case_worker_hours / (
        Decimal(2 * 720) * Decimal("0.13")
    )

    assert report["objective"]["month_days"] == 30
    assert report["objective"]["month_hours"] == 720
    assert capacity["available_lane_hours"] == pytest.approx(187.2)
    assert contracted_utilization > gate.MAX_LANE_UTILIZATION
    assert _gate_map(report)["worst_case_lane_utilization"] is False

def test_canonical_numbers_are_bounded_before_receipt_verification():
    measurement = _record()
    measurement["lanes"]["availability_factor"] = Decimal("1e100000000")

    with pytest.raises(gate.EvidenceError, match="numeric_range_exceeded"):
        _evaluate(measurement)

def test_strict_parser_and_argument_errors_do_not_echo_caller_values(capsys):
    for input_bytes, error_code in (
        (b'{"schema_version":6,"schema_version":6}', "duplicate_field"),
        (b'{"schema_version":NaN}', "invalid_json"),
        (b"\xff", "invalid_encoding"),
    ):
        with pytest.raises(gate.EvidenceError) as caught:
            gate.parse_measurement_bytes(input_bytes)
        assert caught.value.code == error_code
    marker = "caller-secret-key-path"
    with pytest.raises(SystemExit) as caught:
        gate.build_argument_parser().parse_args(["--receipt-key-file", marker])
    emitted = capsys.readouterr().err
    assert caught.value.code == gate.EXIT_INVALID_EVIDENCE
    assert marker not in emitted
    assert json.loads(emitted)["errors"] == [{"code": "invalid_arguments"}]
