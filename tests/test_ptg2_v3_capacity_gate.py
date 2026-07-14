# See LICENSE.

from __future__ import annotations

import copy
import hashlib
import hmac
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from scripts.validation import ptg2_v3_capacity_gate as gate


SCRIPT_PATH = Path(gate.__file__).resolve()
TEST_KEY = b"capacity-gate-test-key-material-32-bytes-minimum"
TEST_NOW = datetime(2026, 7, 13, 10, 10, tzinfo=UTC)
TEST_RELEASE_DIGEST = hashlib.sha256(b"release-image").hexdigest()
TEST_ENVIRONMENT_ID = hashlib.sha256(b"test-environment").hexdigest()
TEST_TRUST = gate.ReceiptTrust(
    key_id="capacity-test-key",
    hmac_key=TEST_KEY,
    release_digest=TEST_RELEASE_DIGEST,
    environment_id=TEST_ENVIRONMENT_ID,
    collector_id="capacity-collector",
    collector_version="3.0.0",
)


def _record():
    record = copy.deepcopy(gate.example_measurement())
    record["release_evidence"] = True
    return record


def _digest(label):
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _signed_record(record, *, receipt_updates=None, commitment_updates=None):
    signed = copy.deepcopy(record)
    commitments = {
        "logical_import_stage_samples": {
            "sha256": _digest("logical-import-stage-samples"),
            "sample_count": signed["end_to_end"]["sample_count"],
        },
        "peak_windows": {
            "sha256": gate.sample_set_sha256(signed["peak_arrival"]["windows"]),
            "sample_count": signed["peak_arrival"]["sample_windows"],
        },
    }
    for name, class_name in (
        ("cold_matched_positive_samples", "matched_positive"),
        ("cold_negative_samples", "negative"),
        ("cold_random_samples", "random"),
    ):
        class_evidence = signed["api"][class_name]
        commitments[name] = {
            "sha256": _digest(name),
            "sample_count": class_evidence["samples"],
            "distinct_query_keys": class_evidence["distinct_query_keys"],
            "contention_samples": class_evidence["contention_samples"],
        }
    if commitment_updates:
        for name, updates in commitment_updates.items():
            commitments[name].update(updates)
    receipt = {
        "version": gate.RECEIPT_VERSION,
        "purpose": gate.RECEIPT_PURPOSE,
        "algorithm": gate.RECEIPT_ALGORITHM,
        "key_id": TEST_TRUST.key_id,
        "collector_id": TEST_TRUST.collector_id,
        "collector_version": TEST_TRUST.collector_version,
        "release_digest": TEST_TRUST.release_digest,
        "environment_id": TEST_TRUST.environment_id,
        "issued_at": "2026-07-13T10:05:00Z",
        "expires_at": "2026-07-13T11:05:00Z",
        "observation_started_at": "2026-07-06T00:00:00Z",
        "observation_ended_at": "2026-07-13T10:00:00Z",
        "measurement_sha256": gate.measurement_sha256(signed),
        "commitments": commitments,
    }
    if receipt_updates:
        receipt.update(receipt_updates)
    receipt["signature"] = hmac.new(
        TEST_KEY,
        gate.receipt_signing_payload(signed, receipt),
        hashlib.sha256,
    ).hexdigest()
    signed["receipt"] = receipt
    return signed


def _evaluate(record, *, target_override=None):
    return gate.evaluate_measurement(
        _signed_record(record),
        target_override=target_override,
        trust=TEST_TRUST,
        now=TEST_NOW,
    )


def _gate_map(report):
    return {item["id"]: item["passed"] for item in report["gates"]}


def _without_retry(record):
    record["retry"]["failed_attempts"] = 0
    record["retry"]["failed_attempt_worker_minutes"] = 0
    return record


def _sync_end_to_end_samples(record):
    unique = record["unique_build"]["sample_count"]
    reuse = record["reuse"]["reuse_only_sample_count"]
    total = unique + reuse
    record["end_to_end"].update(
        {
            "sample_count": total,
            "unique_build_samples": unique,
            "reuse_samples": reuse,
            "complete_stage_samples": total,
            "within_15_minutes": total,
        }
    )
    record["candidate_audit"].update(
        {
            "sample_count": total,
            "successful_audits": total,
            "activations_with_http_floor": total,
            "observed_http_requests": total * 3_000,
            "observed_request_seconds": total * 100,
        }
    )
    return record


def _shrink_sample_population(record):
    record["unique_build"].update(
        {
            "sample_count": 1,
            "fresh_fingerprint_builds": 1,
            "complete_source_coverage_builds": 1,
            "durable_publication_builds": 1,
            "persisted_audit_builds": 1,
            "release_scanner_builds": 1,
            "representative_large_builds": 1,
        }
    )
    record["reuse"]["reuse_only_sample_count"] = 1
    record["scratch"]["cleanup_cycles"] = 2
    return _sync_end_to_end_samples(record)


def _replace_peak_windows(record, windows):
    peak = record["peak_arrival"]
    peak["windows"] = windows
    peak["sample_windows"] = len(windows)
    first = datetime.strptime(windows[0]["started_at"], gate._UTC_TIMESTAMP_FORMAT)
    last = datetime.strptime(windows[-1]["ended_at"], gate._UTC_TIMESTAMP_FORMAT)
    peak["observation_minutes"] = int((last - first).total_seconds() / 60)
    start = datetime.strptime(windows[0]["started_at"], gate._UTC_TIMESTAMP_FORMAT)
    end = datetime.strptime(windows[0]["ended_at"], gate._UTC_TIMESTAMP_FORMAT)
    peak["window_minutes"] = int((end - start).total_seconds() / 60)
    peak["observed_peak_logical_imports"] = max(
        window["logical_imports"] for window in windows
    )
    peak["observed_peak_unique_builds"] = max(
        window["unique_builds"] for window in windows
    )
    peak["max_queue_delay_minutes"] = max(
        window["max_queue_delay_minutes"] for window in windows
    )
    peak["queue_delay_p95_minutes"] = min(
        peak["queue_delay_p95_minutes"],
        peak["max_queue_delay_minutes"],
    )
    return record


def _set_contention(record, *, seconds, requests=None):
    api = record["api"]
    if requests is not None:
        api["requests"] = requests
        api["requests_while_imports_running"] = requests
    started_at = datetime(2026, 7, 13, 8, tzinfo=UTC)
    ended_at = started_at + timedelta(seconds=seconds)
    api["contention_seconds"] = seconds
    api["contention_started_at"] = started_at.strftime(gate._UTC_TIMESTAMP_FORMAT)
    api["contention_ended_at"] = ended_at.strftime(gate._UTC_TIMESTAMP_FORMAT)
    sample_started_at = started_at + timedelta(seconds=1)
    sample_ended_at = ended_at - timedelta(seconds=1)
    for class_name in ("matched_positive", "negative", "random"):
        api[class_name]["sample_started_at"] = sample_started_at.strftime(
            gate._UTC_TIMESTAMP_FORMAT
        )
        api[class_name]["sample_ended_at"] = sample_ended_at.strftime(
            gate._UTC_TIMESTAMP_FORMAT
        )
    load = record["postgresql"]["load"]
    load["sample_seconds"] = seconds
    load["api_requests_per_second"] = api["requests"] / seconds
    load["candidate_audit_requests_per_second"] = (
        api["candidate_audit_requests"] / seconds
    )
    record["postgresql"]["checkpoint"]["sample_seconds"] = seconds
    record["postgresql"]["autovacuum"]["sample_seconds"] = seconds
    return record


def _write_record(tmp_path, record, name="measurement.json"):
    path = tmp_path / name
    path.write_text(json.dumps(record), encoding="utf-8")
    return path


def _run_cli(*args, trust_env=None):
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH), *map(str, args)],
        check=False,
        capture_output=True,
        text=True,
        env=trust_env,
    )


def _trust_environment(tmp_path):
    key_path = tmp_path / "capacity-receipt.key"
    key_path.write_bytes(TEST_KEY)
    key_path.chmod(0o600)
    environment = os.environ.copy()
    environment.update(
        {
            "HLTHPRT_PTG2_CAPACITY_RECEIPT_KEY_FILE": str(key_path),
            "HLTHPRT_PTG2_CAPACITY_RECEIPT_KEY_ID": TEST_TRUST.key_id,
            "HLTHPRT_PTG2_CAPACITY_RELEASE_DIGEST": TEST_TRUST.release_digest,
            "HLTHPRT_PTG2_CAPACITY_ENVIRONMENT_ID": TEST_TRUST.environment_id,
            "HLTHPRT_PTG2_CAPACITY_COLLECTOR_ID": TEST_TRUST.collector_id,
            "HLTHPRT_PTG2_CAPACITY_COLLECTOR_VERSION": TEST_TRUST.collector_version,
        }
    )
    return environment


def _fresh_receipt_updates():
    issued_at = datetime.now(UTC).replace(microsecond=0)
    return {
        "issued_at": issued_at.strftime(gate._UTC_TIMESTAMP_FORMAT),
        "expires_at": (issued_at + timedelta(minutes=60)).strftime(
            gate._UTC_TIMESTAMP_FORMAT
        ),
        "observation_ended_at": (issued_at - timedelta(minutes=5)).strftime(
            gate._UTC_TIMESTAMP_FORMAT
        ),
    }


def test_passing_example_covers_all_capacity_dimensions():
    report = _evaluate(_record())

    assert report["status"] == "pass"
    assert report["release"] is True
    assert report["exit_code"] == gate.EXIT_PASS
    assert report["objective"] == {
        "target_logical_imports_per_month": 2_000,
        "month_days": 30,
    }
    assert report["failures"] == []
    assert all(_gate_map(report).values())

    metrics = report["metrics"]
    assert metrics["unique_build"]["sample_count"] == 40
    assert metrics["unique_build"]["qualifying_builds"] == 40
    assert metrics["end_to_end"]["p95_minutes"] == 12.35
    assert metrics["end_to_end"]["max_minutes"] == 14
    assert metrics["monthly_capacity"]["worst_case_unique_builds"] == 2_000
    assert metrics["monthly_capacity"]["reuse_adjusted_unique_builds"] == 1_600
    assert metrics["monthly_capacity"]["reuse_adjusted_reuse_hits"] == 400
    assert (
        metrics["monthly_capacity"]["retry_adjusted_minutes_per_unique_build"] == 8.25
    )
    assert metrics["monthly_capacity"]["worst_case_worker_hours"] == 275
    assert metrics["monthly_capacity"]["reuse_adjusted_worker_hours"] == 230
    assert metrics["candidate_audit"]["http_requests_per_month"] == 6_000_000
    assert metrics["candidate_audit"]["lane_utilization_fraction"] > 0
    assert metrics["scratch"]["headroom_fraction"] >= 0.2
    assert metrics["postgresql"]["connections"]["headroom_fraction"] >= 0.2
    assert metrics["postgresql"]["pool_wait"]["required_observations"] == 144_000
    assert metrics["postgresql"]["checkpoint"]["headroom_fraction"] >= 0.2
    assert metrics["postgresql"]["temp"]["headroom_fraction"] >= 0.2
    assert metrics["postgresql"]["autovacuum"]["headroom_fraction"] >= 0.2
    assert metrics["gc"]["headroom_fraction"] == 0.2
    assert metrics["api"]["requests_per_second"] == 10
    assert metrics["api"]["matched_positive"]["cold_first_page_p95_ms"] == 32
    assert metrics["api"]["negative"]["cold_first_page_p95_ms"] == 30
    assert metrics["api"]["random"]["cold_first_page_p95_ms"] == 35
    assert metrics["api"]["error_rate_fraction"] == 0


@pytest.mark.parametrize(
    ("minutes", "expected_hours"),
    [(10, 333.333333), (15, 500)],
)
def test_unreused_2000_month_arithmetic_at_10m_and_15m(minutes, expected_hours):
    record = _without_retry(_record())
    record["unique_build"]["p95_minutes"] = minutes
    record["unique_build"]["max_minutes"] = minutes

    report = gate.evaluate_measurement(record)

    monthly = report["metrics"]["monthly_capacity"]
    assert monthly["worst_case_unique_builds"] == 2_000
    assert monthly["worst_case_worker_hours"] == pytest.approx(expected_hours)
    assert _gate_map(report)["unique_build_duration"] is True


def test_one_lane_capacity_threshold():
    record = _without_retry(_record())
    record["unique_build"]["p95_minutes"] = 15
    record["unique_build"]["max_minutes"] = 15
    record["lanes"] = {"count": 1, "availability_factor": 1}
    record["scratch"]["configured_concurrent_unique_builds"] = 1
    record["peak_arrival"]["observed_peak_logical_imports"] = 1
    record["peak_arrival"]["observed_peak_unique_builds"] = 1

    report = gate.evaluate_measurement(record)

    assert report["metrics"]["monthly_capacity"][
        "worst_case_lane_utilization_fraction"
    ] == pytest.approx(0.694444)
    assert _gate_map(report)["worst_case_lane_utilization"] is True


def test_insufficient_lane_headroom_fails_with_explicit_availability_factor():
    record = _without_retry(_record())
    record["unique_build"]["p95_minutes"] = 15
    record["unique_build"]["max_minutes"] = 15
    record["lanes"] = {"count": 1, "availability_factor": 0.95}
    record["scratch"]["configured_concurrent_unique_builds"] = 1
    record["peak_arrival"]["observed_peak_logical_imports"] = 1
    record["peak_arrival"]["observed_peak_unique_builds"] = 1

    report = gate.evaluate_measurement(record)

    assert report["status"] == "fail"
    assert report["metrics"]["monthly_capacity"][
        "worst_case_lane_utilization_fraction"
    ] > 0.7
    assert _gate_map(report)["worst_case_lane_utilization"] is False


def test_one_build_cannot_pass_release_evidence_floor():
    record = _record()
    record["unique_build"].update(
        {
            "sample_count": 1,
            "fresh_fingerprint_builds": 1,
            "complete_source_coverage_builds": 1,
            "durable_publication_builds": 1,
            "persisted_audit_builds": 1,
            "release_scanner_builds": 1,
            "representative_large_builds": 1,
        }
    )
    record["scratch"]["cleanup_cycles"] = 1
    _sync_end_to_end_samples(record)

    report = _evaluate(record)

    assert report["status"] == "fail"
    assert _gate_map(report)["unique_build_sample_floor"] is False


def test_all_unique_builds_must_be_representative_and_qualifying():
    record = _record()
    record["unique_build"]["representative_large_builds"] = 39

    report = _evaluate(record)

    assert _gate_map(report)["unique_build_sample_floor"] is True
    assert _gate_map(report)["unique_build_qualifying_evidence"] is False


def test_15_minute_boundary_uses_committed_per_import_end_to_end_samples():
    record = _record()
    record["end_to_end"].update({"p95_minutes": 15, "max_minutes": 15})

    boundary = _evaluate(record)
    record["end_to_end"]["max_minutes"] = 15.001
    over = _evaluate(record)

    assert boundary["metrics"]["end_to_end"]["max_minutes"] == 15
    assert _gate_map(boundary)["logical_import_end_to_end_duration"] is True
    assert _gate_map(over)["logical_import_end_to_end_duration"] is False


def test_reuse_only_work_is_sampled_and_included_in_monthly_hours():
    record = _record()
    record["reuse"]["reuse_only_sample_count"] = 1
    _sync_end_to_end_samples(record)
    low_sample = _evaluate(record)

    record = _record()
    record["reuse"]["reuse_only_p95_minutes"] = 4
    record["reuse"]["reuse_only_max_minutes"] = 4
    slower = _evaluate(record)

    assert _gate_map(low_sample)["reuse_only_sample_floor"] is False
    assert slower["metrics"]["monthly_capacity"][
        "reuse_adjusted_worker_hours"
    ] == pytest.approx(246.666667)


def test_reuse_discount_requires_audited_activation_evidence():
    record = _record()
    record["reuse"]["audited_activation_verified_hits"] = 0

    report = _evaluate(record)

    monthly = report["metrics"]["monthly_capacity"]
    assert monthly["observed_reuse_hits"] == 200
    assert monthly["qualified_reuse_hits"] == 0
    assert monthly["reuse_adjusted_unique_builds"] == 2_000
    assert _gate_map(report)["reuse_complete_fingerprint_evidence"] is False


@pytest.mark.parametrize(
    ("mutation", "gate_id"),
    [
        (
            _shrink_sample_population,
            "candidate_audit_sample_floor",
        ),
        (
            lambda record: record["candidate_audit"].update(
                {"queue_age_max_minutes": 31}
            ),
            "candidate_audit_queue_slo",
        ),
        (
            lambda record: record["candidate_audit"].update(
                {"http_requests_per_activation": 2_999}
            ),
            "candidate_audit_http_cost",
        ),
        (
            lambda record: record["candidate_audit"].update(
                {
                    "successful_audits": 39,
                    "activations_with_http_floor": 39,
                    "errors": 1,
                }
            ),
            "candidate_audit_errors",
        ),
    ],
)
def test_candidate_audit_evidence_fails_closed(mutation, gate_id):
    record = _record()
    mutation(record)

    report = _evaluate(record)

    assert report["status"] == "fail"
    assert _gate_map(report)[gate_id] is False


def test_candidate_audit_lane_capacity_is_independent_of_build_lanes():
    record = _record()
    record["candidate_audit"]["availability_factor"] = 0.1

    report = _evaluate(record)

    assert _gate_map(report)["worst_case_lane_utilization"] is True
    assert _gate_map(report)["candidate_audit_lane_utilization"] is False


def test_retry_minutes_are_included_in_build_and_activation_capacity():
    record = _record()
    record["retry"].update(
        {
            "successful_unique_builds": 10,
            "failed_attempts": 2,
            "failed_attempt_worker_minutes": 30,
        }
    )

    report = _evaluate(record)

    monthly = report["metrics"]["monthly_capacity"]
    assert monthly["retry_overhead_minutes_per_unique_build"] == 3
    assert monthly["retry_adjusted_minutes_per_unique_build"] == 11
    assert monthly["worst_case_worker_hours"] == pytest.approx(366.666667)
    assert _gate_map(report)["logical_import_end_to_end_duration"] is True


def test_peak_arrival_evidence_must_span_seven_days():
    record = _record()
    _replace_peak_windows(record, record["peak_arrival"]["windows"][:1])

    report = _evaluate(record)

    assert _gate_map(report)["peak_arrival_evidence"] is False


def test_peak_summary_cannot_claim_more_windows_than_are_timestamped():
    record = _record()
    record["peak_arrival"]["sample_windows"] += 1

    with pytest.raises(gate.EvidenceError) as error:
        _evaluate(record)

    assert error.value.code == "inconsistent_evidence"
    assert error.value.field == "peak_arrival.sample_windows"


def test_peak_queue_delay_has_a_fixed_slo():
    record = _record()
    record["peak_arrival"].update(
        {"queue_delay_p95_minutes": 30.001, "max_queue_delay_minutes": 30.001}
    )
    for window in record["peak_arrival"]["windows"]:
        window["max_queue_delay_minutes"] = 30.001

    report = _evaluate(record)

    assert _gate_map(report)["import_queue_delay_slo"] is False


def test_peak_build_and_candidate_audit_demands_are_gated_separately():
    build_burst = _record()
    for window in build_burst["peak_arrival"]["windows"]:
        window.update(
            {"logical_imports": 30, "unique_builds": 30, "candidate_audits": 30}
        )
    _replace_peak_windows(build_burst, build_burst["peak_arrival"]["windows"])
    build_report = _evaluate(build_burst)

    audit_burst = _record()
    audit_burst["candidate_audit"]["lane_count"] = 1
    audit_burst["postgresql"]["load"]["concurrent_candidate_audits"] = 1
    audit_burst["api"]["concurrent_candidate_audits"] = 1
    for window in audit_burst["peak_arrival"]["windows"]:
        window.update(
            {"logical_imports": 30, "unique_builds": 1, "candidate_audits": 30}
        )
    _replace_peak_windows(audit_burst, audit_burst["peak_arrival"]["windows"])
    audit_report = _evaluate(audit_burst)

    assert _gate_map(build_report)["worst_case_peak_arrival"] is False
    assert _gate_map(build_report)["candidate_audit_peak_arrival"] is True
    assert _gate_map(audit_report)["candidate_audit_peak_arrival"] is False


@pytest.mark.parametrize(
    ("mutation", "gate_id"),
    [
        (
            lambda record: record["scratch"].update(
                {"measured_peak_incremental_bytes": 650_000_000_000}
            ),
            "scratch_capacity",
        ),
        (
            lambda record: record["scratch"].update({"cleanup_cycles": 1}),
            "scratch_cleanup",
        ),
        (
            lambda record: record["postgresql"]["pool_wait"].update(
                {"observations": 1, "waited_acquisitions": 1}
            ),
            "postgres_pool_wait_coverage",
        ),
        (
            lambda record: record["postgresql"]["pool_wait"].update({"p95_ms": 11}),
            "postgres_pool_wait_slo",
        ),
        (
            lambda record: record["postgresql"]["checkpoint"].update({"completed": 1}),
            "postgres_checkpoint_coverage",
        ),
        (
            lambda record: record["postgresql"]["checkpoint"].update(
                {"write_seconds": 3_030}
            ),
            "postgres_checkpoint_headroom",
        ),
        (
            lambda record: record["postgresql"]["temp"].update(
                {"measured_peak_incremental_bytes": 130_000_000_000}
            ),
            "postgres_temp_headroom",
        ),
        (
            lambda record: record["postgresql"]["autovacuum"].update(
                {"completed_cycles": 0}
            ),
            "postgres_autovacuum_coverage",
        ),
        (
            lambda record: record["postgresql"]["autovacuum"].update(
                {"peak_workers": 5}
            ),
            "postgres_autovacuum_headroom",
        ),
    ],
)
def test_resource_pressure_requires_coverage_and_20_percent_headroom(
    mutation,
    gate_id,
):
    record = _record()
    mutation(record)

    report = _evaluate(record)

    assert report["status"] == "fail"
    assert _gate_map(report)[gate_id] is False


@pytest.mark.parametrize(
    "updates",
    [
        {"sample_seconds": 1},
        {"api_requests_per_second": 0.001},
        {"candidate_audit_requests_per_second": 0.001},
        {"concurrent_candidate_audits": 1},
    ],
)
def test_tiny_or_incomplete_postgres_contention_cannot_pass(updates):
    record = _record()
    record["postgresql"]["load"].update(updates)

    report = _evaluate(record)

    assert _gate_map(report)["postgres_load_coverage"] is False


def test_gc_requires_a_real_window_layout_volume_and_headroom():
    tiny = _record()
    tiny["gc"].update(
        {
            "window_hours": 1,
            "cycles_observed": 1,
            "executed_cycles": 1,
            "eligible_layouts": 1,
            "deleted_layouts": 1,
        }
    )
    tiny_report = _evaluate(tiny)

    low_headroom = _record()
    low_headroom["gc"].update(
        {"deleted_bytes": 450_000_000_000, "ending_backlog_bytes": 50_000_000_000}
    )
    low_headroom_report = _evaluate(low_headroom)

    assert _gate_map(tiny_report)["gc_measurement_volume"] is False
    assert _gate_map(low_headroom_report)["gc_throughput"] is False


def test_gc_backlog_limit_fails_independently_of_headroom():
    record = _record()
    record["gc"].update(
        {
            "starting_backlog_bytes": 200_000_000_000,
            "ending_backlog_bytes": 100_000_000_000,
            "max_backlog_bytes": 50_000_000_000,
        }
    )

    report = _evaluate(record)

    assert _gate_map(report)["gc_throughput"] is True
    assert _gate_map(report)["gc_backlog"] is False


@pytest.mark.parametrize(
    ("class_name", "gate_id"),
    [
        ("matched_positive", "api_matched_positive_cold_p95"),
        ("negative", "api_negative_cold_p95"),
        ("random", "api_random_cold_p95"),
    ],
)
def test_cold_p95_is_gated_per_request_class(class_name, gate_id):
    record = _record()
    record["api"][class_name]["cold_first_page_p95_ms"] = 40.001

    report = _evaluate(record)

    assert _gate_map(report)[gate_id] is False
    other_latency_gates = {
        "api_matched_positive_cold_p95",
        "api_negative_cold_p95",
        "api_random_cold_p95",
    } - {gate_id}
    assert all(_gate_map(report)[item] for item in other_latency_gates)


def test_class_specific_40ms_boundaries_pass():
    record = _record()
    for class_name in ("matched_positive", "negative", "random"):
        record["api"][class_name]["cold_first_page_p95_ms"] = 40

    report = _evaluate(record)

    assert _gate_map(report)["api_matched_positive_cold_p95"] is True
    assert _gate_map(report)["api_negative_cold_p95"] is True
    assert _gate_map(report)["api_random_cold_p95"] is True


def test_one_distinct_matched_key_cannot_pass():
    record = _record()
    record["api"]["matched_positive"]["distinct_query_keys"] = 1

    report = _evaluate(record)

    assert _gate_map(report)["api_distinct_matched_keys"] is False


def test_api_requires_30_minutes_overlap_meaningful_rps_and_zero_errors():
    short = _record()
    _set_contention(short, seconds=1_799)
    short_report = _evaluate(short)

    slow = _record()
    _set_contention(slow, seconds=3_600, requests=3_500)
    slow_report = _evaluate(slow)

    error = _record()
    error["api"]["errors"] = 1
    error["api"]["matched_positive"]["errors"] = 1
    error_report = _evaluate(error)

    assert _gate_map(short_report)["api_import_overlap"] is False
    assert _gate_map(slow_report)["api_measurement_volume"] is False
    assert _gate_map(error_report)["api_error_rate"] is False


def test_api_class_sample_floors_are_independent():
    record = _record()
    record["api"]["negative"].update(
        {
            "samples": 1,
            "distinct_query_keys": 1,
            "contention_samples": 1,
            "contention_distinct_query_keys": 1,
        }
    )

    report = _evaluate(record)

    assert _gate_map(report)["api_cold_sample_coverage"] is False


def test_aggregate_api_p95_is_rejected_by_fixed_schema():
    record = _record()
    record["api"]["cold_first_page_p95_ms"] = 1

    with pytest.raises(gate.EvidenceError) as error:
        _evaluate(record)

    assert error.value.code == "unexpected_field"
    assert error.value.field == "api"


@pytest.mark.parametrize(
    ("gate_id", "updates"),
    [
        (
            "postgres_connection_headroom",
            [("postgresql", "connections", "max_connections", 120)],
        ),
        (
            "postgres_write_headroom",
            [("postgresql", "write", "sustainable_bytes_per_second", 250_000_000)],
        ),
        (
            "postgres_wal_headroom",
            [("postgresql", "wal", "sustainable_bytes_per_second", 145_000_000)],
        ),
        (
            "storage_retention_capacity",
            [("storage", "capacity_bytes", 300_000_000_000_000)],
        ),
        (
            "storage_measurement_volume",
            [("storage", "logical_imports_observed", 999)],
        ),
    ],
)
def test_database_and_storage_capacity_failures(gate_id, updates):
    record = _record()
    for update in updates:
        container = record
        for key in update[:-2]:
            container = container[key]
        container[update[-2]] = update[-1]

    report = _evaluate(record)

    assert report["status"] == "fail"
    assert _gate_map(report)[gate_id] is False


@pytest.mark.parametrize(
    ("mutate", "code", "field"),
    [
        (
            lambda record: record.pop("candidate_audit"),
            "missing_field",
            "candidate_audit",
        ),
        (
            lambda record: record["lanes"].update({"availability_factor": "0.9"}),
            "invalid_type",
            "lanes.availability_factor",
        ),
        (
            lambda record: record["reuse"].update({"observed_unique_builds": 799}),
            "inconsistent_evidence",
            "reuse.observed_reuse_hits",
        ),
        (
            lambda record: record["peak_arrival"].update({"observation_minutes": 1}),
            "inconsistent_evidence",
            "peak_arrival.observation_minutes",
        ),
        (
            lambda record: record["api"]["matched_positive"].update(
                {"distinct_query_keys": 501}
            ),
            "inconsistent_evidence",
            "api.matched_positive.distinct_query_keys",
        ),
    ],
)
def test_missing_invalid_and_inconsistent_evidence_fails_closed(mutate, code, field):
    record = _record()
    mutate(record)

    with pytest.raises(gate.EvidenceError) as error:
        _evaluate(record)

    report = gate.invalid_report(error.value)
    assert report["status"] == "invalid"
    assert report["exit_code"] == gate.EXIT_INVALID_EVIDENCE
    assert report["errors"] == [{"code": code, "field": field}]


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        (b'{"schema_version":2,"schema_version":2}', "duplicate_field"),
        (b'{"schema_version":NaN}', "invalid_json"),
        (b'{"schema_version":', "invalid_json"),
        (b"\xff", "invalid_encoding"),
    ],
)
def test_strict_json_parser_rejects_ambiguous_or_invalid_input(payload, code):
    with pytest.raises(gate.EvidenceError) as error:
        gate.parse_measurement_bytes(payload)

    assert error.value.code == code


def test_schema_v1_is_rejected_after_breaking_contract_upgrade():
    record = _record()
    record["schema_version"] = 1

    with pytest.raises(gate.EvidenceError) as error:
        _evaluate(record)

    assert error.value.code == "unsupported_schema_version"
    assert error.value.field == "schema_version"


def test_target_defaults_to_2000_and_lower_target_does_not_release():
    record = _record()
    record.pop("objective")
    default_report = _evaluate(record)
    lower_report = _evaluate(record, target_override=1_999)

    assert default_report["objective"]["target_logical_imports_per_month"] == 2_000
    assert default_report["status"] == "pass"
    assert lower_report["status"] == "fail"
    assert _gate_map(lower_report)["logical_import_target"] is False


def test_evaluation_is_deterministic():
    record = _record()
    assert _evaluate(record) == _evaluate(record)


def test_unsigned_and_generated_json_never_become_release_evidence():
    with pytest.raises(gate.EvidenceError) as unsigned_error:
        gate.evaluate_measurement(_record(), trust=TEST_TRUST, now=TEST_NOW)
    with pytest.raises(gate.EvidenceError) as example_error:
        gate.evaluate_measurement(
            gate.example_measurement(),
            trust=TEST_TRUST,
            now=TEST_NOW,
        )

    assert unsigned_error.value.code == "missing_field"
    assert unsigned_error.value.field == "receipt"
    assert example_error.value.code == "non_release_example"


def test_fabricated_or_tampered_receipts_fail_authentication():
    fabricated = _signed_record(_record())
    fabricated["receipt"]["signature"] = hmac.new(
        b"attacker-controlled-key-material-32-bytes",
        gate.receipt_signing_payload(fabricated, fabricated["receipt"]),
        hashlib.sha256,
    ).hexdigest()
    with pytest.raises(gate.EvidenceError) as fabricated_error:
        gate.evaluate_measurement(fabricated, trust=TEST_TRUST, now=TEST_NOW)

    tampered = _signed_record(_record())
    tampered["api"]["random"]["cold_first_page_p95_ms"] = 1
    with pytest.raises(gate.EvidenceError) as tampered_error:
        gate.evaluate_measurement(tampered, trust=TEST_TRUST, now=TEST_NOW)

    assert fabricated_error.value.code == "invalid_receipt_signature"
    assert tampered_error.value.code == "measurement_digest_mismatch"


@pytest.mark.parametrize(
    ("receipt_updates", "code"),
    [
        (
            {
                "issued_at": "2026-07-13T06:00:00Z",
                "expires_at": "2026-07-13T07:00:00Z",
                "observation_ended_at": "2026-07-13T05:55:00Z",
            },
            "expired_receipt",
        ),
        (
            {
                "issued_at": "2026-07-13T10:05:00Z",
                "expires_at": "2026-07-13T11:05:00Z",
                "observation_ended_at": "2026-07-13T09:00:00Z",
            },
            "stale_observation",
        ),
        (
            {
                "issued_at": "2026-07-13T10:20:00Z",
                "expires_at": "2026-07-13T11:20:00Z",
                "observation_ended_at": "2026-07-13T10:15:00Z",
            },
            "receipt_from_future",
        ),
    ],
)
def test_receipt_time_bounds_fail_closed(receipt_updates, code):
    record = _signed_record(_record(), receipt_updates=receipt_updates)

    with pytest.raises(gate.EvidenceError) as error:
        gate.evaluate_measurement(record, trust=TEST_TRUST, now=TEST_NOW)

    assert error.value.code == code


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("release_digest", hashlib.sha256(b"other-release").hexdigest()),
        ("environment_id", hashlib.sha256(b"other-environment").hexdigest()),
        ("collector_id", "different-collector"),
        ("collector_version", "9.9.9"),
    ],
)
def test_receipt_is_bound_to_release_environment_and_collector(field, value):
    record = _signed_record(_record(), receipt_updates={field: value})

    with pytest.raises(gate.EvidenceError) as error:
        gate.evaluate_measurement(record, trust=TEST_TRUST, now=TEST_NOW)

    assert error.value.code == "receipt_identity_mismatch"
    assert error.value.field == f"receipt.{field}"


def test_commitment_counts_and_peak_digest_are_reconciled():
    wrong_count = _signed_record(
        _record(),
        commitment_updates={
            "logical_import_stage_samples": {"sample_count": 79}
        },
    )
    with pytest.raises(gate.EvidenceError) as count_error:
        gate.evaluate_measurement(wrong_count, trust=TEST_TRUST, now=TEST_NOW)

    wrong_peak_digest = _signed_record(
        _record(),
        commitment_updates={"peak_windows": {"sha256": _digest("wrong-peak")}},
    )
    with pytest.raises(gate.EvidenceError) as digest_error:
        gate.evaluate_measurement(wrong_peak_digest, trust=TEST_TRUST, now=TEST_NOW)

    assert count_error.value.code == "commitment_count_mismatch"
    assert digest_error.value.code == "commitment_digest_mismatch"


def test_summed_aggregates_cannot_substitute_for_per_import_end_to_end_evidence():
    record = _record()
    record["end_to_end"]["complete_stage_samples"] = 79

    report = _evaluate(record)

    assert report["metrics"]["unique_build"]["worst_case_minutes"] == 8
    assert report["metrics"]["candidate_audit"]["duration_max_minutes"] == 4
    assert _gate_map(report)["logical_import_end_to_end_duration"] is False


@pytest.mark.parametrize(
    "updates",
    [
        {"observed_http_requests": 239_999},
        {"observed_request_seconds": 20_000},
    ],
)
def test_six_million_monthly_audit_projection_requires_observed_traffic(updates):
    record = _record()
    record["candidate_audit"].update(updates)

    report = _evaluate(record)

    assert report["metrics"]["candidate_audit"]["http_requests_per_month"] == 6_000_000
    assert _gate_map(report)["candidate_audit_http_observation"] is False


def test_contention_request_rates_must_equal_observed_counts_over_duration():
    record = _record()
    record["postgresql"]["load"]["candidate_audit_requests_per_second"] = 25

    report = _evaluate(record)

    assert report["metrics"]["api"]["candidate_audit_requests"] == 108_000
    assert report["metrics"]["api"]["candidate_audit_requests_per_second"] == 30
    assert _gate_map(report)["postgres_load_coverage"] is False


def test_peak_windows_must_be_non_overlapping():
    record = _record()
    record["peak_arrival"]["windows"][1]["started_at"] = "2026-07-06T00:30:00Z"
    record["peak_arrival"]["windows"][1]["ended_at"] = "2026-07-06T01:30:00Z"

    with pytest.raises(gate.EvidenceError) as error:
        _evaluate(record)

    assert error.value.code == "overlapping_windows"


@pytest.mark.parametrize(
    ("class_name", "gate_id"),
    [
        ("negative", "api_distinct_negative_keys"),
        ("random", "api_distinct_random_keys"),
    ],
)
def test_every_cold_class_requires_distinct_keys_during_contention(
    class_name,
    gate_id,
):
    record = _record()
    record["api"][class_name]["distinct_query_keys"] = 1
    record["api"][class_name]["contention_distinct_query_keys"] = 1

    report = _evaluate(record)

    assert _gate_map(report)[gate_id] is False


def test_cold_sample_windows_must_be_inside_contention():
    record = _record()
    record["api"]["random"]["sample_started_at"] = "2026-07-13T07:00:00Z"

    with pytest.raises(gate.EvidenceError) as error:
        _evaluate(record)

    assert error.value.code == "sample_outside_contention"


def test_cli_exit_codes_for_pass_gate_failure_and_invalid_evidence(tmp_path):
    receipt_updates = _fresh_receipt_updates()
    passing = _signed_record(_record(), receipt_updates=receipt_updates)
    failing = _record()
    failing["api"]["random"]["cold_first_page_p95_ms"] = 41
    failing = _signed_record(failing, receipt_updates=receipt_updates)
    invalid = _record()
    invalid.pop("gc")
    invalid = _signed_record(invalid, receipt_updates=receipt_updates)
    trust_env = _trust_environment(tmp_path)

    pass_result = _run_cli(
        _write_record(tmp_path, passing, "pass.json"), trust_env=trust_env
    )
    fail_result = _run_cli(
        _write_record(tmp_path, failing, "fail.json"), trust_env=trust_env
    )
    invalid_result = _run_cli(
        _write_record(tmp_path, invalid, "invalid.json"), trust_env=trust_env
    )

    assert pass_result.returncode == gate.EXIT_PASS
    assert json.loads(pass_result.stdout)["status"] == "pass"
    assert pass_result.stderr == ""
    assert fail_result.returncode == gate.EXIT_GATE_FAILURE
    assert json.loads(fail_result.stdout)["status"] == "fail"
    assert invalid_result.returncode == gate.EXIT_INVALID_EVIDENCE
    assert json.loads(invalid_result.stdout)["status"] == "invalid"


def test_cli_write_example_is_redacted_and_cannot_release(tmp_path):
    example_path = tmp_path / "example.json"

    result = _run_cli("--write-example", example_path)
    example = json.loads(example_path.read_text(encoding="utf-8"))
    with pytest.raises(gate.EvidenceError) as error:
        gate.evaluate_measurement(example, trust=TEST_TRUST, now=TEST_NOW)

    assert result.returncode == gate.EXIT_GATE_FAILURE
    assert result.stdout == ""
    assert result.stderr == ""
    assert example["release_evidence"] is False
    assert "receipt" not in example
    assert error.value.code == "non_release_example"
    serialized = json.dumps(example, sort_keys=True).lower()
    for forbidden in (
        "client",
        "company",
        "organization",
        "plan_id",
        "source_url",
        "target_id",
    ):
        assert forbidden not in serialized


def test_cli_never_echoes_unknown_sensitive_keys_values_or_input_path(tmp_path):
    marker = "forbidden-client-marker"
    record = _record()
    record[f"client_name_{marker}"] = marker
    record = _signed_record(record, receipt_updates=_fresh_receipt_updates())
    input_path = _write_record(tmp_path, record, f"{marker}.json")

    result = _run_cli(input_path, trust_env=_trust_environment(tmp_path))
    emitted = result.stdout + result.stderr
    report = json.loads(result.stdout)

    assert result.returncode == gate.EXIT_INVALID_EVIDENCE
    assert report["errors"] == [{"code": "unexpected_field", "field": "root"}]
    assert marker not in emitted
    assert str(input_path) not in emitted


def test_argparse_errors_are_redacted_json():
    marker = "forbidden-argument-marker"

    result = _run_cli("--not-a-real-option", marker)
    emitted = result.stdout + result.stderr
    report = json.loads(result.stderr)

    assert result.returncode == gate.EXIT_INVALID_EVIDENCE
    assert report["errors"] == [{"code": "invalid_arguments"}]
    assert marker not in emitted
