# See LICENSE.

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.validation import ptg2_v3_capacity_gate as gate


SCRIPT_PATH = Path(gate.__file__).resolve()


def _record():
    return copy.deepcopy(gate.example_measurement())


def _gate_map(report):
    return {item["id"]: item["passed"] for item in report["gates"]}


def _without_retry(record):
    record["retry"]["failed_attempts"] = 0
    record["retry"]["failed_attempt_worker_minutes"] = 0
    return record


def _write_record(tmp_path, record, name="measurement.json"):
    path = tmp_path / name
    path.write_text(json.dumps(record), encoding="utf-8")
    return path


def _run_cli(*args):
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH), *map(str, args)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_passing_example_covers_all_capacity_dimensions():
    report = gate.evaluate_measurement(_record())

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
    assert metrics["monthly_capacity"]["worst_case_unique_builds"] == 2_000
    assert metrics["monthly_capacity"]["reuse_adjusted_unique_builds"] == 1_600
    assert metrics["monthly_capacity"]["retry_adjusted_minutes_per_unique_build"] == 10.25
    assert metrics["monthly_capacity"]["worst_case_worker_hours"] == pytest.approx(
        341.666667
    )
    assert metrics["scratch"]["headroom_bytes"] > 0
    assert metrics["postgresql"]["connections"]["headroom_connections"] > 0
    assert metrics["postgresql"]["write"]["headroom_fraction"] > 0.2
    assert metrics["postgresql"]["wal"]["headroom_fraction"] > 0.2
    assert metrics["storage"]["headroom_bytes_worst_case"] > 0
    assert metrics["gc"]["deleted_bytes_per_hour"] > metrics["gc"][
        "eligible_bytes_per_hour"
    ]
    assert metrics["api"]["cold_first_page_p95_ms"] == 32
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


def test_one_lane_15m_is_below_70_percent_only_at_full_availability():
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


def test_unique_build_over_15_minutes_fails_hard_gate():
    record = _record()
    record["unique_build"]["p95_minutes"] = 15.5
    record["unique_build"]["max_minutes"] = 15.5

    report = gate.evaluate_measurement(record)

    assert report["status"] == "fail"
    assert _gate_map(report)["unique_build_duration"] is False


def test_reuse_discount_counts_only_fully_observed_verified_hits():
    record = _record()
    record["reuse"]["complete_fingerprint_verified_hits"] = 0
    record["reuse"]["logical_publication_verified_hits"] = 0
    record["reuse"]["production_like_observed_hits"] = 0

    report = gate.evaluate_measurement(record)

    monthly = report["metrics"]["monthly_capacity"]
    assert monthly["observed_reuse_hits"] == 200
    assert monthly["qualified_reuse_hits"] == 0
    assert monthly["reuse_adjusted_unique_builds"] == 2_000
    assert _gate_map(report)["reuse_complete_fingerprint_evidence"] is False
    assert report["status"] == "fail"


def test_retry_worker_minutes_are_included_in_hours_and_lane_utilization():
    record = _record()
    record["retry"]["successful_unique_builds"] = 10
    record["retry"]["failed_attempts"] = 2
    record["retry"]["failed_attempt_worker_minutes"] = 30

    report = gate.evaluate_measurement(record)

    monthly = report["metrics"]["monthly_capacity"]
    assert monthly["retry_overhead_minutes_per_unique_build"] == 3
    assert monthly["retry_adjusted_minutes_per_unique_build"] == 13
    assert monthly["worst_case_worker_hours"] == pytest.approx(433.333333)


def test_peak_arrival_burst_fails_even_when_monthly_capacity_passes():
    record = _record()
    record["peak_arrival"]["observed_peak_logical_imports"] = 30
    record["peak_arrival"]["observed_peak_unique_builds"] = 20

    report = gate.evaluate_measurement(record)

    assert _gate_map(report)["worst_case_lane_utilization"] is True
    assert report["metrics"]["peak_arrival"][
        "guaranteed_unique_build_capacity"
    ] < 30
    assert _gate_map(report)["worst_case_peak_arrival"] is False


@pytest.mark.parametrize(
    ("gate_id", "updates"),
    [
        (
            "scratch_capacity",
            [("scratch", "capacity_bytes", 499_999_999_999)],
        ),
        (
            "postgres_connection_headroom",
            [("postgresql", "connections", "max_connections", 120)],
        ),
        (
            "postgres_write_headroom",
            [
                (
                    "postgresql",
                    "write",
                    "sustainable_bytes_per_second",
                    250_000_000,
                )
            ],
        ),
        (
            "postgres_wal_headroom",
            [
                (
                    "postgresql",
                    "wal",
                    "sustainable_bytes_per_second",
                    145_000_000,
                )
            ],
        ),
        (
            "storage_retention_capacity",
            [("storage", "capacity_bytes", 300_000_000_000_000)],
        ),
    ],
)
def test_scratch_database_and_storage_failures(gate_id, updates):
    record = _record()
    for update in updates:
        container = record
        for key in update[:-2]:
            container = container[key]
        container[update[-2]] = update[-1]

    report = gate.evaluate_measurement(record)

    assert report["status"] == "fail"
    assert _gate_map(report)[gate_id] is False
    assert gate_id in report["failures"]


def test_scratch_measurement_must_cover_configured_concurrency():
    record = _record()
    record["scratch"]["measured_concurrent_unique_builds"] = 1

    report = gate.evaluate_measurement(record)

    assert _gate_map(report)["scratch_concurrency_coverage"] is False


def test_postgres_measurement_must_include_configured_concurrency():
    record = _record()
    record["postgresql"]["load"]["concurrent_unique_builds"] = 1

    report = gate.evaluate_measurement(record)

    assert _gate_map(report)["postgres_load_coverage"] is False


def test_gc_throughput_and_growing_backlog_fail():
    record = _record()
    record["gc"]["deleted_bytes"] = 350_000_000_000
    record["gc"]["ending_backlog_bytes"] = 150_000_000_000

    report = gate.evaluate_measurement(record)

    assert report["metrics"]["gc"]["net_drain_bytes_per_hour"] < 0
    assert _gate_map(report)["gc_throughput"] is False
    assert _gate_map(report)["gc_backlog"] is False


def test_gc_backlog_limit_fails_independently_of_throughput():
    record = _record()
    record["gc"]["max_backlog_bytes"] = 40_000_000_000

    report = gate.evaluate_measurement(record)

    assert _gate_map(report)["gc_throughput"] is True
    assert _gate_map(report)["gc_backlog"] is False


@pytest.mark.parametrize(
    ("mutations", "gate_id"),
    [
        ({"cold_first_page_p95_ms": 40.001}, "api_cold_first_page_p95"),
        ({"errors": 1, "cold_first_page_samples": 4_999}, "api_error_rate"),
        ({"requests_while_imports_running": 4_999}, "api_import_overlap"),
        ({"fresh_processes": False}, "api_fresh_processes"),
        ({"cold_first_page_samples": 4_999}, "api_cold_sample_coverage"),
    ],
)
def test_api_cold_latency_error_and_load_failures(mutations, gate_id):
    record = _record()
    record["api"].update(mutations)

    report = gate.evaluate_measurement(record)

    assert report["status"] == "fail"
    assert _gate_map(report)[gate_id] is False


def test_cold_first_page_40ms_boundary_passes():
    record = _record()
    record["api"]["cold_first_page_p95_ms"] = 40

    report = gate.evaluate_measurement(record)

    assert _gate_map(report)["api_cold_first_page_p95"] is True


def test_api_measurement_volume_must_meet_release_audit_floor():
    record = _record()
    record["api"].update(
        {
            "requests": 100,
            "requests_while_imports_running": 100,
            "errors": 0,
            "cold_first_page_samples": 100,
            "distinct_query_keys": 100,
        }
    )

    report = gate.evaluate_measurement(record)

    assert _gate_map(report)["api_measurement_volume"] is False


@pytest.mark.parametrize(
    ("mutate", "code", "field"),
    [
        (
            lambda record: record.pop("postgresql"),
            "missing_field",
            "postgresql",
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
            lambda record: record["storage"].update(
                {"physical_max_bytes_per_unique_build": 1}
            ),
            "inconsistent_evidence",
            "storage.physical_max_bytes_per_unique_build",
        ),
    ],
)
def test_missing_invalid_and_inconsistent_evidence_fails_closed(mutate, code, field):
    record = _record()
    mutate(record)

    with pytest.raises(gate.EvidenceError) as error:
        gate.evaluate_measurement(record)

    report = gate.invalid_report(error.value)
    assert report["status"] == "invalid"
    assert report["exit_code"] == gate.EXIT_INVALID_EVIDENCE
    assert report["errors"] == [{"code": code, "field": field}]


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        (b'{"schema_version":1,"schema_version":1}', "duplicate_field"),
        (b'{"schema_version":NaN}', "invalid_json"),
        (b'{"schema_version":', "invalid_json"),
        (b"\xff", "invalid_encoding"),
    ],
)
def test_strict_json_parser_rejects_ambiguous_or_invalid_input(payload, code):
    with pytest.raises(gate.EvidenceError) as error:
        gate.parse_measurement_bytes(payload)

    assert error.value.code == code


def test_target_defaults_to_2000_and_lower_target_does_not_release():
    record = _record()
    record.pop("objective")
    default_report = gate.evaluate_measurement(record)
    lower_report = gate.evaluate_measurement(record, target_override=1_999)

    assert default_report["objective"]["target_logical_imports_per_month"] == 2_000
    assert default_report["status"] == "pass"
    assert lower_report["status"] == "fail"
    assert _gate_map(lower_report)["logical_import_target"] is False


def test_evaluation_is_deterministic():
    record = _record()
    assert gate.evaluate_measurement(record) == gate.evaluate_measurement(record)


def test_cli_exit_codes_for_pass_gate_failure_and_invalid_evidence(tmp_path):
    passing = _record()
    failing = _record()
    failing["api"]["cold_first_page_p95_ms"] = 41
    invalid = _record()
    invalid.pop("gc")

    pass_result = _run_cli(_write_record(tmp_path, passing, "pass.json"))
    fail_result = _run_cli(_write_record(tmp_path, failing, "fail.json"))
    invalid_result = _run_cli(_write_record(tmp_path, invalid, "invalid.json"))

    assert pass_result.returncode == gate.EXIT_PASS
    assert json.loads(pass_result.stdout)["status"] == "pass"
    assert pass_result.stderr == ""
    assert fail_result.returncode == gate.EXIT_GATE_FAILURE
    assert json.loads(fail_result.stdout)["status"] == "fail"
    assert invalid_result.returncode == gate.EXIT_INVALID_EVIDENCE
    assert json.loads(invalid_result.stdout)["status"] == "invalid"


def test_cli_write_example_is_redacted_and_passes(tmp_path):
    example_path = tmp_path / "example.json"

    result = _run_cli("--write-example", example_path)
    example = json.loads(example_path.read_text(encoding="utf-8"))
    report = gate.evaluate_measurement(example)

    assert result.returncode == gate.EXIT_PASS
    assert result.stdout == ""
    assert result.stderr == ""
    assert report["status"] == "pass"
    serialized = json.dumps(example, sort_keys=True).lower()
    for forbidden in ("client", "company", "organization", "plan_id", "source_url"):
        assert forbidden not in serialized


def test_cli_never_echoes_unknown_sensitive_keys_values_or_input_path(tmp_path):
    marker = "forbidden-client-marker"
    record = _record()
    record[f"client_name_{marker}"] = marker
    input_path = _write_record(tmp_path, record, f"{marker}.json")

    result = _run_cli(input_path)
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
