# See LICENSE.

from __future__ import annotations

import copy
import hashlib
import importlib
import inspect
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from scripts.validation import ptg2_v3_capacity_gate as gate


TEST_NOW = datetime(2026, 7, 13, 10, 10, tzinfo=UTC)
TEST_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
ATTACKER_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
TEST_API_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(65, 97)))
ATTACKER_API_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(97, 129)))
TEST_PUBLIC_KEY = TEST_PRIVATE_KEY.public_key().public_bytes(
    serialization.Encoding.Raw, serialization.PublicFormat.Raw
)
TEST_API_PUBLIC_KEY = TEST_API_PRIVATE_KEY.public_key().public_bytes(
    serialization.Encoding.Raw, serialization.PublicFormat.Raw
)
ATTACKER_API_PUBLIC_KEY = ATTACKER_API_PRIVATE_KEY.public_key().public_bytes(
    serialization.Encoding.Raw, serialization.PublicFormat.Raw
)
assert TEST_API_PUBLIC_KEY == gate.EXAMPLE_API_EVIDENCE_PUBLIC_KEY
TEST_TRUST = gate.ReceiptTrust(
    key_id="capacity-release-key",
    public_key=TEST_PUBLIC_KEY,
    api_evidence_key_id=gate.EXAMPLE_API_EVIDENCE_KEY_ID,
    api_evidence_public_key=TEST_API_PUBLIC_KEY,
    release_digest=gate.EXAMPLE_RELEASE_DIGEST,
    environment_id=gate.EXAMPLE_ENVIRONMENT_ID,
    collector_id="capacity-collector",
    collector_version=gate.SCRIPT_VERSION,
)
BASE_RECORD = gate.example_measurement()


def _record():
    measurement = copy.deepcopy(BASE_RECORD)
    measurement["release_evidence"] = True
    return measurement


def _resign_api_row(row, *, private_key=TEST_API_PRIVATE_KEY):
    payload = {name: row[name] for name in gate._API_SIGNED_PAYLOAD_FIELDS}
    row["api_evidence_signature"] = gate._base64url_encode(
        private_key.sign(gate._api_signature_message(payload))
    )


def _raw_sources(measurement):
    raw = measurement["raw_samples"]
    return {
        "import_lifecycle_samples": raw["import_lifecycle"],
        "audit_result_samples": raw["audit_results"],
        "peak_import_events": raw["peak_import_events"],
        "peak_audit_events": raw["peak_audit_events"],
        "peak_windows": measurement["peak_arrival"]["windows"],
        "resource_telemetry": measurement["resource_telemetry"],
        "cold_matched_positive_samples": raw["http_matched_positive"],
        "cold_negative_samples": raw["http_negative"],
        "cold_random_samples": raw["http_random"],
    }


def _commitments(measurement):
    commitment_by_name = {}
    for name, rows in _raw_sources(measurement).items():
        commitment_dict = {
            "sha256": gate.sample_set_sha256(rows),
            "sample_count": (
                len(rows["intervals"]) if name == "resource_telemetry" else len(rows)
            ),
        }
        if name.startswith("cold_"):
            commitment_dict.update(
                distinct_query_keys=len({row["semantic_query_digest"] for row in rows}),
                contention_samples=len(rows),
            )
        commitment_by_name[name] = commitment_dict
    return commitment_by_name


def _observation_end(measurement):
    values = [measurement["api"]["contention_ended_at"]]
    values += [
        row["activated_at"] for row in measurement["raw_samples"]["import_lifecycle"]
    ]
    values += [
        row["candidate_attested_at"]
        for row in measurement["raw_samples"]["audit_results"]
    ]
    return max(values)


def _signed_record(
    measurement,
    *,
    private_key=TEST_PRIVATE_KEY,
    receipt_updates=None,
    commitment_updates=None,
):
    signed = copy.deepcopy(measurement)
    signed.pop("receipt", None)
    commitment_by_name = _commitments(signed)
    for name, updates in (commitment_updates or {}).items():
        commitment_by_name[name].update(updates)
    receipt_dict = {
        "version": gate.RECEIPT_VERSION,
        "purpose": gate.RECEIPT_PURPOSE,
        "algorithm": gate.RECEIPT_ALGORITHM,
        "key_id": TEST_TRUST.key_id,
        "collector_id": TEST_TRUST.collector_id,
        "collector_version": TEST_TRUST.collector_version,
        "release_digest": TEST_TRUST.release_digest,
        "environment_id": TEST_TRUST.environment_id,
        "issued_at": "2026-07-13T10:08:00Z",
        "expires_at": "2026-07-13T11:08:00Z",
        "observation_started_at": signed["peak_arrival"]["windows"][0]["started_at"],
        "observation_ended_at": _observation_end(signed),
        "contention_run_id": signed["api"]["contention_run_id"],
        "measurement_sha256": gate.measurement_sha256(signed),
        "commitments": commitment_by_name,
    }
    receipt_dict.update(receipt_updates or {})
    receipt_dict["signature"] = private_key.sign(
        gate.receipt_signing_payload(signed, receipt_dict)
    ).hex()
    signed["receipt"] = receipt_dict
    return signed


def _evaluate(measurement, *, now=TEST_NOW, target_override=None):
    return gate._evaluate_measurement_with_trust(
        _signed_record(measurement),
        trust=TEST_TRUST,
        now=now,
        target_override=target_override,
    )


def _evaluate_signed(measurement, *, now=TEST_NOW):
    return gate._evaluate_measurement_with_trust(measurement, trust=TEST_TRUST, now=now)


def _evaluation_error(measurement, *, signed=False, now=TEST_NOW):
    with pytest.raises(gate.EvidenceError) as caught:
        if signed:
            _evaluate_signed(measurement, now=now)
        else:
            _evaluate(measurement, now=now)
    return caught.value


def _gate_map(report):
    return {entry["id"]: entry["passed"] for entry in report["gates"]}


def _set_unique_build_minutes(measurement, duration_minutes):
    delta = timedelta(minutes=duration_minutes - 4)

    def shifted(timestamp):
        value = datetime.fromisoformat(timestamp.replace("Z", "+00:00")) - delta
        return value.isoformat().replace("+00:00", "Z")

    for sample in measurement["raw_samples"]["import_lifecycle"]:
        if sample["kind"] != "unique_build":
            continue
        for field_name in ("enqueued_at", "build_started_at"):
            sample[field_name] = shifted(sample[field_name])

    measurement["unique_build"].update(
        p95_minutes=duration_minutes,
        max_minutes=duration_minutes,
    )
    complete_minutes = duration_minutes + 6
    measurement["end_to_end"].update(
        p95_minutes=complete_minutes,
        max_minutes=complete_minutes,
        within_15_minutes=(60 if complete_minutes <= 15 else 30),
    )
    contention_start = datetime.fromisoformat(
        measurement["api"]["contention_started_at"].replace("Z", "+00:00")
    )
    contention_end = datetime.fromisoformat(
        measurement["api"]["contention_ended_at"].replace("Z", "+00:00")
    )
    intervals = tuple(
        (
            datetime.fromisoformat(sample["build_started_at"].replace("Z", "+00:00")),
            datetime.fromisoformat(sample["build_completed_at"].replace("Z", "+00:00")),
        )
        for sample in measurement["raw_samples"]["import_lifecycle"]
        if sample["kind"] == "unique_build"
    )
    concurrent_builds = gate._maximum_concurrency(
        intervals, contention_start, contention_end
    )
    measurement["api"]["concurrent_unique_builds"] = concurrent_builds
    measurement["scratch"]["measured_concurrent_unique_builds"] = concurrent_builds
    measurement["postgresql"]["load"][
        "concurrent_unique_builds"
    ] = concurrent_builds


def _trust_config(trust=TEST_TRUST):
    return {
        "version": gate.TRUST_CONFIG_VERSION,
        "algorithm": gate.RECEIPT_ALGORITHM,
        "key_id": trust.key_id,
        "public_key_hex": trust.public_key.hex(),
        "api_evidence_key_id": trust.api_evidence_key_id,
        "api_evidence_public_key_hex": trust.api_evidence_public_key.hex(),
        "release_digest": trust.release_digest,
        "environment_id": trust.environment_id,
        "collector_id": trust.collector_id,
        "collector_version": trust.collector_version,
    }


def _install_trust(monkeypatch, tmp_path):
    path = tmp_path / "capacity-trust.json"
    path.write_text(json.dumps(_trust_config()), encoding="utf-8")
    path.chmod(0o600)
    monkeypatch.setattr(gate, "TRUST_CONFIG_PATH", path)
    monkeypatch.setattr(gate, "TRUST_CONFIG_OWNER_UID", os.geteuid())
    return path


def _sync_reduced_audit_workload(measurement):
    measurement["raw_samples"]["audit_results"][0].update(
        http_requests=2_999, http_successes=2_999
    )
    measurement["candidate_audit"].update(
        http_requests_per_activation=2_999,
        activations_with_http_floor=59,
        observed_http_requests=179_999,
    )
    measurement["api"].update(
        candidate_audit_requests=179_999,
        candidate_audit_requests_while_imports_running=179_999,
    )
    measurement["postgresql"]["load"]["candidate_audit_requests_per_second"] = Decimal(
        179_999
    ) / Decimal(3_600)


def _set_reuse_end_to_end_minutes(measurement, duration_minutes):
    for row in measurement["raw_samples"]["import_lifecycle"]:
        if row["kind"] != "reuse":
            continue
        activated_at = datetime.strptime(
            row["activated_at"], gate._UTC_TIMESTAMP_FORMAT
        ).replace(tzinfo=UTC)
        row["enqueued_at"] = gate._timestamp_text(
            activated_at - timedelta(minutes=duration_minutes)
        )
    measurement["reuse"].update(
        reuse_only_p95_minutes=duration_minutes,
        reuse_only_max_minutes=duration_minutes,
    )
    measurement["end_to_end"].update(
        p95_minutes=duration_minutes,
        max_minutes=duration_minutes,
    )


def test_passing_release_recomputes_capacity_from_signed_raw_rows():
    report = _evaluate(_record())
    metrics = report["metrics"]

    assert report["status"] == "pass" and report["failures"] == []
    assert all(_gate_map(report).values())
    assert report["objective"] == {
        "target_logical_imports_per_month": 2_000,
        "month_days": 30,
        "month_hours": 720,
    }
    assert metrics["monthly_capacity"]["worst_case_unique_builds"] == 2_000
    assert metrics["candidate_audit"]["http_requests_per_month"] == 6_000_000
    assert metrics["candidate_audit"]["observed_http_requests"] == 180_000
    assert metrics["candidate_audit"]["lane_utilization_fraction"] > 0
    assert metrics["end_to_end"]["max_minutes"] == 10
    assert metrics["peak_arrival"]["sample_windows"] == 168
    assert metrics["peak_arrival"]["coverage_ratio"] == 1
    assert metrics["scratch"]["headroom_fraction"] >= 0.2
    assert metrics["postgresql"]["connections"]["headroom_fraction"] >= 0.2
    assert metrics["postgresql"]["pool_wait"]["required_observations"] >= 144_000
    assert metrics["postgresql"]["checkpoint"]["headroom_fraction"] >= 0.2
    assert metrics["postgresql"]["temp"]["headroom_fraction"] >= 0.2
    assert metrics["postgresql"]["autovacuum"]["headroom_fraction"] >= 0.2
    assert metrics["gc"]["headroom_fraction"] == 0.2
    assert metrics["api"]["requests_per_second"] >= 1
    assert metrics["api"]["matched_positive"]["cold_first_page_p95_ms"] == 32
    assert metrics["api"]["negative"]["cold_first_page_p95_ms"] == 30
    assert metrics["api"]["random"]["cold_first_page_p95_ms"] == 35
    assert metrics["api"]["error_rate_fraction"] == 0
    assert metrics["api"]["contention_coverage_ratio"] >= 0.99
    assert metrics["api"]["contention_max_gap_seconds"] <= 5
    assert metrics["api"]["planned_requests"] == 3_600
    assert metrics["api"]["attempted_requests"] == 3_600
    assert metrics["api"]["succeeded_requests"] == 3_600
    assert metrics["api"]["failed_requests"] == 0
    assert metrics["api"]["retried_requests"] == 0
    assert metrics["api"]["omitted_requests"] == 0
    threshold = metrics["api"]["threshold_concurrency"]
    assert threshold["build"]["coverage_ratio"] == 1
    assert threshold["audit"]["coverage_ratio"] == 1
    assert threshold["full_lane"]["coverage_ratio"] == 1
    assert threshold["full_lane"]["max_gap_seconds"] == 0
    rate_buckets = metrics["api"]["request_rate_buckets"]
    assert rate_buckets == {
        "scope": "signed_requests_fully_inside_full_lane_intervals",
        "bucket_seconds": 5,
        "bucket_count": 720,
        "minimum_observations": 5,
        "minimum_requests_per_second": 1,
        "underfilled_buckets": 0,
    }
    assert report["limits"]["api_request_rate_bucket_seconds"] == 5
    assert (
        report["evidence_receipt"]["contention_run_id"]
        == BASE_RECORD["api"]["contention_run_id"]
    )
    assert set(report["evidence_receipt"]["commitments"]) == set(gate._COMMITMENT_NAMES)


@pytest.mark.parametrize(
    ("minutes", "expected_hours"),
    [(10, 333.333333), (15, 500)],
)
def test_unreused_2000_month_arithmetic_includes_the_audit_tail(
    minutes, expected_hours
):
    record = _record()
    _set_unique_build_minutes(record, minutes)

    report = _evaluate(record)

    monthly = report["metrics"]["monthly_capacity"]
    assert monthly["worst_case_unique_builds"] == 2_000
    assert monthly["worst_case_worker_hours"] == pytest.approx(expected_hours)
    assert _gate_map(report)["logical_import_end_to_end_duration"] is False


def test_one_lane_capacity_threshold():
    record = _record()
    _set_unique_build_minutes(record, 15)
    record["lanes"] = {"count": 1, "availability_factor": 1}
    record["scratch"]["configured_concurrent_unique_builds"] = 1

    report = _evaluate(record)

    assert report["metrics"]["monthly_capacity"][
        "worst_case_lane_utilization_fraction"
    ] == pytest.approx(0.694444)
    assert _gate_map(report)["worst_case_lane_utilization"] is True


def test_insufficient_lane_headroom_fails_with_explicit_availability_factor():
    record = _record()
    _set_unique_build_minutes(record, 15)
    record["lanes"] = {"count": 1, "availability_factor": 0.95}
    record["scratch"]["configured_concurrent_unique_builds"] = 1

    report = _evaluate(record)

    assert report["status"] == "fail"
    assert report["metrics"]["monthly_capacity"][
        "worst_case_lane_utilization_fraction"
    ] > 0.7
    assert _gate_map(report)["worst_case_lane_utilization"] is False


def test_release_evidence_floor_rejects_insufficient_sample_count():
    record = _record()
    with patch.object(gate, "MIN_QUALIFYING_UNIQUE_BUILDS", 31):
        report = _evaluate(record)

    assert report["status"] == "fail"
    assert _gate_map(report)["unique_build_sample_floor"] is False


def test_public_fixture_is_generic_unsigned_and_non_release():
    example = gate.example_measurement()
    serialized = json.dumps(example, sort_keys=True).lower()

    assert example["release_evidence"] is False and "receipt" not in example
    assert len(example["raw_samples"]["import_lifecycle"]) == 60
    assert len(example["raw_samples"]["audit_results"]) == 60
    assert len(example["raw_samples"]["peak_import_events"]) == 7 * 24 * 8
    assert len(example["raw_samples"]["peak_audit_events"]) == 7 * 24 * 8
    assert len(example["raw_samples"]["http_random"]) == 2_600
    signed_http_row = example["raw_samples"]["http_random"][0]
    signed_payload_map = {
        field_name: signed_http_row[field_name]
        for field_name in gate._API_SIGNED_PAYLOAD_FIELDS
    }
    TEST_API_PRIVATE_KEY.public_key().verify(
        gate._base64url_decode_signature(
            signed_http_row["api_evidence_signature"], "api_evidence_signature"
        ),
        gate._api_signature_message(signed_payload_map),
    )
    assert not {
        "component_digests",
        "challenge_sha256",
        "logical_query_sha256",
        "process_instance_sha256",
    } & set(signed_http_row)
    assert all(
        term not in serialized
        for term in ("client_name", "company_name", "organization_name", "source_url")
    )
    assert _evaluation_error(example).code == "non_release_example"
    unsigned = _record()
    error = _evaluation_error(unsigned, signed=True)
    assert (error.code, error.field) == ("missing_field", "receipt")
