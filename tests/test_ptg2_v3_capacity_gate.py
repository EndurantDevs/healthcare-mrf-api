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


def test_independent_gate_protocol_copy_matches_server_protocol_source():
    api_package = ModuleType("api")
    api_package.__path__ = [str(Path(gate.__file__).resolve().parents[2] / "api")]
    with patch.dict(sys.modules, {"api": api_package}):
        sys.modules.pop("api.ptg2_capacity_evidence", None)
        server_evidence = importlib.import_module("api.ptg2_capacity_evidence")

    assert gate._API_EVIDENCE_VERSION == server_evidence.CAPACITY_EVIDENCE_VERSION
    assert gate._API_SIGNATURE_VERSION == server_evidence.CAPACITY_SIGNATURE_VERSION
    assert gate._API_SIGNATURE_ALGORITHM == server_evidence.CAPACITY_SIGNATURE_ALGORITHM
    assert gate._API_SIGNATURE_DOMAIN == server_evidence.CAPACITY_SIGNATURE_DOMAIN
    assert gate._API_SIGNATURE_ENVELOPE == server_evidence._SIGNATURE_ENVELOPE
    assert gate._API_QUERY_CONTRACT_DIGEST == (
        server_evidence.CAPACITY_QUERY_CONTRACT_DIGEST
    )
    assert set(gate._API_SIGNED_PAYLOAD_FIELDS) == set(
        server_evidence._SIGNED_PAYLOAD_FIELDS
    )


def test_release_verifier_uses_only_protected_fixed_trust(monkeypatch, tmp_path):
    _install_trust(monkeypatch, tmp_path)
    monkeypatch.setenv("HLTHPRT_PTG2_CAPACITY_RECEIPT_KEY_ID", "caller-key")
    monkeypatch.setenv("HLTHPRT_PTG2_CAPACITY_RECEIPT_KEY_FILE", "/tmp/caller-key")
    report = gate.evaluate_measurement(_signed_record(_record()), now=TEST_NOW)
    options = {
        option
        for action in gate.build_argument_parser()._actions
        for option in action.option_strings
    }

    assert report["status"] == "pass"
    assert "trust" not in inspect.signature(gate.evaluate_measurement).parameters
    assert not {"--receipt-key-file", "--receipt-key-id", "--collector-id"} & options


def test_trust_config_requires_protected_file_and_valid_public_key(
    monkeypatch, tmp_path
):
    path = _install_trust(monkeypatch, tmp_path)
    assert gate._load_trust_config(path, os.geteuid()) == TEST_TRUST
    for mode, owner in ((0o620, os.geteuid()), (0o600, os.geteuid() + 1)):
        path.chmod(mode)
        with pytest.raises(gate.EvidenceError, match="insecure_trust_config"):
            gate._load_trust_config(path, owner)
    path.chmod(0o600)
    link = tmp_path / "trust-link.json"
    link.symlink_to(path)
    with pytest.raises(gate.EvidenceError, match="receipt_trust_unavailable"):
        gate._load_trust_config(link, os.geteuid())
    config = _trust_config()
    config["public_key_hex"] = "0" * 64
    with pytest.raises(gate.EvidenceError, match="invalid_trust_configuration"):
        gate._trust_from_config_object(config)
    config = _trust_config()
    config["api_evidence_public_key_hex"] = "0" * 64
    with pytest.raises(gate.EvidenceError, match="invalid_trust_configuration"):
        gate._trust_from_config_object(config)
    config = _trust_config()
    config["version"] = gate.TRUST_CONFIG_VERSION - 1
    with pytest.raises(gate.EvidenceError, match="unsupported_trust_config_version"):
        gate._trust_from_config_object(config)


def test_forged_receipts_and_post_signature_tampering_fail_authentication():
    forged = _signed_record(_record(), private_key=ATTACKER_PRIVATE_KEY)
    assert _evaluation_error(forged, signed=True).code == "invalid_receipt_signature"
    tampered = _signed_record(_record())
    tampered["raw_samples"]["http_random"][0]["server_duration_ns"] = 1
    assert (
        _evaluation_error(tampered, signed=True).code == "measurement_digest_mismatch"
    )


def test_receipt_identity_is_fixed_by_protected_trust():
    identity_by_field = {
        "key_id": "caller-key",
        "collector_id": "caller-collector",
        "collector_version": "9.9.9",
        "release_digest": hashlib.sha256(b"other-release").hexdigest(),
        "environment_id": hashlib.sha256(b"other-environment").hexdigest(),
    }
    for field, value in identity_by_field.items():
        signed = _signed_record(_record(), receipt_updates={field: value})
        error = _evaluation_error(signed, signed=True)
        assert (error.code, error.field) == (
            "receipt_identity_mismatch",
            f"receipt.{field}",
        )


def test_raw_rows_must_match_commitments_and_recomputed_aggregates():
    measurement = _record()
    old_digest = gate.sample_set_sha256(measurement["raw_samples"]["http_random"])
    measurement["raw_samples"]["http_random"][0]["challenge_digest"] = hashlib.sha256(
        b"replacement-request"
    ).hexdigest()
    _resign_api_row(measurement["raw_samples"]["http_random"][0])
    stale = _signed_record(
        measurement,
        commitment_updates={"cold_random_samples": {"sha256": old_digest}},
    )
    assert _evaluation_error(stale, signed=True).code == "commitment_digest_mismatch"
    spoofed = _record()
    spoofed["api"]["random"]["cold_first_page_p95_ms"] = 1
    error = _evaluation_error(spoofed)
    assert (error.code, error.field) == (
        "raw_aggregate_mismatch",
        "api.random.cold_first_page_p95_ms",
    )


def test_import_and_audit_rows_require_unique_joined_ordered_stages():
    def duplicate_id(measurement):
        rows = measurement["raw_samples"]["import_lifecycle"]
        rows[1]["import_id_sha256"] = rows[0]["import_id_sha256"]

    cases = (
        (duplicate_id, "duplicate_sample_id"),
        (
            lambda m: m["raw_samples"]["import_lifecycle"][0].update(
                source_audit_started_at="2026-07-13T08:55:00Z"
            ),
            "invalid_stage_order",
        ),
        (
            lambda m: m["raw_samples"]["audit_results"].pop(),
            "import_audit_join_mismatch",
        ),
        (
            lambda m: m["raw_samples"]["audit_results"][0].update(
                candidate_attested_at="2026-07-13T09:09:01Z"
            ),
            "raw_aggregate_mismatch",
        ),
    )
    for mutate, code in cases:
        measurement = _record()
        mutate(measurement)
        assert _evaluation_error(measurement).code == code


def test_each_import_must_complete_all_stages_within_15_minutes():
    measurement = _record()
    measurement["raw_samples"]["import_lifecycle"][0][
        "enqueued_at"
    ] = "2026-07-13T08:54:00Z"
    measurement["end_to_end"].update(within_15_minutes=59, max_minutes=16)
    report = _evaluate(measurement)

    assert report["metrics"]["end_to_end"]["within_15_minutes"] == 59
    assert report["metrics"]["end_to_end"]["max_minutes"] == 16
    assert _gate_map(report)["logical_import_end_to_end_duration"] is False


def test_receipt_freshness_and_authenticated_observation_bounds_fail_closed():
    cases = (
        (
            {"observation_started_at": "2026-07-05T10:04:00Z"},
            TEST_NOW,
            "stale_evidence",
        ),
        (
            {
                "issued_at": "2026-07-13T10:20:00Z",
                "expires_at": "2026-07-13T11:20:00Z",
                "observation_ended_at": "2026-07-13T10:19:00Z",
            },
            datetime(2026, 7, 13, 10, 21, tzinfo=UTC),
            "stale_contention",
        ),
        (
            {"observation_started_at": "2026-07-06T10:00:00Z"},
            TEST_NOW,
            "sample_outside_observation",
        ),
    )
    for updates, now, code in cases:
        signed = _signed_record(_record(), receipt_updates=updates)
        assert _evaluation_error(signed, signed=True, now=now).code == code


def test_peak_and_import_audit_contention_windows_require_continuous_coverage():
    peak_gap = _record()
    removed_window = peak_gap["peak_arrival"]["windows"].pop(84)
    peak_gap["raw_samples"]["peak_import_events"] = [
        event
        for event in peak_gap["raw_samples"]["peak_import_events"]
        if not (
            removed_window["started_at"]
            <= event["enqueued_at"]
            < removed_window["ended_at"]
        )
    ]
    peak_gap["raw_samples"]["peak_audit_events"] = [
        event
        for event in peak_gap["raw_samples"]["peak_audit_events"]
        if not (
            removed_window["started_at"]
            <= event["queued_at"]
            < removed_window["ended_at"]
        )
    ]
    peak_gap["peak_arrival"]["sample_windows"] = 167
    peak_report = _evaluate(peak_gap)
    assert peak_report["metrics"]["peak_arrival"]["max_gap_minutes"] == 60
    assert peak_report["metrics"]["peak_arrival"]["coverage_ratio"] < 1
    assert _gate_map(peak_report)["peak_arrival_evidence"] is False

    contention_report = _evaluate(_record())
    assert contention_report["metrics"]["api"]["contention_max_gap_seconds"] <= 5
    assert contention_report["metrics"]["api"]["contention_coverage_ratio"] >= Decimal(
        "0.99"
    )
    threshold = contention_report["metrics"]["api"]["threshold_concurrency"]
    assert threshold["build"]["coverage_ratio"] == 1
    assert threshold["audit"]["coverage_ratio"] == 1
    assert threshold["full_lane"]["coverage_ratio"] == 1
    assert _gate_map(contention_report)["api_continuous_contention_coverage"] is True


def test_six_million_audit_projection_comes_from_same_run_raw_rows():
    reduced = _record()
    _sync_reduced_audit_workload(reduced)
    report = _evaluate(reduced)
    audit = report["metrics"]["candidate_audit"]

    assert audit["http_requests_per_activation"] == 2_999
    assert audit["http_requests_per_month"] == 5_998_000
    assert _gate_map(report)["candidate_audit_monthly_workload"] is False
    for location in (
        ("raw_samples", "audit_results", 0, "contention_run_id"),
        ("postgresql", "load", "contention_run_id"),
    ):
        measurement = _record()
        container = measurement
        for component in location[:-1]:
            container = container[component]
        container[location[-1]] = hashlib.sha256(b"other-run").hexdigest()
        assert _evaluation_error(measurement).code in {
            "contention_run_mismatch",
            "raw_aggregate_mismatch",
        }
