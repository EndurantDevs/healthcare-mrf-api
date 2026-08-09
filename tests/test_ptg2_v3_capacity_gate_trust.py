# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused tests split from a shared contract fixture module."""

from __future__ import annotations

from tests.test_ptg2_v3_capacity_gate import (
    ATTACKER_PRIVATE_KEY,
    Decimal,
    ModuleType,
    Path,
    TEST_NOW,
    TEST_TRUST,
    UTC,
    _evaluate,
    _evaluation_error,
    _gate_map,
    _install_trust,
    _record,
    _resign_api_row,
    _signed_record,
    _sync_reduced_audit_workload,
    _trust_config,
    datetime,
    gate,
    hashlib,
    importlib,
    inspect,
    os,
    patch,
    pytest,
    sys,
)


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
