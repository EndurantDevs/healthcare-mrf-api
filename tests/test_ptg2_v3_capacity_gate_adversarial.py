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


def _mutate_first_api_sample(measurement, class_name, **updates):
    api_sample = measurement["raw_samples"][f"http_{class_name}"][0]
    api_sample.update(**updates)
    _resign_api_row(api_sample)


def test_http_distinctness_semantics_and_selection_are_verified_from_rows():
    """Reject duplicated, overlapping, or misclassified HTTP observations."""
    def duplicate_query(measurement):
        samples = measurement["raw_samples"]["http_random"]
        samples[1]["semantic_query_digest"] = samples[0]["semantic_query_digest"]
        _resign_api_row(samples[1])

    def overlap_query(measurement):
        raw_samples = measurement["raw_samples"]
        source = raw_samples["http_matched_positive"][0]
        target = raw_samples["http_negative"][0]
        target["semantic_query_digest"] = source["semantic_query_digest"]
        _resign_api_row(target)

    cases = (
        (duplicate_query, "duplicate_query_key"),
        (overlap_query, "cross_class_query_overlap"),
        (
            lambda measurement: _mutate_first_api_sample(
                measurement, "matched_positive", result_count=0
            ),
            "semantic_class_mismatch",
        ),
        (
            lambda measurement: _mutate_first_api_sample(
                measurement, "negative", result_count=1
            ),
            "semantic_class_mismatch",
        ),
        (
            lambda measurement: _mutate_first_api_sample(
                measurement, "matched_positive", semantic_class="negative"
            ),
            "semantic_class_mismatch",
        ),
        (
            lambda measurement: _mutate_first_api_sample(
                measurement, "matched_positive", selection_method="known_miss_v1"
            ),
            "semantic_class_mismatch",
        ),
        (
            lambda measurement: _mutate_first_api_sample(
                measurement, "matched_positive", selection_ordinal=1
            ),
            "invalid_selection_ordinals",
        ),
        (
            lambda measurement: (
                measurement["raw_samples"]["http_random"][0].update(
                    process_started_at="2026-07-13T08:00:00Z"
                ),
                _resign_api_row(measurement["raw_samples"]["http_random"][0]),
            ),
            "stale_process",
        ),
    )
    for mutate, code in cases:
        measurement = _record()
        mutate(measurement)
        assert _evaluation_error(measurement).code == code


def test_http_status_cold_state_and_nearest_rank_p95_are_recomputed():
    warm = _record()
    warm["raw_samples"]["http_random"][0]["cold"] = False
    warm["api"]["fresh_processes"] = False
    _resign_api_row(warm["raw_samples"]["http_random"][0])
    assert _evaluation_error(warm).code == "cold_observation_required"

    slow = _record()
    for slow_sample in slow["raw_samples"]["http_random"][-131:]:
        slow_sample["response_status"] = 500
        slow_sample["server_duration_ns"] = 41_000_000
        _resign_api_row(slow_sample)
    slow["api"].update(
        succeeded_requests=3_469,
        failed_requests=131,
        errors=131,
    )
    slow["api"]["random"].update(
        errors=131,
        cold_first_page_p95_ms=41,
    )
    slow_report = _evaluate(slow)
    api_metrics = slow_report["metrics"]["api"]
    assert api_metrics["observed_failed_requests"] == 131
    assert api_metrics["failed_requests"] == 131
    assert api_metrics["random"]["all_sample_cold_first_page_p95_ms"] == 41
    assert api_metrics["random"]["cold_first_page_p95_ms"] == 41
    assert _gate_map(slow_report)["api_physical_request_accounting"] is False
    assert _gate_map(slow_report)["api_random_cold_p95"] is False
    assert _gate_map(slow_report)["api_error_rate"] is False


def test_physical_request_failures_retries_and_omissions_fail_release():
    measurement = _record()
    measurement["api"].update(
        attempted_requests=3_601,
        succeeded_requests=3_600,
        failed_requests=1,
        retried_requests=1,
        errors=1,
    )

    report = _evaluate(measurement)
    api_metrics = report["metrics"]["api"]

    assert api_metrics["planned_requests"] == 3_600
    assert api_metrics["attempted_requests"] == 3_601
    assert api_metrics["succeeded_requests"] == 3_600
    assert api_metrics["failed_requests"] == 1
    assert api_metrics["retried_requests"] == 1
    assert api_metrics["omitted_requests"] == 1
    assert api_metrics["observed_successful_requests"] == 3_600
    assert api_metrics["observed_failed_requests"] == 0
    assert _gate_map(report)["api_physical_request_accounting"] is False
    assert report["status"] == "fail"


def test_omitted_successful_raw_request_fails_release():
    measurement = _record()
    random_rows = measurement["raw_samples"]["http_random"]
    random_rows.pop()
    random_summary = measurement["api"]["random"]
    random_summary.update(
        samples=len(random_rows),
        distinct_query_keys=len(random_rows),
        contention_samples=len(random_rows),
        contention_distinct_query_keys=len(random_rows),
        sample_ended_at=random_rows[-1]["server_observed_at"],
    )
    measurement["api"].update(
        requests=3_599,
        requests_while_imports_running=3_599,
    )
    measurement["postgresql"]["load"]["api_requests_per_second"] = Decimal(
        3_599
    ) / Decimal(3_600)

    report = _evaluate(measurement)

    assert report["metrics"]["api"]["omitted_requests"] == 1
    assert report["metrics"]["api"]["failed_requests"] == 0
    assert _gate_map(report)["api_physical_request_accounting"] is False
    assert report["status"] == "fail"


@pytest.mark.parametrize(
    "updates",
    (
        {"attempted_requests": 3_601},
        {"succeeded_requests": 3_599},
        {"planned_requests": 3_599},
        {"errors": 1},
    ),
)
def test_physical_request_counters_must_reconcile(updates):
    measurement = _record()
    measurement["api"].update(updates)

    assert _evaluation_error(measurement).code == "inconsistent_evidence"


def test_server_signature_envelope_and_closed_row_are_independently_verified():
    forged = _record()
    forged_row = forged["raw_samples"]["http_matched_positive"][0]
    _resign_api_row(forged_row, private_key=ATTACKER_API_PRIVATE_KEY)
    assert _evaluation_error(forged).code == "invalid_api_evidence_signature"

    tampered = _record()
    tampered_row = tampered["raw_samples"]["http_matched_positive"][0]
    tampered_row["response_body_sha256"] = hashlib.sha256(b"tampered-body").hexdigest()
    assert _evaluation_error(tampered).code == "invalid_api_evidence_signature"

    rewritten_latency = _record()
    rewritten_latency["raw_samples"]["http_matched_positive"][0][
        "server_duration_ns"
    ] = 0
    assert (
        _evaluation_error(rewritten_latency).code
        == "invalid_api_evidence_signature"
    )

    wrong_envelope = _record()
    wrong_envelope_row = wrong_envelope["raw_samples"]["http_matched_positive"][0]
    signed_payload_map = {
        name: wrong_envelope_row[name] for name in gate._API_SIGNED_PAYLOAD_FIELDS
    }
    wrong_envelope_row["api_evidence_signature"] = gate._base64url_encode(
        TEST_API_PRIVATE_KEY.sign(gate.canonical_json_bytes(signed_payload_map))
    )
    assert _evaluation_error(wrong_envelope).code == "invalid_api_evidence_signature"

    padded = _record()
    padded["raw_samples"]["http_matched_positive"][0]["api_evidence_signature"] += "="
    assert _evaluation_error(padded).code == "invalid_api_evidence_signature"

    unknown = _record()
    unknown["raw_samples"]["http_matched_positive"][0]["raw_plan_id"] = "redacted"
    error = _evaluation_error(unknown)
    assert (error.code, error.field) == (
        "unexpected_field",
        "raw_samples.http_matched_positive.0",
    )

    missing = _record()
    missing["raw_samples"]["http_matched_positive"][0].pop("scope_digest")
    error = _evaluation_error(missing)
    assert (error.code, error.field) == (
        "missing_field",
        "raw_samples.http_matched_positive.0.scope_digest",
    )


def test_api_evidence_key_release_and_environment_are_pinned_separately():
    wrong_key_id = _record()
    key_row = wrong_key_id["raw_samples"]["http_matched_positive"][0]
    key_row["api_evidence_key_id"] = "different-api-key"
    _resign_api_row(key_row)
    assert _evaluation_error(wrong_key_id).code == "api_evidence_key_mismatch"

    for signed_field, mismatched_value, expected_code in (
        (
            "release_digest",
            hashlib.sha256(b"other-release").hexdigest(),
            "api_evidence_release_digest_mismatch",
        ),
        (
            "environment_id",
            hashlib.sha256(b"other-environment").hexdigest(),
            "api_evidence_environment_id_mismatch",
        ),
    ):
        measurement = _record()
        api_sample = measurement["raw_samples"]["http_matched_positive"][0]
        api_sample[signed_field] = mismatched_value
        _resign_api_row(api_sample)
        assert _evaluation_error(measurement).code == expected_code

    signed = _signed_record(_record())
    wrong_api_trust = replace(
        TEST_TRUST, api_evidence_public_key=ATTACKER_API_PUBLIC_KEY
    )
    with pytest.raises(gate.EvidenceError, match="invalid_api_evidence_signature"):
        gate._evaluate_measurement_with_trust(
            signed,
            trust=wrong_api_trust,
            now=datetime(2026, 7, 13, 10, 10, tzinfo=UTC),
        )

    with pytest.raises(gate.EvidenceError, match="invalid_trust_configuration"):
        gate._validate_trust(
            replace(
                TEST_TRUST,
                api_evidence_key_id=TEST_TRUST.key_id,
                api_evidence_public_key=TEST_TRUST.public_key,
            )
        )


def test_signed_protocol_constants_page_ordinal_and_isolation_are_exact():
    cases = (
        ("signature_domain", "other.signature.domain"),
        ("query_contract_digest", hashlib.sha256(b"other-contract").hexdigest()),
        ("page_limit", 99),
        ("observation_ordinal", 1),
        ("isolated", False),
    )
    for field, value in cases:
        measurement = _record()
        row = measurement["raw_samples"]["http_matched_positive"][0]
        row[field] = value
        _resign_api_row(row)
        error = _evaluation_error(measurement)
        assert (error.code, error.field) == (
            "api_evidence_protocol_mismatch",
            f"raw_samples.http_matched_positive.0.{field}",
        )

    invalid_status = _record()
    invalid_status_row = invalid_status["raw_samples"]["http_matched_positive"][0]
    invalid_status_row["response_status"] = 600
    _resign_api_row(invalid_status_row)
    error = _evaluation_error(invalid_status)
    assert (error.code, error.field) == (
        "invalid_value",
        "raw_samples.http_matched_positive.0.response_status",
    )


def test_challenge_query_run_process_and_class_metadata_cannot_be_reused():
    duplicate_challenge = _record()
    challenge_rows = duplicate_challenge["raw_samples"]["http_random"]
    challenge_rows[1]["challenge_digest"] = challenge_rows[0]["challenge_digest"]
    _resign_api_row(challenge_rows[1])
    assert _evaluation_error(duplicate_challenge).code == "duplicate_sample_id"

    other_run = _record()
    run_row = other_run["raw_samples"]["http_random"][0]
    run_row["run_digest"] = hashlib.sha256(b"other-api-run").hexdigest()
    _resign_api_row(run_row)
    assert _evaluation_error(other_run).code == "run_digest_mismatch"

    other_contention = _record()
    other_contention["raw_samples"]["http_random"][0]["contention_run_id"] = (
        hashlib.sha256(b"other-contention-run").hexdigest()
    )
    _resign_api_row(other_contention["raw_samples"]["http_random"][0])
    assert _evaluation_error(other_contention).code == "contention_run_mismatch"

    not_first = _record()
    not_first["raw_samples"]["http_random"][0]["first_observation"] = False
    _resign_api_row(not_first["raw_samples"]["http_random"][0])
    assert _evaluation_error(not_first).code == "first_observation_required"


def test_process_freshness_receive_skew_and_contention_window_fail_closed():
    stale = _record()
    stale_row = stale["raw_samples"]["http_matched_positive"][0]
    stale_row["process_started_at"] = "2026-07-13T08:54:59Z"
    _resign_api_row(stale_row)
    assert _evaluation_error(stale).code == "stale_process"

    skewed = _record()
    skewed_row = skewed["raw_samples"]["http_matched_positive"][0]
    skewed_row["collector_received_at"] = "2026-07-13T09:00:06Z"
    assert _evaluation_error(skewed).code == "collector_receive_skew"

    outside = _record()
    outside_row = outside["raw_samples"]["http_matched_positive"][0]
    outside_row.update(
        process_started_at="2026-07-13T08:59:59Z",
        server_received_at="2026-07-13T08:59:59Z",
        server_observed_at="2026-07-13T08:59:59Z",
        collector_received_at="2026-07-13T08:59:59Z",
    )
    _resign_api_row(outside_row)
    assert _evaluation_error(outside).code == "sample_outside_contention"


def test_reuse_adjusted_monthly_utilization_is_a_release_gate():
    measurement = _record()
    _set_reuse_end_to_end_minutes(measurement, 15)
    measurement["lanes"]["availability_factor"] = Decimal("0.25")

    report = _evaluate(measurement)
    capacity = report["metrics"]["monthly_capacity"]

    assert capacity["worst_case_lane_utilization_fraction"] < Decimal("0.70")
    assert capacity["reuse_adjusted_lane_utilization_fraction"] > Decimal("0.70")
    assert _gate_map(report)["worst_case_lane_utilization"] is True
    assert _gate_map(report)["reuse_adjusted_lane_utilization"] is False


def test_cold_p95_requires_the_entire_request_at_full_concurrency():
    measurement = _record()
    for lifecycle_sample in measurement["raw_samples"]["import_lifecycle"]:
        if lifecycle_sample["kind"] != "unique_build":
            continue
        completed_at = datetime.strptime(
            lifecycle_sample["build_completed_at"], gate._UTC_TIMESTAMP_FORMAT
        ).replace(tzinfo=UTC)
        lifecycle_sample["build_started_at"] = gate._timestamp_text(
            completed_at - timedelta(seconds=1)
        )
    one_second_minutes = Decimal(1) / Decimal(60)
    measurement["unique_build"].update(
        p95_minutes=one_second_minutes,
        max_minutes=one_second_minutes,
    )
    for http_sample in measurement["raw_samples"]["http_random"]:
        observed_at = datetime.strptime(
            http_sample["server_observed_at"], gate._UTC_TIMESTAMP_FORMAT
        ).replace(tzinfo=UTC)
        elapsed_seconds = int(
            (observed_at - datetime(2026, 7, 13, 9, tzinfo=UTC)).total_seconds()
        )
        if (elapsed_seconds + 1) % 240 == 0:
            request_started_at = observed_at - timedelta(seconds=10)
            http_sample["process_started_at"] = gate._timestamp_text(
                request_started_at
            )
            http_sample["server_received_at"] = gate._timestamp_text(
                request_started_at
            )
            http_sample["server_duration_ns"] = 10_000_000_000
            _resign_api_row(http_sample)

    report = _evaluate(measurement)
    random_metrics = report["metrics"]["api"]["random"]

    assert random_metrics["all_sample_cold_first_page_p95_ms"] == 35
    assert random_metrics["cold_first_page_p95_ms"] != 10_000
    assert random_metrics["fully_contended_samples"] < gate.MIN_RANDOM_COLD_SAMPLES
    assert _gate_map(report)["api_cold_sample_coverage"] is False


def test_brief_full_lane_overlap_cannot_satisfy_contention_coverage():
    measurement = _record()
    for lifecycle_sample in measurement["raw_samples"]["import_lifecycle"]:
        if lifecycle_sample["kind"] != "unique_build":
            continue
        completed_at = datetime.strptime(
            lifecycle_sample["build_completed_at"], gate._UTC_TIMESTAMP_FORMAT
        ).replace(tzinfo=UTC)
        lifecycle_sample["build_started_at"] = gate._timestamp_text(
            completed_at - timedelta(seconds=1)
        )
    one_second_minutes = Decimal(1) / Decimal(60)
    measurement["unique_build"].update(
        p95_minutes=one_second_minutes,
        max_minutes=one_second_minutes,
    )

    report = _evaluate(measurement)
    api_metrics = report["metrics"]["api"]
    threshold = api_metrics["threshold_concurrency"]
    gates = _gate_map(report)

    assert api_metrics["concurrent_unique_builds"] == 2
    assert api_metrics["concurrent_candidate_audits"] >= 2
    assert threshold["build"]["coverage_ratio"] < Decimal("0.01")
    assert threshold["build"]["max_gap_seconds"] > 5
    assert threshold["full_lane"]["coverage_ratio"] < Decimal("0.01")
    assert gates["scratch_concurrency_coverage"] is True
    assert gates["postgres_load_coverage"] is True
    assert gates["api_import_overlap"] is True
    assert gates["api_continuous_contention_coverage"] is False


def test_sustained_rate_counts_only_requests_fully_inside_full_lane_intervals():
    started_at = datetime(2026, 7, 13, 9, tzinfo=UTC)
    full_lane = ((started_at, started_at + timedelta(seconds=2)),)
    fully_inside = SimpleNamespace(
        server_received_at=started_at + timedelta(milliseconds=500),
        server_observed_at=started_at + timedelta(seconds=1),
    )
    crossing_boundary = SimpleNamespace(
        server_received_at=started_at + timedelta(seconds=1),
        server_observed_at=started_at + timedelta(seconds=3),
    )

    assert gate._http_samples_within_intervals(
        (fully_inside, crossing_boundary), full_lane
    ) == (fully_inside,)


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
