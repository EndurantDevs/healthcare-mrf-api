# See LICENSE.

from __future__ import annotations

import collections
import concurrent.futures
import datetime as dt
import hashlib
import json
import os
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from api import ptg2_capacity_evidence as capacity
from process.ptg_parts import ptg2_candidate_attestation
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)
from scripts.validation import ptg2_v3_source_api_audit as audit


PUBLIC_KEY = bytes(range(32))
TRUST = audit.CapacityEvidenceTrust(
    api_evidence_key_id="capacity-test-key",
    api_evidence_public_key=PUBLIC_KEY,
    release_digest="11" * 32,
    environment_id="22" * 32,
)
RUN_NONCE = "a1" * 32
CONTENTION_RUN_ID = "77" * 32

from tests.ptg2_v3_source_api_capacity_request_support import (
    _IntentFetcher,
    _capacity_config,
    _capacity_query,
    _contract,
    _fetcher,
    _observation,
    _options,
    _page_payload,
)
from tests.ptg2_v3_source_api_capacity_gate_support import (
    _candidate_attestation_fixture_report,
    _candidate_attestation_target,
    _capacity_gate_cohort_reports,
    _capacity_gate_counters,
    _capacity_gate_latency,
    _capacity_gate_reconciliation,
    _capacity_gate_report,
    _qualified_client_latency_report,
    _sha256,
)

def test_capacity_intents_are_only_on_required_audit_calls():
    config = _capacity_config()
    fetcher = _IntentFetcher(config)
    positive = audit.QueryKey("CPT", "99213", 1234567890)
    negative = audit.QueryKey("CPT", "00000", 1234567890)

    audit._audit_positive_query(
        positive,
        collections.Counter(),
        fetcher=fetcher,
        config=config,
    )
    assert fetcher.calls[0][2] == audit.CapacityObservationIntent(
        audit.CAPACITY_POSITIVE_COHORT,
        True,
    )
    assert fetcher.calls[1][2] is None

    fetcher.calls.clear()
    audit._audit_negative_query(negative, fetcher=fetcher, config=config)
    assert fetcher.calls[0][2] == audit.CapacityObservationIntent(
        audit.CAPACITY_NEGATIVE_COHORT,
        True,
    )
    assert fetcher.calls[1][2] is None

    fetcher.calls.clear()
    cold_request = audit.RandomApiRequest(0, positive, "aa" * 32, 100, "cold")
    warm_request = replace(cold_request, index=1, phase="warm")
    audit._audit_random_api_request(
        cold_request,
        collections.Counter(),
        fetcher=fetcher,
        config=config,
    )
    audit._audit_random_api_request(
        warm_request,
        collections.Counter(),
        fetcher=fetcher,
        config=config,
    )
    assert fetcher.calls[0][2] == audit.CapacityObservationIntent(
        audit.CAPACITY_RANDOM_COHORT,
        True,
    )
    assert fetcher.calls[1][2] == audit.CapacityObservationIntent(
        audit.CAPACITY_RANDOM_COHORT,
        False,
    )


def test_capacity_random_cohort_rejects_duplicate_semantic_queries():
    query = audit.QueryKey("CPT", "99213", 1234567890)
    occurrences = [
        audit.SourceOccurrence("aa" * 32, query, "tuple-a"),
        audit.SourceOccurrence("bb" * 32, query, "tuple-b"),
    ]

    with pytest.raises(
        audit.SourceCoverageError,
        match="capacity_distinct_random_query_population_below_minimum",
    ):
        audit.build_capacity_random_api_requests(occurrences, count=2)

    repeated = audit.build_random_api_requests(
        occurrences[:1],
        count=3,
        max_limit=100,
        seed="duplicate-reporting",
    )
    query_keys, _shape_keys, _digest = audit.AuditRunner._random_plan_details(repeated)
    assert len(query_keys) == 1
    assert sum(request.phase == "warm" for request in repeated) == 2

def test_capacity_gate_enforces_each_strict_request_floor():
    audit.validate_capacity_evidence_preflight(
        distinct_random_queries=2_500,
        negative_queries=400,
        positive_queries=100,
    )
    with pytest.raises(
        audit.SourceCoverageError,
        match="capacity_planned_http_requests_below_minimum",
    ):
        audit.validate_capacity_evidence_preflight(
            distinct_random_queries=2_500,
            negative_queries=250,
            positive_queries=100,
        )
    with pytest.raises(
        audit.SourceCoverageError,
        match="capacity_matched_positive_query_population_below_minimum",
    ):
        audit.validate_capacity_evidence_preflight(
            distinct_random_queries=2_500,
            negative_queries=500,
            positive_queries=0,
        )
    with pytest.raises(
        audit.SourceCoverageError,
        match="capacity_negative_query_population_below_minimum",
    ):
        audit.validate_capacity_evidence_preflight(
            distinct_random_queries=2_900,
            negative_queries=0,
            positive_queries=100,
        )
    assert (
        audit.capacity_evidence_release_failures(_capacity_gate_report(eligible=3_000))
        == ()
    )
    below_total_failures = audit.capacity_evidence_release_failures(
        _capacity_gate_report(eligible=2_999)
    )
    assert "capacity_eligible_http_requests_below_minimum" in below_total_failures
    assert "capacity_request_reconciliation_failed" in below_total_failures
    invalid_commitment = _capacity_gate_report(eligible=3_000)
    invalid_commitment["commitments"]["run_digest"] = "0" * 64
    assert audit.capacity_evidence_release_failures(invalid_commitment) == (
        "capacity_run_commitment_invalid",
    )


@pytest.mark.parametrize(
    ("missing_cohort", "expected_failure"),
    [
        (
            audit.CAPACITY_POSITIVE_COHORT,
            "capacity_matched_positive_eligible_requests_below_minimum",
        ),
        (
            audit.CAPACITY_NEGATIVE_COHORT,
            "capacity_negative_eligible_requests_below_minimum",
        ),
    ],
)
def test_capacity_gate_fails_closed_when_required_cohort_is_missing(
    missing_cohort,
    expected_failure,
):
    report = _capacity_gate_report()
    del report["counters"]["cohorts"][missing_cohort]

    failures = audit.capacity_evidence_release_failures(report)
    assert expected_failure in failures
    assert "capacity_request_reconciliation_failed" in failures


def test_capacity_gate_checks_random_eligible_and_distinct_floors_independently():
    assert audit.capacity_evidence_release_failures(
        _capacity_gate_report(random_eligible=2_499, negative_eligible=401)
    ) == ("capacity_random_eligible_requests_below_minimum",)
    assert audit.capacity_evidence_release_failures(
        _capacity_gate_report(distinct_random=2_499)
    ) == ("capacity_distinct_random_queries_below_minimum",)


def test_capacity_gate_rejects_missing_plan_even_when_all_floors_pass():
    report = _capacity_gate_report()
    counters = report["counters"]
    reconciliation = counters["reconciliation"]
    counters["planned"] += 1
    reconciliation["planned_first_physical_attempts"] += 1
    reconciliation["planned_without_physical_attempt"] = 1
    reconciliation["release_clean"] = False
    reconciliation["structural_failures"] = {
        "planned_without_physical_attempt": 1
    }
    counters["cohorts"][audit.CAPACITY_RANDOM_COHORT]["planned"] += 1

    failures = audit.capacity_evidence_release_failures(report)

    assert counters["eligible"] == 3_000
    assert failures == ("capacity_request_reconciliation_failed",)


def test_capacity_gate_rejects_overprovisioned_error_and_retry_above_floors():
    report = _capacity_gate_report()
    counters = report["counters"]
    reconciliation = counters["reconciliation"]
    counters.update(
        {
            "planned": 3_001,
            "physical": 3_002,
            "verified": 3_001,
            "rejected": 2,
            "retries": 1,
            "http_errors": 1,
            "verification_errors": 1,
            "status": {"200": 3_001, "503": 1},
            "rejections": {
                "retry_attempt": 1,
                "status_not_200": 1,
            },
        }
    )
    random_cohort = counters["cohorts"][audit.CAPACITY_RANDOM_COHORT]
    random_cohort.update(
        {
            "planned": 2_501,
            "physical": 2_502,
            "verified": 2_501,
            "rejected": 2,
        }
    )
    reconciliation.update(
        {
            "accounted": True,
            "release_clean": False,
            "planned_first_physical_attempts": 3_001,
            "plans_with_physical_attempt": 3_001,
            "initial_physical_attempts": 3_001,
            "retry_physical_attempts": 1,
            "eligible_physical_attempts": 3_000,
            "rejected_physical_attempts": 2,
            "verified_rejected_physical_attempts": 1,
        }
    )

    failures = audit.capacity_evidence_release_failures(report)

    assert counters["eligible"] == 3_000
    assert failures == (
        "capacity_complete_request_accounting_failed",
        "capacity_request_reconciliation_failed",
    )


def test_capacity_release_seed_is_derived_from_pinned_trust_and_reported():
    seed = audit.authoritative_release_seed(TRUST)
    expected_selection = audit.sampling_seed_evidence("release", seed, TRUST)
    changed_trust = replace(TRUST, release_digest="12" * 32)

    assert seed == audit.authoritative_release_seed(TRUST)
    assert seed != audit.authoritative_release_seed(changed_trust)
    assert expected_selection["precommitted"] is True
    assert expected_selection["contract"] == audit.RELEASE_CAPACITY_SEED_CONTRACT
    with pytest.raises(
        audit.ConfigurationError,
        match="release_seed_must_match_authority",
    ):
        audit.sampling_seed_evidence("release", "caller-searched", TRUST)

    collector = audit.CapacityEvidenceCollector(
        TRUST,
        contention_run_id=CONTENTION_RUN_ID,
        run_nonce=RUN_NONCE,
        sampling_seed=seed,
        sampling_profile="release",
    )
    assert collector.report()["selection"] == expected_selection


def test_capacity_gate_rejects_caller_substituted_seed_commitment():
    report = _capacity_gate_report()
    report["selection"]["seed_sha256"] = hashlib.sha256(
        b"searched-seed"
    ).hexdigest()

    assert audit.capacity_evidence_release_failures(report) == (
        "capacity_release_seed_commitment_invalid",
    )


def test_challenges_are_unique_under_concurrency():
    collector = audit.CapacityEvidenceCollector(
        TRUST,
        contention_run_id=CONTENTION_RUN_ID,
        run_nonce=RUN_NONCE,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        challenges = list(
            executor.map(lambda _index: collector.fresh_challenge(), range(1_000))
        )

    assert len(challenges) == len(set(challenges)) == 1_000
    assert all(len(challenge) == 64 for challenge in challenges)
    assert all(challenge == challenge.lower() for challenge in challenges)


def test_selection_ordinals_are_assigned_independently_per_cohort():
    collector = audit.CapacityEvidenceCollector(
        TRUST,
        contention_run_id=CONTENTION_RUN_ID,
        run_nonce=RUN_NONCE,
    )
    first_random = collector.plan(
        audit.CapacityObservationIntent(audit.CAPACITY_RANDOM_COHORT, True)
    )
    second_random = collector.plan(
        audit.CapacityObservationIntent(audit.CAPACITY_RANDOM_COHORT, True)
    )
    first_negative = collector.plan(
        audit.CapacityObservationIntent(audit.CAPACITY_NEGATIVE_COHORT, True)
    )

    assert first_random.selection_ordinal == first_negative.selection_ordinal == 0
    assert second_random.selection_ordinal == 1
    headers = collector.request_headers(first_negative, collector.fresh_challenge())
    assert headers[capacity.CAPACITY_SEMANTIC_CLASS_HEADER] == "negative"
    assert headers[capacity.CAPACITY_SELECTION_ORDINAL_HEADER] == "0"


def test_capacity_report_never_emits_raw_request_or_process_values(monkeypatch):
    sensitive_value_map = {
        "plan": "plan-secret",
        "snapshot": "snapshot-secret",
        "code": "99213",
        "npi": "1234567890",
        "challenge": "01" * 32,
        "run": RUN_NONCE,
        "process": "process-instance-secret",
    }

    def collect(_headers, **kwargs):
        observation = _observation(kwargs["query_parameters"])
        observation["opaque_test_value"] = sensitive_value_map["process"]
        return observation

    monkeypatch.setattr(
        audit.capacity_evidence,
        "collect_capacity_http_observation",
        collect,
    )
    collector = audit.CapacityEvidenceCollector(
        TRUST,
        contention_run_id=CONTENTION_RUN_ID,
        run_nonce=RUN_NONCE,
    )
    plan = collector.plan(
        audit.CapacityObservationIntent(audit.CAPACITY_RANDOM_COHORT, True)
    )
    observation_record = collector.collect_response(
        plan,
        challenge=sensitive_value_map["challenge"],
        query_parameters=_capacity_query(),
        response_headers={},
        response_status_code=200,
        response_body=b"{}",
        response_redirect_count=0,
        collector_received_at=dt.datetime.now(dt.timezone.utc),
        attempt_index=0,
        latency_ms=1.0,
    )
    collector.mark_semantic_outcome((observation_record,), successful=True)
    report = collector.report()
    serialized = json.dumps(report, sort_keys=True)

    assert all(
        sensitive_value not in serialized
        for sensitive_value in sensitive_value_map.values()
    )
    assert report["latency"]["eligible_signed_server_duration_ms"][
        "p95_ms"
    ] == 12.0
    assert report["latency"][
        "eligible_verified_client_http_duration_ms"
    ]["p95_ms"] == 1.0
