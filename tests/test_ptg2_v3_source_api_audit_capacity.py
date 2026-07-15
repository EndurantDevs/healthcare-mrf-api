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


def _capacity_query(**overrides):
    query_parameter_map = {
        "plan_id": "plan-secret",
        "snapshot_id": "snapshot-secret",
        "mode": "exact_source",
        "code_system": "CPT",
        "code": "99213",
        "npi": "1234567890",
        "include_providers": "true",
        "include_details": "true",
        "include_sources": "true",
        "include_allowed_amounts": "false",
        "include_unverified_addresses": "true",
        "order_by": "npi",
        "order": "asc",
        "limit": "100",
        "offset": "0",
    }
    query_parameter_map.update(overrides)
    return query_parameter_map


def _observation(query_parameters, *, ordinal=0):
    semantic = hashlib.sha256(
        audit.canonical_json(
            {
                "snapshot_id": query_parameters["snapshot_id"],
                "code_system": query_parameters["code_system"],
                "code": query_parameters["code"],
                "npi": query_parameters["npi"],
            }
        ).encode("utf-8")
    ).hexdigest()
    return {
        "evidence_version": capacity.CAPACITY_EVIDENCE_VERSION,
        "run_digest": "33" * 32,
        "semantic_query_digest": semantic,
        "challenge_digest": f"{ordinal + 1:064x}",
        "scope_digest": "44" * 32,
        "process_instance_digest": "55" * 32,
        "response_body_sha256": "66" * 32,
        "api_evidence_signature": "signed-redacted-value",
        "collector_received_at": "2026-07-14T12:00:00Z",
        "server_duration_ns": 12_000_000,
        "observation_ordinal": ordinal,
        "cold": ordinal == 0,
        "first_observation": ordinal == 0,
    }


def _options(*, retries=0):
    return audit.HttpApiFetcherOptions(
        base_url="https://api.example.invalid",
        api_path=audit.DEFAULT_API_PATH,
        headers={"Authorization": "Bearer private-token"},
        page_size=100,
        max_pages=5,
        timeout_seconds=5.0,
        retries=retries,
        retry_backoff_seconds=0.0,
        max_response_bytes=2 * 1024 * 1024,
        verify_tls=True,
    )


def _fetcher(handler, collector, *, retries=0):
    fetcher = audit.HttpApiFetcher(
        _options(retries=retries),
        capacity_collector=collector,
    )
    fetcher.client.close()
    fetcher.client = httpx.Client(
        transport=httpx.MockTransport(handler),
        headers=dict(_options(retries=retries).headers),
        follow_redirects=False,
    )
    return fetcher


def _page_payload(query_parameter_map, *, provider_items, total, offset):
    return {
        "result_state": "matched" if total else "no_matching_rates",
        "pricing_scope": audit.EXPECTED_PRICING_SCOPE,
        "resolved_snapshot_id": query_parameter_map["snapshot_id"],
        "query": {
            "snapshot_id": query_parameter_map["snapshot_id"],
            "plan_id": query_parameter_map["plan_id"],
            "mode": query_parameter_map["mode"],
        },
        "provenance": {
            "arch_version": audit.EXPECTED_ARCHITECTURE,
            "storage_generation": audit.EXPECTED_STORAGE_GENERATION,
            "database_backend": audit.EXPECTED_DATABASE_BACKEND,
            "database_evidence": {
                "contract": audit.DATABASE_EVIDENCE_CONTRACT,
                "server_version_num": 180000,
                "database_selected": True,
                "backend_session_active": True,
                "transaction_snapshot_observed": True,
            },
            "plan_id": query_parameter_map["plan_id"],
            "snapshot_id": query_parameter_map["snapshot_id"],
            "mode": query_parameter_map["mode"],
            "pricing_scope": audit.EXPECTED_PRICING_SCOPE,
        },
        "items": provider_items,
        "pagination": {
            "offset": offset,
            "limit": 100,
            "total": total,
            "has_more": offset + len(provider_items) < total,
        },
    }


def _contract(config, *, matched):
    return audit.PageContract(
        result_state="matched" if matched else "no_matching_rates",
        pricing_scope=audit.EXPECTED_PRICING_SCOPE,
        resolved_snapshot_id=config.snapshot_id,
        query_snapshot_id=config.snapshot_id,
        query_plan_id=config.plan_id,
        query_mode=audit.EXPECTED_QUERY_MODE,
        provenance_arch_version=audit.EXPECTED_ARCHITECTURE,
        provenance_storage_generation=audit.EXPECTED_STORAGE_GENERATION,
        provenance_database_backend=audit.EXPECTED_DATABASE_BACKEND,
        provenance_database_evidence_contract=audit.DATABASE_EVIDENCE_CONTRACT,
        provenance_postgres_server_version_num=180000,
        provenance_database_selected=True,
        provenance_backend_session_active=True,
        provenance_transaction_snapshot_observed=True,
        provenance_plan_id=config.plan_id,
        provenance_snapshot_id=config.snapshot_id,
        provenance_mode=audit.EXPECTED_QUERY_MODE,
        provenance_pricing_scope=audit.EXPECTED_PRICING_SCOPE,
    )


def _capacity_config():
    return audit.AuditConfig(
        profile="diagnostic",
        api_base_url="https://api.example.invalid",
        api_path=audit.DEFAULT_API_PATH,
        api_audit_path=audit.DEFAULT_API_AUDIT_PATH,
        plan_id="plan-secret",
        snapshot_id="snapshot-secret",
        plan_market_type=None,
        source_key=None,
        seed="capacity-test",
        source_occurrence_samples=3,
        api_occurrence_samples=3,
        negative_samples=2,
        random_api_calls=2_500,
        random_api_max_limit=100,
        min_source_occurrence_checks=1,
        min_api_occurrence_checks=1,
        min_negative_checks=1,
        min_random_api_calls=1,
        min_resolved_rate_fraction=1.0,
        max_unresolved_provider_references=0,
        max_invalid_prices=0,
        max_invalid_npis=0,
        max_invalid_field_types=0,
        page_size=100,
        max_pages=5,
        api_audit_page_size=100,
        api_audit_max_pages=5,
        warm_repeats=1,
        concurrency=1,
        failure_example_limit=10,
        verify_tls=True,
        capacity_evidence_trust=TRUST,
        capacity_contention_run_id=CONTENTION_RUN_ID,
    )


def test_capacity_evidence_requires_one_explicit_contention_run_id():
    with pytest.raises(
        audit.ConfigurationError,
        match="capacity_contention_run_id_invalid",
    ):
        replace(_capacity_config(), capacity_contention_run_id=None)
    with pytest.raises(
        audit.ConfigurationError,
        match="capacity_contention_run_requires_evidence_trust",
    ):
        replace(_capacity_config(), capacity_evidence_trust=None)


def test_request_page_binds_exact_body_status_multivalue_headers_and_latency(
    monkeypatch,
):
    """Bind signed evidence to exact HTTP status, body, headers, and latency."""

    captured_request_map = {}

    def collect(response_headers, **kwargs):
        captured_request_map.update(kwargs)
        captured_request_map["header_pairs"] = response_headers.items(multi=True)
        return _observation(kwargs["query_parameters"])

    monkeypatch.setattr(
        audit.capacity_evidence,
        "collect_capacity_http_observation",
        collect,
    )

    def handler(request):
        assert capacity.CAPACITY_CHALLENGE_HEADER in request.headers
        assert capacity.CAPACITY_RUN_NONCE_HEADER in request.headers
        assert (
            request.headers[capacity.CAPACITY_CONTENTION_RUN_ID_HEADER]
            == CONTENTION_RUN_ID
        )
        assert request.headers[capacity.CAPACITY_SEMANTIC_CLASS_HEADER] == "random"
        assert request.headers[capacity.CAPACITY_SELECTION_ORDINAL_HEADER] == "0"
        return httpx.Response(
            200,
            content=b'{"ok":true}',
            headers=[("X-Duplicate", "one"), ("X-Duplicate", "two")],
            request=request,
        )

    collector = audit.CapacityEvidenceCollector(
        TRUST,
        contention_run_id=CONTENTION_RUN_ID,
        run_nonce=RUN_NONCE,
    )
    with _fetcher(handler, collector) as fetcher:
        plan = collector.plan(
            audit.CapacityObservationIntent(
                audit.CAPACITY_RANDOM_COHORT,
                True,
            )
        )
        response_body_json, page_latency_ms, retries, observations = fetcher._request_page(
            _capacity_query(),
            capacity_plan=plan,
        )
        assert capacity.CAPACITY_CHALLENGE_HEADER not in fetcher.client.headers
        assert capacity.CAPACITY_RUN_NONCE_HEADER not in fetcher.client.headers

    assert response_body_json == {"ok": True}
    assert retries == 0
    assert page_latency_ms >= observations[0].latency_ms >= 0
    assert captured_request_map["response_status_code"] == 200
    assert captured_request_map["response_body"] == b'{"ok":true}'
    assert captured_request_map["response_redirect_count"] == 0
    assert captured_request_map["query_parameters"] == _capacity_query()
    assert captured_request_map["expected_contention_run_id"] == CONTENTION_RUN_ID
    assert captured_request_map["expected_semantic_class"] == "random"
    assert captured_request_map["expected_selection_ordinal"] == 0
    assert isinstance(captured_request_map["collector_received_at"], dt.datetime)
    assert captured_request_map["header_pairs"].count(("x-duplicate", "one")) == 1
    assert captured_request_map["header_pairs"].count(("x-duplicate", "two")) == 1


def _configure_signed_server(monkeypatch):
    """Configure an isolated server signer and return its collector trust."""

    private_key_bytes = bytes(range(1, 33))
    private_key = Ed25519PrivateKey.from_private_bytes(private_key_bytes)
    public_key = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    trust = replace(TRUST, api_evidence_public_key=public_key)
    signing_environment_map = {
        capacity.CAPACITY_ISOLATED_PROCESS_ENV: "1",
        capacity.CAPACITY_PRIVATE_KEY_ENV: private_key_bytes.hex(),
        capacity.CAPACITY_KEY_ID_ENV: trust.api_evidence_key_id,
        capacity.CAPACITY_RELEASE_DIGEST_ENV: trust.release_digest,
        capacity.CAPACITY_ENVIRONMENT_ID_ENV: trust.environment_id,
    }
    observed_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    monkeypatch.setattr(capacity._PROCESS_IDENTITY, "process_id", os.getpid())
    monkeypatch.setattr(capacity._PROCESS_IDENTITY, "instance", "77" * 32)
    monkeypatch.setattr(capacity._PROCESS_IDENTITY, "started_at", observed_at)
    monkeypatch.setattr(
        capacity._PROCESS_IDENTITY,
        "challenge_state",
        capacity.CapacityEvidenceState(),
    )
    monkeypatch.setattr(capacity, "require_control_auth", lambda _request: None)
    return trust, signing_environment_map, observed_at


def _signed_evidence_response_handler(
    *,
    server_state,
    signing_environment_map,
    observed_at,
    response_body,
):
    """Return a mock handler that signs the exact request received by HTTPX."""

    def handler(request):
        request_header_map = {
            header_name: request.headers[header_name]
            for header_name in (
                capacity.CAPACITY_CHALLENGE_HEADER,
                capacity.CAPACITY_RUN_NONCE_HEADER,
                capacity.CAPACITY_CONTENTION_RUN_ID_HEADER,
                capacity.CAPACITY_SEMANTIC_CLASS_HEADER,
                capacity.CAPACITY_SELECTION_ORDINAL_HEADER,
            )
        }
        server_request = SimpleNamespace(
            args=dict(request.url.params),
            headers=request_header_map,
            method="GET",
            path=capacity.CAPACITY_QUERY_PATH,
            ctx=SimpleNamespace(),
        )
        capacity.begin_capacity_evidence(
            server_request,
            state=server_state,
            environ=signing_environment_map,
            observed_at=observed_at,
        )
        server_response = SimpleNamespace(body=response_body, status=200, headers={})
        capacity.finish_capacity_evidence(
            server_request,
            server_response,
            observed_at=observed_at,
            result_count=1,
        )
        return httpx.Response(
            200,
            content=response_body,
            headers=server_response.headers,
            request=request,
        )

    return handler


def test_http_fetcher_accepts_real_server_signed_observation(monkeypatch):
    """The audit fetcher accepts a real isolated-process Ed25519 response."""

    trust, signing_environment_map, observed_at = _configure_signed_server(monkeypatch)
    server_state = capacity.CapacityEvidenceState()
    response_body = b'{"items":[{}]}'
    handler = _signed_evidence_response_handler(
        server_state=server_state,
        signing_environment_map=signing_environment_map,
        observed_at=observed_at,
        response_body=response_body,
    )
    collector = audit.CapacityEvidenceCollector(
        trust,
        contention_run_id=CONTENTION_RUN_ID,
        run_nonce=RUN_NONCE,
    )
    with _fetcher(handler, collector) as fetcher:
        plan = collector.plan(
            audit.CapacityObservationIntent(audit.CAPACITY_RANDOM_COHORT, True)
        )
        _payload, _latency, _retries, observations = fetcher._request_page(
            _capacity_query(),
            capacity_plan=plan,
        )
    collector.mark_semantic_outcome(observations, successful=True)
    report = collector.report()

    assert observations[0].verification_error is None
    assert observations[0].observation is not None
    assert report["counters"]["verified"] == 1
    assert report["counters"]["eligible"] == 1
    assert report["commitments"]["run_digest_count"] == 1


def test_api_signed_warm_observation_cannot_enter_cold_release_cohort(monkeypatch):
    """Local first-attempt labels cannot turn an API-signed warm row cold."""

    def collect(_headers, **kwargs):
        return _observation(kwargs["query_parameters"], ordinal=1)

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
        challenge=collector.fresh_challenge(),
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

    assert report["counters"]["verified"] == 1
    assert report["counters"]["eligible"] == 0
    assert report["counters"]["rejected"] == 1
    assert report["counters"]["rejections"]["api_signed_cold_not_true"] == 1
    assert (
        report["counters"]["rejections"][
            "api_signed_observation_ordinal_not_zero"
        ]
        == 1
    )


def test_retry_uses_fresh_challenge_and_cannot_rescue_eligibility(monkeypatch):
    challenges = []
    status_codes = []
    responses = collections.deque((503, 200))

    def collect(_headers, **kwargs):
        challenges.append(kwargs["challenge"])
        status_codes.append(kwargs["response_status_code"])
        if kwargs["response_status_code"] != 200:
            raise capacity.CapacityEvidenceError(
                "unexpected_status",
                "response_status",
            )
        return _observation(kwargs["query_parameters"], ordinal=1)

    monkeypatch.setattr(
        audit.capacity_evidence,
        "collect_capacity_http_observation",
        collect,
    )

    def handler(request):
        status = responses.popleft()
        return httpx.Response(status, json={"ok": True}, request=request)

    collector = audit.CapacityEvidenceCollector(
        TRUST,
        contention_run_id=CONTENTION_RUN_ID,
        run_nonce=RUN_NONCE,
    )
    with _fetcher(handler, collector, retries=1) as fetcher:
        plan = collector.plan(
            audit.CapacityObservationIntent(audit.CAPACITY_RANDOM_COHORT, True)
        )
        _payload, _latency, retries, observations = fetcher._request_page(
            _capacity_query(),
            capacity_plan=plan,
        )
    collector.mark_semantic_outcome(observations, successful=True)
    report = collector.report()

    assert retries == 1
    assert status_codes == [503, 200]
    assert len(challenges) == len(set(challenges)) == 2
    assert report["counters"]["physical"] == 2
    assert report["counters"]["verified"] == 1
    assert report["counters"]["eligible"] == 0
    assert report["counters"]["retries"] == 1
    assert report["counters"]["rejections"]["retry_attempt"] == 1


def test_later_page_retry_fails_complete_request_release_accounting(monkeypatch):
    """A retry after the signed first page must still fail the release audit."""

    monkeypatch.setattr(
        audit.capacity_evidence,
        "collect_capacity_http_observation",
        lambda _headers, **kwargs: _observation(kwargs["query_parameters"]),
    )
    later_page_attempt_counts = collections.Counter()

    def handler(request):
        offset = int(request.url.params["offset"])
        if offset == 100:
            later_page_attempt_counts["offset_100"] += 1
            if later_page_attempt_counts["offset_100"] == 1:
                return httpx.Response(503, json={"error": "retry"}, request=request)
        item_count = 100 if offset == 0 else 1
        payload = _page_payload(
            request.url.params,
            provider_items=[{"npi": index + offset} for index in range(item_count)],
            total=101,
            offset=offset,
        )
        return httpx.Response(200, json=payload, request=request)

    collector = audit.CapacityEvidenceCollector(
        TRUST,
        contention_run_id=CONTENTION_RUN_ID,
        run_nonce=RUN_NONCE,
    )
    with _fetcher(handler, collector, retries=1) as fetcher:
        fetch_result = fetcher.fetch_all(
            _capacity_query(),
            phase="cold",
            capacity_intent=audit.CapacityObservationIntent(
                audit.CAPACITY_RANDOM_COHORT,
                True,
            ),
        )
        collector.mark_semantic_outcome(
            fetch_result.capacity_observations,
            successful=True,
        )
        report = collector.report(
            complete_request_http_requests=fetcher.request_count,
            complete_request_retries=fetch_result.retries,
        )

    assert fetch_result.pages == 2
    assert fetch_result.retries == 1
    assert report["counters"]["retries"] == 0
    assert report["counters"]["reconciliation"]["release_clean"] is True
    assert "capacity_complete_request_accounting_failed" in (
        audit.capacity_evidence_release_failures(report)
    )


def test_timeout_and_planned_without_attempt_remain_reconciled_and_unclean():
    def handler(request):
        raise httpx.ReadTimeout("timed out", request=request)

    collector = audit.CapacityEvidenceCollector(
        TRUST,
        contention_run_id=CONTENTION_RUN_ID,
        run_nonce=RUN_NONCE,
    )
    collector.plan(
        audit.CapacityObservationIntent(audit.CAPACITY_NEGATIVE_COHORT, True)
    )
    attempted_plan = collector.plan(
        audit.CapacityObservationIntent(audit.CAPACITY_RANDOM_COHORT, True)
    )
    with _fetcher(handler, collector) as fetcher:
        with pytest.raises(audit.ApiError, match="transport_failure"):
            fetcher._request_page(
                _capacity_query(),
                capacity_plan=attempted_plan,
            )

    report = collector.report()
    counters = report["counters"]
    reconciliation = counters["reconciliation"]

    assert counters["planned"] == 2
    assert counters["physical"] == counters["rejected"] == 1
    assert counters["eligible"] == 0
    assert counters["timeouts"] == counters["transport_errors"] == 1
    assert counters["status"] == {"timeout": 1}
    assert reconciliation["accounted"] is True
    assert reconciliation["release_clean"] is False
    assert reconciliation["planned_without_physical_attempt"] == 1
    assert reconciliation["rejected_physical_attempts"] == 1
    assert reconciliation["unclassified_physical_attempts"] == 0


def test_capacity_observation_is_attached_only_to_page_one(monkeypatch):
    collected_offsets = []
    requests = []

    def collect(_headers, **kwargs):
        collected_offsets.append(kwargs["query_parameters"]["offset"])
        return _observation(kwargs["query_parameters"])

    monkeypatch.setattr(
        audit.capacity_evidence,
        "collect_capacity_http_observation",
        collect,
    )

    def handler(request):
        page_parameter_map = dict(request.url.params)
        offset = int(page_parameter_map["offset"])
        requests.append(
            (
                offset,
                capacity.CAPACITY_CHALLENGE_HEADER in request.headers,
                capacity.CAPACITY_RUN_NONCE_HEADER in request.headers,
            )
        )
        item_count = 100 if offset == 0 else 1
        return httpx.Response(
            200,
            json=_page_payload(
                page_parameter_map,
                provider_items=[{} for _index in range(item_count)],
                total=101,
                offset=offset,
            ),
            request=request,
        )

    collector = audit.CapacityEvidenceCollector(
        TRUST,
        contention_run_id=CONTENTION_RUN_ID,
        run_nonce=RUN_NONCE,
    )
    with _fetcher(handler, collector) as fetcher:
        fetch_result = fetcher.fetch_all(
            {
                name: parameter_value
                for name, parameter_value in _capacity_query().items()
                if name not in {"limit", "offset"}
            },
            phase="cold",
            capacity_intent=audit.CapacityObservationIntent(
                audit.CAPACITY_RANDOM_COHORT,
                True,
            ),
        )

    assert requests == [(0, True, True), (100, False, False)]
    assert collected_offsets == ["0"]
    assert len(fetch_result.capacity_observations) == 1


def test_redirect_is_passed_to_verifier_and_rejected(monkeypatch):
    redirects = []

    def collect(_headers, **kwargs):
        redirects.append(kwargs["response_redirect_count"])
        raise capacity.CapacityEvidenceError(
            "redirect_not_allowed",
            "response_redirect_count",
        )

    monkeypatch.setattr(
        audit.capacity_evidence,
        "collect_capacity_http_observation",
        collect,
    )

    def handler(request):
        return httpx.Response(
            302,
            headers={"Location": "/elsewhere"},
            content=b"redirect",
            request=request,
        )

    collector = audit.CapacityEvidenceCollector(
        TRUST,
        contention_run_id=CONTENTION_RUN_ID,
        run_nonce=RUN_NONCE,
    )
    with _fetcher(handler, collector) as fetcher:
        plan = collector.plan(
            audit.CapacityObservationIntent(audit.CAPACITY_NEGATIVE_COHORT, True)
        )
        with pytest.raises(audit.ApiError, match="non_success_status"):
            fetcher._request_page(_capacity_query(), capacity_plan=plan)

    report = collector.report()
    assert redirects == [1]
    assert report["counters"]["redirects"] == 1
    assert report["counters"]["rejections"]["redirect_not_allowed"] == 1


class _IntentFetcher:
    def __init__(self, config):
        self.config = config
        self.calls = []
        self.request_count = 0

    def fetch_all(
        self,
        params,
        *,
        phase,
        page_size=None,
        capacity_intent=None,
    ):
        self.calls.append((phase, page_size, capacity_intent))
        self.request_count += 1
        has_matching_code = str(params["code"]) != "00000"
        return audit.FetchResult(
            items=(),
            contracts=(_contract(self.config, matched=has_matching_code),),
            page_latencies_ms=(1.0,),
            total_latency_ms=1.0,
            pages=1,
            retries=0,
            response_fingerprint="77" * 32,
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


def _capacity_gate_cohort_reports(cohort_count_map, distinct_random):
    return {
        cohort: {
            "planned": count,
            "physical": count,
            "verified": count,
            "eligible": count,
            "rejected": 0,
            "distinct_semantic_queries": (
                distinct_random
                if cohort == audit.CAPACITY_RANDOM_COHORT
                else count
            ),
        }
        for cohort, count in cohort_count_map.items()
    }


def _capacity_gate_reconciliation(eligible):
    return {
        "contract": "planned_first_attempt_outcomes_v1",
        "accounted": True,
        "release_clean": True,
        "planned_first_physical_attempts": eligible,
        "plans_with_physical_attempt": eligible,
        "planned_without_physical_attempt": 0,
        "initial_physical_attempts": eligible,
        "retry_physical_attempts": 0,
        "invalid_attempt_index_records": 0,
        "eligible_physical_attempts": eligible,
        "rejected_physical_attempts": 0,
        "unclassified_physical_attempts": 0,
        "verified_rejected_physical_attempts": 0,
        "semantic_outcomes_without_physical_attempt": 0,
        "structural_failures": {},
    }


def _capacity_gate_counters(eligible, cohort_count_map, distinct_random):
    return {
        "planned": eligible,
        "physical": eligible,
        "verified": eligible,
        "eligible": eligible,
        "rejected": 0,
        "retries": 0,
        "timeouts": 0,
        "transport_errors": 0,
        "http_errors": 0,
        "verification_errors": 0,
        "redirects": 0,
        "status": {"200": eligible},
        "rejections": {},
        "cohorts": _capacity_gate_cohort_reports(
            cohort_count_map, distinct_random
        ),
        "reconciliation": _capacity_gate_reconciliation(eligible),
    }


def _capacity_gate_latency(eligible, cohort_count_map):
    return {
        "gate_source": audit.CAPACITY_CLIENT_LATENCY_SOURCE,
        "eligible_verified_client_http_duration_ms": {
            "count": eligible,
            "p95_ms": 12.0,
        },
        "eligible_verified_client_http_duration_ms_by_cohort": {
            cohort: {
                "count": count,
                "p95_ms": 12.0,
                "gate_p95_ms": 12.0,
            }
            for cohort, count in cohort_count_map.items()
        },
    }


def _capacity_gate_report(
    *,
    eligible=3_000,
    matched_positive_eligible=100,
    negative_eligible=400,
    random_eligible=2_500,
    distinct_random=2_500,
):
    """Build a clean release-gate report with configurable cohort totals."""

    cohort_count_map = {
        audit.CAPACITY_POSITIVE_COHORT: matched_positive_eligible,
        audit.CAPACITY_NEGATIVE_COHORT: negative_eligible,
        audit.CAPACITY_RANDOM_COHORT: random_eligible,
    }
    return {
        "enabled": True,
        "contract": "signed_isolated_standard_http_capacity_v3",
        "trust": TRUST.report_value,
        "selection": audit.sampling_seed_evidence(
            "release",
            audit.authoritative_release_seed(TRUST),
            TRUST,
        ),
        "counters": _capacity_gate_counters(
            eligible, cohort_count_map, distinct_random
        ),
        "page_contract": {
            "latency_gate": audit.CAPACITY_CLIENT_LATENCY_SOURCE,
        },
        "latency": _capacity_gate_latency(eligible, cohort_count_map),
        "complete_request_accounting": {
            "contract": "all_standard_http_attempts_v1",
            "actual_http_requests": eligible,
            "retries": 0,
            "release_clean": True,
        },
        "commitments": {
            "run_digest": "33" * 32,
            "run_digest_count": 1,
        },
    }


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


def _qualified_client_latency_report(monkeypatch):
    signed_duration_ns_by_class = {
        "matched_positive": 41_000_000,
        "negative": 10_000_000,
        "random": 10_000_000,
    }
    client_latency_ms_by_cohort = {
        audit.CAPACITY_POSITIVE_COHORT: 1.0,
        audit.CAPACITY_NEGATIVE_COHORT: 60.0,
        audit.CAPACITY_RANDOM_COHORT: 60.0,
    }

    def collect(_headers, **kwargs):
        observation = _observation(kwargs["query_parameters"])
        observation["server_duration_ns"] = signed_duration_ns_by_class[
            kwargs["expected_semantic_class"]
        ]
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
    for ordinal, cohort in enumerate(
        (
            audit.CAPACITY_POSITIVE_COHORT,
            audit.CAPACITY_NEGATIVE_COHORT,
            audit.CAPACITY_RANDOM_COHORT,
        )
    ):
        plan = collector.plan(audit.CapacityObservationIntent(cohort, True))
        observation_record = collector.collect_response(
            plan,
            challenge=collector.fresh_challenge(),
            query_parameters=_capacity_query(code=f"9921{ordinal}"),
            response_headers={},
            response_status_code=200,
            response_body=b"{}",
            response_redirect_count=0,
            collector_received_at=dt.datetime.now(dt.timezone.utc),
            attempt_index=0,
            latency_ms=client_latency_ms_by_cohort[cohort],
        )
        collector.mark_semantic_outcome((observation_record,), successful=True)
    return collector.report()


def test_top_level_latency_gate_uses_verified_client_http_timing(
    monkeypatch,
):
    """Gate end-to-end HTTP time after API evidence qualifies each response."""

    capacity_report = _qualified_client_latency_report(monkeypatch)
    assert capacity_report["latency"][
        "client_physical_attempt_ms"
    ]["p95_ms"] == 60.0
    assert capacity_report["latency"][
        "eligible_verified_client_http_duration_ms_by_cohort"
    ][audit.CAPACITY_POSITIVE_COHORT]["gate_p95_ms"] == 1.0
    assert capacity_report["latency"][
        "eligible_signed_server_duration_ms_by_cohort"
    ][audit.CAPACITY_POSITIVE_COHORT]["gate_p95_ms"] == 41.0

    runner = object.__new__(audit.AuditRunner)
    runner.config = _capacity_config()
    progress = audit._AuditProgress(
        first_page_first_observation_ms=[1.0],
        negative_first_page_ms=[60.0],
        random_cold_first_page_ms=[60.0],
        cold_page_ms=[1.0, 60.0, 60.0],
        positive_cold_query_ms=[2_000.0],
        negative_cold_query_ms=[2_000.0],
        random_cold_latency_ms=[2_000.0],
    )
    latency_reports = runner._latency_reports(progress, capacity_report)
    top_level_latency = audit.AuditRunner._latency_report(
        progress,
        latency_reports,
    )

    assert top_level_latency["gate_source"] == audit.CAPACITY_CLIENT_LATENCY_SOURCE
    assert top_level_latency["cold"]["first_page_first_observation"][
        "p95_ms"
    ] == 1.0
    assert top_level_latency["cold"]["first_page_first_observation"][
        "client_diagnostics"
    ]["by_class"]["matched_positive"]["p95_ms"] == 1.0
    assert top_level_latency["cold"]["first_page_first_observation"][
        "client_diagnostics"
    ]["by_class"]["negative"]["p95_ms"] == 60.0
    assert progress.failure_counts == {
        "negative_first_page_latency": 1,
        "random_first_page_latency": 1,
    }
    assert top_level_latency["cold"]["logical_query"]["diagnostic_only"] is True


def test_signed_latency_gate_uses_capacity_nearest_rank_p95():
    report = audit._latency_gate_summary([10.0] * 19 + [50.0])

    assert report["gate_p95_ms"] == 10.0


def _sha256(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _candidate_attestation_fixture_report():
    """Build the release-audit fixture with capacity evidence for attestation."""

    completed_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    started_at = completed_at - dt.timedelta(minutes=10)
    candidate_fixture_map = {
        "schema_version": 2,
        "harness": {"name": "ptg2_v3_source_api_audit", "version": "2.10.0"},
        "status": "pass",
        "profile": "release",
        "release_profile_enforced": True,
        "release_gate_eligible": True,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": 600.0,
        "target": {
            "expected_architecture": "postgres_binary_v3",
            "expected_storage_generation": "shared_blocks_v3",
            "expected_database_backend": "postgresql",
            "expected_snapshot_lifecycle": "validated",
            "architecture_assertion": "required_postgresql_session_evidence",
            "api_path_sha256": _sha256("/api/v1/pricing/providers/audit-search-by-procedure"),
            "api_audit_path_sha256": _sha256("/api/v1/pricing/providers/audit-occurrences"),
            "endpoint_contract": "pricing.providers.search_by_procedure",
            "audit_endpoint_contract": "persisted_served_occurrence_sample_v2",
            "snapshot_id_sha256": _sha256("snap_new"),
            "source_key_sha256": _sha256("source_a"),
            "plan_id_sha256": _sha256("12-3456789"),
            "market_type_sha256": _sha256("group"),
            "tls_verified": True,
        },
        "reproducibility": {},
        "source": {
            "provider_identifier_quarantine": (
                provider_identifier_quarantine_payload({})
            )
        },
        "coverage": {"failures": []},
        "checks": {
            "source_occurrence_ids": 2_500,
            "api_occurrence_ids": 2_500,
            "negative_queries": 250,
            "random_api_requests_executed": 2_500,
        },
        "http": {
            "standard_api_actual_http_requests": 3_000,
            "capacity_evidence": _capacity_gate_report(eligible=3_000),
        },
        "random_api_requests": {},
        "latency": {},
        "api_audit_sample": {
            "sample_digest": "ab" * 32,
            "sample_digest_validated": True,
            "source_set_validated": True,
        },
        "failures": {"counts": {}, "examples": []},
        "limitations": [],
        "redaction": {
            "policy": "sensitive_identifiers_excluded",
            "excluded": list(audit.REDACTION_EXCLUDED_FIELDS),
        },
    }

    return candidate_fixture_map


def test_capacity_section_preserves_candidate_attestation_fixture_contract():
    """Capacity evidence remains valid for the candidate attestation fixture."""

    evidence = ptg2_candidate_attestation.validate_candidate_release_audit_report(
        _candidate_attestation_fixture_report(),
        snapshot_id="snap_new",
        source_key="source_a",
        plan_id="12-3456789",
        plan_market_type="group",
    )
    assert evidence["standard_api_actual_http_requests"] == 3_000
