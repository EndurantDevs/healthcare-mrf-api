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
