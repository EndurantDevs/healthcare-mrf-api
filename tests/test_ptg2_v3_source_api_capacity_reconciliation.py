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
