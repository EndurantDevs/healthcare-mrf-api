# See LICENSE.

from __future__ import annotations

import asyncio

import hashlib

import json

from dataclasses import replace

from types import SimpleNamespace

from unittest.mock import AsyncMock, patch

import aiohttp

import pytest

import uvloop

from aiohttp import web

from process.ptg_parts import ptg2_candidate_audit_evidence as evidence

from process.ptg_parts import ptg2_fast_candidate_audit as audit

from process.ptg_parts.ptg2_candidate_audit_evidence import source_challenge

from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)

from process.ptg_parts.ptg2_source_witness import (
    LoadedSourceWitness,
    PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
    SourceWitnessRecord,
)

from tests.test_ptg2_fast_candidate_audit import (
    AUDIT_SAMPLE_DIGEST,
    PAYLOAD_DIGEST,
    SAMPLE_DIGEST,
    SOURCE_DIGEST,
    SOURCE_SET_DIGEST,
    _FakeClient,
    _FakeRequestContext,
    _FakeResponse,
    _ImmediateSemaphore,
    _api_contract_payload,
    _api_item,
    _api_occurrence,
    _candidate_audit_test_app,
    _http,
    _linked_provider,
    _occurrence_record,
    _preflight_payload,
    _provider_record,
    _source_identity,
    _target,
    _witness,
)

@pytest.mark.asyncio
async def test_fast_audit_fails_unrounded_p95_above_audit_only_ceiling(monkeypatch):
    async def preflight(_client, _semaphore, metrics, target):
        metrics.request_count += 1
        metrics.latencies_ms.append(1.0)
        return dict(target.audit_sample)

    async def challenge(_client, _semaphore, metrics, _target, _selected):
        metrics.request_count += 1
        metrics.latencies_ms.append(250.0004)

    monkeypatch.setattr(audit, "_validate_audit_sample_preflight", preflight)
    monkeypatch.setattr(audit, "_run_challenge", challenge)

    report = await audit.run_fast_candidate_audit(
        witness=_witness(20),
        audit_target=_target(),
        http=_http(),
    )

    assert report["latency"]["request_p95_ms"] == 250.0
    assert report["latency"]["request_p95_within_ceiling"] is False
    assert report["status"] == "fail"
    assert report["release_gate_eligible"] is False
    assert report["failures"]["counts"] == {"audit_request_p95_exceeded": 1}

@pytest.mark.asyncio
async def test_fast_audit_cancels_all_requests_at_hard_deadline(monkeypatch):
    cancelled = asyncio.Event()

    async def preflight(_client, _semaphore, metrics, target):
        metrics.request_count += 1
        return dict(target.audit_sample)

    async def challenge(*_args):
        try:
            await asyncio.sleep(5)
        finally:
            cancelled.set()

    monkeypatch.setattr(audit, "_validate_audit_sample_preflight", preflight)
    monkeypatch.setattr(audit, "_run_challenge", challenge)

    with pytest.raises(
        audit.FastCandidateAuditError,
        match="audit_deadline_exceeded",
    ):
        await audit.run_fast_candidate_audit(
            witness=_witness(1),
            audit_target=_target(),
            http=_http(deadline_seconds=1.0),
        )

    assert cancelled.is_set()

@pytest.mark.asyncio
async def test_fast_audit_cancels_requests_when_parent_worker_is_cancelled(monkeypatch):
    all_started = asyncio.Event()
    challenge_count_by_state = {"cancelled": 0, "started": 0}

    async def preflight(_client, _semaphore, metrics, target):
        metrics.request_count += 1
        return dict(target.audit_sample)

    async def challenge(*_args):
        challenge_count_by_state["started"] += 1
        if challenge_count_by_state["started"] == 4:
            all_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            challenge_count_by_state["cancelled"] += 1

    monkeypatch.setattr(audit, "_validate_audit_sample_preflight", preflight)
    monkeypatch.setattr(audit, "_run_challenge", challenge)

    audit_task = asyncio.create_task(
        audit.run_fast_candidate_audit(
            witness=_witness(4),
            audit_target=_target(),
            http=_http(),
        )
    )
    await asyncio.wait_for(all_started.wait(), timeout=1.0)
    audit_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await audit_task

    assert challenge_count_by_state["cancelled"] == 4

@pytest.mark.asyncio
async def test_fast_response_body_rejects_oversized_payload(monkeypatch):
    class Content:
        async def iter_chunked(self, _chunk_size):
            yield b"12345"

    monkeypatch.setattr(audit, "FAST_AUDIT_MAX_RESPONSE_BYTES", 4)

    with pytest.raises(audit.FastCandidateAuditError, match="too_large"):
        await audit._bounded_response_body(type("Response", (), {"content": Content()})())

@pytest.mark.asyncio
async def test_request_json_retries_retryable_status_once(monkeypatch):
    async def response_body(response):
        return response.body

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(audit, "_bounded_response_body", response_body)
    monkeypatch.setattr(audit.asyncio, "sleep", no_sleep)
    metrics = audit.FastAuditHttpMetrics()

    response = await audit._request_json(
        _FakeClient(_FakeResponse(503, b"{}"), _FakeResponse(200, b"{}")),
        _ImmediateSemaphore(),
        metrics,
        "/test",
        {},
    )

    assert response == {}
    assert metrics.request_count == 2
    assert metrics.retry_count == 1

@pytest.mark.parametrize(
    ("responses_or_errors", "message"),
    [
        ((_FakeResponse(400, b"{}"),), "non_success_status"),
        ((_FakeResponse(200, b"["),), "json_invalid"),
        ((_FakeResponse(200, b"[]"),), "not_object"),
        ((asyncio.TimeoutError(), asyncio.TimeoutError()), "request_timeout"),
        (
            (aiohttp.ClientConnectionError(), aiohttp.ClientConnectionError()),
            "transport_failure",
        ),
    ],
)
@pytest.mark.asyncio
async def test_request_json_fails_closed_after_protocol_or_transport_error(
    monkeypatch,
    responses_or_errors,
    message,
):
    async def response_body(response):
        return response.body

    monkeypatch.setattr(audit, "_bounded_response_body", response_body)
    metrics = audit.FastAuditHttpMetrics()

    with pytest.raises(audit.FastCandidateAuditError, match=message):
        await audit._request_json(
            _FakeClient(*responses_or_errors),
            _ImmediateSemaphore(),
            metrics,
            "/test",
            {},
        )

def test_contract_errors_report_malformed_database_and_result_state():
    assert audit._contract_errors({}, _target(), positive=True)
    payload = _api_contract_payload(_target(), response_items=[])
    payload["result_state"] = "no_matching_rates"
    payload["provenance"]["database_evidence"]["database_selected"] = False

    errors = audit._contract_errors(payload, _target(), positive=True)

    assert "database_evidence_mismatch" in errors
    assert "positive_result_state_mismatch" in errors

def test_challenge_params_include_optional_service_code_and_modifiers():
    challenge = replace(
        source_challenge(_occurrence_record()),
        service_codes=("11",),
        modifiers=("25", "59"),
    )

    params = audit._challenge_params(_target(), challenge, offset=100)

    assert params["service_code"] == "11"
    assert params["billing_code_modifier"] == "25,59"
    assert params["offset"] == 100

@pytest.mark.parametrize(
    "mutation",
    [
        lambda payload: payload.update(items=[None]),
        lambda payload: payload["pagination"].update(offset=1),
        lambda payload: payload["pagination"].update(limit=1),
        lambda payload: payload["pagination"].update(total=0),
    ],
)
def test_candidate_page_rejects_item_or_pagination_drift(mutation):
    payload = _api_contract_payload(_target(), response_items=[])
    mutation(payload)

    with pytest.raises(audit.FastCandidateAuditError):
        audit._validated_candidate_page(
            payload,
            _target(),
            requested_offset=0,
            declared_total=1,
        )

@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda payload: payload["query"].update(
                snapshot_id="different-snapshot"
            ),
            "api_audit_contract_mismatch",
        ),
        (lambda payload: payload["source_set"].update(source_count=2), "source_set"),
        (lambda payload: payload.update(audit_sample={}), "audit_sample"),
        (
            lambda payload: payload["audit_sample"].update(sample_digest="0" * 64),
            "audit_sample",
        ),
        (lambda payload: payload.update(items=[]), "item_missing"),
    ],
)
@pytest.mark.asyncio
async def test_preflight_rejects_source_sample_or_item_drift(
    monkeypatch,
    mutation,
    message,
):
    payload = _preflight_payload()
    mutation(payload)

    async def request_json(*_args, **_kwargs):
        return payload

    monkeypatch.setattr(audit, "_request_json", request_json)

    with pytest.raises(audit.FastCandidateAuditError, match=message):
        await audit._validate_audit_sample_preflight(
            object(),
            _ImmediateSemaphore(),
            audit.FastAuditHttpMetrics(),
            _target(),
        )

@pytest.mark.asyncio
async def test_fast_audit_rejects_witness_count_drift_before_http():
    witness = _witness(1)
    witness = replace(
        witness,
        metadata={
            **witness.metadata,
            "occurrence_witness_count": 2,
        },
    )

    with pytest.raises(
        audit.FastCandidateAuditError,
        match="source_witness_challenge_count_mismatch",
    ):
        await audit.run_fast_candidate_audit(
            witness=witness,
            audit_target=_target(),
            http=_http(),
        )

@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "response_items",
        "total",
        "schema_errors",
        "maximum_pages",
        "expected_error",
    ),
    (
        ([{}], 2, ("invalid tuple",), 2, "api_tuple_schema_mismatch"),
        ([{}], 1, (), 2, "source_witness_missing_from_api"),
        ([], 1, (), 2, "api_pagination_stalled"),
        ([{}], 2, (), 1, "api_challenge_page_limit"),
    ),
)
async def test_challenge_pagination_fails_closed(
    monkeypatch,
    response_items,
    total,
    schema_errors,
    maximum_pages,
    expected_error,
):
    monkeypatch.setattr(audit, "FAST_AUDIT_MAX_PAGES", maximum_pages)
    monkeypatch.setattr(
        audit,
        "_request_json",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        audit,
        "_validated_candidate_page",
        lambda *_args, **_kwargs: audit._CandidatePage(
            response_items=response_items,
            total=total,
        ),
    )
    monkeypatch.setattr(
        audit.source_audit,
        "extract_api_tuples",
        lambda *_args, **_kwargs: SimpleNamespace(
            schema_errors=schema_errors,
            tuples={},
        ),
    )

    with pytest.raises(audit.FastCandidateAuditError, match=expected_error):
        await audit._run_challenge(
            object(),
            _ImmediateSemaphore(),
            audit.FastAuditHttpMetrics(),
            _target(),
            source_challenge(_occurrence_record()),
        )
