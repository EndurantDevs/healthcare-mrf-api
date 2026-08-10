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

def test_candidate_evidence_covers_inline_provider_guards():
    occurrence_witness = _occurrence_record()
    coordinate_by_field = {"provider_group_ordinal": 0, "npi_ordinal": 0}
    inline_evidence_by_field = {
        "source_kind": "inline_provider_group",
        **coordinate_by_field,
    }
    raw_inline_by_field = {
        "provider_groups": [{"npi": [1_234_567_890]}],
        "network_names": ["Inline Network"],
    }

    assert evidence._inline_provider_evidence(
        raw_inline_by_field,
        inline_evidence_by_field,
    ) == (1_234_567_890, [])
    with pytest.raises(
        evidence.FastCandidateAuditError,
        match="source_inline_provider_evidence_invalid",
    ):
        evidence._inline_provider_evidence(
            raw_inline_by_field,
            {**inline_evidence_by_field, "extra": True},
        )

    inline_record = replace(
        occurrence_witness,
        provider_evidence=inline_evidence_by_field,
        linked_provider_sha256=None,
        linked_provider_json=None,
    )
    assert evidence._source_npi_and_networks(
        inline_record,
        raw_inline_by_field,
        None,
    )[0] == 1_234_567_890

def test_candidate_evidence_covers_referenced_provider_guards():
    occurrence_witness = _occurrence_record()
    raw_rate = json.loads(occurrence_witness.raw_json)

    with pytest.raises(
        evidence.FastCandidateAuditError,
        match="source_referenced_provider_evidence_invalid",
    ):
        evidence._referenced_provider_evidence(
            occurrence_witness,
            raw_rate,
            {"source_kind": "provider_reference"},
            None,
        )
    with pytest.raises(
        evidence.FastCandidateAuditError,
        match="source_linked_provider_missing",
    ):
        evidence._referenced_provider_evidence(
            replace(occurrence_witness, linked_provider_json=None),
            raw_rate,
            occurrence_witness.provider_evidence,
            None,
        )
    with pytest.raises(
        evidence.FastCandidateAuditError,
        match="source_provider_references_invalid",
    ):
        evidence._referenced_provider_evidence(
            occurrence_witness,
            {**raw_rate, "provider_references": []},
            occurrence_witness.provider_evidence,
            None,
        )
    with pytest.raises(
        evidence.FastCandidateAuditError,
        match="source_provider_evidence_kind_invalid",
    ):
        evidence._source_npi_and_networks(
            replace(
                occurrence_witness,
                provider_evidence={"source_kind": "unknown"},
            ),
            raw_rate,
            None,
        )

@pytest.mark.parametrize(
    ("record", "reason"),
    [
        (
            replace(_occurrence_record(), kind="unknown"),
            "source_occurrence_witness_invalid",
        ),
        (
            replace(_occurrence_record(), expected={"contract": "unknown"}),
            "source_occurrence_expected_contract_invalid",
        ),
        (
            replace(
                _occurrence_record(),
                procedure={
                    **_occurrence_record().procedure,
                    "billing_code": "",
                },
            ),
            "source_rate_query_invalid",
        ),
        (
            replace(_occurrence_record(), coordinate=(7, 0, 99, 0)),
            "source_rate_price_coordinate_invalid",
        ),
    ],
)
def test_candidate_evidence_rejects_invalid_rate_occurrences(record, reason):
    with pytest.raises(evidence.FastCandidateAuditError, match=reason):
        evidence.source_challenge(record)

def test_candidate_evidence_rejects_invalid_provider_witness_shapes():
    with pytest.raises(
        evidence.FastCandidateAuditError,
        match="source_provider_witness_invalid",
    ):
        evidence.validate_provider_witness(
            replace(_provider_record(), kind="rate_occurrence")
        )

    malformed_providers = (
        (
            {"provider_group_id": 1, "provider_groups": [1]},
            "source_provider_group_invalid",
        ),
        (
            {"provider_group_id": 1, "provider_groups": [{"npi": "bad"}]},
            "source_provider_npi_invalid",
        ),
        (
            {"provider_group_id": 1, "provider_groups": [{"npi": [True]}]},
            "source_provider_npi_invalid",
        ),
    )
    for raw_provider, reason in malformed_providers:
        raw_json = json.dumps(raw_provider, separators=(",", ":")).encode()
        with pytest.raises(evidence.FastCandidateAuditError, match=reason):
            evidence.validate_provider_witness(
                replace(_provider_record(), raw_json=raw_json)
            )

def test_candidate_evidence_tuple_matching_fail_closed_paths():
    challenge = evidence.source_challenge(_occurrence_record())
    tuple_digest = evidence.canonical_tuple_digest_without_networks(
        challenge.expected_tuple
    )
    assert evidence.is_canonical_tuple_digest_match(
        challenge.expected_tuple,
        tuple_digest,
    )
    assert not evidence.is_tuple_matching_challenge(
        "{",
        challenge.expected_tuple,
        challenge,
    )
    assert not evidence.is_tuple_matching_challenge(
        "{}",
        challenge.expected_tuple,
        challenge,
    )
    occurrence_key = json.dumps(
        {"raw_container_sha256": challenge.raw_source_sha256}
    )
    assert not evidence.is_tuple_matching_challenge(
        occurrence_key,
        replace(challenge.expected_tuple, code="other"),
        challenge,
    )
    assert evidence.is_tuple_matching_challenge(
        occurrence_key,
        challenge.expected_tuple,
        challenge,
    )

def test_source_challenge_derives_later_price_and_provider_from_raw_evidence():
    challenge = source_challenge(_occurrence_record())

    assert challenge.query == audit.source_audit.QueryKey("CPT", "99213", 1_234_567_890)
    assert challenge.negotiated_rate == "123.45"
    assert challenge.service_codes == ("11",)
    assert challenge.required_network_names == (
        "Provider Network",
        "Rate Network",
    )

def test_source_challenge_rejects_broken_rate_to_provider_link():
    record = _occurrence_record()
    changed_provider = json.loads(record.linked_provider_json)
    changed_provider["provider_group_id"] = 2
    linked_raw = json.dumps(changed_provider, separators=(",", ":")).encode()

    with pytest.raises(
        audit.FastCandidateAuditError,
        match="source_provider_reference_link_mismatch",
    ):
        source_challenge(
            replace(
                record,
                linked_provider_json=linked_raw,
                linked_provider_sha256=hashlib.sha256(linked_raw).hexdigest(),
            )
        )

def test_provider_witness_validates_only_independently_derivable_claims():
    audit.validate_provider_witness(_provider_record())
    unsupported_expected_by_field = {
        **dict(_provider_record().expected),
        "network_names": ["scanner-only"],
    }

    with pytest.raises(
        audit.FastCandidateAuditError,
        match="source_provider_expected_contract_invalid",
    ):
        audit.validate_provider_witness(
            replace(_provider_record(), expected=unsupported_expected_by_field)
        )

def test_candidate_no_match_is_reported_as_missing_source_witness():
    with pytest.raises(
        audit.FastCandidateAuditError,
        match="source_witness_missing_from_api",
    ):
        audit._validated_candidate_page(
            {
                "result_state": "no_match_in_radius",
                "items": [],
                "pagination": {
                    "offset": 0,
                    "limit": audit.FAST_AUDIT_PAGE_SIZE,
                    "total": 0,
                },
            },
            _target(),
            requested_offset=0,
            declared_total=None,
        )

def test_candidate_page_rejects_contract_drift():
    payload = _api_contract_payload(_target(), response_items=[])
    payload["query"]["snapshot_id"] = "different-snapshot"

    with pytest.raises(
        audit.FastCandidateAuditError,
        match="api_contract_mismatch",
    ):
        audit._validated_candidate_page(
            payload,
            _target(),
            requested_offset=0,
            declared_total=None,
        )

@pytest.mark.asyncio
async def test_http_latency_excludes_time_waiting_for_concurrency_slot(monkeypatch):
    events: list[str] = []
    clock_values = iter((10.0, 10.025))

    class Semaphore:
        async def __aenter__(self):
            events.append("slot-acquired")

        async def __aexit__(self, *_args):
            return None

    class Response:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    class Client:
        def get(self, *_args, **_kwargs):
            return Response()

    def perf_counter():
        events.append("clock-read")
        return next(clock_values)

    async def response_body(_response):
        return b"{}"

    monkeypatch.setattr(audit.time, "perf_counter", perf_counter)
    monkeypatch.setattr(audit, "_bounded_response_body", response_body)
    metrics = audit.FastAuditHttpMetrics()

    response = await audit._request_json(
        Client(),
        Semaphore(),
        metrics,
        "/test",
        {},
    )

    assert response == {}
    assert events[:2] == ["slot-acquired", "clock-read"]
    assert metrics.latencies_ms == pytest.approx([25.0])

@pytest.mark.asyncio
async def test_source_challenge_uses_candidate_api_with_exact_filters(
    unused_tcp_port,
):
    """Exercise the exact public filters and preflight against a live aiohttp app."""

    witness_record = _occurrence_record()
    challenge = source_challenge(witness_record)
    audit_target = _target()
    recorded_queries: list[dict[str, str]] = []
    app = _candidate_audit_test_app(
        audit_target, witness_record, recorded_queries
    )
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", unused_tcp_port)
    await site.start()
    metrics = audit.FastAuditHttpMetrics()
    try:
        async with aiohttp.ClientSession(
            base_url=f"http://127.0.0.1:{unused_tcp_port}",
            headers={"Authorization": "Bearer test"},
        ) as client:
            observed_sample = await audit._validate_audit_sample_preflight(
                client,
                asyncio.Semaphore(1),
                metrics,
                audit_target,
            )
            await audit._run_challenge(
                client,
                asyncio.Semaphore(1),
                metrics,
                audit_target,
                challenge,
            )
    finally:
        await runner.cleanup()

    assert observed_sample == audit.public_audit_sample_projection(
        audit_target.audit_sample
    )
    assert len(recorded_queries) == 2
    assert metrics.request_count == 2

@pytest.mark.asyncio
async def test_fast_audit_executes_full_release_witness_contract(
    monkeypatch,
):
    observed_fingerprints: list[str] = []

    async def preflight(_client, _semaphore, metrics, target):
        metrics.request_count += 1
        metrics.latencies_ms.append(1.0)
        return dict(target.audit_sample)

    async def challenge(_client, _semaphore, metrics, _target, selected):
        metrics.request_count += 1
        metrics.latencies_ms.append(2.0)
        observed_fingerprints.append(selected.fingerprint)
        await asyncio.sleep(0)

    monkeypatch.setattr(audit, "_validate_audit_sample_preflight", preflight)
    monkeypatch.setattr(audit, "_run_challenge", challenge)

    report = await audit.run_fast_candidate_audit(
        witness=_witness(
            PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
            PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
        ),
        audit_target=_target(),
        http=_http(),
    )

    assert len(observed_fingerprints) == 10_000
    assert report["status"] == "pass"
    assert report["duration_seconds"] < audit.FAST_AUDIT_DEADLINE_SECONDS
    assert report["checks"]["source_witnesses"] == 11_000
    assert report["checks"]["api_witnesses_matched"] == 10_000
    assert report["checks"]["provider_witnesses_validated"] == 1_000
    assert report["http"]["standard_api_actual_http_requests"] == 10_001
    assert report["latency"]["request_p95_ceiling_ms"] == 250.0
    assert report["latency"]["request_p95_within_ceiling"] is True
    assert report["random_api_requests"] == {
        "requested": 10_000,
        "executed": 10_000,
    }
