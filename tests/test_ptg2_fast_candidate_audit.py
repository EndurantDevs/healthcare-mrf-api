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


SOURCE_DIGEST = "11" * 32
SOURCE_SET_DIGEST = "22" * 32
SAMPLE_DIGEST = "33" * 32
PAYLOAD_DIGEST = "44" * 32
AUDIT_SAMPLE_DIGEST = "55" * 32


def _linked_provider(index: int = 0) -> bytes:
    return json.dumps(
        {
            "provider_group_id": 1,
            "network_name": ["Provider Network"],
            "provider_groups": [
                {"npi": [2_234_567_890]},
                {"npi": [3_234_567_890, 1_234_567_890 + index]},
            ],
        },
        separators=(",", ":"),
    ).encode()


def _occurrence_record(index: int = 0) -> SourceWitnessRecord:
    raw_json = json.dumps(
        {
            "negotiated_prices": [
                {
                    "negotiated_type": "negotiated",
                    "negotiated_rate": 9.99,
                    "expiration_date": "2026-01-01",
                    "service_code": ["22"],
                    "billing_class": "institutional",
                    "setting": "inpatient",
                    "billing_code_modifier": ["ZZ"],
                    "additional_information": "unselected",
                },
                {
                    "negotiated_type": "negotiated",
                    "negotiated_rate": 123.45,
                    "expiration_date": "2027-01-01",
                    "service_code": ["11"],
                    "billing_class": "professional",
                    "setting": "outpatient",
                    "billing_code_modifier": [],
                    "additional_information": None,
                },
            ],
            "provider_references": [1],
            "network_names": ["Rate Network"],
        },
        separators=(",", ":"),
    ).encode()
    linked_provider = _linked_provider(index)
    return SourceWitnessRecord(
        kind="rate_occurrence",
        priority=index,
        tie_breaker=hashlib.sha256(f"occurrence:{index}".encode()).hexdigest(),
        coordinate=(7, index, 1, 0),
        raw_source_sha256=SOURCE_DIGEST,
        raw_sha256=hashlib.sha256(raw_json).hexdigest(),
        linked_provider_sha256=hashlib.sha256(linked_provider).hexdigest(),
        procedure={
            "billing_code_type": "CPT",
            "billing_code": "99213",
            "negotiation_arrangement": "ffs",
            "billing_code_type_version": "2026",
            "name": "Office visit",
            "description": "Established patient",
        },
        provider_evidence={
            "source_kind": "provider_reference",
            "provider_reference_id": "1",
            "provider_group_ordinal": 1,
            "npi_ordinal": 1,
        },
        expected={"contract": "ptg2_v3_source_rate_occurrence_expected_v2"},
        raw_json=raw_json,
        linked_provider_json=linked_provider,
    )


def _provider_record(index: int = 0) -> SourceWitnessRecord:
    provider_id = index + 1
    raw_json = json.dumps(
        {
            "provider_group_id": provider_id,
            "provider_groups": [{"npi": [1_234_567_890 + index]}],
        },
        separators=(",", ":"),
    ).encode()
    return SourceWitnessRecord(
        kind="provider_reference",
        priority=index,
        tie_breaker=hashlib.sha256(f"provider:{index}".encode()).hexdigest(),
        coordinate=(index, 0, 0, 0),
        raw_source_sha256=SOURCE_DIGEST,
        raw_sha256=hashlib.sha256(raw_json).hexdigest(),
        linked_provider_sha256=None,
        procedure=None,
        provider_evidence=None,
        expected={
            "contract": "ptg2_v3_source_provider_expected_v2",
            "provider_group_id": str(provider_id),
        },
        raw_json=raw_json,
        linked_provider_json=None,
    )


@pytest.mark.parametrize(
    ("invoke", "reason"),
    [
        (
            lambda: evidence._json_object(
                b"{}",
                field_name="field",
                evidence_sha256="missing",
                parsed_evidence_by_sha256={},
            ),
            "field_parsed_evidence_missing",
        ),
        (
            lambda: evidence._json_object(b"{", field_name="field"),
            "field_json_invalid",
        ),
        (
            lambda: evidence._json_object(b"[]", field_name="field"),
            "field_not_object",
        ),
        (lambda: evidence._mapping([], field_name="field"), "field_invalid"),
        (lambda: evidence._strict_index(-1, field_name="field"), "field_invalid"),
        (
            lambda: evidence._strict_provider_reference_id(
                "1",
                field_name="field",
            ),
            "field_invalid",
        ),
        (lambda: evidence._provider_groups({}, field_name="field"), "field_invalid"),
    ],
)
def test_candidate_evidence_rejects_invalid_primitive_shapes(invoke, reason):
    with pytest.raises(evidence.FastCandidateAuditError, match=reason):
        invoke()


@pytest.mark.parametrize(
    ("groups", "reason"),
    [
        ([], "source_provider_group_coordinate_invalid"),
        ([{}], "source_provider_npi_coordinate_invalid"),
        ([{"npi": [1]}], "source_provider_npi_invalid"),
    ],
)
def test_candidate_evidence_rejects_invalid_provider_coordinates(groups, reason):
    with pytest.raises(evidence.FastCandidateAuditError, match=reason):
        evidence._npi_at_source_coordinate(
            groups,
            {"provider_group_ordinal": 0, "npi_ordinal": 0},
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


def test_candidate_evidence_wraps_canonical_tuple_failures():
    with patch.object(
        evidence.source_audit.CanonicalTuple,
        "from_parts",
        side_effect=ValueError("bad tuple"),
    ):
        with pytest.raises(
            evidence.FastCandidateAuditError,
            match="source_rate_tuple_invalid",
        ):
            evidence.source_challenge(_occurrence_record())


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


def _witness(
    occurrence_count: int,
    provider_count: int = 0,
) -> LoadedSourceWitness:
    occurrences = tuple(_occurrence_record(index) for index in range(occurrence_count))
    providers = tuple(_provider_record(index) for index in range(provider_count))
    witness_records = (*occurrences, *providers)
    return LoadedSourceWitness(
        metadata={
            "contract": PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
            "format_version": 5,
            "selection_method": PTG2_V3_SOURCE_WITNESS_SELECTION,
            "population_semantics": "queryable_emitted_price_provider_occurrence_v1",
            "unqueryable_rate_policy": "count_but_exclude_from_npi_api_challenges_v1",
            "source_count": 1,
            "source_set_digest": SOURCE_SET_DIGEST,
            "occurrence_target": PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
            "total_target": PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
            "provider_quota": PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
            "sample_digest": SAMPLE_DIGEST,
            "payload_sha256": PAYLOAD_DIGEST,
            "payload_bytes": 1024,
            "compression": "per_record_zlib_shared_evidence_dictionary_v1",
            "queryable_occurrence_population_count": occurrence_count,
            "provider_population_count": provider_count,
            "emitted_rate_row_count": max(1, occurrence_count),
            "unqueryable_rate_row_count": 0,
            "occurrence_witness_count": occurrence_count,
            "provider_witness_count": provider_count,
            "record_count": len(witness_records),
            "evidence_dictionary_count": occurrence_count,
            "evidence_dictionary_raw_bytes": occurrence_count * 100,
            "evidence_dictionary_stored_bytes": occurrence_count * 50,
        },
        records=witness_records,
    )


def _target() -> audit.FastAuditTarget:
    return audit.FastAuditTarget(
        snapshot_id="snapshot-1",
        source_key="source-1",
        plan_id="12-3456789",
        plan_market_type="group",
        source_count=1,
        source_set_digest=SOURCE_SET_DIGEST,
        audit_sample={
            "contract": "persisted_served_occurrence_sample_v2",
            "format_version": 2,
            "method": "publish_time_stratified_v1",
            "sample_count": 1,
            "maximum_rows": 2_560,
            "sample_digest": AUDIT_SAMPLE_DIGEST,
            "source_count": 1,
            "occurrence_identity": "sha256_candidate_ordinal_source_key_v2",
            "complete_population": False,
            "serving_multiplicity_semantics": "source_multiset_v1",
            "work": {"combination_attempts": 2_560},
        },
        provider_identifier_quarantine=provider_identifier_quarantine_payload({}),
    )


def _http(*, deadline_seconds: float = 55.0) -> audit.FastAuditHttpConfig:
    return audit.FastAuditHttpConfig(
        api_base_url="http://candidate-api.default.svc.cluster.local:8080",
        headers={"Authorization": "Bearer test"},
        verify_tls=False,
        transport_contract="authenticated_cluster_service_v1",
        deadline_seconds=deadline_seconds,
        require_uvloop=False,
    )


def _api_contract_payload(
    audit_target: audit.FastAuditTarget,
    *,
    response_items: list[dict],
) -> dict:
    return {
        "result_state": "matched",
        "pricing_scope": audit.source_audit.EXPECTED_PRICING_SCOPE,
        "resolved_snapshot_id": audit_target.snapshot_id,
        "query": {
            "snapshot_id": audit_target.snapshot_id,
            "plan_id": audit_target.plan_id,
            "mode": audit.source_audit.EXPECTED_QUERY_MODE,
            "source_key": audit_target.source_key,
        },
        "provenance": {
            "arch_version": audit.source_audit.EXPECTED_ARCHITECTURE,
            "storage_generation": audit.source_audit.EXPECTED_STORAGE_GENERATION,
            "database_backend": audit.source_audit.EXPECTED_DATABASE_BACKEND,
            "database_evidence": {
                "contract": audit.source_audit.DATABASE_EVIDENCE_CONTRACT,
                "server_version_num": 180000,
                "database_selected": True,
                "backend_session_active": True,
                "transaction_snapshot_observed": True,
            },
            "plan_id": audit_target.plan_id,
            "snapshot_id": audit_target.snapshot_id,
            "mode": audit.source_audit.EXPECTED_QUERY_MODE,
            "pricing_scope": audit.source_audit.EXPECTED_PRICING_SCOPE,
            "source_key": audit_target.source_key,
        },
        "items": response_items,
        "pagination": {"offset": 0, "limit": audit.FAST_AUDIT_PAGE_SIZE, "total": 1},
    }


def _source_identity(record: SourceWitnessRecord) -> dict:
    return {
        "source_artifact_key": 0,
        "source_key": "source-1",
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": "aa" * 32,
        "raw_container_sha256": record.raw_source_sha256,
        "logical_json_sha256": "aa" * 32,
        "logical_hash_deferred": False,
        "source_trace_set_hash": "bb" * 32,
        "source_trace": [{"source_file_version_id": "source-file-0"}],
    }


def _api_item(record: SourceWitnessRecord) -> dict:
    challenge = source_challenge(record)
    tuple_payload = challenge.expected_tuple.payload
    price_by_field = {
        key: tuple_payload[key]
        for key in audit.source_audit.PRICE_FIELDS
    }
    price_by_field["negotiated_rate"] = 123.45
    return {
        "reported_code_system": tuple_payload["code_system"],
        "reported_code": tuple_payload["code"],
        "npi": tuple_payload["npi"],
        "negotiation_arrangement": tuple_payload["negotiation_arrangement"],
        "billing_code_type_version": tuple_payload["billing_code_type_version"],
        "procedure_name": tuple_payload["name"],
        "procedure_description": tuple_payload["description"],
        "network_names": [
            *tuple_payload["network_names"],
            "Network From Another Referenced Group",
        ],
        "prices": [price_by_field],
        **_source_identity(record),
    }


def _api_occurrence(record: SourceWitnessRecord) -> dict:
    challenge = source_challenge(record)
    tuple_payload = challenge.expected_tuple.payload
    tuple_payload["negotiated_rate"] = 123.45
    return {
        "occurrence_id": "cc" * 32,
        "tuple": tuple_payload,
        **_source_identity(record),
    }


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


def test_release_runtime_recognizes_uvloop():
    async def active_contract() -> str:
        return audit._event_loop_contract(require_uvloop=True)

    assert uvloop.run(active_contract()) == "uvloop"


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
    requests: list[dict[str, str]] = []

    async def handler(request: web.Request) -> web.Response:
        requests.append(dict(request.query))
        assert request.path == audit.source_audit.DEFAULT_CANDIDATE_API_PATH
        assert request.query["snapshot_id"] == audit_target.snapshot_id
        assert request.query["source_key"] == audit_target.source_key
        assert request.query["code_system"] == "CPT"
        assert request.query["code"] == "99213"
        assert request.query["npi"] == "1234567890"
        assert request.query["negotiated_rate"] == "123.45"
        assert request.query["negotiated_rate_tolerance"] == "0"
        assert request.headers["Authorization"] == "Bearer test"
        return web.json_response(
            _api_contract_payload(
                audit_target,
                response_items=[_api_item(witness_record)],
            )
        )

    async def preflight_handler(request: web.Request) -> web.Response:
        requests.append(dict(request.query))
        assert request.path == audit.source_audit.DEFAULT_API_AUDIT_PATH
        assert request.query["limit"] == "1"
        response_fields = _api_contract_payload(
            audit_target,
            response_items=[_api_occurrence(witness_record)],
        )
        response_fields["source_set"] = {
            "contract": audit.source_audit.SOURCE_SET_CONTRACT,
            "source_count": audit_target.source_count,
            "raw_container_sha256_digest": audit_target.source_set_digest,
        }
        response_fields["audit_sample"] = audit.public_audit_sample_projection(
            audit_target.audit_sample
        )
        response_fields["pagination"] = {"offset": 0, "limit": 1, "total": 1}
        return web.json_response(response_fields)

    app = web.Application()
    app.router.add_get(audit.source_audit.DEFAULT_CANDIDATE_API_PATH, handler)
    app.router.add_get(
        audit.source_audit.DEFAULT_API_AUDIT_PATH,
        preflight_handler,
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
    assert len(requests) == 2
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


class _ImmediateSemaphore:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class _FakeResponse:
    def __init__(self, status, body):
        self.status = status
        self.body = body


class _FakeRequestContext:
    def __init__(self, response_or_error):
        self.response_or_error = response_or_error

    async def __aenter__(self):
        if isinstance(self.response_or_error, BaseException):
            raise self.response_or_error
        return self.response_or_error

    async def __aexit__(self, *_args):
        return None


class _FakeClient:
    def __init__(self, *responses_or_errors):
        self.responses_or_errors = list(responses_or_errors)

    def get(self, *_args, **_kwargs):
        return _FakeRequestContext(self.responses_or_errors.pop(0))


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


def _preflight_payload():
    payload = _api_contract_payload(
        _target(),
        response_items=[_api_occurrence(_occurrence_record())],
    )
    payload["source_set"] = {
        "contract": audit.source_audit.SOURCE_SET_CONTRACT,
        "source_count": 1,
        "raw_container_sha256_digest": SOURCE_SET_DIGEST,
    }
    payload["audit_sample"] = audit.public_audit_sample_projection(
        _target().audit_sample
    )
    payload["pagination"] = {"offset": 0, "limit": 1, "total": 1}
    return payload


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
