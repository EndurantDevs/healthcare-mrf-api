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
                {"npi": [3_234_567_890, str(1_234_567_890 + index)]},
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

def _candidate_audit_test_app(
    audit_target: audit.FastAuditTarget,
    witness_record: SourceWitnessRecord,
    recorded_queries: list[dict[str, str]],
) -> web.Application:
    async def challenge_handler(request: web.Request) -> web.Response:
        recorded_queries.append(dict(request.query))
        assert request.path == audit.source_audit.DEFAULT_CANDIDATE_API_PATH
        expected_query_by_name = {
            "snapshot_id": audit_target.snapshot_id,
            "source_key": audit_target.source_key,
            "code_system": "CPT",
            "code": "99213",
            "npi": "1234567890",
            "negotiated_rate": "123.45",
            "negotiated_rate_tolerance": "0",
        }
        assert {
            name: request.query[name] for name in expected_query_by_name
        } == expected_query_by_name
        assert request.headers["Authorization"] == "Bearer test"
        return web.json_response(
            _api_contract_payload(
                audit_target,
                response_items=[_api_item(witness_record)],
            )
        )

    async def preflight_handler(request: web.Request) -> web.Response:
        recorded_queries.append(dict(request.query))
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
    app.router.add_get(
        audit.source_audit.DEFAULT_CANDIDATE_API_PATH, challenge_handler
    )
    app.router.add_get(
        audit.source_audit.DEFAULT_API_AUDIT_PATH, preflight_handler
    )
    return app

def test_release_runtime_recognizes_uvloop():
    async def active_contract() -> str:
        return audit._event_loop_contract(require_uvloop=True)

    assert uvloop.run(active_contract()) == "uvloop"

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
