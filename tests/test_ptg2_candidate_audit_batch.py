from __future__ import annotations

import json
import types
from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock

import orjson
import pytest
from sanic.exceptions import Forbidden, InvalidUsage

from api import ptg2_candidate_audit_batch as batch
from api import ptg2_serving
from api.endpoint import pricing
from api.ptg2_candidate_audit import (
    PTG2_CANDIDATE_AUDIT_HEADER,
    PTG2CandidateAuditAccess,
)
from api.ptg2_candidate_audit_capacity import (
    PTG2_CANDIDATE_AUDIT_MAX_RETAINED_DECODED_BYTES,
)
from api.ptg2_response import _response_wire_value
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
    AuditBatchRequest,
    AuditBatchWitnessBinding,
    PTG2_AUDIT_BATCH_RESPONSE_CONTRACT,
    build_audit_batch_request,
    matched_audit_batch_digest,
)
from process.ptg_parts.ptg2_candidate_audit_evidence import (
    canonical_network_name_digests,
    canonical_tuple_digest_without_networks,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_shared_source_set import (
    ordered_source_ordinal_digest,
)
from scripts.validation import ptg2_v3_source_api_audit as source_audit


def _access() -> PTG2CandidateAuditAccess:
    return PTG2CandidateAuditAccess(
        snapshot_id="candidate-snapshot",
        source_key="source-a",
        plan_id="12-3456789",
        plan_market_type="group",
    )


def _price_payload() -> dict:
    return {
        "negotiated_type": "negotiated",
        "negotiated_rate": "123.45",
        "expiration_date": "2026-12-31",
        "service_code": ["11"],
        "billing_class": "professional",
        "setting": "office",
        "billing_code_modifier": ["25"],
        "additional_information": "test",
    }


def _challenge(*, multiplicity: int = 2) -> AuditBatchChallenge:
    query = source_audit.QueryKey("CPT", "99213", 1234567890)
    canonical_tuple = source_audit.CanonicalTuple.from_parts(
        query,
        "ffs",
        _price_payload(),
        billing_code_type_version="2026",
        name="Office visit",
        description="Established patient",
        network_names=("Alpha Network",),
    )
    return AuditBatchChallenge(
        code_system="CPT",
        code="99213",
        npi=1234567890,
        source_artifact_key=0,
        tuple_digest=canonical_tuple_digest_without_networks(canonical_tuple),
        network_name_digests=canonical_network_name_digests(("Alpha Network",)),
        multiplicity=multiplicity,
    )


def _audit_request(challenge_count: int = 2) -> AuditBatchRequest:
    return AuditBatchRequest(
        snapshot_id="candidate-snapshot",
        source_key="source-a",
        plan_id="12-3456789",
        plan_market_type="group",
        audit_sample_digest="c" * 64,
        source_witness_sample_digest="d" * 64,
        source_witness_payload_sha256="e" * 64,
        ordered_source_ordinal_digest=ordered_source_ordinal_digest(("a" * 64,)),
        challenge_count=challenge_count,
        request_digest="f" * 64,
    )


def _route_payload() -> dict:
    return build_audit_batch_request(
        snapshot_id="candidate-snapshot",
        source_key="source-a",
        plan_id="12-3456789",
        plan_market_type="group",
        witness_binding=AuditBatchWitnessBinding(
            audit_sample_digest="c" * 64,
            source_witness_sample_digest="d" * 64,
            source_witness_payload_sha256="e" * 64,
            raw_container_sha256=("a" * 64,),
            source_witness_occurrence_count=2,
        ),
    ).payload


def _route_request(
    request_payload: dict,
    *,
    snapshot_header: str | None,
    token: str = "operator-secret",
):
    header_map = {"Authorization": f"Bearer {token}"}
    if snapshot_header is not None:
        header_map[PTG2_CANDIDATE_AUDIT_HEADER] = snapshot_header
    return types.SimpleNamespace(
        body=orjson.dumps(request_payload),
        json=request_payload,
        headers=header_map,
        ctx=types.SimpleNamespace(sa_session=object()),
    )


def _audit_data(
    challenge: AuditBatchChallenge | None = None,
) -> batch._CandidateAuditData:
    selected_challenge = challenge or _challenge()
    available_challenge = _challenge()
    condition_key = (
        available_challenge.code_system,
        available_challenge.code,
        available_challenge.npi,
        available_challenge.source_artifact_key,
        available_challenge.tuple_digest,
    )
    return batch._CandidateAuditData(
        challenges=(selected_challenge,),
        witness_io={"payload_reads": 1},
        network_digest_sets_by_condition={
            condition_key: (
                frozenset(
                    canonical_network_name_digests(("Alpha Network", "Other"))
                ),
            )
        },
        candidate_processing_io={"candidate_projection_builds": 1},
    )


def test_challenge_match_uses_precomputed_network_digests(monkeypatch):
    monkeypatch.setattr(
        batch,
        "canonical_network_name_digests",
        lambda _network_names: pytest.fail("network digest was recomputed"),
    )

    assert batch._is_challenge_match(_challenge(), _audit_data()) is True


@pytest.mark.parametrize(
    (
        "reported_system",
        "reported_code",
        "query_system",
        "query_code",
        "rate",
        "source_name",
        "source_description",
    ),
    [
        (
            "CPT",
            "99213",
            "CPT",
            "99213",
            "123.4567890123456789",
            None,
            "",
        ),
        (
            "REVENUE_CODE",
            "450",
            "RC",
            "0450",
            Decimal("0.0000000000000000000125"),
            "Facility rate",
            None,
        ),
        (
            "PLACE_OF_SERVICE",
            "11",
            "POS",
            "11",
            "42",
            "Office",
            "Office setting",
        ),
    ],
)
def test_batch_projection_matches_public_exact_source_wire_tuple(
    reported_system,
    reported_code,
    query_system,
    query_code,
    rate,
    source_name,
    source_description,
):
    query = source_audit.QueryKey(query_system, query_code, 1234567890)
    code_fields_by_name = {
        "reported_code_system": reported_system,
        "reported_code": reported_code,
        "negotiation_arrangement": "ffs",
        "billing_code_type_version": None,
        "source_name": source_name,
        "source_description": source_description,
    }
    network_names = (" Alpha Network ", "Other")
    price_fields_by_name = {
        **_price_payload(),
        "negotiated_rate": rate,
        "service_code": ["11", "011"],
        "billing_code_modifier": ["tc", "25"],
    }
    public_item = ptg2_serving._ptg2_manifest_provider_procedure_item(
        npi=query.npi,
        serving_data={
            **code_fields_by_name,
            "source_procedure_name": source_name,
            "source_procedure_description": source_description,
            "network_names": list(network_names),
            "source_key": 0,
        },
        prices=[price_fields_by_name],
        procedure_detail={
            "procedure_name": "catalog fallback",
            "procedure_description": "catalog fallback",
        },
        provider_context=None,
        args={"mode": "exact_source", "source_key": "source-a"},
    )
    public_wire_item = _response_wire_value(public_item)
    public_tuple = source_audit.canonical_api_price_tuple(
        public_wire_item,
        public_wire_item["prices"][0],
        query,
    )

    batch_tuple = batch._build_canonical_candidate_tuple(
        query,
        code_fields_by_name,
        network_names,
        price_fields_by_name,
    )

    assert batch_tuple == public_tuple


class _TransactionSession:
    def __init__(self):
        self.calls = []

    async def execute(self, statement, params=None):
        self.calls.append((str(statement), dict(params or {})))


@pytest.mark.asyncio
async def test_batch_audit_uses_one_read_only_transaction_and_matches_multiplicity(
    monkeypatch,
):
    session = _TransactionSession()
    load_data = AsyncMock(return_value=_audit_data())
    monkeypatch.setattr(batch, "_candidate_audit_data", load_data)

    audit_result = await batch.audit_candidate_source_witness_batch(
        session,
        _audit_request(),
        _access(),
    )

    assert audit_result.matched_challenge_count == 2
    assert audit_result.unique_challenge_count == 1
    assert session.calls == [
        ("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", {})
    ]
    load_data.assert_awaited_once()
    assert (
        load_data.await_args.args[3].maximum_bytes
        == PTG2_CANDIDATE_AUDIT_MAX_RETAINED_DECODED_BYTES
    )


@pytest.mark.asyncio
async def test_batch_audit_fails_closed_on_any_unmatched_condition(monkeypatch):
    mismatched = replace(_challenge(), tuple_digest="0" * 64)
    monkeypatch.setattr(
        batch,
        "_candidate_audit_data",
        AsyncMock(return_value=_audit_data(mismatched)),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="missing"):
        await batch.audit_candidate_source_witness_batch(
            _TransactionSession(),
            _audit_request(),
            _access(),
        )


@pytest.mark.asyncio
async def test_batch_audit_rejects_capability_mismatch_before_database_access():
    session = _TransactionSession()
    mismatched_access = replace(_access(), source_key="other-source")

    with pytest.raises(PTG2ManifestArtifactError, match="access mismatch"):
        await batch.audit_candidate_source_witness_batch(
            session,
            _audit_request(),
            mismatched_access,
        )

    assert session.calls == []


@pytest.mark.asyncio
async def test_batch_audit_route_authorizes_exact_candidate_and_returns_ledger(
    monkeypatch,
):
    request_payload = _route_payload()
    request = _route_request(request_payload, snapshot_header="candidate-snapshot")
    resolver = AsyncMock(
        return_value=batch.CandidateAuditBatchResult(
            matched_challenge_count=2,
            unique_challenge_count=1,
            witness_io={"payload_reads": 1},
            candidate_processing_io={"candidate_projection_builds": 1},
            persisted_audit_occurrence_count=2,
            validated_persisted_audit_occurrence_count=2,
        )
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "operator-secret")
    monkeypatch.setattr(pricing, "audit_candidate_source_witness_batch", resolver)

    response = await pricing.audit_ptg2_source_witness_batch(request)

    response_payload = json.loads(response.body)
    assert response_payload == {
        "contract": PTG2_AUDIT_BATCH_RESPONSE_CONTRACT,
        "request_digest": request_payload["request_digest"],
        "challenge_count": 2,
        "unique_challenge_count": 1,
        "matched_challenge_count": 2,
        "persisted_audit_occurrence_count": 2,
        "validated_persisted_audit_occurrence_count": 2,
        "matched_challenge_digest": matched_audit_batch_digest(
            request_payload["request_digest"],
            2,
        ),
        "duration_ms": response_payload["duration_ms"],
        "block_io": {
            "logical_block_deliveries": 0,
            "physical_mapping_references": 0,
            "physical_mapping_aliases": 0,
            "unique_physical_blocks": 0,
            "physical_block_reads": 0,
            "physical_block_decodes": 0,
            "physical_payload_preparations": 0,
            "expected_logical_payload_processes": 0,
            "logical_payload_processes": 0,
            "logical_payload_fragment_references": 0,
            "logical_payload_fragment_aliases": 0,
            "repeated_physical_reads": 0,
            "repeated_physical_decodes": 0,
            "repeated_physical_preparations": 0,
            "repeated_logical_payload_processes": 0,
            "peak_raw_bytes": 0,
        },
        "witness_io": {"payload_reads": 1},
        "candidate_processing_io": {"candidate_projection_builds": 1},
    }
    assert response_payload["duration_ms"] >= 0
    parsed_request, candidate_access = resolver.await_args.args[1:]
    assert parsed_request.payload == request_payload
    assert candidate_access == _access()


@pytest.mark.parametrize(
    ("snapshot_header", "token"),
    [(None, "operator-secret"), ("other-candidate", "operator-secret"), ("candidate-snapshot", "wrong-token")],
)
@pytest.mark.asyncio
async def test_batch_audit_route_rejects_missing_or_mismatched_candidate_header(
    monkeypatch,
    snapshot_header,
    token,
):
    payload = _route_payload()
    request = _route_request(payload, snapshot_header=snapshot_header, token=token)
    resolver = AsyncMock()
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "operator-secret")
    monkeypatch.setattr(pricing, "audit_candidate_source_witness_batch", resolver)

    with pytest.raises(Forbidden, match="candidate audit|control API token"):
        await pricing.audit_ptg2_source_witness_batch(request)

    resolver.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_audit_route_fails_closed_on_incomplete_resolver_count(monkeypatch):
    request_payload = _route_payload()
    request = _route_request(request_payload, snapshot_header="candidate-snapshot")
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "operator-secret")
    monkeypatch.setattr(
        pricing,
        "audit_candidate_source_witness_batch",
        AsyncMock(
            return_value=batch.CandidateAuditBatchResult(
                matched_challenge_count=1,
                unique_challenge_count=1,
                witness_io={},
                candidate_processing_io={},
            )
        ),
    )

    with pytest.raises(InvalidUsage, match="incomplete match count"):
        await pricing.audit_ptg2_source_witness_batch(request)


@pytest.mark.asyncio
async def test_batch_audit_route_authenticates_before_json_parsing(monkeypatch):
    class UnparsedRequest:
        body = b"{}"
        headers = {}

        @property
        def json(self):
            raise AssertionError("JSON must not be parsed before authentication")

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "operator-secret")

    with pytest.raises(Forbidden, match="candidate audit"):
        await pricing.audit_ptg2_source_witness_batch(UnparsedRequest())
