from __future__ import annotations

import asyncio
import json
import types
from dataclasses import replace
from unittest.mock import AsyncMock

import orjson
import pytest

from api import ptg2_candidate_audit_batch as candidate_batch
from api import ptg2_candidate_audit_partition as candidate_partition
from api.endpoint import pricing
from api.ptg2_candidate_audit import (
    PTG2_CANDIDATE_AUDIT_HEADER,
    PTG2CandidateAuditAccess,
)
from api.ptg2_shared_blocks import PTG2SharedBlockError
from api.ptg2_candidate_audit_capacity import (
    PTG2_CANDIDATE_AUDIT_PARTITION_MAX_RETAINED_DECODED_BYTES,
    CandidateAuditProcessAdmission,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts import ptg2_partitioned_candidate_audit_contract as contract
from process.ptg_parts.ptg2_candidate_audit_evidence import (
    canonical_network_name_digests,
)


def _access():
    return PTG2CandidateAuditAccess(
        snapshot_id="candidate-snapshot",
        source_key="test-source",
        plan_id="12-3456789",
        plan_market_type="group",
    )


def _request():
    plan = contract.build_partitioned_candidate_audit_plan(
        binding=contract.PartitionedCandidateAuditBinding(
            snapshot_id="candidate-snapshot",
            source_key="test-source",
            plan_id="12-3456789",
            plan_market_type="group",
            audit_sample_digest="a" * 64,
            source_witness_sample_digest="b" * 64,
            source_witness_payload_sha256="c" * 64,
            ordered_source_ordinal_digest="d" * 64,
            source_occurrence_count=1,
            persisted_occurrence_count=1,
        ),
        source_challenges=(
            contract.PartitionedSourceChallenge(
                ordinal=0,
                code_system="CPT",
                code="99213",
                npi=1_234_567_890,
                source_artifact_key=0,
                tuple_digest="e" * 64,
                network_name_digests=("f" * 64,),
                multiplicity=1,
            ),
        ),
        persisted_occurrences=(
            contract.PartitionedPersistedOccurrence(
                ordinal=0,
                occurrence_id=b"g" * 32,
                code_system="CPT",
                code="99213",
                code_key=7,
                provider_set_key=8,
                price_key=9,
                source_artifact_key=0,
                npi=1_234_567_890,
                atom_ordinal=0,
                atom_key=10,
            ),
        ),
    )
    return plan.requests[0]


class _Session:
    def __init__(self):
        self.calls = []

    async def execute(self, statement, _params=None):
        self.calls.append(str(statement))


def _block_io():
    return {
        "logical_block_deliveries": 1,
        "physical_mapping_references": 1,
        "physical_mapping_aliases": 0,
        "unique_physical_blocks": 1,
        "physical_block_reads": 1,
        "physical_block_decodes": 1,
        "physical_payload_preparations": 1,
        "expected_logical_payload_processes": 1,
        "logical_payload_processes": 1,
        "logical_payload_fragment_references": 1,
        "logical_payload_fragment_aliases": 0,
        "repeated_physical_reads": 0,
        "repeated_physical_decodes": 0,
        "repeated_physical_preparations": 0,
        "repeated_logical_payload_processes": 0,
        "peak_raw_bytes": 1024,
    }


def _candidate_io():
    return {
        "candidate_occurrence_deliveries": 1,
        "unique_candidate_projections": 1,
        "candidate_projection_builds": 1,
        "candidate_projection_reuse_deliveries": 0,
        "repeated_candidate_projection_builds": 0,
        "availability_condition_count": 1,
        "duplicate_availability_deliveries": 0,
    }


@pytest.mark.asyncio
async def test_partition_resolver_receives_only_explicit_coordinates(monkeypatch):
    audit_request = _request()
    session = _Session()
    monkeypatch.setattr(
        candidate_batch, "snapshot_serving_tables", AsyncMock(return_value=object())
    )
    binding_validator = AsyncMock()
    monkeypatch.setattr(candidate_partition, "_validate_partition_binding", binding_validator)
    challenge = audit_request.source_challenges[0]
    condition_key = (
        challenge.code_system,
        challenge.code,
        challenge.npi,
        challenge.source_artifact_key,
        challenge.tuple_digest,
    )
    data_loader = AsyncMock(
        return_value=candidate_batch._CandidateAuditData(
            challenges=candidate_partition._partition_challenges(
                audit_request
            ),
            witness_io={},
            network_digest_sets_by_condition={
                condition_key: (
                    frozenset(
                        canonical_network_name_digests(("ignored",))
                    )
                    | frozenset(challenge.network_name_digests),
                )
            },
            candidate_processing_io=_candidate_io(),
            persisted_audit_occurrence_count=1,
        )
    )
    monkeypatch.setattr(
        candidate_batch,
        "_candidate_data_for_conditions",
        data_loader,
    )

    audit_result = await candidate_partition.audit_candidate_partition(
        session,
        audit_request,
        _access(),
    )

    assert audit_result.matched_challenge_count == 1
    assert audit_result.validated_persisted_audit_occurrence_count == 1
    assert len(session.calls) == 1
    kwargs = data_loader.await_args.kwargs
    assert len(kwargs["challenges"]) == 1
    assert len(kwargs["persisted_audit_occurrences"]) == 1
    assert kwargs["retention_budget"].maximum_bytes == (
        PTG2_CANDIDATE_AUDIT_PARTITION_MAX_RETAINED_DECODED_BYTES
    )
    binding_validator.assert_awaited_once()


@pytest.mark.asyncio
async def test_partition_access_mismatch_precedes_database_access():
    session = _Session()
    mismatched = PTG2CandidateAuditAccess(
        snapshot_id="candidate-snapshot",
        source_key="other-source",
        plan_id="12-3456789",
        plan_market_type="group",
    )

    with pytest.raises(Exception, match="access mismatch"):
        await candidate_partition.audit_candidate_partition(
            session,
            _request(),
            mismatched,
        )

    assert session.calls == []


@pytest.mark.asyncio
async def test_route_returns_strict_partition_result(monkeypatch):
    audit_request = _request()
    raw_payload = audit_request.payload
    web_request = types.SimpleNamespace(
        body=orjson.dumps(raw_payload),
        json=raw_payload,
        headers={
            "Authorization": "Bearer operator-secret",
            PTG2_CANDIDATE_AUDIT_HEADER: "candidate-snapshot",
        },
        ctx=types.SimpleNamespace(sa_session=object()),
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "operator-secret")
    resolver = AsyncMock(
        return_value=candidate_batch.CandidateAuditBatchResult(
            matched_challenge_count=1,
            unique_challenge_count=1,
            witness_io={},
            candidate_processing_io=_candidate_io(),
            persisted_audit_occurrence_count=1,
            validated_persisted_audit_occurrence_count=1,
        )
    )
    monkeypatch.setattr(pricing, "audit_candidate_partition", resolver)
    monkeypatch.setattr(
        pricing,
        "_read_once_block_io_map",
        lambda _scope: _block_io(),
    )

    response = await pricing.audit_ptg2_source_witness_batch(web_request)

    response_payload = json.loads(response.body)
    parsed_result = contract.parse_partitioned_candidate_audit_result(
        response_payload,
        request=audit_request,
    )
    assert parsed_result.item_count == 2
    assert parsed_result.matched_source_occurrence_count == 1
    assert parsed_result.validated_persisted_occurrence_count == 1
    resolver.assert_awaited_once()


@pytest.mark.asyncio
async def test_partition_route_reports_process_admission_rejection(
    monkeypatch,
):
    audit_request = _request()
    web_request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=object()),
    )
    resolver = AsyncMock()
    monkeypatch.setattr(pricing, "audit_candidate_partition", resolver)
    monkeypatch.setattr(pricing, "_candidate_audit_batch_raw_limit", lambda: 100)
    monkeypatch.setattr(
        pricing,
        "_candidate_audit_process_admission",
        lambda: CandidateAuditProcessAdmission(100),
    )

    with pytest.raises(
        Exception,
        match="partition requires .* process limit",
    ) as exc_info:
        await pricing._partitioned_candidate_audit_response(
            web_request,
            audit_request.payload,
            _access(),
        )

    assert type(exc_info.value).__name__ == "ServiceUnavailable"
    resolver.assert_not_awaited()


@pytest.mark.asyncio
async def test_partition_route_cancellation_releases_process_weight_once(
    monkeypatch,
):
    audit_request = _request()
    web_request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=object()),
    )
    raw_limit = 100
    admission = CandidateAuditProcessAdmission(
        pricing._candidate_audit_partition_weight(raw_limit)
    )
    monkeypatch.setattr(
        pricing,
        "_candidate_audit_batch_raw_limit",
        lambda: raw_limit,
    )
    monkeypatch.setattr(
        pricing,
        "_candidate_audit_process_admission",
        lambda: admission,
    )
    monkeypatch.setattr(
        pricing,
        "audit_candidate_partition",
        AsyncMock(side_effect=asyncio.CancelledError()),
    )

    with pytest.raises(asyncio.CancelledError):
        await pricing._partitioned_candidate_audit_response(
            web_request,
            audit_request.payload,
            _access(),
        )

    assert admission.snapshot.retained_bytes == 0
    assert admission.snapshot.admitted_count == 1
    assert admission.snapshot.released_count == 1


def _serving_tables(audit_request):
    binding = audit_request.binding
    return types.SimpleNamespace(
        source_witness={
            "occurrence_witness_count": binding.source_occurrence_count,
            "sample_digest": binding.source_witness_sample_digest,
            "payload_sha256": binding.source_witness_payload_sha256,
        },
        audit_sample={
            "sample_count": binding.persisted_occurrence_count,
            "sample_digest": binding.audit_sample_digest,
        },
        source_set="sealed-source-set",
    )


@pytest.mark.parametrize("invalid_field", ["source_witness", "audit_sample"])
@pytest.mark.asyncio
async def test_partition_binding_requires_both_sealed_metadata_maps(
    monkeypatch,
    invalid_field,
):
    audit_request = _request()
    serving_tables = _serving_tables(audit_request)
    setattr(serving_tables, invalid_field, None)
    monkeypatch.setattr(candidate_batch, "_required_source_count", lambda _tables: 1)

    with pytest.raises(PTG2ManifestArtifactError, match="sealed audit metadata"):
        await candidate_partition._validate_partition_binding(
            object(),
            serving_tables,
            audit_request,
        )


@pytest.mark.asyncio
async def test_partition_binding_rejects_sealed_count_or_digest_drift(monkeypatch):
    audit_request = _request()
    serving_tables = _serving_tables(audit_request)
    serving_tables.audit_sample["sample_count"] = 2
    monkeypatch.setattr(candidate_batch, "_required_source_count", lambda _tables: 1)

    with pytest.raises(PTG2ManifestArtifactError, match="sealed audit metadata"):
        await candidate_partition._validate_partition_binding(
            object(),
            serving_tables,
            audit_request,
        )


@pytest.mark.asyncio
async def test_partition_binding_translates_source_identity_read_failure(monkeypatch):
    audit_request = _request()
    serving_tables = _serving_tables(audit_request)
    monkeypatch.setattr(candidate_batch, "_required_source_count", lambda _tables: 1)
    monkeypatch.setattr(
        candidate_partition,
        "fetch_snapshot_source_set_identity",
        AsyncMock(side_effect=PTG2SharedBlockError("identity unavailable")),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="identity unavailable"):
        await candidate_partition._validate_partition_binding(
            object(),
            serving_tables,
            audit_request,
        )


@pytest.mark.asyncio
async def test_partition_binding_accepts_matching_sealed_source_scope(monkeypatch):
    audit_request = _request()
    serving_tables = _serving_tables(audit_request)
    source_identity = AsyncMock(
        return_value=(
            serving_tables.source_set,
            audit_request.binding.ordered_source_ordinal_digest,
            ("a" * 64,),
        )
    )
    monkeypatch.setattr(candidate_batch, "_required_source_count", lambda _tables: 1)
    monkeypatch.setattr(
        candidate_partition,
        "fetch_snapshot_source_set_identity",
        source_identity,
    )

    await candidate_partition._validate_partition_binding(
        object(),
        serving_tables,
        audit_request,
    )

    source_identity.assert_awaited_once()


@pytest.mark.parametrize("scope_drift", ["source_set", "ordinal", "source_key"])
@pytest.mark.asyncio
async def test_partition_binding_rejects_source_scope_drift(
    monkeypatch,
    scope_drift,
):
    audit_request = _request()
    serving_tables = _serving_tables(audit_request)
    monkeypatch.setattr(candidate_batch, "_required_source_count", lambda _tables: 1)
    observed_source_set = (
        "other-source-set" if scope_drift == "source_set" else serving_tables.source_set
    )
    observed_ordinal_digest = (
        "0" * 64
        if scope_drift == "ordinal"
        else audit_request.binding.ordered_source_ordinal_digest
    )
    monkeypatch.setattr(
        candidate_partition,
        "fetch_snapshot_source_set_identity",
        AsyncMock(
            return_value=(observed_source_set, observed_ordinal_digest, ("a" * 64,))
        ),
    )
    if scope_drift == "source_key":
        audit_request = replace(
            audit_request,
            source_challenges=(
                replace(audit_request.source_challenges[0], source_artifact_key=1),
            ),
        )

    with pytest.raises(PTG2ManifestArtifactError, match="source scope"):
        await candidate_partition._validate_partition_binding(
            object(),
            serving_tables,
            audit_request,
        )


@pytest.mark.asyncio
async def test_partition_rejects_unmatched_source_condition(monkeypatch):
    audit_request = _request()
    session = _Session()
    monkeypatch.setattr(
        candidate_batch,
        "snapshot_serving_tables",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        candidate_partition,
        "_validate_partition_binding",
        AsyncMock(),
    )
    monkeypatch.setattr(
        candidate_batch,
        "_candidate_data_for_conditions",
        AsyncMock(
            return_value=candidate_batch._CandidateAuditData(
                challenges=candidate_partition._partition_challenges(audit_request),
                witness_io={},
                network_digest_sets_by_condition={},
                candidate_processing_io=_candidate_io(),
                persisted_audit_occurrence_count=1,
            )
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="missing"):
        await candidate_partition.audit_candidate_partition(
            session,
            audit_request,
            _access(),
        )
