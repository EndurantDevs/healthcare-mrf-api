from __future__ import annotations

import types
from copy import deepcopy
from dataclasses import replace
from unittest.mock import Mock

import pytest

from process.ptg_parts import ptg2_partitioned_candidate_audit_contract as contract
from process.ptg_parts import ptg2_partitioned_candidate_audit as audit
from process.ptg_parts import ptg2_partitioned_candidate_audit_request_contract as request_contract
from process.ptg_parts import ptg2_partitioned_candidate_audit_types as audit_types


def _binding(source_count: int = 1, persisted_count: int = 1):
    return contract.PartitionedCandidateAuditBinding(
        snapshot_id="candidate-snapshot",
        source_key="test-source",
        plan_id="12-3456789",
        plan_market_type="group",
        audit_sample_digest="a" * 64,
        source_witness_sample_digest="b" * 64,
        source_witness_payload_sha256="c" * 64,
        ordered_source_ordinal_digest="d" * 64,
        source_occurrence_count=source_count,
        persisted_occurrence_count=persisted_count,
    )


def _source(*, code: str = "99213", npi: int = 1_234_567_890):
    return contract.PartitionedSourceChallenge(
        ordinal=0,
        code_system="CPT",
        code=code,
        npi=npi,
        source_artifact_key=0,
        tuple_digest=f"{npi:064x}",
        network_name_digests=(),
        multiplicity=1,
    )


def _persisted(*, occurrence_id: bytes = b"p" * 32):
    return contract.PartitionedPersistedOccurrence(
        ordinal=0,
        occurrence_id=occurrence_id,
        code_system="CPT",
        code="99213",
        code_key=7,
        provider_set_key=8,
        price_key=9,
        source_artifact_key=0,
        npi=1_234_567_890,
        atom_ordinal=0,
        atom_key=10,
    )


def _plan():
    return contract.build_partitioned_candidate_audit_plan(
        binding=_binding(),
        source_challenges=(_source(),),
        persisted_occurrences=(_persisted(),),
    )


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
        "peak_raw_bytes": 1,
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


def _loaded_worker_witness():
    return types.SimpleNamespace(
        provider_records=("provider-record",),
        occurrence_records=("occurrence-record",),
        evidence_by_sha256={"evidence": {}},
        metadata={
            "sample_digest": "b" * 64,
            "payload_sha256": "c" * 64,
            "occurrence_witness_count": 1,
        },
    )


def _persisted_worker_sample():
    persisted_record = types.SimpleNamespace(
        occurrence_id=b"p" * 32,
        code_system="CPT",
        code="99213",
        code_key=7,
        provider_set_key=8,
        price_key=9,
        source_artifact_key=0,
        npi=1_234_567_890,
        atom_ordinal=0,
        atom_key=10,
    )
    return types.SimpleNamespace(sample_count=1, records=(persisted_record,))


def _worker_audit_target():
    return types.SimpleNamespace(
        snapshot_id="candidate-snapshot",
        source_key="test-source",
        plan_id="12-3456789",
        plan_market_type="group",
        audit_sample={"sample_digest": "a" * 64},
        raw_container_sha256=("1" * 64,),
    )


def _grouped_worker_challenge():
    return types.SimpleNamespace(
        code_system="CPT",
        code="99213",
        npi=1_234_567_890,
        source_artifact_key=0,
        tuple_digest="e" * 64,
        network_name_digests=("f" * 64,),
        multiplicity=1,
    )


def _result(request=None):
    request = request or _plan().requests[0]
    return contract.build_partitioned_candidate_audit_result(
        request=request,
        matched_source_occurrence_count=request.source_occurrence_count,
        validated_persisted_occurrence_count=len(request.persisted_occurrences),
        duration_ms=1,
        block_io=_block_io(),
        candidate_processing_io=_candidate_io(),
    )


@pytest.mark.parametrize(
    ("validator", "value"),
    [
        (lambda value: audit_types.lower_hex(value, field_name="digest"), "A" * 64),
        (lambda value: audit_types.bounded_text(value, field_name="text"), None),
        (lambda value: audit_types.bounded_text(value, field_name="text"), "\n"),
        (lambda value: audit_types.nonnegative_integer(value, field_name="count"), True),
        (audit_types.valid_npi, 999),
    ],
)
def test_scalar_contracts_reject_noncanonical_values(validator, value):
    with pytest.raises(ValueError):
        validator(value)


@pytest.mark.parametrize(
    "challenge",
    [
        replace(_source(), network_name_digests=("e" * 64, "e" * 64)),
        replace(
            _source(),
            network_name_digests=tuple(f"{index:064x}" for index in range(65)),
        ),
    ],
)
def test_plan_rejects_noncanonical_network_digest_sets(challenge):
    with pytest.raises(ValueError, match="network_digests"):
        contract.build_partitioned_candidate_audit_plan(
            binding=_binding(),
            source_challenges=(challenge,),
            persisted_occurrences=(_persisted(),),
        )


@pytest.mark.parametrize("occurrence_id", ["not-bytes", b"short"])
def test_plan_rejects_invalid_persisted_occurrence_ids(occurrence_id):
    with pytest.raises(ValueError, match="occurrence_id"):
        contract.build_partitioned_candidate_audit_plan(
            binding=_binding(),
            source_challenges=(_source(),),
            persisted_occurrences=(_persisted(occurrence_id=occurrence_id),),
        )


@pytest.mark.parametrize(
    ("binding", "sources", "persisted", "message"),
    [
        (_binding(2, 1), (_source(), _source()), (_persisted(),), "duplicate_item"),
        (
            _binding(1, 2),
            (_source(),),
            (_persisted(), _persisted()),
            "duplicate_item",
        ),
        (_binding(2, 1), (_source(),), (_persisted(),), "sealed_count_mismatch"),
    ],
)
def test_plan_rejects_empty_duplicate_or_mismatched_populations(
    binding,
    sources,
    persisted,
    message,
):
    with pytest.raises(ValueError, match=message):
        contract.build_partitioned_candidate_audit_plan(
            binding=binding,
            source_challenges=sources,
            persisted_occurrences=persisted,
        )


@pytest.mark.parametrize(
    ("sources", "persisted"),
    [((), (_persisted(),)), ((_source(),), ())],
)
def test_validated_populations_require_both_audit_cohorts(sources, persisted):
    with pytest.raises(ValueError, match="population_empty"):
        request_contract._validated_plan_populations(sources, persisted)


def test_partition_packing_combines_small_groups_and_handles_exact_boundary():
    small_groups = tuple(
        _source(code="99213", npi=1_000_000_000 + index)
        for index in range(20)
    ) + tuple(
        _source(code="99214", npi=2_000_000_000 + index)
        for index in range(30)
    )
    exact_group_items = tuple(
        _source(npi=3_000_000_000 + index) for index in range(100)
    )

    assert [
        len(partition)
        for partition in request_contract._partition_items(small_groups)
    ] == [50]
    assert [
        len(partition)
        for partition in request_contract._partition_items(exact_group_items)
    ] == [50, 50]


def test_partition_plan_processes_each_source_and_persisted_record_once(monkeypatch):
    """Build one request after validating each local audit record once."""

    loaded_witness = _loaded_worker_witness()
    candidate_target = _worker_audit_target()
    provider_validator = Mock()
    condition_builder = Mock(return_value="source-condition")
    challenge_grouper = Mock(return_value=(_grouped_worker_challenge(),))
    monkeypatch.setattr(audit, "validate_provider_witness", provider_validator)
    monkeypatch.setattr(audit, "source_audit_condition", condition_builder)
    monkeypatch.setattr(audit, "group_audit_batch_challenges", challenge_grouper)

    partition_plan = audit.build_candidate_audit_partition_plan(
        audit_target=candidate_target,
        witness=loaded_witness,
        persisted_sample=_persisted_worker_sample(),
    )

    provider_validator.assert_called_once_with(
        "provider-record",
        parsed_evidence_by_sha256=loaded_witness.evidence_by_sha256,
    )
    condition_builder.assert_called_once_with(
        "occurrence-record",
        parsed_evidence_by_sha256=loaded_witness.evidence_by_sha256,
    )
    challenge_grouper.assert_called_once_with(
        candidate_target.raw_container_sha256,
        ("source-condition",),
    )
    assert partition_plan.binding.source_occurrence_count == 1
    assert partition_plan.binding.persisted_occurrence_count == 1
    assert partition_plan.requests[0].item_count == 2
    assert len(partition_plan.requests) == 1


@pytest.mark.parametrize(
    "mutation",
    [
        lambda payload: payload.clear(),
        lambda payload: payload.update(contract="unsupported"),
        lambda payload: payload.update(source_challenges={}),
        lambda payload: payload["source_challenges"][0].pop("code"),
        lambda payload: payload["source_challenges"][0].update(network_name_digests=()),
        lambda payload: payload["persisted_occurrences"][0].pop("code"),
    ],
)
def test_request_parser_rejects_framing_and_item_shape_drift(mutation):
    payload = deepcopy(_plan().requests[0].payload)
    mutation(payload)

    with pytest.raises(ValueError):
        contract.parse_partitioned_candidate_audit_request(payload)


def test_request_parser_rejects_duplicate_contiguous_ordinal():
    plan = _plan()
    original = plan.requests[0]
    duplicate_ordinal_request = request_contract._partition_request(
        binding=original.binding,
        source_challenge_count=1,
        plan_digest=original.plan_digest,
        partition_index=0,
        partition_count=1,
        partition_items=(
            replace(original.source_challenges[0], ordinal=0),
            replace(original.persisted_occurrences[0], ordinal=0),
        ),
    )

    with pytest.raises(ValueError, match="duplicate_ordinal"):
        contract.parse_partitioned_candidate_audit_request(
            duplicate_ordinal_request.payload
        )


@pytest.mark.parametrize(
    "raw_result",
    [
        None,
        {},
        {**_result().payload, "contract": "unsupported"},
        {**_result().payload, "duration_ms": True},
    ],
)
def test_result_parser_rejects_invalid_framing(raw_result):
    with pytest.raises(ValueError):
        contract.parse_partitioned_candidate_audit_result(
            raw_result,
            request=_plan().requests[0],
        )


def test_result_builder_rejects_invalid_duration_and_ledger_fields():
    request = _plan().requests[0]
    with pytest.raises(ValueError, match="duration"):
        contract.build_partitioned_candidate_audit_result(
            request=request,
            matched_source_occurrence_count=1,
            validated_persisted_occurrence_count=1,
            duration_ms=float("nan"),
            block_io=_block_io(),
            candidate_processing_io=_candidate_io(),
        )
    with pytest.raises(ValueError, match="fields"):
        contract.build_partitioned_candidate_audit_result(
            request=request,
            matched_source_occurrence_count=1,
            validated_persisted_occurrence_count=1,
            duration_ms=1,
            block_io={},
            candidate_processing_io=_candidate_io(),
        )


def test_result_parser_rejects_result_digest_drift():
    request = _plan().requests[0]
    payload = {**_result(request).payload, "result_digest": "0" * 64}

    with pytest.raises(ValueError, match="binding"):
        contract.parse_partitioned_candidate_audit_result(payload, request=request)


def test_result_aggregation_rejects_plan_and_count_mismatch():
    plan = _plan()
    result = _result(plan.requests[0])

    with pytest.raises(ValueError, match="plan_mismatch"):
        contract.validate_partitioned_candidate_audit_results(
            plan,
            (replace(result, request_digest="0" * 64),),
        )
    with pytest.raises(ValueError, match="aggregate_count_mismatch"):
        contract.validate_partitioned_candidate_audit_results(
            replace(plan, source_occurrence_count=2),
            (result,),
        )
