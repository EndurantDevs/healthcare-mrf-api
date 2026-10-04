from __future__ import annotations

import json
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
    provider_record = types.SimpleNamespace(kind="provider_reference", linked_provider_sha256=None)
    occurrence_record = types.SimpleNamespace(kind="rate_occurrence", linked_provider_sha256="provider")
    return types.SimpleNamespace(
        provider_records=(provider_record,),
        occurrence_records=(occurrence_record,),
        records=(provider_record, occurrence_record),
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
            network_name_digests=("f" * 64, "e" * 64),
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


@pytest.mark.parametrize("network_count", [65, 1024, 31000])
def test_plan_and_parser_preserve_large_complete_network_sets(network_count):
    network_digests = tuple(f"{index:064x}" for index in range(network_count))
    plan = contract.build_partitioned_candidate_audit_plan(
        binding=_binding(),
        source_challenges=(replace(_source(), network_name_digests=network_digests),),
        persisted_occurrences=(_persisted(),),
    )
    request = plan.requests[0]

    assert request.source_challenges[0].network_name_digests == network_digests
    assert contract.parse_partitioned_candidate_audit_request(request.payload) == request


def test_byte_partitioning_preserves_whole_challenges_and_exact_once_ordinals():
    networks = tuple(f"{index:064x}" for index in range(16000))
    sources = tuple(
        replace(_source(npi=1_234_567_890 + index), network_name_digests=networks)
        for index in range(2)
    )
    plan = contract.build_partitioned_candidate_audit_plan(
        binding=_binding(2, 1), source_challenges=sources, persisted_occurrences=(_persisted(),)
    )
    reordered_plan = contract.build_partitioned_candidate_audit_plan(
        binding=_binding(2, 1), source_challenges=tuple(reversed(sources)), persisted_occurrences=(_persisted(),)
    )

    assert plan == reordered_plan
    assert len(plan.requests) == 2
    assert [challenge.network_name_digests for request in plan.requests for challenge in request.source_challenges] == [
        networks, networks
    ]
    assert sorted(
        audit_item.ordinal
        for request in plan.requests
        for audit_item in (*request.source_challenges, *request.persisted_occurrences)
    ) == [0, 1, 2]
    assert all(contract.parse_partitioned_candidate_audit_request(request.payload) == request for request in plan.requests)


def test_network_count_cannot_exceed_the_wire_byte_ceiling():
    network_count = audit_types.PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_NETWORK_DIGESTS + 1
    challenge = replace(
        _source(), network_name_digests=tuple(f"{index:064x}" for index in range(network_count))
    )

    with pytest.raises(ValueError, match="network_digests"):
        request_contract._validated_source_challenge(challenge)


def test_request_byte_boundary_is_identical_in_planner_and_parser(monkeypatch):
    payload = _plan().requests[0].payload
    serialized_bytes = len(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    monkeypatch.setattr(
        request_contract, "PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_REQUEST_BYTES", serialized_bytes
    )
    assert _plan().requests[0].payload == payload
    assert contract.parse_partitioned_candidate_audit_request(payload).payload == payload

    monkeypatch.setattr(
        request_contract, "PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_REQUEST_BYTES", serialized_bytes - 1
    )
    assert len(_plan().requests) == 2
    with pytest.raises(ValueError, match="request_too_large"):
        contract.parse_partitioned_candidate_audit_request(payload)


def test_unsplittable_item_exceeds_request_bytes(monkeypatch):
    monkeypatch.setattr(request_contract, "PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_REQUEST_BYTES", 1)

    with pytest.raises(ValueError, match="request_too_large"):
        _plan()


def test_request_byte_limit_counts_compact_utf8_not_ascii_escapes(monkeypatch):
    plan = contract.build_partitioned_candidate_audit_plan(
        binding=replace(_binding(), plan_id="é" * 512),
        source_challenges=(_source(),), persisted_occurrences=(_persisted(),),
    )
    payload = plan.requests[0].payload
    compact_bytes = len(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    assert len(json.dumps(payload).encode("ascii")) > compact_bytes
    monkeypatch.setattr(request_contract, "PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_REQUEST_BYTES", compact_bytes)

    assert contract.parse_partitioned_candidate_audit_request(payload) == plan.requests[0]


@pytest.mark.parametrize("invalid_value", [{"a" * 64}, "\ud800"])
def test_parser_rejects_non_json_request_fields(invalid_value):
    payload = _plan().requests[0].payload
    payload["source_challenges"][0]["network_name_digests"] = invalid_value

    with pytest.raises(ValueError, match="request_fields_invalid"):
        contract.parse_partitioned_candidate_audit_request(payload)


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
    fitting_groups = tuple(
        _source(code="99213", npi=900_000_000 + index)
        for index in range(10)
    ) + tuple(
        _source(code="99214", npi=950_000_000 + index)
        for index in range(10)
    )
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
        for partition in request_contract._partition_items(fitting_groups)
    ] == [20]
    assert [
        len(partition)
        for partition in request_contract._partition_items(small_groups)
    ] == [20, 25, 5]
    assert [
        len(partition)
        for partition in request_contract._partition_items(exact_group_items)
    ] == [25, 25, 25, 25]


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
        loaded_witness.provider_records[0],
        parsed_evidence_by_sha256=loaded_witness.evidence_by_sha256,
    )
    condition_builder.assert_called_once_with(
        loaded_witness.occurrence_records[0],
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
