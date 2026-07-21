from __future__ import annotations

from copy import deepcopy

import pytest

from process.ptg_parts import ptg2_partitioned_candidate_audit_contract as contract


def _binding(source_count: int, persisted_count: int):
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


def _source_items(count: int):
    return tuple(
        contract.PartitionedSourceChallenge(
            ordinal=0,
            code_system="CPT",
            code="99213",
            npi=1_000_000_000 + index,
            source_artifact_key=0,
            tuple_digest=f"{index:064x}",
            network_name_digests=("e" * 64,),
            multiplicity=1,
        )
        for index in range(count)
    )


def _persisted_items(count: int):
    return tuple(
        contract.PartitionedPersistedOccurrence(
            ordinal=0,
            occurrence_id=index.to_bytes(32, "big"),
            code_system="CPT",
            code="99213",
            code_key=7,
            provider_set_key=index,
            price_key=index + 10,
            source_artifact_key=0,
            npi=2_000_000_000 + index,
            atom_ordinal=0,
            atom_key=index + 20,
        )
        for index in range(1, count + 1)
    )


def _plan(source_count: int = 201, persisted_count: int = 2):
    return contract.build_partitioned_candidate_audit_plan(
        binding=_binding(source_count, persisted_count),
        source_challenges=_source_items(source_count),
        persisted_occurrences=_persisted_items(persisted_count),
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
        "peak_raw_bytes": 1024,
    }


def _candidate_io(request):
    count = len(request.source_challenges)
    return {
        "candidate_occurrence_deliveries": count,
        "unique_candidate_projections": count,
        "candidate_projection_builds": count,
        "candidate_projection_reuse_deliveries": 0,
        "repeated_candidate_projection_builds": 0,
        "availability_condition_count": count,
        "duplicate_availability_deliveries": 0,
    }


def _result(request):
    return contract.build_partitioned_candidate_audit_result(
        request=request,
        matched_source_occurrence_count=request.source_occurrence_count,
        validated_persisted_occurrence_count=len(
            request.persisted_occurrences
        ),
        duration_ms=12.5,
        block_io=_block_io(),
        candidate_processing_io=_candidate_io(request),
    )


def test_plan_is_deterministic_max_50_and_exact_once():
    plan = _plan()
    repeated = _plan()

    assert plan == repeated
    assert [request.item_count for request in plan.requests] == [50, 50, 50, 50, 3]
    assert all(
        request.item_count
        <= contract.PTG2_PARTITIONED_CANDIDATE_AUDIT_MAX_ITEMS
        for request in plan.requests
    )
    ordinals = [
        item.ordinal
        for request in plan.requests
        for item in (*request.source_challenges, *request.persisted_occurrences)
    ]
    assert sorted(ordinals) == list(range(203))
    assert len(ordinals) == len(set(ordinals))
    assert sum(request.source_occurrence_count for request in plan.requests) == 201
    assert sum(len(request.persisted_occurrences) for request in plan.requests) == 2
    assert len({request.request_digest for request in plan.requests}) == 5
    assert all(
        contract.parse_partitioned_candidate_audit_request(request.payload)
        == request
        for request in plan.requests
    )


def test_plan_preserves_code_groups_when_they_fit():
    source_challenges = list(_source_items(80))
    source_challenges.extend(
        contract.PartitionedSourceChallenge(
            ordinal=0,
            code_system="CPT",
            code="99214",
            npi=3_000_000_000 + index,
            source_artifact_key=0,
            tuple_digest=f"{index + 1000:064x}",
            network_name_digests=(),
            multiplicity=1,
        )
        for index in range(30)
    )
    plan = contract.build_partitioned_candidate_audit_plan(
        binding=_binding(110, 1),
        source_challenges=source_challenges,
        persisted_occurrences=_persisted_items(1),
    )

    assert [request.item_count for request in plan.requests] == [50, 31, 30]
    first_codes = {
        (audit_item.code_system, audit_item.code)
        for audit_item in (
            *plan.requests[0].source_challenges,
            *plan.requests[0].persisted_occurrences,
        )
    }
    assert first_codes == {("CPT", "99213")}


@pytest.mark.parametrize(
    "mutation",
    (
        lambda payload: payload.update(item_count=101),
        lambda payload: payload.update(request_digest="0" * 64),
        lambda payload: payload.update(partition_digest="0" * 64),
        lambda payload: payload["source_challenges"][0].update(multiplicity=2),
        lambda payload: payload.update(partition_index=payload["partition_count"]),
    ),
)
def test_request_rejects_mutated_counts_items_and_digests(mutation):
    payload = deepcopy(_plan().requests[0].payload)
    mutation(payload)

    with pytest.raises(ValueError):
        contract.parse_partitioned_candidate_audit_request(payload)


def test_request_rejects_more_than_100_explicit_items():
    payload = deepcopy(_plan().requests[0].payload)
    for index in range(51):
        extra = deepcopy(payload["source_challenges"][0])
        extra["ordinal"] = 999 + index
        extra["npi"] = 9_000_000_000 + index
        extra["tuple_digest"] = f"{index + 10_000:064x}"
        payload["source_challenges"].append(extra)
        payload["source_challenge_count"] += 1
        payload["source_occurrence_count"] += 1
        payload["item_count"] += 1

    with pytest.raises(ValueError, match="item_count"):
        contract.parse_partitioned_candidate_audit_request(payload)


def test_results_parse_and_aggregate_complete_plan():
    plan = _plan()
    results = tuple(_result(request) for request in plan.requests)
    parsed_results = tuple(
        contract.parse_partitioned_candidate_audit_result(
            result.payload,
            request=request,
        )
        for request, result in zip(plan.requests, results)
    )

    aggregate = contract.validate_partitioned_candidate_audit_results(
        plan,
        tuple(reversed(parsed_results)),
    )

    assert aggregate.request_count == 5
    assert aggregate.source_challenge_count == 201
    assert aggregate.source_occurrence_count == 201
    assert aggregate.persisted_occurrence_count == 2
    assert aggregate.block_io["physical_block_reads"] == 5
    assert aggregate.block_io["peak_raw_bytes"] == 1024


def test_aggregate_rejects_missing_and_duplicate_results():
    plan = _plan()
    results = tuple(_result(request) for request in plan.requests)

    with pytest.raises(ValueError, match="incomplete"):
        contract.validate_partitioned_candidate_audit_results(plan, results[:-1])
    with pytest.raises(ValueError, match="duplicate"):
        contract.validate_partitioned_candidate_audit_results(
            plan,
            (*results, results[0]),
        )


def test_result_rejects_incomplete_counts_and_repeat_ledger():
    request = _plan().requests[0]
    with pytest.raises(ValueError, match="incomplete"):
        contract.build_partitioned_candidate_audit_result(
            request=request,
            matched_source_occurrence_count=request.source_occurrence_count - 1,
            validated_persisted_occurrence_count=len(
                request.persisted_occurrences
            ),
            duration_ms=1,
            block_io=_block_io(),
            candidate_processing_io=_candidate_io(request),
        )
    repeated_io = _block_io()
    repeated_io["repeated_physical_reads"] = 1
    with pytest.raises(ValueError, match="repeated_work"):
        contract.build_partitioned_candidate_audit_result(
            request=request,
            matched_source_occurrence_count=request.source_occurrence_count,
            validated_persisted_occurrence_count=len(
                request.persisted_occurrences
            ),
            duration_ms=1,
            block_io=repeated_io,
            candidate_processing_io=_candidate_io(request),
        )
