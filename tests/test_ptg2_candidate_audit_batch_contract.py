from __future__ import annotations

from dataclasses import replace
from copy import deepcopy
import typing

import pytest

from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchWitnessBinding,
    PTG2_AUDIT_BATCH_MAX_CHALLENGES,
    build_audit_batch_request,
    group_audit_batch_challenges,
    matched_audit_batch_digest,
    parse_audit_batch_request,
    parse_audit_batch_response,
)
from process.ptg_parts.ptg2_candidate_audit_evidence import SourceChallenge
from scripts.validation import ptg2_v3_source_api_audit as source_audit


_RAW_SOURCE_DIGEST = "a" * 64
_AUDIT_SAMPLE_DIGEST = "c" * 64
_WITNESS_SAMPLE_DIGEST = "d" * 64
_WITNESS_PAYLOAD_SHA256 = "e" * 64


def _challenge(*, fingerprint: str = "fingerprint") -> SourceChallenge:
    query = source_audit.QueryKey("CPT", "99213", 1234567890)
    canonical_tuple = source_audit.CanonicalTuple.from_parts(
        query,
        "ffs",
        {
            "negotiated_type": "negotiated",
            "negotiated_rate": "123.45",
            "expiration_date": "2026-12-31",
            "service_code": ["11"],
            "billing_class": "professional",
            "setting": "office",
            "billing_code_modifier": ["25"],
            "additional_information": "test",
        },
        billing_code_type_version="2026",
        name="Office visit",
        description="Established patient",
        network_names=("Alpha Network",),
    )
    return SourceChallenge(
        query=query,
        expected_tuple=canonical_tuple,
        required_network_names=("Alpha Network",),
        raw_source_sha256=_RAW_SOURCE_DIGEST,
        negotiated_rate="123.45",
        service_codes=("11",),
        modifiers=("25",),
        fingerprint=fingerprint,
    )


def _request_payload() -> dict:
    return build_audit_batch_request(
        snapshot_id="ptg2:202607:candidate",
        source_key="health-plan-source",
        plan_id="123456789",
        plan_market_type="group",
        witness_binding=AuditBatchWitnessBinding(
            audit_sample_digest=_AUDIT_SAMPLE_DIGEST,
            source_witness_sample_digest=_WITNESS_SAMPLE_DIGEST,
            source_witness_payload_sha256=_WITNESS_PAYLOAD_SHA256,
            raw_container_sha256=(_RAW_SOURCE_DIGEST,),
            source_witness_occurrence_count=2,
        ),
    ).payload


def _block_io() -> dict[str, int]:
    return {
        "logical_block_deliveries": 7,
        "physical_mapping_references": 7,
        "physical_mapping_aliases": 2,
        "unique_physical_blocks": 5,
        "physical_block_reads": 5,
        "physical_block_decodes": 5,
        "physical_payload_preparations": 5,
        "expected_logical_payload_processes": 5,
        "logical_payload_processes": 5,
        "logical_payload_fragment_references": 5,
        "logical_payload_fragment_aliases": 0,
        "repeated_physical_reads": 0,
        "repeated_physical_decodes": 0,
        "repeated_physical_preparations": 0,
        "repeated_logical_payload_processes": 0,
        "peak_raw_bytes": 4_096,
    }


def _witness_io() -> dict[str, int]:
    return {
        "payload_reads": 1,
        "payload_decodes": 1,
        "record_decodes": 3,
        "unique_evidence_entries": 2,
        "evidence_decompressions": 2,
        "evidence_sha256_hashes": 2,
        "evidence_json_parses": 2,
        "evidence_reuse_deliveries": 1,
        "repeated_evidence_decompressions": 0,
        "repeated_evidence_sha256_hashes": 0,
        "repeated_evidence_json_parses": 0,
    }


def _candidate_processing_io() -> dict[str, int]:
    return {
        "candidate_occurrence_deliveries": 3,
        "unique_candidate_projections": 2,
        "candidate_projection_builds": 2,
        "candidate_projection_reuse_deliveries": 1,
        "repeated_candidate_projection_builds": 0,
        "availability_condition_count": 2,
        "duplicate_availability_deliveries": 0,
    }


def _response_payload() -> dict:
    request = parse_audit_batch_request(_request_payload())
    return {
        "contract": "ptg2_v3_source_witness_batch_response_v1",
        "request_digest": request.request_digest,
        "challenge_count": 2,
        "unique_challenge_count": 1,
        "matched_challenge_count": 2,
        "persisted_audit_occurrence_count": 2,
        "validated_persisted_audit_occurrence_count": 2,
        "matched_challenge_digest": matched_audit_batch_digest(
            request.request_digest,
            2,
        ),
        "duration_ms": 12.5,
        "block_io": _block_io(),
        "witness_io": _witness_io(),
        "candidate_processing_io": _candidate_processing_io(),
    }


def _parse_response(response_payload: dict):
    return parse_audit_batch_response(
        response_payload,
        request=parse_audit_batch_request(_request_payload()),
        expected_source_witness={
            "record_count": 3,
            "evidence_dictionary_count": 2,
        },
        expected_audit_sample={"sample_count": 2},
    )


def test_batch_request_round_trips_without_caller_authored_conditions():
    payload = _request_payload()

    parsed = parse_audit_batch_request(payload)

    assert parsed.challenge_count == 2
    assert parsed.audit_sample_digest == _AUDIT_SAMPLE_DIGEST
    assert parsed.source_witness_sample_digest == _WITNESS_SAMPLE_DIGEST
    assert parsed.source_witness_payload_sha256 == _WITNESS_PAYLOAD_SHA256
    assert len(parsed.ordered_source_ordinal_digest) == 64
    assert "challenges" not in payload
    serialized = str(payload)
    assert _RAW_SOURCE_DIGEST not in serialized
    assert "Alpha Network" not in serialized
    assert "123.45" not in serialized


def test_batch_request_digest_binds_every_field():
    payload = _request_payload()
    payload["source_witness_sample_digest"] = "0" * 64

    with pytest.raises(ValueError, match="digest_mismatch"):
        parse_audit_batch_request(payload)


def test_batch_request_digest_binds_ordered_source_identity():
    payload = _request_payload()
    payload["ordered_source_ordinal_digest"] = "0" * 64

    with pytest.raises(ValueError, match="digest_mismatch"):
        parse_audit_batch_request(payload)


def test_server_groups_duplicate_derived_conditions_once():
    grouped = group_audit_batch_challenges(
        (_RAW_SOURCE_DIGEST,),
        (_challenge(), replace(_challenge(), fingerprint="other")),
    )

    assert len(grouped) == 1
    assert grouped[0].multiplicity == 2
    assert grouped[0].source_artifact_key == 0
    assert grouped[0].condition_key == (
        "CPT",
        "99213",
        1234567890,
        0,
        grouped[0].tuple_digest,
        grouped[0].network_name_digests,
    )
    assert grouped[0].payload["network_name_digests"] == list(
        grouped[0].network_name_digests
    )


def test_batch_request_rejects_unknown_fields():
    payload = _request_payload()
    payload["unexpected"] = True

    with pytest.raises(ValueError, match="fields_invalid"):
        parse_audit_batch_request(payload)


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "message"),
    [
        ("contract", "other", "contract_invalid"),
        ("snapshot_id", "", "coordinates_invalid"),
        ("challenge_count", 0, "challenge_count_invalid"),
        (
            "challenge_count",
            PTG2_AUDIT_BATCH_MAX_CHALLENGES + 1,
            "challenge_counts_invalid",
        ),
        ("audit_sample_digest", "bad", "sample_digest_invalid"),
    ],
)
def test_batch_request_rejects_invalid_contract_fields(
    field_name,
    invalid_value,
    message,
):
    payload = _request_payload()
    payload[field_name] = invalid_value

    with pytest.raises(ValueError, match=message):
        parse_audit_batch_request(payload)


def test_challenge_grouping_rejects_duplicate_or_unknown_sources():
    with pytest.raises(ValueError, match="raw_source_scope_invalid"):
        group_audit_batch_challenges(
            (_RAW_SOURCE_DIGEST, _RAW_SOURCE_DIGEST),
            (_challenge(),),
        )
    with pytest.raises(ValueError, match="challenge_source_unknown"):
        group_audit_batch_challenges(
            (_RAW_SOURCE_DIGEST,),
            (replace(_challenge(), raw_source_sha256="b" * 64),),
        )


def test_batch_request_builder_rejects_oversized_witness_count():
    witness_binding = AuditBatchWitnessBinding(
        audit_sample_digest=_AUDIT_SAMPLE_DIGEST,
        source_witness_sample_digest=_WITNESS_SAMPLE_DIGEST,
        source_witness_payload_sha256=_WITNESS_PAYLOAD_SHA256,
        raw_container_sha256=(_RAW_SOURCE_DIGEST,),
        source_witness_occurrence_count=PTG2_AUDIT_BATCH_MAX_CHALLENGES + 1,
    )

    with pytest.raises(ValueError, match="challenge_counts_invalid"):
        build_audit_batch_request(
            snapshot_id="candidate",
            source_key="source",
            plan_id="plan",
            plan_market_type="group",
            witness_binding=witness_binding,
        )


def test_batch_response_accepts_aliases_but_proves_each_payload_once():
    response = _parse_response(_response_payload())

    assert response.challenge_count == 2
    assert response.persisted_audit_occurrence_count == 2
    assert response.block_io["physical_mapping_aliases"] == 2
    assert response.witness_io["evidence_reuse_deliveries"] == 1
    assert response.candidate_processing_io[
        "candidate_projection_reuse_deliveries"
    ] == 1


def test_batch_response_runtime_annotations_resolve():
    type_hints = typing.get_type_hints(parse_audit_batch_response)

    assert type_hints["request"].__name__ == "AuditBatchRequest"
    assert type_hints["return"].__name__ == "AuditBatchResponse"


def test_batch_response_accepts_partially_aliased_fragmented_payloads():
    response_payload = _response_payload()
    response_payload["block_io"] = {
        "logical_block_deliveries": 4,
        "physical_mapping_references": 4,
        "physical_mapping_aliases": 1,
        "unique_physical_blocks": 3,
        "physical_block_reads": 3,
        "physical_block_decodes": 3,
        "physical_payload_preparations": 3,
        "expected_logical_payload_processes": 2,
        "logical_payload_processes": 2,
        "logical_payload_fragment_references": 4,
        "logical_payload_fragment_aliases": 1,
        "repeated_physical_reads": 0,
        "repeated_physical_decodes": 0,
        "repeated_physical_preparations": 0,
        "repeated_logical_payload_processes": 0,
        "peak_raw_bytes": 4_096,
    }

    response = _parse_response(response_payload)

    assert response.block_io["unique_physical_blocks"] == 3
    assert response.block_io["expected_logical_payload_processes"] == 2
    assert response.block_io["logical_payload_fragment_aliases"] == 1


def test_batch_response_rejects_false_fragment_alias_count():
    response_payload = _response_payload()
    response_payload["block_io"]["logical_payload_fragment_aliases"] = 1

    with pytest.raises(ValueError, match="block_io_repeated"):
        _parse_response(response_payload)


def test_batch_response_rejects_more_logical_payloads_than_fragments():
    response_payload = _response_payload()
    response_payload["block_io"]["expected_logical_payload_processes"] = 6
    response_payload["block_io"]["logical_payload_processes"] = 6

    with pytest.raises(ValueError, match="block_io_repeated"):
        _parse_response(response_payload)


@pytest.mark.parametrize(
    ("section", "field_name", "invalid_value", "message"),
    (
        (None, "matched_challenge_count", 1, "candidate_mismatch"),
        (None, "persisted_audit_occurrence_count", 1, "candidate_mismatch"),
        ("block_io", "physical_block_reads", 6, "block_io_repeated"),
        ("block_io", "unique_physical_blocks", 0, "block_io_repeated"),
        ("block_io", "repeated_physical_decodes", 1, "block_io_repeated"),
        ("witness_io", "payload_reads", 2, "witness_io_repeated"),
        (
            "witness_io",
            "repeated_evidence_json_parses",
            1,
            "witness_io_repeated",
        ),
        (
            "candidate_processing_io",
            "candidate_projection_builds",
            3,
            "candidate_processing_repeated",
        ),
        (
            "candidate_processing_io",
            "availability_condition_count",
            4,
            "candidate_processing_repeated",
        ),
        (
            "candidate_processing_io",
            "duplicate_availability_deliveries",
            2,
            "candidate_processing_repeated",
        ),
    ),
)
def test_batch_response_rejects_mismatches_and_repeated_work(
    section,
    field_name,
    invalid_value,
    message,
):
    response_payload = deepcopy(_response_payload())
    target_mapping = (
        response_payload if section is None else response_payload[section]
    )
    target_mapping[field_name] = invalid_value

    with pytest.raises(ValueError, match=message):
        _parse_response(response_payload)


def test_batch_response_rejects_unknown_ledger_fields():
    response_payload = _response_payload()
    response_payload["block_io"]["unexpected"] = 0

    with pytest.raises(ValueError, match="block_io_fields_invalid"):
        _parse_response(response_payload)
