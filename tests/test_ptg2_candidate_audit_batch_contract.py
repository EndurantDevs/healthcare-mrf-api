from __future__ import annotations

from dataclasses import replace

import pytest

from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchWitnessBinding,
    PTG2_AUDIT_BATCH_MAX_CHALLENGES,
    build_audit_batch_request,
    group_audit_batch_challenges,
    parse_audit_batch_request,
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
