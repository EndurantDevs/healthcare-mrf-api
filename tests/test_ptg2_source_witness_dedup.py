# See LICENSE.

from __future__ import annotations

import hashlib
import json
import random
import struct
import zlib
from decimal import Decimal

from process.ptg_parts.ptg2_candidate_audit_evidence import (
    source_audit_condition,
)
from process.ptg_parts import ptg2_source_witness_persisted_encode as witness_encoder
from process.ptg_parts.ptg2_source_witness import (
    decode_persisted_source_witness,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT,
)
from process.ptg_parts.ptg2_source_witness_persisted_encode import (
    SourceWitnessPayloadCounts,
    encode_persisted_source_witness,
)


SOURCE_DIGEST = "11" * 32
U32 = struct.Struct(">I")


def _payload_counts(occurrence_count: int) -> SourceWitnessPayloadCounts:
    return SourceWitnessPayloadCounts(
        source_count=1,
        source_digest=hashlib.sha256(bytes.fromhex(SOURCE_DIGEST)).hexdigest(),
        occurrence_population=occurrence_count,
        provider_population=0,
        emitted_rate_rows=occurrence_count,
        unqueryable_rate_rows=0,
        occurrence_count=occurrence_count,
        provider_count=0,
        record_count=occurrence_count,
    )


def _compressed_occurrence(
    index: int,
    linked_provider_json: bytes,
    *,
    raw_json: bytes | None = None,
) -> CompressedSourceWitnessRecord:
    raw_json = raw_json or f'{{"rate":{index}}}'.encode()
    linked_sha256 = hashlib.sha256(linked_provider_json).hexdigest()
    metadata = {
        "contract": PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT,
        "kind": "rate_occurrence",
        "priority": f"{index:016x}",
        "tie_breaker": hashlib.sha256(f"rate:{index}".encode()).hexdigest(),
        "coordinate": {
            "object_ordinal": 1,
            "rate_ordinal": index,
            "price_ordinal": 0,
            "provider_ordinal": 0,
        },
        "raw_sha256": hashlib.sha256(raw_json).hexdigest(),
        "linked_provider_sha256": linked_sha256,
        "procedure": {
            "billing_code_type": "CPT",
            "billing_code": "99213",
        },
        "provider_evidence": {
            "source_kind": "provider_reference",
            "provider_reference_id": "1",
            "provider_group_ordinal": 0,
            "npi_ordinal": 0,
        },
        "expected": {
            "contract": "ptg2_v3_source_rate_occurrence_expected_v2",
        },
    }
    metadata_bytes = json.dumps(
        metadata,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    decoded_record = b"".join(
        (
            b"PTG2SWR2",
            U32.pack(len(metadata_bytes)),
            metadata_bytes,
            U32.pack(len(raw_json)),
            raw_json,
            U32.pack(len(linked_provider_json)),
            linked_provider_json,
        )
    )
    compressed_record = zlib.compress(decoded_record, level=1)
    return CompressedSourceWitnessRecord(
        kind="rate_occurrence",
        priority=index,
        tie_breaker=metadata["tie_breaker"],
        raw_source_sha256=SOURCE_DIGEST,
        compressed=compressed_record,
    )


def test_persisted_witness_deduplicates_exact_source_evidence():
    linked_provider_json = json.dumps(
        {
            "provider_group_id": 7,
            "provider_groups": [
                {
                    "npi": list(range(1_234_560_000, 1_234_570_000)),
                    "tin": {"type": "ein", "value": "000000007"},
                }
            ],
        },
        separators=(",", ":"),
    ).encode()
    shared_raw_json = json.dumps(
        {
            "negotiated_prices": [
                {"negotiated_rate": 100 + index} for index in range(1_000)
            ]
        },
        separators=(",", ":"),
    ).encode()
    compressed_records = [
        _compressed_occurrence(
            index,
            linked_provider_json,
            raw_json=shared_raw_json,
        )
        for index in range(100)
    ]
    witness_payload, witness_metadata = encode_persisted_source_witness(
        compressed_records,
        _payload_counts(100),
    )
    loaded_witness = decode_persisted_source_witness(
        witness_payload,
        expected_raw_source_sha256=[SOURCE_DIGEST],
        expected_metadata=witness_metadata,
    )

    assert witness_metadata["evidence_dictionary_count"] == 2
    assert (
        witness_metadata["evidence_dictionary_raw_bytes"]
        == len(linked_provider_json) + len(shared_raw_json)
    )
    assert witness_metadata["evidence_dictionary_stored_bytes"] > 0
    assert len(witness_payload) < sum(
        len(witness_record.compressed)
        for witness_record in compressed_records
    ) // 4
    assert {
        witness_record.linked_provider_json
        for witness_record in loaded_witness.occurrence_records
    } == {linked_provider_json}
    assert {
        witness_record.raw_json
        for witness_record in loaded_witness.occurrence_records
    } == {shared_raw_json}


def test_persisted_witness_shares_evidence_across_raw_and_linked_roles():
    shared_evidence = b'{"provider_group_id":7,"provider_groups":[]}'
    compressed_record = _compressed_occurrence(
        0,
        shared_evidence,
        raw_json=shared_evidence,
    )
    witness_payload, witness_metadata = encode_persisted_source_witness(
        [compressed_record],
        _payload_counts(1),
    )
    loaded_witness = decode_persisted_source_witness(
        witness_payload,
        expected_raw_source_sha256=[SOURCE_DIGEST],
        expected_metadata=witness_metadata,
    )

    assert witness_metadata["evidence_dictionary_count"] == 1
    assert loaded_witness.records[0].raw_json == shared_evidence
    assert loaded_witness.records[0].linked_provider_json == shared_evidence


def test_persisted_witness_reuses_exact_decimal_evidence_for_audit_condition():
    raw_json = (
        b'{"negotiated_prices":[{"negotiated_type":"negotiated",'
        b'"negotiated_rate":0.0000000000000000000125,'
        b'"expiration_date":"2027-01-01","service_code":["11"],'
        b'"billing_class":"professional","setting":"outpatient",'
        b'"billing_code_modifier":[],"additional_information":null}],'
        b'"provider_references":[1]}'
    )
    linked_provider_json = (
        b'{"provider_group_id":1,"provider_groups":'
        b'[{"npi":[1234567890]}]}'
    )
    witness_payload, witness_metadata = encode_persisted_source_witness(
        [
            _compressed_occurrence(
                0,
                linked_provider_json,
                raw_json=raw_json,
            )
        ],
        _payload_counts(1),
    )

    loaded_witness = decode_persisted_source_witness(
        witness_payload,
        expected_raw_source_sha256=[SOURCE_DIGEST],
        expected_metadata=witness_metadata,
    )
    occurrence = loaded_witness.occurrence_records[0]
    parsed_rate = loaded_witness.evidence_by_sha256[occurrence.raw_sha256][
        "negotiated_prices"
    ][0]["negotiated_rate"]
    retained_condition = source_audit_condition(
        occurrence,
        parsed_evidence_by_sha256=loaded_witness.evidence_by_sha256,
    )
    direct_condition = source_audit_condition(occurrence)

    assert parsed_rate == Decimal("0.0000000000000000000125")
    assert retained_condition == direct_condition
    assert retained_condition.expected_tuple.payload["negotiated_rate"] == (
        "0.0000000000000000000125"
    )


def test_persisted_witness_fits_repeated_large_source_evidence_within_budget(
    monkeypatch,
):
    shared_raw_json = json.dumps(
        {"source_noise": random.Random(7).randbytes(64 * 1024).hex()},
        separators=(",", ":"),
    ).encode()
    linked_provider_json = b'{"provider_group_id":7,"provider_groups":[]}'
    compressed_records = [
        _compressed_occurrence(
            index,
            linked_provider_json,
            raw_json=shared_raw_json,
        )
        for index in range(50)
    ]
    payload_budget = 1024 * 1024
    assert (
        sum(
            len(compressed_record.compressed)
            for compressed_record in compressed_records
        )
        > payload_budget
    )
    monkeypatch.setattr(
        witness_encoder,
        "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES",
        payload_budget,
    )
    witness_payload, witness_metadata = encode_persisted_source_witness(
        compressed_records,
        _payload_counts(50),
    )
    loaded_witness = decode_persisted_source_witness(
        witness_payload,
        expected_raw_source_sha256=[SOURCE_DIGEST],
        expected_metadata=witness_metadata,
    )

    assert len(witness_payload) < payload_budget
    assert len(loaded_witness.records) == 50
    assert {
        witness_record.raw_json for witness_record in loaded_witness.records
    } == {shared_raw_json}
