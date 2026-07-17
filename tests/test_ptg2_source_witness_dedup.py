# See LICENSE.

from __future__ import annotations

import hashlib
import json
import struct
import zlib

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


def _compressed_occurrence(
    index: int,
    linked_provider_json: bytes,
) -> CompressedSourceWitnessRecord:
    raw_json = f'{{"rate":{index}}}'.encode()
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


def test_persisted_witness_deduplicates_linked_provider_json():
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
    compressed_records = [
        _compressed_occurrence(index, linked_provider_json)
        for index in range(100)
    ]
    counts = SourceWitnessPayloadCounts(
        source_count=1,
        source_digest=hashlib.sha256(bytes.fromhex(SOURCE_DIGEST)).hexdigest(),
        occurrence_population=100,
        provider_population=0,
        emitted_rate_rows=100,
        unqueryable_rate_rows=0,
        occurrence_count=100,
        provider_count=0,
        record_count=100,
    )

    witness_payload, witness_metadata = encode_persisted_source_witness(
        compressed_records,
        counts,
    )
    loaded_witness = decode_persisted_source_witness(
        witness_payload,
        expected_raw_source_sha256=[SOURCE_DIGEST],
        expected_metadata=witness_metadata,
    )

    assert witness_metadata["linked_provider_dictionary_count"] == 1
    assert (
        witness_metadata["linked_provider_dictionary_raw_bytes"]
        == len(linked_provider_json)
    )
    assert witness_metadata["linked_provider_dictionary_stored_bytes"] > 0
    assert len(witness_payload) < sum(
        len(witness_record.compressed)
        for witness_record in compressed_records
    ) // 4
    assert {
        witness_record.linked_provider_json
        for witness_record in loaded_witness.occurrence_records
    } == {linked_provider_json}
