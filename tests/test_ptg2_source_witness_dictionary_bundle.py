# See LICENSE.

from __future__ import annotations

import hashlib
import json
import struct
import zlib
from pathlib import Path

from process.ptg_parts import ptg2_source_witness as witness
from process.ptg_parts import ptg2_source_witness_codec as witness_codec
from tests.test_ptg2_source_witness import SOURCE_A, _record


U32 = struct.Struct(">I")


def _externalize_scanner_records(
    compressed_records: list[bytes],
) -> tuple[list[bytes], dict[str, bytes]]:
    """Externalize exact evidence from legacy-shaped scanner records."""

    externalized_records: list[bytes] = []
    evidence_by_sha256: dict[str, bytes] = {}
    for compressed_record in compressed_records:
        externalized_record, record_evidence_by_sha256 = (
            witness_codec.externalize_source_evidence_record(
                compressed_record,
                SOURCE_A,
            )
        )
        externalized_records.append(externalized_record)
        evidence_by_sha256.update(record_evidence_by_sha256)
    return externalized_records, evidence_by_sha256


def _scanner_bundle_header(record_count: int) -> bytes:
    """Encode the authenticated format-v3 scanner bundle header."""

    header_by_field = {
        "contract": witness.PTG2_V3_SOURCE_WITNESS_CONTRACT,
        "format_version": 3,
        "selection_method": witness.PTG2_V3_SOURCE_WITNESS_SELECTION,
        "unqueryable_rate_policy": witness.PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
        "raw_source_sha256": SOURCE_A,
        "occurrence_target": witness.PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
        "total_target": witness.PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
        "provider_quota": witness.PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
        "rate_occurrence": {
            "population_count": record_count,
            "selected_count": record_count,
            "emitted_rate_row_count": record_count,
            "unqueryable_rate_row_count": 0,
        },
        "provider_reference": {
            "population_count": 0,
            "selected_count": 0,
        },
    }
    return json.dumps(
        header_by_field,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _dictionary_bundle_payload(compressed_records: list[bytes]) -> bytes:
    """Build one scanner dictionary bundle with deterministic framing."""

    externalized_records, evidence_by_sha256 = _externalize_scanner_records(
        compressed_records
    )
    header_bytes = _scanner_bundle_header(len(externalized_records))
    bundle_bytes = bytearray(b"PTG2SW03")
    bundle_bytes.extend(U32.pack(len(header_bytes)))
    bundle_bytes.extend(header_bytes)
    bundle_bytes.extend(U32.pack(len(evidence_by_sha256)))
    for evidence_sha256, raw_evidence in sorted(evidence_by_sha256.items()):
        compressed_evidence = zlib.compress(raw_evidence, level=1)
        bundle_bytes.extend(bytes.fromhex(evidence_sha256))
        bundle_bytes.extend(U32.pack(len(raw_evidence)))
        bundle_bytes.extend(U32.pack(len(compressed_evidence)))
        bundle_bytes.extend(compressed_evidence)
    bundle_bytes.extend(U32.pack(len(externalized_records)))
    for externalized_record in externalized_records:
        bundle_bytes.extend(U32.pack(len(externalized_record)))
        bundle_bytes.extend(externalized_record)
    return bytes(bundle_bytes)


def _write_dictionary_bundle(
    temporary_directory: Path,
    compressed_records: list[bytes],
) -> dict[str, object]:
    """Write one authenticated dictionary bundle and return its scanner entry."""

    bundle_payload = _dictionary_bundle_payload(compressed_records)
    bundle_path = temporary_directory / "dictionary.bin"
    bundle_path.write_bytes(bundle_payload)
    return {
        "path": str(bundle_path),
        "sha256": hashlib.sha256(bundle_payload).hexdigest(),
        "byte_count": len(bundle_payload),
        "row_count": len(compressed_records),
        "raw_source_sha256": SOURCE_A,
    }


def test_dictionary_scanner_bundle_round_trips_deduplicated_evidence(tmp_path):
    shared_raw = b'{"negotiated_prices":[{"negotiated_rate":123.45}]}'
    linked_provider_raw = b'{"provider_group_id":7,"provider_groups":[]}'
    compressed_records = [
        _record(
            kind="rate_occurrence",
            priority=index,
            item_ordinal=index,
            raw_json=shared_raw,
            linked_provider_json=linked_provider_raw,
        )
        for index in range(8)
    ]
    bundle_entry = _write_dictionary_bundle(tmp_path, compressed_records)

    witness_payload, witness_metadata = witness.build_persisted_source_witness(
        [bundle_entry],
        expected_raw_source_sha256=[SOURCE_A],
    )
    loaded_witness = witness.decode_persisted_source_witness(
        witness_payload,
        expected_raw_source_sha256=[SOURCE_A],
        expected_metadata=witness_metadata,
    )

    assert witness_metadata["evidence_dictionary_count"] == 2
    assert len(loaded_witness.occurrence_records) == len(compressed_records)
    assert {
        witness_record.raw_json
        for witness_record in loaded_witness.occurrence_records
    } == {shared_raw}
    assert {
        witness_record.linked_provider_json
        for witness_record in loaded_witness.occurrence_records
    } == {linked_provider_raw}
