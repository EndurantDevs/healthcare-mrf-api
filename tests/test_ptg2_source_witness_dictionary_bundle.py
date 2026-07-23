# See LICENSE.

from __future__ import annotations

import hashlib
import json
import os
import struct
import zlib
from io import BytesIO
from pathlib import Path

import pytest

from process.ptg_parts import ptg2_source_witness as witness
from process.ptg_parts import ptg2_source_witness_bundle as witness_bundle
from process.ptg_parts import ptg2_source_witness_codec as witness_codec
from process.ptg_parts import (
    ptg2_source_witness_locator_materialize as witness_materialize,
)
from process.ptg_parts import ptg2_source_witness_locator_reader as witness_reader
from process.ptg_parts import (
    ptg2_source_witness_streaming_encode as witness_streaming_encode,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    SourceWitnessRecordLocator,
    WitnessPayloadLimitError,
)
from process.ptg_parts.ptg2_source_witness_persisted_encode import (
    SourceWitnessPayloadCounts,
)
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


def test_locator_reader_matches_eager_payload_without_read_bytes(
    tmp_path,
    monkeypatch,
):
    compressed_records = [
        _record(
            kind="rate_occurrence",
            priority=10 - index,
            item_ordinal=index,
            raw_json=f'{{"rate":{index}}}'.encode(),
            linked_provider_json=b'{"provider_group_id":7}',
        )
        for index in range(6)
    ]
    bundle_entry = _write_dictionary_bundle(tmp_path, compressed_records)
    eager_header, eager_records = witness_bundle.read_scanner_bundle(bundle_entry)

    def reject_eager_read(_path):
        raise AssertionError("current dictionary bundle must not use Path.read_bytes")

    monkeypatch.setattr(Path, "read_bytes", reject_eager_read)
    locator_header, locator_records = witness_reader.read_scanner_bundle_locators(
        bundle_entry
    )
    assert locator_header == eager_header
    assert all(
        isinstance(witness_record, SourceWitnessRecordLocator)
        for witness_record in locator_records
    )
    assert all(
        len(witness_record.evidence_by_sha256) <= 2
        for witness_record in locator_records
    )

    materialized_records = witness_materialize.materialize_source_witness_locators(
        locator_records
    )
    assert [witness_record.selection_key for witness_record in materialized_records] == [
        witness_record.selection_key for witness_record in eager_records
    ]
    assert [witness_record.compressed for witness_record in materialized_records] == [
        witness_record.compressed for witness_record in eager_records
    ]
    for locator, materialized, eager in zip(
        locator_records,
        materialized_records,
        eager_records,
        strict=True,
    ):
        assert set(materialized.evidence_by_sha256 or {}) == set(
            locator.evidence_by_sha256
        )
        assert materialized.evidence_by_sha256 == {
            digest: (eager.evidence_by_sha256 or {})[digest]
            for digest in locator.evidence_by_sha256
        }


def test_locator_and_legacy_merge_produce_identical_persisted_payload(
    tmp_path,
    monkeypatch,
):
    compressed_records = [
        _record(
            kind="rate_occurrence",
            priority=index,
            item_ordinal=index,
            raw_json=f'{{"rate":{index}}}'.encode(),
            linked_provider_json=b'{"provider_group_id":7}',
        )
        for index in range(8)
    ]
    bundle_entry = _write_dictionary_bundle(tmp_path, compressed_records)

    monkeypatch.setattr(
        witness,
        "read_scanner_bundle",
        witness_bundle.read_scanner_bundle,
    )
    eager_payload, eager_metadata = witness.build_persisted_source_witness(
        [bundle_entry],
        expected_raw_source_sha256=[SOURCE_A],
    )
    monkeypatch.setattr(
        witness,
        "read_scanner_bundle",
        witness_reader.read_scanner_bundle_locators,
    )
    locator_payload, locator_metadata = witness.build_persisted_source_witness(
        [bundle_entry],
        expected_raw_source_sha256=[SOURCE_A],
    )

    assert locator_payload == eager_payload
    assert locator_metadata == eager_metadata


def test_materialization_decompresses_only_selected_evidence(
    tmp_path,
    monkeypatch,
):
    compressed_records = [
        _record(
            kind="rate_occurrence",
            priority=index,
            item_ordinal=index,
            raw_json=f'{{"unique_rate":{index}}}'.encode(),
            linked_provider_json=None,
        )
        for index in range(5)
    ]
    bundle_entry = _write_dictionary_bundle(tmp_path, compressed_records)
    _header, locator_records = witness_reader.read_scanner_bundle_locators(
        bundle_entry
    )
    selected_record = locator_records[0]
    decompressed_lengths: list[int] = []
    original_decompress = witness_materialize._decompress_evidence

    def track_decompression(compressed, raw_length):
        decompressed_lengths.append(raw_length)
        return original_decompress(compressed, raw_length)

    monkeypatch.setattr(
        witness_materialize,
        "_decompress_evidence",
        track_decompression,
    )
    materialized = witness_materialize.materialize_source_witness_locators(
        [selected_record]
    )

    assert len(materialized) == 1
    assert len(decompressed_lengths) == len(selected_record.evidence_by_sha256)
    assert len(decompressed_lengths) < len(compressed_records)


def _payload_counts(record_count: int) -> SourceWitnessPayloadCounts:
    return SourceWitnessPayloadCounts(
        source_count=1,
        source_digest="a" * 64,
        occurrence_population=record_count,
        provider_population=0,
        emitted_rate_rows=record_count,
        unqueryable_rate_rows=0,
        occurrence_count=record_count,
        provider_count=0,
        record_count=record_count,
    )


def test_streaming_encoder_decompresses_only_selected_locator_evidence(
    tmp_path,
    monkeypatch,
):
    compressed_records = [
        _record(
            kind="rate_occurrence",
            priority=index,
            item_ordinal=index,
            raw_json=f'{{"unique_rate":{index}}}'.encode(),
            linked_provider_json=None,
        )
        for index in range(5)
    ]
    bundle_entry = _write_dictionary_bundle(tmp_path, compressed_records)
    _header, locator_records = witness_reader.read_scanner_bundle_locators(
        bundle_entry
    )
    selected_record = locator_records[0]
    decompressed_lengths: list[int] = []
    original_decompress = witness_streaming_encode._decompress_evidence

    def track_decompression(compressed, raw_length):
        decompressed_lengths.append(raw_length)
        return original_decompress(compressed, raw_length)

    monkeypatch.setattr(
        witness_streaming_encode,
        "_decompress_evidence",
        track_decompression,
    )
    witness_streaming_encode.encode_persisted_source_witness_candidates(
        [selected_record],
        _payload_counts(1),
    )

    assert len(decompressed_lengths) == len(selected_record.evidence_by_sha256)
    assert len(decompressed_lengths) < len(compressed_records)


def test_streaming_encoder_rejects_aggregate_decoded_budget_before_decompression(
    tmp_path,
    monkeypatch,
):
    compressed_records = [
        _record(
            kind="rate_occurrence",
            priority=index,
            item_ordinal=index,
            raw_json=f'{{"unique_rate":{index}}}'.encode(),
            linked_provider_json=None,
        )
        for index in range(2)
    ]
    bundle_entry = _write_dictionary_bundle(tmp_path, compressed_records)
    _header, locator_records = witness_reader.read_scanner_bundle_locators(
        bundle_entry
    )
    aggregate_raw_bytes = sum(
        {
            evidence.sha256: evidence.raw_byte_count
            for locator_record in locator_records
            for evidence in locator_record.evidence_by_sha256.values()
        }.values()
    )
    monkeypatch.setattr(
        witness_streaming_encode,
        "PTG2_V3_SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES",
        aggregate_raw_bytes - 1,
    )
    monkeypatch.setattr(
        witness_streaming_encode,
        "_decompress_evidence",
        lambda *_args: pytest.fail("aggregate preflight must run before decompression"),
    )

    with pytest.raises(WitnessPayloadLimitError, match="aggregate decoded") as exc_info:
        witness_streaming_encode.encode_persisted_source_witness_candidates(
            locator_records,
            _payload_counts(len(locator_records)),
        )

    assert exc_info.value.retryable is False


def test_streaming_encoder_reserves_stored_budget_before_stage_write(monkeypatch):
    raw_evidence = b'{"rate":1}'
    evidence_sha256 = hashlib.sha256(raw_evidence).hexdigest()
    stage_file = BytesIO()
    budget = witness_streaming_encode._StreamingBudget()
    monkeypatch.setattr(
        witness_streaming_encode,
        "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES",
        48,
    )

    with pytest.raises(WitnessPayloadLimitError, match="logical payload"):
        witness_streaming_encode._stage_evidence(
            stage_file,
            {},
            budget,
            evidence_sha256=evidence_sha256,
            raw_evidence=raw_evidence,
        )

    assert stage_file.getvalue() == b""


def test_streaming_encoder_rejects_per_entry_decoded_budget(monkeypatch):
    monkeypatch.setattr(
        witness_streaming_encode,
        "PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES",
        3,
    )
    budget = witness_streaming_encode._StreamingBudget()

    with pytest.raises(WitnessPayloadLimitError, match="64 MiB per-entry"):
        budget.register_decoded_evidence("a" * 64, 4)


def test_locator_reader_rehashes_after_parse_even_when_stat_is_preserved(
    tmp_path,
    monkeypatch,
):
    bundle_entry = _write_dictionary_bundle(
        tmp_path,
        [
            _record(
                kind="rate_occurrence",
                priority=0,
                item_ordinal=0,
                raw_json=b'{"rate":1}',
                linked_provider_json=None,
            )
        ],
    )
    original_reader = witness_reader._read_current_dictionary_bundle

    def mutate_after_parse(bundle_file, bundle_identity, scanner_entry):
        result = original_reader(bundle_file, bundle_identity, scanner_entry)
        bundle_path = Path(bundle_identity.path)
        initial_stat = bundle_path.stat()
        with bundle_path.open("r+b") as mutable_bundle:
            mutable_bundle.seek(-1, os.SEEK_END)
            final_byte = mutable_bundle.read(1)
            mutable_bundle.seek(-1, os.SEEK_END)
            mutable_bundle.write(bytes([final_byte[0] ^ 1]))
            mutable_bundle.flush()
            os.fsync(mutable_bundle.fileno())
        os.utime(
            bundle_path,
            ns=(initial_stat.st_atime_ns, bundle_identity.mtime_ns),
        )
        return result

    monkeypatch.setattr(
        witness_reader,
        "_read_current_dictionary_bundle",
        mutate_after_parse,
    )

    with pytest.raises(RuntimeError, match="changed while reading"):
        witness_reader.read_scanner_bundle_locators(bundle_entry)


def test_materialization_rejects_replaced_authenticated_bundle(tmp_path):
    compressed_records = [
        _record(
            kind="rate_occurrence",
            priority=0,
            item_ordinal=0,
            raw_json=b'{"rate":1}',
            linked_provider_json=None,
        )
    ]
    bundle_entry = _write_dictionary_bundle(tmp_path, compressed_records)
    _header, locator_records = witness_reader.read_scanner_bundle_locators(
        bundle_entry
    )
    bundle_path = Path(str(bundle_entry["path"]))
    replacement_payload = bundle_path.read_bytes()
    replaced_path = tmp_path / "replaced.bin"
    bundle_path.replace(replaced_path)
    bundle_path.write_bytes(replacement_payload)

    try:
        witness_materialize.materialize_source_witness_locators(locator_records)
    except RuntimeError as exc:
        assert "changed before materialization" in str(exc)
    else:
        raise AssertionError("replaced authenticated bundle was accepted")


def test_materialization_rejects_in_place_record_rewrite_with_preserved_stat(
    tmp_path,
):
    compressed_records = [
        _record(
            kind="rate_occurrence",
            priority=0,
            item_ordinal=0,
            raw_json=b'{"rate":1}',
            linked_provider_json=None,
        )
    ]
    bundle_entry = _write_dictionary_bundle(tmp_path, compressed_records)
    _header, locator_records = witness_reader.read_scanner_bundle_locators(
        bundle_entry
    )
    locator = locator_records[0]
    bundle_path = Path(str(bundle_entry["path"]))
    authenticated_stat = bundle_path.stat()

    with bundle_path.open("r+b") as bundle_file:
        bundle_file.seek(locator.offset)
        compressed_record = bundle_file.read(locator.length)
        decoded_record = zlib.decompress(compressed_record)
        tampered_record = decoded_record.replace(
            b'"object_ordinal":1',
            b'"object_ordinal":2',
            1,
        )
        assert tampered_record != decoded_record
        tampered_compressed = zlib.compress(tampered_record, level=1)
        assert len(tampered_compressed) == locator.length
        bundle_file.seek(locator.offset)
        bundle_file.write(tampered_compressed)
        bundle_file.flush()
        os.fsync(bundle_file.fileno())
    os.utime(
        bundle_path,
        ns=(authenticated_stat.st_atime_ns, authenticated_stat.st_mtime_ns),
    )

    try:
        witness_materialize.materialize_source_witness_locators(locator_records)
    except RuntimeError as exc:
        assert "changed" in str(exc) or "digest" in str(exc)
    else:
        raise AssertionError("in-place rewritten authenticated bundle was accepted")
