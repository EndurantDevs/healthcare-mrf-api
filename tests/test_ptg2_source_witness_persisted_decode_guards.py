# See LICENSE.

from __future__ import annotations

import hashlib
import struct
import zlib
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_source_witness_persisted_decode as decode


_U32 = struct.Struct(">I")
_SOURCE_A = "11" * 32


def _evidence_entry(
    raw_json: bytes,
    *,
    digest: bytes | None = None,
) -> bytes:
    compressed = zlib.compress(raw_json)
    return b"".join(
        (
            digest or hashlib.sha256(raw_json).digest(),
            _U32.pack(len(compressed)),
            compressed,
        )
    )


def test_persisted_decode_rejects_header_and_scope_corruption():
    magic = decode.PERSISTED_PAYLOAD_MAGIC

    with pytest.raises(RuntimeError, match="header is truncated"):
        decode._read_header(magic + _U32.pack(3) + b"{}")
    with pytest.raises(RuntimeError, match="header is invalid"):
        decode._read_header(magic + _U32.pack(1) + b"{")
    with pytest.raises(RuntimeError, match="header is invalid"):
        decode._read_header(magic + _U32.pack(2) + b"[]")
    with pytest.raises(RuntimeError, match="contract is invalid"):
        decode._validate_header_contract({})

    valid_scope_by_field = {
        "source_count": 1,
        "source_set_digest": decode.source_set_digest([_SOURCE_A]),
        "emitted_rate_row_count": 1,
        "unqueryable_rate_row_count": 0,
        "evidence_dictionary_count": 0,
        "record_count": 1,
    }
    with pytest.raises(RuntimeError, match="rate policy"):
        decode._validate_header_scope(
            {**valid_scope_by_field, "emitted_rate_row_count": 0},
            [_SOURCE_A],
        )
    with pytest.raises(RuntimeError, match="dictionary count"):
        decode._validate_header_scope(
            {**valid_scope_by_field, "evidence_dictionary_count": 3},
            [_SOURCE_A],
        )


def test_persisted_decode_rejects_evidence_compression_corruption(monkeypatch):
    with monkeypatch.context() as scoped_patch:
        scoped_patch.setattr(
            decode,
            "PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES",
            1,
        )
        with pytest.raises(RuntimeError, match="decode budget"):
            decode._decompress_evidence(zlib.compress(b"{}"))

    with pytest.raises(RuntimeError, match="invalid zlib framing"):
        decode._decompress_evidence(b"not-zlib")
    with pytest.raises(RuntimeError, match="violates its zlib framing"):
        decode._decompress_evidence(zlib.compress(b"{}")[:-1])


def test_persisted_decode_rejects_evidence_entry_corruption():
    with pytest.raises(RuntimeError, match="digest is truncated"):
        decode._read_evidence_entry(b"x" * 31, 0, "")
    with pytest.raises(RuntimeError, match="order is invalid"):
        decode._read_evidence_entry(b"\0" * 32, 0, "0" * 64)
    with pytest.raises(RuntimeError, match="record is invalid"):
        decode._read_evidence_entry(b"\1" * 32 + _U32.pack(0), 0, "")
    with pytest.raises(RuntimeError, match="digest is invalid"):
        decode._read_evidence_entry(
            _evidence_entry(b"{}", digest=b"\1" * 32),
            0,
            "",
        )
    with pytest.raises(RuntimeError, match="JSON is invalid"):
        decode._read_evidence_entry(_evidence_entry(b"{"), 0, "")
    with pytest.raises(RuntimeError, match="must be an object"):
        decode._read_evidence_entry(_evidence_entry(b"[]"), 0, "")


def test_persisted_decode_rejects_dictionary_metrics_and_counts():
    with pytest.raises(RuntimeError, match="byte counts"):
        decode._validate_evidence_dictionary_metrics(
            {
                "evidence_dictionary_raw_bytes": 0,
                "evidence_dictionary_stored_bytes": 0,
            },
            raw_byte_count=1,
            stored_byte_count=0,
        )
    with pytest.raises(RuntimeError, match="count does not match"):
        decode._decode_evidence_dictionary(
            _U32.pack(1),
            dictionary_offset=0,
            header={"evidence_dictionary_count": 0},
        )


def test_persisted_decode_rejects_record_framing():
    with pytest.raises(RuntimeError, match="source digest is truncated"):
        decode._read_persisted_record(
            b"x" * 31,
            record_offset=0,
            expected_sources={_SOURCE_A},
            evidence_by_sha256={},
        )
    with pytest.raises(RuntimeError, match="unknown source"):
        decode._read_persisted_record(
            b"\0" * 32,
            record_offset=0,
            expected_sources={_SOURCE_A},
            evidence_by_sha256={},
        )
    with pytest.raises(RuntimeError, match="record framing"):
        decode._read_persisted_record(
            bytes.fromhex(_SOURCE_A) + _U32.pack(0),
            record_offset=0,
            expected_sources={_SOURCE_A},
            evidence_by_sha256={},
        )


def test_persisted_decode_rejects_record_collection_corruption(monkeypatch):
    with pytest.raises(RuntimeError, match="record count"):
        decode._decode_persisted_records(
            _U32.pack(0),
            record_offset=0,
            expected_sources={_SOURCE_A},
            evidence_by_sha256={},
        )

    witness_record = SimpleNamespace(
        raw_sha256=None,
        linked_provider_sha256=None,
    )
    monkeypatch.setattr(
        decode,
        "_read_persisted_record",
        lambda *_args, **_kwargs: (witness_record, b"x", _SOURCE_A, 4),
    )

    with pytest.raises(RuntimeError, match="trailing bytes"):
        decode._decode_persisted_records(
            _U32.pack(1) + b"x",
            record_offset=0,
            expected_sources={_SOURCE_A},
            evidence_by_sha256={},
        )
    with pytest.raises(RuntimeError, match="coverage is inconsistent"):
        decode._decode_persisted_records(
            _U32.pack(1),
            record_offset=0,
            expected_sources={_SOURCE_A},
            evidence_by_sha256={"aa": b"{}"},
        )


def test_persisted_decode_rejects_final_count_and_framing_corruption():
    header_by_field = {
        "queryable_occurrence_population_count": 1,
        "provider_population_count": 0,
        "occurrence_witness_count": 1,
        "provider_witness_count": 0,
        "record_count": 1,
    }
    with pytest.raises(RuntimeError, match="coverage is incomplete"):
        decode._validate_persisted_counts(header_by_field, [])

    with pytest.raises(RuntimeError, match="framing is invalid"):
        decode.decode_persisted_source_witness(
            b"",
            expected_raw_source_sha256=[_SOURCE_A],
        )
    with pytest.raises(RuntimeError, match="source set is invalid"):
        decode.decode_persisted_source_witness(
            decode.PERSISTED_PAYLOAD_MAGIC,
            expected_raw_source_sha256=[_SOURCE_A, _SOURCE_A],
        )


def test_persisted_decode_rejects_final_digest_corruption(monkeypatch):
    monkeypatch.setattr(
        decode,
        "_persisted_header",
        lambda *_args, **_kwargs: ({"sample_digest": "expected"}, 0),
    )
    monkeypatch.setattr(
        decode,
        "_decode_evidence_dictionary",
        lambda *_args, **_kwargs: ({}, {}, 0),
    )
    monkeypatch.setattr(
        decode,
        "_decode_persisted_records",
        lambda *_args, **_kwargs: ([], "actual"),
    )
    monkeypatch.setattr(
        decode,
        "_validate_persisted_counts",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(RuntimeError, match="digest is inconsistent"):
        decode.decode_persisted_source_witness(
            decode.PERSISTED_PAYLOAD_MAGIC,
            expected_raw_source_sha256=[_SOURCE_A],
        )
