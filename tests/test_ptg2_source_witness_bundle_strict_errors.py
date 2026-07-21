# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import json
import zlib

import pytest

from process.ptg_parts import ptg2_source_witness_bundle as bundle
from process.ptg_parts.ptg2_source_witness_primitives import U32


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _entry(path, payload: bytes, **overrides):
    return {
        "path": str(path),
        "sha256": _digest(payload),
        "byte_count": len(payload),
        "raw_source_sha256": "00" * 32,
        "row_count": 0,
        **overrides,
    }


def _header(raw_source_sha256: str = "00" * 32):
    return {
        "contract": bundle.PTG2_V3_SOURCE_WITNESS_CONTRACT,
        "selection_method": bundle.PTG2_V3_SOURCE_WITNESS_SELECTION,
        "format_version": 2,
        "occurrence_target": bundle.PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
        "total_target": bundle.PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
        "provider_quota": bundle.PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
        "unqueryable_rate_policy": bundle.PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
        "raw_source_sha256": raw_source_sha256,
    }


def _evidence_entry(raw_evidence: bytes, *, digest: str | None = None) -> bytes:
    compressed_evidence = zlib.compress(raw_evidence)
    return b"".join(
        (
            bytes.fromhex(digest or _digest(raw_evidence)),
            U32.pack(len(raw_evidence)),
            U32.pack(len(compressed_evidence)),
            compressed_evidence,
        )
    )


def test_source_witness_bundle_header_json_must_be_an_object():
    with pytest.raises(RuntimeError, match="invalid JSON"):
        bundle._json_object(b"{", field_name="bundle header")
    with pytest.raises(RuntimeError, match="must be an object"):
        bundle._json_object(b"[]", field_name="bundle header")


def test_source_witness_bundle_file_must_exist_and_be_nonempty(tmp_path):
    missing_path = tmp_path / "missing.bin"
    with pytest.raises(RuntimeError, match="bundle is missing"):
        bundle._authenticated_bundle_payload(_entry(missing_path, b""))

    empty_path = tmp_path / "empty.bin"
    empty_path.write_bytes(b"")
    with pytest.raises(RuntimeError, match="bundle size is invalid"):
        bundle._authenticated_bundle_payload(_entry(empty_path, b""))


def test_source_witness_bundle_detects_file_changes_while_reading(
    tmp_path,
    monkeypatch,
):
    bundle_path = tmp_path / "bundle.bin"
    bundle_path.write_bytes(b"x")
    monkeypatch.setattr(bundle.Path, "read_bytes", lambda _path: b"changed")

    with pytest.raises(RuntimeError, match="changed while reading"):
        bundle._authenticated_bundle_payload(_entry(bundle_path, b"x"))


def test_source_witness_bundle_authenticates_digest_size_and_magic(tmp_path):
    bundle_path = tmp_path / "bundle.bin"
    bundle_path.write_bytes(b"x")

    with pytest.raises(RuntimeError, match="digest does not match"):
        bundle._authenticated_bundle_payload(
            _entry(bundle_path, b"x", sha256="00" * 32)
        )
    with pytest.raises(RuntimeError, match="byte count does not match"):
        bundle._authenticated_bundle_payload(
            _entry(bundle_path, b"x", byte_count=2)
        )
    with pytest.raises(RuntimeError, match="bundle magic is invalid"):
        bundle._authenticated_bundle_payload(_entry(bundle_path, b"x"))


def test_source_witness_bundle_rejects_invalid_record_framing():
    with pytest.raises(RuntimeError, match="record framing is invalid"):
        bundle._bundle_records(
            U32.pack(0),
            record_count=1,
            record_offset=0,
            raw_source_sha256="00" * 32,
        )


@pytest.mark.parametrize(
    ("compressed_evidence", "raw_length", "expected_error"),
    (
        (zlib.compress(b"x"), 0, "length is invalid"),
        (zlib.compress(b"ab"), 1, "violates its framing"),
        (zlib.compress(b"a"), 2, "violates its framing"),
        (b"not-zlib", 1, "invalid zlib framing"),
    ),
)
def test_source_witness_bundle_rejects_invalid_evidence_compression(
    compressed_evidence,
    raw_length,
    expected_error,
):
    with pytest.raises(RuntimeError, match=expected_error):
        bundle._decompress_evidence(compressed_evidence, raw_length)


def test_source_witness_bundle_rejects_malformed_evidence_dictionary():
    with pytest.raises(RuntimeError, match="digest is truncated"):
        bundle._bundle_evidence_dictionary(
            b"x" * 31,
            evidence_count=1,
            evidence_offset=0,
        )
    with pytest.raises(RuntimeError, match="framing is invalid"):
        bundle._bundle_evidence_dictionary(
            (b"\x00" * 32) + U32.pack(1) + U32.pack(0),
            evidence_count=1,
            evidence_offset=0,
        )
    with pytest.raises(RuntimeError, match="digest is invalid"):
        bundle._bundle_evidence_dictionary(
            _evidence_entry(b"evidence", digest="00" * 32),
            evidence_count=1,
            evidence_offset=0,
        )


def test_source_witness_bundle_rejects_duplicate_evidence_digest():
    evidence_entry = _evidence_entry(b"evidence")
    with pytest.raises(RuntimeError, match="framing is invalid"):
        bundle._bundle_evidence_dictionary(
            evidence_entry + evidence_entry,
            evidence_count=2,
            evidence_offset=0,
        )


def test_source_witness_bundle_rejects_contract_and_source_digest_changes():
    invalid_header_by_field = {**_header(), "contract": "invalid"}
    with pytest.raises(RuntimeError, match="bundle contract is invalid"):
        bundle._validate_bundle_header(
            invalid_header_by_field,
            {"raw_source_sha256": "00" * 32},
        )

    with pytest.raises(RuntimeError, match="source digest changed"):
        bundle._validate_bundle_header(
            _header("00" * 32),
            {"raw_source_sha256": "11" * 32},
        )


def test_source_witness_bundle_rejects_invalid_local_coverage_metrics():
    with pytest.raises(RuntimeError, match="cohort metrics are invalid"):
        bundle._validate_local_coverage(
            {"rate_occurrence": None, "provider_reference": {}},
            [],
        )
    with pytest.raises(RuntimeError, match="unqueryable rate count is invalid"):
        bundle._validate_local_coverage(
            {
                "rate_occurrence": {
                    "emitted_rate_row_count": 0,
                    "unqueryable_rate_row_count": 1,
                },
                "provider_reference": {},
            },
            [],
        )


def test_source_witness_bundle_reader_rejects_truncated_header(monkeypatch):
    payload = bundle.SOURCE_BUNDLE_MAGIC + U32.pack(4) + b"x"
    monkeypatch.setattr(bundle, "_authenticated_bundle_payload", lambda _entry: payload)

    with pytest.raises(RuntimeError, match="bundle header is truncated"):
        bundle.read_scanner_bundle({})


def test_source_witness_bundle_reader_rejects_row_count_mismatch(monkeypatch):
    header_bytes = json.dumps(
        _header(),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    payload = b"".join(
        (
            bundle.SOURCE_BUNDLE_MAGIC,
            U32.pack(len(header_bytes)),
            header_bytes,
            U32.pack(0),
        )
    )
    monkeypatch.setattr(bundle, "_authenticated_bundle_payload", lambda _entry: payload)

    with pytest.raises(RuntimeError, match="record count does not match"):
        bundle.read_scanner_bundle(
            {"raw_source_sha256": "00" * 32, "row_count": 1}
        )
