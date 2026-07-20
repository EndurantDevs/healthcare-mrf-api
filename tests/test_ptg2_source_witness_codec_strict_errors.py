# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import zlib
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_source_witness_codec as codec


def _framed_tokens(raw_json: bytes = b"", linked_provider_json: bytes = b""):
    return b"".join(
        (
            codec.U32.pack(len(raw_json)),
            raw_json,
            codec.U32.pack(len(linked_provider_json)),
            linked_provider_json,
        )
    )


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def test_source_witness_rejects_non_object_metadata_and_coordinates():
    with pytest.raises(RuntimeError, match="invalid JSON"):
        codec._json_object(b"{", field_name="record metadata")
    with pytest.raises(RuntimeError, match="must be an object"):
        codec._json_object(b"[]", field_name="record metadata")
    with pytest.raises(RuntimeError, match="coordinate is invalid"):
        codec._coordinate({})
    with pytest.raises(RuntimeError, match="coordinate is invalid"):
        codec._coordinate(
            {
                "coordinate": {
                    "object_ordinal": True,
                    "rate_ordinal": 0,
                    "price_ordinal": 0,
                    "provider_ordinal": 0,
                }
            }
        )
    with pytest.raises(RuntimeError, match="expected evidence is invalid"):
        codec._optional_mapping([], field_name="expected evidence")


def test_source_witness_rejects_trailing_compressed_data():
    with pytest.raises(RuntimeError, match="violates its zlib framing"):
        codec._decompress_record(zlib.compress(b"record") + b"trailing")


def test_source_witness_rejects_invalid_token_and_metadata_framing():
    with pytest.raises(RuntimeError, match="raw JSON token is invalid"):
        codec._framed_raw_tokens(codec.U32.pack(4) + b"x", 0)
    with pytest.raises(RuntimeError, match="linked provider token is invalid"):
        codec._framed_raw_tokens(
            codec.U32.pack(0) + codec.U32.pack(0) + b"x",
            0,
        )
    with pytest.raises(RuntimeError, match="record magic is invalid"):
        codec._decoded_record_metadata(zlib.compress(b"bad"))
    with pytest.raises(RuntimeError, match="record metadata is truncated"):
        codec._decoded_record_metadata(
            zlib.compress(codec.SOURCE_RECORD_MAGIC + codec.U32.pack(1))
        )


def test_persisted_source_witness_rejects_embedded_or_missing_raw_evidence():
    raw_json = b"raw"
    raw_sha256 = _digest(raw_json)

    with pytest.raises(RuntimeError, match="unexpectedly embeds raw evidence"):
        codec._verified_raw_evidence(
            _framed_tokens(raw_json),
            0,
            {"raw_sha256": raw_sha256},
            evidence_by_sha256={raw_sha256: raw_json},
        )
    with pytest.raises(RuntimeError, match="persisted raw evidence is missing"):
        codec._verified_raw_evidence(
            _framed_tokens(),
            0,
            {"raw_sha256": raw_sha256},
            evidence_by_sha256={},
        )
    with pytest.raises(RuntimeError, match="raw token digest is invalid"):
        codec._verified_raw_evidence(
            _framed_tokens(raw_json),
            0,
            {"raw_sha256": _digest(b"different")},
        )


def test_persisted_source_witness_rejects_invalid_linked_evidence():
    raw_json = b"raw"
    linked_provider_json = b"linked"
    raw_sha256 = _digest(raw_json)
    linked_sha256 = _digest(linked_provider_json)
    metadata = {
        "raw_sha256": raw_sha256,
        "linked_provider_sha256": linked_sha256,
    }

    with pytest.raises(RuntimeError, match="unexpectedly embeds provider evidence"):
        codec._verified_raw_evidence(
            _framed_tokens(linked_provider_json=linked_provider_json),
            0,
            metadata,
            evidence_by_sha256={
                raw_sha256: raw_json,
                linked_sha256: linked_provider_json,
            },
        )
    with pytest.raises(RuntimeError, match="persisted provider evidence is missing"):
        codec._verified_raw_evidence(
            _framed_tokens(),
            0,
            metadata,
            evidence_by_sha256={raw_sha256: raw_json},
        )
    with pytest.raises(RuntimeError, match="linked provider evidence is incomplete"):
        codec._verified_raw_evidence(
            _framed_tokens(raw_json),
            0,
            metadata,
        )
    with pytest.raises(RuntimeError, match="linked provider digest is invalid"):
        codec._verified_raw_evidence(
            _framed_tokens(raw_json, linked_provider_json),
            0,
            {
                "raw_sha256": raw_sha256,
                "linked_provider_sha256": _digest(b"different"),
            },
        )


def test_source_witness_rejects_invalid_record_fields():
    valid_metadata = {
        "contract": codec.PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT,
        "kind": "provider_reference",
        "priority": "0000000000000001",
        "tie_breaker": "00" * 32,
        "expected": {},
    }

    invalid_contract = {**valid_metadata, "contract": "invalid"}
    with pytest.raises(RuntimeError, match="record contract is invalid"):
        codec._record_fields(invalid_contract)

    invalid_kind = {**valid_metadata, "kind": "invalid"}
    with pytest.raises(RuntimeError, match="record kind is invalid"):
        codec._record_fields(invalid_kind)

    invalid_priority = {**valid_metadata, "priority": "1"}
    with pytest.raises(RuntimeError, match="priority is invalid"):
        codec._record_fields(invalid_priority)

    missing_expected = {**valid_metadata, "expected": None}
    with pytest.raises(RuntimeError, match="expected evidence is missing"):
        codec._record_fields(missing_expected)


@pytest.mark.parametrize(
    ("arguments", "message"),
    (
        (
            {
                "witness_kind": "provider_reference",
                "procedure": {},
                "provider_evidence": None,
                "linked_provider_json": None,
            },
            "provider witness shape is invalid",
        ),
        (
            {
                "witness_kind": "rate_occurrence",
                "procedure": None,
                "provider_evidence": {},
                "linked_provider_json": None,
            },
            "occurrence witness shape is incomplete",
        ),
        (
            {
                "witness_kind": "rate_occurrence",
                "procedure": {},
                "provider_evidence": {"source_kind": "provider_reference"},
                "linked_provider_json": None,
            },
            "lacks raw provider evidence",
        ),
        (
            {
                "witness_kind": "rate_occurrence",
                "procedure": {},
                "provider_evidence": {"source_kind": "inline_provider_group"},
                "linked_provider_json": b"linked",
            },
            "unexpected provider evidence",
        ),
        (
            {
                "witness_kind": "rate_occurrence",
                "procedure": {},
                "provider_evidence": {"source_kind": "invalid"},
                "linked_provider_json": None,
            },
            "provider source is invalid",
        ),
    ),
)
def test_source_witness_rejects_invalid_record_shapes(arguments, message):
    with pytest.raises(RuntimeError, match=message):
        codec._validate_record_shape(**arguments)


def test_source_witness_externalization_rejects_incomplete_linked_evidence(
    monkeypatch,
):
    monkeypatch.setattr(
        codec,
        "decode_record",
        lambda *_arguments: SimpleNamespace(
            raw_sha256=_digest(b"raw"),
            raw_json=b"raw",
            linked_provider_sha256=_digest(b"linked"),
            linked_provider_json=None,
        ),
    )
    monkeypatch.setattr(
        codec,
        "_decoded_record_metadata",
        lambda *_arguments: (b"", {}, 0),
    )

    with pytest.raises(RuntimeError, match="linked provider evidence is incomplete"):
        codec.externalize_source_evidence_record(b"record", "00" * 32)


def test_source_witness_externalization_rejects_digest_alias_conflicts(monkeypatch):
    shared_digest = _digest(b"raw")
    monkeypatch.setattr(
        codec,
        "decode_record",
        lambda *_arguments: SimpleNamespace(
            raw_sha256=shared_digest,
            raw_json=b"raw",
            linked_provider_sha256=shared_digest,
            linked_provider_json=b"different",
        ),
    )
    monkeypatch.setattr(
        codec,
        "_decoded_record_metadata",
        lambda *_arguments: (b"", {}, 0),
    )

    with pytest.raises(RuntimeError, match="source evidence digest is inconsistent"):
        codec.externalize_source_evidence_record(b"record", "00" * 32)
