# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import hashlib
import json
import weakref
import zlib
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal

import pytest

from process.ptg_parts import ptg2_candidate_audit_evidence as audit_evidence
from process.ptg_parts import ptg2_source_witness_codec as codec
from process.ptg_parts import ptg2_source_witness_fragment_decode as decode
from process.ptg_parts.ptg2_source_witness_contract import CompressedSourceWitnessRecord
from process.ptg_parts.ptg2_source_witness_fragment_encode import encode_fragment_source_witness_candidates
from process.ptg_parts.ptg2_source_witness_fragments import encode_fragment_recipe
from process.ptg_parts.ptg2_source_witness_persisted_encode import (
    SourceWitnessPayloadCounts,
    _payload_header,
    _PersistedRecord,
)
from process.ptg_parts.ptg2_source_witness_primitives import U32
from process.ptg_parts.ptg2_source_witness_selection import source_set_digest
from tests.test_ptg2_fast_candidate_audit import _occurrence_record, _provider_record
from tests.test_ptg2_source_witness import SOURCE_A, SOURCE_B, _record, _record_metadata_by_field


def _parts():
    prefix = b" " * 4096
    provider = b'{"provider_group_id":1,"provider_groups":[]}'
    tokens = [
        prefix + b'{"rate":1.2300e+02}',
        prefix + b'{"rate":2.000}',
        b'{"name":"' + b"x" * 4086 + "\u20ac\U0001f642".encode() + b'","rate":0.0000000000000000001}',
        b'{"negotiated_prices":[]}',
        provider,
    ]
    fragment_bytes_by_sha256 = {}
    recipe_by_sha256 = {}
    record_frames = []
    for index, token in enumerate(tokens):
        original = _record(
            kind="provider_reference" if index == 4 else "rate_occurrence",
            priority=index,
            item_ordinal=index,
            raw_json=token,
            linked_provider_json=provider if index == 3 else None,
        )
        source_digest = SOURCE_B if index == 4 else SOURCE_A
        compressed, evidence = codec.externalize_source_evidence_record(original, source_digest)
        record_frames.append((source_digest, compressed))
        for digest, raw in evidence.items():
            recipe_by_sha256[digest] = encode_fragment_recipe(raw, fragment_bytes_by_sha256.__setitem__)
    record_frames.sort(
        key=lambda row: (
            codec.decode_record_locator_fields(row[1]).kind,
            codec.decode_record_locator_fields(row[1]).priority,
        )
    )
    counts = SourceWitnessPayloadCounts(2, source_set_digest([SOURCE_A, SOURCE_B]), 4, 1, 4, 0, 4, 1, 5)
    header = _payload_header(
        counts, [_PersistedRecord(source_digest, compressed) for source_digest, compressed in record_frames], []
    )
    header.update(
        contract=decode.PTG2_V3_SOURCE_WITNESS_FRAGMENT_PAYLOAD_CONTRACT,
        format_version=6,
        compression=decode.PTG2_V3_SOURCE_WITNESS_FRAGMENT_COMPRESSION,
        fragment_byte_count=4096,
    )
    return (
        header,
        [(digest, len(raw), zlib.compress(raw)) for digest, raw in sorted(fragment_bytes_by_sha256.items())],
        [recipe_by_sha256[digest] for digest in sorted(recipe_by_sha256)],
        record_frames,
    )


def _payload(parts, **overrides):
    header_by_field, fragments, recipes, record_frames = parts
    header_by_field = {
        **header_by_field,
        "fragment_count": len(fragments),
        "evidence_dictionary_count": len(recipes),
        "evidence_dictionary_raw_bytes": sum(length for _, length, _ in fragments),
        "evidence_dictionary_stored_bytes": sum(len(compressed) for _, _, compressed in fragments),
        "evidence_reconstructed_bytes": sum(recipe["raw_byte_count"] for recipe in recipes),
        "recipe_reference_count": sum(len(recipe["fragment_sha256"]) for recipe in recipes),
        **overrides,
    }
    encoded_header = json.dumps(header_by_field, sort_keys=True, separators=(",", ":")).encode()
    body = bytearray(
        decode.FRAGMENT_PERSISTED_PAYLOAD_MAGIC
        + U32.pack(len(encoded_header))
        + encoded_header
        + U32.pack(len(fragments))
    )
    for digest, raw_length, compressed in fragments:
        body.extend(bytes.fromhex(digest) + U32.pack(raw_length) + U32.pack(len(compressed)) + compressed)
    body.extend(U32.pack(len(recipes)))
    for recipe in recipes:
        body.extend(
            bytes.fromhex(recipe["raw_sha256"])
            + U32.pack(recipe["raw_byte_count"])
            + U32.pack(len(recipe["fragment_sha256"]))
        )
        body.extend(b"".join(bytes.fromhex(digest) for digest in recipe["fragment_sha256"]))
    body.extend(U32.pack(len(record_frames)))
    for source_digest, compressed in record_frames:
        body.extend(bytes.fromhex(source_digest) + U32.pack(len(compressed)) + compressed)
    return bytes(body)


def _decode(payload, **kwargs):
    return decode.decode_fragment_source_witness(payload, expected_raw_source_sha256=[SOURCE_A, SOURCE_B], **kwargs)


def test_v6_authenticates_complete_corpus_and_decodes_records_lazily(monkeypatch):
    parts = _parts()
    witness_payload = bytearray(_payload(parts))
    original_decoder = decode.decode_persisted_record
    decoded_calls = []

    def decode_one(*args, **kwargs):
        decoded_calls.append(args[1])
        return original_decoder(*args, **kwargs)

    monkeypatch.setattr(decode, "decode_persisted_record", decode_one)
    view = _decode(witness_payload)
    assert decoded_calls == []
    assert view.evidence_by_sha256 is None
    assert len(view.records) == 5
    assert view.metadata["evidence_reconstructed_bytes"] > view.metadata["evidence_dictionary_raw_bytes"]
    assert view.metadata["payload_sha256"] == hashlib.sha256(witness_payload).hexdigest()
    witness_payload[:] = b"x" * len(witness_payload)
    first_slice = view.records[:1]
    assert decoded_calls == []
    assert first_slice[0] == view.records[0]
    assert view.records[-1] == view.records[4]
    assert tuple(view.records) == tuple(view.records)
    assert len(decoded_calls) == 14
    with pytest.raises(IndexError):
        view.records[5]
    with pytest.raises(TypeError):
        view.records[1.5]
    with pytest.raises(TypeError):
        view.metadata["format_version"] = 5
    assert _decode(_payload(parts), expected_metadata=view.metadata).metadata == view.metadata


def test_v6_preserves_utf8_decimal_and_provider_linked_bytes(monkeypatch):
    original_loads = decode.json.loads
    parsed_rates = []

    def load_json(raw, **kwargs):
        value = original_loads(raw, **kwargs)
        if kwargs.get("parse_float") is Decimal and isinstance(value, dict) and "rate" in value:
            parsed_rates.append(value["rate"])
        return value

    monkeypatch.setattr(decode.json, "loads", load_json)
    view = _decode(_payload(_parts()))
    records = tuple(view.records)
    assert Decimal("1.2300e+02") in parsed_rates
    assert Decimal("0.0000000000000000001") in parsed_rates
    assert all(isinstance(rate, Decimal) for rate in parsed_rates)
    provider = records[0]
    linked = next(record for record in records if record.linked_provider_json is not None)
    assert linked.linked_provider_json == provider.raw_json
    assert linked.linked_provider_sha256 == provider.raw_sha256
    assert any("\u20ac\U0001f642".encode() in record.raw_json for record in records)
    assert any(b"1.2300e+02" in record.raw_json for record in records)


def test_v6_native_encoder_roundtrip_preserves_original_source_records():
    provider = b'{"provider_group_id":1,"provider_groups":[]}'
    selected_candidates = []
    expected_records = []
    for index, (kind, raw, linked) in enumerate(
        [
            ("provider_reference", provider, None),
            ("rate_occurrence", b'{"rate":1.2300e+02}', provider),
            ("rate_occurrence", b'{"name":"\xe2\x82\xac","rate":0.0000000000000000001}', None),
        ]
    ):
        compressed = _record(kind=kind, priority=index, item_ordinal=index, raw_json=raw, linked_provider_json=linked)
        source_digest = SOURCE_B if index == 0 else SOURCE_A
        externalized, evidence = codec.externalize_source_evidence_record(compressed, source_digest)
        decoded_record = codec.decode_persisted_record(externalized, source_digest, evidence_by_sha256=evidence)
        expected_records.append(decoded_record)
        selected_candidates.append(
            CompressedSourceWitnessRecord(
                kind, decoded_record.priority, decoded_record.tie_breaker, source_digest, compressed
            )
        )
    counts = SourceWitnessPayloadCounts(2, source_set_digest([SOURCE_A, SOURCE_B]), 2, 1, 2, 0, 2, 1, 3)
    witness_payload, metadata = encode_fragment_source_witness_candidates(selected_candidates, counts)
    view = _decode(witness_payload, expected_metadata=metadata)
    assert tuple(view.records) == tuple(expected_records)
    assert view.evidence_by_sha256 is None
    assert (
        view.metadata["sample_digest"]
        == hashlib.sha256(
            b"".join(
                bytes.fromhex(candidate.raw_source_sha256)
                + codec.externalize_source_evidence_record(candidate.compressed, candidate.raw_source_sha256)[0]
                for candidate in selected_candidates
            )
        ).hexdigest()
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("contract", "unknown"),
        ("format_version", 5),
        ("format_version", 6.0),
        ("compression", "unknown"),
        ("fragment_byte_count", 2048),
        ("fragment_count", 0),
        ("fragment_count", True),
        ("evidence_dictionary_count", 0),
        ("recipe_reference_count", 0),
        ("evidence_reconstructed_bytes", 0),
        ("record_count", 0),
        ("occurrence_witness_count", 3),
        ("provider_witness_count", 0),
        ("evidence_dictionary_raw_bytes", 0),
        ("evidence_dictionary_stored_bytes", 0),
        ("sample_digest", "00" * 32),
        ("source_count", 1),
    ],
)
def test_v6_rejects_header_contract_and_count_drift(field, value):
    with pytest.raises(RuntimeError):
        _decode(_payload(_parts(), **{field: value}))


@pytest.mark.parametrize("table", ["fragments", "recipes", "records"])
def test_v6_rejects_noncanonical_order(table):
    parts = _parts()
    parts[["fragments", "recipes", "records"].index(table) + 1].reverse()
    with pytest.raises(RuntimeError, match="order"):
        _decode(_payload(parts))


@pytest.mark.parametrize(
    "corruption",
    ["digest", "length", "truncated_zlib", "zlib_trailing", "unknown_ref", "ref_count", "token_length", "whole_digest"],
)
def test_v6_rejects_fragment_and_recipe_corruption(corruption):
    header, fragments, recipes, records = _parts()
    digest, length, compressed = fragments[0]
    match corruption:
        case "digest":
            fragments[0] = (digest, length, zlib.compress(b"x" * length))
        case "length":
            fragments[0] = (digest, 0, compressed)
        case "truncated_zlib":
            fragments[0] = (digest, length, compressed[:-1])
        case "zlib_trailing":
            fragments[0] = (digest, length, compressed + b"extra")
        case "unknown_ref":
            recipes[0]["fragment_sha256"][0] = "00" * 32
        case "ref_count":
            recipes[0]["fragment_sha256"].append(recipes[0]["fragment_sha256"][0])
        case "token_length":
            recipes[0]["raw_byte_count"] = 0
        case "whole_digest":
            recipes[0]["fragment_sha256"], recipes[1]["fragment_sha256"] = (
                recipes[1]["fragment_sha256"],
                recipes[0]["fragment_sha256"],
            )
    with pytest.raises(RuntimeError):
        _decode(_payload((header, fragments, recipes, records)))


def test_v6_rejects_unused_evidence_and_trailing_bytes():
    parts = _parts()
    raw = b'{"unused":true}'
    digest = hashlib.sha256(raw).hexdigest()
    parts[1].append((digest, len(raw), zlib.compress(raw)))
    parts[1].sort()
    with pytest.raises(RuntimeError, match="unused"):
        _decode(_payload(parts))
    parts[2].append({"raw_sha256": digest, "raw_byte_count": len(raw), "fragment_sha256": [digest]})
    parts[2].sort(key=lambda recipe: recipe["raw_sha256"])
    with pytest.raises(RuntimeError, match="unused"):
        _decode(_payload(parts))
    with pytest.raises(RuntimeError, match="trailing"):
        _decode(_payload(_parts()) + b"extra")


@pytest.mark.parametrize("raw", [b"[]", b"{", b'{"name":"\xff"}'])
def test_v6_rejects_invalid_json_before_view_acceptance(raw):
    digest = hashlib.sha256(raw).hexdigest()
    compressed = zlib.compress(raw)
    fragment_by_sha256 = {digest: decode._Fragment(len(raw), 0, len(compressed))}
    recipe = decode._Recipe(len(raw), len(zlib.compress(raw)), 1)
    framed = compressed + bytes.fromhex(digest)
    with pytest.raises(RuntimeError, match="JSON"):
        decode._validate_tokens(framed, {digest: recipe}, fragment_by_sha256)


@pytest.mark.parametrize(
    ("constant", "header_field"),
    [
        ("PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENT_REFERENCES", "recipe_reference_count"),
        ("PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES", "evidence_reconstructed_bytes"),
        ("PTG2_V3_SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES", "evidence_dictionary_raw_bytes"),
    ],
)
def test_v6_checks_work_budgets_before_reconstruction(monkeypatch, constant, header_field):
    monkeypatch.setattr(decode, constant, 1)
    monkeypatch.setattr(decode, "_token", lambda *_args: pytest.fail("work bound must be checked first"))
    with pytest.raises(decode.WitnessPayloadLimitError, match="bound"):
        _decode(_payload(_parts(), **{header_field: 2}))


def test_v6_rejects_missing_recipe_metrics_manifest_drift_and_wrong_sources():
    header, fragments, recipes, records = _parts()
    for field in ("evidence_reconstructed_bytes", "recipe_reference_count"):
        with pytest.raises(RuntimeError):
            _decode(_payload((header, fragments, recipes, records), **{field: None}))
    with pytest.raises(RuntimeError, match="manifest fields changed"):
        _decode(_payload(_parts()), expected_metadata={})
    with pytest.raises(RuntimeError, match="source set"):
        decode.decode_fragment_source_witness(_payload(_parts()), expected_raw_source_sha256=[SOURCE_A, SOURCE_A])
    malformed = deepcopy(records)
    malformed[0] = ("33" * 32, malformed[0][1])
    with pytest.raises(RuntimeError, match="unknown source"):
        _decode(_payload((header, fragments, recipes, malformed)))


@pytest.mark.parametrize("payload", [None, "not-bytes", b"", b"wrong-magic"])
def test_v6_refuses_invalid_payload_type_or_magic(payload):
    with pytest.raises(RuntimeError, match="payload framing|payload magic"):
        _decode(payload)


def test_v6_rejects_payload_and_header_storage_bounds_before_parsing(monkeypatch):
    payload = _payload(_parts())
    monkeypatch.setattr(decode, "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES", len(payload) - 1)
    with pytest.raises(decode.WitnessPayloadLimitError, match="payload bytes"):
        _decode(payload)
    monkeypatch.setattr(decode, "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES", len(payload))
    monkeypatch.setattr(decode, "PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES", 1)
    with pytest.raises(decode.WitnessPayloadLimitError, match="header"):
        _decode(payload)


def test_v6_refuses_invalid_zlib_and_missing_record_recipe():
    header, fragments, recipes, records = _parts()
    digest, length, _ = fragments[0]
    fragments[0] = (digest, length, b"not-zlib")
    with pytest.raises(RuntimeError, match="zlib framing"):
        _decode(_payload((header, fragments, recipes, records)))
    header, fragments, recipes, records = _parts()
    compressed = _record(kind="rate_occurrence", priority=0, item_ordinal=0, raw_json=b'{"unknown":true}')
    externalized, _ = codec.externalize_source_evidence_record(compressed, SOURCE_A)
    records[1] = (SOURCE_A, externalized)
    with pytest.raises(RuntimeError, match="recipe is missing"):
        _decode(_payload((header, fragments, recipes, records)))


def test_v6_refuses_empty_source_authority_before_header_acceptance():
    with pytest.raises(RuntimeError, match="source set"):
        decode.decode_fragment_source_witness(_payload(_parts()), expected_raw_source_sha256=[])


def test_v6_propagates_cancellation_without_accepting_a_view(monkeypatch):
    def cancel(*_args):
        raise asyncio.CancelledError()

    monkeypatch.setattr(decode, "_token", cancel)
    with pytest.raises(asyncio.CancelledError):
        _decode(_payload(_parts()))


def _semantic_record_bytes(record):
    metadata = _record_metadata_by_field(
        kind=record.kind,
        priority=record.priority,
        item_ordinal=record.coordinate[1],
        raw_json=record.raw_json,
        linked_provider_json=record.linked_provider_json,
    )
    metadata.update(
        tie_breaker=record.tie_breaker,
        coordinate=dict(
            zip(("object_ordinal", "rate_ordinal", "price_ordinal", "provider_ordinal"), record.coordinate)
        ),
        procedure=record.procedure,
        provider_evidence=record.provider_evidence,
        expected=record.expected,
    )
    encoded = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()
    linked = record.linked_provider_json or b""
    return zlib.compress(
        codec.SOURCE_RECORD_MAGIC
        + U32.pack(len(encoded))
        + encoded
        + U32.pack(len(record.raw_json))
        + record.raw_json
        + U32.pack(len(linked))
        + linked
    )


def _semantic_view():
    original = _occurrence_record()
    records = (
        replace(_provider_record(), raw_source_sha256=SOURCE_B),
        original,
        _occurrence_record(1),
        replace(original, priority=2, tie_breaker="ff" * 32, coordinate=(7, 2, 1, 0)),
    )
    candidates = [
        CompressedSourceWitnessRecord(
            record.kind, record.priority, record.tie_breaker, record.raw_source_sha256, _semantic_record_bytes(record)
        )
        for record in records
    ]
    counts = SourceWitnessPayloadCounts(2, source_set_digest([SOURCE_A, SOURCE_B]), 3, 1, 3, 0, 3, 1, 4)
    witness_payload, metadata = encode_fragment_source_witness_candidates(candidates, counts)
    return _decode(witness_payload, expected_metadata=metadata), records


def _audit_result(record, parsed_evidence):
    if record.kind == "provider_reference":
        audit_evidence.validate_provider_witness(record, parsed_evidence_by_sha256=parsed_evidence)
        return None
    return (
        audit_evidence.source_audit_condition(record, parsed_evidence_by_sha256=parsed_evidence),
        audit_evidence.source_challenge(record, parsed_evidence_by_sha256=parsed_evidence),
    )


def test_v6_grouped_mapping_preserves_record_order_and_real_audit_conditions():
    view, records = _semantic_view()
    callback_priorities = []

    def derive(record, parsed_evidence):
        callback_priorities.append(record.priority)
        expected = records[0 if record.kind == "provider_reference" else record.priority + 1]
        assert record.raw_json == expected.raw_json
        assert record.linked_provider_json == expected.linked_provider_json
        return _audit_result(record, parsed_evidence)

    results, counters = view.map_records(derive)
    assert results == tuple(_audit_result(record, None) for record in records)
    assert callback_priorities == [0, 0, 2, 1]
    assert counters["record_decodes"] == 4
    assert counters["evidence_reuse_deliveries"] == 2


def test_v6_grouped_mapping_counters_include_initialization_and_actual_repeats(monkeypatch):
    call_count_by_kind = {"fragment": 0, "token": 0, "json": 0, "bytes": 0}
    original_fragment, original_token, original_parse = decode._fragment_bytes, decode._token, decode._parsed_token

    def fragment(*args):
        call_count_by_kind["fragment"] += 1
        return original_fragment(*args)

    def token(*args):
        raw = original_token(*args)
        call_count_by_kind["token"] += 1
        call_count_by_kind["bytes"] += len(raw)
        return raw

    def parsed(raw):
        call_count_by_kind["json"] += 1
        return original_parse(raw)

    monkeypatch.setattr(decode, "_fragment_bytes", fragment)
    monkeypatch.setattr(decode, "_token", token)
    monkeypatch.setattr(decode, "_parsed_token", parsed)
    view = _decode(_payload(_parts()))
    _, counters = view.map_records(lambda record, _parsed: record.priority)
    assert counters["evidence_decompressions"] == call_count_by_kind["fragment"]
    assert counters["fragment_decompressions"] == call_count_by_kind["fragment"]
    assert counters["token_reconstructions"] == call_count_by_kind["token"]
    assert counters["evidence_json_parses"] == call_count_by_kind["json"]
    assert counters["reconstruction_bytes"] == call_count_by_kind["bytes"]
    assert (
        counters["decoded_evidence_bytes"]
        == call_count_by_kind["bytes"] + view.metadata["evidence_dictionary_raw_bytes"]
    )
    assert (
        counters["evidence_sha256_hashes"]
        == 2 * call_count_by_kind["fragment"] - view.metadata["fragment_count"] + call_count_by_kind["token"]
    )
    assert all(counters[field] > 0 for field in counters if field.startswith("repeated_"))
    assert view.map_records(lambda record, _parsed: record.priority)[1] == counters


def test_v6_grouped_mapping_work_guard_precedes_all_token_reads(monkeypatch):
    view, _ = _semantic_view()
    _, work = decode._record_groups(view.records, view.metadata["evidence_reconstructed_bytes"])
    monkeypatch.setattr(
        decode,
        "PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES",
        view.metadata["evidence_reconstructed_bytes"] + work["byte_count"] - 1,
    )
    monkeypatch.setattr(decode, "_token", lambda *_args: pytest.fail("mapping work bound must precede token reads"))
    with pytest.raises(decode.WitnessPayloadLimitError, match="grouped reconstruction work"):
        view.map_records(lambda *_args: pytest.fail("mapping work bound must precede callbacks"))


def test_v6_grouped_mapping_releases_previous_pair_maps_and_raw_records():
    view, _ = _semantic_view()
    evidence_views, record_refs = [], []

    def compact(record, parsed_evidence):
        if evidence_views and parsed_evidence is not evidence_views[-1]:
            assert not evidence_views[-1]
        assert all(reference() is None for reference in record_refs)
        assert len(parsed_evidence) <= 2
        evidence_views.append(parsed_evidence)
        record_refs.append(weakref.ref(record))
        return record.priority

    assert view.map_records(compact)[0] == (0, 0, 1, 2)
    assert all(not evidence for evidence in evidence_views)
    assert all(reference() is None for reference in record_refs)


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
def test_v6_grouped_mapping_clears_current_evidence_and_propagates_failure(monkeypatch, failure):
    view, _ = _semantic_view()
    raw_maps, parsed_maps = [], []
    original = decode.decode_persisted_record

    def decode_record(*args, **kwargs):
        raw_maps.append(kwargs["evidence_by_sha256"])
        return original(*args, **kwargs)

    def fail(_record, parsed_evidence):
        parsed_maps.append(parsed_evidence)
        raise failure("stopped mapping")

    monkeypatch.setattr(decode, "decode_persisted_record", decode_record)
    with pytest.raises(failure, match="stopped mapping"):
        view.map_records(fail)
    assert len(raw_maps) == len(parsed_maps) == 1
    assert not raw_maps[0] and not parsed_maps[0]
