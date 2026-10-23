# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lossless tagged-source replay, bounded parts, and corruption refusals."""

from __future__ import annotations

import gzip
import json
from dataclasses import replace
from decimal import Decimal
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from process.custom_import import capture
from process.custom_import import prepared_source_codec as codec
from process.custom_import.build_source import _prepare_row
from process.custom_import.capture import CaptureLimits, DecodedRecord
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_capture import _capture, _stream
from tests.test_custom_import_snowflake_bundle import _definition as _integer_definition

_LIMITS = CaptureLimits(maximum_record_bytes=256, maximum_decoded_bytes=4096, maximum_fields_per_record=16)


def _parts(records, *, limits=_LIMITS, row_limit=2, byte_limit=None):
    return codec.iter_prepared_source_parts(
        records,
        limits=limits,
        part_byte_limit=byte_limit or codec.prepared_source_row_byte_limit(limits),
        part_row_limit=row_limit,
    )


def _records(part, *, limits=_LIMITS, row_limit=2, byte_limit=None, binary=None):
    return codec.iter_prepared_source_records(
        binary or BytesIO(part.payload),
        limits=limits,
        part_byte_limit=byte_limit or codec.prepared_source_row_byte_limit(limits),
        part_row_limit=row_limit,
        first_source_ordinal=part.first_source_ordinal,
        record_count=part.record_count,
    )


def _signature(records):
    return [
        (
            record.ordinal,
            [
                (label, type(value), value.as_tuple() if isinstance(value, Decimal) else value)
                for label, value in record.values.items()
            ],
        )
        for record in records
    ]


def test_exact_scalar_roundtrip_preserves_sparse_order_and_tagged_bytes():
    records = [
        DecodedRecord(1, {"extra": None, "é": 'line\n\x00"\\🙂', "flag": False, "large": -(2**100)}),
        DecodedRecord(2, {"exponent": Decimal("1.2300E+7"), "negative": Decimal("-0.000"), "zero": 0}),
        DecodedRecord(3, {"exponent": Decimal("-0E+12")}),
    ]
    parts = list(_parts(iter(records)))
    assert (
        parts[0].payload.splitlines()[0]
        == (
            '[1,[["extra","null",null],["é","string","line\\n\\u0000\\"\\\\🙂"],'
            '["flag","boolean",false],["large","integer","-1267650600228229401496703205376"]]]'
        ).encode()
    )
    assert b'["exponent","decimal","1.2300E+7"]' in parts[0].payload
    assert b'["negative","decimal","-0.000"]' in parts[0].payload
    replay_records = [record for part in parts for record in _records(part)]
    assert _signature(replay_records) == _signature(records)
    assert "extra" not in replay_records[1].values


def test_encoder_does_not_repeat_original_scalar_validation(monkeypatch):
    def unexpected_validation(*_args):
        raise AssertionError("trusted decoder values must be tagged only once")

    monkeypatch.setattr(codec, "_decoded_record", unexpected_validation)
    assert list(_parts([DecodedRecord(1, {"value": Decimal("1.00")})]))[0].record_count == 1


@pytest.mark.parametrize("score", ["2", True, Decimal("2"), None, "missing"])
def test_supported_wrong_types_and_missing_null_keep_family_rejection(score):
    request = _request(definition=_integer_definition())
    values_by_label = {"npi": "1003000126", "enabled": True, "extra": "retained"}
    if score != "missing":
        values_by_label["score"] = score
    record = DecodedRecord(1, values_by_label)
    replay = list(_records(next(_parts([record]))))[0]
    assert _signature([replay]) == _signature([record])
    stream = request.definition.source_streams[0]
    original = _prepare_row(request, stream, record.values)
    prepared = _prepare_row(request, stream, replay.values)
    assert prepared.rejection is not None
    assert prepared.rejection.code == original.rejection.code
    assert prepared.rejection.canonical_evidence == original.rejection.canonical_evidence
    assert (prepared.raw_key, prepared.typed_key, prepared.byte_count) == (
        original.raw_key,
        original.typed_key,
        original.byte_count,
    )


def test_parts_preserve_global_ordinals_and_late_part_needs_no_prefix():
    consumed_ordinals = []

    def source_records():
        for ordinal in range(1, 6):
            consumed_ordinals.append(ordinal)
            yield DecodedRecord(ordinal, {"value": ordinal})

    parts = _parts(source_records())
    first = next(parts)
    assert consumed_ordinals == [1, 2]
    assert (first.first_source_ordinal, first.record_count) == (1, 2)
    remaining_parts = list(parts)
    assert [(part.first_source_ordinal, part.record_count) for part in remaining_parts] == [(3, 2), (5, 1)]
    assert [record.ordinal for record in _records(remaining_parts[-1])] == [5]


def test_empty_stream_is_one_zero_payload_part_without_extra_terminal_parts():
    empty_parts = list(_parts(iter(())))
    assert empty_parts == [codec.PreparedSourcePart(1, 0, b"")]
    assert list(_records(empty_parts[0])) == []
    assert len(list(_parts([DecodedRecord(1, {}), DecodedRecord(2, {})]))) == 1


def test_byte_boundary_includes_lf_and_does_not_split_rows():
    limits = replace(_LIMITS, maximum_record_bytes=128, maximum_fields_per_record=4)
    records = [DecodedRecord(ordinal, {"value": "x" * 110}) for ordinal in range(1, 21)]
    byte_limit = codec.prepared_source_row_byte_limit(limits)
    parts = list(_parts(records, limits=limits, row_limit=100, byte_limit=byte_limit))
    assert len(parts) > 1
    assert all(part.payload.endswith(b"\n") and len(part.payload) <= byte_limit for part in parts)
    assert _signature(record for part in parts for record in _records(part, limits=limits, row_limit=100)) == (
        _signature(records)
    )


@pytest.mark.parametrize(
    "payload",
    [
        b'[1,[["value","integer","+1"]]]\n',
        b'[1,[["value","integer","01"]]]\n',
        b'[1,[["value","integer","-0"]]]\n',
        b'[1,[["value","integer",1]]]\n',
        b'[1,[["value","decimal","1e3"]]]\n',
        b'[1,[["value","decimal","1.0 "]]]\n',
        b'[1,[["value","boolean",1]]]\n',
        b'[1,[["value","null",false]]]\n',
        b'[1,[["value","string",null]]]\n',
        b'[1,[["value","float","1.0"]]]\n',
        b'[1,[["value","string",[]]]]\n',
        b'[1,[["value","string","one"],["value","string","two"]]]\n',
        b'[1,[["value","string"]]]\n',
        b'[1,[{"value":"synthetic"}]]\n',
        b'[1,[["","string","synthetic"]]]\n',
        b'[1,[["value","string","\\ud800"]]]\n',
        b'[1,[["value","string","\xff"]]]\n',
        b"[1, []]\n",
        b"[1,[]]\r\n",
        b'[1,[["value","string","\\u00e9"]]]\n',
        b'{"ordinal":1,"entries":[]}\n',
        b"[1,[]]trailing\n",
        b"\n",
    ],
)
def test_malformed_wire_grammar_is_redacted(payload):
    with pytest.raises(codec.PreparedSourceCodecError, match="^prepared_source_row_invalid$") as caught:
        list(_records(codec.PreparedSourcePart(1, 1, payload)))
    assert caught.value.__context__ is None or caught.value.__suppress_context__
    assert "synthetic" not in str(caught.value)


@pytest.mark.parametrize("ordinal", [0, 2, True, 1.0, "1"])
def test_decode_rejects_ordinal_gaps_and_noninteger_ordinals(ordinal):
    payload = json.dumps([ordinal, []], separators=(",", ":")).encode() + b"\n"
    with pytest.raises(codec.PreparedSourceCodecError, match="^prepared_source_ordinal_invalid$"):
        list(_records(codec.PreparedSourcePart(1, 1, payload)))


@pytest.mark.parametrize("ordinal", [0, 2, True, 1.0])
def test_encoder_rejects_ordinal_gaps(ordinal):
    with pytest.raises(codec.PreparedSourceCodecError, match="^prepared_source_ordinal_invalid$"):
        list(_parts([DecodedRecord(ordinal, {})]))


@pytest.mark.parametrize("value", [1.0, [], {}, b"synthetic"])
def test_encoder_does_not_widen_scalar_domain(value):
    with pytest.raises(codec.PreparedSourceCodecError, match="^prepared_source_row_invalid$"):
        list(_parts([DecodedRecord(1, {"value": value})]))


@pytest.mark.parametrize(
    "part,error",
    [
        (codec.PreparedSourcePart(1, 1, b"[1,[]]"), "framing_invalid"),
        (codec.PreparedSourcePart(1, 1, b""), "record_count_invalid"),
        (codec.PreparedSourcePart(1, 0, b"[1,[]]\n"), "record_count_invalid"),
        (codec.PreparedSourcePart(1, 1, b"[1,[]]\n[2,[]]\n"), "record_count_invalid"),
        (codec.PreparedSourcePart(2, 0, b""), "metadata_invalid"),
        (codec.PreparedSourcePart(1, 3, b""), "metadata_invalid"),
    ],
)
def test_part_framing_and_exact_coverage(part, error):
    with pytest.raises(codec.PreparedSourceCodecError, match=f"^prepared_source_part_{error}$"):
        list(_records(part))


def test_decode_bounds_read_before_json_parse(monkeypatch):
    read_sizes = []
    row_byte_limit = codec.prepared_source_row_byte_limit(_LIMITS)

    class BoundedInput(BytesIO):
        def readline(self, size=-1):
            read_sizes.append(size)
            return super().readline(size)

    def unexpected_json(*_args):
        raise AssertionError("oversized rows must fail before parsing")

    monkeypatch.setattr(codec.json, "loads", unexpected_json)
    binary = BoundedInput(b"x" * (row_byte_limit + 100))
    with pytest.raises(codec.PreparedSourceCodecError, match="byte_limit_exceeded"):
        list(_records(codec.PreparedSourcePart(1, 1, b""), binary=binary))
    assert read_sizes == [row_byte_limit + 1]
    assert binary.tell() == row_byte_limit + 1


@pytest.mark.parametrize(
    "payload",
    [
        b"[1,[" + b",".join([b"[]"] * 17) + b"]]\n",
        b"[1,[[[]]]]\n",
        b'[1,[["label","string",' + b"0," * 50 + b"0]]]\n",
        b'[1,[["label","string",{}]]]\n',
    ],
)
def test_decode_bounds_structure_before_json_allocation(monkeypatch, payload):
    def unexpected_json(*_args):
        raise AssertionError("excess structure must fail before parsing")

    monkeypatch.setattr(codec.json, "loads", unexpected_json)
    with pytest.raises(codec.PreparedSourceCodecError, match="^prepared_source_row_invalid$"):
        list(_records(codec.PreparedSourcePart(1, 1, payload)))


def test_decode_enforces_total_part_bytes_before_parsing_the_overrun(monkeypatch):
    rows = [
        json.dumps([ordinal, [["value", "string", "x" * 240]]], separators=(",", ":")).encode() + b"\n"
        for ordinal in range(1, 12)
    ]
    parsed_documents = []
    original_loads = codec.json.loads

    def counted_loads(document):
        parsed_documents.append(document)
        return original_loads(document)

    monkeypatch.setattr(codec.json, "loads", counted_loads)
    with pytest.raises(codec.PreparedSourceCodecError, match="part_byte_limit_exceeded"):
        list(_records(codec.PreparedSourcePart(1, len(rows), b"".join(rows)), row_limit=20))
    assert len(parsed_documents) == codec.prepared_source_row_byte_limit(_LIMITS) // len(rows[0])


def test_encoder_enforces_fields_and_encoded_envelope():
    limits = replace(_LIMITS, maximum_fields_per_record=1)
    with pytest.raises(codec.PreparedSourceCodecError, match="row_invalid"):
        list(_parts([DecodedRecord(1, {"a": None, "b": None})], limits=limits))
    oversized = "x" * codec.prepared_source_row_byte_limit(_LIMITS)
    with pytest.raises(codec.PreparedSourceCodecError, match="row_byte_limit_exceeded"):
        list(_parts([DecodedRecord(1, {"value": oversized})]))


def test_original_field_record_and_count_limits_remain_in_force():
    for limits, payload in (
        (replace(_LIMITS, maximum_fields_per_record=1), b'[1,[["a","null",null],["b","null",null]]]\n'),
        (replace(_LIMITS, maximum_record_bytes=8), b'[1,[["value","string","long"]]]\n'),
    ):
        with pytest.raises(codec.PreparedSourceCodecError, match="row_invalid"):
            list(_records(codec.PreparedSourcePart(1, 1, payload), limits=limits))
    with pytest.raises(codec.PreparedSourceCodecError, match="row_invalid"):
        list(_parts([DecodedRecord(1, {}), DecodedRecord(2, {})], limits=replace(_LIMITS, maximum_records=1)))
    with pytest.raises(codec.PreparedSourceCodecError, match="metadata_invalid"):
        list(_records(codec.PreparedSourcePart(2, 1, b""), limits=replace(_LIMITS, maximum_records=1)))


@pytest.mark.parametrize("byte_limit,row_limit", [(1, 1), (64 * 1024 * 1024 + 1, 1), (4096, 0), (4096, True)])
def test_invalid_part_budgets(byte_limit, row_limit):
    with pytest.raises(codec.PreparedSourceCodecError, match="part_limits_invalid"):
        list(codec.iter_prepared_source_parts([], limits=_LIMITS, part_byte_limit=byte_limit, part_row_limit=row_limit))


def _format_payload(format_name):
    if format_name == "parquet":
        binary = BytesIO()
        pq.write_table(pa.table({"id": [1, 2], "extra": [Decimal("-0.00"), Decimal("1.20")]}), binary)
        return binary.getvalue()
    return {
        "csv": "id,extra\n1,é\n2,other\n",
        "tsv": "id\textra\n1\té\n2\tother\n",
        "json": '[{"id":1,"extra":null},{"id":2,"extra":1.20}]',
        "ndjson": '{"id":1,"extra":null}\n{"id":2,"extra":1.20}\n',
        "xml": "<rows><row><id>1</id><extra>é</extra></row><row><id>2</id></row></rows>",
    }[format_name].encode()


@pytest.mark.parametrize("format_name", ["csv", "tsv", "json", "ndjson", "xml", "parquet"])
@pytest.mark.parametrize("compression", ["none", "gzip"])
def test_standalone_decoder_maps_roundtrip_without_transport_coercion(format_name, compression):
    stream = _stream(
        format_name=format_name, compression=compression, record_path="row" if format_name == "xml" else None
    )
    payload = _format_payload(format_name)
    if compression == "gzip":
        payload = gzip.compress(payload)
    sealed = _capture(payload, stream)
    original_records = list(capture.iter_records(sealed, stream))
    replay_records = [record for part in _parts(iter(original_records)) for record in _records(part)]
    assert _signature(replay_records) == _signature(original_records)
