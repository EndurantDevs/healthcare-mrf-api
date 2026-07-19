from decimal import Decimal

import orjson
import pytest

from api.endpoint.pricing import _json_response
from api.ptg2_response import (
    _canonical_price_row,
    _response_wire_value,
    _shape_ptg2_response,
)


def _serialized_rate(value):
    row = _canonical_price_row({"negotiated_rate": value})
    payload = _shape_ptg2_response({"items": [{"prices": [row]}]}, {})
    return orjson.dumps(payload["items"][0]["prices"][0], default=str)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("123.4567890123456789", b'{"negotiated_rate":123.4567890123456789}'),
        ("-123.4567890123456789", b'{"negotiated_rate":-123.4567890123456789}'),
        (Decimal("1.25E+25"), b'{"negotiated_rate":12500000000000000000000000}'),
        (Decimal("-1.25E-20"), b'{"negotiated_rate":-0.0000000000000000000125}'),
        ((1 << 64), b'{"negotiated_rate":18446744073709551616}'),
        (-(1 << 63) - 1, b'{"negotiated_rate":-9223372036854775809}'),
        ("12.34", b'{"negotiated_rate":12.34}'),
        ("-12.34", b'{"negotiated_rate":-12.34}'),
        ("42", b'{"negotiated_rate":42}'),
        ("-42", b'{"negotiated_rate":-42}'),
        (None, b'{"negotiated_rate":null}'),
    ],
)
def test_negotiated_rate_serializes_as_an_exact_json_number(value, expected):
    assert _serialized_rate(value) == expected


@pytest.mark.parametrize("value", ["not-a-number", '0,"fabricated":1', "NaN", ""])
def test_invalid_negotiated_rate_text_remains_json_text(value):
    body = _serialized_rate(value)

    assert orjson.loads(body) == {"negotiated_rate": value}


def test_large_valid_number_is_not_silently_changed_to_json_text():
    value = "1" + ("0" * 5000)

    assert _serialized_rate(value) == b'{"negotiated_rate":' + value.encode() + b"}"


def test_pricing_http_serializer_preserves_exact_numeric_fragment():
    row = _canonical_price_row({"negotiated_rate": "123.4567890123456789"})
    payload = _shape_ptg2_response({"items": [{"prices": [row]}]}, {})

    response = _json_response(payload)

    assert b'"negotiated_rate":123.4567890123456789' in response.body


def test_response_wire_value_restores_exact_rate_as_decimal():
    row = _canonical_price_row(
        {"negotiated_rate": "123.4567890123456789"}
    )

    assert _response_wire_value(row) == {
        "negotiated_rate": Decimal("123.4567890123456789")
    }


def test_source_identity_fields_are_opt_in():
    source_field_map = {
        "source_key": "synthetic-source",
        "source_artifact_key": 1,
        "source_type": "in_network",
        "identity_kind": "raw_container_sha256_v1",
        "identity_sha256": "a" * 64,
        "raw_container_sha256": "a" * 64,
        "logical_json_sha256": None,
        "logical_hash_deferred": True,
        "source_trace_set_hash": "b" * 64,
        "source_trace": [{"source_file_version_id": "synthetic"}],
    }

    hidden = _shape_ptg2_response({"items": [source_field_map]}, {})
    included = _shape_ptg2_response(
        {"items": [source_field_map]},
        {"include_sources": "true"},
    )

    assert hidden["items"] == [{}]
    assert included["items"] == [source_field_map]
