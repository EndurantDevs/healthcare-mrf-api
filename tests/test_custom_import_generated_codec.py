# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Generated records retain the generic canonical codec's exact contract."""

from __future__ import annotations

import datetime as dt
from dataclasses import replace
from decimal import Decimal

import pytest

from process.custom_import import definition
from process.custom_import import runner_codec as codec
from process.custom_import.definition import Field
from tests.test_custom_import_runner_scale import _child_document, _definition, _family


def _field(value_type="string", field_id="value"):
    return Field(field_id, 1, value_type, True, None, None)


def _outcome(function):
    try:
        return "value", function()
    except Exception as exc:
        return "error", type(exc), str(exc), type(exc.__cause__)


def _assert_generic_parity(monkeypatch, fields, values):
    operations = (
        lambda: codec.record_payload(fields, values),
        lambda: codec.digest_text("root-payload", codec.record_payload(fields, values)),
        lambda: codec.key_document(tuple(values), {field.field_id: field for field in fields}, values),
    )
    actual_outcomes = tuple(_outcome(operation) for operation in operations)
    with monkeypatch.context() as generic:
        generic.setattr(
            codec,
            "_canonical_fields",
            lambda contract, encoded: codec.canonical({"contract": contract, "fields": encoded}),
        )
        expected_outcomes = tuple(_outcome(operation) for operation in operations)
    assert actual_outcomes == expected_outcomes


@pytest.mark.parametrize(
    "value_type,value",
    [
        ("string", 'Unicode \u00e9\U0001f642, quote " and slash \\, newline\n, NUL\x00'),
        ("string", "\ud800"),
        ("string", "\udc00"),
        ("string", b"bytes"),
        ("integer", 0),
        ("integer", -(2**80)),
        ("integer", 10**4_500),
        ("integer", True),
        ("integer", 1.0),
        ("boolean", True),
        ("boolean", 1),
        ("decimal", Decimal("-0.000")),
        ("decimal", "-0.00"),
        ("decimal", Decimal("123.4500")),
        ("decimal", Decimal("NaN")),
        ("decimal", "1e3"),
        ("date", dt.date(2024, 2, 29)),
        ("date", dt.datetime(2024, 2, 29)),
        ("timestamp", dt.datetime(2024, 2, 29, 23, 4, 5, 123456, tzinfo=dt.timezone(dt.timedelta(hours=7)))),
        ("timestamp", dt.datetime(2024, 2, 29)),
        ("timestamp", dt.datetime(1, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=1)))),
        ("unrecognized", {"nested": [None, 1, "text"]}),
        ("unrecognized", {"floating": 0.5}),
        ("unrecognized", object()),
    ],
    ids=lambda value: type(value).__name__,
)
def test_generated_scalar_bytes_and_failures_match_generic(monkeypatch, value_type, value):
    _assert_generic_parity(monkeypatch, (_field(value_type),), {"value": value})


@pytest.mark.parametrize("value_type", ["string", "integer", "decimal", "boolean", "date", "timestamp"])
@pytest.mark.parametrize("nullable", [True, False])
@pytest.mark.parametrize("values", [{}, {"value": None}])
def test_generated_missing_and_null_keep_original_contract(monkeypatch, value_type, nullable, values):
    _assert_generic_parity(monkeypatch, (replace(_field(value_type), nullable=nullable),), values)


def test_generated_order_and_empty_records_match_generic(monkeypatch):
    fields = (_field("integer", "second"), _field("string", "first"))
    _assert_generic_parity(monkeypatch, fields, {"first": "text", "second": 3})
    _assert_generic_parity(monkeypatch, (), {})


@pytest.mark.parametrize("extra_node", [False, True])
def test_generated_node_boundary_matches_generic(monkeypatch, extra_node):
    fields = (_field("integer"),) * 1_664 + (
        _field("string", "missing_a"),
        _field("string", "missing_b"),
        _field("string", "null"),
    )
    values_by_field = {"value": 1, "null": None}
    if extra_node:
        values_by_field["missing_a"] = None
    _assert_generic_parity(monkeypatch, fields, values_by_field)
    outcome = _outcome(lambda: codec.record_payload(fields, values_by_field))
    assert outcome[0] == ("error" if extra_node else "value")


@pytest.mark.parametrize("depth", [28, 29])
def test_non_scalar_depth_boundary_keeps_generic_fallback(monkeypatch, depth):
    nested_values = "leaf"
    for _ in range(depth):
        nested_values = [nested_values]
    _assert_generic_parity(monkeypatch, (_field("unrecognized"),), {"value": nested_values})
    outcome = _outcome(lambda: codec.record_payload((_field("unrecognized"),), {"value": nested_values}))
    assert outcome[0] == ("value" if depth == 28 else "error")


def test_generated_depth_limit_is_not_bypassed(monkeypatch):
    monkeypatch.setattr(definition, "MAX_DEFINITION_DEPTH", 3)
    monkeypatch.setattr(codec, "MAX_DEFINITION_DEPTH", 3)
    _assert_generic_parity(monkeypatch, (_field(),), {"value": "text"})
    assert _outcome(lambda: codec.record_payload((_field(),), {"value": "text"}))[0] == "error"


def test_plain_generated_fields_avoid_generic_container_copy(monkeypatch):
    def unexpected_generic(_document):
        raise AssertionError("plain generated fields do not require a second container traversal")

    monkeypatch.setattr(codec, "canonical", unexpected_generic)
    assert '"value":"text"' in codec.record_payload((_field(),), {"value": "text"})
    assert '"value":"text"' in codec.key_document(("value",), {"value": _field()}, {"value": "text"})


def test_non_native_leaves_fall_back_before_serialization(monkeypatch):
    class CustomString(str):
        pass

    class CustomInteger(int):
        pass

    for field, value in ((_field(), CustomString("text")), (_field("integer"), CustomInteger(3))):
        _assert_generic_parity(monkeypatch, (field,), {"value": value})
    calls = []
    generic_canonical = codec.canonical

    def checked_generic(document):
        calls.append(document)
        return generic_canonical(document)

    monkeypatch.setattr(codec, "canonical", checked_generic)
    codec.record_payload((_field(),), {"value": CustomString("text")})
    assert len(calls) == 1


def _ordered_family_digest(schema, family, corrupt):
    documents = sorted(_child_document(schema, child) for child in family.children["rates"])
    if corrupt:
        key_hash, key, payload = documents[0]
        documents[0] = key_hash, key, payload + " "
    return codec.new_family_hash_ordered(schema, family.root, {"rates": iter(documents)})


@pytest.mark.parametrize("corrupt", [False, True])
def test_generated_family_digest_and_rejection_match_generic(monkeypatch, corrupt):
    schema = _definition()
    children = (
        {"rate_npi": "1234567893", "service_code": 'A\n"\u03b4', "amount": Decimal("-0.00")},
        {"rate_npi": "1234567893", "service_code": "B", "amount": None},
        {"rate_npi": "1234567893", "service_code": "C"},
    )
    family = _family(children + children[:1])
    actual = _outcome(lambda: _ordered_family_digest(schema, family, corrupt))
    with monkeypatch.context() as generic:
        generic.setattr(
            codec,
            "_canonical_fields",
            lambda contract, encoded: codec.canonical({"contract": contract, "fields": encoded}),
        )
        expected = _outcome(lambda: _ordered_family_digest(schema, family, corrupt))
        if not corrupt:
            assert expected == ("value", codec.new_family_hash(schema, family))
    assert actual == expected
    assert actual[0] == ("error" if corrupt else "value")
