# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Differential payload decoding with unchanged structural and canonical limits."""

import json
import sys
from collections.abc import Mapping

import pytest

from process.custom_import import definition
from process.custom_import import runner_codec as codec
from process.custom_import.runner_types import CandidateRunnerError


def _reference_parse(canonical_payload, label):
    """Retain the copying decoder as an independent behavior oracle."""
    if not isinstance(canonical_payload, str):
        raise CandidateRunnerError(f"{label} is not canonical text")
    try:
        parsed_payload = json.loads(canonical_payload)
        if definition.canonical_json(parsed_payload) != canonical_payload:
            raise ValueError("not canonical")
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise CandidateRunnerError(f"{label} is malformed") from exc
    if not isinstance(parsed_payload, Mapping) or parsed_payload.get("contract") != "custom-import-record/v1":
        raise CandidateRunnerError(f"{label} has an unknown contract")
    return parsed_payload


def _reference_values(fields, canonical_payload, label):
    parsed = _reference_parse(canonical_payload, label)
    encoded_fields = parsed.get("fields")
    if not isinstance(encoded_fields, list) or len(encoded_fields) != len(fields):
        raise CandidateRunnerError(f"{label} fields do not match the definition")
    values_by_field = {}
    for field, encoded_field in zip(fields, encoded_fields, strict=True):
        value = codec.payload_field_value(field, encoded_field, label)
        if value is not codec._MISSING:
            values_by_field[field.field_id] = value
    return values_by_field


def _outcome(operation):
    try:
        value = operation()
    except Exception as exc:
        cause = exc.__cause__
        return "error", type(exc), str(exc), type(cause), str(cause)
    return "value", type(value), value


def _compact(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _nested_list(depth):
    nested_values = 0
    for _ in range(depth):
        nested_values = [nested_values]
    return nested_values


def _parser_corpus():
    document_dict = {"contract": "custom-import-record/v1", "fields": []}
    text = _compact(document_dict)
    payload_cases = [
        None,
        1,
        True,
        b"{}",
        "",
        "not-json",
        "null",
        "[]",
        "{}",
        '{"contract":"other"}',
        text,
        text + " ",
        " " + text,
        '{"fields":[],"contract":"custom-import-record/v1"}',
        '{"contract":"custom-import-record/v1","contract":"custom-import-record/v1","fields":[]}',
        '{"contract":"custom-import-record/v1","fields":[],"fields":[]}',
        '{"contract":"custom-import-record/v1","fields":[],"unused":{"x":1,"x":1}}',
        '{"contract":"custom-import-record/v1","fields":[],"unused":-0}',
        '{"contract":"custom-import-record/v1","fields":[],"unused":01}',
        "[" * 2_000 + "0" + "]" * 2_000,
    ]
    for extra in (
        None,
        False,
        0,
        9_007_199_254_740_993,
        'Δ😀\x00\n\\"',
        "\ud800",
        [None, True, {"text": "synthetic"}],
        1.0,
        float("nan"),
        float("inf"),
        float("-inf"),
        _nested_list(definition.MAX_DEFINITION_DEPTH - 1),
        _nested_list(definition.MAX_DEFINITION_DEPTH),
        [0] * (definition.MAX_DEFINITION_NODES - 4),
        [0] * (definition.MAX_DEFINITION_NODES - 3),
    ):
        payload_cases.append(_compact({**document_dict, "unused": extra}))
    payload_cases.append(text.replace("custom-import", "custom\\u002dimport"))
    limit = sys.get_int_max_str_digits()
    if limit:
        payload_cases.append('{"contract":"custom-import-record/v1","fields":[],"unused":' + "1" * (limit + 1) + "}")
    return payload_cases


@pytest.mark.parametrize("canonical_payload", _parser_corpus())
def test_loaded_payload_parser_matches_copying_decoder(canonical_payload):
    label = "synthetic payload"
    assert _outcome(lambda: codec.parse_canonical_payload(canonical_payload, label)) == _outcome(
        lambda: _reference_parse(canonical_payload, label)
    )


@pytest.mark.parametrize("label", ("build child payload", "frozen scalar payload", "winner root payload"))
def test_payload_parser_preserves_error_labels(label):
    for value in (None, "not-json", '{"contract":"other"}'):
        assert _outcome(lambda: codec.parse_canonical_payload(value, label)) == _outcome(
            lambda: _reference_parse(value, label)
        )


@pytest.mark.parametrize(
    ("kind", "value"),
    (
        ("string", 'Δ😀\x00\n\\"'),
        ("integer", 9_007_199_254_740_993),
        ("integer", -(10**100)),
        ("decimal", "-0.00"),
        ("decimal", "12.500"),
        ("boolean", True),
        ("date", "2026-10-09"),
        ("timestamp", "2026-10-09T12:34:56.123456+02:00"),
    ),
)
@pytest.mark.parametrize("state", ("value", "null", "missing"))
def test_payload_values_preserve_all_types_missing_null_and_extras(kind, value, state):
    field = definition.Field("synthetic", 1, kind, True, None, None)
    encoded_value_dict = {"state": state, "type": kind, "value": value, "unused": ["extra"]}
    payload = _compact(
        {
            "contract": "custom-import-record/v1",
            "fields": [{"field": field.field_id, "value": encoded_value_dict, "unused": None}],
            "unused": {"text": "extra"},
        }
    )
    label = "synthetic payload"
    assert _outcome(lambda: codec.payload_values((field,), payload, label=label)) == _outcome(
        lambda: _reference_values((field,), payload, label)
    )


@pytest.mark.parametrize(
    "encoded",
    (
        None,
        {},
        {"field": "other", "value": {}},
        {"field": "synthetic", "value": 1},
        {"field": "synthetic", "value": {"state": "missing"}},
        {"field": "synthetic", "value": {"state": "null", "type": "integer"}},
        {"field": "synthetic", "value": {"state": "value", "type": "string", "value": "1"}},
        {"field": "synthetic", "value": {"state": "value", "type": "integer", "value": True}},
        {"field": "synthetic", "value": {"state": "value", "type": "integer", "value": 1.5}},
        {"field": "synthetic", "value": {"state": "value", "type": "integer"}},
    ),
)
def test_payload_values_preserve_malformed_field_errors(encoded):
    field = definition.Field("synthetic", 1, "integer", False, None, None)
    payload = _compact({"contract": "custom-import-record/v1", "fields": [encoded]})
    label = "synthetic payload"
    assert _outcome(lambda: codec.payload_values((field,), payload, label=label)) == _outcome(
        lambda: _reference_values((field,), payload, label)
    )


def test_loaded_validation_reuses_tree_but_default_still_copies():
    input_dict = {"items": [1, {"text": "synthetic"}]}
    untouched = _compact(input_dict)
    assert definition._validate_wire_value(input_dict, copy_containers=False) is input_dict
    copied = definition._validate_wire_value(input_dict)
    assert copied == input_dict and copied is not input_dict
    assert copied["items"] is not input_dict["items"]
    assert copied["items"][1] is not input_dict["items"][1]
    assert definition.canonical_json(input_dict) == untouched == _compact(input_dict)
