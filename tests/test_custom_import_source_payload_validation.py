# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Differential payload validation and ordered landing-boundary failures."""

import importlib
import inspect
import json
import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.custom_import import bulk_page_codec as codec
from process.custom_import import runner_codec
from process.custom_import import source_payload_validation as validation
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_source import _root
from tests.test_custom_import_build_source_bulk import _bulk_context, _prepared_page


def _fields(kind="string", nullable=True):
    return (SimpleNamespace(field_id="item", value_type=kind, nullable=nullable),)


def _document(kind="string", value="synthetic", **changes):
    encoded = {"state": "value", "type": kind, "value": value} | changes
    return {"contract": "custom-import-record/v1", "fields": [{"field": "item", "value": encoded}]}


def _text(document):
    return json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _serial_error(fields, payload):
    try:
        runner_codec.payload_values(fields, payload, label="landing source payload")
    except CandidateRunnerError as error:
        return str(error)
    return None


@pytest.fixture
def native(monkeypatch):
    module = pytest.importorskip("ptg2_address_canon")
    validator = getattr(module, "custom_import_validate_source_payloads_v1", None)
    assert inspect.isbuiltin(validator), "the installed extension must expose bounded payload validation"
    monkeypatch.setattr(validation, "native_validator", lambda: validator)
    return validator


@pytest.fixture
def validation_bridge(request, monkeypatch):
    """Exercise Python routing portably while keeping compiled parity separate."""
    if request.param == "native":
        return request.getfixturevalue("native")
    serial = runner_codec.payload_values

    def checked(layout, payloads):
        fields = tuple(
            SimpleNamespace(field_id=name, value_type=kind, nullable=nullable) for name, kind, nullable in layout
        )
        for index, payload in enumerate(payloads):
            try:
                serial(fields, payload, label="landing source payload")
            except CandidateRunnerError as error:
                return index, str(error).removeprefix("landing source payload ")
        return None

    monkeypatch.setattr(validation, "native_validator", lambda: checked)
    return checked


@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("kind", ["string", "integer", "decimal", "boolean"])
@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        False,
        0,
        -(2**63),
        2**130,
        "",
        'Ω\n\x00"\\😀',
        "-0.000",
        "001.2500",
        "-123456789012345678901234567890.123456789012345",
        "+1",
        "1e3",
        "1.",
        ".5",
        " 1",
        "１",
        [],
        {},
    ],
)
def test_native_scalar_outcomes_match_complete_serial_decoder(native, kind, nullable, value):
    fields = _fields(kind, nullable)
    payload = _text(_document(kind, value))
    outcome = native(validation._native_layout(fields), [payload])
    actual = None if outcome is None else f"landing source payload {outcome[1]}"
    assert actual == _serial_error(fields, payload)
    if outcome is not None:
        assert outcome[0] == 0


@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("state", ["missing", "null", "other", None, 1])
@pytest.mark.parametrize("kind", ["string", "decimal"])
def test_native_presence_keeps_nullable_and_ignored_member_semantics(native, nullable, state, kind):
    fields = _fields(kind, nullable)
    document = _document(kind, [], state=state, extra={"ignored": [True, None, 2**130]})
    if state == "missing":
        document["fields"][0]["value"]["type"] = "ignored"
    payload = _text(document | {"extra": {"retained": "canonical"}})
    outcome = native(validation._native_layout(fields), [payload])
    assert (None if outcome is None else f"landing source payload {outcome[1]}") == _serial_error(fields, payload)


def _malformed_payloads():
    valid = _text(_document())
    yield valid + " "
    yield valid.replace('"value":"synthetic"', '"value":1.0')
    yield valid.replace('"value":"synthetic"', '"value":NaN')
    yield valid.replace('"value":"synthetic"', '"value":-0')
    yield valid.replace('"item"', '"wrong"')
    yield valid.replace('"field":"item"', '"field":"item","field":"item"')
    yield valid.replace('"synthetic"', '"\\ud800"')
    yield _text(_document() | {"contract": "other"})
    for fields in (None, {}, [], [None], [{"field": "item", "value": None}]):
        yield _text(_document() | {"fields": fields})
    for extra in ([None] * 9990, [None] * 9991):
        yield _text(_document() | {"extra": extra})
    for depth in (31, 32):
        nested_values = None
        for _ in range(depth):
            nested_values = [nested_values]
        yield _text(_document() | {"extra": nested_values})
    yield "[]"
    yield "{}"
    yield "null"
    yield "{"


@pytest.mark.parametrize("payload", list(_malformed_payloads()), ids=lambda payload: f"payload-{len(payload)}")
def test_native_canonical_structure_and_field_errors_match_serial(native, payload):
    outcome = native(validation._native_layout(_fields()), [payload])
    assert (None if outcome is None else f"landing source payload {outcome[1]}") == _serial_error(_fields(), payload)


def test_deeply_nested_payload_is_rejected_without_serial_retry(native, monkeypatch):
    payload = _text(_document() | {"extra": "nested"}).replace('"nested"', "[" * 2_000 + "null" + "]" * 2_000)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("native rejection must not retry serial validation")

    monkeypatch.setattr(runner_codec, "payload_values", forbidden)
    with pytest.raises(CandidateRunnerError, match="^landing source payload is malformed$"):
        validation.validate_landing_payloads(_fields(), [(payload, len(payload))])
    valid = _text(_document())
    validation.validate_landing_payloads(_fields(), [(valid, len(valid))])


@pytest.mark.parametrize("limit", [0, 640, 4300])
def test_native_honors_current_python_integer_digit_limit(native, limit):
    original = sys.get_int_max_str_digits()
    try:
        sys.set_int_max_str_digits(limit)
        payload = _text(_document("integer", "sentinel")).replace('"sentinel"', "9" * ((limit or 4300) + 1))
        outcome = native(validation._native_layout(_fields("integer")), [payload])
        actual = None if outcome is None else f"landing source payload {outcome[1]}"
        assert actual == _serial_error(_fields("integer"), payload)
        assert actual == ("landing source payload is malformed" if limit else None)
    finally:
        sys.set_int_max_str_digits(original)


@pytest.mark.parametrize("reentry", ["validator", "encoder"])
def test_integer_digit_getter_can_reenter_source_without_deadlock(native, reentry):
    artifact = importlib.import_module("ptg2_address_canon.ptg2_address_canon").__file__
    script = textwrap.dedent("""\
        import importlib.util
        import sys

        artifact, reentry, payload = sys.argv[1:]
        spec = importlib.util.spec_from_file_location("ptg2_address_canon", artifact)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        validator = module.custom_import_validate_source_payloads_v1
        layout = (("item", "string", True),)
        original = sys.get_int_max_str_digits
        calls = 0

        def getter():
            global calls
            calls += 1
            if calls == 1:
                if reentry == "validator":
                    assert validator(layout, [payload]) is None
                else:
                    source_layout = (False, (("item", "string"),), (("item", 0),), ())
                    assert len(module.custom_import_source_documents_v1(
                        source_layout, [(("value", "synthetic"),)]
                    )) == 1
            return original()

        try:
            sys.get_int_max_str_digits = getter
            assert validator(layout, [payload]) is None
            assert calls == (2 if reentry == "validator" else 1)
        finally:
            sys.get_int_max_str_digits = original
        assert validator(layout, [payload]) is None
        """)
    # run() kills and waits for this isolated child if the native call deadlocks.
    subprocess.run(
        [sys.executable, "-I", "-c", script, artifact, reentry, _text(_document())],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_native_validates_all_rows_in_order_and_concurrent_calls_release_slot(native):
    layout = validation._native_layout(_fields())
    valid = _text(_document())
    with ThreadPoolExecutor(max_workers=4) as callers:
        results = list(callers.map(lambda index: native(layout, [valid] * index + ["{}", "["]), range(4)))
    assert results == [(index, "has an unknown contract") for index in range(4)]
    assert native(layout, [valid] * 64) is None


@pytest.mark.parametrize(
    "field_layouts,payloads",
    [
        ((("item", "string", True),), [_text(_document())] * 65),
        (tuple((f"item_{index}", "string", True) for index in range(65)), [_text(_document())]),
        ((("item", "string", True),), ["x" * 70_000] * 2),
        ((("item", "string", True),), [None]),
        ((("item", "date", True),), [_text(_document())]),
        ((("item", "string", 1),), [_text(_document())]),
        ((("item", "string", True),) * 2, [_text(_document())]),
    ],
    ids=["rows", "fields", "bytes", "text", "type", "nullable", "duplicate"],
)
def test_native_envelope_denial_releases_slot(native, field_layouts, payloads):
    with pytest.raises((ValueError, TypeError)):
        native(field_layouts, payloads)
    assert native(validation._native_layout(_fields()), [_text(_document())]) is None


@pytest.mark.parametrize("validation_bridge", ["native", "portable"], indirect=True)
def test_landing_batches_without_recovering_python_values(validation_bridge, monkeypatch):
    calls = []

    def checked(layout, payloads):
        calls.append(len(payloads))
        return validation_bridge(layout, payloads)

    def forbidden(*args, **kwargs):
        raise AssertionError("native payloads must not recover unused Python values")

    monkeypatch.setattr(validation, "native_validator", lambda: checked)
    monkeypatch.setattr(runner_codec, "payload_values", forbidden)
    payload = _text(_document())
    validation.validate_landing_payloads(_fields(), [(payload, len(payload))] * 130)
    assert calls == [64, 64, 2]
    context = _bulk_context()
    page = _prepared_page(context, [_root()] * 4)
    assert len(codec._page_values(page.records, context, context.request.definition.root_fields)) == 4


@pytest.mark.parametrize("kind,value", [("date", "2020-W01-1"), ("timestamp", "2020-01-01T01:02:03+02:00")])
def test_unsupported_layout_is_serial_before_capability_selection(monkeypatch, kind, value):
    def forbidden():
        raise AssertionError("unsupported layouts must preselect serial")

    monkeypatch.setattr(validation, "native_validator", forbidden)
    payload = _text(_document(kind, value))
    validation.validate_landing_payloads(_fields(kind), [(payload, len(payload))])


@pytest.mark.parametrize("validation_bridge", ["native", "portable"], indirect=True)
def test_byte_bound_splits_batches_and_large_serial_row_retains_order(validation_bridge, monkeypatch):
    calls = []
    payload = _text(_document(value="x" * 70_000))

    def checked(layout, payloads):
        calls.append(len(payloads))
        return validation_bridge(layout, payloads)

    monkeypatch.setattr(validation, "native_validator", lambda: checked)
    validation.validate_landing_payloads(_fields(), [(payload, len(payload))] * 2)
    large = _text(_document(value="x" * validation.MAX_INPUT_BYTES))
    validation.validate_landing_payloads(_fields(), [(large, len(large))])
    assert calls == [1, 1]


@pytest.mark.parametrize("name", ["$serde_json::private::Number", "$serde_json::private::RawValue"])
@pytest.mark.parametrize("validation_bridge", ["native", "portable"], indirect=True)
def test_parser_specific_object_members_preselect_serial(validation_bridge, monkeypatch, name):
    calls = []

    def checked(layout, payloads):
        calls.append(len(payloads))
        return validation_bridge(layout, payloads)

    monkeypatch.setattr(validation, "native_validator", lambda: checked)
    ordinary = _text(_document())
    payload = _text(_document() | {"extra": {name: "1"}})
    assert _serial_error(_fields(), payload) is None
    validation.validate_landing_payloads(_fields(), [(ordinary, len(ordinary)), (payload, len(payload))])
    assert calls == [1]


def _changed_payload(row, payload):
    return replace(row, payload=payload, byte_count=row.byte_count - len(row.payload.encode("utf-8")) + len(payload))


@pytest.mark.parametrize("fault", ["identity", "accounting", "rejection"])
@pytest.mark.parametrize("earlier_payload", [False, True])
@pytest.mark.parametrize("validation_bridge", ["native", "portable"], indirect=True)
def test_payload_errors_keep_original_row_precedence(validation_bridge, fault, earlier_payload):
    context = _bulk_context()
    good = _prepared_page(context).records[0]
    malformed = _changed_payload(good, "{")
    if fault == "identity":
        other = replace(good, raw_key=None)
    elif fault == "accounting":
        other = replace(good, byte_count=good.byte_count + 1)
    else:
        other = _prepared_page(context, [_root(npi=None)]).records[0]
        other.rejection.dataset_id += 1
    rows = (malformed, other) if earlier_payload else (other, malformed)
    fields = context.request.definition.root_fields
    with pytest.raises(Exception) as serial:
        [codec._row_values(row, context, fields) for row in rows]
    with pytest.raises(type(serial.value), match="^" + str(serial.value) + "$"):
        codec._page_values(rows, context, fields)


@pytest.mark.parametrize("validation_bridge", ["native", "portable"], indirect=True)
@pytest.mark.parametrize("malformed_payload", [False, True])
def test_fatal_envelope_error_does_not_validate_pending_payloads(validation_bridge, monkeypatch, malformed_payload):
    context = _bulk_context()
    good = _prepared_page(context).records[0]
    first = _changed_payload(good, "{") if malformed_payload else good
    failure = MemoryError("synthetic allocation failure")
    original = codec._row_fields
    calls = []

    def row_fields(prepared, context):
        if prepared is second:
            raise failure
        return original(prepared, context)

    def validate(layout, payloads):
        calls.append(len(payloads))
        return validation_bridge(layout, payloads)

    second = replace(good)
    monkeypatch.setattr(codec, "_row_fields", row_fields)
    monkeypatch.setattr(validation, "native_validator", lambda: validate)
    with pytest.raises(MemoryError) as caught:
        codec._page_values((first, second), context, context.request.definition.root_fields)
    assert caught.value is failure and calls == []


@pytest.mark.parametrize("outcome", [False, (), (-1, "is malformed"), (0, 1), (2, "is malformed")])
def test_invalid_native_result_never_retries_serial(monkeypatch, outcome):
    monkeypatch.setattr(validation, "native_validator", lambda: lambda *_args: outcome)
    payload = _text(_document())
    with pytest.raises(CandidateRunnerError, match="validator returned an invalid result"):
        validation.validate_landing_payloads(_fields(), [(payload, len(payload))])


def test_native_exception_never_retries_serial(monkeypatch):
    def failed(*_args):
        raise ValueError("synthetic native failure")

    monkeypatch.setattr(validation, "native_validator", lambda: failed)
    payload = _text(_document())
    with pytest.raises(ValueError, match="synthetic native failure"):
        validation.validate_landing_payloads(_fields(), [(payload, len(payload))])


def test_absent_capability_preserves_complete_serial_validation(monkeypatch):
    monkeypatch.setattr(validation, "native_validator", lambda: None)
    payload = _text(_document())
    validation.validate_landing_payloads(_fields(), [(payload, len(payload))])
    with pytest.raises(CandidateRunnerError, match="has an unknown contract"):
        validation.validate_landing_payloads(_fields(), [("{}", 2)])
