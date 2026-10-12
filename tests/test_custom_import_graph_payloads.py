# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native graph decoding must match the existing full canonical/projection oracle."""

from __future__ import annotations

import datetime as dt
import decimal
from dataclasses import replace
from decimal import Decimal
from unittest.mock import Mock

import pytest

from process.custom_import import build_graph as graph
from process.custom_import import build_graph_sets as sets
from process.custom_import import graph_payloads, runner_codec
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_graph import _registry, _request
from tests.test_custom_import_build_graph_sets import family_input
from tests.test_custom_import_definition import _rate, _raw_definition


@pytest.fixture(autouse=True)
def isolate_decoder_capability(monkeypatch):
    """Keep this file's eight-field decoder checks independent of batch projection."""
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: None)


def _definition():
    raw = _raw_definition()
    for slot, (name, kind) in enumerate((("note", "string"), ("visits", "integer"), ("enabled", "boolean")), 6):
        raw["schema"]["children"][0]["fields"].append(
            {"id": name, "slot": slot, "type": kind, "nullable": True, "projection_slot": slot - 1}
        )
    return CustomImportDefinition.from_mapping(raw)


def _group(request, child_values):
    prepared, children = family_input(request, 1, values=child_values)
    current = graph.CustomImportBuildFamily(build_id=7, root_record_id=1, family_revision_id=9, selection_kind="source")
    family = graph.CustomImportFamilyRevision(
        family_revision_id=9, root_record_id=1, entity_binding_id=6001, family_sha256=prepared.family_sha256
    )
    return sets._ChildGroup(sets._Started(prepared, current, family, False), children)


def _proof(request, registry, group, child):
    fields = runner_codec.fields_by_collection(request.definition)["rates"]
    values = runner_codec.payload_values(fields, child.canonical_payload, label="oracle")
    _edge, scalars, _contexts = graph._child_projection_values(
        request, registry, 7, group.state.family, group.state.source.values, child, values, "rates"
    )
    cells = tuple(
        (("null", None) if values[field.field_id] is None else ("value", values[field.field_id]))
        if field.field_id in values
        else ("missing", None)
        for field in fields
    )
    scalar_rows = tuple(
        (row["field_slot"], row["projection_slot"], *(row[name] for _kind, name in sets._SCALAR_COLUMNS[2:]))
        for row in scalars
    )
    byte_count = sum(len(value.encode("utf-8")) for row in scalars for value in row.values() if isinstance(value, str))
    return (
        child.canonical_parent_key,
        bytes(child.parent_key_sha256),
        child.canonical_child_key,
        bytes(child.child_key_sha256),
        bytes(child.payload_sha256),
        cells,
        scalar_rows,
        byte_count,
    )


def _native_proof(request, proof):
    """Mirror the text-only decimal ABI before the Python boundary converts it."""
    cells, scalars = list(proof[5]), list(proof[6])
    by_slot = {}
    fields = runner_codec.fields_by_collection(request.definition)["rates"]
    for index, (field, (state, value)) in enumerate(zip(fields, cells, strict=True)):
        if field.value_type == "decimal" and state == "value":
            text = format(value, "f")
            cells[index] = (state, text)
            by_slot[field.field_slot] = text
    for index, scalar in enumerate(scalars):
        if scalar[2:4] == ("decimal", "value"):
            scalars[index] = (*scalar[:6], by_slot[scalar[0]], *scalar[7:])
    return (*proof[:5], tuple(cells), tuple(scalars), proof[7])


@pytest.fixture
def oracle_decoder(monkeypatch):
    def install(request, group):
        registry = _registry(request.definition)
        by_payload = {
            child.canonical_payload: _native_proof(request, _proof(request, registry, group, child))
            for child in group.children
        }
        decoder = Mock(side_effect=lambda _layout, payloads: [by_payload.get(payload) for payload in payloads])
        monkeypatch.setattr(graph_payloads, "native_decoder", lambda: decoder)
        return decoder

    return install


@pytest.mark.parametrize("optional", ["missing", "null", "value"])
def test_native_arrays_and_request_limits_match_complete_python_projection(monkeypatch, oracle_decoder, optional):
    request = _request(definition=_definition(), page_row_limit=256)
    child_values_by_field = _rate("0000000001", 'A"\\\nΔ', Decimal("1.000000000001"))
    if optional != "missing":
        child_values_by_field.update(
            {"note": "é😀", "visits": -(1 << 63), "enabled": False}
            if optional == "value"
            else {"note": None, "visits": None, "enabled": None}
        )
    group = _group(request, [child_values_by_field])
    registry = _registry(request.definition)
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: None)
    expected = sets._child_arguments(request, registry, (group,))
    decoder = oracle_decoder(request, group)
    assert sets._child_arguments(request, registry, (group,)) == expected
    decoder.assert_called_once()
    scalars, contexts, scalar_bytes = sets._source_projections(request, registry, group)
    total_bytes = scalar_bytes + sum(
        len(context_value.encode("utf-8")) if isinstance(context_value, str) else len(context_value)
        for context in contexts
        for context_value in context.values()
        if isinstance(context_value, (str, bytes))
    )
    row_limit = 3 + len(scalars) + len(contexts)
    sets._child_arguments(replace(request, page_row_limit=row_limit, page_byte_limit=total_bytes), registry, (group,))
    for changed, message in (
        (replace(request, page_row_limit=row_limit - 1), "row page"),
        (replace(request, page_byte_limit=total_bytes - 1), "byte page"),
    ):
        with pytest.raises(CandidateRunnerError, match=message):
            sets._child_arguments(changed, registry, (group,))


def test_native_batches_bound_input_and_drain_before_later_read_error(monkeypatch):
    definition = _definition()
    decoder = Mock(side_effect=lambda _layout, payloads: [None] * len(payloads))
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: decoder)
    inputs = [(index, "x" * 3_000) for index in range(150)]
    assert list(graph_payloads.decode_records(definition, "rates", inputs)) == [(index, None) for index in range(150)]
    layout_bytes = 256 + graph_payloads._layout_bytes(graph_payloads._layout(definition, "rates")[0])
    assert decoder.call_count > 3
    for call in decoder.call_args_list:
        payloads = call.args[1]
        assert len(payloads) <= 64
        assert layout_bytes + sum(64 + len(payload.encode("utf-8")) for payload in payloads) <= 131_072

    def failing_input():
        yield 1, "{}"
        raise CandidateRunnerError("later read error")

    decoded = graph_payloads.decode_records(definition, "rates", failing_input())
    assert next(decoded) == (1, None)
    with pytest.raises(CandidateRunnerError, match="later read error"):
        next(decoded)


@pytest.mark.parametrize("chain", ["direct", "cause", "context"])
@pytest.mark.parametrize("malformed_pending", [False, True])
def test_fatal_input_failure_does_not_flush_pending_graph_documents(oracle_decoder, chain, malformed_pending):
    request = _request(page_row_limit=256)
    group = _group(request, [_rate("0000000001")])
    decoder = oracle_decoder(request, group)
    child = group.children[0]
    allocation = MemoryError("synthetic allocation failure")
    failure = allocation if chain == "direct" else OSError("synthetic input cleanup")
    if chain == "cause":
        failure.__cause__ = allocation
    elif chain == "context":
        failure.__context__ = allocation
    close_events = []

    def documents():
        try:
            key_hash = b"x" * 32 if malformed_pending else bytes(child.child_key_sha256)
            yield key_hash, child.canonical_child_key, child.canonical_payload
            raise failure
        finally:
            close_events.append("closed")

    with pytest.raises(type(failure)) as caught:
        list(graph_payloads.validated_documents(request.definition, "rates", documents()))
    assert caught.value is failure
    decoder.assert_not_called()
    assert close_events == ["closed"]
    if chain != "direct":
        assert getattr(failure, f"__{chain}__") is allocation


def test_unsupported_input_keeps_ordered_python_fallback(monkeypatch):
    definition = _definition()
    decoder = Mock(side_effect=lambda _layout, payloads: [None] * len(payloads))
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: decoder)
    inputs = [(0, "{}"), (1, "x" * 131_072), (2, "\ud800"), (3, None), (4, "{}")]
    assert list(graph_payloads.decode_records(definition, "rates", inputs)) == [(index, None) for index in range(5)]
    assert [call.args[1] for call in decoder.call_args_list] == [["{}"], ["{}"]]


@pytest.mark.parametrize("replacement", ["wrong_value", "reentry"])
def test_decoder_decimal_constructor_replacement_cannot_change_values_or_reenter(monkeypatch, replacement):
    request = _request(page_row_limit=256)
    group = _group(request, [_rate("0000000001", "A", Decimal("1.25"))])
    child = group.children[0]
    expected = _proof(request, _registry(request.definition), group, child)
    native = _native_proof(request, expected)
    hook = Mock(
        return_value=Decimal("0"),
        side_effect=AssertionError("must not call a constructor that could reenter native admission")
        if replacement == "reentry"
        else None,
    )

    def decode(*_args):
        # This models replacement while native admission/decoding releases the GIL.
        monkeypatch.setattr(decimal, "Decimal", hook)
        return [native]

    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: decode)
    converted = Mock(wraps=Decimal)
    monkeypatch.setattr(graph_payloads, "Decimal", converted)
    [(actual_child, actual)] = graph_payloads.decode_records(
        request.definition, "rates", [(child, child.canonical_payload)]
    )
    assert actual_child is child and actual == expected
    hook.assert_not_called()
    fields = runner_codec.fields_by_collection(request.definition)["rates"]
    decimal_cells_by_field = {
        field.field_slot: cell[1]
        for field, cell in zip(fields, actual[5], strict=True)
        if field.value_type == "decimal" and cell[0] == "value"
    }
    assert converted.call_count == len(decimal_cells_by_field)
    for scalar in actual[6]:
        if scalar[2:4] == ("decimal", "value"):
            assert scalar[6] is decimal_cells_by_field[scalar[0]]


@pytest.mark.parametrize("at_decoder", [True, False])
def test_decimal_resource_failure_propagates_without_serial_retry(monkeypatch, at_decoder):
    request = _request(page_row_limit=256)
    group = _group(request, [_rate("0000000001")])
    child = group.children[0]
    native = _native_proof(request, _proof(request, _registry(request.definition), group, child))
    failure = MemoryError("synthetic allocation failure")
    decoder = Mock(side_effect=failure) if at_decoder else Mock(return_value=[native])
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: decoder)
    if not at_decoder:
        monkeypatch.setattr(graph_payloads, "Decimal", Mock(side_effect=failure))
    with pytest.raises(MemoryError) as caught:
        list(graph_payloads.decode_records(request.definition, "rates", [(child, child.canonical_payload)]))
    assert caught.value is failure
    decoder.assert_called_once()


@pytest.mark.parametrize("corruption", ["type", "length", "invalid", "nonfinite", "projection"])
def test_native_decimal_text_and_projection_must_agree(monkeypatch, corruption):
    request = _request(page_row_limit=256)
    group = _group(request, [_rate("0000000001")])
    child = group.children[0]
    proof_parts = list(_native_proof(request, _proof(request, _registry(request.definition), group, child)))
    fields = runner_codec.fields_by_collection(request.definition)["rates"]
    index = next(index for index, field in enumerate(fields) if field.value_type == "decimal")
    if corruption == "projection":
        scalars = list(proof_parts[6])
        projected = next(index for index, scalar in enumerate(scalars) if scalar[2] == "decimal")
        scalar = scalars[projected]
        scalars[projected] = (*scalar[:6], "0.5", *scalar[7:])
        proof_parts[6] = tuple(scalars)
    else:
        cells = list(proof_parts[5])
        cells[index] = ("value", {"type": 1, "length": "1" * 65, "invalid": "bad", "nonfinite": "NaN"}[corruption])
        proof_parts[5] = tuple(cells)
    decoder = Mock(return_value=[tuple(proof_parts)])
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: decoder)
    with pytest.raises(CandidateRunnerError, match="native graph.*decimal"):
        list(graph_payloads.decode_records(request.definition, "rates", [(child, child.canonical_payload)]))
    decoder.assert_called_once()


def test_date_layout_uses_existing_typed_projection_without_native_entry(monkeypatch):
    raw = _raw_definition()
    raw["schema"]["children"][0]["fields"].append(
        {"id": "day", "slot": 6, "type": "date", "nullable": True, "projection_slot": 5}
    )
    request = _request(definition=CustomImportDefinition.from_mapping(raw), page_row_limit=256)
    group = _group(request, [_rate("0000000001") | {"day": dt.date(2024, 2, 29)}])
    decoder = Mock(side_effect=AssertionError("unsupported layout"))
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: decoder)
    _name, arguments = sets._child_arguments(request, _registry(request.definition), (group,))
    assert dt.date(2024, 2, 29) in arguments[20][1]
    decoder.assert_not_called()


def test_family_prepass_keeps_earlier_validation_error_before_later_read_error(oracle_decoder):
    request = _request(page_row_limit=256)
    group = _group(request, [_rate("0000000001")])
    oracle_decoder(request, group)
    child = group.children[0]

    def documents():
        yield b"x" * 32, child.canonical_child_key, child.canonical_payload
        raise CandidateRunnerError("later read error")

    with pytest.raises(CandidateRunnerError, match="ordered child key digest does not match"):
        list(graph_payloads.validated_documents(request.definition, "rates", documents()))


@pytest.mark.parametrize("corruption", ["prefix", "row", "binding", "lane", "accounting"])
def test_malformed_native_output_fails_closed(monkeypatch, oracle_decoder, corruption):
    request = _request(page_row_limit=256)
    group = _group(request, [_rate("0000000001")])
    decoder = oracle_decoder(request, group)
    proof_parts = list(_native_proof(request, _proof(request, _registry(request.definition), group, group.children[0])))
    if corruption == "prefix":
        decoder.side_effect = lambda *_: []
    elif corruption == "row":
        decoder.side_effect = lambda *_: [("malformed",)]
    else:
        if corruption == "accounting":
            proof_parts[7] += 1
        else:
            scalars = [list(scalar) for scalar in proof_parts[6]]
            scalars[0][0 if corruption == "binding" else 9] = 99
            proof_parts[6] = tuple(tuple(scalar) for scalar in scalars)
        decoder.side_effect = lambda *_: [tuple(proof_parts)]
    with pytest.raises(CandidateRunnerError, match="native graph"):
        sets._child_arguments(request, _registry(request.definition), (group,))


def test_compiled_decoder_matches_full_codec_projection_and_family_hash(monkeypatch):
    module = pytest.importorskip("ptg2_address_canon")
    decoder = getattr(module, "custom_import_graph_children_v1", None)
    if decoder is None:
        pytest.skip("installed native module predates graph decoding")
    request = _request(definition=_definition(), page_row_limit=256)
    child_values = [
        _rate("0000000001", 'A"\\\nΔ', Decimal("-1.000000000001")),
        _rate("0000000001", "B") | {"note": None, "visits": None, "enabled": None},
        _rate("0000000001", "C") | {"note": "é😀", "visits": (1 << 63) - 1, "enabled": False},
    ]
    group = _group(request, child_values)
    registry = _registry(request.definition)
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: None)
    expected = sets._child_arguments(request, registry, (group,))
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: decoder)
    assert sets._child_arguments(request, registry, (group,)) == expected
    for child, proof in graph_payloads.decode_records(
        request.definition, "rates", ((child, child.canonical_payload) for child in group.children)
    ):
        assert proof == _proof(request, registry, group, child)
    documents = sorted(
        (bytes(child.child_key_sha256), child.canonical_child_key, child.canonical_payload) for child in group.children
    )
    assert list(graph_payloads.validated_documents(request.definition, "rates", documents)) == documents
    digest = runner_codec._family_hash_from_documents(
        request.definition,
        group.state.source.values,
        {"rates": graph_payloads.validated_documents(request.definition, "rates", documents)},
    )
    assert digest == group.state.source.family_sha256
