# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepared child columns preserve the serial codec and reject untrusted accounting."""

from decimal import Decimal
from unittest.mock import Mock

import pytest

from process.custom_import import build_graph as graph
from process.custom_import import build_graph_prepared as scratch
from process.custom_import import build_graph_sets as sets
from process.custom_import import graph_payloads
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_graph import _registry, _request
from tests.test_custom_import_definition import _rate
from tests.test_custom_import_graph_payloads import _definition, _group, _proof
from tests.test_custom_import_graph_payloads import oracle_decoder as oracle_decoder
from tests.test_custom_import_graph_prepared_replay import prepare_groups, replay_groups


@pytest.fixture
def column_case(monkeypatch, oracle_decoder):
    request = _request(definition=_definition(), page_row_limit=256)
    values = _rate("0000000001", 'Ω😀"\\\n', Decimal("1.000000000001"))
    values.update(note=None, visits=-(1 << 63), enabled=False)
    group, registry = _group(request, [values]), _registry(request.definition)
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: None)
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: None)
    expected = sets._child_arguments(request, registry, (group,))
    decoder = oracle_decoder(request, group)
    stream = scratch.PreparedChildren()
    try:
        prepare_groups(request, registry, (group,), stream)
        prepared = replay_groups((group,), stream)[0].children[0]
        columns = prepared.record()[6]
        assert columns is not None
        decoder.assert_called_once()
        return request, registry, group, columns, expected
    finally:
        stream.close()


def replay_columns(request, registry, group, columns):
    child = group.children[0]
    stream = scratch.PreparedChildren()
    try:
        stream.append(group.state.current.root_record_id, "rates", child, graph._model_bytes((child,)), columns)
        stream.finish()
        return sets._child_arguments(request, registry, replay_groups((group,), stream))
    finally:
        stream.close()


def test_prepared_columns_preserve_serial_arguments_and_utf8_charges(column_case):
    request, registry, group, columns, expected = column_case
    serial = sets._serial_source_columns(request, registry, group, group.children[0])
    assert columns[1:] == serial[1:]
    assert columns[0][:6] == serial[0][:6]
    assert columns[0][7:] == serial[0][7:]
    assert tuple(None if value is None else Decimal(value) for value in columns[0][6]) == serial[0][6]
    assert columns[2] > sum(len(value) for column in serial[0] for value in column if isinstance(value, str))
    assert replay_columns(request, registry, group, columns) == expected


@pytest.mark.parametrize(
    ("section", "value", "message"),
    [
        (None, 42, "prepared child columns differ"),
        (None, (), "prepared child columns differ"),
        (0, (), "projection column shape differs"),
        (1, (), "projection column shape differs"),
    ],
)
def test_prepared_replay_rejects_column_shapes(column_case, section, value, message):
    request, registry, group, columns, _expected = column_case
    changed_columns = list(columns)
    if section is None:
        changed_columns = value
    else:
        changed_columns[section] = value
        changed_columns = tuple(changed_columns)
    with pytest.raises(CandidateRunnerError, match=message):
        replay_columns(request, registry, group, changed_columns)


@pytest.mark.parametrize("section", [0, 1], ids=["scalar", "context"])
@pytest.mark.parametrize("corruption", ["prefix", "type", "identity"])
def test_prepared_replay_binds_every_column_to_its_child(column_case, section, corruption):
    request, registry, group, columns, _expected = column_case
    changed_columns = list(columns)
    lanes = list(changed_columns[section])
    assert lanes[0]
    if corruption == "prefix":
        lanes[0] = lanes[0][:-1]
        message = "projection column prefix differs"
    elif corruption == "type":
        lanes[0] = tuple(True for _value in lanes[0])
        message = "projection column type differs"
    else:
        lanes[0] = tuple(value + 1 for value in lanes[0])
        message = "prepared child column accounting differs"
    changed_columns[section] = tuple(lanes)
    with pytest.raises(CandidateRunnerError, match=message):
        replay_columns(request, registry, group, tuple(changed_columns))


@pytest.mark.parametrize("charge", [2, 3, 4], ids=["scalar", "context", "retained"])
@pytest.mark.parametrize("corruption", ["understated", "negative", "boolean"])
def test_prepared_replay_rejects_invalid_byte_charges(column_case, charge, corruption):
    request, registry, group, columns, _expected = column_case
    changed_columns = list(columns)
    assert changed_columns[charge] > 0
    if corruption == "boolean":
        changed_columns = [((),) * 10, ((),) * 3, 0, 0, 0]
        changed_columns[charge] = False
    else:
        changed_columns[charge] = changed_columns[charge] - 1 if corruption == "understated" else -1
    with pytest.raises(CandidateRunnerError, match="prepared child column accounting differs"):
        replay_columns(request, registry, group, tuple(changed_columns))


@pytest.mark.parametrize("decimal_text", ["not-a-decimal", "NaN"])
def test_prepared_replay_rejects_invalid_decimal_text(column_case, decimal_text):
    request, registry, group, columns, _expected = column_case
    scalars = list(columns[0])
    scalars[6] = tuple(decimal_text if value is not None else None for value in scalars[6])
    changed_columns = (tuple(scalars), *columns[1:])
    with pytest.raises(CandidateRunnerError, match="projection decimal text differs"):
        replay_columns(request, registry, group, changed_columns)


@pytest.mark.parametrize("mismatch", ["missing", "parent_key", "parent_digest"])
def test_preparation_keeps_serial_fallback_for_unbound_proofs(column_case, mismatch):
    request, registry, group, _columns, expected = column_case
    child = group.children[0]
    proof_parts = list(_proof(request, registry, group, child))
    if mismatch == "missing":
        proof_parts = None
    else:
        proof_parts[0 if mismatch == "parent_key" else 1] = "other-parent" if mismatch == "parent_key" else b"x" * 32
        proof_parts = tuple(proof_parts)
    columns = sets.prepare_child_columns(request, registry, group.state.source, "rates", child, proof_parts)
    assert columns is None
    assert replay_columns(request, registry, group, columns) == expected


def test_preparation_rejects_payload_digest_mismatch(column_case):
    request, registry, group, _columns, _expected = column_case
    child = group.children[0]
    proof = _proof(request, registry, group, child)
    with pytest.raises(CandidateRunnerError, match="native child payload digest differs"):
        sets.prepare_child_columns(
            request, registry, group.state.source, "rates", child, (*proof[:4], b"x" * 32, *proof[5:])
        )


@pytest.mark.parametrize(
    "error_type", [graph.material.ScalarProjectionError, graph.material.WinnerMaterializationError]
)
def test_ordinary_context_errors_defer_to_complete_serial_replay(monkeypatch, column_case, error_type):
    request, registry, group, _columns, expected = column_case
    contexts = Mock(side_effect=error_type("context rejected"))
    stream = scratch.PreparedChildren()
    try:
        with monkeypatch.context() as failing:
            failing.setattr(graph, "_context_values", contexts)
            prepare_groups(request, registry, (group,), stream)
        contexts.assert_called_once()
        assert stream.row_count == 1
        assert all(record[6] is None and record[7] is not None for _, _, record in stream.records())
        assert sets._child_arguments(request, registry, replay_groups((group,), stream)) == expected
    finally:
        stream.close()


@pytest.mark.parametrize(
    "error_type", [graph.material.ScalarProjectionError, graph.material.WinnerMaterializationError]
)
@pytest.mark.parametrize("chain", ["__cause__", "__context__"])
def test_context_allocation_failure_chains_abort_preparation(monkeypatch, column_case, error_type, chain):
    request, registry, group, _columns, _expected = column_case
    error = error_type("context rejected")
    setattr(error, chain, MemoryError("allocation failed"))
    monkeypatch.setattr(graph, "_context_values", Mock(side_effect=error))
    stream = scratch.PreparedChildren()
    try:
        with pytest.raises(error_type, match="context rejected") as caught:
            prepare_groups(request, registry, (group,), stream)
        assert caught.value is error
        assert stream.row_count == 0
        assert not stream.pending and stream.file is None
    finally:
        stream.close()


def test_unsupported_decimal_keeps_complete_serial_replay(monkeypatch, column_case):
    request, registry, group, _columns, expected = column_case
    encoder = Mock(return_value=None)
    stream = scratch.PreparedChildren()
    try:
        with monkeypatch.context() as unsupported:
            unsupported.setattr(graph_payloads, "_projection_decimal", encoder)
            prepare_groups(request, registry, (group,), stream)
        encoder.assert_called_once_with(Decimal("1.000000000001"))
        assert all(record[6] is None and record[7] is not None for _, _, record in stream.records())
        assert sets._child_arguments(request, registry, replay_groups((group,), stream)) == expected
    finally:
        stream.close()
