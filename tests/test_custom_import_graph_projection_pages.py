# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native column preparation keeps logical page policy and the serial write codec."""

from __future__ import annotations

import importlib
from dataclasses import replace
from unittest.mock import Mock

import pytest

from process.custom_import import build_graph as graph
from process.custom_import import build_graph_sets as sets
from process.custom_import import graph_payloads
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_graph import _registry, _request
from tests.test_custom_import_build_graph_sets import CallerDatabase, family_input
from tests.test_custom_import_graph_projection_batch import _wide_context_group, oracle_encoded


def source_groups(request, counts):
    groups = []
    for root_id, count in enumerate(counts, 1):
        source, children = family_input(request, root_id, count=count)
        current = graph.CustomImportBuildFamily(
            build_id=7, root_record_id=root_id, family_revision_id=10 + root_id, selection_kind="source"
        )
        family = graph.CustomImportFamilyRevision(
            family_revision_id=current.family_revision_id,
            root_record_id=root_id,
            entity_binding_id=6000 + root_id,
            family_sha256=source.family_sha256,
        )
        groups.append(sets._ChildGroup(sets._Started(source, current, family, False), children))
    return tuple(groups)


def prepared_packet(request, registry, groups, records, *, rejected_ids=()):
    groups_by_child = {child.child_revision_id: group for group in groups for child in group.children}
    spans, scalars, contexts = [], [[] for _ in range(10)], [[] for _ in range(4)]
    for record in records:
        if record.child.child_revision_id in rejected_ids:
            spans.append(None)
            continue
        group = groups_by_child[record.child.child_revision_id]
        encoded = oracle_encoded(request, registry, replace(group, children=(record.child,)))
        span = encoded[0][0]
        spans.append((len(scalars[0]), span[1], len(contexts[0]), *span[3:]))
        for target, column in zip((*scalars, *contexts), (*encoded[1], *encoded[2]), strict=True):
            target.extend(column)
    return graph_payloads._projection_result(
        (tuple(spans), tuple(map(tuple, scalars)), tuple(map(tuple, contexts))), len(records), (0, 0)
    )


def serial_arguments(monkeypatch, request, registry, groups):
    with monkeypatch.context() as serial:
        serial.setattr(graph_payloads, "native_projector", lambda: None)
        return sets._child_arguments(request, registry, groups)


@pytest.fixture(autouse=True)
def serial_decoder(monkeypatch):
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: None)


def test_one_packet_spans_families_without_reconstructing_row_models(monkeypatch):
    request = _request(page_row_limit=256)
    registry, groups = _registry(request.definition), source_groups(request, (2, 3))
    expected = serial_arguments(monkeypatch, request, registry, groups)
    all_records = tuple(
        graph_payloads.ProjectionRecord(child, group.state.source.record, group.state.family, group.state.source.values)
        for group in groups
        for child in group.children
    )
    prepared = prepared_packet(request, registry, groups, all_records)
    projector = Mock(return_value=prepared)
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: projector)
    monkeypatch.setattr(graph_payloads, "project_batch", projector)
    monkeypatch.setattr(sets, "_serial_child_projection", Mock(side_effect=AssertionError("serial row work")))
    assert sets._child_arguments(request, registry, groups) == expected
    projector.assert_called_once()
    assert projector.call_args.args[2] == all_records
    assert projector.call_args.kwargs == {"retained_bytes": 0}


def test_adaptive_packets_keep_order_and_account_for_retained_columns(monkeypatch):
    request = _request(page_row_limit=256, page_byte_limit=4_194_304)
    registry, groups = _registry(request.definition), source_groups(request, (20, 20))
    expected = serial_arguments(monkeypatch, request, registry, groups)
    observed_packets = []

    def prepare(_definition, _slots, records, *, retained_bytes):
        observed_packets.append((tuple(record.child.child_revision_id for record in records), retained_bytes))
        return None if len(records) > 16 else prepared_packet(request, registry, groups, records)

    monkeypatch.setattr(graph_payloads, "native_projector", lambda: prepare)
    monkeypatch.setattr(graph_payloads, "project_batch", prepare)
    assert sets._child_arguments(request, registry, groups) == expected
    assert [len(ids) for ids, _bytes in observed_packets] == [40, 20, 10, 10, 20, 10, 10]
    assert (
        observed_packets[0][1]
        == observed_packets[1][1]
        == observed_packets[2][1]
        == 0
        < observed_packets[3][1]
        < observed_packets[4][1]
        == observed_packets[5][1]
        < observed_packets[6][1]
    )
    assert tuple(child_id for ids, _bytes in observed_packets if len(ids) <= 16 for child_id in ids) == tuple(
        child.child_revision_id for group in groups for child in group.children
    )


def test_rejected_native_packets_keep_serial_columns_and_retained_accounting(monkeypatch):
    request = _request(page_row_limit=256)
    registry, groups = _registry(request.definition), source_groups(request, (2,))
    expected = serial_arguments(monkeypatch, request, registry, groups)
    projector = Mock(return_value=None)
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: projector)
    monkeypatch.setattr(graph_payloads, "project_batch", projector)
    assert sets._child_arguments(request, registry, groups) == expected
    assert [len(call.args[2]) for call in projector.call_args_list] == [2, 1, 1]
    assert [call.args[2][0].child for call in projector.call_args_list[1:]] == list(groups[0].children)
    assert projector.call_args_list[0].kwargs["retained_bytes"] == 0
    assert projector.call_args_list[1].kwargs["retained_bytes"] == 0
    assert projector.call_args_list[2].kwargs["retained_bytes"] > 0


@pytest.mark.parametrize("corruption", ["unknown_family", "descending_family"])
def test_child_projection_rejects_rows_outside_ordered_family_prefix(corruption):
    groups = source_groups(_request(), (1, 1))
    rows = [(group.state.current.root_record_id, group.children[0], True) for group in groups]
    if corruption == "unknown_family":
        rows[0] = (99, rows[0][1], True)
    else:
        rows.reverse()
    with pytest.raises(CandidateRunnerError, match="family order"):
        sets._logical_child_groups(rows, tuple(group.state for group in groups))


def test_child_projection_rejects_count_beyond_frozen_family_before_write(monkeypatch):
    request = _request(page_row_limit=256)
    registry, groups = _registry(request.definition), source_groups(request, (2,))
    groups[0].state.current.attached_child_count = 0
    source = replace(groups[0].state.source, child_count=1)
    states = (replace(groups[0].state, source=source),)
    database = CallerDatabase(monkeypatch, request, [(source, groups[0].children)])
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: None)
    with database.read_transaction(database.session, request, 7):
        with pytest.raises(CandidateRunnerError, match="inconsistent durable progress"):
            sets._read_child_batch(database.session, request, registry, (source,), states, "source", 25, 64)
    assert not database.read_open and not database.write_open
    assert not database.calls and not database.seen
    assert states[0].current.attached_child_count == 0


@pytest.mark.parametrize("bound", [{"page_row_limit": 1}, {"page_byte_limit": 1}])
def test_native_column_charges_preserve_logical_page_rejection(monkeypatch, bound):
    request = _request(**bound)
    registry, groups = _registry(request.definition), source_groups(request, (2,))
    with pytest.raises(CandidateRunnerError) as expected:
        serial_arguments(monkeypatch, request, registry, groups)

    def prepare(_definition, _slots, records, **_bounds):
        return prepared_packet(request, registry, groups, records)

    monkeypatch.setattr(graph_payloads, "native_projector", lambda: prepare)
    monkeypatch.setattr(graph_payloads, "project_batch", prepare)
    with pytest.raises(type(expected.value)) as actual:
        sets._child_arguments(request, registry, groups)
    assert str(actual.value) == str(expected.value)


def test_earlier_semantic_error_precedes_later_packets_and_page_limits(monkeypatch):
    request = _request(page_row_limit=1)
    registry, groups = _registry(request.definition), source_groups(request, (65,))
    bad_child = groups[0].children[0]
    bad_child.child_key_sha256 = b"x" * 32
    with pytest.raises(CandidateRunnerError) as expected:
        serial_arguments(monkeypatch, request, registry, groups)
    calls = []

    def prepare(_definition, _slots, records, **_bounds):
        calls.append(len(records))
        return prepared_packet(request, registry, groups, records, rejected_ids=(bad_child.child_revision_id,))

    monkeypatch.setattr(graph_payloads, "native_projector", lambda: prepare)
    monkeypatch.setattr(graph_payloads, "project_batch", prepare)
    with pytest.raises(type(expected.value)) as actual:
        sets._child_arguments(request, registry, groups)
    assert str(actual.value) == str(expected.value)
    assert calls == [64]


@pytest.mark.parametrize("failure", [MemoryError("allocation"), KeyboardInterrupt(), SystemExit(2)])
def test_fatal_native_error_is_not_split_or_serial_retried(monkeypatch, failure):
    request = _request()
    registry, groups = _registry(request.definition), source_groups(request, (2,))
    projector = Mock(side_effect=failure)
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: projector)
    monkeypatch.setattr(graph_payloads, "project_batch", projector)
    with pytest.raises(type(failure)):
        sets._child_arguments(request, registry, groups)
    projector.assert_called_once()


def test_layout_eligibility_error_does_not_mask_earlier_child_corruption(monkeypatch):
    request = _request()
    registry, groups = _registry(request.definition), source_groups(request, (2,))
    groups[0].children[0].payload_sha256 = b"x" * 32
    with pytest.raises(CandidateRunnerError) as expected:
        serial_arguments(monkeypatch, request, registry, groups)
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: object())
    monkeypatch.setattr(graph_payloads, "_projection_layout", Mock(side_effect=ValueError("later layout error")))
    with pytest.raises(type(expected.value)) as actual:
        sets._child_arguments(request, registry, groups)
    assert str(actual.value) == str(expected.value)


def test_compiled_caller_columns_equal_independent_serial_write_arguments(monkeypatch):
    projector = graph_payloads.native_projector()
    if projector is None:
        pytest.skip("compiled graph projection capability is not installed")
    request = _request(page_row_limit=256, page_byte_limit=4_194_304)
    registry, groups = _registry(request.definition), source_groups(request, (20, 20))
    expected = serial_arguments(monkeypatch, request, registry, groups)
    calls = Mock(wraps=projector)
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: calls)
    assert sets._child_arguments(request, registry, groups) == expected
    calls.assert_called_once()


def test_compiled_logical_pages_reserve_prior_columns_before_native_dispatch(monkeypatch):
    projector = graph_payloads.native_projector()
    if projector is None:
        pytest.skip("compiled graph projection capability is not installed")
    request, registry, group = _wide_context_group()
    request = replace(request, page_row_limit=256, page_byte_limit=4_194_304)
    group.state.current.attached_child_count = 0
    inputs, states = (group.state.source,), (group.state,)
    database = CallerDatabase(monkeypatch, request, [(inputs[0], group.children)])

    def read_pages():
        with database.read_transaction(database.session, request, 7):
            return sets._read_child_batch(database.session, request, registry, inputs, states, "source", 25, 64)

    with monkeypatch.context() as serial:
        serial.setattr(graph_payloads, "native_projector", lambda: None)
        expected = read_pages()
    native_calls = Mock(wraps=projector)
    decoder_calls = Mock(wraps=importlib.import_module("ptg2_address_canon").custom_import_graph_children_v1)
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: native_calls)
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: decoder_calls)
    source_columns = sets._source_columns
    retained_charges, dispatches_before_pages = [], []

    def prepare(_request, _registry, groups, *, retained_bytes):
        retained_charges.append(retained_bytes)
        dispatches_before_pages.append(native_calls.call_count)
        return source_columns(_request, _registry, groups, retained_bytes=retained_bytes)

    monkeypatch.setattr(sets, "_source_columns", prepare)
    actual = read_pages()
    assert actual[1:] == expected[1:]
    assert len(retained_charges) == 3
    assert retained_charges[0] == 0 < retained_charges[1] < 4_194_304 <= retained_charges[2]
    assert 0 < native_calls.call_count == dispatches_before_pages[2]
    decoder_calls.assert_not_called()
