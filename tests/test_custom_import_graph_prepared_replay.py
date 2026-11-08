# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical preparation reuse preserves protected write arguments and failure order."""

from collections import deque
from dataclasses import replace
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from process.custom_import import build_graph as graph
from process.custom_import import build_graph_prepare_page as prepare
from process.custom_import import build_graph_prepared as scratch
from process.custom_import import build_graph_sets as sets
from process.custom_import import graph_payloads
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_graph import _registry, _request
from tests.test_custom_import_build_graph_sets import CallerDatabase, family_input
from tests.test_custom_import_definition import _rate, _raw_definition
from tests.test_custom_import_graph_payloads import _definition, _group
from tests.test_custom_import_graph_projection_pages import source_groups


def prepare_groups(request, registry, groups, stream):
    for group in groups:
        source = group.state.source
        child_rows = ((source.plan.root_record_id, "rates", child, True) for child in group.children)
        actual = prepare._source_input_with_digest(
            request,
            registry,
            source,
            iter((((source.plan.root_record_id, "rates"), child_rows),)),
            deque(),
            physical=True,
            prepared=stream,
        )
        assert (actual.family_sha256, actual.child_count) == (source.family_sha256, source.child_count)
    stream.finish()


def replay_groups(groups, stream):
    return tuple(
        replace(
            group,
            children=tuple(
                row[1]
                for row in stream.rows(
                    tuple(
                        (
                            group.state.current.root_record_id,
                            "rates",
                            bytes(child.child_key_sha256),
                            child.child_revision_id,
                            graph._model_bytes((child,)),
                        )
                        for child in group.children
                    )
                )
            ),
        )
        for group in groups
    )


def scope_metadata(database):
    original = database.execute

    def execute(statement):
        result_rows = original(statement).all()
        if statement._limit_clause is not None and "scope_valid" in statement.selected_columns:
            result_rows = [(*row, database.valid) for row in result_rows]
        return SimpleNamespace(all=lambda: result_rows)

    database.session.execute = execute


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("state", ["missing", "null", "value"])
def test_prepared_columns_keep_full_serial_arguments_and_decode_once(monkeypatch, native, state):
    decoder = graph_payloads.native_decoder()
    if native and decoder is None:
        pytest.skip("compiled graph decoder is not installed")
    request = _request(definition=_definition(), page_row_limit=256)
    values_by_field = _rate("0000000001", 'Ω😀"\\\n', Decimal("1.000000000001"))
    if state != "missing":
        values_by_field.update(
            {"note": "é\t", "visits": -(1 << 63), "enabled": False}
            if state == "value"
            else dict(note=None, visits=None, enabled=None)
        )
    group, registry = _group(request, [values_by_field]), _registry(request.definition)
    with monkeypatch.context() as serial:
        serial.setattr(graph_payloads, "native_projector", lambda: None)
        serial.setattr(graph_payloads, "native_decoder", lambda: None)
        expected = sets._child_arguments(request, registry, (group,))
    calls = Mock(wraps=decoder) if native else None
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: calls)
    stream = scratch.PreparedChildren()
    try:
        prepare_groups(request, registry, (group,), stream)
        child_records = [child_record for _, _, child_record in stream.records()]
        assert (child_records[0][6] is not None) == native
        if native:
            calls.assert_called_once()
            monkeypatch.setattr(sets, "_serial_child_projection", Mock(side_effect=AssertionError("second decode")))
        monkeypatch.setattr(graph_payloads, "decode_records", Mock(side_effect=AssertionError("second decode")))
        monkeypatch.setattr(graph_payloads, "project_batch", Mock(side_effect=AssertionError("second projection")))
        assert sets._child_arguments(request, registry, replay_groups((group,), stream)) == expected
    finally:
        stream.close()


def test_replay_metadata_preserves_original_byte_geometry_and_scope(monkeypatch):
    request = _request(page_row_limit=32, page_byte_limit=8192)
    registry, groups = _registry(request.definition), source_groups(request, (20, 20))
    for group in groups:
        group.state.current.attached_child_count = 0
    families = [(group.state.source, group.children) for group in groups]
    database = CallerDatabase(monkeypatch, request, families)
    scope_metadata(database)
    inputs, states = tuple(group.state.source for group in groups), tuple(group.state for group in groups)
    expected = sets._read_child_batch(database.session, request, registry, inputs, states, "source", (3, 40))
    original_reads = len(database.reads)
    stream = scratch.PreparedChildren()
    try:
        prepare_groups(request, registry, groups, stream)
        actual = sets._read_child_batch(
            database.session, request, registry, inputs, states, "source", (3, 40), prepared=stream
        )
        assert actual[1:] == expected[1:]
        assert len(database.reads) == original_reads + 1
        assert all(isinstance(child, scratch.PreparedChild) for group in actual[0] for child in group.children)
        assert all(sets._child_model_bytes(child) > 100 for group in actual[0] for child in group.children)
        database.valid = False
        with pytest.raises(CandidateRunnerError, match="identity or provenance"):
            sets._read_child_batch(
                database.session, request, registry, inputs, states, "source", (3, 40), prepared=stream
            )
        assert not database.calls
    finally:
        stream.close()


def test_later_canonical_failure_precedes_deferred_projection_error(monkeypatch):
    if graph_payloads.native_decoder() is None:
        pytest.skip("compiled graph decoder is not installed")
    request = _request(page_row_limit=256)
    registry, groups = _registry(request.definition), source_groups(request, (2,))
    contexts = Mock(side_effect=graph.material.WinnerMaterializationError("context rejected"))
    monkeypatch.setattr(graph, "_context_values", contexts)
    later = groups[0].children[-1]
    digest = later.payload_sha256
    later.payload_sha256 = b"x" * 32
    failed = scratch.PreparedChildren()
    try:
        with pytest.raises(CandidateRunnerError, match="payload or parent digest"):
            prepare_groups(request, registry, groups, failed)
        assert contexts.call_count == 1
    finally:
        failed.close()
    later.payload_sha256 = digest
    stream = scratch.PreparedChildren()
    try:
        prepare_groups(request, registry, groups, stream)
        assert all(record[6] is None for _, _, record in stream.records())
        with pytest.raises(graph.material.WinnerMaterializationError, match="context rejected"):
            sets._child_arguments(request, registry, replay_groups(groups, stream))
    finally:
        stream.close()


@pytest.mark.parametrize("failure", [MemoryError("allocation"), CandidateRunnerError("malformed native proof")])
def test_fatal_preparation_is_not_deferred(monkeypatch, failure):
    if graph_payloads.native_decoder() is None:
        pytest.skip("compiled graph decoder is not installed")
    request = _request()
    registry, groups = _registry(request.definition), source_groups(request, (1,))
    monkeypatch.setattr(graph_payloads, "projection_values", Mock(side_effect=failure))
    stream = scratch.PreparedChildren()
    try:
        with pytest.raises(type(failure), match=str(failure)):
            prepare_groups(request, registry, groups, stream)
        assert stream.row_count == 0
    finally:
        stream.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", [False, True])
async def test_prepared_partition_preserves_committed_writer_arguments(monkeypatch, retained):
    request = _request(page_row_limit=32)
    registry = _registry(request.definition)
    families = [family_input(request, root_id, retained=retained, count=31) for root_id in (1, 2)]
    inputs = tuple(source for source, _children in families)
    baseline = CallerDatabase(monkeypatch, request, families)
    await sets.consume_family_page(None, request, registry, inputs)
    expected_calls, expected_progress = baseline.calls, baseline.progress
    actual = CallerDatabase(monkeypatch, request, families)
    scope_metadata(actual)
    groups = source_groups(request, (31, 31))
    stream = scratch.PreparedChildren()
    try:
        if not retained:
            prepare_groups(request, registry, groups, stream)
        await sets.consume_family_page(None, request, registry, inputs, prepared=stream)
        assert actual.calls == expected_calls
        assert actual.progress == expected_progress
        assert actual.seen == baseline.seen
        assert len(actual.reads) == len(baseline.reads) // (1 if retained else 2)
    finally:
        stream.close()


@pytest.mark.asyncio
async def test_rebuilt_stream_resumes_only_after_committed_child_tip(monkeypatch):
    request = _request(page_row_limit=32)
    registry, groups = _registry(request.definition), source_groups(request, (35,))
    family_source, children = groups[0].state.source, groups[0].children
    database = CallerDatabase(monkeypatch, request, [(family_source, children)])
    scope_metadata(database)
    monkeypatch.setattr(sets, "MAX_BATCH_ROWS", 96)

    async def lose_acknowledgement(*args):
        result = await database.call(*args)
        if "roots_page" not in args[1]:
            database.fail_after_commit = ConnectionError("lost child acknowledgement")
        return result

    monkeypatch.setattr(graph, "_source_call", lose_acknowledgement)
    first = scratch.PreparedChildren()
    try:
        prepare_groups(request, registry, groups, first)
        with pytest.raises(ConnectionError, match="lost child acknowledgement"):
            await sets.consume_family_page(None, request, registry, (family_source,), prepared=first)
    finally:
        first.close()
    prefix = database.progress[1].attached_child_count
    assert 0 < prefix < len(children)
    assert database.seen == {child.child_revision_id for child in children[:prefix]}
    database.fail_after_commit = None
    monkeypatch.setattr(graph, "_source_call", database.call)
    rebuilt = scratch.PreparedChildren()
    try:
        prepare_groups(request, registry, groups, rebuilt)
        await sets.consume_family_page(None, request, registry, (family_source,), prepared=rebuilt)
        assert rebuilt.tips_verified
        assert database.progress[1].complete
        assert database.seen == {child.child_revision_id for child in children}
        written_ids = [
            child_id for name, arguments in database.calls if "roots_page" not in name for child_id in arguments[11][1]
        ]
        assert written_ids == [child.child_revision_id for child in children]
    finally:
        rebuilt.close()


def test_temporal_rows_replay_through_existing_typed_oracle(monkeypatch):
    raw = _raw_definition()
    raw["schema"]["children"][0]["fields"].append(
        {"id": "day", "slot": 6, "type": "date", "nullable": True, "projection_slot": 5}
    )
    request = _request(definition=CustomImportDefinition.from_mapping(raw), page_row_limit=256)
    registry = _registry(request.definition)
    groups = (_group(request, [_rate("0000000001") | {"day": date(2024, 2, 29)}]),)
    expected = sets._child_arguments(request, registry, groups)
    decoder = Mock(side_effect=AssertionError("unsupported native layout"))
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: decoder)
    stream = scratch.PreparedChildren()
    try:
        prepare_groups(request, registry, groups, stream)
        assert all(record[6] is None for _, _, record in stream.records())
        assert sets._child_arguments(request, registry, replay_groups(groups, stream)) == expected
        decoder.assert_not_called()
    finally:
        stream.close()


@pytest.mark.asyncio
async def test_partition_quota_fallback_keeps_writes_and_stops_preparation(monkeypatch):
    request = _request(page_row_limit=256)
    registry, groups = _registry(request.definition), source_groups(request, (70, 70))
    families = [(group.state.source, group.children) for group in groups]
    inputs = tuple(source for source, _children in families)
    baseline = CallerDatabase(monkeypatch, request, families)
    await sets.consume_family_page(None, request, registry, inputs)
    database = CallerDatabase(monkeypatch, request, families)
    columns = Mock(wraps=sets.prepare_child_columns)
    monkeypatch.setattr(sets, "prepare_child_columns", columns)
    monkeypatch.setattr(scratch, "MAX_BATCH_BYTES", 1)
    stream = scratch.PreparedChildren()
    try:
        prepare_groups(request, registry, groups, stream)
        assert not stream.enabled and stream.file is None and not stream.pending
        assert 0 < columns.call_count < sum(len(group.children) for group in groups)
        await sets.consume_family_page(None, request, registry, inputs, prepared=stream if stream.enabled else None)
        assert database.calls == baseline.calls
        assert database.seen == baseline.seen
        assert database.progress == baseline.progress
    finally:
        stream.close()
