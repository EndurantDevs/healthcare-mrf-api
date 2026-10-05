# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Frozen physical scans retain bounded metadata and exact keyset cursors."""

from unittest.mock import Mock

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from db.models.custom_import import CustomImportRootRevision
from process.custom_import import build_graph as graph
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_graph import _read_session, _request


def _query():
    root = CustomImportRootRevision
    return select(root.root_revision_id, root.canonical_payload), (root.root_revision_id,), (root,)


def test_physical_byte_cutoff_preserves_range_cursor(monkeypatch):
    statement, keys, models = _query()
    metadata_bytes = graph._physical_key_bytes(keys)
    fixed_bytes = metadata_bytes + 64 + 16 * (len(statement.selected_columns) + 1)
    reserve_bytes = 64
    monkeypatch.setattr(graph, "MAX_BATCH_BYTES", reserve_bytes + 6 * metadata_bytes + 2 * (1000 + fixed_bytes))
    session = _read_session(
        monkeypatch,
        [
            [(1, 1000), (2, 1000), (3, 1000)],
            [(1, "one", 1, 2), (2, "two", 2, 2)],
            [(3, 1000), (4, 1000)],
            [(3, "three", 3, 1)],
            [(4, 1000)],
            [(4, "four", 4, 1)],
            [],
        ],
    )
    factory = Mock(return_value=(statement, keys, models))
    page_records = list(
        graph._read_query_pages(
            session,
            _request(page_row_limit=1, page_byte_limit=1000),
            7,
            factory,
            after=(0,),
            bounds=graph._ReadPage(physical=True, reserve_bytes=reserve_bytes),
        )
    )
    assert page_records == [(1, "one"), (2, "two"), (3, "three"), (4, "four")]
    assert factory.call_count == 4
    statements = [call.args[0].compile(dialect=postgresql.dialect()) for call in session.execute.call_args_list]
    for index, lower, upper in ((1, 0, 2), (3, 2, 3), (5, 3, 4)):
        assert " > " in str(statements[index]) and " <= " in str(statements[index])
        assert lower in statements[index].params.values() and upper in statements[index].params.values()
        assert " IN " not in str(statements[index]) and "POSTCOMPILE" not in str(statements[index])
        assert "count(*)" in str(statements[index]) and str(statements[index]).count("FROM ") == 2
    assert session.execute.call_count == 7


def test_physical_row_limit_is_independent_of_logical_pages(monkeypatch):
    count = (graph.MAX_BATCH_ROWS - 1) // 2
    metadata = [(index, 1) for index in range(count)]
    payloads = [(index, "x", index, count) for index in range(count)]
    session = _read_session(monkeypatch, [metadata, payloads, [(count, 1)], [(count, "x", count, 1)], []])
    factory = Mock(return_value=_query())
    records = graph._read_query_pages(
        session, _request(page_row_limit=1), 7, factory, bounds=graph._ReadPage(physical=True)
    )
    assert sum(1 for _record in records) == count + 1
    assert factory.call_count == 3 and session.execute.call_count == 5
    statements = [call.args[0].compile(dialect=postgresql.dialect()) for call in session.execute.call_args_list]
    assert count in statements[0].params.values()
    assert all(len(statement.params) <= 3 for statement in statements[1::2])
    assert all("POSTCOMPILE" not in str(statement) for statement in statements)


@pytest.mark.parametrize("identities", [(1,), (1, 3), (2, 1), (1, 2, 3), (1, 1)])
def test_physical_payload_keys_must_match_metadata(monkeypatch, identities):
    session = _read_session(monkeypatch, [[(1, 1), (2, 1)], [(identity, "x", identity, 2) for identity in identities]])
    with pytest.raises(CandidateRunnerError, match="changed during its read"):
        list(
            graph._read_query_pages(
                session, _request(), 7, lambda _session: _query(), bounds=graph._ReadPage(physical=True)
            )
        )


def test_physical_prefix_count_rejects_unfetched_duplicates(monkeypatch):
    session = _read_session(monkeypatch, [[(1, 1), (2, 1)], [(1, "x", 1, 3), (2, "x", 2, 3)]])
    with pytest.raises(CandidateRunnerError, match="changed during its read"):
        next(
            graph._read_query_pages(
                session, _request(), 7, lambda _session: _query(), bounds=graph._ReadPage(physical=True)
            )
        )
    assert session.execute.call_args_list[1].args[0]._limit_clause.value == 2


@pytest.mark.parametrize("metadata", [[], [(1, 1)]])
def test_physical_reserved_envelope_allows_only_empty_eof(monkeypatch, metadata):
    session = _read_session(monkeypatch, [metadata])
    records = graph._read_query_pages(
        session,
        _request(),
        7,
        lambda _session: _query(),
        bounds=graph._ReadPage(physical=True, reserve_bytes=graph.MAX_BATCH_BYTES),
    )
    if metadata:
        with pytest.raises(CandidateRunnerError, match="one build record exceeds"):
            list(records)
    else:
        assert list(records) == []
    assert session.execute.call_count == 1


def test_physical_explicit_variable_columns_preserve_row_shape(monkeypatch):
    root = CustomImportRootRevision
    session = _read_session(monkeypatch, [[(1, 3)], [(1, "one", 1, 1)], []])
    records = graph._read_query_pages(
        session,
        _request(),
        7,
        lambda _session: _query(),
        bounds=graph._ReadPage(physical=True, variable_columns=(root.canonical_payload,)),
    )
    assert list(records) == [(1, "one")]
    statement = str(session.execute.call_args_list[0].args[0])
    assert "canonical_payload)" in statement
    assert "payload_sha256" not in statement and "producing_token_sha256" not in statement
