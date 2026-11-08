# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical winner cursors retain bounded transfer and live build authority."""

from __future__ import annotations

from collections import deque
from contextlib import closing, contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import asyncpg

from process.custom_import import build_graph as graph
from process.custom_import import compact_materialization as compact
from process.custom_import import publication
from process.custom_import.runner_types import CancellationRequested, CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.storage_layout import snapshot_models
from tests.test_custom_import_build_graph import _request
from tests.test_custom_import_build_output import _generation


def _winner(number):
    digest = number.to_bytes(32, "big")
    return (1, 100 - number, digest, 200 - number, 0, None, "npi", digest, "1234567890", digest, digest, b"", digest)


def _cursor_session(monkeypatch, pages, *, fail_authority=None, failure=CancellationRequested):
    """Expose transaction, authority and fetch order without a database connection."""
    events, deadlines, authority_calls = [], [], []
    pages = iter(pages)

    def fetch(limit):
        events.append("fetch")
        return next(pages)

    @contextmanager
    def transaction():
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    def authority(*_args):
        events.append("authority")
        authority_calls.append(len(authority_calls) + 1)
        if authority_calls[-1] == fail_authority:
            raise failure("synthetic authority failure")
        return object(), authority_calls[-1]

    def binding(*_args):
        events.append("binding")
        return snapshot_models(41), None

    cursor = SimpleNamespace(fetchmany=Mock(side_effect=fetch), close=Mock(side_effect=lambda: events.append("close")))
    session = SimpleNamespace(
        begin=transaction,
        execute=Mock(return_value=cursor),
        expunge_all=Mock(side_effect=lambda: events.append("expunge")),
    )
    monkeypatch.setattr(graph, "_read_authority", authority)
    monkeypatch.setattr(graph, "_build_storage_models", binding)
    monkeypatch.setattr(graph, "_prepare_read", lambda *_args: events.append("prepare"))
    monkeypatch.setattr(graph, "_require_budget", deadlines.append)
    return session, cursor, events, deadlines


def test_winner_cursor_preserves_order_and_exact_eof(monkeypatch):
    rows = [_winner(1), _winner(2)]
    session, cursor, events, deadlines = _cursor_session(monkeypatch, [[rows[0]], [rows[1]], []])
    request = _request(page_row_limit=2)
    assert list(compact._winner_rows(session, request, 7, _generation(request))) == rows
    assert [call.args for call in cursor.fetchmany.call_args_list] == [(2,), (2,), (2,)]
    for index, event in enumerate(events):
        if event == "fetch":
            assert events[index - 3 : index] == ["authority", "binding", "prepare"]
    statement = session.execute.call_args.args[0]
    original, keys, _models = compact._winner_query(snapshot_models(41), _generation(request))
    assert str(statement.compile(dialect=postgresql.dialect())) == str(
        original.order_by(*keys).compile(dialect=postgresql.dialect())
    )
    assert statement.get_execution_options() == {"stream_results": True, "yield_per": 2, "max_row_buffer": 2}
    session.execute.assert_called_once()
    cursor.close.assert_called_once()
    assert events[-3:] == ["close", "expunge", "commit"]
    assert deadlines[-1] == 5


@pytest.mark.parametrize("failure", [CancellationRequested, LeaseAuthorityLost])
@pytest.mark.parametrize("authority_call,fetches", [(2, 0), (3, 0), (4, 1)])
def test_winner_cursor_checks_live_authority_before_each_fetch(monkeypatch, failure, authority_call, fetches):
    session, cursor, events, _deadlines = _cursor_session(
        monkeypatch, [[_winner(1)], []], fail_authority=authority_call, failure=failure
    )
    request = _request()
    with pytest.raises(failure, match="synthetic authority"):
        list(compact._winner_rows(session, request, 7, _generation(request)))
    assert cursor.fetchmany.call_count == fetches
    if authority_call == 2:
        session.execute.assert_not_called()
        cursor.close.assert_not_called()
    else:
        cursor.close.assert_called_once()
        assert events[-2:] == ["close", "rollback"]
    assert events[-1] == "rollback"


@pytest.mark.parametrize("numbers", [(2, 1), (1, 1)])
def test_winner_cursor_rejects_regression_or_duplicate_across_batches(monkeypatch, numbers):
    session, cursor, events, _deadlines = _cursor_session(monkeypatch, [[_winner(number)] for number in numbers])
    request = _request(page_row_limit=1)
    with pytest.raises(CandidateRunnerError, match="canonical order"):
        list(compact._winner_rows(session, request, 7, _generation(request)))
    cursor.close.assert_called_once()
    assert events[-1] == "rollback"


@pytest.mark.parametrize("column,value", [(6, "é" * 127), (7, b"x" * 33), (8, "é" * 1025)])
def test_winner_cursor_checks_actual_utf8_and_binary_widths(monkeypatch, column, value):
    winner_values = list(_winner(1))
    winner_values[column] = value
    session, cursor, _events, _deadlines = _cursor_session(monkeypatch, [[tuple(winner_values)]])
    request = _request()
    with pytest.raises(CandidateRunnerError, match="bounded column width"):
        list(compact._winner_rows(session, request, 7, _generation(request)))
    cursor.close.assert_called_once()


def test_winner_cursor_rejects_an_oversized_batch(monkeypatch):
    session, cursor, _events, _deadlines = _cursor_session(monkeypatch, [[_winner(1), _winner(2)]])
    request = _request(page_row_limit=1)
    with pytest.raises(CandidateRunnerError, match="physical page"):
        list(compact._winner_rows(session, request, 7, _generation(request)))
    cursor.close.assert_called_once()


@pytest.mark.parametrize("exit_kind", ["consumer", "fetch", "deadline"])
def test_winner_cursor_closes_before_transaction_exit_on_failure(monkeypatch, exit_kind):
    session, cursor, events, _deadlines = _cursor_session(monkeypatch, [[_winner(1)], []])
    request = _request()
    if exit_kind == "fetch":
        cursor.fetchmany.side_effect = RuntimeError("synthetic fetch failure")
    with pytest.raises(RuntimeError, match="synthetic"):
        with closing(compact._winner_rows(session, request, 7, _generation(request))) as rows:
            next(rows)
            if exit_kind == "deadline":
                monkeypatch.setattr(graph, "_require_budget", Mock(side_effect=RuntimeError("synthetic expiry")))
                next(rows)
            raise RuntimeError("synthetic consumer failure")
    cursor.close.assert_called_once()
    assert events[-2:] == ["close", "rollback"]


def test_winner_cursor_rechecks_snapshot_binding_before_fetch(monkeypatch):
    session, cursor, events, _deadlines = _cursor_session(monkeypatch, [[_winner(1)]])
    monkeypatch.setattr(
        graph,
        "_build_storage_models",
        Mock(side_effect=[(snapshot_models(41), None), (snapshot_models(41), None), (snapshot_models(42), None)]),
    )
    request = _request()
    with pytest.raises(CandidateRunnerError, match="snapshot changed"):
        list(compact._winner_rows(session, request, 7, _generation(request)))
    cursor.fetchmany.assert_not_called()
    cursor.close.assert_called_once()
    assert events[-1] == "rollback"


def test_winner_cursor_eof_does_not_replace_exact_population_check(monkeypatch):
    session, cursor, _events, _deadlines = _cursor_session(monkeypatch, [[_winner(1)], []])
    monkeypatch.setattr(compact, "_compact_rows", lambda *_args: iter(()))
    request = _request()
    digests = (publication._new_digest("test-content"), publication._new_digest("test-effective"))
    with pytest.raises(CandidateRunnerError, match="exact structural counts"):
        compact._summaries(
            session,
            request,
            7,
            _generation(request),
            SimpleNamespace(family_count=0, winner_count=2),
            digests,
            b"s" * 32,
        )
    cursor.close.assert_called_once()


def test_summary_hash_failure_closes_cursor_before_rollback(monkeypatch):
    session, cursor, events, _deadlines = _cursor_session(monkeypatch, [[_winner(1)]])
    monkeypatch.setattr(compact, "_compact_rows", lambda *_args: iter(()))
    monkeypatch.setattr(
        publication, "_add_digest_record_to_all", Mock(side_effect=RuntimeError("synthetic hash failure"))
    )
    request = _request()
    with pytest.raises(RuntimeError, match="synthetic hash"):
        compact._summaries(
            session, request, 7, _generation(request), SimpleNamespace(family_count=0, winner_count=1), (), b"s" * 32
        )
    cursor.close.assert_called_once()
    assert events[-2:] == ["close", "rollback"]


def test_tiny_byte_policy_keeps_metadata_first_admission(monkeypatch):
    session, cursor, _events, _deadlines = _cursor_session(monkeypatch, [])
    request = _request(page_byte_limit=256)
    fallback = Mock(return_value=iter([_winner(1)]))
    monkeypatch.setattr(compact, "_compact_rows", fallback)
    assert list(compact._winner_rows(session, request, 7, _generation(request))) == [_winner(1)]
    fallback.assert_called_once()
    session.execute.assert_not_called()
    cursor.fetchmany.assert_not_called()


def test_cursor_budget_reserves_prefetch_samples_carry_and_two_batch_copies(monkeypatch):
    request = _request(page_row_limit=256)
    statement, _keys, _models = compact._winner_query(snapshot_models(41), _generation(request))
    row_bytes = graph._physical_key_bytes(tuple(statement.selected_columns))
    retained = 50 + compact.MAX_SELECTION_PROFILES * compact.WINNER_CANDIDATES + 2
    monkeypatch.setattr(graph, "MAX_BATCH_BYTES", (retained + 6) * row_bytes)
    monkeypatch.setattr(graph, "MAX_BATCH_ROWS", retained + 8)
    limit, widths = compact._winner_cursor_bounds(statement, request)
    assert limit == 3
    assert widths[6] == 63 * 4 and widths[8] == 512 * 4
    assert (retained + 2 * limit) * row_bytes <= graph.MAX_BATCH_BYTES
    assert retained + 2 * limit <= graph.MAX_BATCH_ROWS
    monkeypatch.setattr(graph, "MAX_BATCH_BYTES", (retained + 1) * row_bytes)
    with pytest.raises(CandidateRunnerError, match="physical page budget"):
        compact._winner_cursor_bounds(statement, request)


def test_supported_asyncpg_cursor_prefetch_fits_reserved_rows(monkeypatch):
    cursor = object.__new__(asyncpg.AsyncAdapt_asyncpg_ss_cursor)
    cursor._cursor = SimpleNamespace(fetch=Mock(side_effect=lambda count: list(range(count))))
    cursor._rowbuffer = deque()
    monkeypatch.setattr(asyncpg, "await_", lambda result: result)
    assert cursor.fetchmany(1) == [0]
    cursor._cursor.fetch.assert_called_once_with(50)
    assert len(cursor._rowbuffer) == 49
