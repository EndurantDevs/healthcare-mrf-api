# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Observe genuine Reader business transactions without assigning setup snapshots."""

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tests import test_registry_provider_profile_transport_postgres as transport
from tests.test_custom_import_managed_detail_postgres import _assert_collection_reader


def _observer(monkeypatch):
    callbacks = []

    def listen(engine, event):
        assert event == "after_cursor_execute"

        def register(callback):
            callbacks.append(callback)
            return callback

        return register

    monkeypatch.setattr(transport.event, "listens_for", listen)
    engine = SimpleNamespace(sync_engine=object())
    verified_by_field = {"value": False}
    database = SimpleNamespace(
        engine=engine,
        _reader_database=SimpleNamespace(engine=engine),
        has_reader_session=lambda: verified_by_field["value"],
    )
    statements, transactions = [], []
    setup = transport._observe_reader_transactions(
        database, SimpleNamespace(roles={"owner": "synthetic_owner"}), statements, transactions
    )
    database.engine = None
    probe = Mock()
    probe.fetchone.return_value = (
        "synthetic_reader",
        "synthetic_reader",
        "on",
        "repeatable read",
        123,
        False,
        False,
        "8/2",
    )
    connection = SimpleNamespace(connection=SimpleNamespace(cursor=Mock(return_value=probe)))
    return SimpleNamespace(
        verified=verified_by_field,
        database=database,
        statements=statements,
        transactions=transactions,
        setup=setup,
        probe=probe,
        connection=connection,
        callback=callbacks[0],
        role="synthetic_reader",
        writer_statements=[],
    )


def _observe(case, statement):
    case.callback(case.connection, None, statement, None, None, False)


def test_reader_observer_never_assigns_a_setup_snapshot(monkeypatch):
    case = _observer(monkeypatch)
    statements = [
        "SET LOCAL statement_timeout = '5s'",
        "SELECT name, c.oid FROM pg_class c",
        'LOCK TABLE "mrf"."provider_directory_profile" IN ACCESS SHARE MODE',
    ]
    for statement in statements:
        _observe(case, statement)
    case.connection.connection.cursor.assert_not_called()
    assert case.setup == statements
    assert case.statements == case.transactions == []
    case.verified["value"] = True
    _observe(case, "SELECT * FROM mrf.provider_directory_profile")
    _observe(case, "SELECT * FROM mrf.provider_directory_profile_evidence")
    assert len(case.statements) == len(case.transactions) == 2
    assert case.probe.execute.call_count == case.probe.close.call_count == 2
    _assert_collection_reader(case)


@pytest.mark.parametrize("transaction", [None, "8/3"])
def test_reader_observer_keeps_missing_or_changed_business_transaction(monkeypatch, transaction):
    case = _observer(monkeypatch)
    case.verified["value"] = True
    _observe(case, "SELECT * FROM mrf.provider_directory_profile")
    case.probe.fetchone.return_value = (*case.probe.fetchone.return_value[:-1], transaction)
    _observe(case, "SELECT * FROM mrf.provider_directory_profile_evidence")
    assert case.transactions == ["8/2", transaction]
    with pytest.raises(AssertionError):
        _assert_collection_reader(case)


@pytest.mark.parametrize("failure", [RuntimeError("probe failed"), asyncio.CancelledError()])
def test_reader_observer_preserves_probe_failure_and_closes_cursor(monkeypatch, failure):
    case = _observer(monkeypatch)
    case.verified["value"] = True
    case.probe.execute.side_effect = failure
    with pytest.raises(type(failure)) as caught:
        _observe(case, "SELECT * FROM mrf.provider_directory_profile")
    assert caught.value is failure
    case.probe.close.assert_called_once()
    assert case.statements == case.transactions == []
