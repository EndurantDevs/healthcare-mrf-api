# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""The operator page command preserves retained read fences and bounded output."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

import pytest
from sqlalchemy.sql.elements import TextClause

from process.custom_import import cli
from process.custom_import.rejection_evidence import MAX_PAGE_BYTES
from tests.test_custom_import_cli import _LifecycleDatabase
from tests.test_custom_import_rejection_evidence import _event, _retained, _Session

_ARGUMENTS = ["rejections", "--dataset-id", "3", "--execution-id", "17", "--build-id", "23"]


class _ReadDatabase(_LifecycleDatabase):
    def __init__(self, read_session, **options):
        super().__init__(self._fresh_session, **options)
        self.read_session = read_session

    @asynccontextmanager
    async def _fresh_session(self):
        self.events.append("session_open")
        try:
            yield self.read_session
        finally:
            self.events.append("session_close")

    def transaction(self):
        raise AssertionError("the rejection reader must own its read-only transaction")


@pytest.fixture
def retained_database(monkeypatch):
    definition, rows, collections = _retained()
    session = _Session([rows], collections, [_event(definition)])
    database = _ReadDatabase(session, emit_output=True)
    monkeypatch.setattr(cli, "db", database)
    return database


def _output(capsys):
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out.endswith("\n")
    assert len(captured.out[:-1].encode("utf-8")) <= MAX_PAGE_BYTES
    return json.loads(captured.out)


def test_command_owns_one_fresh_session_and_returns_only_checked_evidence(retained_database, capsys):
    assert cli.run_command(_ARGUMENTS) == 0
    page = _output(capsys)
    assert page["scope"]["build_id"] == "23" and page["scope"]["execution_id"] == "17"
    assert page["complete"] and page["returned_rejection_events"] == 1
    assert page["events"][0]["logical_key"] == {"npi": {"type": "string", "value": "0000000001"}}
    assert page["events"][0]["offending_value_status"] == "not_retained"
    assert "canonical_evidence" not in json.dumps(page)
    assert retained_database.events == ["connect", "session_open", "session_close", "disconnect"]
    assert retained_database.disconnected_echo is True
    assert not retained_database.read_session.in_transaction()
    assert str(retained_database.read_session.statements[0]) == (
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
    )


def test_digest_bound_pages_preserve_order_and_explicit_completion(retained_database, capsys):
    definition, _, _ = _retained()
    retained_database.read_session.events = [_event(definition, ordinal) for ordinal in range(3)]
    assert cli.run_command([*_ARGUMENTS, "--limit", "2"]) == 0
    first = _output(capsys)
    assert [event["ordinal"] for event in first["events"]] == ["0", "1"] and not first["complete"]
    continuation_arguments = ["--after-ordinal", first["next_after_ordinal"], "--scope-sha256", first["scope_sha256"]]
    assert cli.run_command([*_ARGUMENTS, *continuation_arguments, "--limit", "2"]) == 0
    second = _output(capsys)
    assert [event["ordinal"] for event in second["events"]] == ["2"]
    assert second["complete"] and second["next_after_ordinal"] is None
    assert cli.run_command([*_ARGUMENTS, "--after-ordinal", "2", "--scope-sha256", first["scope_sha256"]]) == 0
    empty = _output(capsys)
    assert empty["events"] == [] and empty["complete"]
    assert retained_database.events.count("session_open") == 3


def test_unicode_serialization_does_not_expand_the_reader_byte_budget(retained_database, capsys):
    definition, _, _ = _retained()
    retained_database.read_session.events = [_event(definition, value="é" * 20_000)]
    assert cli.run_command(_ARGUMENTS) == 0
    page = _output(capsys)
    assert page["events"][0]["logical_key"]["npi"]["value"] == "é" * 20_000


@pytest.mark.parametrize(
    "extra",
    [
        ["--limit", "0"],
        ["--limit", "101"],
        ["--limit", "01"],
        ["--after-ordinal", "-1"],
        ["--after-ordinal", "0"],
        ["--after-ordinal", str(2**63), "--scope-sha256", "a" * 64],
        ["--scope-sha256", "A" * 64],
        ["--scope-sha256", "synthetic-private-value"],
        ["--build-id", "0"],
        ["--build-id", str(2**63)],
        ["--include-sample"],
        ["--output", "synthetic-private-path"],
    ],
)
def test_invalid_selectors_fail_before_connect_without_echo(retained_database, capsys, extra):
    with pytest.raises(SystemExit) as error:
        cli.run_command([*_ARGUMENTS, *extra])
    assert error.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == '{"code":"invalid_arguments","status":"error"}\n'
    assert retained_database.events == []


@pytest.mark.parametrize("selector", ["--dataset-id", "--execution-id", "--build-id"])
def test_wrong_scope_fails_before_any_leaf_read(retained_database, capsys, selector):
    assert cli.run_command([*_ARGUMENTS, selector, "99"]) == 1
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == '{"code":"failed","status":"error"}\n'
    statements = retained_database.read_session.statements
    assert len(statements) == 3 and statements[-1].column_descriptions[0]["name"] == "CustomImportExecution"
    assert retained_database.events[-2:] == ["session_close", "disconnect"]


def test_nonterminal_and_scope_drift_never_emit_a_partial_page(retained_database, capsys):
    session = retained_database.read_session
    session.scope_rows[0][0].state = "running"
    assert cli.run_command(_ARGUMENTS) == 1
    assert capsys.readouterr().out == ""
    session.scope_rows[0][0].state = "failed"
    assert cli.run_command([*_ARGUMENTS, "--after-ordinal", "0", "--scope-sha256", "a" * 64]) == 1
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == '{"code":"failed","status":"error"}\n'
    projections = [statement for statement in session.statements if not isinstance(statement, TextClause)]
    assert all(statement.column_descriptions[0]["name"] == "CustomImportExecution" for statement in projections)


@pytest.mark.parametrize(
    "failure,exit_code,code",
    [
        (RuntimeError("synthetic-private-database-error"), 1, "failed"),
        (PermissionError("synthetic-private-grant-error"), 1, "failed"),
        (asyncio.CancelledError(), 130, "canceled"),
    ],
)
def test_read_failures_close_and_redact(retained_database, capsys, failure, exit_code, code):
    retained_database.read_session.failure = failure
    assert cli.run_command(_ARGUMENTS) == exit_code
    captured = capsys.readouterr()
    assert captured.out == "" and json.loads(captured.err) == {"code": code, "status": "error"}
    assert retained_database.events == ["connect", "session_open", "session_close", "disconnect"]
    assert not retained_database.read_session.active


def test_cleanup_failure_discards_an_otherwise_complete_page(retained_database, capsys):
    retained_database.disconnect_error = RuntimeError("synthetic-private-disconnect-error")
    assert cli.run_command(_ARGUMENTS) == 1
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == '{"code":"failed","status":"error"}\n'
    assert retained_database.events[-2:] == ["session_close", "disconnect"]


def test_help_exposes_page_selectors_without_opening_database(retained_database, capsys):
    with pytest.raises(SystemExit) as error:
        cli.run_command(["rejections", "--help"])
    assert error.value.code == 0
    captured = capsys.readouterr()
    assert captured.err == "" and "--scope-sha256" in captured.out and "--build-id" in captured.out
    assert retained_database.events == []
