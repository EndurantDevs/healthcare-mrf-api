"""Receipt source admission precedes member controls and durable writes."""

from __future__ import annotations

import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql

from process import ptg_wave_ordinary_terminal_receipt as terminal
from process.ptg_parts.ptg2_lifecycle_lock import PTG2LifecycleLockDeferred
from process.ptg_parts.source_snapshot_reference_lifetime import SnapshotReferenceIdentityError
from tests.ptg_blank_terminal_support import blank_ordinary_result
from tests.ptg_wave_ordinary_terminal_receipt_support import (
    ISSUED_AT,
    OPERATION_ID,
    QueuedTerminalSession,
    ScalarResult,
    TerminalTransaction,
    keyring,
    ordinary_result,
)


class _Session(QueuedTerminalSession):
    async def execute(self, statement, params=None):
        if str(statement) == "SET TRANSACTION READ ONLY":
            self.statements.append((statement, params))
            return ScalarResult(None)
        return await super().execute(statement, params)


def _rows(state):
    return [state[field] for field in ("wave", "intent", "quarantine", "run", "engine_run", "engine_snapshot")]


def _bind(monkeypatch, *sessions):
    engine = SimpleNamespace()
    engine.engine = engine
    monkeypatch.setattr(terminal.db, "engine", SimpleNamespace(sync_engine=engine))
    for session in sessions:
        session.get_bind = lambda: engine
    return engine


@pytest.mark.asyncio
@pytest.mark.parametrize("factory", (ordinary_result, blank_ordinary_result))
async def test_preflight_is_independent_readonly_unlocked_and_does_not_project_metrics(monkeypatch, factory):
    state = factory(monkeypatch)
    state["run"].metrics = {"function": "ptg_control_start", **state["run"].metrics}
    if state["run"].status == "failed":
        state["run"].metrics = {"function": "ptg_control_start"}
    metrics_before = copy.deepcopy(state["run"].metrics)
    session = _Session(_rows(state))
    _bind(monkeypatch, session)
    monkeypatch.setattr(terminal.db, "session_factory", lambda: TerminalTransaction(session))
    monkeypatch.setattr(terminal.db, "session", lambda: pytest.fail("must not borrow a bound session"))
    monkeypatch.setattr(terminal.db, "transaction", lambda: pytest.fail("preflight must not use writer controls"))

    reference = await terminal._preflight_terminal_snapshot_reference(state["request"], keyring(monkeypatch))

    assert reference == terminal._terminal_snapshot_reference(state)
    assert state["run"].metrics == metrics_before
    assert session.flush_count == 0
    assert session.added == []
    sql_statements = [str(statement.compile(dialect=postgresql.dialect())) for statement, _ in session.statements]
    assert sql_statements[0] == "SET TRANSACTION READ ONLY"
    assert all("FOR UPDATE" not in query and "pg_advisory" not in query for query in sql_statements)
    assert session.statements[1][1] == {"lock_timeout": "250ms", "statement_timeout": "5s"}


@pytest.mark.asyncio
async def test_source_fence_precedes_member_and_row_locks(monkeypatch):
    state = ordinary_result(monkeypatch)
    reference = terminal._terminal_snapshot_reference(state)
    session = _Session([None, *_rows(state)])
    _bind(monkeypatch, session)
    monkeypatch.setattr(terminal.db, "transaction", lambda: TerminalTransaction(session))
    monkeypatch.setattr(terminal, "_preflight_terminal_snapshot_reference", AsyncMock(return_value=reference))

    async def acquire_lifetime(writer, **coordinates):
        assert writer is session
        assert len(session.statements) == 1
        assert "set_config" in str(session.statements[0][0])
        assert coordinates == {
            "schema_name": terminal.resolve_ptg2_schema(),
            "snapshot_id": reference.snapshot_id,
            "internal_run_id": reference.internal_run_id,
            "lock_timeout": "250ms",
            "statement_timeout": "5s",
        }
        return reference.source_key

    monkeypatch.setattr(terminal, "acquire_snapshot_reference_lifetime", acquire_lifetime)
    release = AsyncMock(return_value=1)
    monkeypatch.setattr(terminal, "_release_terminal_snapshot_pin", release)

    receipt, created = await terminal.issue_ordinary_terminal_receipt(
        OPERATION_ID, state["request"], receipt_keyring=keyring(monkeypatch), receipt_issued_at=ISSUED_AT
    )

    assert created is True
    assert receipt["payload"]["snapshot_id"] == reference.snapshot_id
    assert "pg_advisory_xact_lock" in str(session.statements[1][0])
    assert len(session.added) == 1
    locked_reads = [
        statement for statement, _ in session.statements if getattr(statement, "_for_update_arg", None) is not None
    ]
    assert len(locked_reads) == 4
    assert all(statement.get_execution_options()["populate_existing"] for statement in locked_reads)
    assert release.await_args.args == (session,)


@pytest.mark.asyncio
async def test_two_phase_issuance_projects_blank_metrics_only_in_writer(monkeypatch):
    state = blank_ordinary_result(monkeypatch)
    state["run"].metrics = {"function": "ptg_control_start"}
    writer_state = copy.deepcopy(state)
    preflight = _Session(_rows(state))
    writer = _Session([None, *_rows(writer_state)])
    _bind(monkeypatch, preflight, writer)
    monkeypatch.setattr(terminal.db, "session_factory", lambda: TerminalTransaction(preflight))
    monkeypatch.setattr(terminal.db, "transaction", lambda: TerminalTransaction(writer))

    async def acquire_lifetime(session, **_coordinates):
        assert session is writer
        assert not preflight.scalar_results
        assert preflight.flush_count == 0
        assert state["run"].metrics == {"function": "ptg_control_start"}
        return state["engine_run"].options["source_key"]

    monkeypatch.setattr(terminal, "acquire_snapshot_reference_lifetime", acquire_lifetime)
    monkeypatch.setattr(terminal, "_release_terminal_snapshot_pin", AsyncMock(return_value=1))

    receipt, created = await terminal.issue_ordinary_terminal_receipt(
        OPERATION_ID, state["request"], receipt_keyring=keyring(monkeypatch), receipt_issued_at=ISSUED_AT
    )

    assert created is True
    assert receipt["payload"]["terminal_result"]["status"] == "blank"
    assert writer_state["run"].metrics["status"] == "blank"
    assert writer.flush_count == 2
    assert preflight.flush_count == 0
    assert state["run"].metrics == {"function": "ptg_control_start"}


@pytest.mark.parametrize(
    "change", ("missing-engine", "missing-snapshot", "wrong-run", "noncanonical-source", "source-type")
)
def test_reference_requires_exact_canonical_engine_snapshot_metadata(monkeypatch, change):
    state = ordinary_result(monkeypatch)
    if change == "missing-engine":
        state["engine_run"] = None
    elif change == "missing-snapshot":
        state["engine_snapshot"] = None
    elif change == "wrong-run":
        state["engine_snapshot"].import_run_id = "other-run"
    else:
        state["engine_run"].options["source_key"] = "Source-Key" if change == "noncanonical-source" else 123
    with pytest.raises(terminal.PTGWaveOrdinaryTerminalConflict, match="identity is unavailable"):
        terminal._terminal_snapshot_reference(state)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ("snapshot", "run", "source"))
async def test_locked_reload_refuses_changed_identity_before_blank_projection(monkeypatch, changed):
    state = blank_ordinary_result(monkeypatch)
    reference = terminal._terminal_snapshot_reference(state)
    state["run"].metrics = {"function": "ptg_control_start"}
    if changed == "snapshot":
        state["engine_snapshot"].snapshot_id = "other-snapshot"
    elif changed == "run":
        state["engine_run"].import_run_id = "other-run"
        state["engine_snapshot"].import_run_id = "other-run"
    else:
        state["engine_run"].options = {**state["engine_run"].options, "source_key": "other_source"}
    session = _Session(_rows(state))
    monkeypatch.setattr(
        terminal, "_project_blank_metrics", lambda *_args: pytest.fail("drift must fail before projection")
    )

    with pytest.raises(terminal.PTGWaveOrdinaryTerminalConflict, match="identity changed"):
        await terminal._load_terminal_snapshot(session, state["request"], expected_reference=reference)

    assert session.flush_count == 0
    assert session.added == []
    assert state["run"].metrics == {"function": "ptg_control_start"}


@pytest.mark.asyncio
async def test_source_drift_refuses_before_member_controls(monkeypatch):
    state = ordinary_result(monkeypatch)
    reference = terminal._terminal_snapshot_reference(state)
    session = _Session([])
    _bind(monkeypatch, session)
    monkeypatch.setattr(terminal.db, "transaction", lambda: TerminalTransaction(session))
    monkeypatch.setattr(terminal, "_preflight_terminal_snapshot_reference", AsyncMock(return_value=reference))
    monkeypatch.setattr(terminal, "acquire_snapshot_reference_lifetime", AsyncMock(return_value="other_source"))

    with pytest.raises(terminal.PTGWaveOrdinaryTerminalConflict, match="source changed"):
        await terminal.issue_ordinary_terminal_receipt(
            OPERATION_ID, state["request"], receipt_keyring=keyring(monkeypatch)
        )

    assert len(session.statements) == 1
    assert session.flush_count == 0
    assert session.added == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected"),
    (
        (PTG2LifecycleLockDeferred("busy"), terminal.PTGWaveOrdinaryTerminalRetryable),
        (asyncio.CancelledError(), asyncio.CancelledError),
        (SnapshotReferenceIdentityError("changed"), terminal.PTGWaveOrdinaryTerminalConflict),
    ),
)
async def test_failed_source_admission_never_takes_member_controls(monkeypatch, failure, expected):
    state = ordinary_result(monkeypatch)
    reference = terminal._terminal_snapshot_reference(state)
    session = _Session([])
    _bind(monkeypatch, session)
    exits = []

    class Scope(TerminalTransaction):
        async def __aexit__(self, exc_type, exc, traceback):
            exits.append(exc)
            return False

    monkeypatch.setattr(terminal.db, "transaction", lambda: Scope(session))
    monkeypatch.setattr(terminal, "_preflight_terminal_snapshot_reference", AsyncMock(return_value=reference))
    monkeypatch.setattr(terminal, "acquire_snapshot_reference_lifetime", AsyncMock(side_effect=failure))
    with pytest.raises(expected):
        await terminal.issue_ordinary_terminal_receipt(
            OPERATION_ID, state["request"], receipt_keyring=keyring(monkeypatch)
        )

    assert exits == [failure]
    assert len(session.statements) == 1
    assert session.flush_count == 0
    assert session.added == []


def test_other_engine_binding_is_rejected(monkeypatch):
    session = _Session([])
    _bind(monkeypatch, session)
    other_engine = SimpleNamespace()
    other_engine.engine = other_engine
    session.get_bind = lambda: other_engine
    with pytest.raises(terminal.PTGWaveOrdinaryTerminalConflict, match="database binding changed"):
        terminal._require_receipt_database_engine(session)


@pytest.mark.asyncio
async def test_terminal_pin_release_preserves_receipt_timeout_budgets(monkeypatch):
    state = ordinary_result(monkeypatch)
    delete = AsyncMock(return_value=1)
    monkeypatch.setattr(terminal, "delete_ordinary_terminal_snapshot_pin", delete)
    session = object()

    assert (
        await terminal._release_terminal_snapshot_pin(
            session, request=state["request"], payload={"snapshot_id": state["engine_snapshot"].snapshot_id}
        )
        == 1
    )

    delete.assert_awaited_once_with(
        session,
        schema_name=terminal.resolve_ptg2_schema(),
        operation_id=OPERATION_ID,
        member_ordinal=0,
        snapshot_id=state["engine_snapshot"].snapshot_id,
        lock_timeout="250ms",
        statement_timeout="5s",
    )
