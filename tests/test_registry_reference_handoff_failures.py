# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host failure contracts; catalog seams do not establish native publication authority."""

import importlib
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import control_lifecycle
from process import nucc_protected_control as control
from process import nucc_protected_source as source
from tests.test_nucc_protected_control import publication
from tests.test_nucc_protected_source import closed_marker, copying_marker, ready_input, source_run

nucc = importlib.import_module("process.nucc")
pytestmark = pytest.mark.asyncio


def _database(session, events):
    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield session
        finally:
            events.append("end")

    return SimpleNamespace(transaction=transaction)


def _ready_stage():
    marker = copying_marker()
    return replace(source.NuccSourceStage.from_wire(marker), ready=ready_input(marker))


async def test_wait_releases_each_read_before_polling_and_returns_exact_closed_handoff(monkeypatch):
    stage = _ready_stage()
    closed = closed_marker()
    pending = source_run(stage.marker, ready=stage.ready)
    completed = source_run(closed, ready=stage.ready, status="finalizing")
    completed["metrics"]["nucc_handoff"] = closed["handoff"]
    events = []
    session = object()
    state = AsyncMock(side_effect=[(pending, stage.marker), (completed, closed)])

    async def poll(seconds):
        assert seconds == source.POLL_SECONDS and events[-1] == "end"
        events.append("poll")

    monkeypatch.setattr(source, "_source_stage_state", state)
    monkeypatch.setattr(source.asyncio, "sleep", poll)
    result = await source.wait_nucc_source_handoff(_database(session, events), stage)
    assert result == closed["handoff"]
    assert events == ["begin", "end", "poll", "begin", "end"]
    assert state.await_args_list[0].args == (session, stage)
    assert all(call.kwargs == {"copying": False} for call in state.await_args_list)


@pytest.mark.parametrize("failure", ["uncommitted", "changed-ready", "copying", "marker", "status", "census"])
async def test_wait_refuses_uncommitted_changed_or_inconsistent_owner_handoff(monkeypatch, failure):
    stage = _ready_stage()
    marker = closed_marker()
    run = source_run(marker, ready=deepcopy(stage.ready), status="finalizing")
    run["metrics"]["nucc_handoff"] = deepcopy(marker["handoff"])
    message = "closed Source handoff differs"
    if failure == "uncommitted":
        marker = stage.marker
        run = source_run(marker)
        message = "ready transition did not commit"
    if failure == "changed-ready":
        run["metrics"]["nucc_source_ready"]["stage_oid"] += 1
        message = "committed ready input differs"
    if failure == "copying":
        marker = stage.marker
    if failure == "marker":
        marker["handoff"]["publication_fence"] = "55555555-5555-4555-8555-555555555555"
    if failure == "status":
        run["status"] = "running"
    if failure == "census":
        handoff = run["metrics"]["nucc_handoff"]
        handoff["row_count"] += 1
        handoff["handoff_sha256"] = control.receipt_digest(
            {key: field_value for key, field_value in handoff.items() if key != "handoff_sha256"}
        )
        marker["handoff"] = handoff
        message = "closed input census differs"
    monkeypatch.setattr(source, "_source_stage_state", AsyncMock(return_value=(run, marker)))
    poll = AsyncMock()
    monkeypatch.setattr(source.asyncio, "sleep", poll)
    events = []
    with pytest.raises(source.native.ReferenceFamilyArchiveError, match=message):
        await source.wait_nucc_source_handoff(_database(object(), events), stage)
    poll.assert_not_awaited()
    assert events == ["begin", "end"]


@pytest.mark.parametrize("status", ["canceling", "canceled", "cancelled", "failed", "dead_letter"])
async def test_wait_stops_terminal_input_without_polling_or_returning_a_handoff(monkeypatch, status):
    stage = _ready_stage()
    run = source_run(stage.marker, ready=stage.ready, status=status)
    monkeypatch.setattr(source, "_source_stage_state", AsyncMock(return_value=(run, stage.marker)))
    poll = AsyncMock()
    monkeypatch.setattr(source.asyncio, "sleep", poll)
    expected = source.ImportCancelledError if status.startswith("cancel") else source.native.ReferenceFamilyArchiveError
    with pytest.raises(expected):
        await source.wait_nucc_source_handoff(_database(object(), []), stage)
    poll.assert_not_awaited()


@pytest.fixture
def acknowledgement_case(monkeypatch):
    """Use real handoff decoding while recording I/O order at the catalog boundary."""
    marker = closed_marker()
    handoff = marker["handoff"]
    receipt = publication()
    receipt.update({key: handoff[key] for key in control.HANDOFF_FIELDS & control.PUBLICATION_FIELDS - {"contract"}})
    run = source_run(marker, status="finalizing")
    run["metrics"]["nucc_handoff"] = handoff
    events = []
    callbacks_by_name = {}

    for name, callback_result in {
        "_source_transaction": {"node_id": marker["node_id"]},
        "_read_owner_event": receipt,
        "_require_source_controls": None,
        "_locked_source_run": run,
        "_project_generation": None,
        "_acknowledge_run": None,
        "_recheck_source_transaction": None,
    }.items():

        async def record(*arguments, _name=name, _result=callback_result):
            events.append(_name)
            return _result

        callbacks_by_name[name] = AsyncMock(side_effect=record)
        monkeypatch.setattr(control, name, callbacks_by_name[name])

    async def action(executor, run_id):
        assert executor._connection is session and run_id == marker["run_id"]
        events.append("action-lock")

    session = object()
    monkeypatch.setattr(control_lifecycle, "acquire_control_run_worker_action_lock", action)
    return SimpleNamespace(session=session, receipt=receipt, run=run, events=events, callbacks=callbacks_by_name)


async def test_acknowledgement_projects_only_the_exact_committed_owner_event(acknowledgement_case):
    case = acknowledgement_case
    result = await control.acknowledge_source_publication(
        case.session, case.receipt["run_id"], case.receipt["publication_fence"]
    )
    assert result is case.receipt
    assert case.events == [
        "_source_transaction",
        "action-lock",
        "_read_owner_event",
        "_require_source_controls",
        "_locked_source_run",
        "_project_generation",
        "_acknowledge_run",
        "_recheck_source_transaction",
    ]
    case.callbacks["_acknowledge_run"].assert_awaited_once_with(
        case.session, case.run, case.run["metrics"]["nucc_handoff"], case.receipt
    )


@pytest.mark.parametrize("status", ["canceling", "canceled", "cancelled", "failed", "dead_letter"])
async def test_committed_owner_publication_wins_over_later_worker_status(acknowledgement_case, status):
    case = acknowledgement_case
    case.run["status"] = status
    assert (
        await control.acknowledge_source_publication(
            case.session, case.receipt["run_id"], case.receipt["publication_fence"]
        )
        == case.receipt
    )
    case.callbacks["_acknowledge_run"].assert_awaited_once()


async def test_acknowledgement_replay_rechecks_authority_without_reprojecting(acknowledgement_case):
    case = acknowledgement_case
    case.run["status"] = "succeeded"
    case.run["metrics"]["nucc_native_publication"] = deepcopy(case.receipt)
    assert (
        await control.acknowledge_source_publication(
            case.session, case.receipt["run_id"], case.receipt["publication_fence"]
        )
        == case.receipt
    )
    case.callbacks["_project_generation"].assert_not_awaited()
    case.callbacks["_acknowledge_run"].assert_not_awaited()
    assert case.events[-1] == "_recheck_source_transaction"


@pytest.mark.parametrize("failure", ["purpose", "run", "handoff", "attempt", "parameters", "running", "success"])
async def test_acknowledgement_refuses_mismatched_event_or_attempt_before_projection(acknowledgement_case, failure):
    case = acknowledgement_case
    message = "Source ACK purpose differs"
    if failure == "purpose":
        case.receipt["purpose"] = "restore"
    if failure == "run":
        case.receipt["run_id"] = "other-run"
    if failure == "handoff":
        case.receipt["semantic_sha256"] = "0" * 64
        message = "Source ACK handoff differs"
    if failure in {"attempt", "parameters"}:
        if failure == "attempt":
            case.run["progress"]["attempt_id"] = "other-attempt"
        else:
            case.run["params"]["test_mode"] = True
        message = "Source attempt or parameters differ"
    if failure in {"running", "success"}:
        case.run["status"] = "running" if failure == "running" else "succeeded"
        message = "Source ACK run changed"
    with pytest.raises(control.NuccProtectedControlError, match=message):
        await control.acknowledge_source_publication(case.session, "test-run", case.receipt["publication_fence"])
    case.callbacks["_project_generation"].assert_not_awaited()
    case.callbacks["_acknowledge_run"].assert_not_awaited()


@pytest.mark.parametrize("boundary", ["_project_generation", "_acknowledge_run", "_recheck_source_transaction"])
async def test_acknowledgement_propagates_failed_projection_or_cas(acknowledgement_case, boundary):
    case = acknowledgement_case
    failure = RuntimeError("synthetic transaction refusal")
    case.callbacks[boundary].side_effect = failure
    with pytest.raises(RuntimeError) as caught:
        await control.acknowledge_source_publication(
            case.session, case.receipt["run_id"], case.receipt["publication_fence"]
        )
    assert caught.value is failure
    if boundary == "_project_generation":
        case.callbacks["_acknowledge_run"].assert_not_awaited()


@pytest.mark.parametrize(
    "candidate_oid,current_oid,safe", [(None, 99, True), (105, 105, True), (106, 99, False), (105, 99, True)]
)
async def test_cleanup_preserves_absent_published_and_replaced_candidates(
    monkeypatch, candidate_oid, current_oid, safe
):
    stage_by_field = {"schema": "mrf", "name": "nucc_taxonomy_s_synthetic", "oid": 105, "owner_oid": 110}
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=safe))
    catalog = AsyncMock()
    monkeypatch.setattr(nucc.native, "_relation_oid", AsyncMock(side_effect=[candidate_oid, current_oid]))
    monkeypatch.setattr(nucc.native, "require_native_read_catalog", catalog)
    monkeypatch.setattr(nucc, "db", _database(session, []))
    context_by_field = {"context": {"nucc_plain_stage": stage_by_field}}
    if candidate_oid is not None and current_oid != stage_by_field["oid"] and not safe:
        with pytest.raises(nucc.native.ReferenceFamilyArchiveError, match="candidate identity changed"):
            await nucc._cleanup_plain_stage(context_by_field)
    else:
        await nucc._cleanup_plain_stage(context_by_field)
    if candidate_oid == 105 and current_oid != 105 and safe:
        catalog.assert_awaited_once_with(session, (105,))
        assert str(session.execute.await_args.args[0]) == 'DROP TABLE "mrf"."nucc_taxonomy_s_synthetic"'
    else:
        session.execute.assert_not_awaited()
        catalog.assert_not_awaited()
    if candidate_oid is not None and current_oid != 105:
        statement, parameters = session.scalar.await_args.args
        assert parameters == {"oid": 105, "owner": 110, "relation": '"mrf"."nucc_taxonomy_s_synthetic"'}
        assert "c.oid=:oid AND c.relowner=:owner" in str(statement)
        assert "session_user=current_user" in str(statement) and "pg_inherits" in str(statement)


async def test_plain_cleanup_propagates_catalog_refusal_without_dropping_heap(monkeypatch):
    stage_by_field = {"schema": "mrf", "name": "nucc_taxonomy_s_synthetic", "oid": 105, "owner_oid": 110}
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=True))
    failure = nucc.native.ReferenceFamilyArchiveError("synthetic changed ACL")
    monkeypatch.setattr(nucc.native, "_relation_oid", AsyncMock(side_effect=[105, 99]))
    monkeypatch.setattr(nucc.native, "require_native_read_catalog", AsyncMock(side_effect=failure))
    monkeypatch.setattr(nucc, "db", _database(session, []))
    with pytest.raises(nucc.native.ReferenceFamilyArchiveError) as caught:
        await nucc._cleanup_plain_stage({"context": {"nucc_plain_stage": stage_by_field}})
    assert caught.value is failure
    session.execute.assert_not_awaited()


@pytest.fixture
def plain_readback_case(monkeypatch):
    """Bind a controlled attempt and actual readback function to recorded catalog replies."""
    marker = copying_marker()
    context_by_field = {
        "control_run_id": marker["run_id"],
        "context": {
            "_control_attempt_id": marker["attempt_id"],
            "_control_attempt_started_at": marker["attempt_started_at"],
        },
    }
    stage_by_field = {"schema": "mrf", "name": "nucc_taxonomy_s_synthetic", "oid": 105, "owner_oid": 110}
    observed_by_field = {
        "status": "succeeded",
        "progress": {
            "attempt_id": marker["attempt_id"],
            "attempt_started_at": marker["attempt_started_at"],
            "phase": "nucc published",
        },
        "metrics": {"rows": 42},
        "heartbeat_at": datetime(2026, 1, 1, 1, 2, 3),
        "finished_at": datetime(2026, 1, 1, 1, 2, 4),
        "error": None,
    }
    query_result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: observed_by_field))
    session = SimpleNamespace(execute=AsyncMock(return_value=query_result), scalar=AsyncMock(return_value=True))
    relation = AsyncMock(return_value=105)
    catalog = AsyncMock()
    census = AsyncMock(return_value={"row_count": 42})
    monkeypatch.setattr(nucc, "db", _database(session, []))
    monkeypatch.setattr(nucc.native, "_relation_oid", relation)
    monkeypatch.setattr(nucc.native, "require_native_read_catalog", catalog)
    monkeypatch.setattr(nucc.native, "_validate_nucc_table_set", census)
    return SimpleNamespace(
        context=context_by_field,
        stage=stage_by_field,
        observed=observed_by_field,
        session=session,
        relation=relation,
        catalog=catalog,
        census=census,
    )


async def test_lost_commit_readback_accepts_only_exact_published_heap_and_attempt(plain_readback_case):
    case = plain_readback_case
    assert await nucc._read_plain_commit(case.context, case.stage, {"rows": 42}) == {
        "heartbeat_at": "2026-01-01T01:02:03.000000+00:00",
        "finished_at": "2026-01-01T01:02:04.000000+00:00",
    }
    case.relation.assert_awaited_once_with(case.session, "mrf", "nucc_taxonomy")
    case.catalog.assert_awaited_once_with(case.session, (105,))
    case.census.assert_awaited_once_with(case.session, "mrf", "nucc_taxonomy")
    statement, parameters = case.session.execute.await_args.args
    assert "FOR UPDATE" in str(statement) and parameters == {"run": "test-run"}


async def test_lost_commit_readback_distinguishes_original_unpublished_attempt(plain_readback_case):
    case = plain_readback_case
    case.observed["status"] = "running"
    assert await nucc._read_plain_commit(case.context, case.stage, {"rows": 42}) is None
    case.relation.assert_awaited_once_with(case.session, "mrf", case.stage["name"])
    case.catalog.assert_not_awaited()
    case.census.assert_not_awaited()


@pytest.mark.parametrize(
    "failure", ["status", "finished", "error", "rows", "attempt", "started", "phase", "heap", "census"]
)
async def test_lost_commit_readback_refuses_changed_run_heap_or_census(plain_readback_case, failure):
    case = plain_readback_case
    message = "publication readback differs"
    if failure == "status":
        case.observed["status"] = "failed"
    if failure == "finished":
        case.observed["finished_at"] = None
    if failure == "error":
        case.observed["error"] = "synthetic failed attempt"
    if failure == "rows":
        case.observed["metrics"]["rows"] += 1
    if failure in {"attempt", "started", "phase"}:
        field = {"attempt": "attempt_id", "started": "attempt_started_at", "phase": "phase"}[failure]
        case.observed["progress"][field] = "foreign-attempt"
    if failure == "heap":
        case.relation.return_value = 106
    if failure == "census":
        case.census.return_value = {"row_count": 43}
        message = "published census changed"
    with pytest.raises(nucc.native.ReferenceFamilyArchiveError, match=message):
        await nucc._read_plain_commit(case.context, case.stage, {"rows": 42})
    if failure != "census":
        case.catalog.assert_not_awaited()
        case.census.assert_not_awaited()
