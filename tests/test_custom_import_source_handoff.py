# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Paired runner dispatch and durable SOURCE resume, without writer fallback."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import build_source as staging
from process.custom_import import source_worker
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_source import _request


def _page(monkeypatch, streams=(), *, phase="source"):
    session = SimpleNamespace(scalars=AsyncMock(return_value=SimpleNamespace(all=lambda: streams)))

    @asynccontextmanager
    async def page(_factory, _request, _build_id):
        yield session, SimpleNamespace(phase=phase)

    monkeypatch.setattr(staging, "_page_session", page)
    monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())
    return session


@pytest.mark.parametrize("complete", [(False, False), (True, False), (True, True)])
async def test_resume_selects_only_pending_streams_or_one_global_freeze(monkeypatch, complete):
    streams = [
        SimpleNamespace(stream_slot=slot, replay_verified_at=True if done else None)
        for slot, done in enumerate(complete, 1)
    ]
    _page(monkeypatch, streams)
    slots = await staging._source_handoff_slots(
        object(), _request(), 1, SimpleNamespace(stream_slots={"root": 1, "child": 2})
    )
    expected = tuple(slot for slot, done in enumerate(complete, 1) if not done) or (1,)
    assert slots == expected


async def test_source_handoff_denies_incomplete_retained_stream_membership(monkeypatch):
    _page(monkeypatch, [SimpleNamespace(stream_slot=1, replay_verified_at=None)])
    with pytest.raises(CandidateRunnerError, match="streams differ"):
        await staging._source_handoff_slots(
            object(), _request(), 1, SimpleNamespace(stream_slots={"root": 1, "child": 2})
        )


async def test_paired_dispatch_never_uses_local_source_or_admission(monkeypatch):
    events = []
    request = _request()
    registry = SimpleNamespace(stream_slots={"root": 1})
    pair = SimpleNamespace(
        source=SimpleNamespace(bind_request=Mock(side_effect=lambda value: value)),
        admission=SimpleNamespace(bind_request=Mock(side_effect=lambda value: value)),
    )
    result_by_operation = {
        "_begin_build": (1, registry),
        "_stage_source_handoff": None,
        "_prepare_snapshot_indexes": None,
        "_stage_admission_handoff": "complete",
        "_stage_local_source": None,
        "_admit_local_pages": None,
    }
    mock_by_operation = {}
    for name, outcome in result_by_operation.items():

        async def invoke(*args, _name=name, _outcome=outcome, **kwargs):
            events.append(_name)
            return _outcome

        mock_by_operation[name] = AsyncMock(side_effect=invoke)
        monkeypatch.setattr(staging, name, mock_by_operation[name])
    assert await staging.stage_segmented_source("pool", request, writer_transports=pair) == "complete"
    assert events == list(result_by_operation)[:4]
    mock_by_operation["_stage_source_handoff"].assert_awaited_once_with("pool", request, 1, registry, pair.source)
    mock_by_operation["_stage_admission_handoff"].assert_awaited_once_with("pool", request, 1, pair.admission)
    pair.admission.bind_request.assert_called_once_with(request)
    pair.source.bind_request.assert_called_once_with(request)


@pytest.mark.parametrize("reconciled", [False, True])
async def test_source_handoff_continues_only_after_verified_forward_progress(monkeypatch, reconciled):
    monkeypatch.setattr(staging, "_source_handoff_slots", AsyncMock(return_value=(1, 2)))
    forward = SimpleNamespace(phase="source", stream_complete=False)
    lost_ack = source_worker.SourceReconciliationRequired(object(), forward if reconciled else None)
    step = AsyncMock(
        side_effect=[
            lost_ack,
            SimpleNamespace(phase="source", stream_complete=True),
            SimpleNamespace(phase="admission", stream_complete=True),
        ]
    )
    monkeypatch.setattr(source_worker, "source_next_batch", step)
    if reconciled:
        await staging._stage_source_handoff("pool", _request(), 1, object(), "transport")
        assert [call.args[3] for call in step.await_args_list] == [1, 1, 2]
    else:
        with pytest.raises(source_worker.SourceReconciliationRequired):
            await staging._stage_source_handoff("pool", _request(), 1, object(), "transport")
        assert step.await_count == 1


async def test_source_handoff_requires_committed_freeze_before_admission(monkeypatch):
    monkeypatch.setattr(staging, "_source_handoff_slots", AsyncMock(return_value=(1,)))
    monkeypatch.setattr(
        source_worker,
        "source_next_batch",
        AsyncMock(return_value=SimpleNamespace(phase="source", stream_complete=True)),
    )
    _page(monkeypatch)
    with pytest.raises(CandidateRunnerError, match="committed global freeze"):
        await staging._stage_source_handoff("pool", _request(), 1, object(), "transport")


async def test_source_handoff_propagates_lost_authority_without_retry(monkeypatch):
    monkeypatch.setattr(staging, "_source_handoff_slots", AsyncMock(return_value=(1,)))
    step = AsyncMock(side_effect=LeaseAuthorityLost("expired"))
    monkeypatch.setattr(source_worker, "source_next_batch", step)
    with pytest.raises(LeaseAuthorityLost):
        await staging._stage_source_handoff("pool", _request(), 1, object(), "transport")
    assert step.await_count == 1
