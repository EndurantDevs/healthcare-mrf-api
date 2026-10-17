# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Event-driven lease renewal checks without network, database or elapsed waits."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.custom_import import https_candidate
from process.custom_import.execution import ExecutionTransition
from process.custom_import.https_candidate import HttpsCandidateError, run_https_candidate
from tests.test_custom_import_https_candidate import _retained
from tests.test_custom_import_https_candidate import coordinator as coordinator
from tests.test_custom_import_https_candidate import no_network as no_network


class _LeaseClock:
    def __init__(self, coordinator):
        self.coordinator = coordinator
        self.seconds = 0
        self.expires = https_candidate.execution.DEFAULT_LEASE_SECONDS
        self.waiting = asyncio.Event()
        self.wake = asyncio.Event()
        self.renewed = asyncio.Event()
        self.task = None
        self.calls = []
        self.failure = None
        self.original_sleep = asyncio.sleep

    async def sleep(self, delay):
        if delay == 0:
            # Preserve the decoder's scheduling points, without elapsed waits.
            return await self.original_sleep(0)
        assert delay == min(5.0, https_candidate.execution.DEFAULT_LEASE_SECONDS / 3)
        self.task = asyncio.current_task()
        self.waiting.set()
        await self.wake.wait()
        self.wake.clear()
        self.waiting.clear()

    async def heartbeat(self, session, **arguments):
        assert session.is_active
        assert arguments == {
            "execution_id": self.coordinator.grant.execution_id,
            "fence": 2 if self.coordinator.request.lease_token == "resumed-owner" else 1,
            "token": self.coordinator.request.lease_token,
        }
        self.calls.append((self.seconds, session, asyncio.current_task()))
        self.renewed.set()
        if self.failure == "lost" or self.seconds >= self.expires:
            return None
        if self.failure == "error":
            raise RuntimeError("synthetic-heartbeat-diagnostic")
        self.expires = self.seconds + https_candidate.execution.DEFAULT_LEASE_SECONDS
        return replace(
            self.coordinator.grant,
            fence=arguments["fence"],
            state="canceling" if self.failure == "canceling" else "running",
        )

    async def advance(self, seconds):
        await asyncio.wait_for(self.waiting.wait(), 1)
        self.seconds += seconds
        self.renewed.clear()
        self.wake.set()
        await asyncio.wait_for(self.renewed.wait(), 1)


@pytest.fixture
def lease_clock(coordinator, monkeypatch):
    clock = _LeaseClock(coordinator)
    monkeypatch.setattr(https_candidate.asyncio, "sleep", clock.sleep)
    coordinator.mocks_by_name["heartbeat_execution"].side_effect = clock.heartbeat
    return clock


def _hold_stage(coordinator, monkeypatch, stage):
    held = SimpleNamespace(started=asyncio.Event(), release=asyncio.Event(), closed=asyncio.Event(), failure=None)
    original = coordinator.acquire.side_effect if stage == "acquisition" else https_candidate._replay

    async def blocked(*arguments, **keywords):
        held.started.set()
        try:
            await held.release.wait()
            if held.failure is not None:
                raise held.failure
            return await original(*arguments, **keywords)
        finally:
            held.closed.set()

    if stage == "acquisition":
        coordinator.acquire.side_effect = blocked
    else:
        monkeypatch.setattr(https_candidate, "_replay", blocked)
    return held


@pytest.mark.asyncio
@pytest.mark.parametrize(("stage", "resume"), [("acquisition", False), ("replay", False), ("replay", True)])
async def test_long_preparation_renews_separately_and_joins_before_runner(
    coordinator, monkeypatch, lease_clock, stage, resume
):
    coordinator.request = replace(
        coordinator.request,
        timeout_seconds=600,
        lease_token="resumed-owner" if resume else coordinator.request.lease_token,
    )
    retained = coordinator.mocks_by_name["load_replayable_parquet_bundle"]
    retained.return_value = _retained(coordinator.request, coordinator.payloads_by_stream)
    held = _hold_stage(coordinator, monkeypatch, stage)
    runner = coordinator.mocks_by_name["run_claimed_candidate"]

    async def handoff(*_arguments):
        assert held.closed.is_set() and lease_clock.task.done()
        assert lease_clock.calls[-1][2] is not lease_clock.task
        assert all(not session.is_active for session in coordinator.sessions)
        return runner.return_value

    runner.side_effect = handoff
    worker = asyncio.create_task(run_https_candidate(coordinator.session_factory, coordinator.request, resume=resume))
    try:
        await asyncio.wait_for(held.started.wait(), 1)
        for elapsed in (100, 100, 101):
            await lease_clock.advance(elapsed)
        assert lease_clock.seconds > https_candidate.execution.DEFAULT_LEASE_SECONDS
        assert lease_clock.expires > lease_clock.seconds
        held.release.set()
        assert (await worker).status == "no_change"
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
    renewals = [call for call in lease_clock.calls if call[2] is lease_clock.task]
    assert [call[0] for call in renewals] == [100, 200, 301]
    assert len({id(call[1]) for call in lease_clock.calls}) == len(lease_clock.calls)
    assert all(call[1].outcome == "commit" for call in lease_clock.calls)
    runner.assert_awaited_once()
    if resume:
        coordinator.acquire.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["acquisition", "replay"])
@pytest.mark.parametrize("failure", ["lost", "canceling", "error"])
async def test_renewal_failure_stops_preparation_without_publication(
    coordinator, monkeypatch, lease_clock, stage, failure
):
    held = _hold_stage(coordinator, monkeypatch, stage)
    finish = coordinator.mocks_by_name["finish_execution"]

    async def finish_canceled(*_arguments, **keywords):
        assert held.closed.is_set() and lease_clock.task.done()
        assert keywords["terminal_state"] == "canceled"
        return ExecutionTransition(17, "canceled", True)

    finish.side_effect = finish_canceled
    worker = asyncio.create_task(run_https_candidate(coordinator.session_factory, coordinator.request))
    try:
        # Let the ordinary pre-acquisition heartbeat succeed; fail the background renewal.
        lease_clock.failure = None
        await asyncio.wait_for(held.started.wait(), 1)
        await asyncio.wait_for(lease_clock.waiting.wait(), 1)
        lease_clock.failure = failure
        lease_clock.wake.set()
        if failure == "error":
            with pytest.raises(HttpsCandidateError, match="^https_candidate_failed$"):
                await worker
        else:
            assert (await worker).status == ("lease_lost" if failure == "lost" else "canceled")
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
    assert held.closed.is_set() and lease_clock.task.done()
    assert all(not session.is_active for session in coordinator.sessions)
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()
    if stage == "acquisition":
        coordinator.mocks_by_name["register_replayable_parquet_bundle"].assert_not_awaited()
    else:
        coordinator.mocks_by_name["bind_execution_capture_bundle"].assert_awaited_once()
    assert finish.await_count == (1 if failure == "canceling" else 0)


@pytest.mark.asyncio
async def test_non_yielding_admission_expiry_still_refuses_runner(coordinator, monkeypatch, lease_clock):
    original = https_candidate.assemble_root_families

    def expired_admission(*arguments):
        admitted = original(*arguments)
        lease_clock.seconds = lease_clock.expires
        return admitted

    monkeypatch.setattr(https_candidate, "assemble_root_families", expired_admission)
    result = await run_https_candidate(coordinator.session_factory, coordinator.request)
    assert result.status == "lease_lost"
    assert lease_clock.task.done() and lease_clock.calls[-1][2] is not lease_clock.task
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_cancellation_waits_for_heartbeat_transaction_cleanup(coordinator, monkeypatch, lease_clock):
    held = _hold_stage(coordinator, monkeypatch, "replay")
    entered, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def heartbeat(session, **_arguments):
        assert session.is_active
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            closing.set()
            await release.wait()

    coordinator.mocks_by_name["heartbeat_execution"].side_effect = heartbeat
    worker = asyncio.create_task(run_https_candidate(coordinator.session_factory, coordinator.request, resume=True))
    try:
        await asyncio.wait_for(held.started.wait(), 1)
        await asyncio.wait_for(lease_clock.waiting.wait(), 1)
        lease_clock.wake.set()
        await asyncio.wait_for(entered.wait(), 1)
        worker.cancel()
        await asyncio.wait_for(closing.wait(), 1)
        assert not worker.done() and any(session.is_active for session in coordinator.sessions)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await worker
    finally:
        release.set()
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
    assert held.closed.is_set() and lease_clock.task.done()
    assert all(not session.is_active for session in coordinator.sessions)
    assert coordinator.mocks_by_name["heartbeat_execution"].call_args.args[0].outcome == "rollback"
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()


@pytest.mark.asyncio
async def test_preparation_error_joins_renewal_and_cannot_reach_runner(coordinator, monkeypatch, lease_clock):
    held = _hold_stage(coordinator, monkeypatch, "replay")
    held.failure = ValueError("synthetic-replay-diagnostic")
    worker = asyncio.create_task(run_https_candidate(coordinator.session_factory, coordinator.request))
    try:
        await asyncio.wait_for(held.started.wait(), 1)
        await asyncio.wait_for(lease_clock.waiting.wait(), 1)
        held.release.set()
        with pytest.raises(HttpsCandidateError, match="^https_candidate_failed$"):
            await worker
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
    assert held.closed.is_set() and lease_clock.task.done()
    assert all(not session.is_active for session in coordinator.sessions)
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()


@pytest.mark.asyncio
async def test_renewal_and_replay_cleanup_failures_are_joined_and_redacted(coordinator, monkeypatch, lease_clock):
    coordinator.request = replace(coordinator.request, lease_token="resumed-owner")
    entered, closed = asyncio.Event(), asyncio.Event()

    async def replay(*_arguments, **_keywords):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            closed.set()
            raise ValueError("synthetic-replay-cleanup")

    monkeypatch.setattr(https_candidate, "_replay", replay)
    worker = asyncio.create_task(run_https_candidate(coordinator.session_factory, coordinator.request, resume=True))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.wait_for(lease_clock.waiting.wait(), 1)
        lease_clock.failure = "error"
        lease_clock.wake.set()
        with pytest.raises(HttpsCandidateError, match="^https_candidate_failed$") as error:
            await worker
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
    failure = error.value.__context__
    assert isinstance(failure, ExceptionGroup)
    assert {str(nested_failure) for nested_failure in failure.exceptions} == {
        "synthetic-heartbeat-diagnostic",
        "synthetic-replay-cleanup",
    }
    assert error.value.__suppress_context__ is True
    assert closed.is_set() and lease_clock.task.done()
    assert all(not session.is_active for session in coordinator.sessions)
    assert coordinator.mocks_by_name["heartbeat_execution"].call_args.args[0].outcome == "rollback"
    coordinator.acquire.assert_not_awaited()
    coordinator.mocks_by_name["finish_execution"].assert_not_awaited()
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()
