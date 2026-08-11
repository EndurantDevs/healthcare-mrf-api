# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lease, retry, heartbeat, and census worker failure boundaries."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from process.provider_directory_rooted_graph_acquisition import (
    ProviderDirectoryRootedGraphAcquisitionError,
)
from process.provider_directory_rooted_graph_acquisition_worker import (
    _ClaimState,
    _RootRunner,
    _cancel_and_drain,
    drain_operation,
)
from process.provider_directory_rooted_graph_http import (
    ProviderDirectoryRootedGraphHTTPResult,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphCensusClaim,
)
from tests.provider_directory_rooted_graph_acquisition_test_support import (
    identity,
    replay_claim,
    snapshot,
)
from tests.provider_directory_rooted_graph_runtime_test_support import (
    RuntimeHarness,
    enabled_config,
)


def _runner(
    harness: RuntimeHarness, *, dependencies=None, **config_changes
) -> _RootRunner:
    return _RootRunner(
        identity(),
        snapshot(),
        config=replace(enabled_config(), **config_changes),
        dependencies=dependencies or harness.dependencies(),
        database=object(),
    )


@pytest.mark.asyncio
async def test_drain_operation_propagates_operation_failure() -> None:
    async def fail() -> None:
        await asyncio.sleep(0)
        raise RuntimeError("synthetic failure")

    with pytest.raises(RuntimeError, match="synthetic failure"):
        await drain_operation(fail(), preserve_cancellation=False)


@pytest.mark.asyncio
async def test_drain_operation_preserves_repeated_cancellation() -> None:
    started = asyncio.Event()
    allow_completion = asyncio.Event()

    async def finish_after_gate() -> str:
        started.set()
        await allow_completion.wait()
        return "finished"

    task = asyncio.create_task(
        drain_operation(finish_after_gate(), preserve_cancellation=True)
    )
    await started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    allow_completion.set()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_cancel_and_drain_accepts_already_completed_task() -> None:
    task = asyncio.create_task(asyncio.sleep(0))
    await task
    await _cancel_and_drain(task)
    assert task.done()


def test_runner_rejects_foreign_claim_and_invalid_retry_counter() -> None:
    runner = _runner(RuntimeHarness())
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError):
        runner._require_work_claim(object())
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError):
        runner._finish_delayed_retry()

    runner._delayed_retry_count = 2
    runner._no_delayed_retries.clear()
    runner._finish_delayed_retry()
    assert runner._delayed_retry_count == 1
    assert not runner._no_delayed_retries.is_set()


@pytest.mark.asyncio
async def test_claim_rechecks_delayed_retry_fence_under_lock() -> None:
    runner = _runner(RuntimeHarness())
    await runner._claim_lock.acquire()
    claim_task = asyncio.create_task(runner.claim())
    await asyncio.sleep(0)
    runner._no_delayed_retries.clear()
    runner._claim_lock.release()
    await asyncio.sleep(0)
    assert not claim_task.done()
    runner._no_delayed_retries.set()
    assert await claim_task is None


class _BlockingExitLock:
    def __init__(self) -> None:
        self.exit_started = asyncio.Event()
        self.allow_exit = asyncio.Event()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self.exit_started.set()
        await self.allow_exit.wait()
        return False


@pytest.mark.asyncio
async def test_claim_cancellation_after_lease_releases_exact_claim() -> None:
    harness = RuntimeHarness()
    root = identity()
    await harness.initialize_root(root, database=object())
    runner = _runner(harness)
    blocking_lock = _BlockingExitLock()
    runner._claim_lock = blocking_lock

    claim_task = asyncio.create_task(runner.claim())
    await blocking_lock.exit_started.wait()
    claim_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await claim_task
    assert any(event[0] == "release" for event in harness.events)


@pytest.mark.asyncio
async def test_claim_cancellation_before_lease_has_nothing_to_release() -> None:
    harness = RuntimeHarness()
    claim_started = asyncio.Event()
    never_return = asyncio.Event()

    async def block_claim(*_args, **_kwargs):
        claim_started.set()
        await never_return.wait()

    runner = _runner(
        harness,
        dependencies=replace(harness.dependencies(), claim_work=block_claim),
    )
    claim_task = asyncio.create_task(runner.claim())
    await claim_started.wait()
    claim_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await claim_task
    assert not any(event[0] == "release" for event in harness.events)


@pytest.mark.asyncio
async def test_release_failures_restore_retry_fence_and_are_best_effort() -> None:
    harness = RuntimeHarness()

    async def fail_release(*_args, **_kwargs):
        raise RuntimeError("synthetic release failure")

    runner = _runner(
        harness,
        dependencies=replace(harness.dependencies(), release_work=fail_release),
    )
    claim = harness._claims("baseline")["role"]
    with pytest.raises(RuntimeError, match="release failure"):
        await runner.release_for_retry(claim, _ClaimState())
    assert runner._delayed_retry_count == 0
    assert runner._no_delayed_retries.is_set()
    await runner.release_unmaterialized(claim)


@pytest.mark.asyncio
async def test_claim_retry_can_find_no_released_query() -> None:
    harness = RuntimeHarness()
    runner = _runner(harness)
    runner._delayed_retry_count = 1
    runner._no_delayed_retries.clear()
    assert await runner.claim_retry("pdrgq_" + "0" * 48, 0) is None
    assert runner._no_delayed_retries.is_set()


@pytest.mark.asyncio
async def test_claim_retry_cancellation_releases_reclaimed_query() -> None:
    harness = RuntimeHarness()
    root = identity()
    claim = harness._claims("baseline")["role"]
    harness.generic_pending[root.acquisition_id].append(claim)
    runner = _runner(harness)
    runner._delayed_retry_count = 1
    runner._no_delayed_retries.clear()
    require_calls: list[object] = []
    original_require = runner._require_work_claim

    def cancel_once(reclaimed_claim):
        require_calls.append(reclaimed_claim)
        if len(require_calls) == 1:
            raise asyncio.CancelledError
        return original_require(reclaimed_claim)

    runner._require_work_claim = cancel_once
    with pytest.raises(asyncio.CancelledError):
        await runner.claim_retry(claim.query_id, 0)
    assert require_calls == [claim, claim]
    assert any(event[0] == "release" for event in harness.events)


@pytest.mark.asyncio
async def test_fetch_result_type_is_fail_closed_and_lease_released() -> None:
    harness = RuntimeHarness()

    async def invalid_fetch(*_args, **_kwargs):
        return object()

    runner = _runner(
        harness,
        dependencies=replace(harness.dependencies(), fetch=invalid_fetch),
    )
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as error_info:
        await runner.process_claim(
            {"session_id": 1},
            harness._claims("baseline")["role"],
        )
    assert error_info.value.code == "state"
    assert any(event[0] == "release" for event in harness.events)


@pytest.mark.asyncio
async def test_heartbeat_failure_aborts_fetch_and_releases_lease() -> None:
    harness = RuntimeHarness()
    harness.block_fetch = True

    async def fail_heartbeat(*_args, **_kwargs):
        raise RuntimeError("synthetic heartbeat failure")

    runner = _runner(
        harness,
        dependencies=replace(harness.dependencies(), heartbeat=fail_heartbeat),
    )
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as error_info:
        await runner.process_claim(
            {"session_id": 1},
            harness._claims("baseline")["role"],
        )
    assert error_info.value.code == "state"
    assert any(event[0] == "release" for event in harness.events)


@pytest.mark.asyncio
async def test_heartbeat_cancellation_preserves_caller_cancellation() -> None:
    harness = RuntimeHarness()
    harness.block_fetch = True

    async def cancel_heartbeat(*_args, **_kwargs):
        raise asyncio.CancelledError

    runner = _runner(
        harness,
        dependencies=replace(harness.dependencies(), heartbeat=cancel_heartbeat),
    )
    with pytest.raises(asyncio.CancelledError):
        await runner.process_claim(
            {"session_id": 1},
            harness._claims("baseline")["role"],
        )
    assert any(event[0] == "release" for event in harness.events)


@pytest.mark.asyncio
async def test_unexpected_transport_failure_terminalizes_as_error() -> None:
    harness = RuntimeHarness()

    async def fail_fetch(*_args, **_kwargs):
        raise RuntimeError("synthetic transport failure")

    runner = _runner(
        harness,
        dependencies=replace(harness.dependencies(), fetch=fail_fetch),
    )
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as error_info:
        await runner.process_claim(
            {"session_id": 1},
            harness._claims("baseline")["role"],
        )
    assert error_info.value.code == "root_unsealable"
    assert ("error", identity().acquisition_id, "transport_failure") in harness.events


@pytest.mark.asyncio
async def test_invalid_fhir_result_terminalizes_response_error() -> None:
    harness = RuntimeHarness()
    claim = harness._claims("baseline")["role"]

    async def invalid_response(*_args, **_kwargs):
        return ProviderDirectoryRootedGraphHTTPResult(
            query_id=claim.query_id,
            resources=({"resourceType": "Organization", "id": "wrong.type"},),
            advertised_total=None,
            terminal_page_count=1,
            total_bytes=1,
        )

    runner = _runner(
        harness,
        dependencies=replace(harness.dependencies(), fetch=invalid_response),
    )
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError):
        await runner.process_claim({"session_id": 1}, claim)
    assert ("error", identity().acquisition_id, "response_invalid") in harness.events


@pytest.mark.asyncio
async def test_attempt_beyond_bound_terminalizes_without_fetch() -> None:
    harness = RuntimeHarness()
    claim = replay_claim(
        harness._claims("baseline")["role"],
        enabled_config().max_attempts + 1,
    )
    runner = _runner(harness)
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError):
        await runner.process_claim({"session_id": 1}, claim)
    assert ("error", identity().acquisition_id, "retry_exhausted") in harness.events
    assert not any(event[0] == "fetch" for event in harness.events)


@pytest.mark.asyncio
async def test_worker_continues_when_released_retry_was_not_reclaimable() -> None:
    harness = RuntimeHarness()
    root = identity()
    await harness.initialize_root(root, database=object())
    claim = harness._claims("baseline")["role"]
    harness.transient_once.add(claim.query_id)

    async def drop_release(released_claim, *, database):
        harness.events.append(("release", released_claim.acquisition_id, "dropped"))

    runner = _runner(
        harness,
        dependencies=replace(harness.dependencies(), release_work=drop_release),
    )
    await runner.worker({"session_id": 1})
    assert any(event[0] == "generic_empty" for event in harness.events)


@pytest.mark.asyncio
async def test_frontier_failure_cancels_and_drains_sibling_workers() -> None:
    harness = RuntimeHarness()
    runner = _runner(harness, concurrency=2)
    first_started = asyncio.Event()
    never_finish = asyncio.Event()
    worker_indexes = iter(range(2))

    async def controlled_worker(_session):
        worker_index = next(worker_indexes)
        if worker_index == 0:
            first_started.set()
            await never_finish.wait()
            return
        await first_started.wait()
        raise RuntimeError("synthetic worker failure")

    runner.worker = controlled_worker
    with pytest.raises(RuntimeError, match="worker failure"):
        await runner.drain_generic_frontier({"session_id": 1})


@pytest.mark.asyncio
async def test_census_absence_requires_prior_completion() -> None:
    harness = RuntimeHarness()

    async def completed_state(*_args, **_kwargs):
        return "completed"

    completed_runner = _runner(
        harness,
        dependencies=replace(harness.dependencies(), census_state=completed_state),
    )
    await completed_runner.process_census({"session_id": 1})

    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError):
        await _runner(harness).process_census({"session_id": 1})


@pytest.mark.asyncio
async def test_census_claim_type_and_identity_are_fail_closed() -> None:
    harness = RuntimeHarness()

    async def invalid_claim(*_args, **_kwargs):
        return object()

    runner = _runner(
        harness,
        dependencies=replace(harness.dependencies(), claim_census=invalid_claim),
    )
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError):
        await runner.process_census({"session_id": 1})


def _census_claim(
    harness: RuntimeHarness,
    *,
    attempt: int,
    references: tuple[str, ...] = ("Organization/network.synthetic-1",),
) -> ProviderDirectoryRootedGraphCensusClaim:
    work_claim = replay_claim(harness._claims("baseline")["census"], attempt)
    return ProviderDirectoryRootedGraphCensusClaim(
        work_claim=work_claim,
        root_network_references=references,
    )


@pytest.mark.asyncio
async def test_census_retry_reclaims_same_anchor_set() -> None:
    harness = RuntimeHarness()
    first_claim = _census_claim(harness, attempt=1)
    second_claim = _census_claim(harness, attempt=2)
    claims = [first_claim, second_claim]
    harness.transient_once.add(first_claim.work_claim.query_id)

    async def claim_census(*_args, **_kwargs):
        return claims.pop(0)

    runner = _runner(
        harness,
        dependencies=replace(harness.dependencies(), claim_census=claim_census),
    )
    await runner.process_census({"session_id": 1})
    assert harness.fetch_attempts[first_claim.work_claim.query_id] == 2


@pytest.mark.parametrize("replay_kind", ("missing", "changed"))
@pytest.mark.asyncio
async def test_census_retry_rejects_missing_or_changed_anchor_set(
    replay_kind: str,
) -> None:
    harness = RuntimeHarness()
    first_claim = _census_claim(harness, attempt=1)
    replayed_claim = (
        None
        if replay_kind == "missing"
        else _census_claim(
            harness,
            attempt=2,
            references=("Organization/different",),
        )
    )
    claims = [first_claim, replayed_claim]
    harness.transient_once.add(first_claim.work_claim.query_id)

    async def claim_census(*_args, **_kwargs):
        return claims.pop(0)

    runner = _runner(
        harness,
        dependencies=replace(harness.dependencies(), claim_census=claim_census),
    )
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError):
        await runner.process_census({"session_id": 1})
    if replayed_claim is not None:
        assert any(event[0] == "release" for event in harness.events)
