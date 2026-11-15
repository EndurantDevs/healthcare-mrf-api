# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.provider_directory_rooted_graph_acquisition import (
    ProviderDirectoryRootedGraphAcquisitionError,
)
from process.provider_directory_rooted_graph_acquisition_runtime import (
    _RootRunner,
    run_root,
)
from process.provider_directory_rooted_graph_http import (
    ProviderDirectoryRootedGraphHTTPError,
)
from process.provider_directory_rooted_graph_query import build_rooted_graph_direct_read
from tests.provider_directory_rooted_graph_acquisition_test_support import (
    API_BASE,
    claim_for_query,
    identity,
    snapshot,
)
from tests.provider_directory_rooted_graph_runtime_test_support import (
    RuntimeHarness,
    enabled_config,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_seed", [False, True])
@pytest.mark.parametrize("concurrency", [4, 8])
async def test_idle_workers_resume_when_one_query_discovers_a_wide_frontier(retry_seed, concurrency) -> None:
    harness = RuntimeHarness()
    root = identity()
    await harness.initialize_root(root, database=object())
    seed = harness._claims("baseline")["role"]
    if retry_seed:
        harness.transient_once.add(seed.query_id)
    child_claims = [
        claim_for_query(
            build_rooted_graph_direct_read(
                api_base=API_BASE, resource_type="Organization", resource_id=f"child-{index}"
            )
        )
        for index in range(concurrency * 2)
    ]
    at_capacity, release = asyncio.Event(), asyncio.Event()
    activity = SimpleNamespace(active=0, maximum=0)

    async def fetch(session, api_base, claim, *, bounds):
        if claim.query_id == seed.query_id:
            await asyncio.sleep(0)
            return await harness.fetch(session, api_base, claim, bounds=bounds)
        activity.active += 1
        activity.maximum = max(activity.maximum, activity.active)
        if activity.active == concurrency:
            at_capacity.set()
        try:
            await release.wait()
            return harness._missing_result(claim)
        finally:
            activity.active -= 1

    async def complete(claim, _result, *, database):
        assert claim.query_id == seed.query_id
        harness.generic_pending[root.acquisition_id].extend(child_claims)

    runner = _RootRunner(
        root,
        snapshot(),
        config=replace(enabled_config(), concurrency=concurrency),
        dependencies=replace(harness.dependencies(), fetch=fetch, complete_result=complete),
        database=object(),
    )
    task = asyncio.create_task(runner.drain_generic_frontier({"session_id": 1}))
    try:
        await asyncio.wait_for(at_capacity.wait(), 1)
        release.set()
        await asyncio.wait_for(task, 1)
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert activity.maximum == concurrency and activity.active == 0
    assert len([event for event in harness.events if event[0] == "missing"]) == concurrency * 2
    assert not harness.generic_pending[root.acquisition_id]


@pytest.mark.asyncio
async def test_empty_frontier_exits_without_fetching() -> None:
    harness = RuntimeHarness()
    runner = _RootRunner(
        identity(),
        snapshot(),
        config=replace(enabled_config(), concurrency=8),
        dependencies=harness.dependencies(),
        database=object(),
    )
    await asyncio.wait_for(runner.drain_generic_frontier({"session_id": 1}), 1)
    assert not any(event[0] == "fetch" for event in harness.events)


@pytest.mark.asyncio
async def test_frontier_cancellation_drains_active_and_idle_workers() -> None:
    harness = RuntimeHarness()
    harness.block_fetch = True
    root = identity()
    await harness.initialize_root(root, database=object())
    runner = _RootRunner(
        root,
        snapshot(),
        config=replace(enabled_config(), concurrency=8),
        dependencies=harness.dependencies(),
        database=object(),
    )
    task = asyncio.create_task(runner.drain_generic_frontier({"session_id": 1}))
    await asyncio.wait_for(harness.fetch_started.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert runner._active_work == 0
    assert any(event[0] == "release" for event in harness.events)
    assert not any("_RootRunner.worker" in item.get_coro().__qualname__ for item in asyncio.all_tasks())


@pytest.mark.asyncio
async def test_retry_releases_then_reclaims_exact_query_from_attempt_two() -> None:
    harness = RuntimeHarness()
    root = identity()
    await harness.initialize_root(root, database=object())
    role_claim = harness._claims("baseline")["role"]
    harness.transient_once.add(role_claim.query_id)

    summary, _elapsed = await run_root(
        root,
        snapshot(),
        config=enabled_config(),
        dependencies=harness.dependencies(),
        database=object(),
    )

    assert summary.acquisition_id == root.acquisition_id
    assert harness.fetch_attempts[role_claim.query_id] == 2
    release_index = next(index for index, event in enumerate(harness.events) if event[0] == "release")
    sleep_index = next(index for index, event in enumerate(harness.events) if event[0] == "sleep")
    assert release_index < sleep_index
    attempts = [event[4] for event in harness.events if event[0] == "fetch" and event[3] == role_claim.query_id]
    assert attempts == [1, 2]


@pytest.mark.asyncio
async def test_heartbeat_runs_while_fetch_is_in_flight() -> None:
    harness = RuntimeHarness()
    harness.block_fetch = True
    root = identity()
    claim = harness._claims("baseline")["role"]
    runner = _RootRunner(
        root,
        snapshot(),
        config=enabled_config(),
        dependencies=harness.dependencies(),
        database=object(),
    )
    task = asyncio.create_task(runner.process_claim({"session_id": 1}, claim))
    await harness.fetch_started.wait()
    while harness.heartbeat_count == 0:
        await asyncio.sleep(0.005)
    harness.allow_fetch.set()
    await task
    assert harness.heartbeat_count >= 1


@pytest.mark.asyncio
async def test_cancellation_during_fetch_releases_unmaterialized_lease() -> None:
    harness = RuntimeHarness()
    harness.block_fetch = True
    root = identity()
    claim = harness._claims("baseline")["role"]
    runner = _RootRunner(
        root,
        snapshot(),
        config=enabled_config(),
        dependencies=harness.dependencies(),
        database=object(),
    )
    task = asyncio.create_task(runner.process_claim({"session_id": 1}, claim))
    await harness.fetch_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert any(event[0] == "release" for event in harness.events)
    assert not any(event[0] == "complete" for event in harness.events)


@pytest.mark.asyncio
async def test_cancellation_drains_terminal_completion_without_release() -> None:
    harness = RuntimeHarness()
    harness.block_completion = True
    root = identity()
    claim = harness._claims("baseline")["role"]
    runner = _RootRunner(
        root,
        snapshot(),
        config=enabled_config(),
        dependencies=harness.dependencies(),
        database=object(),
    )
    task = asyncio.create_task(runner.process_claim({"session_id": 1}, claim))
    await harness.completion_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    harness.allow_completion.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert any(event[0] == "complete" for event in harness.events)
    assert not any(event[0] == "release" for event in harness.events)


@pytest.mark.asyncio
async def test_root_deadline_cancels_workers_and_releases_live_lease() -> None:
    harness = RuntimeHarness()
    harness.block_fetch = True
    root = identity()
    await harness.initialize_root(root, database=object())
    config = replace(enabled_config(), root_timeout_seconds=0.02)

    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as error_info:
        await run_root(
            root,
            snapshot(),
            config=config,
            dependencies=harness.dependencies(),
            database=object(),
        )

    assert error_info.value.code == "root_unsealable"
    assert any(event[0] == "release" for event in harness.events)
    assert not any(event[0] == "seal" for event in harness.events)


@pytest.mark.asyncio
async def test_nonretryable_transport_failure_terminalizes_and_blocks_seal() -> None:
    harness = RuntimeHarness()
    root = identity()
    claim = harness._claims("baseline")["role"]

    async def fail_fetch(*_args, **_kwargs):
        raise ProviderDirectoryRootedGraphHTTPError("resource_limit")

    dependencies = replace(harness.dependencies(), fetch=fail_fetch)
    runner = _RootRunner(
        root,
        snapshot(),
        config=enabled_config(),
        dependencies=dependencies,
        database=object(),
    )
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as error_info:
        await runner.process_claim({"session_id": 1}, claim)
    assert error_info.value.code == "root_unsealable"
    assert any(event[-1] == "resource_limit" for event in harness.events)
    assert not any(event[0] == "release" for event in harness.events)


@pytest.mark.asyncio
async def test_eight_claims_keep_renewing_while_one_completion_writer_is_held() -> None:
    harness = RuntimeHarness()
    claims = [
        claim_for_query(
            build_rooted_graph_direct_read(
                api_base=API_BASE, resource_type="Organization", resource_id=f"queued-{index}"
            )
        )
        for index in range(8)
    ]
    writer, release = asyncio.Lock(), asyncio.Event()
    entries = []
    at_writer = asyncio.Event()
    renewed_ids = set()

    async def complete(*_args, **_kwargs):
        entries.append("entered")
        if len(entries) == 8:
            at_writer.set()
        async with writer:
            await release.wait()

    async def heartbeat(claim, **_kwargs):
        renewed_ids.add(claim.query_id)

    runner = _RootRunner(
        identity(),
        snapshot(),
        config=replace(enabled_config(), concurrency=8),
        dependencies=replace(harness.dependencies(), complete_missing=complete, heartbeat=heartbeat),
        database=object(),
    )
    pending = asyncio.gather(*(runner.process_claim({"session_id": 1}, claim) for claim in claims))
    try:
        await asyncio.wait_for(at_writer.wait(), 1)
        async with asyncio.timeout(1):
            while len(renewed_ids) != 8:
                await asyncio.sleep(0.001)
        assert not pending.done()
        release.set()
        await pending
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
    assert sum(event[0] == "fetch" for event in harness.events) == 8
    assert not any(event[0] == "release" for event in harness.events)


@pytest.mark.asyncio
async def test_durable_completion_wins_a_concurrent_renewal_failure() -> None:
    harness = RuntimeHarness()
    release, durable = asyncio.Event(), asyncio.Event()

    async def complete(*_args, **_kwargs):
        await release.wait()
        durable.set()

    async def heartbeat(*_args, **_kwargs):
        release.set()
        await durable.wait()
        raise RuntimeError("synthetic renewal raced a committed result")

    runner = _RootRunner(
        identity(),
        snapshot(),
        config=enabled_config(),
        dependencies=replace(harness.dependencies(), complete_result=complete, heartbeat=heartbeat),
        database=object(),
    )
    assert await runner.process_claim({"session_id": 1}, harness._claims("baseline")["role"]) is None
    assert durable.is_set()
    assert not any(event[0] == "release" for event in harness.events)


@pytest.mark.asyncio
async def test_durable_error_keeps_its_failure_when_renewal_races_commit() -> None:
    harness = RuntimeHarness()
    release, durable = asyncio.Event(), asyncio.Event()

    async def fetch(*_args, **_kwargs):
        raise ProviderDirectoryRootedGraphHTTPError("resource_limit")

    async def complete(*_args, **_kwargs):
        await release.wait()
        durable.set()

    async def heartbeat(*_args, **_kwargs):
        release.set()
        await durable.wait()
        raise RuntimeError("synthetic renewal raced a committed error")

    runner = _RootRunner(
        identity(),
        snapshot(),
        config=enabled_config(),
        dependencies=replace(harness.dependencies(), fetch=fetch, complete_error=complete, heartbeat=heartbeat),
        database=object(),
    )
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError) as failed:
        await runner.process_claim({"session_id": 1}, harness._claims("baseline")["role"])
    assert failed.value.code == "root_unsealable" and durable.is_set()
    assert not any(event[0] == "release" for event in harness.events)


@pytest.mark.asyncio
async def test_root_deadline_bounds_shielded_terminal_write_without_resetting_budget() -> None:
    harness = RuntimeHarness()
    entered, drained = asyncio.Event(), asyncio.Event()

    async def complete(*_args, **_kwargs):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            drained.set()

    root = identity()
    await harness.initialize_root(root, database=object())
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError, match="cannot be sealed"):
        await asyncio.wait_for(
            run_root(
                root,
                snapshot(),
                config=replace(enabled_config(), root_timeout_seconds=0.04),
                dependencies=replace(harness.dependencies(), complete_result=complete),
                database=object(),
            ),
            1,
        )
    assert entered.is_set() and drained.is_set()
    assert any(event[0] == "release" for event in harness.events)
    assert not any(event[0] == "seal" for event in harness.events)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "invalid", "cancel"])
async def test_pure_reduction_renews_and_drains_its_thread(monkeypatch, outcome) -> None:
    from process import provider_directory_rooted_graph_acquisition_worker as worker

    harness = RuntimeHarness()
    claim = harness._claims("baseline")["role"]
    response = await harness.fetch({"session_id": 1}, API_BASE, claim, bounds=enabled_config().http_bounds())
    expected = worker.build_provider_directory_rooted_graph_query_result(
        claim,
        response.resources,
        advertised_total=response.advertised_total,
        terminal_page_count=response.terminal_page_count,
        reachable_network_references=(),
    )
    original = worker.build_provider_directory_rooted_graph_query_result
    loop = asyncio.get_running_loop()
    entered, release, drained = asyncio.Event(), threading.Event(), threading.Event()
    observed_results = []

    def build(*args, **kwargs):
        loop.call_soon_threadsafe(entered.set)
        try:
            assert release.wait(2), "test did not release pure reduction"
            if outcome == "invalid":
                raise ValueError("synthetic invalid result")
            return original(*args, **kwargs)
        finally:
            drained.set()

    async def complete(_claim, result, **_kwargs):
        observed_results.append(result)

    monkeypatch.setattr(worker, "build_provider_directory_rooted_graph_query_result", build)
    runner = _RootRunner(
        identity(),
        snapshot(),
        config=enabled_config(),
        dependencies=replace(harness.dependencies(), complete_result=complete),
        database=object(),
    )
    task = asyncio.create_task(runner.process_claim({"session_id": 1}, claim))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        async with asyncio.timeout(1):
            while harness.heartbeat_count == 0:
                await asyncio.sleep(0.001)
        if outcome == "cancel":
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done() and not drained.is_set()
        release.set()
        await _assert_reduction_outcome(task, outcome, harness, observed_results, expected)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
    assert drained.is_set()


async def _assert_reduction_outcome(task, outcome, harness, observed_results, expected):
    if outcome == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not observed_results and any(event[0] == "release" for event in harness.events)
    elif outcome == "invalid":
        with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError, match="cannot be sealed"):
            await task
        assert not observed_results and any(event[-1] == "response_invalid" for event in harness.events)
    else:
        await task
        assert observed_results == [expected]
