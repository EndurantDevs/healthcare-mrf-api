# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from dataclasses import replace

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
from tests.provider_directory_rooted_graph_acquisition_test_support import (
    identity,
    snapshot,
)
from tests.provider_directory_rooted_graph_runtime_test_support import (
    RuntimeHarness,
    enabled_config,
)


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
    release_index = next(
        index for index, event in enumerate(harness.events) if event[0] == "release"
    )
    sleep_index = next(
        index for index, event in enumerate(harness.events) if event[0] == "sleep"
    )
    assert release_index < sleep_index
    attempts = [
        event[4]
        for event in harness.events
        if event[0] == "fetch" and event[3] == role_claim.query_id
    ]
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
