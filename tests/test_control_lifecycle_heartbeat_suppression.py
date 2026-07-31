# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from process import control_lifecycle


async def _run_one_heartbeat_tick(
    monkeypatch: pytest.MonkeyPatch,
    *,
    run_id: str,
) -> None:
    sleep_call_counts = [0]

    async def one_tick_then_stop(_interval: float) -> None:
        sleep_call_counts[0] += 1
        if sleep_call_counts[0] > 1:
            raise asyncio.CancelledError

    monkeypatch.setenv(
        "HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS",
        "1",
    )
    monkeypatch.setattr(
        control_lifecycle.asyncio,
        "sleep",
        one_tick_then_stop,
    )
    with pytest.raises(asyncio.CancelledError):
        await control_lifecycle._live_progress_heartbeat(
            run_id,
            "provider_directory_fhir",
            "provider_directory_fhir_import",
            "2026-07-30T00:00:00+00:00",
        )


def _patch_in_flight_heartbeat_collaborators(
    monkeypatch: pytest.MonkeyPatch,
    *,
    heartbeat_sleep,
    is_heartbeat_persisted,
    emitted_events: list[dict[str, object]],
) -> None:
    """Install the clock, persistence, and progress seams for the barrier test."""
    monkeypatch.setenv(
        "HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS",
        "1",
    )
    monkeypatch.setattr(
        control_lifecycle.asyncio,
        "sleep",
        heartbeat_sleep,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "_is_control_run_heartbeat_persisted",
        is_heartbeat_persisted,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "enqueue_live_progress",
        lambda **event_by_field: emitted_events.append(event_by_field),
    )


@pytest.mark.asyncio
async def test_suppression_waits_for_in_flight_heartbeat_persistence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wait for an in-flight write before entering the suppression barrier."""
    persistence_started = asyncio.Event()
    release_persistence = asyncio.Event()
    suppression_entered = asyncio.Event()
    release_suppression = asyncio.Event()
    block_next_tick = asyncio.Event()
    event_list: list[dict[str, object]] = []
    sleep_call_counts = [0]

    async def heartbeat_sleep(_interval: float) -> None:
        sleep_call_counts[0] += 1
        if sleep_call_counts[0] > 1:
            await block_next_tick.wait()

    async def is_heartbeat_persisted(*_args, **_kwargs) -> bool:
        persistence_started.set()
        await release_persistence.wait()
        return True
    async def hold_suppression() -> None:
        async with (
            control_lifecycle.suppress_control_run_heartbeat_persistence(
                "run-in-flight"
            )
        ):
            suppression_entered.set()
            await release_suppression.wait()

    _patch_in_flight_heartbeat_collaborators(
        monkeypatch,
        heartbeat_sleep=heartbeat_sleep,
        is_heartbeat_persisted=is_heartbeat_persisted,
        emitted_events=event_list,
    )

    heartbeat_task = asyncio.create_task(
        control_lifecycle._live_progress_heartbeat(
            "run-in-flight", "provider_directory_fhir",
            "provider_directory_fhir_import", "2026-07-30T00:00:00+00:00",
        )
    )
    await persistence_started.wait()
    suppression_task = asyncio.create_task(hold_suppression())
    yielded = asyncio.get_running_loop().create_future()
    asyncio.get_running_loop().call_soon(yielded.set_result, None)
    await yielded

    assert suppression_entered.is_set() is False

    release_persistence.set()
    await suppression_entered.wait()
    release_suppression.set()
    await suppression_task
    heartbeat_task.cancel()
    await asyncio.gather(heartbeat_task, return_exceptions=True)

    assert sleep_call_counts[0] >= 2
    assert len(event_list) == 1


@pytest.mark.asyncio
async def test_suppression_preserves_live_heartbeat_without_database_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persist_heartbeat = AsyncMock(return_value=True)
    event_list: list[dict[str, object]] = []
    monkeypatch.setattr(
        control_lifecycle,
        "_is_control_run_heartbeat_persisted",
        persist_heartbeat,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "enqueue_live_progress",
        lambda **event_by_field: event_list.append(event_by_field),
    )

    async with control_lifecycle.suppress_control_run_heartbeat_persistence(
        "run-suppressed"
    ):
        await _run_one_heartbeat_tick(
            monkeypatch,
            run_id="run-suppressed",
        )

    persist_heartbeat.assert_not_awaited()
    assert len(event_list) == 1
    assert event_list[0]["run_id"] == "run-suppressed"
    assert event_list[0]["source"] == "engine-heartbeat"


@pytest.mark.asyncio
async def test_nested_suppression_is_reversible_only_after_outer_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persist_heartbeat = AsyncMock(return_value=True)
    event_list: list[dict[str, object]] = []
    monkeypatch.setattr(
        control_lifecycle,
        "_is_control_run_heartbeat_persisted",
        persist_heartbeat,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "enqueue_live_progress",
        lambda **event_by_field: event_list.append(event_by_field),
    )

    async with control_lifecycle.suppress_control_run_heartbeat_persistence(
        "run-nested"
    ):
        await _run_one_heartbeat_tick(monkeypatch, run_id="run-nested")
        async with (
            control_lifecycle.suppress_control_run_heartbeat_persistence(
                "run-nested"
            )
        ):
            await _run_one_heartbeat_tick(monkeypatch, run_id="run-nested")
        await _run_one_heartbeat_tick(monkeypatch, run_id="run-nested")

    await _run_one_heartbeat_tick(monkeypatch, run_id="run-nested")

    assert persist_heartbeat.await_count == 1
    assert len(event_list) == 4


@pytest.mark.asyncio
async def test_suppression_isolated_to_exact_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persist_heartbeat = AsyncMock(return_value=True)
    event_list: list[dict[str, object]] = []
    monkeypatch.setattr(
        control_lifecycle,
        "_is_control_run_heartbeat_persisted",
        persist_heartbeat,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "enqueue_live_progress",
        lambda **event_by_field: event_list.append(event_by_field),
    )

    async with control_lifecycle.suppress_control_run_heartbeat_persistence(
        "run-a"
    ):
        await _run_one_heartbeat_tick(monkeypatch, run_id="run-a")
        await _run_one_heartbeat_tick(monkeypatch, run_id="run-b")

    persist_heartbeat.assert_awaited_once()
    assert persist_heartbeat.await_args.args[:2] == (
        "run-b",
        "provider_directory_fhir_import",
    )
    assert [event["run_id"] for event in event_list] == ["run-a", "run-b"]
