# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cancellation and lock-lifetime proof for hospital-price imports."""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from typing import Any

import pytest

from tests.hospital_price_orchestration_support import (
    ArtifactStore,
    orchestrator_module,
)


_ArtifactStore = ArtifactStore
_orchestrator_module = orchestrator_module


def test_progress_reports_complete_empty_batch(monkeypatch):
    orchestrator = _orchestrator_module()
    events = []
    monkeypatch.setattr(
        orchestrator._runtime,
        "enqueue_live_progress",
        lambda **event_by_field: events.append(event_by_field),
    )

    orchestrator._progress(None, "complete", 0, 0, "complete")

    assert events[0]["pct"] == 100


def test_strict_positive_env_rejects_zero(monkeypatch):
    orchestrator = _orchestrator_module()
    monkeypatch.setenv("HOSPITAL_TEST_LIMIT", "0")

    with pytest.raises(RuntimeError, match="must be a positive integer"):
        orchestrator._runtime.strict_positive_env("HOSPITAL_TEST_LIMIT")


@pytest.mark.asyncio
async def test_cancellation_guard_returns_completed_operation():
    orchestrator = _orchestrator_module()

    async def operation():
        return "done"

    assert await orchestrator._guard_cancellation(
        {}, {}, operation(), [], "owner", 30, 10
    ) == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("heartbeat_seconds", [0, 1_000])
async def test_cancellation_monitor_checks_and_renews(
    monkeypatch, heartbeat_seconds
):
    orchestrator = _orchestrator_module()
    calls_by_kind = {"checks": 0, "renewals": 0}

    async def check_cancelled(*_args):
        calls_by_kind["checks"] += 1
        if calls_by_kind["checks"] == 2:
            raise RuntimeError("cancelled")

    async def renew(*_args, **_kwargs):
        calls_by_kind["renewals"] += 1

    async def operation():
        await asyncio.Future()

    monkeypatch.setattr(orchestrator._runtime, "positive_env", lambda *_args: 0)
    monkeypatch.setattr(orchestrator._runtime, "raise_if_cancelled", check_cancelled)
    monkeypatch.setattr(orchestrator._runtime, "renew_attempt_leases", renew)

    with pytest.raises(RuntimeError, match="cancelled"):
        await asyncio.wait_for(
            orchestrator._guard_cancellation(
                {}, {}, operation(), [], "owner", 30, heartbeat_seconds
            ),
            timeout=1,
        )
    assert calls_by_kind == {
        "checks": 2,
        "renewals": 1 if heartbeat_seconds == 0 else 0,
    }


@pytest.mark.asyncio
async def test_bounded_failure_cancels_and_drains_siblings():
    orchestrator = _orchestrator_module()
    slow_started, slow_cleaned = asyncio.Event(), asyncio.Event()

    async def operation(name: str) -> str:
        if name == "failure":
            await slow_started.wait()
            raise ValueError("failed")
        slow_started.set()
        try:
            await asyncio.Future()
        finally:
            slow_cleaned.set()

    with pytest.raises(ValueError, match="failed"):
        await orchestrator._bounded(("slow", "failure"), 2, operation)
    assert slow_cleaned.is_set()


@pytest.mark.asyncio
async def test_bounded_outer_cancel_does_not_interrupt_async_cleanup():
    orchestrator = _orchestrator_module()
    operation_started_by_name = {
        "fast": asyncio.Event(), "slow": asyncio.Event()
    }
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()
    cleanup_interrupted = asyncio.Event()

    async def operation(name: str) -> None:
        operation_started_by_name[name].set()
        try:
            await asyncio.Future()
        finally:
            if name == "slow":
                cleanup_started.set()
                try:
                    await allow_cleanup.wait()
                    cleanup_finished.set()
                except asyncio.CancelledError:
                    cleanup_interrupted.set()
                    raise

    bounded_task = asyncio.create_task(
        orchestrator._bounded(("fast", "slow"), 2, operation)
    )
    await asyncio.gather(
        *(started.wait() for started in operation_started_by_name.values())
    )
    bounded_task.cancel()
    await cleanup_started.wait()
    await asyncio.sleep(0)
    bounded_task.cancel()
    allow_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await bounded_task

    assert cleanup_finished.is_set()
    assert not cleanup_interrupted.is_set()


@pytest.mark.asyncio
async def test_resource_lock_releases_after_repeated_cancellation(tmp_path):
    orchestrator = _orchestrator_module()
    acquire_started = threading.Event()
    allow_acquire = threading.Event()

    class Lock:
        releases = 0

        def try_acquire(self) -> "Lock | None":
            acquire_started.set()
            assert allow_acquire.wait(timeout=1)
            return self

        def release(self) -> None:
            self.releases += 1

    lock = Lock()
    store = _ArtifactStore(tmp_path)
    store.named_lock = lambda *_args: lock

    async def enter_lock() -> None:
        async with orchestrator._hospital_resource_slot(store, "load", 1):
            raise AssertionError("cancelled acquisition entered the lock")

    lock_task = asyncio.create_task(enter_lock())
    assert await asyncio.to_thread(acquire_started.wait, 1)
    lock_task.cancel()
    await asyncio.sleep(0)
    lock_task.cancel()
    allow_acquire.set()

    with pytest.raises(asyncio.CancelledError):
        await lock_task
    assert lock.releases == 1


@pytest.mark.asyncio
async def test_cancellation_guard_drains_operation_after_repeated_cancel():
    orchestrator = _orchestrator_module()
    operation_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def operation() -> None:
        operation_started.set()
        try:
            await asyncio.Future()
        finally:
            cleanup_started.set()
            await allow_cleanup.wait()
            cleanup_finished.set()

    guarded = asyncio.create_task(
        orchestrator._guard_cancellation(
            {}, {}, operation(), [], "hospital-prices:test", 300, 60
        )
    )
    await operation_started.wait()
    guarded.cancel()
    await cleanup_started.wait()
    guarded.cancel()
    await asyncio.sleep(0)
    assert not guarded.done()
    allow_cleanup.set()

    with pytest.raises(asyncio.CancelledError):
        await guarded
    assert cleanup_finished.is_set()


@pytest.mark.asyncio
async def test_cancelled_format_detection_is_drained(tmp_path, monkeypatch):
    orchestrator = _orchestrator_module()
    detection_started = threading.Event()
    allow_detection = threading.Event()

    def detect_format(*_args: Any) -> str:
        detection_started.set()
        assert allow_detection.wait(timeout=1)
        return "json"

    monkeypatch.setattr(orchestrator, "detect_hospital_mrf_format", detect_format)
    raw = SimpleNamespace(
        raw_sha256="a" * 64, raw_path=str(tmp_path / "source.json"), byte_count=2
    )
    operation = asyncio.create_task(
        orchestrator._ensure_content(
            {}, {}, _ArtifactStore(tmp_path), raw, 2048, 1024, 1
        )
    )
    assert await asyncio.to_thread(detection_started.wait, 1)

    operation.cancel()
    await asyncio.sleep(0)
    assert not operation.done()
    operation.cancel()
    allow_detection.set()

    with pytest.raises(asyncio.CancelledError):
        await operation
