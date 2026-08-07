# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cancellation and defensive branches for the manual formulary adapter."""

from __future__ import annotations

import asyncio

import pytest

import process.formulary_fhir.manual_worker as manual_module
from process.formulary_fhir.manual_worker import MANUAL_SYNC_ENABLED_ENV
from process.formulary_fhir.manual_worker import ManualSynchronizationError
from process.formulary_fhir.manual_worker import (
    synchronize_verified_dataset_manually,
)
from tests.test_formulary_fhir_manual_worker import CUTOFF
from tests.test_formulary_fhir_manual_worker import _database
from tests.test_formulary_fhir_manual_worker import _result


def test_error_fallback_and_z_cutoff_are_stable():
    fallback_error = ManualSynchronizationError("private-error")

    assert fallback_error.code == "lock_unavailable"
    assert "private-error" not in str(fallback_error)
    assert manual_module._normalized_cutoff("2026-08-07T12:00:00Z") == CUTOFF


@pytest.mark.asyncio
async def test_drain_propagates_cleanup_task_failure():
    original_error = RuntimeError("cleanup failed")

    async def fail_cleanup():
        raise original_error

    with pytest.raises(RuntimeError) as caught:
        await manual_module._drain(
            fail_cleanup(),
            preserve_cancellation=False,
        )

    assert caught.value is original_error


@pytest.mark.asyncio
async def test_drain_preserves_cancellation_over_cleanup_failure():
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def fail_cleanup():
        cleanup_started.set()
        await release_cleanup.wait()
        raise RuntimeError("cleanup failed")

    drain_task = asyncio.create_task(
        manual_module._drain(
            fail_cleanup(),
            preserve_cancellation=True,
        )
    )
    await cleanup_started.wait()
    drain_task.cancel()
    await asyncio.sleep(0)
    release_cleanup.set()

    with pytest.raises(asyncio.CancelledError):
        await drain_task


@pytest.mark.asyncio
async def test_invalid_sync_result_invalidates_source_lock(monkeypatch):
    monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, "true")
    database, events = _database()

    async def invalid_sync(**_values):
        return object()

    monkeypatch.setattr(manual_module, "synchronize_verified_dataset", invalid_sync)
    with pytest.raises(ManualSynchronizationError) as caught:
        await synchronize_verified_dataset_manually(
            source_id="source-alpha",
            run_id="synthetic-run",
            cutoff=CUTOFF,
            timeout_seconds=10,
            database=database,
        )

    assert caught.value.code == "invalid_result"
    assert "driver-invalidate" in events
    assert not any(
        "pg_advisory_unlock" in event[1]
        for event in events
        if isinstance(event, tuple)
    )


@pytest.mark.asyncio
async def test_cancellation_while_waiting_for_lock_invalidates_driver(monkeypatch):
    monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, "true")
    monkeypatch.setattr(manual_module, "LOCK_WAIT_SECONDS", 10)
    monkeypatch.setattr(manual_module, "LOCK_RETRY_SECONDS", 10)
    try_lock_started = asyncio.Event()
    database, events = _database(
        try_lock_results=[False],
        try_lock_started=try_lock_started,
    )
    synchronization_task = asyncio.create_task(
        synchronize_verified_dataset_manually(
            source_id="source-alpha",
            run_id="synthetic-run",
            cutoff=CUTOFF,
            timeout_seconds=10,
            database=database,
        )
    )
    await try_lock_started.wait()
    synchronization_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await synchronization_task

    assert "driver-invalidate" in events


@pytest.mark.asyncio
async def test_cancellation_during_normal_close_is_drained(monkeypatch):
    monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, "true")
    exit_started = asyncio.Event()
    release_exit = asyncio.Event()
    database, events = _database(
        exit_started=exit_started,
        release_exit=release_exit,
    )

    async def synchronize(**_values):
        return _result()

    monkeypatch.setattr(manual_module, "synchronize_verified_dataset", synchronize)
    synchronization_task = asyncio.create_task(
        synchronize_verified_dataset_manually(
            source_id="source-alpha",
            run_id="synthetic-run",
            cutoff=CUTOFF,
            timeout_seconds=10,
            database=database,
        )
    )
    await exit_started.wait()
    synchronization_task.cancel()
    await asyncio.sleep(0)
    synchronization_task.cancel()
    release_exit.set()

    with pytest.raises(asyncio.CancelledError):
        await synchronization_task

    assert "driver-close" in events
    assert "driver-invalidate" not in events
    assert any(
        "pg_advisory_unlock" in event[1]
        for event in events
        if isinstance(event, tuple)
    )


@pytest.mark.asyncio
async def test_connection_close_failure_is_sanitized(monkeypatch):
    monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, "true")
    database, events = _database(exit_error=RuntimeError("private connection"))

    async def synchronize(**_values):
        return _result()

    monkeypatch.setattr(manual_module, "synchronize_verified_dataset", synchronize)
    with pytest.raises(ManualSynchronizationError) as caught:
        await synchronize_verified_dataset_manually(
            source_id="source-alpha",
            run_id="synthetic-run",
            cutoff=CUTOFF,
            timeout_seconds=10,
            database=database,
        )

    assert caught.value.code == "cleanup"
    assert "private connection" not in str(caught.value)
    assert "driver-close" not in events


@pytest.mark.asyncio
async def test_unlock_operation_failure_is_sanitized(monkeypatch):
    monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, "true")
    database, events = _database(
        unlock_result=RuntimeError("private unlock failure")
    )

    async def synchronize(**_values):
        return _result()

    monkeypatch.setattr(manual_module, "synchronize_verified_dataset", synchronize)
    with pytest.raises(ManualSynchronizationError) as caught:
        await synchronize_verified_dataset_manually(
            source_id="source-alpha",
            run_id="synthetic-run",
            cutoff=CUTOFF,
            timeout_seconds=10,
            database=database,
        )

    assert caught.value.code == "cleanup"
    assert "private unlock" not in str(caught.value)
    assert "driver-invalidate" in events
