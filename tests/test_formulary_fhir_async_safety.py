# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cancellation contracts for long local formulary workers."""

from __future__ import annotations

import asyncio
import threading

import pytest

from process.formulary_fhir.async_safety import cancellable_to_thread


@pytest.mark.asyncio
async def test_cancellable_thread_stops_worker_before_propagating_cancel() -> None:
    """Async cancellation signals, drains, and then raises to its caller."""

    started = threading.Event()
    finished = threading.Event()

    def worker(*, cancel_check) -> None:
        started.set()
        try:
            while True:
                cancel_check()
        finally:
            finished.set()

    operation = asyncio.create_task(cancellable_to_thread(worker))
    await asyncio.to_thread(started.wait, 2.0)
    operation.cancel()

    with pytest.raises(asyncio.CancelledError):
        await operation
    assert finished.is_set()


@pytest.mark.asyncio
async def test_repeated_cancel_does_not_abandon_worker_cleanup() -> None:
    """Repeated cancellation cannot interrupt the worker-drain loop."""

    started = threading.Event()
    release_cleanup = threading.Event()
    finished = threading.Event()

    def worker(*, cancel_check) -> None:
        started.set()
        try:
            while True:
                cancel_check()
        finally:
            release_cleanup.wait(2.0)
            finished.set()

    operation = asyncio.create_task(cancellable_to_thread(worker))
    await asyncio.to_thread(started.wait, 2.0)
    operation.cancel()
    await asyncio.sleep(0)
    operation.cancel()
    release_cleanup.set()

    with pytest.raises(asyncio.CancelledError):
        await operation
    assert finished.is_set()
