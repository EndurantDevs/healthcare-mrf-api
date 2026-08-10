# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared cancellation-safe draining for formulary cleanup operations."""

from __future__ import annotations

import asyncio
import threading
from typing import Any


async def drain_operation(
    operation: Any,
    *,
    preserve_cancellation: bool,
    should_prefer_operation_error: bool = False,
) -> Any:
    """Drain one operation through repeated outer cancellation."""

    operation_task = asyncio.create_task(operation)
    cancellation_error: asyncio.CancelledError | None = None
    while not operation_task.done():
        try:
            await asyncio.shield(operation_task)
        except asyncio.CancelledError as error:
            if cancellation_error is None:
                cancellation_error = error
        except BaseException:
            break
    try:
        operation_result = operation_task.result()
    except BaseException:
        if (
            cancellation_error is not None
            and preserve_cancellation
            and not should_prefer_operation_error
        ):
            raise cancellation_error
        raise
    if cancellation_error is not None and preserve_cancellation:
        raise cancellation_error
    return operation_result


class CooperativeThreadCancellation(RuntimeError):
    """Stop one local worker after its owning async task was cancelled."""


async def cancellable_to_thread(
    operation: Any,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Run a cooperative worker and drain it through repeated cancellation."""

    cancellation_signal = threading.Event()

    def cancel_check() -> None:
        """Raise inside the worker once its owning async task is cancelled."""

        if cancellation_signal.is_set():
            raise CooperativeThreadCancellation(
                "formulary local operation was cancelled"
            )

    operation_task = asyncio.create_task(
        asyncio.to_thread(
            operation,
            *args,
            cancel_check=cancel_check,
            **kwargs,
        )
    )
    cancellation_error: asyncio.CancelledError | None = None
    while not operation_task.done():
        try:
            await asyncio.shield(operation_task)
        except asyncio.CancelledError as error:
            cancellation_signal.set()
            if cancellation_error is None:
                cancellation_error = error
        except BaseException:
            break
    try:
        operation_result = operation_task.result()
    except CooperativeThreadCancellation:
        if cancellation_error is not None:
            raise cancellation_error
        raise
    except BaseException:
        if cancellation_error is not None:
            raise cancellation_error
        raise
    if cancellation_error is not None:
        raise cancellation_error
    return operation_result


__all__ = (
    "CooperativeThreadCancellation",
    "cancellable_to_thread",
    "drain_operation",
)
