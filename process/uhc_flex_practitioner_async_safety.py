# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cancellation-safe draining for Flex Practitioner lease operations."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any


async def drain_operation(
    operation: Awaitable[Any],
    *,
    preserve_cancellation: bool,
) -> Any:
    """Shield and drain one fence-changing operation through cancellation."""

    operation_task = asyncio.create_task(operation)
    cancellation: asyncio.CancelledError | None = None
    while not operation_task.done():
        try:
            await asyncio.shield(operation_task)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error
        except BaseException:
            break
    if cancellation is not None and preserve_cancellation:
        if not operation_task.cancelled():
            operation_task.exception()
        raise cancellation
    return operation_task.result()


__all__ = ("drain_operation",)
