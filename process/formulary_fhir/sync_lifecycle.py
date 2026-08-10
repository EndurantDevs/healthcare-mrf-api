# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared cancellation-safe failure lifecycle for formulary synchronizers."""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
from sqlalchemy import exc as sqlalchemy_error

from process.formulary_fhir.async_safety import drain_operation
from process.formulary_fhir.continuation import FHIRTransportError
from process.formulary_fhir.repository import DatasetRef


def is_resumable_synchronization_error(error: BaseException) -> bool:
    """Classify only cancellation and explicitly transient infrastructure errors."""

    if isinstance(error, (asyncio.CancelledError, TimeoutError)):
        return True
    if isinstance(error, FHIRTransportError):
        return error.is_transient is True
    if getattr(error, "retryable", False) is True:
        return True
    transient_database_errors = (
        asyncpg.CannotConnectNowError,
        asyncpg.DeadlockDetectedError,
        asyncpg.PostgresConnectionError,
        asyncpg.SerializationError,
        asyncpg.TooManyConnectionsError,
    )
    if isinstance(error, transient_database_errors):
        return True
    if isinstance(
        error,
        (
            sqlalchemy_error.DisconnectionError,
            sqlalchemy_error.InterfaceError,
            sqlalchemy_error.OperationalError,
            sqlalchemy_error.TimeoutError,
        ),
    ):
        return True
    return isinstance(getattr(error, "orig", None), transient_database_errors)


async def shield_synchronization_lifecycle(update: Any) -> None:
    """Drain and suppress a secondary lifecycle failure or repeated cancel."""

    try:
        await drain_operation(update, preserve_cancellation=False)
    except BaseException:
        return


async def record_synchronization_failure(
    repository: Any,
    dataset: DatasetRef,
    error: BaseException,
) -> None:
    """Record resumable interruption or terminal failure without masking cause."""

    update = (
        repository.interrupt_dataset(dataset, error)
        if is_resumable_synchronization_error(error)
        else repository.fail_dataset(dataset, error)
    )
    await shield_synchronization_lifecycle(update)


__all__ = (
    "is_resumable_synchronization_error",
    "record_synchronization_failure",
    "shield_synchronization_lifecycle",
)
