# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cancellation-safe source advisory lease for manual formulary operations."""

from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

from process.formulary_fhir.async_safety import drain_operation


LOCK_DOMAIN = "fhir-formulary-manual-sync-v1"
TRY_LOCK_SQL = (
    "SELECT pg_try_advisory_lock(hashtextextended($1::text, 0::bigint));"
)
UNLOCK_SQL = "SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint));"
LOCK_ERROR_CODES = frozenset({"busy", "cleanup", "lock_unavailable"})


class ManualSourceLockError(RuntimeError):
    """Carry one sanitized source-lease failure code."""

    def __init__(self, code: str) -> None:
        self.code = code if code in LOCK_ERROR_CODES else "lock_unavailable"
        super().__init__("FHIR formulary manual source lease failed")


@dataclass(slots=True, repr=False)
class _DriverLease:
    """Retain one advisory lock on one entered driver connection manager."""

    manager: Any = field(repr=False)
    driver: Any = field(repr=False)
    identity: str = field(repr=False)


def _lock_identity(source_id: str) -> str:
    return f"{LOCK_DOMAIN}:{source_id}"


_drain = drain_operation


async def _exit_lease(
    lease: _DriverLease,
    error: BaseException | None,
    *,
    preserve_cancellation: bool,
) -> None:
    error_type = type(error) if error is not None else None
    error_traceback = error.__traceback__ if error is not None else None
    await _drain(
        lease.manager.__aexit__(error_type, error, error_traceback),
        preserve_cancellation=preserve_cancellation,
    )


async def _discard_entered_manager(
    manager: Any,
    error: BaseException,
) -> None:
    with contextlib.suppress(BaseException):
        await _drain(
            manager.__aexit__(type(error), error, error.__traceback__),
            preserve_cancellation=False,
        )


async def _acquire_source_lease(
    database: Any,
    source_id: str,
    *,
    wait_seconds: float,
    retry_seconds: float,
) -> _DriverLease:
    manager = database.acquire_driver()
    identity = _lock_identity(source_id)
    try:
        async with asyncio.timeout(wait_seconds):
            driver = await manager.__aenter__()
            while True:
                acquired = await driver.fetchval(TRY_LOCK_SQL, identity)
                if acquired is True:
                    return _DriverLease(manager, driver, identity)
                await asyncio.sleep(retry_seconds)
    except asyncio.CancelledError as error:
        await _discard_entered_manager(manager, error)
        raise
    except TimeoutError as error:
        await _discard_entered_manager(manager, error)
        raise ManualSourceLockError("busy") from None
    except Exception as error:
        await _discard_entered_manager(manager, error)
        raise ManualSourceLockError("lock_unavailable") from None


async def _release_successful_lease(lease: _DriverLease) -> None:
    try:
        released = await _drain(
            lease.driver.fetchval(UNLOCK_SQL, lease.identity),
            preserve_cancellation=True,
        )
        if released is not True:
            raise ManualSourceLockError("cleanup")
    except asyncio.CancelledError as error:
        with contextlib.suppress(BaseException):
            await _exit_lease(lease, error, preserve_cancellation=False)
        raise
    except BaseException as error:
        with contextlib.suppress(BaseException):
            await _exit_lease(lease, error, preserve_cancellation=False)
        if isinstance(error, ManualSourceLockError):
            raise
        raise ManualSourceLockError("cleanup") from None
    try:
        await _exit_lease(lease, None, preserve_cancellation=True)
    except asyncio.CancelledError:
        raise
    except Exception:
        raise ManualSourceLockError("cleanup") from None


@asynccontextmanager
async def manual_source_lease(
    database: Any,
    source_id: str,
    *,
    wait_seconds: float,
    retry_seconds: float,
) -> AsyncIterator[None]:
    """Hold one source-scoped session lock and drain every exit path."""

    lease = await _acquire_source_lease(
        database,
        source_id,
        wait_seconds=wait_seconds,
        retry_seconds=retry_seconds,
    )
    try:
        yield
    except BaseException as error:
        with contextlib.suppress(BaseException):
            await _exit_lease(lease, error, preserve_cancellation=False)
        raise
    else:
        await _release_successful_lease(lease)


__all__ = ("ManualSourceLockError", "manual_source_lease")
