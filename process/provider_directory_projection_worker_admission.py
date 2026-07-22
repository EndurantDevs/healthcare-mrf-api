# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Process-local worker admission that reserves one database connection."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
from typing import Any

from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)


_ADMISSION_ATTRIBUTE = "_healthporta_provider_directory_projection_admission"


@dataclass(frozen=True)
class ProjectionWorkerAdmission:
    """One event-loop-bound gate shared by all sources using a database pool."""

    event_loop: asyncio.AbstractEventLoop
    worker_limit: int
    semaphore: asyncio.Semaphore


def _database_pool_capacity() -> int:
    raw_pool_capacity = os.getenv("HLTHPRT_DB_POOL_MAX_SIZE", "5")
    try:
        pool_capacity = int(raw_pool_capacity)
    except ValueError as error:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_database_pool_capacity_invalid"
        ) from error
    if not 2 <= pool_capacity <= 256:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_database_pool_capacity_invalid"
        )
    return pool_capacity


def _configured_worker_limit() -> int:
    raw_worker_limit = os.getenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_GLOBAL_WORKERS",
        "4",
    )
    try:
        worker_limit = int(raw_worker_limit)
    except ValueError as error:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_global_worker_limit_invalid"
        ) from error
    if not 1 <= worker_limit <= 4:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_global_worker_limit_invalid"
        )
    return worker_limit


def projection_global_worker_limit() -> int:
    """Bound aggregate source workers while reserving one heartbeat connection."""

    return min(_configured_worker_limit(), _database_pool_capacity() - 1)


def projection_worker_admission(database: Any) -> ProjectionWorkerAdmission:
    """Return the loop-local gate shared by concurrent calls on one database."""

    event_loop = asyncio.get_running_loop()
    worker_limit = projection_global_worker_limit()
    current_admission = getattr(database, _ADMISSION_ATTRIBUTE, None)
    if (
        isinstance(current_admission, ProjectionWorkerAdmission)
        and current_admission.event_loop is event_loop
    ):
        if current_admission.worker_limit != worker_limit:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_global_worker_limit_changed"
            )
        return current_admission
    admission = ProjectionWorkerAdmission(
        event_loop,
        worker_limit,
        asyncio.Semaphore(worker_limit),
    )
    try:
        setattr(database, _ADMISSION_ATTRIBUTE, admission)
    except (AttributeError, TypeError) as error:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_worker_admission_unavailable"
        ) from error
    return admission


__all__ = (
    "ProjectionWorkerAdmission",
    "projection_global_worker_limit",
    "projection_worker_admission",
)
