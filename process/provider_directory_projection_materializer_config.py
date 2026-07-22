# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validated off-by-default runtime settings for native projection workers."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Mapping

from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)


@dataclass(frozen=True)
class ProjectionMaterializerSettings:
    enabled: bool | None
    max_workers: int | None
    native_threads: int | None
    lease_seconds: int
    heartbeat_seconds: float
    native_timeout_seconds: float | None
    executable: str | None


def is_projection_materializer_enabled(enabled: bool | None = None) -> bool:
    """Require an explicit opt-in or the exact dedicated runtime flag."""

    if enabled is not None:
        if type(enabled) is not bool:
            raise ProviderDirectoryProjectionError(
                "provider_directory_projection_native_enabled_invalid"
            )
        return enabled
    return os.getenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_NATIVE_ENABLED",
        "",
    ).strip().lower() in {"1", "true", "yes", "on"}


def projection_materializer_worker_count(max_workers: int | None = None) -> int:
    """Resolve the explicit aggregate native-memory concurrency bound."""

    raw_worker_count: Any = max_workers
    if raw_worker_count is None:
        raw_worker_count = os.getenv(
            "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_WORKERS",
            "4",
        )
    try:
        worker_count = int(raw_worker_count)
    except (TypeError, ValueError) as error:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_worker_count_invalid"
        ) from error
    if not 1 <= worker_count <= 4:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_worker_count_invalid"
        )
    return worker_count


def projection_native_timeout_seconds(timeout_seconds: float | None = None) -> float:
    """Resolve one bounded wall-clock limit for a native child."""

    raw_timeout: Any = timeout_seconds
    if raw_timeout is None:
        raw_timeout = os.getenv(
            "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_NATIVE_TIMEOUT_SECONDS",
            "600",
        )
    try:
        timeout = float(raw_timeout)
    except (TypeError, ValueError) as error:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_timeout_invalid"
        ) from error
    if not 30 <= timeout <= 3600:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_timeout_invalid"
        )
    return timeout


def projection_materializer_settings(
    materializer_options_map: Mapping[str, Any],
) -> ProjectionMaterializerSettings:
    """Validate the complete keyword-only orchestration option set."""

    allowed_options = {
        "enabled",
        "max_workers",
        "native_threads",
        "lease_seconds",
        "heartbeat_seconds",
        "native_timeout_seconds",
        "executable",
    }
    if set(materializer_options_map) - allowed_options:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_materializer_options_invalid"
        )
    lease_seconds = materializer_options_map.get("lease_seconds", 300)
    heartbeat_seconds = materializer_options_map.get("heartbeat_seconds", 30)
    if (
        type(lease_seconds) is not int
        or not 30 <= lease_seconds <= 3600
        or isinstance(heartbeat_seconds, bool)
        or not isinstance(heartbeat_seconds, (int, float))
        or not 0 < heartbeat_seconds <= lease_seconds / 2
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_heartbeat_interval_invalid"
        )
    return ProjectionMaterializerSettings(
        enabled=materializer_options_map.get("enabled"),
        max_workers=materializer_options_map.get("max_workers"),
        native_threads=materializer_options_map.get("native_threads"),
        lease_seconds=lease_seconds,
        heartbeat_seconds=float(heartbeat_seconds),
        native_timeout_seconds=materializer_options_map.get("native_timeout_seconds"),
        executable=materializer_options_map.get("executable"),
    )


__all__ = (
    "ProjectionMaterializerSettings",
    "is_projection_materializer_enabled",
    "projection_materializer_settings",
    "projection_materializer_worker_count",
    "projection_native_timeout_seconds",
)
