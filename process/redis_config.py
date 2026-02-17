# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Helpers for constructing Redis connection settings."""

from __future__ import annotations

import os

from arq.connections import RedisSettings


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return max(default, minimum)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return max(default, minimum)
    return max(value, minimum)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


def build_redis_settings() -> RedisSettings:
    """
    Resolve Redis connection details from environment variables.

    Prefers the single-string DSN (`HLTHPRT_REDIS_ADDRESS`), otherwise falls back to
    individual pieces (`HLTHPRT_REDIS_HOST`, `HLTHPRT_REDIS_PORT`, etc.).
    """
    dsn = os.getenv("HLTHPRT_REDIS_ADDRESS")
    if dsn:
        settings = RedisSettings.from_dsn(dsn)
    else:
        settings = RedisSettings(
            host=os.getenv("HLTHPRT_REDIS_HOST", "127.0.0.1"),
            port=_env_int("HLTHPRT_REDIS_PORT", 6379, minimum=1),
            password=os.getenv("HLTHPRT_REDIS_PASSWORD") or None,
            database=_env_int("HLTHPRT_REDIS_DB", 0, minimum=0),
        )

    settings.conn_timeout = _env_int(
        "HLTHPRT_REDIS_CONN_TIMEOUT_SECONDS",
        max(settings.conn_timeout if settings.conn_timeout is not None else 1, 10),
        minimum=1,
    )
    settings.conn_retries = _env_int(
        "HLTHPRT_REDIS_CONN_RETRIES",
        max(settings.conn_retries if settings.conn_retries is not None else 5, 10),
        minimum=1,
    )
    settings.conn_retry_delay = _env_int(
        "HLTHPRT_REDIS_CONN_RETRY_DELAY_SECONDS",
        settings.conn_retry_delay if settings.conn_retry_delay is not None else 1,
        minimum=0,
    )
    max_connections_raw = os.getenv("HLTHPRT_REDIS_MAX_CONNECTIONS")
    if max_connections_raw not in (None, ""):
        settings.max_connections = _env_int(
            "HLTHPRT_REDIS_MAX_CONNECTIONS",
            settings.max_connections if settings.max_connections is not None else 100,
            minimum=1,
        )
    settings.retry_on_timeout = _env_bool("HLTHPRT_REDIS_RETRY_ON_TIMEOUT", default=True)
    return settings


__all__ = ["build_redis_settings"]
