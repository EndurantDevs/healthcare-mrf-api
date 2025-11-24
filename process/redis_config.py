# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Helpers for constructing Redis connection settings."""

from __future__ import annotations

import os

from arq.connections import RedisSettings


def build_redis_settings() -> RedisSettings:
    """
    Resolve Redis connection details from environment variables.

    Prefers the single-string DSN (`HLTHPRT_REDIS_ADDRESS`), otherwise falls back to
    individual pieces (`HLTHPRT_REDIS_HOST`, `HLTHPRT_REDIS_PORT`, etc.).
    """
    dsn = os.getenv("HLTHPRT_REDIS_ADDRESS")
    if dsn:
        return RedisSettings.from_dsn(dsn)

    return RedisSettings(
        host=os.getenv("HLTHPRT_REDIS_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_REDIS_PORT", "6379")),
        password=os.getenv("HLTHPRT_REDIS_PASSWORD") or None,
        database=int(os.getenv("HLTHPRT_REDIS_DB", "0")),
    )


__all__ = ["build_redis_settings"]
