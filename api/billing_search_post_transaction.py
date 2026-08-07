# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded read-only transaction setup for billing-search POST."""

from __future__ import annotations

from sqlalchemy import text

BILLING_SEARCH_LOCK_TIMEOUT_MS = 250
BILLING_SEARCH_STATEMENT_TIMEOUT_MS = 2_000

_READ_ONLY_SNAPSHOT_SQL = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
_LOCK_TIMEOUT_SQL = f"SET LOCAL lock_timeout = '{BILLING_SEARCH_LOCK_TIMEOUT_MS}ms'"
_STATEMENT_TIMEOUT_SQL = (
    f"SET LOCAL statement_timeout = '{BILLING_SEARCH_STATEMENT_TIMEOUT_MS}ms'"
)


async def configure_billing_search_read_snapshot(session) -> None:
    """Apply isolation and bounded PostgreSQL timeouts before the first read."""

    await session.execute(text(_READ_ONLY_SNAPSHOT_SQL))
    await session.execute(text(_LOCK_TIMEOUT_SQL))
    await session.execute(text(_STATEMENT_TIMEOUT_SQL))


__all__ = [
    "BILLING_SEARCH_LOCK_TIMEOUT_MS",
    "BILLING_SEARCH_STATEMENT_TIMEOUT_MS",
    "configure_billing_search_read_snapshot",
]
