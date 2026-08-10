# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Two-connection PostgreSQL proof for the terminal transition lock."""

from __future__ import annotations

import asyncio
import os

import asyncpg

from tests.provider_directory_subset_completion_pg_setup import (
    MigrationSqlCapture,
)
from tests.tin_npi_connector_postgres_support import POSTGRES_DSN_ENV


def _upgrade_statements(migration) -> list[str]:
    capture = MigrationSqlCapture()
    migration.op = capture
    migration.upgrade()
    return capture.statements


async def _execute_upgrade(scenario, statements) -> None:
    async with scenario.connection.transaction():
        for statement in statements:
            await scenario.connection.execute(statement)


async def _wait_for_retry_sleep(observer, backend_pid: int) -> None:
    for _attempt in range(100):
        wait_event = await observer.fetchval(
            """
            SELECT wait_event
              FROM pg_catalog.pg_stat_activity
             WHERE pid = $1
            """,
            backend_pid,
        )
        if wait_event == "PgSleep":
            return
        await asyncio.sleep(0.05)
    raise AssertionError("scope-binding migration did not enter bounded retry")


async def assert_upgrade_retries_held_dataset_write(
    scenario,
    migration,
) -> None:
    """Hold RowExclusive first, then prove bounded retry and clean upgrade."""

    holder = await asyncpg.connect(os.environ[POSTGRES_DSN_ENV])
    holder_transaction = holder.transaction()
    migration_task = None
    try:
        await holder_transaction.start()
        await holder.execute(
            f"""
            UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
               SET resource_count = resource_count
             WHERE dataset_id = 'dataset-a'
            """
        )
        backend_pid = await scenario.connection.fetchval(
            "SELECT pg_catalog.pg_backend_pid()"
        )
        migration_task = asyncio.create_task(
            _execute_upgrade(scenario, _upgrade_statements(migration))
        )
        await _wait_for_retry_sleep(holder, backend_pid)
        assert migration_task.done() is False
        await holder_transaction.commit()
        await asyncio.wait_for(migration_task, timeout=10)
        migration_task = None
    finally:
        if migration_task is not None:
            migration_task.cancel()
            await asyncio.gather(migration_task, return_exceptions=True)
        if holder.is_in_transaction():
            await holder_transaction.rollback()
        await holder.close()


__all__ = ("assert_upgrade_retries_held_dataset_write",)
