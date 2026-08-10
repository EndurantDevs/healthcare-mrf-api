# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL lock-retry proof for the terminal-window migration."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass

from tests.provider_directory_subset_completion_pg_setup import (
    MigrationSqlCapture,
)
from tests.tin_npi_connector_postgres_support import open_test_connection


@dataclass(frozen=True)
class TerminalWindowLockContract:
    """Rendered lock statement and the exact guarded relations."""

    checkpoint_ref: str
    dataset_ref: str
    relation_names: tuple[str, ...]
    lock_sql: str


def _lock_contract(scenario, migration) -> TerminalWindowLockContract:
    subset = migration._subset()
    abandonment = migration._abandonment()
    relation_names = (
        subset._ENDPOINT_DATASET,
        subset._DATASET_RESOURCE,
        subset._SOURCE,
        abandonment._PROOF_SHARD,
        abandonment._CHECKPOINT,
        abandonment._BULK_CHECKPOINT,
    )
    recorder = MigrationSqlCapture()
    migration.op = recorder
    migration._lock_relations(scenario.schema)
    assert len(recorder.statements) == 1
    return TerminalWindowLockContract(
        checkpoint_ref=subset._qf(
            scenario.schema,
            abandonment._CHECKPOINT,
        ),
        dataset_ref=subset._qf(
            scenario.schema,
            subset._ENDPOINT_DATASET,
        ),
        relation_names=relation_names,
        lock_sql=recorder.statements[0],
    )


async def _has_sleeping_backend(connection, backend_pid, task) -> bool:
    for _attempt in range(100):
        if task.done():
            return False
        is_sleeping = await connection.fetchval(
            """
            SELECT state = 'active' AND wait_event = 'PgSleep'
              FROM pg_catalog.pg_stat_activity
             WHERE pid = $1
            """,
            backend_pid,
        )
        if is_sleeping:
            return True
        await asyncio.sleep(0.01)
    return False


async def _granted_exclusive_relation_names(
    connection,
    backend_pid,
    schema,
    relation_names,
) -> set[str]:
    rows = await connection.fetch(
        """
        SELECT relation.relname
          FROM pg_catalog.pg_locks AS relation_lock
          JOIN pg_catalog.pg_class AS relation
            ON relation.oid = relation_lock.relation
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
         WHERE relation_lock.pid = $1
           AND relation_lock.mode = 'AccessExclusiveLock'
           AND relation_lock.granted IS TRUE
           AND namespace.nspname = $2
           AND relation.relname = ANY($3::text[])
        """,
        backend_pid,
        schema,
        relation_names,
    )
    return {row["relname"] for row in rows}


async def _assert_partial_locks_released(
    scenario,
    contract,
    writer_connection,
    migration_backend_pid,
    lock_task,
) -> None:
    assert await _has_sleeping_backend(
        writer_connection,
        migration_backend_pid,
        lock_task,
    )
    partial_locks = await _granted_exclusive_relation_names(
        writer_connection,
        migration_backend_pid,
        scenario.schema,
        contract.relation_names,
    )
    assert partial_locks == set()
    dataset_count = await asyncio.wait_for(
        writer_connection.fetchval(
            f"SELECT count(*) FROM {contract.dataset_ref}"
        ),
        timeout=2,
    )
    assert dataset_count >= 0


async def _assert_complete_lock_set(
    scenario,
    contract,
    writer_connection,
    migration_backend_pid,
) -> None:
    granted_locks = await _granted_exclusive_relation_names(
        writer_connection,
        migration_backend_pid,
        scenario.schema,
        contract.relation_names,
    )
    assert granted_locks == set(contract.relation_names)


async def _exercise_lock_retry(scenario, contract, writer_connection) -> None:
    writer_transaction = None
    migration_transaction = None
    lock_task = None
    try:
        writer_transaction = writer_connection.transaction()
        await writer_transaction.start()
        await writer_connection.execute(
            f"LOCK TABLE {contract.checkpoint_ref} IN ROW EXCLUSIVE MODE"
        )
        migration_transaction = scenario.connection.transaction()
        await migration_transaction.start()
        lock_task = asyncio.create_task(
            scenario.connection.execute(contract.lock_sql)
        )
        migration_backend_pid = scenario.connection.get_server_pid()
        await _assert_partial_locks_released(
            scenario,
            contract,
            writer_connection,
            migration_backend_pid,
            lock_task,
        )
        await writer_transaction.commit()
        writer_transaction = None
        await asyncio.wait_for(lock_task, timeout=10)
        await _assert_complete_lock_set(
            scenario,
            contract,
            writer_connection,
            migration_backend_pid,
        )
    finally:
        if lock_task is not None:
            if not lock_task.done():
                lock_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await lock_task
        if migration_transaction is not None:
            with suppress(Exception):
                await migration_transaction.rollback()
        if writer_transaction is not None:
            with suppress(Exception):
                await writer_transaction.rollback()


async def prove_terminal_window_lock_retry(scenario, migration) -> None:
    """Prove failed partial locks roll back and a full set survives."""

    contract = _lock_contract(scenario, migration)
    writer_connection = await open_test_connection()
    try:
        await _exercise_lock_retry(scenario, contract, writer_connection)
    finally:
        with suppress(Exception):
            await writer_connection.close()
