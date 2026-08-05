# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure and concurrency proofs for connector generation storage."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from process.tin_npi_connector_generation_store import (
    TinNpiConnectorGenerationStoreError,
    load_and_seal_admitted_connector_generation,
)
from tests.tin_npi_connector_pg_generation_store import (
    _assert_complete_generation,
    _close_committed_scenario,
    _committed_scenario,
    _current_pointer,
    _database_counts,
    _limits_for,
    _scenario_bundle,
)
from tests.tin_npi_connector_postgres_support import open_test_connection


async def prove_store_cancel_rollback(monkeypatch):
    scenario = await _committed_scenario(monkeypatch)
    load_task = None
    try:
        bundle = _scenario_bundle(scenario)
        limits = _limits_for(bundle)
        connection = _CopyBlockingConnection(scenario.connection)
        load_task = asyncio.create_task(
            load_and_seal_admitted_connector_generation(
                connection,
                bundle,
                limits=limits,
                schema=scenario.session.schema,
            )
        )
        await asyncio.wait_for(connection.copy_started.wait(), timeout=5)

        load_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await load_task

        assert not scenario.connection.is_in_transaction()
        assert await _database_counts(scenario) == (0, 0, 0, 0, 0, 0)
        assert await _current_pointer(scenario) == (0, None)

        retry = await load_and_seal_admitted_connector_generation(
            scenario.connection,
            bundle,
            limits=limits,
            schema=scenario.session.schema,
        )
        assert retry.reused is False
        await _assert_complete_generation(scenario, retry.generation_key, bundle)
    finally:
        if load_task is not None and not load_task.done():
            load_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await load_task
        await _close_committed_scenario(scenario)


async def prove_store_commit_ack_recovery(monkeypatch):
    scenario = await _committed_scenario(monkeypatch)
    retry_connection = None
    try:
        bundle = _scenario_bundle(scenario)
        limits = _limits_for(bundle)
        connection = _CommitAcknowledgementLostConnection(scenario.connection)

        with pytest.raises(
            TinNpiConnectorGenerationStoreError,
            match="^connector generation database operation failed$",
        ):
            await load_and_seal_admitted_connector_generation(
                connection,
                bundle,
                limits=limits,
                schema=scenario.session.schema,
            )

        assert not scenario.connection.is_in_transaction()
        committed_counts = await _database_counts(scenario)
        assert committed_counts == (1, 1, 1, 1, 2, 3)
        assert await _current_pointer(scenario) == (0, None)

        retry_connection = await open_test_connection()
        retry = await load_and_seal_admitted_connector_generation(
            retry_connection,
            bundle,
            limits=limits,
            schema=scenario.session.schema,
        )
        assert retry.reused is True
        assert await _database_counts(scenario) == committed_counts
        await _assert_complete_generation(scenario, retry.generation_key, bundle)
    finally:
        if retry_connection is not None:
            await retry_connection.close()
        await _close_committed_scenario(scenario)


async def prove_store_concurrent_reuse(monkeypatch):
    """Prove the source-vector advisory lock yields one exact generation."""

    scenario = await _committed_scenario(monkeypatch)
    second_connection = await open_test_connection()
    first_task = None
    second_task = None
    first_connection = _PauseAfterAdvisoryLockConnection(scenario.connection)
    second_advisory_attempt = asyncio.Event()
    try:
        bundle = _scenario_bundle(scenario)
        limits = _concurrent_limits(bundle)
        second_guarded_connection = _SignalBeforeAdvisoryLockConnection(
            second_connection,
            second_advisory_attempt,
        )
        first_task = asyncio.create_task(
            load_and_seal_admitted_connector_generation(
                first_connection,
                bundle,
                limits=limits,
                schema=scenario.session.schema,
            )
        )
        await asyncio.wait_for(first_connection.lock_acquired.wait(), timeout=5)
        second_task = asyncio.create_task(
            load_and_seal_admitted_connector_generation(
                second_guarded_connection,
                bundle,
                limits=limits,
                schema=scenario.session.schema,
            )
        )
        await asyncio.wait_for(second_advisory_attempt.wait(), timeout=5)
        assert not second_task.done()

        first_connection.release_lock.set()
        load_outcomes = await asyncio.gather(first_task, second_task)

        assert {outcome.generation_key for outcome in load_outcomes} == {
            load_outcomes[0].generation_key
        }
        assert sorted(outcome.reused for outcome in load_outcomes) == [False, True]
        assert await _database_counts(scenario) == (1, 1, 1, 1, 2, 3)
        assert await _current_pointer(scenario) == (0, None)
        await _assert_complete_generation(
            scenario,
            load_outcomes[0].generation_key,
            bundle,
        )
    finally:
        first_connection.release_lock.set()
        await _cancel_tasks(first_task, second_task)
        await second_connection.close()
        await _close_committed_scenario(scenario)


def _concurrent_limits(bundle):
    return replace(
        _limits_for(bundle),
        build_lease_seconds=30,
        lock_timeout_ms=2_000,
        statement_timeout_ms=3_000,
        operation_timeout_seconds=4,
    )


async def _cancel_tasks(*tasks):
    created_tasks = [task for task in tasks if task is not None]
    for task in created_tasks:
        if not task.done():
            task.cancel()
    if created_tasks:
        await asyncio.gather(*created_tasks, return_exceptions=True)


class _DelegatingConnection:
    def __init__(self, delegate):
        self.delegate = delegate

    def transaction(self):
        return self.delegate.transaction()

    def is_in_transaction(self):
        return self.delegate.is_in_transaction()

    async def execute(self, sql, *arguments):
        return await self.delegate.execute(sql, *arguments)

    async def fetchval(self, sql, *arguments):
        return await self.delegate.fetchval(sql, *arguments)

    async def fetchrow(self, sql, *arguments):
        return await self.delegate.fetchrow(sql, *arguments)

    async def copy_records_to_table(
        self,
        table_name,
        *,
        schema_name,
        columns,
        records,
    ):
        return await self.delegate.copy_records_to_table(
            table_name,
            schema_name=schema_name,
            columns=columns,
            records=records,
        )


class _CopyBlockingConnection(_DelegatingConnection):
    def __init__(self, delegate):
        super().__init__(delegate)
        self.copy_started = asyncio.Event()
        self.release_copy = asyncio.Event()

    async def copy_records_to_table(
        self,
        table_name,
        *,
        schema_name,
        columns,
        records,
    ):
        if table_name == "tin_npi_connector_evidence":
            self.copy_started.set()
            await self.release_copy.wait()
        return await super().copy_records_to_table(
            table_name,
            schema_name=schema_name,
            columns=columns,
            records=records,
        )


class _PauseAfterAdvisoryLockConnection(_DelegatingConnection):
    def __init__(self, delegate):
        super().__init__(delegate)
        self.lock_acquired = asyncio.Event()
        self.release_lock = asyncio.Event()

    async def execute(self, sql, *arguments):
        result = await super().execute(sql, *arguments)
        if "pg_advisory_xact_lock" in sql:
            self.lock_acquired.set()
            await self.release_lock.wait()
        return result


class _SignalBeforeAdvisoryLockConnection(_DelegatingConnection):
    def __init__(self, delegate, advisory_attempt):
        super().__init__(delegate)
        self.advisory_attempt = advisory_attempt

    async def execute(self, sql, *arguments):
        if "pg_advisory_xact_lock" in sql:
            self.advisory_attempt.set()
        return await super().execute(sql, *arguments)


class _CommitAcknowledgementLostConnection(_DelegatingConnection):
    def transaction(self):
        return _CommitAcknowledgementLostTransaction(self.delegate.transaction())


class _CommitAcknowledgementLostTransaction:
    def __init__(self, delegate):
        self.delegate = delegate

    async def __aenter__(self):
        return await self.delegate.__aenter__()

    async def __aexit__(self, error_type, error, traceback):
        result = await self.delegate.__aexit__(error_type, error, traceback)
        if error_type is None:
            raise ConnectionError("synthetic commit acknowledgement lost")
        return result
