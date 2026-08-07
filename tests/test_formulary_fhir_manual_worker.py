# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit contracts for the default-off manual formulary adapter."""

from __future__ import annotations

import asyncio
import datetime as dt
from contextlib import asynccontextmanager

import pytest

import process.formulary_fhir.manual_worker as manual_module
from process.formulary_fhir.manual_worker import MANUAL_SYNC_ENABLED_ENV
from process.formulary_fhir.manual_worker import ManualSynchronizationError
from process.formulary_fhir.manual_worker import manual_result_json
from process.formulary_fhir.manual_worker import (
    synchronize_verified_dataset_manually,
)
from process.formulary_fhir.synchronizer import SynchronizationResult


CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)


def _result() -> SynchronizationResult:
    return SynchronizationResult(
        dataset_id="ffd_" + "1" * 48,
        acquisition_contract_hash="a" * 64,
        list_count=2,
        alias_count=3,
        medication_membership_count=4,
        coverage_hash="b" * 64,
        membership_hash="c" * 64,
        full_aliases=1,
        reused_aliases=2,
        resumed_aliases=1,
        request_count=5,
        transient_retry_count=1,
        throttle_count=0,
    )


class _Driver:
    def __init__(
        self,
        events: list[object],
        *,
        try_lock_results: list[object] | None = None,
        unlock_result: object = True,
        try_lock_started: asyncio.Event | None = None,
        unlock_started: asyncio.Event | None = None,
        release_unlock: asyncio.Event | None = None,
    ) -> None:
        self.events = events
        self.try_lock_results = list(try_lock_results or [True])
        self.unlock_result = unlock_result
        self.try_lock_started = try_lock_started
        self.unlock_started = unlock_started
        self.release_unlock = release_unlock

    async def fetchval(self, statement: str, identity: str):
        self.events.append(("sql", statement, identity))
        if "pg_try_advisory_lock" in statement:
            if self.try_lock_started is not None:
                self.try_lock_started.set()
            selected_result = (
                self.try_lock_results.pop(0)
                if len(self.try_lock_results) > 1
                else self.try_lock_results[0]
            )
        else:
            if self.unlock_started is not None:
                self.unlock_started.set()
            if self.release_unlock is not None:
                await self.release_unlock.wait()
            selected_result = self.unlock_result
        if isinstance(selected_result, BaseException):
            raise selected_result
        return selected_result


class _Database:
    def __init__(
        self,
        driver: _Driver,
        events: list[object],
        *,
        exit_started: asyncio.Event | None = None,
        release_exit: asyncio.Event | None = None,
        exit_error: BaseException | None = None,
    ) -> None:
        self.driver = driver
        self.events = events
        self.exit_started = exit_started
        self.release_exit = release_exit
        self.exit_error = exit_error
        self.acquire_count = 0

    @asynccontextmanager
    async def acquire_driver(self):
        self.acquire_count += 1
        self.events.append("driver-enter")
        try:
            yield self.driver
        except BaseException:
            self.events.append("driver-invalidate")
            raise
        else:
            if self.exit_started is not None:
                self.exit_started.set()
            if self.release_exit is not None:
                await self.release_exit.wait()
            if self.exit_error is not None:
                raise self.exit_error
            self.events.append("driver-close")


def _database(
    *,
    try_lock_results: list[object] | None = None,
    unlock_result: object = True,
    try_lock_started: asyncio.Event | None = None,
    unlock_started: asyncio.Event | None = None,
    release_unlock: asyncio.Event | None = None,
    exit_started: asyncio.Event | None = None,
    release_exit: asyncio.Event | None = None,
    exit_error: BaseException | None = None,
) -> tuple[_Database, list[object]]:
    events: list[object] = []
    driver = _Driver(
        events,
        try_lock_results=try_lock_results,
        unlock_result=unlock_result,
        try_lock_started=try_lock_started,
        unlock_started=unlock_started,
        release_unlock=release_unlock,
    )
    return (
        _Database(
            driver,
            events,
            exit_started=exit_started,
            release_exit=release_exit,
            exit_error=exit_error,
        ),
        events,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("disabled_value", [None, "", "0", "false", "typo"])
async def test_manual_gate_is_default_off_before_database(
    monkeypatch,
    disabled_value,
):
    if disabled_value is None:
        monkeypatch.delenv(MANUAL_SYNC_ENABLED_ENV, raising=False)
    else:
        monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, disabled_value)
    database, _events = _database()

    with pytest.raises(ManualSynchronizationError) as caught:
        await synchronize_verified_dataset_manually(
            source_id="source-alpha",
            run_id="synthetic-run",
            cutoff=CUTOFF,
            timeout_seconds=10,
            database=database,
        )

    assert caught.value.code == "disabled"
    assert database.acquire_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed_request",
    [
        {"source_id": " "},
        {"source_id": "source\nalpha"},
        {"source_id": "s" * 65},
        {"run_id": ""},
        {"run_id": "r" * 65},
        {"cutoff": "not-a-time"},
        {"cutoff": "2026-08-07T12:00:00"},
        {"cutoff": dt.datetime.now(dt.UTC) + dt.timedelta(days=1)},
        {"timeout_seconds": True},
        {"timeout_seconds": 0},
        {"timeout_seconds": 604_801},
    ],
)
async def test_invalid_manual_request_never_opens_database(
    monkeypatch,
    changed_request,
):
    monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, "true")
    database, _events = _database()
    request_by_field = {
        "source_id": "source-alpha",
        "run_id": "synthetic-run",
        "cutoff": CUTOFF,
        "timeout_seconds": 10,
    }
    request_by_field.update(changed_request)

    with pytest.raises(ManualSynchronizationError) as caught:
        await synchronize_verified_dataset_manually(
            **request_by_field,
            database=database,
        )

    assert caught.value.code == "invalid_request"
    assert database.acquire_count == 0


@pytest.mark.asyncio
async def test_success_binds_source_lock_and_unlocks_same_connection(monkeypatch):
    monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, "yes")
    database, events = _database()
    synchronization_calls: list[dict[str, object]] = []

    async def synchronize(**values):
        events.append("synchronize")
        synchronization_calls.append(values)
        return _result()

    monkeypatch.setattr(manual_module, "synchronize_verified_dataset", synchronize)
    synchronization_result = await synchronize_verified_dataset_manually(
        source_id="source-alpha",
        run_id="synthetic-run",
        cutoff="2026-08-07T14:00:00+02:00",
        timeout_seconds=10,
        database=database,
    )

    sql_events = [event for event in events if isinstance(event, tuple)]
    assert synchronization_result == _result()
    assert synchronization_calls == [
        {
            "source_id": "source-alpha",
            "run_id": "synthetic-run",
            "cutoff": CUTOFF,
            "database": database,
        }
    ]
    assert [event[0] if isinstance(event, tuple) else event for event in events] == [
        "driver-enter",
        "sql",
        "synchronize",
        "sql",
        "driver-close",
    ]
    assert all("source-alpha" not in event[1] for event in sql_events)
    assert all("$1" in event[1] for event in sql_events)
    assert sql_events[0][2] == sql_events[1][2]


@pytest.mark.asyncio
async def test_busy_and_lock_error_are_sanitized_and_skip_sync(monkeypatch):
    monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, "on")
    monkeypatch.setattr(manual_module, "LOCK_WAIT_SECONDS", 0.01)
    monkeypatch.setattr(manual_module, "LOCK_RETRY_SECONDS", 0.001)
    synchronization_calls: list[bool] = []

    async def synchronize(**_values):
        synchronization_calls.append(True)
        return _result()

    monkeypatch.setattr(manual_module, "synchronize_verified_dataset", synchronize)
    busy_database, busy_events = _database(try_lock_results=[False])
    with pytest.raises(ManualSynchronizationError) as busy_error:
        await synchronize_verified_dataset_manually(
            source_id="source-alpha",
            run_id="synthetic-run-a",
            cutoff=CUTOFF,
            timeout_seconds=10,
            database=busy_database,
        )
    failed_database, failed_events = _database(
        try_lock_results=[RuntimeError("https://secret.invalid?token=value")]
    )
    with pytest.raises(ManualSynchronizationError) as lock_error:
        await synchronize_verified_dataset_manually(
            source_id="source-alpha",
            run_id="synthetic-run-b",
            cutoff=CUTOFF,
            timeout_seconds=10,
            database=failed_database,
        )

    assert busy_error.value.code == "busy"
    assert lock_error.value.code == "lock_unavailable"
    assert "secret" not in str(lock_error.value)
    assert synchronization_calls == []
    assert "driver-invalidate" in busy_events
    assert "driver-invalidate" in failed_events


@pytest.mark.asyncio
async def test_body_failure_and_task_cancellation_invalidate_lock(monkeypatch):
    monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, "1")
    original_error = RuntimeError("private downstream location")
    failed_database, failed_events = _database()

    async def fail_sync(**_values):
        raise original_error

    monkeypatch.setattr(manual_module, "synchronize_verified_dataset", fail_sync)
    with pytest.raises(RuntimeError) as caught:
        await synchronize_verified_dataset_manually(
            source_id="source-alpha",
            run_id="synthetic-run-a",
            cutoff=CUTOFF,
            timeout_seconds=10,
            database=failed_database,
        )
    assert caught.value is original_error
    assert "driver-invalidate" in failed_events

    sync_started = asyncio.Event()
    cancelled_database, cancelled_events = _database()

    async def block_sync(**_values):
        sync_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(manual_module, "synchronize_verified_dataset", block_sync)
    synchronization_task = asyncio.create_task(
        synchronize_verified_dataset_manually(
            source_id="source-alpha",
            run_id="synthetic-run-b",
            cutoff=CUTOFF,
            timeout_seconds=10,
            database=cancelled_database,
        )
    )
    await sync_started.wait()
    synchronization_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await synchronization_task
    assert "driver-invalidate" in cancelled_events


@pytest.mark.asyncio
async def test_timeout_invalidates_lock_and_does_not_unlock(monkeypatch):
    monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, "true")
    database, events = _database()

    async def block_sync(**_values):
        await asyncio.Event().wait()

    monkeypatch.setattr(manual_module, "synchronize_verified_dataset", block_sync)
    with pytest.raises(TimeoutError):
        await synchronize_verified_dataset_manually(
            source_id="source-alpha",
            run_id="synthetic-run",
            cutoff=CUTOFF,
            timeout_seconds=1,
            database=database,
        )

    sql_statements = [event[1] for event in events if isinstance(event, tuple)]
    assert "driver-invalidate" in events
    assert not any("pg_advisory_unlock" in statement for statement in sql_statements)


@pytest.mark.asyncio
async def test_unlock_anomaly_fails_closed(monkeypatch):
    monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, "true")
    database, events = _database(unlock_result=False)

    async def synchronize(**_values):
        return _result()

    monkeypatch.setattr(manual_module, "synchronize_verified_dataset", synchronize)
    with pytest.raises(ManualSynchronizationError) as caught:
        await synchronize_verified_dataset_manually(
            source_id="source-alpha",
            run_id="synthetic-run",
            cutoff=CUTOFF,
            timeout_seconds=10,
            database=database,
        )

    assert caught.value.code == "cleanup"
    assert "driver-invalidate" in events
    assert "driver-close" not in events


@pytest.mark.asyncio
async def test_repeated_cancellation_drains_unlock(monkeypatch):
    monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, "true")
    unlock_started = asyncio.Event()
    release_unlock = asyncio.Event()
    database, events = _database(
        unlock_started=unlock_started,
        release_unlock=release_unlock,
    )

    async def synchronize(**_values):
        return _result()

    monkeypatch.setattr(manual_module, "synchronize_verified_dataset", synchronize)
    synchronization_task = asyncio.create_task(
        synchronize_verified_dataset_manually(
            source_id="source-alpha",
            run_id="synthetic-run",
            cutoff=CUTOFF,
            timeout_seconds=10,
            database=database,
        )
    )
    await unlock_started.wait()
    synchronization_task.cancel()
    await asyncio.sleep(0)
    synchronization_task.cancel()
    release_unlock.set()

    with pytest.raises(asyncio.CancelledError):
        await synchronization_task
    assert "driver-invalidate" in events


def test_manual_result_json_is_an_explicit_safe_schema():
    rendered_result = manual_result_json(_result())

    assert '"status":"verified"' in rendered_result
    assert '"dataset_id":"ffd_' in rendered_result
    assert "source-alpha" not in rendered_result
    assert "synthetic-run" not in rendered_result
    assert "2026-08-07" not in rendered_result
    with pytest.raises(ManualSynchronizationError) as caught:
        manual_result_json(object())
    assert caught.value.code == "invalid_result"
