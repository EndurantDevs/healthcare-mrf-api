# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL session-lock proof for manual formulary synchronization."""

from __future__ import annotations

import asyncio
import datetime as dt
import uuid

import pytest

from db.connection import Database
import process.formulary_fhir.manual_worker as manual_module
from process.formulary_fhir.manual_worker import MANUAL_SYNC_ENABLED_ENV
from process.formulary_fhir.manual_worker import ManualSynchronizationError
from process.formulary_fhir.manual_worker import (
    synchronize_verified_dataset_manually,
)
from process.formulary_fhir.synchronizer import SynchronizationResult
from tests.test_formulary_fhir_storage_postgres import _database_url


CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)


def _result(dataset_digit: str) -> SynchronizationResult:
    return SynchronizationResult(
        "ffd_" + dataset_digit * 48,
        "a" * 64,
        1,
        1,
        1,
        "b" * 64,
        "c" * 64,
        1,
        0,
        0,
        2,
        0,
        0,
    )


def _configure_database(monkeypatch) -> Database:
    database_url = _database_url()
    monkeypatch.setenv(MANUAL_SYNC_ENABLED_ENV, "true")
    monkeypatch.setattr(manual_module, "LOCK_WAIT_SECONDS", 0.2)
    monkeypatch.setattr(manual_module, "LOCK_RETRY_SECONDS", 0.01)
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "postgresql+asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(database_url.host))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(database_url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(database_url.username))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(database_url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", str(database_url.database))
    monkeypatch.setenv("HLTHPRT_DB_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "5")
    return Database()


async def _run_manual_sync(
    database: Database,
    source_id: str,
    run_id: str,
) -> SynchronizationResult:
    return await synchronize_verified_dataset_manually(
        source_id=source_id,
        run_id=run_id,
        cutoff=CUTOFF,
        timeout_seconds=10,
        database=database,
    )


@pytest.mark.asyncio
async def test_postgres_source_lock_contention_overlap_and_cancel_release(monkeypatch):
    """Prove same-source exclusion, cross-source overlap, and cancel release."""

    database = _configure_database(monkeypatch)
    source_suffix = uuid.uuid4().hex
    first_source_id = f"source-a-{source_suffix}"
    second_source_id = f"source-b-{source_suffix}"
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    synchronization_calls: list[tuple[str, str]] = []

    async def synchronize(*, source_id, run_id, **_values):
        synchronization_calls.append((source_id, run_id))
        if run_id == "synthetic-run-first":
            first_started.set()
            await release_first.wait()
        return _result("1" if source_id == first_source_id else "2")

    monkeypatch.setattr(manual_module, "synchronize_verified_dataset", synchronize)
    first_task = asyncio.create_task(
        _run_manual_sync(database, first_source_id, "synthetic-run-first")
    )
    try:
        await first_started.wait()
        with pytest.raises(ManualSynchronizationError) as busy_error:
            await _run_manual_sync(
                database,
                first_source_id,
                "synthetic-run-contender",
            )
        assert busy_error.value.code == "busy"
        second_result = await _run_manual_sync(
            database,
            second_source_id,
            "synthetic-run-second-source",
        )
        assert second_result.dataset_id == "ffd_" + "2" * 48

        first_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_task
        retry_result = await _run_manual_sync(
            database,
            first_source_id,
            "synthetic-run-retry",
        )
        assert retry_result.dataset_id == "ffd_" + "1" * 48
        assert (first_source_id, "synthetic-run-contender") not in (
            synchronization_calls
        )
        assert (second_source_id, "synthetic-run-second-source") in (
            synchronization_calls
        )
        assert (first_source_id, "synthetic-run-retry") in synchronization_calls
    finally:
        release_first.set()
        if not first_task.done():
            first_task.cancel()
            await asyncio.gather(first_task, return_exceptions=True)
        await database.disconnect()
