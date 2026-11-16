# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Offline formatted-address archive refresh contracts."""

from __future__ import annotations

import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

address_formatted_address = importlib.import_module("process.address_formatted_address")


def test_archive_refresh_sql_is_keyset_bounded_and_offline(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "address_archive_custom")
    sql = address_formatted_address._archive_format_batch_sql("mrf")

    assert "address_key > CAST(:after_address_key AS uuid)" in sql
    assert "ORDER BY address_key" in sql
    assert "LIMIT :batch_size" in sql
    assert '"mrf"."address_archive_custom"' in sql
    assert '"mrf".addr_formatted_address_v2' in sql
    assert "formatted_address_version = :renderer_version" in sql
    assert "formatted_address_source = :renderer_source" in sql
    assert "geocode" not in sql.lower()
    assert "similarity" not in sql.lower()


def test_archive_refresh_rejects_an_unsafe_configured_table(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "unsafe.table")

    with pytest.raises(ValueError, match="Unsafe SQL identifier"):
        address_formatted_address._archive_format_batch_sql("mrf")


@pytest.mark.parametrize("batch_size", (0, -1, 100_001))
def test_archive_refresh_rejects_unsafe_batch_sizes(batch_size: int) -> None:
    with pytest.raises(ValueError, match="batch_size must be between"):
        address_formatted_address._validated_batch_size(batch_size)


@pytest.mark.asyncio
async def test_control_worker_does_not_widen_an_explicit_zero_batch(
    monkeypatch,
) -> None:
    refresh_archive = AsyncMock()
    monkeypatch.setattr(
        address_formatted_address,
        "refresh_address_archive_formatted_addresses",
        refresh_archive,
    )

    with pytest.raises(ValueError, match="batch_size must be between"):
        await address_formatted_address.process_address_formatted_address(
            {},
            {"batch_size": 0},
        )

    refresh_archive.assert_not_awaited()


@pytest.mark.asyncio
async def test_archive_refresh_advances_stable_keyset_batches(monkeypatch) -> None:
    monkeypatch.setattr(address_formatted_address.batches.address_batch, "is_native_available", lambda: False)
    batch_rows = (
        SimpleNamespace(
            scanned=2,
            updated=2,
            last_address_key="00000000-0000-0000-0000-000000000002",
        ),
        SimpleNamespace(
            scanned=1,
            updated=0,
            last_address_key="00000000-0000-0000-0000-000000000003",
        ),
        SimpleNamespace(scanned=0, updated=0, last_address_key=None),
    )
    database_first = AsyncMock(side_effect=batch_rows)
    cancel_check = AsyncMock()
    progress_callback = Mock()
    monkeypatch.setattr(address_formatted_address.db, "first", database_first)

    refresh_stats = await address_formatted_address.refresh_address_archive_formatted_addresses(
        schema="mrf",
        batch_size=2,
        cancel_check=cancel_check,
        progress_callback=progress_callback,
    )

    assert refresh_stats == address_formatted_address.AddressFormatRefreshStats(
        scanned=3,
        updated=2,
        batches=2,
        renderer_version=2,
    )
    assert database_first.await_count == 3
    assert database_first.await_args_list[1].kwargs["after_address_key"].endswith("0002")
    assert database_first.await_args_list[2].kwargs["after_address_key"].endswith("0003")
    assert cancel_check.await_count == 3
    assert progress_callback.call_args_list == [
        ((2, 2),),
        ((3, 2),),
    ]


@pytest.mark.asyncio
async def test_archive_refresh_allows_omitted_optional_callbacks(monkeypatch) -> None:
    monkeypatch.setattr(address_formatted_address.batches.address_batch, "is_native_available", lambda: False)
    database_first = AsyncMock(
        side_effect=(
            SimpleNamespace(
                scanned=1,
                updated=1,
                last_address_key="00000000-0000-0000-0000-000000000001",
            ),
            SimpleNamespace(scanned=0, updated=0, last_address_key=None),
        )
    )
    monkeypatch.setattr(address_formatted_address.db, "first", database_first)

    refresh_stats = await address_formatted_address.refresh_address_archive_formatted_addresses(
        schema="mrf",
        batch_size=1,
    )

    assert refresh_stats.scanned == 1
    assert refresh_stats.updated == 1
    assert refresh_stats.batches == 1
    assert database_first.await_count == 2


@pytest.mark.asyncio
async def test_control_worker_reports_archive_refresh_result(monkeypatch) -> None:
    refresh_stats = address_formatted_address.AddressFormatRefreshStats(
        scanned=5,
        updated=4,
        batches=2,
        renderer_version=2,
    )

    async def _refresh_archive(**options_by_name):
        await options_by_name["cancel_check"]()
        options_by_name["progress_callback"](3, 2)
        return refresh_stats

    refresh_archive = AsyncMock(side_effect=_refresh_archive)
    cancel_check = AsyncMock()
    progress_events = Mock()
    monkeypatch.setattr(
        address_formatted_address,
        "refresh_address_archive_formatted_addresses",
        refresh_archive,
    )
    monkeypatch.setattr(
        address_formatted_address,
        "enqueue_live_progress",
        progress_events,
    )
    monkeypatch.setattr(
        address_formatted_address,
        "raise_if_cancelled",
        cancel_check,
    )

    result_by_field = await address_formatted_address.process_address_formatted_address(
        {"control_run_id": "run-synthetic"},
        {"batch_size": 50},
    )

    assert result_by_field == {
        "scanned": 5,
        "updated": 4,
        "batches": 2,
        "renderer_version": 2,
    }
    assert refresh_archive.await_args.kwargs["batch_size"] == 50
    cancel_check.assert_awaited_once()
    assert [progress_call.kwargs["status"] for progress_call in progress_events.call_args_list] == [
        "running",
        "succeeded",
    ]
    assert {progress_call.kwargs["run_id"] for progress_call in progress_events.call_args_list} == {"run-synthetic"}


@pytest.mark.asyncio
async def test_command_runs_inline_or_enqueues_exact_worker(monkeypatch) -> None:
    inline_result_by_field = {
        "scanned": 1,
        "updated": 1,
        "batches": 1,
        "renderer_version": 2,
    }
    process_refresh = AsyncMock(return_value=inline_result_by_field)
    monkeypatch.setattr(
        address_formatted_address,
        "process_address_formatted_address",
        process_refresh,
    )

    assert (
        await address_formatted_address.run_address_formatted_address_command(batch_size=25) == inline_result_by_field
    )
    assert process_refresh.await_args.args == ({}, {"batch_size": 25})

    redis = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(
        address_formatted_address,
        "create_pool",
        AsyncMock(return_value=redis),
    )
    assert (
        await address_formatted_address.run_address_formatted_address_command(
            batch_size=25,
            enqueue=True,
        )
        is None
    )
    redis.enqueue_job.assert_awaited_once_with(
        "process_address_formatted_address",
        {"batch_size": 25},
        _queue_name="arq:AddressArchive",
    )


def test_archive_native_sql_guards_changed_inputs_and_null_labels() -> None:
    sql = address_formatted_address._archive_format_batch_sql("mrf", '"pg_temp"."batch_labels"')
    assert 'LEFT JOIN "pg_temp"."batch_labels" address_fields' in sql
    assert "AS address_lookup_key" in sql
    assert "=batch.address_lookup_key THEN CASE WHEN" in sql
    assert '"address_fields".native_fields IS NOT NULL' in sql
    assert "native_fields->>'formatted_address'" in sql
    assert "COALESCE" not in sql
    assert 'ELSE "mrf".addr_formatted_address_v2(archived.first_line' in sql


@pytest.mark.asyncio
async def test_archive_refresh_keeps_native_batch_accounting(monkeypatch) -> None:
    staged_batches = []

    @asynccontextmanager
    async def native_fields(database, schema, name, source_sql, **options):
        staged_batches.append((database, schema, name, source_sql, options))
        yield '"pg_temp"."batch_labels"'

    first = AsyncMock(
        side_effect=(
            SimpleNamespace(scanned=2, updated=1, last_address_key="00000000-0000-0000-0000-000000000002"),
            SimpleNamespace(scanned=0, updated=0, last_address_key=None),
        )
    )
    cancel = AsyncMock()
    monkeypatch.setattr(address_formatted_address.batches, "staged_address_fields", native_fields)
    monkeypatch.setattr(address_formatted_address.db, "first", first)
    refresh_stats = await address_formatted_address.refresh_address_archive_formatted_addresses(
        schema="archive_test",
        batch_size=2,
        cancel_check=cancel,
    )
    assert (refresh_stats.scanned, refresh_stats.updated, refresh_stats.batches) == (2, 1, 1)
    assert cancel.await_count == 2
    assert len(staged_batches) == 2 and all(entry[4]["temporary"] for entry in staged_batches)
    assert all(entry[2].startswith("address_format_") for entry in staged_batches)
    assert "ORDER BY address_key LIMIT :batch_size" in staged_batches[0][3]
    assert staged_batches[1][4]["parameters"]["after_address_key"].endswith("0002")
    assert staged_batches[0][4]["cancel_check"] is cancel
    assert all('LEFT JOIN "pg_temp"."batch_labels"' in call.args[0] for call in first.await_args_list)


@pytest.mark.asyncio
async def test_archive_rejected_native_stage_keeps_sql_fallback(monkeypatch) -> None:
    @asynccontextmanager
    async def unavailable_fields(*args, **kwargs):
        yield None

    first = AsyncMock(return_value=SimpleNamespace(scanned=0, updated=0, last_address_key=None))
    monkeypatch.setattr(address_formatted_address.batches, "staged_address_fields", unavailable_fields)
    monkeypatch.setattr(address_formatted_address.db, "first", first)
    result = await address_formatted_address.refresh_address_archive_formatted_addresses(schema="archive_test")
    assert result.scanned == result.updated == result.batches == 0
    assert "address_lookup_key" not in first.await_args.args[0]
    assert "addr_formatted_address_v2" in first.await_args.args[0]


@pytest.mark.asyncio
async def test_archive_update_failure_is_not_swallowed(monkeypatch) -> None:
    retired_stages = []

    @asynccontextmanager
    async def native_fields(*args, **kwargs):
        try:
            yield '"pg_temp"."batch_labels"'
        finally:
            retired_stages.append(True)

    monkeypatch.setattr(address_formatted_address.batches, "staged_address_fields", native_fields)
    monkeypatch.setattr(address_formatted_address.db, "first", AsyncMock(side_effect=RuntimeError("update failed")))
    with pytest.raises(RuntimeError, match="update failed"):
        await address_formatted_address.refresh_address_archive_formatted_addresses(schema="archive_test")
    assert retired_stages == [True]
