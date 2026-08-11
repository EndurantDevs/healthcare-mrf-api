# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Offline formatted-address archive refresh contracts."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest


address_formatted_address = importlib.import_module(
    "process.address_formatted_address"
)


def test_archive_refresh_sql_is_keyset_bounded_and_offline(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "address_archive_custom")
    sql = address_formatted_address._archive_format_batch_sql("mrf")

    assert "address_key > CAST(:after_address_key AS uuid)" in sql
    assert "ORDER BY address_key" in sql
    assert "LIMIT :batch_size" in sql
    assert '"mrf"."address_archive_custom"' in sql
    assert '"mrf".addr_formatted_address_v1' in sql
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

    refresh_stats = (
        await address_formatted_address.refresh_address_archive_formatted_addresses(
            schema="mrf",
            batch_size=2,
            cancel_check=cancel_check,
            progress_callback=progress_callback,
        )
    )

    assert refresh_stats == address_formatted_address.AddressFormatRefreshStats(
        scanned=3,
        updated=2,
        batches=2,
        renderer_version=1,
    )
    assert database_first.await_count == 3
    assert database_first.await_args_list[1].kwargs["after_address_key"].endswith(
        "0002"
    )
    assert database_first.await_args_list[2].kwargs["after_address_key"].endswith(
        "0003"
    )
    assert cancel_check.await_count == 3
    assert progress_callback.call_args_list == [
        ((2, 2),),
        ((3, 2),),
    ]


@pytest.mark.asyncio
async def test_archive_refresh_allows_omitted_optional_callbacks(monkeypatch) -> None:
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

    refresh_stats = (
        await address_formatted_address.refresh_address_archive_formatted_addresses(
            schema="mrf",
            batch_size=1,
        )
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
        renderer_version=1,
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
        "renderer_version": 1,
    }
    assert refresh_archive.await_args.kwargs["batch_size"] == 50
    cancel_check.assert_awaited_once()
    assert [
        progress_call.kwargs["status"]
        for progress_call in progress_events.call_args_list
    ] == ["running", "succeeded"]
    assert {
        progress_call.kwargs["run_id"]
        for progress_call in progress_events.call_args_list
    } == {"run-synthetic"}


@pytest.mark.asyncio
async def test_command_runs_inline_or_enqueues_exact_worker(monkeypatch) -> None:
    inline_result_by_field = {
        "scanned": 1,
        "updated": 1,
        "batches": 1,
        "renderer_version": 1,
    }
    process_refresh = AsyncMock(return_value=inline_result_by_field)
    monkeypatch.setattr(
        address_formatted_address,
        "process_address_formatted_address",
        process_refresh,
    )

    assert (
        await address_formatted_address.run_address_formatted_address_command(
            batch_size=25
        )
        == inline_result_by_field
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
