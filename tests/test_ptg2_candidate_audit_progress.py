from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_partitioned_candidate_audit as audit
from process.ptg_parts.ptg2_partitioned_candidate_audit_report import (
    PartitionedAuditHttpMetrics,
)


@pytest.mark.asyncio
async def test_collect_partition_results_reports_monotonic_exact_counters():
    """Report the initial count and every observed completion through the end."""

    metrics = PartitionedAuditHttpMetrics(planned_request_count=3)

    async def complete_partition(value, delay):
        await asyncio.sleep(delay)
        metrics.completed_request_count += 1
        return value

    task_list = [
        asyncio.create_task(complete_partition("first", 0.01)),
        asyncio.create_task(complete_partition("second", 0.02)),
        asyncio.create_task(complete_partition("third", 0.03)),
    ]
    progress_update_list: list[tuple[int, int]] = []

    async def publish_progress(completed, total):
        progress_update_list.append((completed, total))

    partition_result_list = await audit._collect_partition_results(
        tasks=task_list,
        metrics=metrics,
        progress_callback=publish_progress,
        total=3,
    )

    assert partition_result_list == ["first", "second", "third"]
    assert progress_update_list == [(0, 3), (1, 3), (2, 3), (3, 3)]


@pytest.mark.asyncio
async def test_partition_progress_callback_is_best_effort():
    """A failed status sink must not become an audit failure."""

    callback = AsyncMock(side_effect=RuntimeError("status sink unavailable"))

    await audit._publish_partition_progress(
        callback,
        completed=2,
        total=5,
    )

    callback.assert_awaited_once_with(2, 5)
