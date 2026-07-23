# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Measured progress coverage for the strict Rust finalizer."""

from __future__ import annotations

import asyncio
import sys
import time

import pytest

from process.ptg_parts.ptg2_shared_finalize import (
    _monitor_finalizer_output,
    _parse_process_cpu_milliseconds,
)


@pytest.mark.parametrize(
    ("raw_cpu_time", "expected_milliseconds"),
    (
        ("0:01.23", 1_230),
        ("2-03:04:05.67", 183_845_670),
        ("03:04:05", 11_045_000),
        ("not-a-time", 0),
        ("0:99.12", 0),
        ("0:nan", 0),
    ),
)
def test_process_cpu_time_parser_is_portable(
    raw_cpu_time,
    expected_milliseconds,
):
    assert _parse_process_cpu_milliseconds(raw_cpu_time) == expected_milliseconds


@pytest.mark.asyncio
async def test_busy_finalizer_cpu_moves_measured_progress_before_exit(tmp_path):
    """A CPU-only plateau reports real work well inside the four-second target."""

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        (
            "import time\n"
            "end = time.monotonic() + 1.5\n"
            "while time.monotonic() < end:\n"
            "    pass\n"
        ),
    )
    stop = asyncio.Event()
    cpu_events = []

    def record_progress(metric, amount):
        if metric == "process_cpu_milliseconds":
            cpu_events.append((time.monotonic(), amount))

    monitor = asyncio.create_task(
        _monitor_finalizer_output(
            output_directory=tmp_path / "finalizer-output",
            process_id=process.pid,
            stop=stop,
            progress_callback=record_progress,
            poll_seconds=0.15,
        )
    )
    await process.wait()
    stop.set()
    await monitor

    assert len(cpu_events) >= 2
    assert sum(amount for _observed_at, amount in cpu_events) > 0
    assert max(
        later[0] - earlier[0]
        for earlier, later in zip(cpu_events, cpu_events[1:])
    ) < 1.0
