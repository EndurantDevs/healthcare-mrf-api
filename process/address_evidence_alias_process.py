# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cancellation-safe subprocess runner for native evidence matching."""

from __future__ import annotations

import asyncio
import os
import subprocess
from contextlib import suppress
from pathlib import Path
from typing import Any

from process.ptg_parts.rust_scanner import (
    _ScannerProcessControl,
    _await_cancellation_resistant_cleanup,
    _subprocess_session_options,
)


def _spawn_native_process(
    binary: Path,
    arguments: tuple[str, ...],
    thread_count: int,
    pass_fds: tuple[int, ...],
) -> subprocess.Popen[bytes]:
    options_by_field: dict[str, Any] = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "env": {**os.environ, "RAYON_NUM_THREADS": str(thread_count)},
        **_subprocess_session_options(subprocess.Popen),
    }
    if pass_fds:
        options_by_field["pass_fds"] = pass_fds
    return subprocess.Popen((str(binary), *arguments), **options_by_field)


def _communicate_native_process(
    process: subprocess.Popen[bytes],
    label: str,
) -> bytes:
    stdout, stderr = process.communicate()
    if process.returncode:
        detail = (stderr or stdout).decode("utf-8", errors="replace")[-4000:]
        raise RuntimeError(
            f"{label} failed with exit code {process.returncode}: {detail}"
        )
    return stdout


async def _stop_native_process(
    process_control: _ScannerProcessControl,
    native_task: asyncio.Task[bytes],
    cleanup_deadline_monotonic: float | None,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 10.0
    if cleanup_deadline_monotonic is not None:
        deadline = min(deadline, cleanup_deadline_monotonic)
    async with asyncio.timeout_at(deadline):
        await asyncio.to_thread(process_control.terminate)
        with suppress(Exception):
            await asyncio.shield(native_task)


async def run_native_process(
    binary: Path,
    arguments: tuple[str, ...],
    label: str,
    *,
    thread_count: int,
    cleanup_deadline_monotonic: float | None = None,
    pass_fds: tuple[int, ...] = (),
) -> bytes:
    """Run one owned process and reap it before cancellation is delivered."""
    process_control = _ScannerProcessControl()
    process = _spawn_native_process(binary, arguments, thread_count, pass_fds)
    process_control.attach(process)
    native_task = asyncio.create_task(
        asyncio.to_thread(_communicate_native_process, process, label)
    )
    try:
        return await asyncio.shield(native_task)
    except BaseException:
        cleanup_task = asyncio.create_task(
            _stop_native_process(
                process_control,
                native_task,
                cleanup_deadline_monotonic,
            )
        )
        await _await_cancellation_resistant_cleanup(cleanup_task)
        raise


__all__ = ["run_native_process"]
