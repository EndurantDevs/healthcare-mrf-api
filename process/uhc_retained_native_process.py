# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded subprocess output and cancellation cleanup for UHC retention."""

import asyncio
import contextlib

from process.ptg_parts.rust_scanner import _terminate_asyncio_subprocess_group
from process.uhc_retained_types import UHCRetainedAdmissionError


async def _read_bounded(
    stream: asyncio.StreamReader,
    *,
    byte_limit: int,
    label: str,
) -> bytes:
    chunks = bytearray()
    while True:
        remaining = byte_limit + 1 - len(chunks)
        chunk = await stream.read(min(64 * 1024, remaining))
        if not chunk:
            return bytes(chunks)
        chunks.extend(chunk)
        if len(chunks) > byte_limit:
            raise UHCRetainedAdmissionError(
                f"UHC native retention {label} exceeds the byte limit"
            )


async def collect_process_output(
    process: asyncio.subprocess.Process,
    *,
    stdout_limit: int,
    stderr_limit: int,
) -> tuple[bytes, bytes, int]:
    """Read both pipes concurrently under independent hard byte limits."""

    if process.stdout is None or process.stderr is None:
        raise UHCRetainedAdmissionError("UHC native retention pipes are unavailable")
    stdout_task = asyncio.create_task(
        _read_bounded(process.stdout, byte_limit=stdout_limit, label="stdout")
    )
    stderr_task = asyncio.create_task(
        _read_bounded(process.stderr, byte_limit=stderr_limit, label="stderr")
    )
    try:
        stdout, stderr = await asyncio.gather(stdout_task, stderr_task)
        return_code = await process.wait()
        return stdout, stderr, int(return_code)
    finally:
        for task in (stdout_task, stderr_task):
            if not task.done():
                task.cancel()
        for task in (stdout_task, stderr_task):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task


async def cleanup_native_process(
    process: asyncio.subprocess.Process | None,
    spawn_task: asyncio.Task[asyncio.subprocess.Process],
) -> None:
    """Resolve creation and terminate the native process group when needed."""

    if process is None:
        try:
            process = await asyncio.shield(spawn_task)
        except (asyncio.CancelledError, Exception):
            process = None
    if process is not None:
        if process.returncode is None:
            await _terminate_asyncio_subprocess_group(process)
        else:
            await process.wait()
