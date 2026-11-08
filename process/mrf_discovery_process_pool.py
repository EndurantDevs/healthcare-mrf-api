# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Drain discovery worker processes before their source claims are released."""

from __future__ import annotations

import asyncio
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from process.mrf_discovery_checkpoints import SourceProcessResult
    from process.mrf_source_discovery import DiscoverySourceProcessingOptions


class DiscoverySourceProcessPool:
    """Bridge cancellable asyncio source tasks to reusable spawned processes."""

    def __init__(self, process_workers: int) -> None:
        self._process_pool = ProcessPoolExecutor(
            max_workers=process_workers,
            mp_context=mp.get_context("spawn"),
        )
        self._processes_by_pid: dict[int, Any] = {}
        self._shutdown_future: asyncio.Future[None] | None = None

    async def process_source(
        self,
        source_record: dict[str, Any],
        processing_options: DiscoverySourceProcessingOptions,
    ) -> SourceProcessResult:
        """Submit one source and await its process result without blocking asyncio."""

        if self._shutdown_future is not None:
            await self._shutdown(terminate=False)
            raise RuntimeError("discovery process pool is closed")
        from process.mrf_source_discovery import _run_discovery_source_process

        result_future = None
        try:
            process_result = self._process_pool.submit(
                _run_discovery_source_process,
                source_record,
                processing_options,
            )
            # shortcut: retain CPython handles until the executor offers a draining kill API.
            self._processes_by_pid.update(self._process_pool._processes or {})
            result_future = asyncio.wrap_future(process_result)
            while True:
                completed, _ = await asyncio.wait((result_future,), timeout=1)
                if completed:
                    return result_future.result()
                if any(process.exitcode is not None for process in self._processes_by_pid.values()):
                    raise BrokenProcessPool("a discovery process exited before returning its result")
        except BrokenProcessPool, asyncio.CancelledError:
            if result_future is not None:
                result_future.cancel()
                if not result_future.cancelled():
                    result_future.exception()
            await self.terminate()
            raise

    async def close(self) -> None:
        """Finish submitted source work and stop the process pool cleanly."""

        await self._shutdown(terminate=False)

    async def terminate(self) -> None:
        """Kill process work before canceled source claims become retryable."""

        await self._shutdown(terminate=True)

    def _kill_processes(self) -> None:
        """Stop owned writers even if they ignore graceful termination."""

        for process in self._processes_by_pid.values():
            if process.is_alive():
                process.kill()

    def _join_processes(self, cancel_futures: bool) -> None:
        """Join the executor manager and every retained process handle."""

        self._process_pool.shutdown(wait=True, cancel_futures=cancel_futures)
        for process in self._processes_by_pid.values():
            process.join()

    async def _shutdown(self, *, terminate: bool) -> None:
        """Drain once before any waiter can release a source claim."""

        if terminate:
            self._kill_processes()
        if self._shutdown_future is None:
            self._shutdown_future = asyncio.get_running_loop().run_in_executor(None, self._join_processes, terminate)
        is_cancelled = False
        while True:
            try:
                await asyncio.shield(self._shutdown_future)
                break
            except asyncio.CancelledError:
                is_cancelled = True
                self._kill_processes()
        if is_cancelled:
            raise asyncio.CancelledError
