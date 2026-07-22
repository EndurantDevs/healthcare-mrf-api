# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio

import pytest

from api.endpoint import pricing


@pytest.mark.asyncio
async def test_process_admission_cache_never_duplicates_a_live_gate():
    cached_gate = pricing._candidate_audit_process_admission_for_limit
    cached_gate.cache_clear()
    active_gate = cached_gate(20)
    active_lease = await active_gate.acquire(20)
    waiting_acquire = None
    try:
        for maximum_bytes in range(21, 30):
            cached_gate(maximum_bytes)

        assert cached_gate(20) is active_gate
        waiting_acquire = asyncio.create_task(cached_gate(20).acquire(20))
        await asyncio.sleep(0)
        assert not waiting_acquire.done()
        active_lease.release()
        waiting_lease = await waiting_acquire
        waiting_lease.release()
        waiting_acquire = None
        assert active_gate.snapshot.retained_bytes == 0
    finally:
        if not active_lease.is_released:
            active_lease.release()
        if waiting_acquire is not None:
            waiting_acquire.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting_acquire
        cached_gate.cache_clear()
