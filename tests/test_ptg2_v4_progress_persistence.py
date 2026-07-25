# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import json

import pytest

from process import live_progress
from process.ptg_parts import live_progress as ptg_live_progress
from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from tests.live_progress_atomic_redis import AtomicLiveProgressRedis


def _accepted_compiler_state() -> compiler._CompilerProgressState:
    state = compiler._CompilerProgressState()
    assert state.is_accepted(
        {
            "version": 1,
            "seq": 1,
            "phase": "derive_patterns",
            "done": 50,
            "total": 100,
            "unit": "groups",
            "elapsed_ms": 1_000,
            "terminal": False,
        }
    )
    return state


async def _publish_compiler_state(
    state: compiler._CompilerProgressState,
    *,
    heartbeat: bool = False,
) -> None:
    await compiler._publish_compiler_progress_state(
        state,
        emit_lock=asyncio.Lock(),
        input_bytes=123,
        input_factor_edges=456,
        input_factor_owners=78,
        checkpoint_reused=False,
        heartbeat=heartbeat,
    )


@pytest.mark.asyncio
async def test_compiler_heartbeat_persists_stage_without_fake_movement(
    monkeypatch,
) -> None:
    """Heartbeat refreshes observation time but preserves measured compiler work."""

    elapsed_seconds = [1.0]
    progress_writes = []
    base_time = datetime.datetime(2026, 7, 24, 10, 0, 0)
    fake_redis = AtomicLiveProgressRedis(
        on_progress_write=lambda _key, _ttl, encoded: progress_writes.append(
            json.loads(encoded)
        )
    )
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
    monkeypatch.setattr(
        live_progress,
        "_utc_now",
        lambda: base_time + datetime.timedelta(seconds=elapsed_seconds[0]),
    )
    monkeypatch.setattr(live_progress, "enqueue_status_event", lambda _event: None)
    state = _accepted_compiler_state()
    token = ptg_live_progress.set_live_progress_context(
        run_id="run-v4-compiler-heartbeat",
        attempt_id="attempt-1",
        attempt_started_at="2026-07-24T10:00:00Z",
    )
    try:
        await _publish_compiler_state(state)
        elapsed_seconds[0] = 5.0
        await _publish_compiler_state(
            compiler.replace(state, elapsed_ms=5_000),
            heartbeat=True,
        )
    finally:
        ptg_live_progress.reset_live_progress_context(token)

    first, heartbeat = progress_writes
    assert first["stage_id"] == "ptg2_v4_provider_graph_compile"
    assert first["stage_ordinal"] == 5
    assert first["done"] == heartbeat["done"] == 50
    assert first["total"] == heartbeat["total"] == 100
    assert first["pct"] == heartbeat["pct"]
    assert first["progress_seq"] == heartbeat["progress_seq"]
    assert first["progressed_at"] == heartbeat["progressed_at"]
    assert first["observed_at"] != heartbeat["observed_at"]
    assert heartbeat["event_seq"] == first["event_seq"] + 1
