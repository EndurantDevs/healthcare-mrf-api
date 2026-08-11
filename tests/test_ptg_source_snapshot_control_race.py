# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
from unittest.mock import AsyncMock

from process.ptg_parts import snapshot_cleanup, source_pointers, source_snapshot_control
from process.ptg_parts.ptg2_legacy_global_projection_queue import (
    PTG2LegacyGlobalProjectionDrain,
)


class _InterleavingState:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.promotion_locked = asyncio.Event()
        self.cleanup_waiting = asyncio.Event()
        self.current_snapshot_id = "snap_old"
        self.events = []


class _PromotionTransaction:
    def __init__(self, state):
        self.state = state
        self.locked = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.locked:
            self.state.lock.release()
            self.state.events.append("promotion_committed")
        return False

    async def execute(self, statement, params):
        sql = str(statement)
        if "set_config('lock_timeout'" in sql:
            self._assert_timeouts(params)
            return None
        if "pg_advisory_xact_lock_shared" in sql:
            await self._acquire_promotion_fence(params)
            return None
        if "hashtextextended" in sql:
            assert params == {
                "source_lock_key": "ptg2_source_lifecycle_v2:source_a"
            }
            return None
        if "INSERT INTO \"mrf\".ptg2_current_source_snapshot" in sql:
            await self._record_pointer_update(params)
            return None
        if "ptg2_v3_snapshot_plan_scope" in sql:
            assert params == {"snapshot_id": "snap_new"}
            return [{"plan_id": "P1", "plan_market_type": "group"}]
        return None

    @staticmethod
    def _assert_timeouts(params):
        assert params == {
            "lock_timeout": "500ms",
            "statement_timeout": "5s",
        }

    async def _acquire_promotion_fence(self, params):
        assert params == {
            "gc_lock_key": source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY
        }
        await self.state.lock.acquire()
        self.locked = True
        self.state.events.append("promotion_locked")
        self.state.promotion_locked.set()

    async def _record_pointer_update(self, params):
        await self.state.cleanup_waiting.wait()
        self.state.current_snapshot_id = params["snapshot_id"]
        self.state.events.append("promotion_repointed")


class _InterleavingDB:
    def __init__(self, state):
        self.state = state

    def transaction(self):
        return _PromotionTransaction(self.state)

    def acquire(self):
        raise AssertionError("ordinary source completion must not scan global state")

    @staticmethod
    def text(statement):
        return statement


def _validated_candidate_row():
    return {
        "snapshot_id": "snap_new",
        "import_run_id": "run_new",
        "status": "validated",
        "import_month": datetime.date(2026, 7, 1),
        "created_at": datetime.datetime(2026, 7, 1),
        "validated_at": datetime.datetime(2026, 7, 1, 0, 1),
        "published_at": None,
        "previous_snapshot_id": "snap_old",
        "snapshot_key": 17,
        "plan_id": "P1",
        "plan_market_type": "group",
        "coverage_scope_id": b"c" * 32,
        "manifest": {
            "activation": {
                "contract": "ptg2_candidate_activation_v1",
                "state": "validated",
                "source_key": "source_a",
                "expected_previous_snapshot_id": "snap_old",
            },
            "serving_index": {"source_key": "source_a"},
        },
    }


def _install_control_fakes(monkeypatch, state):
    """Support the install control fakes test fixture."""
    fake_db = _InterleavingDB(state)
    monkeypatch.setattr(source_pointers, "db", fake_db)
    monkeypatch.setattr(snapshot_cleanup, "db", fake_db)
    monkeypatch.setattr(
        source_pointers,
        "_locked_candidate_activation_row",
        AsyncMock(return_value=_validated_candidate_row()),
    )
    monkeypatch.setattr(
        source_pointers,
        "_database_utc_timestamp",
        AsyncMock(return_value=datetime.datetime(2026, 7, 1, 0, 2)),
    )

    async def noop(*_args, **_kwargs):
        return None

    async def verify(*_args, **_kwargs):
        state.events.append("audit_verified")
        return b"r" * 32

    async def compare_and_swap(*_args, **params):
        await state.cleanup_waiting.wait()
        state.current_snapshot_id = params["snapshot_id"]
        state.events.append("promotion_repointed")

    async def consume(*_args, **_kwargs):
        state.events.append("audit_consumed")

    async def mark_projection(*_args, **_kwargs):
        state.events.append("projection_marked")

    async def drain_projection(*_args, **_kwargs):
        return PTG2LegacyGlobalProjectionDrain(deferred=1)

    collaborator_by_name = {
        "verify_candidate_audit_attestation_in_transaction": verify,
        "_compare_and_swap_source_pointer": compare_and_swap,
        "_publish_snapshot_in_pointer_transaction": noop,
        "mark_legacy_global_projection_dirty": mark_projection,
        "drain_legacy_global_projection_queue": drain_projection,
        "_replace_source_plan_pointers": noop,
        "consume_candidate_audit_attestation_in_transaction": consume,
    }
    for collaborator_name, collaborator in collaborator_by_name.items():
        monkeypatch.setattr(source_pointers, collaborator_name, collaborator)
    monkeypatch.setattr(source_snapshot_control, "_clear_ptg2_snapshot_cache", lambda: None)


async def _run_interleaving(state):
    promotion_task = asyncio.create_task(
        source_snapshot_control.promote_ptg2_source_snapshot(
            source_key="source_a",
            snapshot_id="snap_new",
            expected_current_snapshot_id="snap_old",
        )
    )
    await state.promotion_locked.wait()
    await asyncio.wait_for(
        snapshot_cleanup._cleanup_old_ptg2_source_tables(
            "source_a",
            {"snap_old"},
            lock_pointer_state=True,
        ),
        timeout=0.5,
    )
    assert not promotion_task.done()
    state.cleanup_waiting.set()
    await asyncio.wait_for(promotion_task, timeout=1)


def test_source_completion_cleanup_does_not_gate_promotion_or_scan_global_state(
    monkeypatch,
):
    """Keep global scanning and GC outside the source completion path."""
    state = _InterleavingState()
    _install_control_fakes(monkeypatch, state)
    monkeypatch.setenv(snapshot_cleanup.PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE_ENV, "1")

    asyncio.run(_run_interleaving(state))

    assert state.events == [
        "promotion_locked",
        "audit_verified",
        "promotion_repointed",
        "audit_consumed",
        "projection_marked",
        "promotion_committed",
    ]
