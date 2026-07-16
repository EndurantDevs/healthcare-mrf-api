# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
from unittest.mock import AsyncMock

from process.ptg_parts import snapshot_cleanup, source_pointers, source_snapshot_control


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
        if "pg_advisory_xact_lock" in sql:
            assert params == {"publish_lock_key": source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY}
            await self.state.lock.acquire()
            self.locked = True
            self.state.events.append("promotion_locked")
            self.state.promotion_locked.set()
        elif "INSERT INTO \"mrf\".ptg2_current_source_snapshot" in sql:
            await self.state.cleanup_waiting.wait()
            self.state.current_snapshot_id = params["snapshot_id"]
            self.state.events.append("promotion_repointed")
        elif "ptg2_v3_snapshot_plan_scope" in sql:
            assert params == {"snapshot_id": "snap_new"}
            return [{"plan_id": "P1", "plan_market_type": "group"}]


class _CleanupConnection:
    def __init__(self, state):
        self.state = state
        self.locked = False

    async def status(self, statement, **params):
        if "pg_advisory_xact_lock" in statement:
            assert params == {"publish_lock_key": source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY}
            self.state.events.append("cleanup_waiting")
            self.state.cleanup_waiting.set()
            await self.state.lock.acquire()
            self.locked = True
            self.state.events.append("cleanup_locked")
        elif "DROP TABLE" in statement:
            self.state.events.append(statement)
        return 1

    async def all(self, statement, **params):
        if "current_refs" in statement:
            assert params == {"source_key": "source_a"}
            self.state.events.append("cleanup_read_current")
            return [{"snapshot_id": self.state.current_snapshot_id}]
        if "ptg2_snapshot" in statement:
            assert params == {}
            return [
                {
                    "snapshot_id": "snap_new",
                    "previous_snapshot_id": "snap_old",
                    "manifest": {
                        "serving_index": {
                            "source_key": "source_a",
                            "table": "mrf.ptg2_serving_new",
                        }
                    },
                },
                {
                    "snapshot_id": "snap_old",
                    "previous_snapshot_id": None,
                    "manifest": {
                        "serving_index": {
                            "source_key": "source_a",
                            "table": "mrf.ptg2_serving_old",
                        }
                    },
                },
            ]
        raise AssertionError(statement)


class _CleanupAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        if self.connection.locked:
            self.connection.state.lock.release()
        return False


class _InterleavingDB:
    def __init__(self, state):
        self.state = state
        self.cleanup_connection = _CleanupConnection(state)

    def transaction(self):
        return _PromotionTransaction(self.state)

    def acquire(self):
        return _CleanupAcquire(self.cleanup_connection)

    @staticmethod
    def text(statement):
        return statement


def _install_control_fakes(monkeypatch, state):
    """Support the install control fakes test fixture."""
    fake_db = _InterleavingDB(state)
    monkeypatch.setattr(source_pointers, "db", fake_db)
    monkeypatch.setattr(snapshot_cleanup, "db", fake_db)
    monkeypatch.setattr(
        source_pointers,
        "_locked_candidate_activation_row",
        AsyncMock(
            return_value={
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
        ),
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

    monkeypatch.setattr(
        source_pointers,
        "verify_candidate_audit_attestation_in_transaction",
        verify,
    )
    monkeypatch.setattr(source_pointers, "_compare_and_swap_source_pointer", compare_and_swap)
    monkeypatch.setattr(source_pointers, "_publish_snapshot_in_pointer_transaction", noop)
    monkeypatch.setattr(source_pointers, "_reconcile_global_snapshot_pointer", noop)
    monkeypatch.setattr(source_pointers, "_replace_source_plan_pointers", noop)
    monkeypatch.setattr(
        source_pointers,
        "consume_candidate_audit_attestation_in_transaction",
        consume,
    )
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
    cleanup_task = asyncio.create_task(
        snapshot_cleanup._cleanup_old_ptg2_source_tables(
            "source_a",
            {"snap_old"},
            lock_pointer_state=True,
        )
    )
    await asyncio.gather(promotion_task, cleanup_task)


def test_cleanup_waits_for_manual_promotion_and_preserves_promoted_snapshot(monkeypatch):
    """Cleanup must replan after a manual promotion holding the shared GC lock."""
    state = _InterleavingState()
    _install_control_fakes(monkeypatch, state)
    monkeypatch.setenv(snapshot_cleanup.PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE_ENV, "1")

    asyncio.run(_run_interleaving(state))

    assert state.events == [
        "promotion_locked",
        "audit_verified",
        "cleanup_waiting",
        "promotion_repointed",
        "audit_consumed",
        "promotion_committed",
        "cleanup_locked",
        "cleanup_read_current",
    ]
