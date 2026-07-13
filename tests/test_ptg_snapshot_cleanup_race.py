# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
from dataclasses import dataclass, field

from process.ptg_parts import snapshot_cleanup, source_pointers


SNAPSHOT_ROWS = [
    {
        "snapshot_id": "snap_b",
        "previous_snapshot_id": "snap_a",
        "manifest": {
            "serving_index": {
                "source_key": "source_a",
                "arch_version": "postgres_binary_v3",
                "storage_generation": "shared_blocks_v3",
            }
        },
    },
    {
        "snapshot_id": "snap_a",
        "previous_snapshot_id": "snap_previous_1",
        "manifest": {
            "serving_index": {
                "source_key": "source_a",
                "arch_version": "postgres_binary_v3",
                "storage_generation": "shared_blocks_v3",
            }
        },
    },
    {
        "snapshot_id": "snap_previous_1",
        "previous_snapshot_id": "snap_previous_2",
        "manifest": {
            "serving_index": {
                "source_key": "source_a",
                "arch_version": "postgres_binary_v3",
                "storage_generation": "shared_blocks_v3",
            }
        },
    },
    {
        "snapshot_id": "snap_previous_2",
        "previous_snapshot_id": "snap_old",
        "manifest": {
            "serving_index": {
                "source_key": "source_a",
                "arch_version": "postgres_binary_v3",
                "storage_generation": "shared_blocks_v3",
            }
        },
    },
    {
        "snapshot_id": "snap_old",
        "previous_snapshot_id": None,
        "manifest": {
            "serving_index": {
                "source_key": "source_a",
                "arch_version": "postgres_binary_v3",
                "storage_generation": "shared_blocks_v3",
            }
        },
    },
]


@dataclass
class _InterleavingState:
    events: list[str] = field(default_factory=list)
    current_snapshot_id: str = "snap_a"
    publish_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    b_has_publish_lock: asyncio.Event = field(default_factory=asyncio.Event)
    a_is_waiting: asyncio.Event = field(default_factory=asyncio.Event)
    allow_b_commit: asyncio.Event = field(default_factory=asyncio.Event)


class _InterleavingConnection:
    def __init__(self, state: _InterleavingState):
        self.state = state
        self.lock_held = False

    async def status(self, statement, **params):
        if "pg_advisory_xact_lock" in statement:
            assert (
                params["publish_lock_key"]
                == source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY
            )
            self.state.events.append("a_cleanup_waiting_for_publish_lock")
            self.state.a_is_waiting.set()
            await self.state.publish_lock.acquire()
            self.lock_held = True
            self.state.events.append("a_cleanup_acquired_publish_lock")
        elif "DROP TABLE" in statement:
            self.state.events.append(statement)
        return 1

    async def all(self, statement, **params):
        if "current_refs" in statement:
            assert params == {"source_key": "source_a"}
            self.state.events.append("a_cleanup_replanned_current_pointer")
            return [{"snapshot_id": self.state.current_snapshot_id}]
        if "ptg2_snapshot" in statement:
            assert params == {}
            self.state.events.append("a_cleanup_loaded_lineage")
            return SNAPSHOT_ROWS
        raise AssertionError(statement)


class _Acquire:
    def __init__(self, connection: _InterleavingConnection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        if self.connection.lock_held:
            self.connection.state.publish_lock.release()
        return False


class _FakeDB:
    def __init__(self, connection: _InterleavingConnection):
        self.connection = connection

    def acquire(self):
        return _Acquire(self.connection)


async def _publish_b(state: _InterleavingState) -> None:
    await state.publish_lock.acquire()
    state.events.append("b_acquired_publish_lock")
    state.b_has_publish_lock.set()
    await state.allow_b_commit.wait()
    state.current_snapshot_id = "snap_b"
    state.events.append("b_committed_current_pointer")
    state.publish_lock.release()


async def _run_interleaving(monkeypatch) -> list[str]:
    state = _InterleavingState()
    connection = _InterleavingConnection(state)
    monkeypatch.setattr(snapshot_cleanup, "db", _FakeDB(connection))

    async def release_unbound(*, schema_name, executor, require_shared):
        assert schema_name == "mrf"
        assert executor is connection
        assert require_shared is True
        state.events.append("a_cleanup_released_unbound_layouts")

    monkeypatch.setattr(
        snapshot_cleanup,
        "release_unbound_ptg2_shared_layouts",
        release_unbound,
    )
    publish_task = asyncio.create_task(_publish_b(state))
    await state.b_has_publish_lock.wait()
    cleanup_task = asyncio.create_task(
        snapshot_cleanup._cleanup_old_ptg2_source_tables(
            "source_a",
            {"snap_a"},
            lock_pointer_state=True,
        )
    )
    await state.a_is_waiting.wait()
    state.allow_b_commit.set()
    await asyncio.gather(publish_task, cleanup_task)
    return state.events


def test_stale_post_publish_cleanup_replans_after_newer_promotion(monkeypatch):
    monkeypatch.delenv(snapshot_cleanup.PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE_ENV, raising=False)

    events = asyncio.run(_run_interleaving(monkeypatch))

    assert events == [
        "b_acquired_publish_lock",
        "a_cleanup_waiting_for_publish_lock",
        "b_committed_current_pointer",
        "a_cleanup_acquired_publish_lock",
        "a_cleanup_replanned_current_pointer",
        "a_cleanup_loaded_lineage",
        "a_cleanup_released_unbound_layouts",
    ]
