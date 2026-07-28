# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deterministic fail-closed contracts for frozen PTG runtime boundaries."""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager

import pytest

from process import control_lifecycle, live_progress
from process.ptg_parts import frozen_rate_binding_store


@pytest.mark.asyncio
async def test_anonymous_binding_admission_avoids_database_work():
    """An import without source identity has no durable binding to inspect."""

    assert (
        await frozen_rate_binding_store.insert_or_compare_frozen_binding(
            object(),
            {},
        )
        is None
    )


@pytest.mark.asyncio
async def test_binding_transaction_uses_one_owned_connection(monkeypatch):
    """The transaction wrapper passes one acquired connection to admission."""

    connection = object()
    observed_calls: list[tuple[object, dict[str, str]]] = []

    @asynccontextmanager
    async def acquire():
        yield connection

    async def admit(
        acquired_connection,
        params_by_name,
    ):
        observed_calls.append((acquired_connection, params_by_name))
        return {"source_file_import_id": "source-import"}

    monkeypatch.setattr(frozen_rate_binding_store.db, "acquire", acquire)
    monkeypatch.setattr(
        frozen_rate_binding_store,
        "insert_or_compare_frozen_binding",
        admit,
    )

    result = (
        await frozen_rate_binding_store.insert_or_compare_frozen_binding_transaction(
            {"source_file_import_id": "source-import"}
        )
    )

    assert result == {"source_file_import_id": "source-import"}
    assert observed_calls == [
        (connection, {"source_file_import_id": "source-import"})
    ]


def test_live_progress_read_fails_closed_when_redis_is_unavailable(monkeypatch):
    """Redis read failures are absence of advisory progress, never fatal."""

    class UnavailableRedis:
        def get(self, _key):
            raise ConnectionError("redis unavailable")

    monkeypatch.setattr(live_progress, "_redis", UnavailableRedis)

    assert live_progress._read_live_progress_payload("run-id") is None


def test_heartbeat_preserves_the_last_movement_timestamp():
    """A heartbeat advances observation without inventing work progress."""

    previous_progress_by_field = {
        "run_id": "run-frozen-heartbeat",
        "event_seq": 4,
        "progress_seq": 3,
        "progressed_at": "2026-07-28T00:00:00Z",
    }
    heartbeat_progress_by_field = {
        "run_id": "run-frozen-heartbeat",
        "source": "engine-heartbeat",
    }

    live_progress._sequence_progress(
        heartbeat_progress_by_field,
        previous_progress_by_field,
        now=dt.datetime(2026, 7, 28, 0, 0, 15),
        succeeded=False,
    )

    assert heartbeat_progress_by_field["event_seq"] == 5
    assert heartbeat_progress_by_field["progress_seq"] == 3
    assert (
        heartbeat_progress_by_field["progressed_at"]
        == previous_progress_by_field["progressed_at"]
    )

    fresh_heartbeat_by_field = {
        "run_id": "run-frozen-heartbeat-without-movement",
        "source": "engine-heartbeat",
    }
    live_progress._sequence_progress(
        fresh_heartbeat_by_field,
        {"run_id": "run-frozen-heartbeat-without-movement"},
        now=dt.datetime(2026, 7, 28, 0, 0, 30),
        succeeded=False,
    )

    assert fresh_heartbeat_by_field["progress_seq"] == 0
    assert "progressed_at" not in fresh_heartbeat_by_field


def test_control_db_throttle_fails_closed_when_redis_is_unavailable(monkeypatch):
    """A throttle-store failure must suppress an extra database update."""

    class UnavailableRedis:
        def set(self, *_args, **_kwargs):
            raise ConnectionError("redis unavailable")

    monkeypatch.setattr(
        control_lifecycle,
        "_control_run_db_throttle_client",
        UnavailableRedis,
    )

    assert control_lifecycle._is_db_update_slot_claimed("slot", 0.25) is True


def test_invalid_control_db_throttle_config_uses_safe_default(monkeypatch):
    """Malformed tuning cannot disable the database-write throttle."""

    monkeypatch.setenv(
        "HLTHPRT_CONTROL_RUN_DB_UPDATE_THROTTLE_SECONDS",
        "not-a-duration",
    )

    assert control_lifecycle._control_run_db_update_throttle_seconds() == 30.0


@pytest.mark.asyncio
async def test_control_update_rejects_unreadable_database_results(monkeypatch):
    """An uninspectable update result cannot be reported as a persisted row."""

    class UnreadableResult:
        def all(self):
            raise RuntimeError("result unavailable")

    async def connect():
        return None

    async def execute(_statement):
        return UnreadableResult()

    monkeypatch.setattr(control_lifecycle.db, "connect", connect)
    monkeypatch.setattr(control_lifecycle.db, "execute", execute)

    assert await control_lifecycle._execute_control_run_update(object()) == 0


def test_live_progress_cas_rejects_read_and_write_failures(monkeypatch):
    """CAS storage failures never publish advisory progress as accepted."""

    class ReadUnavailableRedis:
        def get(self, _key):
            raise ConnectionError("read unavailable")

    assert (
        live_progress._is_live_progress_written_with_cas(
            redis_client=ReadUnavailableRedis(),
            run_id="run-read",
            context={},
            progress_by_field={},
            observed_at="2026-07-28T00:00:00Z",
            now=dt.datetime(2026, 7, 28),
            status_event_payload=None,
        )
        is False
    )

    class WriteUnavailableRedis:
        def get(self, _key):
            return None

        def eval(self, *_args):
            raise ConnectionError("write unavailable")

    monkeypatch.setattr(
        live_progress,
        "_merged_live_progress_candidate",
        lambda **_kwargs: {"run_id": "run-write"},
    )

    assert (
        live_progress._is_live_progress_written_with_cas(
            redis_client=WriteUnavailableRedis(),
            run_id="run-write",
            context={},
            progress_by_field={},
            observed_at="2026-07-28T00:00:00Z",
            now=dt.datetime(2026, 7, 28),
            status_event_payload=None,
        )
        is False
    )


def test_live_progress_lock_release_is_best_effort():
    """A failed unlock cannot turn accepted progress into a worker failure."""

    class UnavailableRedis:
        def eval(self, *_args):
            raise ConnectionError("unlock unavailable")

    live_progress._release_progress_publication_lock(
        UnavailableRedis(),
        "run-id",
        "lock-token",
    )
