"""Focused test support for PTG import orchestration exits."""

from __future__ import annotations

from contextlib import asynccontextmanager
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock


class RecordingSourceLock:
    """Record source-lock lifecycle while allowing a configured entry failure."""

    def __init__(self, *, enter_error: BaseException | None = None) -> None:
        self.enter_error = enter_error
        self.enter_count = 0
        self.exit_count = 0

    async def __aenter__(self) -> "RecordingSourceLock":
        self.enter_count += 1
        if self.enter_error is not None:
            raise self.enter_error
        return self

    async def __aexit__(self, _error_type, _error, _traceback) -> bool:
        self.exit_count += 1
        return False


class FirstResult:
    """Minimal database result exposing one first row."""

    def __init__(self, first_row) -> None:
        self.first_row = first_row

    def first(self):
        """Return the configured first row."""

        return self.first_row


class AllResult:
    """Minimal database result exposing all configured rows."""

    def __init__(self, rows) -> None:
        self.rows = rows

    def all(self):
        """Return the configured result rows."""

        return self.rows


class RecordingSession:
    """Capture executed statements for direct orchestration helper tests."""

    def __init__(self, results=None) -> None:
        self.results = list(results or [])
        self.executions: list[tuple[object, object]] = []

    async def execute(self, statement, parameters=None):
        """Record one statement and return the next configured result."""

        self.executions.append((statement, parameters))
        if self.results:
            return self.results.pop(0)
        raise AssertionError(f"unexpected SQL after scripted results: {statement}")


class TaskState:
    """Deterministic awaitable exposing the task state API under test."""

    def __init__(self, *, done: bool, cancelled: bool = False) -> None:
        self.is_done = done
        self.is_cancelled = cancelled
        self.done = lambda: self.is_done
        self.cancelled = lambda: self.is_cancelled
        self.cancel_count = 0
        self.result_count = 0

    def result(self) -> None:
        """Record retrieval of a completed task result."""

        self.result_count += 1

    def cancel(self) -> None:
        """Record cancellation of a running task."""

        self.cancel_count += 1

    def __await__(self):
        """Raise the cancellation observed after a running task is stopped."""

        async def cancelled_wait():
            raise asyncio.CancelledError

        return cancelled_wait().__await__()


def transaction_factory(session):
    """Return a database transaction factory yielding one fake session."""

    @asynccontextmanager
    async def transaction():
        yield session

    return transaction


def _install_progress_boundary(monkeypatch, process_ptg):
    """Install recording doubles for live progress context lifecycle."""

    progress_events: list[dict[str, object]] = []
    reset_tokens: list[object] = []
    monkeypatch.setattr(
        process_ptg,
        "set_live_progress_context",
        lambda **_fields: "live-token",
    )
    monkeypatch.setattr(process_ptg, "reset_live_progress_context", reset_tokens.append)
    monkeypatch.setattr(
        process_ptg,
        "write_live_progress",
        lambda **fields: progress_events.append(fields),
    )
    return progress_events, reset_tokens


def install_import_boundary(
    monkeypatch,
    process_ptg,
    *,
    snapshot_state=None,
    source_pointer=None,
    allowed_pointer=None,
    source_lock=None,
):
    """Install the common database and lifecycle boundary doubles."""

    lock = source_lock or RecordingSourceLock()
    pushed_snapshot = snapshot_state or {
        "snapshot_id": "snapshot-test",
        "import_run_id": "ptg2:orchestration-test",
        "status": process_ptg.PTG2_STATUS_BUILDING,
        "snapshot_claim_status": "acquired",
    }
    push = AsyncMock(return_value=pushed_snapshot)
    progress_events, reset_tokens = _install_progress_boundary(
        monkeypatch,
        process_ptg,
    )

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_ptg2_source_import_lock", lambda _key: lock)
    monkeypatch.setattr(
        process_ptg,
        "_current_source_snapshot_id",
        AsyncMock(return_value=source_pointer),
    )
    monkeypatch.setattr(
        process_ptg,
        "_current_allowed_snapshot_id",
        AsyncMock(return_value=allowed_pointer),
    )
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={}))
    monkeypatch.setattr(process_ptg, "_cleanup_failed_ptg2_source_state", AsyncMock())
    monkeypatch.setattr(process_ptg, "_mark_ptg2_import_failed", AsyncMock(return_value={}))
    monkeypatch.setattr(process_ptg, "_is_failed_shared_layout_abandoned", AsyncMock())
    monkeypatch.setattr(process_ptg, "_heartbeat_ptg2_import_run", AsyncMock())
    monkeypatch.setattr(process_ptg, "_stop_ptg2_import_heartbeat", AsyncMock())
    monkeypatch.setattr(process_ptg, "_cleanup_manifest_copy_entries", Mock())
    monkeypatch.setattr(process_ptg, "_cleanup_strict_v3_graph_artifacts", Mock())
    return SimpleNamespace(
        lock=lock,
        push=push,
        progress_events=progress_events,
        reset_tokens=reset_tokens,
    )


def import_arguments(**overrides):
    """Return stable arguments for an isolated strict-V3 orchestration run."""

    arguments_by_name = {
        "import_id": "orchestration-test",
        "source_key": "source-test",
        "import_month": "2026-07",
    }
    arguments_by_name.update(overrides)
    return arguments_by_name
