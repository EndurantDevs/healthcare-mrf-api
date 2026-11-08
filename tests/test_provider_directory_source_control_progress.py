# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Progress keeps its attempt identity while leaving the Source transaction."""

import asyncio
import importlib
from unittest.mock import AsyncMock

import pytest

from db import connection as database_context
from process import provider_directory_generic_source_policy as source
from process import provider_directory_owned_wal_transaction as owned
from process import provider_directory_source_checkpoint_control as control
from process.live_progress import current_live_progress_context, reset_live_progress_context, set_live_progress_context


@pytest.mark.asyncio
async def test_ordinary_progress_keeps_its_original_task_and_binding():
    parent = asyncio.current_task()
    original = database_context._TRANSACTION.get()

    async def marker(run_id, **fields):
        assert asyncio.current_task() is parent
        assert database_context._TRANSACTION.get() is original
        return run_id, fields

    assert await control.persist_control_progress(marker, "run", phase="scanning") == ("run", {"phase": "scanning"})


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "failure", "cancel"])
async def test_source_progress_uses_clean_writer_and_preserves_parent(monkeypatch, outcome):
    parent = asyncio.current_task()
    bindings = (
        (database_context._TRANSACTION, (object(),)),
        (database_context._READER, object()),
        (database_context._SESSION, object()),
        (source._PUBLISHER, object()),
        (source._OWNER, object()),
        (source._FACTORY, object()),
        (owned._ACTIVE_OWNED_TRANSACTION, object()),
    )
    tokens = [(variable, variable.set(context_value)) for variable, context_value in bindings]
    live = set_live_progress_context(run_id="run", attempt_id="attempt", attempt_started_at="2026-08-07T00:00:00Z")
    attempts = current_live_progress_context()
    entered, exited = asyncio.Event(), asyncio.Event()
    monkeypatch.setattr(control, "bound_publisher", lambda: source._PUBLISHER.get())

    async def is_progress_written(run_id, **fields):
        try:
            assert asyncio.current_task() is not parent
            assert database_context._TRANSACTION.get() == ()
            for variable, _value in bindings[1:]:
                assert variable.get() is None
            assert current_live_progress_context() == attempts
            assert (run_id, fields) == ("run", {"phase": "scanning"})
            entered.set()
            if outcome == "failure":
                raise RuntimeError("ordinary progress failed")
            if outcome == "cancel":
                raise asyncio.CancelledError
            return True
        finally:
            exited.set()

    try:
        if outcome == "failure":
            with pytest.raises(RuntimeError, match="ordinary progress failed"):
                await control.persist_control_progress(is_progress_written, "run", phase="scanning")
        elif outcome == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await control.persist_control_progress(is_progress_written, "run", phase="scanning")
        else:
            assert await control.persist_control_progress(is_progress_written, "run", phase="scanning") is True
        assert entered.is_set() and exited.is_set()
        for variable, context_value in bindings:
            assert variable.get() is context_value
        assert current_live_progress_context() == attempts
    finally:
        reset_live_progress_context(live)
        for variable, token in reversed(tokens):
            variable.reset(token)


@pytest.mark.asyncio
async def test_unverified_source_context_never_starts_progress(monkeypatch):
    token = source._PUBLISHER.set(object())
    marker = AsyncMock()

    def refuse():
        raise RuntimeError("publisher context changed")

    monkeypatch.setattr(control, "bound_publisher", refuse)
    try:
        with pytest.raises(RuntimeError, match="publisher context changed"):
            await control.persist_control_progress(marker, "run")
        marker.assert_not_awaited()
    finally:
        source._PUBLISHER.reset(token)


@pytest.mark.asyncio
async def test_fhir_progress_uses_the_separate_control_writer(monkeypatch):
    directory = importlib.import_module("process.provider_directory_fhir")

    publisher = source._PUBLISHER.set(object())
    transaction = database_context._TRANSACTION.set((object(),))
    monkeypatch.setattr(control, "bound_publisher", lambda: source._PUBLISHER.get())
    calls = []

    async def is_progress_written(run_id, **fields):
        assert database_context._TRANSACTION.get() == () and source._PUBLISHER.get() is None
        calls.append((run_id, fields))
        return True

    monkeypatch.setattr(directory, "mark_control_run", is_progress_written)
    try:
        await directory._persist_provider_directory_progress(
            "run", None, phase="scanning", message="scanning", progress_by_name={"done": 1}, metrics={"pages": 1}
        )
        assert calls == [
            (
                "run",
                dict(
                    status="running",
                    phase_detail="scanning",
                    progress_message="scanning",
                    progress={"done": 1},
                    metrics={"pages": 1},
                ),
            )
        ]
        assert database_context._TRANSACTION.get() and source._PUBLISHER.get() is not None
    finally:
        database_context._TRANSACTION.reset(transaction)
        source._PUBLISHER.reset(publisher)
