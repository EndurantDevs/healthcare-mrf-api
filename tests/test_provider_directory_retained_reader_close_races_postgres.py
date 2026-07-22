# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Adversarial close ordering for claimed retained-artifact readers."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest

from process import provider_directory_retained_reader as reader_module
from process.provider_directory_retained_artifact_contract import (
    RetainedArtifactError,
)
from process.provider_directory_retained_reader import (
    claimed_retained_artifact_reader,
)
from tests import (
    provider_directory_retained_reader_postgres_support as reader_test_support,
)
from tests.provider_directory_retained_core_postgres_support import (
    retained_database,
    retained_peer_connection,
)


pytest_plugins = ("tests.provider_directory_retained_reader_fixtures",)


class _BlockedRead:
    """Hold one physical read and record unsafe early cleanup."""

    def __init__(self, *, fail: bool = False):
        self.started = threading.Event()
        self.release = threading.Event()
        self.ended = threading.Event()
        self.is_abort_early = False
        self.is_release_early = False
        self.fail = fail
        self.original_read = reader_module._OpenedArtifactBlob.read_at

    def read_at(self, opened_blob, byte_count: int, offset: int):
        self.started.set()
        if not self.release.wait(timeout=5):
            raise TimeoutError("blocked retained read timed out")
        try:
            if self.fail:
                raise RetainedArtifactError("fixture_worker_failure")
            return self.original_read(opened_blob, byte_count, offset)
        finally:
            self.ended.set()


class _ReleaseTrace:
    """Record whether claim release crossed an unsettled operation."""

    def __init__(self):
        self.is_early = False


async def _wait_for_thread_event(event: threading.Event) -> None:
    assert await asyncio.wait_for(asyncio.to_thread(event.wait, 3), timeout=4)


def _trace_cleanup(monkeypatch, control: _BlockedRead) -> None:
    original_abort = reader_module._OpenedArtifactBlob.abort
    original_release = reader_module._release_reader_claim

    def traced_abort(opened_blob) -> None:
        if not control.ended.is_set():
            control.is_abort_early = True
        original_abort(opened_blob)

    async def traced_release(reader) -> None:
        if not control.ended.is_set():
            control.is_release_early = True
        await original_release(reader)

    def delegated_read(opened_blob, byte_count: int, offset: int):
        return control.read_at(opened_blob, byte_count, offset)

    monkeypatch.setattr(reader_module._OpenedArtifactBlob, "abort", traced_abort)
    monkeypatch.setattr(reader_module, "_release_reader_claim", traced_release)
    monkeypatch.setattr(reader_module._OpenedArtifactBlob, "read_at", delegated_read)


async def _enter_claimed_reader(connection, campaign_id: str):
    manager = claimed_retained_artifact_reader(
        connection,
        campaign_id=campaign_id,
        consumer_recipe_id=reader_test_support.READER_RECIPE_ID,
    )
    return manager, await manager.__aenter__()


async def _assert_claim_active(campaign_id: str) -> None:
    async with retained_peer_connection() as peer_connection:
        await reader_test_support.assert_reader_claim_active(
            peer_connection,
            campaign_id,
            reader_test_support.READER_RECIPE_ID,
        )


async def _cancel_twice(task: asyncio.Task, prefix: str) -> None:
    task.cancel(f"first-{prefix}-cancellation")
    await asyncio.sleep(0)
    task.cancel(f"repeated-{prefix}-cancellation")
    await asyncio.sleep(0)
    assert not task.done()


@pytest.mark.asyncio
async def test_explicit_close_joins_failed_worker_through_repeated_cancellation(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    """Do not abort an in-flight descriptor; preserve both task outcomes."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, _bytes, _path = (
            await reader_test_support.sealed_reader_campaign(
                connection, retained_artifact_test_root, "reader-close-race"
            )
        )
        control = _BlockedRead(fail=True)
        _trace_cleanup(monkeypatch, control)
        manager, reader = await _enter_claimed_reader(connection, campaign_id)
        stream = await reader.iter_full(retained_item.source_item_id, chunk_bytes=1)
        stream.__aiter__()
        read_task = asyncio.create_task(stream.__anext__())
        await _wait_for_thread_event(control.started)
        close_task = asyncio.create_task(stream.aclose())
        await asyncio.sleep(0)
        await _cancel_twice(close_task, "close")
        assert stream._opened_blob._descriptor >= 0
        await _assert_claim_active(campaign_id)
        assert not control.is_abort_early and not control.is_release_early
        control.release.set()
        with pytest.raises(RetainedArtifactError, match="fixture_worker_failure"):
            await read_task
        with pytest.raises(asyncio.CancelledError) as cancellation:
            await close_task
        assert str(cancellation.value) == "first-close-cancellation"
        assert control.ended.is_set() and not control.is_abort_early
        assert stream._opened_blob._descriptor == -1
        with pytest.raises(RetainedArtifactError, match="incomplete_read"):
            await manager.__aexit__(None, None, None)
        assert not control.is_release_early
        await reader_test_support.assert_reader_claim_released(connection, campaign_id)


class _ConsumerFailure(RuntimeError):
    """Primary consumer failure used to drive exceptional context exit."""


@pytest.mark.asyncio
async def test_consumer_exception_exit_joins_read_before_abort_and_release(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    """Exceptional context cleanup cannot outrun another task's read."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, _bytes, _path = (
            await reader_test_support.sealed_reader_campaign(
                connection, retained_artifact_test_root, "reader-error-exit-race"
            )
        )
        control = _BlockedRead()
        _trace_cleanup(monkeypatch, control)
        manager, reader = await _enter_claimed_reader(connection, campaign_id)
        stream = await reader.iter_full(retained_item.source_item_id, chunk_bytes=1)
        stream.__aiter__()
        read_task = asyncio.create_task(stream.__anext__())
        await _wait_for_thread_event(control.started)
        try:
            raise _ConsumerFailure("primary consumer failure")
        except _ConsumerFailure as error:
            exit_task = asyncio.create_task(
                manager.__aexit__(type(error), error, error.__traceback__)
            )
        await asyncio.sleep(0)
        assert not exit_task.done() and stream._opened_blob._descriptor >= 0
        assert not control.is_abort_early and not control.is_release_early
        await _assert_claim_active(campaign_id)
        control.release.set()
        assert await read_task
        assert await exit_task is False
        assert control.ended.is_set() and not control.is_abort_early
        assert stream._opened_blob._descriptor == -1
        assert not control.is_release_early
        await reader_test_support.assert_reader_claim_released(connection, campaign_id)


@pytest.mark.asyncio
async def test_context_exit_joins_pending_stream_start_before_claim_release(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, _bytes, _path = (
            await reader_test_support.sealed_reader_campaign(
                connection, retained_artifact_test_root, "reader-start-race"
            )
        )
        manager, reader = await _enter_claimed_reader(connection, campaign_id)
        binding_started = asyncio.Event()
        release_binding = asyncio.Event()
        opened = asyncio.Event()
        release_trace = _ReleaseTrace()
        original_assert = reader._assert_binding
        original_open = reader_module._open_retained_artifact_blob
        original_release = reader_module._release_reader_claim

        async def blocked_assert(artifact, layout_range) -> None:
            binding_started.set()
            await release_binding.wait()
            await original_assert(artifact, layout_range)

        def tracked_open(*args):
            opened_blob = original_open(*args)
            opened.set()
            return opened_blob

        async def tracked_release(closing_reader) -> None:
            if not opened.is_set():
                release_trace.is_early = True
            await original_release(closing_reader)

        monkeypatch.setattr(reader, "_assert_binding", blocked_assert)
        monkeypatch.setattr(reader_module, "_open_retained_artifact_blob", tracked_open)
        monkeypatch.setattr(reader_module, "_release_reader_claim", tracked_release)
        start_task = asyncio.create_task(reader.iter_full(retained_item.source_item_id))
        await binding_started.wait()
        exit_task = asyncio.create_task(manager.__aexit__(None, None, None))
        await asyncio.sleep(0)
        assert not exit_task.done()
        assert not opened.is_set() and not release_trace.is_early
        await _assert_claim_active(campaign_id)
        release_binding.set()
        stream = await start_task
        with pytest.raises(RetainedArtifactError, match="incomplete_read"):
            await exit_task
        assert opened.is_set() and stream._opened_blob._descriptor == -1
        assert not release_trace.is_early
        stream.__aiter__()
        with pytest.raises(RetainedArtifactError, match="reader_closed"):
            await stream.__anext__()
        await reader_test_support.assert_reader_claim_released(connection, campaign_id)


@pytest.mark.asyncio
async def test_context_exit_joins_registered_heartbeat_through_cancellation(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    """Close excludes new heartbeats and releases only after the active one."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, _bytes, _path = (
            await reader_test_support.sealed_reader_campaign(
                connection, retained_artifact_test_root, "reader-heartbeat-race"
            )
        )
        manager, reader = await _enter_claimed_reader(connection, campaign_id)
        stream = await reader.iter_full(retained_item.source_item_id, chunk_bytes=1)
        stream.__aiter__()
        heartbeat_started = asyncio.Event()
        release_heartbeat = asyncio.Event()
        heartbeat_finished = asyncio.Event()
        release_trace = _ReleaseTrace()
        original_heartbeat = reader_module.heartbeat_retained_campaign_consumer
        original_release = reader_module._release_reader_claim

        async def blocked_heartbeat(*args, **kwargs) -> None:
            heartbeat_started.set()
            await release_heartbeat.wait()
            await original_heartbeat(*args, **kwargs)
            heartbeat_finished.set()

        async def tracked_release(closing_reader) -> None:
            if not heartbeat_finished.is_set():
                release_trace.is_early = True
            await original_release(closing_reader)

        monkeypatch.setattr(
            reader_module, "heartbeat_retained_campaign_consumer", blocked_heartbeat
        )
        monkeypatch.setattr(reader_module, "_release_reader_claim", tracked_release)
        heartbeat_task = asyncio.create_task(reader.heartbeat())
        await heartbeat_started.wait()
        exit_task = asyncio.create_task(manager.__aexit__(None, None, None))
        await asyncio.sleep(0)
        await _cancel_twice(exit_task, "heartbeat-exit")
        with pytest.raises(RetainedArtifactError, match="reader_closed"):
            await reader.heartbeat()
        assert stream._close_started is False
        with pytest.raises(RetainedArtifactError, match="reader_closed"):
            await stream.__anext__()
        await _assert_claim_active(campaign_id)
        assert not release_trace.is_early
        release_heartbeat.set()
        await heartbeat_task
        with pytest.raises(asyncio.CancelledError) as cancellation:
            await exit_task
        assert str(cancellation.value) == "first-heartbeat-exit-cancellation"
        assert heartbeat_finished.is_set() and not release_trace.is_early
        assert stream._opened_blob._descriptor == -1
        await reader_test_support.assert_reader_claim_released(connection, campaign_id)


@pytest.mark.asyncio
async def test_same_reader_binding_waits_for_heartbeat_database_lock(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    """Serialize same-connection heartbeat and binding database operations."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, artifact_bytes, _path = (
            await reader_test_support.sealed_reader_campaign(
                connection, retained_artifact_test_root, "reader-database-lock"
            )
        )
        manager, reader = await _enter_claimed_reader(connection, campaign_id)
        heartbeat_holds_lock = asyncio.Event()
        release_heartbeat = asyncio.Event()
        binding_entered = asyncio.Event()
        original_heartbeat = reader_module.heartbeat_retained_campaign_consumer
        original_binding = reader_module.assert_active_reader_binding

        async def blocked_heartbeat(*args, **kwargs) -> None:
            heartbeat_holds_lock.set()
            await release_heartbeat.wait()
            await original_heartbeat(*args, **kwargs)

        async def observed_binding(*args, **kwargs) -> None:
            binding_entered.set()
            await original_binding(*args, **kwargs)

        monkeypatch.setattr(
            reader_module, "heartbeat_retained_campaign_consumer", blocked_heartbeat
        )
        monkeypatch.setattr(
            reader_module, "assert_active_reader_binding", observed_binding
        )
        heartbeat_task = asyncio.create_task(reader.heartbeat())
        await heartbeat_holds_lock.wait()
        start_task = asyncio.create_task(reader.iter_full(retained_item.source_item_id))
        await asyncio.sleep(0)
        assert not binding_entered.is_set() and not start_task.done()
        release_heartbeat.set()
        await heartbeat_task
        stream = await start_task
        assert binding_entered.is_set()
        assert await reader_test_support.collect_new_stream(stream) == artifact_bytes
        assert await manager.__aexit__(None, None, None) is False
        await reader_test_support.assert_reader_claim_released(connection, campaign_id)
