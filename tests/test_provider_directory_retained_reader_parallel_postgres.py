# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import threading
from contextlib import suppress
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


class _OverlapControl:
    """Bound two physical reads until their claim state is inspected."""

    def __init__(self):
        self.read_barrier = threading.Barrier(2)
        self.reads_overlapped = threading.Event()
        self.release_reads = threading.Event()
        self.thread_ids: set[int] = set()
        self.thread_lock = threading.Lock()
        self.original_read_at = reader_module._OpenedArtifactBlob.read_at

    def read_at(self, opened_blob, byte_count: int, offset: int):
        """Record, synchronize, and release one private physical read."""

        with self.thread_lock:
            self.thread_ids.add(threading.get_ident())
        self.read_barrier.wait(timeout=3)
        self.reads_overlapped.set()
        if not self.release_reads.wait(timeout=5):
            raise TimeoutError("overlap read release timed out")
        return self.original_read_at(opened_blob, byte_count, offset)


class _CancellationControl:
    """Hold one worker read until repeated caller cancellation is observed."""

    def __init__(self):
        self.read_started = threading.Event()
        self.release_read = threading.Event()

    def blocked_failing_read_at(self, _opened_blob, _byte_count: int, _offset: int):
        """Settle with a private failure only after the proof releases the worker."""

        self.read_started.set()
        if not self.release_read.wait(timeout=5):
            raise TimeoutError("cancelled read release timed out")
        raise RetainedArtifactError("worker_failure_after_cancellation")


def _install_read_at_delegate(monkeypatch, read_at_callback) -> None:
    def delegated_read_at(opened_blob, byte_count: int, offset: int):
        return read_at_callback(opened_blob, byte_count, offset)

    monkeypatch.setattr(
        reader_module._OpenedArtifactBlob,
        "read_at",
        delegated_read_at,
    )


async def _start_overlapping_reads(
    first_reader,
    second_reader,
    retained_item,
    artifact_bytes: bytes,
):
    first_stream = await first_reader.iter_full(
        retained_item.source_item_id,
        chunk_bytes=len(artifact_bytes),
    )
    second_stream = await second_reader.iter_full(
        retained_item.source_item_id,
        chunk_bytes=len(artifact_bytes),
    )
    read_tasks = (
        asyncio.create_task(reader_test_support.collect_new_stream(first_stream)),
        asyncio.create_task(reader_test_support.collect_new_stream(second_stream)),
    )
    return first_stream, second_stream, read_tasks


async def _assert_overlapping_claims_active(
    connection,
    peer_connection,
    campaign_id: str,
    streams,
    control: _OverlapControl,
) -> None:
    assert await asyncio.wait_for(
        asyncio.to_thread(control.reads_overlapped.wait, 3),
        timeout=4,
    )
    assert all(stream._opened_blob._descriptor >= 0 for stream in streams)
    await reader_test_support.assert_reader_claim_active(
        connection,
        campaign_id,
        reader_test_support.OVERLAP_RECIPE_IDS[0],
    )
    await reader_test_support.assert_reader_claim_active(
        peer_connection,
        campaign_id,
        reader_test_support.OVERLAP_RECIPE_IDS[1],
    )


async def _finish_overlapping_reads(
    read_tasks,
    artifact_bytes: bytes,
    control: _OverlapControl,
) -> None:
    control.release_reads.set()
    results = await asyncio.wait_for(asyncio.gather(*read_tasks), timeout=8)
    assert results == [artifact_bytes, artifact_bytes]
    assert len(control.thread_ids) == 2


async def _consume_blocked_artifact(
    connection,
    campaign_id: str,
    retained_item,
    artifact_bytes: bytes,
    stream_by_name: dict,
) -> None:
    async with claimed_retained_artifact_reader(
        connection,
        campaign_id=campaign_id,
        consumer_recipe_id=reader_test_support.READER_RECIPE_ID,
    ) as reader:
        byte_stream = await reader.iter_full(
            retained_item.source_item_id,
            chunk_bytes=len(artifact_bytes),
        )
        stream_by_name["stream"] = byte_stream
        await reader_test_support.collect_new_stream(byte_stream)


async def _cancel_twice_and_assert_pending(
    consumer_task: asyncio.Task,
    control: _CancellationControl,
) -> None:
    assert await asyncio.wait_for(
        asyncio.to_thread(control.read_started.wait, 3),
        timeout=4,
    )
    consumer_task.cancel("first-reader-cancellation")
    await asyncio.sleep(0)
    consumer_task.cancel("repeated-reader-cancellation")
    await asyncio.sleep(0)
    assert not consumer_task.done()


async def _finish_cancelled_consumer(
    peer_connection,
    campaign_id: str,
    consumer_task: asyncio.Task,
    byte_stream,
    control: _CancellationControl,
) -> None:
    await reader_test_support.assert_reader_claim_active(
        peer_connection,
        campaign_id,
        reader_test_support.READER_RECIPE_ID,
    )
    control.release_read.set()
    completed, pending = await asyncio.wait((consumer_task,), timeout=6)
    assert completed == {consumer_task}
    assert not pending
    with pytest.raises(asyncio.CancelledError) as cancellation:
        await consumer_task
    assert str(cancellation.value) == "first-reader-cancellation"
    assert byte_stream._opened_blob._descriptor == -1
    await reader_test_support.assert_reader_claim_released(
        peer_connection,
        campaign_id,
    )


@pytest.mark.asyncio
async def test_reader_streams_full_range_heartbeats_and_releases_exact_claim(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    """Stream full and range views without exposing private storage locators."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, artifact_bytes, _path = (
            await reader_test_support.sealed_reader_campaign(
                connection, retained_artifact_test_root, "reader-happy"
            )
        )
        async with claimed_retained_artifact_reader(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id=reader_test_support.READER_RECIPE_ID,
        ) as reader:
            await reader.heartbeat()
            public_names = {name for name in dir(reader) if not name.startswith("_")}
            assert not any(
                token in name.lower()
                for name in public_names
                for token in ("path", "locator", "descriptor", "fd")
            )
            assert "fixture://" not in repr(reader.campaign)
            full_stream = await reader.iter_full(
                retained_item.source_item_id, chunk_bytes=3
            )
            assert await reader_test_support.collect_new_stream(full_stream) == (
                artifact_bytes
            )
            range_stream = await reader.iter_range(
                retained_item.source_item_id, 0, chunk_bytes=2
            )
            assert await reader_test_support.collect_new_stream(range_stream) == (
                artifact_bytes
            )
        await reader_test_support.assert_reader_claim_released(connection, campaign_id)


@pytest.mark.asyncio
async def test_reader_streams_a_claimed_nonzero_offset_range(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    """Read the exact verified suffix declared by a nonzero range offset."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, artifact_bytes, _path = (
            await reader_test_support.sealed_reader_campaign(
                connection,
                retained_artifact_test_root,
                "reader-nonzero-range",
                raw_byte_start=5,
            )
        )
        async with claimed_retained_artifact_reader(
            connection,
            campaign_id=campaign_id,
            consumer_recipe_id=reader_test_support.READER_RECIPE_ID,
        ) as reader:
            byte_stream = await reader.iter_range(
                retained_item.source_item_id, 0, chunk_bytes=2
            )
            assert await reader_test_support.collect_new_stream(byte_stream) == (
                artifact_bytes[5:]
            )
        await reader_test_support.assert_reader_claim_released(connection, campaign_id)


@pytest.mark.asyncio
async def test_distinct_claimed_readers_overlap_physical_reads(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    """Prove same-campaign consumers overlap reads on independent connections."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, artifact_bytes, _path = (
            await reader_test_support.sealed_reader_campaign(
                connection, retained_artifact_test_root, "reader-overlap"
            )
        )
        control = _OverlapControl()
        _install_read_at_delegate(monkeypatch, control.read_at)
        async with retained_peer_connection() as peer_connection:
            assert peer_connection is not connection
            async with claimed_retained_artifact_reader(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id=reader_test_support.OVERLAP_RECIPE_IDS[0],
            ) as first_reader, claimed_retained_artifact_reader(
                peer_connection,
                campaign_id=campaign_id,
                consumer_recipe_id=reader_test_support.OVERLAP_RECIPE_IDS[1],
            ) as second_reader:
                first_stream, second_stream, tasks = await _start_overlapping_reads(
                    first_reader, second_reader, retained_item, artifact_bytes
                )
                await _assert_overlapping_claims_active(
                    connection,
                    peer_connection,
                    campaign_id,
                    (first_stream, second_stream),
                    control,
                )
                await _finish_overlapping_reads(tasks, artifact_bytes, control)
            for recipe_id, claim_connection in zip(
                reader_test_support.OVERLAP_RECIPE_IDS,
                (connection, peer_connection),
            ):
                await reader_test_support.assert_reader_claim_released(
                    claim_connection, campaign_id, recipe_id
                )


@pytest.mark.asyncio
async def test_cancellation_joins_inflight_read_before_abort_and_release(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    """Keep the claim and descriptor alive until a cancelled worker settles."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, artifact_bytes, _path = (
            await reader_test_support.sealed_reader_campaign(
                connection, retained_artifact_test_root, "reader-cancellation"
            )
        )
        control = _CancellationControl()
        _install_read_at_delegate(monkeypatch, control.blocked_failing_read_at)
        stream_by_name = {}
        consumer_task = asyncio.create_task(
            _consume_blocked_artifact(
                connection,
                campaign_id,
                retained_item,
                artifact_bytes,
                stream_by_name,
            )
        )
        try:
            await _cancel_twice_and_assert_pending(consumer_task, control)
            byte_stream = stream_by_name["stream"]
            assert byte_stream._opened_blob._descriptor >= 0
            async with retained_peer_connection() as peer_connection:
                await _finish_cancelled_consumer(
                    peer_connection, campaign_id, consumer_task, byte_stream, control
                )
        finally:
            control.release_read.set()
            if not consumer_task.done():
                consumer_task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await asyncio.wait_for(asyncio.shield(consumer_task), timeout=6)


@pytest.mark.asyncio
async def test_reader_propagates_private_read_failure_and_releases(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    """Propagate a private worker failure and release its exact claim."""

    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, artifact_bytes, _path = (
            await reader_test_support.sealed_reader_campaign(
                connection,
                retained_artifact_test_root,
                "reader-private-read-failure",
            )
        )

        def failing_read_at(_opened_blob, _byte_count: int, _offset: int):
            raise RetainedArtifactError("fixture_private_read_failure")

        monkeypatch.setattr(
            reader_module._OpenedArtifactBlob, "read_at", failing_read_at
        )
        with pytest.raises(RetainedArtifactError, match="fixture_private_read_failure"):
            async with claimed_retained_artifact_reader(
                connection,
                campaign_id=campaign_id,
                consumer_recipe_id=reader_test_support.READER_RECIPE_ID,
            ) as reader:
                byte_stream = await reader.iter_full(
                    retained_item.source_item_id,
                    chunk_bytes=len(artifact_bytes),
                )
                await reader_test_support.collect_new_stream(byte_stream)
        await reader_test_support.assert_reader_claim_released(connection, campaign_id)
