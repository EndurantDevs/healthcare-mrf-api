# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Claim-scoped streaming reader for retained Provider Directory artifacts."""

from __future__ import annotations

import asyncio
import hashlib
from contextlib import asynccontextmanager
from typing import AsyncIterator

import asyncpg

from process.provider_directory_retained_artifact_contract import (
    ArtifactLayoutRange,
    RetainedArtifactError,
    RetainedCampaignMismatch,
    SealedRetainedArtifact,
    SealedRetainedCampaign,
    require_digest,
    require_nonnegative_int,
    require_positive_int,
    require_safe_id,
)
from process.provider_directory_retained_blob_store import (
    _OpenedArtifactBlob,
    _open_retained_artifact_blob,
)
from process.provider_directory_retained_consumer_claim_store import (
    claim_sealed_retained_campaign,
    heartbeat_retained_campaign_consumer,
    release_retained_campaign_consumer,
)
from process.provider_directory_retained_reader_store import (
    assert_active_reader_binding,
    assert_active_reader_claim,
)


DEFAULT_READER_CHUNK_BYTES = 4 * 1024 * 1024
MAX_READER_CHUNK_BYTES = 64 * 1024 * 1024


def _validated_chunk_bytes(chunk_bytes: int) -> int:
    chunk_size = require_positive_int(chunk_bytes, "reader_chunk_bytes")
    if chunk_size > MAX_READER_CHUNK_BYTES:
        raise RetainedArtifactError("reader_chunk_bytes_invalid")
    return chunk_size


async def _settle_blob_read(
    read_task: asyncio.Task[bytes],
) -> tuple[bytes, BaseException | None]:
    try:
        return await read_task, None
    except BaseException as error:
        return b"", error


async def _join_settlement(
    settled: asyncio.Event,
) -> asyncio.CancelledError | None:
    """Wait through repeated cancellation and return the first signal."""

    if settled.is_set():
        return None
    waiter = asyncio.create_task(settled.wait())
    pending_cancellation: asyncio.CancelledError | None = None
    while not waiter.done():
        try:
            await asyncio.shield(waiter)
        except asyncio.CancelledError as error:
            if pending_cancellation is None:
                pending_cancellation = error
    return pending_cancellation


async def _read_blob_chunk(
    opened_blob: _OpenedArtifactBlob,
    requested_bytes: int,
    offset: int,
) -> bytes:
    """Join one private read before re-delivering caller cancellation."""

    read_task = asyncio.create_task(
        asyncio.to_thread(opened_blob.read_at, requested_bytes, offset)
    )
    settlement_task = asyncio.create_task(_settle_blob_read(read_task))
    pending_cancellation: asyncio.CancelledError | None = None
    while not settlement_task.done():
        try:
            await asyncio.shield(settlement_task)
        except asyncio.CancelledError as error:
            if pending_cancellation is None:
                pending_cancellation = error
    byte_chunk, read_error = settlement_task.result()
    if pending_cancellation is not None:
        raise pending_cancellation
    if read_error is not None:
        raise read_error
    return byte_chunk


class _RetainedArtifactByteStream:
    """Single-use asynchronous iterator over one verified byte interval."""

    def __init__(
        self,
        reader: "RetainedArtifactReader",
        artifact: SealedRetainedArtifact,
        layout_range: ArtifactLayoutRange | None,
        opened_blob: _OpenedArtifactBlob,
        chunk_bytes: int,
    ):
        self._reader = reader
        self._artifact = artifact
        self._layout_range = layout_range
        self._opened_blob = opened_blob
        self._chunk_bytes = chunk_bytes
        self._offset = 0 if layout_range is None else layout_range.raw_byte_start
        self._remaining = (
            artifact.artifact_byte_count
            if layout_range is None
            else layout_range.raw_byte_count
        )
        self._expected_sha256 = (
            artifact.artifact_sha256
            if layout_range is None
            else layout_range.raw_sha256
        )
        self._digest = hashlib.sha256()
        self._read_byte_count = 0
        self._iteration_started = False
        self._next_pending = False
        self._next_settled = asyncio.Event()
        self._next_settled.set()
        self._close_started = False
        self._complete = False

    def __aiter__(self) -> "_RetainedArtifactByteStream":
        if self._iteration_started:
            raise RetainedArtifactError("retained_reader_repeated_iteration")
        self._iteration_started = True
        return self

    async def __anext__(self) -> bytes:
        if not self._iteration_started:
            raise RetainedArtifactError("retained_reader_iteration_not_started")
        if self._complete:
            raise StopAsyncIteration
        if self._close_started:
            raise RetainedArtifactError("retained_reader_closed")
        if self._next_pending:
            raise RetainedArtifactError("retained_reader_concurrent_iteration")
        self._reader._begin_operation()
        self._next_pending = True
        self._next_settled.clear()
        try:
            return await self._next_verified_chunk()
        except BaseException:
            self._opened_blob.abort()
            self._reader._stream_failed(self)
            raise
        finally:
            self._next_pending = False
            self._next_settled.set()
            self._reader._end_operation()

    async def _next_verified_chunk(self) -> bytes:
        requested_bytes = min(self._chunk_bytes, self._remaining)
        byte_chunk = await _read_blob_chunk(
            self._opened_blob,
            requested_bytes,
            self._offset,
        )
        if len(byte_chunk) != requested_bytes:
            raise RetainedArtifactError("retained_blob_truncated")
        self._digest.update(byte_chunk)
        self._offset += len(byte_chunk)
        self._remaining -= len(byte_chunk)
        self._read_byte_count += len(byte_chunk)
        if self._remaining == 0:
            await self._finish_verified_read()
        return byte_chunk

    async def _finish_verified_read(self) -> None:
        if self._digest.hexdigest() != self._expected_sha256:
            raise RetainedArtifactError("retained_blob_digest_mismatch")
        self._opened_blob.verify_and_close()
        await self._reader._stream_completed(
            self,
            self._artifact,
            self._layout_range,
        )
        self._complete = True

    async def aclose(self) -> None:
        """Close this single-use stream, poisoning partial reads."""

        self._close_started = True
        pending_cancellation = await _join_settlement(self._next_settled)
        if not self._complete:
            self._opened_blob.abort()
            self._reader._stream_failed(self)
        if pending_cancellation is not None:
            raise pending_cancellation


class RetainedArtifactReader:
    """Public claim-scoped facade that exposes only verified bytes."""

    def __init__(
        self,
        connection: asyncpg.Connection,
        consumer_recipe_id: str,
        campaign: SealedRetainedCampaign,
    ):
        self._connection = connection
        self._consumer_recipe_id = consumer_recipe_id
        self._campaign = campaign
        self._artifacts_by_source_item = {
            artifact.source_item_id: artifact for artifact in campaign.artifacts
        }
        if len(self._artifacts_by_source_item) != len(campaign.artifacts):
            raise RetainedCampaignMismatch(
                "retained_reader_artifact_identity_duplicate"
            )
        self._active_stream: _RetainedArtifactByteStream | None = None
        self._stream_start_pending = False
        self._pending_operation_count = 0
        self._operations_settled = asyncio.Event()
        self._operations_settled.set()
        self._database_lock = asyncio.Lock()
        self._completed_read_count = 0
        self._failed = False
        self._closing = False
        self._closed = False

    @property
    def campaign(self) -> SealedRetainedCampaign:
        """Return the immutable, locator-free descriptor claimed on entry."""

        return self._campaign

    async def heartbeat(self) -> None:
        """Heartbeat this reader's exact claim generation."""

        self._begin_operation()
        try:
            async with self._database_lock:
                await heartbeat_retained_campaign_consumer(
                    self._connection,
                    campaign_id=self._campaign.campaign_id,
                    consumer_recipe_id=self._consumer_recipe_id,
                    claimed_campaign_sha256=self._campaign.campaign_sha256,
                    consumer_claim_generation=self._campaign.consumer_claim_generation,
                )
        finally:
            self._end_operation()

    async def iter_full(
        self,
        source_item_id: str,
        *,
        chunk_bytes: int = DEFAULT_READER_CHUNK_BYTES,
    ) -> AsyncIterator[bytes]:
        """Open one full artifact as a single-use verified byte iterator."""

        artifact = self._artifact_for_source_item(source_item_id)
        return await self._prepare_stream(artifact, None, chunk_bytes)

    async def iter_range(
        self,
        source_item_id: str,
        range_ordinal: int,
        *,
        chunk_bytes: int = DEFAULT_READER_CHUNK_BYTES,
    ) -> AsyncIterator[bytes]:
        """Open one admitted layout range as a verified byte iterator."""

        artifact = self._artifact_for_source_item(source_item_id)
        layout_range = self._range_for_ordinal(artifact, range_ordinal)
        return await self._prepare_stream(artifact, layout_range, chunk_bytes)

    def _artifact_for_source_item(self, source_item_id: str) -> SealedRetainedArtifact:
        item_digest = require_digest(source_item_id, "source_item_id")
        artifact = self._artifacts_by_source_item.get(item_digest)
        if artifact is None:
            raise RetainedArtifactError("retained_reader_artifact_not_claimed")
        return artifact

    @staticmethod
    def _range_for_ordinal(
        artifact: SealedRetainedArtifact,
        range_ordinal: int,
    ) -> ArtifactLayoutRange:
        ordinal = require_nonnegative_int(range_ordinal, "range_ordinal")
        if ordinal >= len(artifact.ranges):
            raise RetainedArtifactError("retained_reader_range_not_claimed")
        layout_range = artifact.ranges[ordinal]
        if layout_range.range_ordinal != ordinal:
            raise RetainedCampaignMismatch("retained_reader_range_identity_mismatch")
        return layout_range

    async def _prepare_stream(
        self,
        artifact: SealedRetainedArtifact,
        layout_range: ArtifactLayoutRange | None,
        chunk_bytes: int,
    ) -> _RetainedArtifactByteStream:
        chunk_size = _validated_chunk_bytes(chunk_bytes)
        self._require_idle_reader()
        self._begin_operation()
        self._stream_start_pending = True
        try:
            await self._assert_binding(artifact, layout_range)
            opened_blob = _open_retained_artifact_blob(
                artifact.artifact_sha256,
                artifact.artifact_byte_count,
            )
            stream = _RetainedArtifactByteStream(
                self,
                artifact,
                layout_range,
                opened_blob,
                chunk_size,
            )
            self._active_stream = stream
            return stream
        except BaseException:
            self._failed = True
            raise
        finally:
            self._stream_start_pending = False
            self._end_operation()

    async def _assert_binding(
        self,
        artifact: SealedRetainedArtifact,
        layout_range: ArtifactLayoutRange | None,
    ) -> None:
        async with self._database_lock:
            await assert_active_reader_binding(
                self._connection,
                self._campaign,
                self._consumer_recipe_id,
                artifact,
                layout_range,
            )

    async def _stream_completed(
        self,
        stream: _RetainedArtifactByteStream,
        artifact: SealedRetainedArtifact,
        layout_range: ArtifactLayoutRange | None,
    ) -> None:
        if self._active_stream is not stream:
            raise RetainedCampaignMismatch("retained_reader_stream_identity_mismatch")
        await self._assert_binding(artifact, layout_range)
        self._active_stream = None
        self._completed_read_count += 1

    def _stream_failed(self, stream: _RetainedArtifactByteStream) -> None:
        if self._active_stream is stream:
            self._active_stream = None
        self._failed = True

    def _require_open_reader(self) -> None:
        if self._closing or self._closed:
            raise RetainedArtifactError("retained_reader_closed")
        if self._failed:
            raise RetainedArtifactError("retained_reader_failed")

    def _require_idle_reader(self) -> None:
        self._require_open_reader()
        if self._stream_start_pending or self._active_stream is not None:
            raise RetainedArtifactError("retained_reader_concurrent_iteration")

    def _begin_operation(self) -> None:
        self._require_open_reader()
        self._pending_operation_count += 1
        self._operations_settled.clear()

    def _end_operation(self) -> None:
        self._pending_operation_count -= 1
        if self._pending_operation_count == 0:
            self._operations_settled.set()

    async def _close_normally(self) -> None:
        self._closing = True
        pending_cancellation = await _join_settlement(self._operations_settled)
        if self._active_stream is not None:
            try:
                await self._active_stream.aclose()
            except asyncio.CancelledError as error:
                if pending_cancellation is None:
                    pending_cancellation = error
        if pending_cancellation is not None:
            self._closed = True
            raise pending_cancellation
        if self._failed:
            raise RetainedArtifactError("retained_reader_incomplete_read")
        if self._completed_read_count == 0:
            raise RetainedArtifactError("retained_reader_verified_read_required")
        async with self._database_lock:
            await assert_active_reader_claim(
                self._connection,
                self._campaign,
                self._consumer_recipe_id,
            )
        self._closed = True

    async def _abort_for_consumer_exception(self) -> None:
        self._closing = True
        await _join_settlement(self._operations_settled)
        if self._active_stream is not None:
            try:
                await self._active_stream.aclose()
            except asyncio.CancelledError:
                self._closed = True
                return
        self._closed = True


async def _release_reader_claim(reader: RetainedArtifactReader) -> None:
    campaign = reader.campaign
    await release_retained_campaign_consumer(
        reader._connection,
        campaign_id=campaign.campaign_id,
        consumer_recipe_id=reader._consumer_recipe_id,
        claimed_campaign_sha256=campaign.campaign_sha256,
        consumer_claim_generation=campaign.consumer_claim_generation,
    )


async def _cleanup_reader_after_error(
    reader: RetainedArtifactReader,
    *,
    should_abort: bool,
) -> None:
    try:
        if should_abort:
            await reader._abort_for_consumer_exception()
        await _release_reader_claim(reader)
    except BaseException:
        return


@asynccontextmanager
async def claimed_retained_artifact_reader(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    consumer_recipe_id: str,
) -> AsyncIterator[RetainedArtifactReader]:
    """Claim, stream, heartbeat, revalidate, and release one campaign."""

    require_digest(campaign_id, "campaign_id")
    recipe_id = require_safe_id(consumer_recipe_id, "consumer_recipe_id")
    claimed_campaign = await claim_sealed_retained_campaign(
        connection,
        campaign_id=campaign_id,
        consumer_recipe_id=recipe_id,
    )
    reader = RetainedArtifactReader(connection, recipe_id, claimed_campaign)
    try:
        yield reader
    except BaseException:
        await _cleanup_reader_after_error(reader, should_abort=True)
        raise
    try:
        await reader._close_normally()
    except BaseException:
        await _cleanup_reader_after_error(reader, should_abort=False)
        raise
    await _release_reader_claim(reader)


__all__ = (
    "DEFAULT_READER_CHUNK_BYTES",
    "MAX_READER_CHUNK_BYTES",
    "RetainedArtifactReader",
    "claimed_retained_artifact_reader",
)
