# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded zero-decode forwarding of native PostgreSQL binary COPY output."""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any

from process.provider_directory_projection_copy_summary import (
    NATIVE_COPY_MAGIC,
    NATIVE_COPY_STREAM_MAX_BYTES,
    NATIVE_COPY_SUMMARY_MAX_BYTES,
    NativeCopySummary,
    native_copy_projection_proof,
    validated_native_copy_summary,
)
from process.provider_directory_projection_json import invalid_native_spool
from process.provider_directory_projection_stage import (
    copy_projection_stage_binary_stream,
)
from process.provider_directory_projection_types import (
    ProjectionProofShard,
    ProjectionRetainedChildLease,
    ProjectionShardClaim,
    ProjectionStage,
)


_PGCOPY_HEADER = b"PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00"
_PGCOPY_TRAILER = b"\xff\xff"
_COPY_READ_CHUNK_BYTES = 64 * 1024


class _NativeCopySource:
    """Yield exactly one declared COPY stream while hashing forwarded bytes."""

    def __init__(self, stream: Any, byte_count: int) -> None:
        self._stream = stream
        self._remaining_byte_count = byte_count
        self._digest = hashlib.sha256()
        self._prefix_bytes = bytearray()
        self._suffix_bytes = bytearray()
        self.forwarded_byte_count = 0

    def __aiter__(self) -> "_NativeCopySource":
        return self

    async def __anext__(self) -> bytes:
        if self._remaining_byte_count == 0:
            raise StopAsyncIteration
        chunk = await self._stream.read(
            min(_COPY_READ_CHUNK_BYTES, self._remaining_byte_count)
        )
        if not chunk:
            raise invalid_native_spool()
        self._remaining_byte_count -= len(chunk)
        self.forwarded_byte_count += len(chunk)
        self._digest.update(chunk)
        if len(self._prefix_bytes) < len(_PGCOPY_HEADER):
            prefix_remaining = len(_PGCOPY_HEADER) - len(self._prefix_bytes)
            self._prefix_bytes.extend(chunk[:prefix_remaining])
        self._suffix_bytes.extend(chunk)
        if len(self._suffix_bytes) > len(_PGCOPY_TRAILER):
            del self._suffix_bytes[: -len(_PGCOPY_TRAILER)]
        return chunk

    def assert_complete(self, expected_sha256: str) -> None:
        """Require full driver consumption, envelope bytes, and exact digest."""

        if (
            self._remaining_byte_count != 0
            or bytes(self._prefix_bytes) != _PGCOPY_HEADER
            or bytes(self._suffix_bytes) != _PGCOPY_TRAILER
            or self._digest.hexdigest() != expected_sha256
        ):
            raise invalid_native_spool()


async def _read_exactly(stream: Any, byte_count: int) -> bytes:
    try:
        return await stream.readexactly(byte_count)
    except asyncio.IncompleteReadError as error:
        raise invalid_native_spool() from error


async def _native_copy_header(stream: Any) -> tuple[bytes, int]:
    magic = await _read_exactly(stream, len(NATIVE_COPY_MAGIC))
    summary_length = int.from_bytes(await _read_exactly(stream, 4), "big")
    if (
        magic != NATIVE_COPY_MAGIC
        or not 1 <= summary_length <= NATIVE_COPY_SUMMARY_MAX_BYTES
    ):
        raise invalid_native_spool()
    summary_payload = await _read_exactly(stream, summary_length)
    copy_byte_count = int.from_bytes(await _read_exactly(stream, 8), "big")
    if not 21 <= copy_byte_count <= NATIVE_COPY_STREAM_MAX_BYTES:
        raise invalid_native_spool()
    return summary_payload, copy_byte_count


async def _assert_native_copy_terminal(stream: Any) -> None:
    if await _read_exactly(stream, 1) != b"\xff" or await stream.read(1):
        raise invalid_native_spool()


def _assert_raw_input_census(
    summary: NativeCopySummary,
    raw_census: tuple[int, str],
) -> None:
    raw_byte_count, raw_sha256 = raw_census
    summary_map = summary.summary_map
    if (
        raw_byte_count != summary_map["input_byte_count"]
        or raw_sha256 != summary_map["input_sha256"]
    ):
        raise invalid_native_spool()


async def consume_native_copy_spool(
    stream: Any,
    *,
    claim: ProjectionShardClaim,
    child_lease: ProjectionRetainedChildLease,
    stage: ProjectionStage,
    transaction: Any,
    framing: str,
    raw_census_task: asyncio.Task[tuple[int, str]],
) -> tuple[ProjectionProofShard, dict[str, Any], int]:
    """Validate one native summary and forward only its bounded COPY bytes."""

    summary_payload, copy_byte_count = await _native_copy_header(stream)
    summary = validated_native_copy_summary(
        summary_payload,
        claim=claim,
        child_lease=child_lease,
        framing=framing,
        copy_byte_count=copy_byte_count,
    )
    copy_source = _NativeCopySource(stream, copy_byte_count)
    copied_record_count = await copy_projection_stage_binary_stream(
        stage,
        copy_source,
        transaction=transaction,
    )
    copy_source.assert_complete(summary.copy_sha256)
    await _assert_native_copy_terminal(stream)
    _assert_raw_input_census(summary, await raw_census_task)
    if copied_record_count != summary.resource_count:
        raise invalid_native_spool()
    shard_proof = native_copy_projection_proof(summary, claim)
    return shard_proof, dict(summary.summary_map), copied_record_count


__all__ = ("consume_native_copy_spool",)
