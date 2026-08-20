# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained-storage adapter for the source-neutral projection core."""

from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from typing import Any, AsyncIterable, AsyncIterator

from process.provider_directory_projection_child_read_contract import (
    validated_child_read_lease,
)
from process.provider_directory_projection_materializer import (
    materialize_projection_shards,
)
from process.provider_directory_projection_types import (
    ProjectionLease,
    ProjectionProofShard,
    ProjectionRetainedChildLease,
)
from process.provider_directory_retained_artifact_contract import (
    RetainedArtifactError,
)
from process.provider_directory_retained_blob_store import (
    _open_retained_artifact_blob,
)
from process.provider_directory_retained_reader import (
    DEFAULT_READER_CHUNK_BYTES,
    _read_blob_chunk,
    _validated_chunk_bytes,
)


@asynccontextmanager
async def projection_retained_child_stream(
    lease: ProjectionRetainedChildLease,
    *,
    chunk_bytes: int = DEFAULT_READER_CHUNK_BYTES,
) -> AsyncIterator[AsyncIterable[bytes]]:
    """Open the exact retained bytes fenced by one projection child lease."""

    lease = validated_child_read_lease(lease)
    chunk_size = _validated_chunk_bytes(chunk_bytes)
    opened_blob = _open_retained_artifact_blob(
        lease.retained_artifact_sha256,
        lease.artifact_byte_count,
    )
    stream_state_by_field = {"is_verified": False, "is_normal_exit": False}

    async def _verified_chunks() -> AsyncIterator[bytes]:
        offset = lease.raw_byte_start
        remaining = lease.expected_byte_count
        payload_digest = hashlib.sha256()
        try:
            while remaining:
                requested = min(chunk_size, remaining)
                chunk = await _read_blob_chunk(opened_blob, requested, offset)
                if len(chunk) != requested:
                    raise RetainedArtifactError("retained_blob_truncated")
                offset += requested
                remaining -= requested
                payload_digest.update(chunk)
                if remaining == 0:
                    if payload_digest.hexdigest() != lease.expected_payload_sha256:
                        raise RetainedArtifactError("retained_blob_digest_mismatch")
                    opened_blob.verify_and_close(content_digest_verified=True)
                    stream_state_by_field["is_verified"] = True
                yield chunk
        finally:
            if not stream_state_by_field["is_verified"]:
                opened_blob.abort()

    stream = _verified_chunks()
    try:
        yield stream
        stream_state_by_field["is_normal_exit"] = True
    finally:
        try:
            await stream.aclose()
        finally:
            opened_blob.abort()
        if (
            stream_state_by_field["is_normal_exit"]
            and not stream_state_by_field["is_verified"]
        ):
            raise RetainedArtifactError("retained_reader_incomplete_read")


async def materialize_retained_projection_shards(
    lease: ProjectionLease,
    admission_id: str,
    **materializer_options_map: Any,
) -> tuple[ProjectionProofShard, ...]:
    """Materialize retained shards through the shared projection core."""

    return await materialize_projection_shards(
        lease,
        admission_id,
        child_stream_factory=projection_retained_child_stream,
        **materializer_options_map,
    )


__all__ = (
    "materialize_retained_projection_shards",
    "projection_retained_child_stream",
)
