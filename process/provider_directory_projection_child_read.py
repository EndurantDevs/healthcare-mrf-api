# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public, locator-free child retained-read lease boundary."""

from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
import logging
from typing import Any, AsyncIterable, AsyncIterator

from db.connection import db
from process.provider_directory_projection_child_read_contract import (
    validated_child_read_lease,
)
from process.provider_directory_projection_child_read_store import (
    assert_verified_projection_child_read_lease,
    claim_projection_child_read_lease,
    heartbeat_projection_child_read_lease,
    release_projection_child_read_lease,
    verify_projection_child_read_lease,
)
from process.provider_directory_projection_types import (
    ProjectionRetainedChildLease,
    ProjectionShardClaim,
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


LOGGER = logging.getLogger(__name__)


async def _projection_retained_chunks(
    lease: ProjectionRetainedChildLease,
    opened_blob: Any,
    chunk_bytes: int,
) -> AsyncIterator[bytes]:
    offset = lease.raw_byte_start
    remaining = lease.expected_byte_count
    payload_digest = hashlib.sha256()
    verified = False
    try:
        while remaining:
            requested = min(chunk_bytes, remaining)
            chunk = await _read_blob_chunk(opened_blob, requested, offset)
            if len(chunk) != requested:
                raise RetainedArtifactError("retained_blob_truncated")
            offset += requested
            remaining -= requested
            payload_digest.update(chunk)
            yield chunk
        if payload_digest.hexdigest() != lease.expected_payload_sha256:
            raise RetainedArtifactError("retained_blob_digest_mismatch")
        opened_blob.verify_and_close(content_digest_verified=True)
        verified = True
    finally:
        if not verified:
            opened_blob.abort()


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
    stream = _projection_retained_chunks(lease, opened_blob, chunk_size)
    try:
        yield stream
    finally:
        try:
            await stream.aclose()
        finally:
            opened_blob.abort()


async def _release_child_after_failure(
    lease: ProjectionRetainedChildLease,
    *,
    database: Any,
    schema: str,
) -> None:
    """Best-effort release without replacing the triggering failure."""

    try:
        await release_projection_child_read_lease(
            lease,
            database=database,
            schema=schema,
        )
    except BaseException:
        LOGGER.exception("provider_directory_projection_child_cleanup_failed")


@asynccontextmanager
async def claimed_projection_child_read_lease(
    claim: ProjectionShardClaim,
    *,
    lease_seconds: int = 300,
    database: Any = db,
    schema: str = "mrf",
) -> AsyncIterator[ProjectionRetainedChildLease]:
    """Own one child; verification and shard completion occur before exit."""

    lease = await claim_projection_child_read_lease(
        claim,
        lease_seconds=lease_seconds,
        database=database,
        schema=schema,
    )
    try:
        yield validated_child_read_lease(lease)
    except BaseException:
        await _release_child_after_failure(
            lease,
            database=database,
            schema=schema,
        )
        raise
    else:
        try:
            await assert_verified_projection_child_read_lease(
                lease,
                database=database,
                schema=schema,
            )
        except BaseException:
            await _release_child_after_failure(
                lease,
                database=database,
                schema=schema,
            )
            raise
        await release_projection_child_read_lease(
            lease,
            database=database,
            schema=schema,
        )


__all__ = (
    "assert_verified_projection_child_read_lease",
    "claim_projection_child_read_lease",
    "claimed_projection_child_read_lease",
    "heartbeat_projection_child_read_lease",
    "projection_retained_child_stream",
    "release_projection_child_read_lease",
    "verify_projection_child_read_lease",
)
