# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from dataclasses import replace
import hashlib
import threading
from pathlib import Path

import pytest

from process import provider_directory_projection_child_read as child_read
from process import provider_directory_projection_materializer as materializer
from process.provider_directory_retained_artifact_contract import (
    RetainedArtifactError,
)
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)
from tests.provider_directory_projection_lifecycle_support import (
    Database,
    LifecycleFakes,
    native_result,
)
from tests.provider_directory_retained_reader_support import (
    write_retained_artifact_blob,
)


pytest_plugins = ("tests.provider_directory_retained_reader_fixtures",)


def _child_for_bytes(payload: bytes, *, artifact_bytes: bytes | None = None):
    artifact = payload if artifact_bytes is None else artifact_bytes
    lease = synthetic_projection_context("ndjson").child_lease
    return replace(
        lease,
        retained_artifact_sha256=hashlib.sha256(artifact).hexdigest(),
        artifact_byte_count=len(artifact),
        raw_byte_start=artifact.index(payload),
        expected_byte_count=len(payload),
        expected_payload_sha256=hashlib.sha256(payload).hexdigest(),
        retained_range_ordinal=None if artifact is payload else 0,
    )


async def _read_child(lease, *, chunk_bytes: int = 4) -> bytes:
    async with child_read.projection_retained_child_stream(
        lease,
        chunk_bytes=chunk_bytes,
    ) as chunks:
        return b"".join([chunk async for chunk in chunks])


@pytest.mark.asyncio
async def test_projection_stream_reads_verified_full_artifact(
    retained_artifact_test_root: Path,
) -> None:
    payload = b'{"resourceType":"Practitioner","id":"synthetic"}\n'
    write_retained_artifact_blob(retained_artifact_test_root, payload)

    assert await _read_child(_child_for_bytes(payload), chunk_bytes=7) == payload


@pytest.mark.asyncio
async def test_projection_stream_reads_only_verified_layout_range(
    retained_artifact_test_root: Path,
) -> None:
    payload = b'{"resourceType":"Organization","id":"synthetic"}\n'
    artifact = b"ignored-prefix\n" + payload + b"ignored-suffix\n"
    write_retained_artifact_blob(retained_artifact_test_root, artifact)

    assert await _read_child(
        _child_for_bytes(payload, artifact_bytes=artifact),
        chunk_bytes=5,
    ) == payload


@pytest.mark.asyncio
async def test_projection_stream_rejects_tampered_retained_bytes(
    retained_artifact_test_root: Path,
) -> None:
    payload = b'{"resourceType":"Location","id":"synthetic"}\n'
    _artifact_sha256, blob_path = write_retained_artifact_blob(
        retained_artifact_test_root,
        payload,
    )
    blob_path.write_bytes(b"X" * len(payload))

    with pytest.raises(RetainedArtifactError, match="digest_mismatch"):
        await _read_child(_child_for_bytes(payload))


@pytest.mark.asyncio
async def test_projection_stream_joins_cancelled_read_before_abort(monkeypatch) -> None:
    payload = b'{"resourceType":"Endpoint","id":"synthetic"}\n'

    class BlockingBlob:
        def __init__(self) -> None:
            self.started = threading.Event()
            self.release = threading.Event()
            self.aborted = False

        def read_at(self, requested: int, offset: int) -> bytes:
            self.started.set()
            assert self.release.wait(timeout=5)
            return payload[offset : offset + requested]

        def verify_and_close(self, *, content_digest_verified: bool) -> None:
            raise AssertionError(content_digest_verified)

        def abort(self) -> None:
            self.aborted = True

    opened_blob = BlockingBlob()
    monkeypatch.setattr(
        child_read,
        "_open_retained_artifact_blob",
        lambda *_args: opened_blob,
    )

    async with child_read.projection_retained_child_stream(
        _child_for_bytes(payload),
        chunk_bytes=len(payload),
    ) as chunks:
        pending_read = asyncio.create_task(anext(chunks))
        assert await asyncio.to_thread(opened_blob.started.wait, 2)
        pending_read.cancel()
        opened_blob.release.set()
        with pytest.raises(asyncio.CancelledError):
            await pending_read

    assert opened_blob.aborted is True


@pytest.mark.asyncio
async def test_projection_stream_aborts_when_consumer_never_starts(monkeypatch) -> None:
    payload = b'{"resourceType":"Endpoint","id":"unused"}\n'

    class UnusedBlob:
        aborted = False

        def abort(self) -> None:
            self.aborted = True

    opened_blob = UnusedBlob()
    monkeypatch.setattr(
        child_read,
        "_open_retained_artifact_blob",
        lambda *_args: opened_blob,
    )

    async with child_read.projection_retained_child_stream(
        _child_for_bytes(payload)
    ):
        pass

    assert opened_blob.aborted is True


@pytest.mark.asyncio
async def test_projection_stream_rejects_short_retained_read(monkeypatch) -> None:
    payload = b'{"resourceType":"Endpoint","id":"short"}\n'

    class ShortBlob:
        aborted = False

        def read_at(self, requested: int, _offset: int) -> bytes:
            return payload[: requested - 1]

        def abort(self) -> None:
            self.aborted = True

    opened_blob = ShortBlob()
    monkeypatch.setattr(
        child_read,
        "_open_retained_artifact_blob",
        lambda *_args: opened_blob,
    )

    with pytest.raises(RetainedArtifactError, match="truncated"):
        await _read_child(_child_for_bytes(payload), chunk_bytes=len(payload))

    assert opened_blob.aborted is True


@pytest.mark.asyncio
async def test_materializer_reads_retained_bytes_without_injected_stream(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    context = synthetic_projection_context("ndjson")
    artifact_sha256, _blob_path = write_retained_artifact_blob(
        retained_artifact_test_root,
        context.fixture.encoded,
    )
    child = replace(
        context.child_lease,
        retained_artifact_sha256=artifact_sha256,
        artifact_byte_count=len(context.fixture.encoded),
        expected_byte_count=len(context.fixture.encoded),
        expected_payload_sha256=artifact_sha256,
    )
    lifecycle_events: list[tuple] = []
    _unused_stream, framing_resolver = LifecycleFakes(
        (child,),
        lifecycle_events,
    ).install(monkeypatch)
    database = Database(lifecycle_events)

    async def consume_retained_bytes(
        _claim,
        claimed_child,
        _stage,
        byte_stream,
        **_options,
    ):
        assert b"".join([chunk async for chunk in byte_stream]) == (
            context.fixture.encoded
        )
        return replace(
            native_result(claimed_child, "retained-default"),
            native_thread_count=1,
            materializer_worker_count=1,
        )

    (proof,) = await materializer.materialize_projection_shards(
        child.shard_claim.recipe_lease,
        child.shard_claim.admission_id,
        native_runner=consume_retained_bytes,
        framing_resolver=framing_resolver,
        database=database,
        enabled=True,
        max_workers=1,
        native_threads=1,
        heartbeat_seconds=10,
    )

    assert proof.partition_id == child.shard_claim.shard.partition_id
