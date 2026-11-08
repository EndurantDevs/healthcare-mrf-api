# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact chunk commitments and bounded scratch lifetime for native row fingerprints."""

import asyncio
import errno
import hashlib
import struct
from pathlib import Path

import pytest

from process import network_bootstrap_sources as bootstrap
from process import provider_directory_proof_store as proof


class Fingerprints:
    def __init__(self, values):
        self.values = values
        self.closed = False

    def cursor(self, query, **options):
        async def rows():
            try:
                for value in self.values:
                    yield {"fingerprint": value}
            finally:
                self.closed = True

        return rows()


def reference(values):
    ordered = sorted(value.hex().encode("ascii") for value in values)
    digest = hashlib.sha256(b"entity-address-row-chunks/v1\0")
    for offset in range(0, len(ordered), 4096):
        chunk = ordered[offset : offset + 4096]
        digest.update(struct.pack(">I", len(chunk)))
        digest.update(hashlib.sha256(b"".join(chunk)).digest())
    return len(ordered), digest.hexdigest()


@pytest.fixture
def scratch_paths(monkeypatch):
    paths, create = [], bootstrap.tempfile.TemporaryDirectory

    def observe(*arguments, **options):
        directory = create(*arguments, **options)
        paths.append(Path(directory.name))
        return directory

    monkeypatch.setattr(bootstrap.tempfile, "TemporaryDirectory", observe)
    return paths


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 1, 4095, 4096, 4097, 65537])
async def test_native_content_preserves_global_order_duplicates_and_chunk_boundaries(count, scratch_paths):
    values = [hashlib.sha256(str(index % 5003).encode()).digest() for index in range(count)]
    connection = Fingerprints(list(reversed(values)))
    assert await bootstrap._content(connection, "synthetic", "rows") == reference(values)
    assert connection.closed
    assert scratch_paths and all(not path.exists() for path in scratch_paths)


@pytest.mark.asyncio
async def test_native_content_merge_has_bounded_fan_in(monkeypatch, scratch_paths):
    monkeypatch.setattr(proof, "_SPOOL_ROWS", 7)
    monkeypatch.setattr(proof, "_MERGE_FAN_IN", 2)
    values = [hashlib.sha256(str(index).encode()).digest() for index in range(100)]
    assert await bootstrap._content(Fingerprints(values), "synthetic", "rows") == reference(values)
    assert all(not path.exists() for path in scratch_paths)


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, b"short", "0" * 64])
async def test_native_content_refuses_invalid_fingerprints(value, scratch_paths):
    with pytest.raises(bootstrap.NetworkBootstrapSourceError, match="accounting"):
        await bootstrap._content(Fingerprints([value]), "synthetic", "rows")
    assert all(not path.exists() for path in scratch_paths)


@pytest.mark.asyncio
async def test_native_content_scratch_failure_propagates_and_cleans(monkeypatch, scratch_paths):
    def fail(self):
        with (self.directory / "partial.ndjson").open("wb") as output:
            output.write(b"0" * 64)
            raise OSError(errno.ENOSPC, "synthetic storage full")

    monkeypatch.setattr(proof._RecordSpool, "flush", fail)
    with pytest.raises(OSError, match="synthetic storage full") as refused:
        await bootstrap._content(Fingerprints([bytes(32)]), "synthetic", "rows")
    assert refused.value.errno == errno.ENOSPC
    assert all(not path.exists() for path in scratch_paths)


@pytest.mark.asyncio
async def test_native_content_cancelled_scan_cleans_partial_runs(monkeypatch, scratch_paths):
    monkeypatch.setattr(proof, "_SPOOL_ROWS", 2)
    scanning, closed = asyncio.Event(), []

    class WaitingFingerprints:
        def cursor(self, query, **options):
            async def rows():
                try:
                    for _ in range(10):
                        yield {"fingerprint": bytes(32)}
                    scanning.set()
                    await asyncio.Event().wait()
                finally:
                    closed.append(True)

            return rows()

    task = asyncio.create_task(bootstrap._content(WaitingFingerprints(), "synthetic", "rows"))
    await asyncio.wait_for(scanning.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed and all(not path.exists() for path in scratch_paths)


@pytest.mark.asyncio
async def test_native_content_cancelled_merge_closes_runs_and_cleans(monkeypatch, scratch_paths):
    monkeypatch.setattr(proof, "_SPOOL_ROWS", 2)
    monkeypatch.setattr(proof, "_MERGE_FAN_IN", 2)
    monkeypatch.setattr(proof, "_PROOF_CHECKPOINT_ROWS", 1)
    merging, closed = asyncio.Event(), []
    original = proof._RecordSpool._merge_path_steps

    def observe(self, *arguments):
        steps = original(self, *arguments)
        try:
            for value in steps:
                merging.set()
                yield value
        finally:
            steps.close()
            closed.append(True)

    monkeypatch.setattr(proof._RecordSpool, "_merge_path_steps", observe)
    task = asyncio.create_task(bootstrap._content(Fingerprints([bytes(32)] * 100), "synthetic", "rows"))
    await asyncio.wait_for(merging.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed and all(not path.exists() for path in scratch_paths)
