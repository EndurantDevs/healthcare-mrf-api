# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact legacy bytes, bounded compression, and complete storage EOF."""

import hashlib
import random
import struct
from collections import deque
from compression import zstd
from io import BytesIO

import pytest

from process.custom_import import family_shard_spool as spool


def _encode(payload):
    target, charges = BytesIO(), []
    writer = spool.BlockWriter(target, charges.append)
    for start in range(0, len(payload), 137):
        writer.write(payload[start : start + 137])
        assert len(writer.buffer) < spool.BLOCK_BYTES
    writer.finish()
    stored = target.getvalue()
    assert sum(charges) == len(stored)
    assert writer.logical_bytes == len(payload)
    assert writer.digest.digest() == hashlib.sha256(payload).digest()
    return stored


@pytest.mark.parametrize("kind", ("empty", "repeated", "entropy", "boundary"))
def test_storage_preserves_exact_bytes_and_hashes_across_bounded_reads(kind):
    payload_by_kind = {
        "empty": b"",
        "repeated": (b"\x00\x00\x00\x00\x00\x00\x00\x07legacy\x00" * 16000),
        "entropy": random.Random(72941).randbytes(3 * spool.BLOCK_BYTES + 1),
        "boundary": bytes(range(256)) * 256,
    }
    payload = payload_by_kind[kind]
    stored = _encode(payload)
    reader = spool.BlockReader(BytesIO(stored), maximum_logical_bytes=len(payload))
    actual = b"".join(iter(lambda: reader.read(211), b""))
    assert actual == payload and reader.complete
    assert reader.digest.digest() == hashlib.sha256(payload).digest()
    assert reader.logical_bytes == len(payload)
    if kind == "repeated":
        assert len(stored) < len(payload) // 10
    elif kind == "entropy":
        assert len(stored) > len(payload)


@pytest.mark.parametrize("damage", ("prefix", "truncate", "suffix", "digest", "size", "logical_bound"))
def test_storage_refuses_corrupt_or_incomplete_commitments(damage):
    payload = b"bounded original bytes" * 7000
    stored = bytearray(_encode(payload))
    maximum = len(payload)
    match damage:
        case "prefix":
            stored[0] ^= 1
        case "truncate":
            del stored[-1]
        case "suffix":
            stored += b"uncommitted"
        case "digest":
            stored[-1] ^= 1
        case "size":
            stored[len(spool.MAGIC) + 1 : len(spool.MAGIC) + 5] = struct.pack(">I", spool.BLOCK_BYTES + 1)
        case "logical_bound":
            maximum -= 1
    with pytest.raises(ValueError):
        reader = spool.BlockReader(BytesIO(stored), maximum_logical_bytes=maximum)
        deque(iter(lambda: reader.read(4096), b""), maxlen=0)


@pytest.mark.parametrize("damage", ("checksum", "concatenated", "declared_size", "window"))
def test_compressed_blocks_require_exact_checksum_window_and_single_eof(damage):
    payload = b"x" * 4096
    frame = zstd.compress(payload, options={zstd.CompressionParameter.checksum_flag: damage != "checksum"})
    raw_size = len(payload)
    if damage == "concatenated":
        frame += frame
    elif damage == "declared_size":
        raw_size -= 1
    elif damage == "window":
        # Retain the two-byte content size, but declare a one-MiB streaming window.
        assert frame[4] & 32 and frame[4] >> 6 == 1
        frame = frame[:4] + bytes((frame[4] & ~32, 80)) + frame[5:]
    stored = spool.MAGIC + struct.pack(">BII", 1, raw_size, len(frame)) + frame
    stored += spool.footer(raw_size, hashlib.sha256(payload).digest())
    with pytest.raises(ValueError):
        reader = spool.BlockReader(BytesIO(stored))
        deque(iter(lambda: reader.read(4096), b""), maxlen=0)


def test_storage_charges_before_write_and_preserves_cancellation():
    target = BytesIO()

    def refuse(_count):
        raise RuntimeError("synthetic reservation failure")

    with pytest.raises(RuntimeError, match="reservation"):
        spool.BlockWriter(target, refuse)
    assert target.getvalue() == b""
    stored = _encode(b"x" * 100000)
    reader = spool.BlockReader(BytesIO(stored), check=lambda: refuse(0))
    with pytest.raises(RuntimeError, match="reservation"):
        reader.read(1)
    assert reader.logical_bytes == 0


def test_legacy_raw_stream_is_returned_without_reencoding():
    original = BytesIO(struct.pack(">Q", 6) + b"legacy")
    assert spool.logical_reader(original) is original
    assert original.read() == struct.pack(">Q", 6) + b"legacy"
