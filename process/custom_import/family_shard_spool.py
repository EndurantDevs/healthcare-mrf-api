# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded storage blocks containing unchanged legacy spool bytes."""

import hashlib
import struct
from compression import zstd

MAGIC = b"CI-SPOOL\x00\x01"
BLOCK_BYTES = 65536
_HEADER = struct.Struct(">BII")
BLOCK_OVERHEAD = _HEADER.size
_END = 255
_FOOTER = struct.Struct(">Q32s")


def encoded_block(raw):
    """Compress one bounded block, retaining raw bytes when compression expands."""
    if type(raw) is not bytes or not 0 < len(raw) <= BLOCK_BYTES:
        raise ValueError("spool storage block length differs")
    compressed = zstd.compress(
        raw,
        options={zstd.CompressionParameter.compression_level: 3, zstd.CompressionParameter.checksum_flag: True},
    )
    kind, stored = (1, compressed) if len(compressed) < len(raw) else (0, raw)
    return _HEADER.pack(kind, len(raw), len(stored)) + stored


def footer(logical_bytes, logical_sha256):
    """Close the storage stream with its original logical length and digest."""
    return _HEADER.pack(_END, 0, 0) + _FOOTER.pack(logical_bytes, logical_sha256)


def _require_window(stored):
    """Bound the declared window even when a decoder can optimize a short frame."""
    if stored[4] & 24:
        raise ValueError("spool storage compressed header flags differ")
    if not stored[4] & 32:
        if len(stored) < 6:
            raise ValueError("spool storage compressed header is incomplete")
        base = 1 << (10 + (stored[5] >> 3))
        if base + (base >> 3) * (stored[5] & 7) > BLOCK_BYTES:
            raise ValueError("spool storage compressed window exceeds its bound")


def _decoded_block(kind, raw_size, stored):
    if kind == 0:
        if len(stored) != raw_size:
            raise ValueError("raw spool storage block length differs")
        return stored
    if kind != 1 or stored[:4] != b"\x28\xb5\x2f\xfd" or len(stored) < 5 or not stored[4] & 4:
        raise ValueError("spool storage block codec or checksum differs")
    _require_window(stored)
    try:
        info = zstd.get_frame_info(stored)
        if info.decompressed_size != raw_size or info.dictionary_id != 0:
            raise ValueError("spool storage compressed declaration differs")
        decoder = zstd.ZstdDecompressor(options={zstd.DecompressionParameter.window_log_max: 16})
        raw = decoder.decompress(stored, max_length=raw_size + 1)
    except zstd.ZstdError as error:
        raise ValueError("spool storage compressed block is malformed") from error
    if len(raw) != raw_size or not decoder.eof or decoder.unused_data:
        raise ValueError("spool storage compressed EOF or length differs")
    return raw


class BlockReader:
    """Expose exact logical bytes while holding at most one decoded block."""

    def __init__(self, source, *, check=None, maximum_logical_bytes=None):
        self.source, self.check = source, check
        self.maximum_logical_bytes = maximum_logical_bytes
        self.buffer, self.position, self.complete = b"", 0, False
        self.logical_bytes, self.digest = 0, hashlib.sha256()
        if source.read(len(MAGIC)) != MAGIC:
            raise ValueError("spool storage contract differs")

    def _next(self):
        if self.check is not None:
            self.check()
        prefix = self.source.read(_HEADER.size)
        if len(prefix) != _HEADER.size:
            raise ValueError("spool storage frame is incomplete")
        kind, raw_size, stored_size = _HEADER.unpack(prefix)
        if kind == _END:
            expected = self.source.read(_FOOTER.size)
            if raw_size or stored_size or len(expected) != _FOOTER.size or self.source.read(1):
                raise ValueError("spool storage EOF differs")
            if _FOOTER.unpack(expected) != (self.logical_bytes, self.digest.digest()):
                raise ValueError("spool storage logical commitment differs")
            self.complete = True
            return
        if not 0 < raw_size <= BLOCK_BYTES or not 0 < stored_size <= raw_size:
            raise ValueError("spool storage block exceeds its bounds")
        if self.maximum_logical_bytes is not None and self.logical_bytes + raw_size > self.maximum_logical_bytes:
            raise ValueError("spool storage logical byte budget differs")
        stored = self.source.read(stored_size)
        if len(stored) != stored_size:
            raise ValueError("spool storage block is incomplete")
        self.buffer = _decoded_block(kind, raw_size, stored)
        self.position = 0
        self.logical_bytes += len(self.buffer)
        self.digest.update(self.buffer)

    def read(self, size):
        """Read an explicitly bounded amount; callers retain their frame bounds."""
        if type(size) is not int or size < 0:
            raise ValueError("spool storage reads require an explicit bound")
        output, remaining = bytearray(size), size
        target = memoryview(output)
        while remaining:
            available = len(self.buffer) - self.position
            if not available:
                self.buffer, self.position = b"", 0
                if self.complete:
                    break
                self._next()
                continue
            count = min(remaining, available)
            target[size - remaining : size - remaining + count] = memoryview(self.buffer)[
                self.position : self.position + count
            ]
            self.position += count
            remaining -= count
        target.release()
        if remaining:
            del output[size - remaining :]
        return output


class BlockWriter:
    """Charge every stored byte before writing; buffers never exceed one block."""

    def __init__(self, target, charge):
        self.target, self.charge = target, charge
        self.buffer = bytearray()
        self.logical_bytes, self.digest = 0, hashlib.sha256()
        self.complete = False
        self._stored(MAGIC)

    def _stored(self, payload):
        self.charge(len(payload))
        self.target.write(payload)

    def write(self, payload):
        """Preserve the caller's exact framing while grouping bounded blocks."""
        if self.complete:
            raise ValueError("spool storage writer is already closed")
        self.logical_bytes += len(payload)
        self.digest.update(payload)
        remaining = memoryview(payload)
        while remaining:
            count = min(BLOCK_BYTES - len(self.buffer), len(remaining))
            self.buffer.extend(remaining[:count])
            remaining = remaining[count:]
            if len(self.buffer) == BLOCK_BYTES:
                self._stored(encoded_block(bytes(self.buffer)))
                self.buffer.clear()

    def finish(self):
        """Publish complete logical EOF only after all buffered bytes are written."""
        if self.complete:
            raise ValueError("spool storage writer is already closed")
        if self.buffer:
            self._stored(encoded_block(bytes(self.buffer)))
            self.buffer.clear()
        self._stored(footer(self.logical_bytes, self.digest.digest()))
        self.complete = True


def logical_reader(source, *, check=None, maximum_logical_bytes=None):
    """Read the explicitly versioned envelope or an unchanged legacy raw file."""
    prefix = source.read(len(MAGIC))
    source.seek(0)
    if prefix == MAGIC:
        return BlockReader(source, check=check, maximum_logical_bytes=maximum_logical_bytes)
    return source
