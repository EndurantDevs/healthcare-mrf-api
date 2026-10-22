# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded original-capture framing; the registrar still verifies actual bytes."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import AsyncIterator
from dataclasses import asdict, fields

from process.custom_import._source_text import validate_snapshot_token
from process.custom_import.admission_authorization import _object
from process.custom_import.capture import CaptureManifest, SealedCapture, _source_stream_sha256
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import (
    _COMPRESSIONS,
    _FORMATS,
    _IDENTIFIER,
    MAX_CHILD_COLLECTIONS,
    CustomImportDefinition,
    SourceStream,
)
from process.custom_import.raw_capture_store import _check_manifest_sizes, _checked_limits

MAGIC = b"CICAP1\0\0"
HEADER_MAX = 65_536
_CONTENT_DOMAIN = b"custom-import/capture-content/v1\0"
_MANIFEST_FIELDS = frozenset(item.name for item in fields(CaptureManifest))
_DIGEST_FIELDS = ("stream_sha256", "compressed_sha256", "decoded_sha256", "capture_sha256")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_DEFAULT_LIMITS = CaptureLimits()


class CaptureWireError(ValueError):
    """A redacted capture frame or declared-content identity is invalid."""


def _invalid():
    return CaptureWireError("custom_import_capture_wire_invalid")


def _canonical(document):
    return json.dumps(document, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")).encode(
        "ascii"
    )


def _bounded_sequence(values):
    if type(values) not in {tuple, list} or not 1 <= len(values) <= MAX_CHILD_COLLECTIONS + 1:
        raise _invalid()
    return values


def _identifier(value):
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise _invalid()
    return value


def _digest(value):
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise _invalid()
    return value


def _snapshot(value):
    if type(value) is not str:
        raise _invalid()
    try:
        return validate_snapshot_token(value)
    except ValueError:
        raise _invalid() from None


def _manifests(values, limits):
    manifests = tuple(_bounded_sequence(values))
    for manifest in manifests:
        if type(manifest) is not CaptureManifest:
            raise _invalid()
        _identifier(manifest.stream_id)
        _snapshot(manifest.source_snapshot_token)
        if type(manifest.compressed_bytes) is not int or type(manifest.decoded_bytes) is not int:
            raise _invalid()
        if (
            type(manifest.format) is not str
            or manifest.format not in _FORMATS
            or type(manifest.compression) is not str
            or manifest.compression not in _COMPRESSIONS
        ):
            raise _invalid()
        for name in _DIGEST_FIELDS:
            _digest(getattr(manifest, name))
    if (
        len({manifest.stream_id for manifest in manifests}) != len(manifests)
        or len({manifest.source_snapshot_token for manifest in manifests}) != 1
    ):
        raise _invalid()
    try:
        _check_manifest_sizes(manifests, limits)
    except ValueError:
        raise _invalid() from None
    return manifests


def _limits(limits):
    try:
        return _checked_limits(limits)
    except TypeError, ValueError:
        raise _invalid() from None


async def iter_capture_body(captures, *, limits: CaptureLimits) -> AsyncIterator[bytes]:
    """Yield a fully bounded header and original payload slices, never a joined bundle."""
    limits = _limits(limits)
    captures = tuple(_bounded_sequence(captures))
    if any(type(capture) is not SealedCapture or type(capture.payload) is not bytes for capture in captures):
        raise _invalid()
    manifests = _manifests(tuple(capture.manifest for capture in captures), limits)
    if any(len(capture.payload) != manifest.compressed_bytes for capture, manifest in zip(captures, manifests)):
        raise _invalid()
    header = _canonical({"captures": [asdict(manifest) for manifest in manifests]})
    if len(header) > HEADER_MAX:
        raise _invalid()
    for segment in (MAGIC + len(header).to_bytes(4, "big"), header):
        for offset in range(0, len(segment), limits.read_chunk_bytes):
            yield segment[offset : offset + limits.read_chunk_bytes]
    for capture in captures:
        for offset in range(0, len(capture.payload), limits.read_chunk_bytes):
            yield capture.payload[offset : offset + limits.read_chunk_bytes]


class _Reader:
    """Keep one transport fragment and one declared-size destination at a time."""

    def __init__(self, chunks, maximum):
        try:
            self.chunks = aiter(chunks)
        except TypeError:
            raise _invalid() from None
        self.maximum = maximum
        self.received = 0
        self.pending = memoryview(b"")

    async def _has_bytes(self):
        if self.pending:
            return True
        try:
            chunk = await anext(self.chunks)
        except StopAsyncIteration:
            return False
        if type(chunk) is not bytes or not chunk:
            raise _invalid()
        self.received += len(chunk)
        if self.received > self.maximum:
            raise _invalid()
        self.pending = memoryview(chunk)
        return True

    async def _exact(self, size):
        destination = bytearray(size)
        offset = 0
        while offset < size:
            if not await self._has_bytes():
                raise _invalid()
            count = min(size - offset, len(self.pending))
            destination[offset : offset + count] = self.pending[:count]
            self.pending = self.pending[count:] if count < len(self.pending) else memoryview(b"")
            offset += count
        return bytes(destination)


def _header_manifests(raw, limits):
    try:
        document = _object(raw, HEADER_MAX)
    except ValueError:
        raise _invalid() from None
    if set(document) != {"captures"} or type(document["captures"]) is not list:
        raise _invalid()
    entries = _bounded_sequence(document["captures"])
    if any(type(entry) is not dict or set(entry) != _MANIFEST_FIELDS for entry in entries):
        raise _invalid()
    return _manifests(tuple(CaptureManifest(**entry) for entry in entries), limits)


async def read_capture_body(chunks: AsyncIterator[bytes], *, limits: CaptureLimits) -> tuple[SealedCapture, ...]:
    """Validate all allocation budgets before retaining exact-length original objects."""
    limits = _limits(limits)
    reader = _Reader(chunks, len(MAGIC) + 4 + HEADER_MAX + limits.maximum_compressed_bytes)
    if await reader._exact(len(MAGIC)) != MAGIC:
        raise _invalid()
    header_size = int.from_bytes(await reader._exact(4), "big")
    if not 1 <= header_size <= HEADER_MAX:
        raise _invalid()
    manifests = _header_manifests(await reader._exact(header_size), limits)
    captures = []
    for manifest in manifests:
        captures.append(SealedCapture(await reader._exact(manifest.compressed_bytes), manifest))
    if await reader._has_bytes():
        raise _invalid()
    return tuple(captures)


def _streams(definition):
    if type(definition) is not CustomImportDefinition:
        raise _invalid()
    streams = _bounded_sequence(definition.source_streams)
    if any(type(stream) is not SourceStream for stream in streams):
        raise _invalid()
    by_id = {_identifier(stream.stream_id): stream for stream in streams}
    if len(by_id) != len(streams):
        raise _invalid()
    return by_id


def _content_digest(entries, streams):
    by_id = {entry["stream_id"]: entry for entry in entries}
    if len(by_id) != len(entries) or set(by_id) != set(streams):
        raise _invalid()
    document_by_key = {"captures": [by_id[name] for name in sorted(by_id)]}
    return hashlib.sha256(_CONTENT_DOMAIN + _canonical(document_by_key)).hexdigest()


def expected_content_sha256(definition, objects, source_snapshot_token) -> str:
    """Bind approved content and persisted stream shape, excluding transport identities."""
    streams = _streams(definition)
    snapshot = _snapshot(source_snapshot_token)
    entries = []
    for source_object in _bounded_sequence(objects):
        stream_id = _identifier(getattr(source_object, "stream_id", None))
        size = getattr(source_object, "expected_bytes", None)
        if (
            stream_id not in streams
            or type(size) is not int
            or not 0 <= size <= _DEFAULT_LIMITS.maximum_compressed_bytes
        ):
            raise _invalid()
        entries.append(
            {
                "stream_id": stream_id,
                "stream_sha256": _source_stream_sha256(streams[stream_id]),
                "source_snapshot_token": snapshot,
                "expected_bytes": size,
                "expected_sha256": _digest(getattr(source_object, "expected_sha256", None)),
            }
        )
    if sum(entry["expected_bytes"] for entry in entries) > _DEFAULT_LIMITS.maximum_compressed_bytes:
        raise _invalid()
    return _content_digest(entries, streams)


def manifested_content_sha256(definition, manifests) -> str:
    """Recompute the same content identity from complete, shared-snapshot manifests."""
    streams = _streams(definition)
    entries = []
    for manifest in _manifests(manifests, _DEFAULT_LIMITS):
        stream = streams.get(manifest.stream_id)
        if (
            stream is None
            or manifest.stream_sha256 != _source_stream_sha256(stream)
            or manifest.format != stream.format
            or manifest.compression != stream.compression
        ):
            raise _invalid()
        entries.append(
            {
                "stream_id": manifest.stream_id,
                "stream_sha256": manifest.stream_sha256,
                "source_snapshot_token": manifest.source_snapshot_token,
                "expected_bytes": manifest.compressed_bytes,
                "expected_sha256": manifest.compressed_sha256,
            }
        )
    return _content_digest(entries, streams)
