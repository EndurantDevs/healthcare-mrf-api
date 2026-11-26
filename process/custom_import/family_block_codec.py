# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded zstd families with separate semantic and storage commitments.

The caller supplies a trusted manifest, pins its complete database scope and
checks relational/scalar authority. This codec grants no write or read authority.
Fresh families recompute the existing semantic hash. Proven retained families
preserve their inherited hash: legacy SQL payloads can be opaque and their hash
inputs cannot be inferred from current parsing. Only an authorized retained
reader may select that internal path. Both paths verify the entire envelope
before exposing even one record. No persisted per-child hashes exist in the
canonical lane; sparse text, hashes and independent scalars remain exact.
"""

import hashlib
import hmac
import struct
import tempfile
import threading
from collections import deque
from collections.abc import Mapping
from compression import zstd
from contextlib import AbstractContextManager
from dataclasses import dataclass
from functools import partial

import msgpack

from process.custom_import import runner_codec
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_records import (
    FamilyBlockError,
    FamilyRecord,
    SparseRecord,
    _integer,
    _record_from_wire,
    _sha256,
    record_from_wire,
    record_parts,
)

__all__ = (
    "FamilyBlockError",
    "FamilyBlockResourceLimit",
    "FamilyBlockLimits",
    "FamilyBlockManifest",
    "FamilyRecord",
    "SparseRecord",
    "EncodedFamily",
    "VerifiedFamily",
    "encode_family",
    "encode_retained_family",
    "verify_family",
)

_CONTRACT = "custom-import/family-block/v1"
_ENVELOPE_DOMAIN = b"custom-import/family-block/v1\x00storage\x00"
_LENGTH = struct.Struct("!Q")
_FRAGMENT = struct.Struct("!QQ")
_DIRECTORY = struct.Struct("!qQqQ")
_ZSTD_COMPRESS_OPTIONS = {
    zstd.CompressionParameter.compression_level: 3,
    zstd.CompressionParameter.checksum_flag: True,
}
_ZSTD_LOCAL = threading.local()


def _zstd_compressor() -> zstd.ZstdCompressor:
    compressor = getattr(_ZSTD_LOCAL, "compressor", None)
    if compressor is None:
        compressor = zstd.ZstdCompressor(options=_ZSTD_COMPRESS_OPTIONS)
        _ZSTD_LOCAL.compressor = compressor
    return compressor


class FamilyBlockResourceLimit(FamilyBlockError):
    """The caller's resource budget was exhausted, not a logical input rejection."""


@dataclass(frozen=True)
class FamilyBlockLimits:
    """Caller budgets; fragments and memory thresholds never cap family size.

    ``max_record_bytes`` admits one original record, including sparse evidence.
    The caller must preserve its existing singleton allowance. Larger families
    spill to a temporary file and may use arbitrarily many bounded fragments,
    up to the caller's explicit spool budget. Owners supply their accounted work
    directory through ``spool_directory``; its lifetime remains caller-owned.
    """

    max_spool_bytes: int
    max_record_bytes: int
    fragment_bytes: int = 64 * 1024
    memory_bytes: int = 1024 * 1024
    spool_directory: str | None = None

    def __post_init__(self):
        for value in (self.max_spool_bytes, self.max_record_bytes, self.fragment_bytes, self.memory_bytes):
            _integer(value, "resource bound")
        if max(self.max_record_bytes, self.fragment_bytes, self.memory_bytes) > self.max_spool_bytes:
            raise ValueError("family resource bounds exceed the spool budget")


@dataclass(frozen=True)
class FamilyBlockManifest:
    """Trusted owner evidence, kept outside the untrusted fragment stream."""

    definition_sha256: str
    family_sha256: bytes
    storage_sha256: bytes
    retained: bool
    child_count: int
    fragment_count: int
    raw_bytes: int
    stored_bytes: int


class _OwnedSpool(AbstractContextManager):
    def __init__(self, limits):
        self._file = tempfile.SpooledTemporaryFile(max_size=limits.memory_bytes, mode="w+b", dir=limits.spool_directory)
        self.limits = limits

    def close(self):
        """Release the exact owned encoding or verification spool."""
        self._file.close()

    def __exit__(self, *_exc):
        self.close()


class EncodedFamily(_OwnedSpool):
    """Completed encoding; its fragment iterator is replayable until close."""

    _prefix = ()

    def fragments(self):
        """Replay every completed compressed fragment in original sequence."""
        for _, frame in self.indexed_fragments():
            yield frame

    def indexed_fragments(self):
        """Replay each fragment and its original record boundaries without decoding it."""
        yield from self._prefix
        yield from self._body_fragments()

    def _body_fragments(self):
        self._file.seek(0)
        while prefix := self._file.read(_LENGTH.size):
            directory = _DIRECTORY.unpack(self._file.read(_DIRECTORY.size))
            frame = self._file.read(_LENGTH.unpack(prefix)[0])
            yield _indexed_fragment(directory, frame)


class VerifiedFamily(_OwnedSpool):
    """A fully checked family; iterators borrow its spool and run sequentially."""

    def records(self, collection=None):
        """Replay one borrowed, fully verified root or child collection stream."""
        for record, _wire, _parts in self.decoded_records(collection):
            yield record

    def decoded_records(self, collection=None):
        """Replay each verified record with its checked wire cells and canonical parts."""
        if collection is not None and collection not in self.definition.collections_by_name:
            raise FamilyBlockError("family collection differs")
        self._file.seek(0)
        _read_object(self._file, self.limits)
        root = _read_object(self._file, self.limits)
        if collection is None:
            record, parts = record_from_wire(self.definition, None, root, retained=self.manifest.retained)
            yield record, root, parts
            return
        for name in sorted(self.definition.collections_by_name):
            _read_object(self._file, self.limits)
            while True:
                item = _read_object(self._file, self.limits)
                if item[0] == "end":
                    break
                if name == collection:
                    record, parts = record_from_wire(self.definition, name, item[1], retained=self.manifest.retained)
                    yield record, item[1], parts
            if name == collection:
                return


def _header(definition, family_sha256, retained):
    return [_CONTRACT, definition.digest, family_sha256, retained]


class _Encoder:
    def __init__(self, output, reserved_spool_bytes, *, prefix_bytes=0):
        self.output = output
        self.reserved_spool_bytes = reserved_spool_bytes
        self.pending = bytearray()
        self.digest = hashlib.sha256(_ENVELOPE_DOMAIN)
        self.fragments = self.raw_bytes = self.stored_bytes = 0
        self.next_record = self.record_starts = self.continuation = 0
        self.first_record = self.first_offset = -1
        self.prefix_bytes = prefix_bytes
        self.prefix_raw_bytes = 0
        self.prefix_frames = []

    def write(self, value, *, is_record=False):
        """Frame one exact record, splitting it across bounded compressed fragments."""
        packed = msgpack.packb(value, use_bin_type=True)
        if len(packed) > self.output.limits.max_record_bytes:
            raise FamilyBlockResourceLimit("family record exceeds the caller's resource budget")
        if is_record:
            if self.record_starts == 0:
                self.first_record, self.first_offset = self.next_record, len(self.pending)
            self.record_starts += 1
            self.next_record += 1
        fragment_bytes = self.output.limits.fragment_bytes
        remaining = _LENGTH.size + len(packed)
        for data in (_LENGTH.pack(len(packed)), packed):
            position = 0
            while position < len(data):
                count = min(fragment_bytes - len(self.pending), len(data) - position)
                self.pending.extend(memoryview(data)[position : position + count])
                position += count
                remaining -= count
                if len(self.pending) == fragment_bytes:
                    self.flush()
                    self.continuation = min(fragment_bytes, remaining)

    def flush(self):
        """Persist a checksum-bearing fragment within the caller's spool budget."""
        if not self.pending:
            return
        directory = (self.first_record, self.record_starts, self.first_offset, self.continuation)
        is_header_fragment = self.raw_bytes < self.prefix_bytes
        self.raw_bytes += len(self.pending)
        self.fragments += 1
        if is_header_fragment:
            self.prefix_raw_bytes += len(self.pending)
            self._check_budget()
            self.prefix_frames.append((self.pending, directory))
            self.pending = bytearray()
        else:
            frame = _compress(self.fragments - 1, self.pending)
            prefix = _LENGTH.pack(len(frame))
            self.stored_bytes += len(prefix) + len(frame)
            self._check_budget()
            if not self.prefix_bytes:
                self.digest.update(prefix)
                self.digest.update(frame)
            self.output._file.write(prefix)
            self.output._file.write(_DIRECTORY.pack(*directory))
            self.output._file.write(frame)
            self.pending.clear()
        self.first_record = self.first_offset = -1
        self.record_starts = self.continuation = 0

    def _check_budget(self):
        spool_bytes = self.stored_bytes + self.fragments * _DIRECTORY.size + self.reserved_spool_bytes
        if max(self.raw_bytes, spool_bytes + self.prefix_raw_bytes) > self.output.limits.max_spool_bytes:
            raise FamilyBlockResourceLimit("family exceeds the caller's spool budget")

    def finish_prefix(self, header):
        """Fill the digest in the delayed header, preserving exact fragment boundaries."""
        packed = _frame_object(header)
        offset = 0
        prefix_fragments = []
        for ordinal, (raw, directory) in enumerate(self.prefix_frames):
            count = min(len(raw), len(packed) - offset)
            raw[:count] = packed[offset : offset + count]
            offset += count
            frame = _compress(ordinal, raw)
            prefix_fragments.append(_indexed_fragment(directory, frame))
            self.stored_bytes += _LENGTH.size + len(frame)
        self.prefix_frames.clear()
        self.prefix_raw_bytes = 0
        self._check_budget()
        self.output._prefix = tuple(prefix_fragments)
        for _, frame in self.output.indexed_fragments():
            self.digest.update(_LENGTH.pack(len(frame)))
            self.digest.update(frame)

    def complete(self, definition, family_sha256, child_count, *, retained):
        """Finish the envelope and report the exact committed fragment sizes."""
        self.write(["complete", child_count])
        self.flush()
        if self.prefix_bytes:
            self.finish_prefix(_header(definition, family_sha256, retained))
        return FamilyBlockManifest(
            definition.digest,
            family_sha256,
            self.digest.digest(),
            retained,
            child_count,
            self.fragments,
            self.raw_bytes,
            self.stored_bytes,
        )


def _indexed_fragment(directory, frame):
    first, count, offset, continuation = directory
    ordinal, raw_size = _FRAGMENT.unpack_from(frame)
    return (ordinal, raw_size, None if first < 0 else first, count, None if offset < 0 else offset, continuation), frame


def _compress(ordinal, raw):
    return _FRAGMENT.pack(ordinal, len(raw)) + _zstd_compressor().compress(raw, mode=zstd.ZstdCompressor.FLUSH_FRAME)


def _frame_object(value):
    packed = msgpack.packb(value, use_bin_type=True)
    return _LENGTH.pack(len(packed)) + packed


def _check_family_digest(definition, root_values, documents_by_collection, expected_digest, *, retained):
    if retained:
        for documents in documents_by_collection.values():
            deque(documents, maxlen=0)
        return expected_digest
    actual = runner_codec._family_hash_from_documents(definition, root_values, documents_by_collection)
    if expected_digest is not None and not hmac.compare_digest(actual, expected_digest):
        raise FamilyBlockError("family semantic digest differs")
    return actual


def _encode(definition, root, children, expected_family_sha256, limits, *, retained, reserved_spool_bytes=0):
    """Write ordered records with their existing semantic hash and exact byte census."""
    if type(definition) is not CustomImportDefinition or type(limits) is not FamilyBlockLimits:
        raise ValueError("family encoding requires a definition and resource bounds")
    needs_family_digest = expected_family_sha256 is None and not retained
    placeholder = bytes(32) if needs_family_digest else expected_family_sha256
    _sha256(placeholder)
    _integer(reserved_spool_bytes, "reserved spool bytes", zero=True)
    if not isinstance(children, Mapping) or set(children) != set(definition.collections_by_name):
        raise FamilyBlockError("family collections differ")
    output = EncodedFamily(limits)
    try:
        header = _header(definition, placeholder, retained)
        encoder = _Encoder(
            output, reserved_spool_bytes, prefix_bytes=len(_frame_object(header)) if needs_family_digest else 0
        )
        encoder.write(header)
        root_wire, _ = record_parts(definition, None, root, retained=retained)
        output.root_payload_sha256 = runner_codec.digest_text("root-payload", root_wire[3])
        output.root_scalar_count = len(root_wire[4])
        output.child_scalar_count = 0
        encoder.write(root_wire, is_record=True)
        root_values = (
            None if retained else runner_codec.payload_values(definition.root_fields, root_wire[3], label="family root")
        )
        count_by_collection = {}

        def documents(name):
            """Write a complete collection while yielding its ordered hash inputs."""
            encoder.write(["collection", name])
            count, previous = 0, None
            for count, child in enumerate(children[name], 1):
                wire, parts = record_parts(definition, name, child, retained=retained)
                if previous is not None and parts < previous:
                    raise FamilyBlockError("family child documents are not ordered")
                encoder.write(["record", wire], is_record=True)
                output.child_scalar_count += len(wire[4])
                yield parts
                previous = parts
            count_by_collection[name] = count
            encoder.write(["end", count])

        documents_by_collection = {name: documents(name) for name in sorted(children)}
        family_sha256 = _check_family_digest(
            definition, root_values, documents_by_collection, expected_family_sha256, retained=retained
        )
        child_count = sum(count_by_collection.values())
        output.manifest = encoder.complete(definition, family_sha256, child_count, retained=retained)
        return output
    except BaseException:
        output.close()
        raise


def encode_family(definition, root, children_by_collection, expected_family_sha256, *, limits, reserved_spool_bytes=0):
    """Encode fresh canonical records, deriving their digest from the stream if absent."""
    return _encode(
        definition,
        root,
        children_by_collection,
        expected_family_sha256,
        limits,
        retained=False,
        reserved_spool_bytes=reserved_spool_bytes,
    )


def encode_retained_family(
    definition, root, children_by_collection, inherited_family_sha256, *, limits, reserved_spool_bytes=0
):
    """Encode a caller-proven retained family without redefining its semantic hash.

    The owner must first pin and verify the base, its exact original records and
    scalar/provenance evidence, including its existing key/payload relationship
    checks. Opaque hydration is not semantic-read authority. Raw sparse hash
    checks remain here too. This is not a fallback for failed fresh decoding.
    """
    return _encode(
        definition,
        root,
        children_by_collection,
        inherited_family_sha256,
        limits,
        retained=True,
        reserved_spool_bytes=reserved_spool_bytes,
    )


def _read_object(source, limits):
    prefix = source.read(_LENGTH.size)
    if len(prefix) != _LENGTH.size:
        raise FamilyBlockError("family record stream is truncated")
    size = _LENGTH.unpack(prefix)[0]
    if size > limits.max_record_bytes:
        raise FamilyBlockResourceLimit("family record exceeds the caller's resource budget")
    packed = source.read(size)
    if len(packed) != size:
        raise FamilyBlockError("family record stream is truncated")
    try:
        return msgpack.unpackb(packed, raw=False, strict_map_key=True)
    except (ValueError, TypeError, msgpack.UnpackException) as error:
        raise FamilyBlockError("family record encoding is malformed") from error


def _child_wires(read, name, check):
    if read() != ["collection", name]:
        raise FamilyBlockError("family collection order differs")
    count = 0
    while True:
        if check is not None:
            check()
        item = read()
        if type(item) is not list or len(item) != 2:
            raise FamilyBlockError("family collection record is malformed")
        if item[0] == "end":
            if type(item[1]) is not int or item[1] != count:
                raise FamilyBlockError("family collection count differs")
            return
        if item[0] != "record":
            raise FamilyBlockError("family collection record is malformed")
        count += 1
        yield item[1]


def _verify_records(output, check, observe):
    """Check every record and collection count against the manifest before reading."""
    record_file, definition, manifest = output._file, output.definition, output.manifest
    record_file.seek(0)
    read = partial(_read_object, record_file, output.limits)
    header = read()
    if (
        type(header) is not list
        or len(header) != 4
        or type(header[3]) is not bool
        or header != _header(definition, manifest.family_sha256, manifest.retained)
    ):
        raise FamilyBlockError("family header differs from trusted evidence")
    if check is not None:
        check()
    root, parts = record_from_wire(definition, None, read(), retained=manifest.retained)
    if observe is not None:
        observe(None, root, parts)
    fields_by_collection = runner_codec.fields_by_collection(definition)
    count_by_collection = {}

    def documents(name):
        """Verify the wire fields and order of one complete child collection."""
        fields = fields_by_collection[name]
        by_name = {field.field_id: field for field in fields}
        count, previous = 0, None
        for count, wire in enumerate(_child_wires(read, name, check), 1):
            record, parts = _record_from_wire(definition, name, wire, fields, by_name, manifest.retained)
            if observe is not None:
                observe(name, record, parts)
            if previous is not None and parts < previous:
                raise FamilyBlockError("family child documents are not ordered")
            yield parts
            previous = parts
        count_by_collection[name] = count

    documents_by_collection = {name: documents(name) for name in sorted(definition.collections_by_name)}
    _check_family_digest(
        definition, root.values_by_field, documents_by_collection, manifest.family_sha256, retained=manifest.retained
    )
    if sum(count_by_collection.values()) != manifest.child_count or read() != ["complete", manifest.child_count]:
        raise FamilyBlockError("family final count differs")
    if record_file.read(1):
        raise FamilyBlockError("family contains trailing records")


def _verify_fragments(output, fragments, check):
    limits, manifest = output.limits, output.manifest
    digest = hashlib.sha256(_ENVELOPE_DOMAIN)
    count = raw_bytes = stored_bytes = 0
    for frame in fragments:
        if check is not None:
            check()
        if type(frame) is not bytes or len(frame) < _FRAGMENT.size:
            raise FamilyBlockError("family fragment is malformed")
        ordinal, raw_size = _FRAGMENT.unpack(frame[: _FRAGMENT.size])
        if ordinal != count:
            raise FamilyBlockError("family fragment order differs")
        if not raw_size:
            raise FamilyBlockError("family fragment is empty")
        if raw_size > limits.fragment_bytes:
            raise FamilyBlockResourceLimit("family fragment exceeds the caller's resource budget")
        prefix = _LENGTH.pack(len(frame))
        stored_bytes += len(prefix) + len(frame)
        raw_bytes += raw_size
        if max(raw_bytes, stored_bytes) > limits.max_spool_bytes:
            raise FamilyBlockResourceLimit("family exceeds the caller's spool budget")
        window_log = min(31, max(10, (raw_size - 1).bit_length()))
        decoder = zstd.ZstdDecompressor(options={zstd.DecompressionParameter.window_log_max: window_log})
        try:
            raw = decoder.decompress(frame[_FRAGMENT.size :], max_length=raw_size + 1)
        except zstd.ZstdError as error:
            raise FamilyBlockError("family compressed fragment is malformed") from error
        if len(raw) != raw_size or not decoder.eof or decoder.unused_data:
            raise FamilyBlockError("family compressed fragment is incomplete or oversized")
        output._file.write(raw)
        digest.update(prefix)
        digest.update(frame)
        count += 1
    if (count, raw_bytes, stored_bytes) != (manifest.fragment_count, manifest.raw_bytes, manifest.stored_bytes):
        raise FamilyBlockError("family fragment census differs")
    if not hmac.compare_digest(digest.digest(), manifest.storage_sha256):
        raise FamilyBlockError("family storage digest differs")


def verify_family(definition, fragments, manifest, *, limits, check=None, observe=None):
    """Verify every fragment, record and semantic commitment before exposing a family."""
    if type(definition) is not CustomImportDefinition or type(limits) is not FamilyBlockLimits:
        raise ValueError("family decoding requires a definition and resource bounds")
    if type(manifest) is not FamilyBlockManifest or type(manifest.retained) is not bool:
        raise FamilyBlockError("family manifest is malformed")
    _sha256(manifest.family_sha256)
    _sha256(manifest.storage_sha256)
    for manifest_count in (manifest.child_count, manifest.fragment_count, manifest.raw_bytes, manifest.stored_bytes):
        _integer(manifest_count, "manifest count", zero=True)
    if manifest.definition_sha256 != definition.digest:
        raise FamilyBlockError("family definition differs")
    output = VerifiedFamily(limits)
    output.definition, output.manifest = definition, manifest
    try:
        _verify_fragments(output, fragments, check)
        _verify_records(output, check, observe)
        if check is not None:
            check()
        return output
    except BaseException:
        output.close()
        raise
