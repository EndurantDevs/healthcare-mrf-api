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
from collections import deque
from collections.abc import Mapping
from compression import zstd
from contextlib import AbstractContextManager
from dataclasses import dataclass

import msgpack

from process.custom_import import runner_codec
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_records import (
    FamilyBlockError,
    FamilyRecord,
    SparseRecord,
    _integer,
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

    def fragments(self):
        """Replay every completed compressed fragment in original sequence."""
        self._file.seek(0)
        while prefix := self._file.read(_LENGTH.size):
            yield self._file.read(_LENGTH.unpack(prefix)[0])


class VerifiedFamily(_OwnedSpool):
    """A fully checked family; iterators borrow its spool and run sequentially."""

    def records(self, collection=None):
        """Replay one borrowed, fully verified root or child collection stream."""
        if collection is not None and collection not in self.definition.collections_by_name:
            raise FamilyBlockError("family collection differs")
        self._file.seek(0)
        _read_object(self._file, self.limits)
        root = _read_object(self._file, self.limits)
        if collection is None:
            yield record_from_wire(self.definition, None, root, retained=self.manifest.retained)[0]
            return
        for name in sorted(self.definition.collections_by_name):
            _read_object(self._file, self.limits)
            while True:
                item = _read_object(self._file, self.limits)
                if item[0] == "end":
                    break
                if name == collection:
                    yield record_from_wire(self.definition, name, item[1], retained=self.manifest.retained)[0]
            if name == collection:
                return


def _header(definition, family_sha256, retained):
    return [_CONTRACT, definition.digest, family_sha256, retained]


class _Encoder:
    def __init__(self, output):
        self.output = output
        self.pending = bytearray()
        self.digest = hashlib.sha256(_ENVELOPE_DOMAIN)
        self.fragments = self.raw_bytes = self.stored_bytes = 0

    def write(self, value):
        """Frame one exact record, splitting it across bounded compressed fragments."""
        packed = msgpack.packb(value, use_bin_type=True)
        if len(packed) > self.output.limits.max_record_bytes:
            raise FamilyBlockResourceLimit("family record exceeds the caller's resource budget")
        for data in (_LENGTH.pack(len(packed)), packed):
            position = 0
            while position < len(data):
                count = min(self.output.limits.fragment_bytes - len(self.pending), len(data) - position)
                self.pending.extend(memoryview(data)[position : position + count])
                position += count
                if len(self.pending) == self.output.limits.fragment_bytes:
                    self.flush()

    def flush(self):
        """Persist a checksum-bearing fragment within the caller's spool budget."""
        if not self.pending:
            return
        frame = _FRAGMENT.pack(self.fragments, len(self.pending)) + zstd.compress(
            self.pending,
            options={zstd.CompressionParameter.compression_level: 3, zstd.CompressionParameter.checksum_flag: True},
        )
        self.raw_bytes += len(self.pending)
        self.stored_bytes += _LENGTH.size + len(frame)
        if max(self.raw_bytes, self.stored_bytes) > self.output.limits.max_spool_bytes:
            raise FamilyBlockResourceLimit("family exceeds the caller's spool budget")
        prefix = _LENGTH.pack(len(frame))
        self.digest.update(prefix)
        self.digest.update(frame)
        self.output._file.write(prefix)
        self.output._file.write(frame)
        self.fragments += 1
        self.pending.clear()


def _ordered_documents(records, definition, name, retained, consume):
    previous = None
    for record in records:
        wire, parts = record_parts(definition, name, record, retained=retained)
        if previous is not None and parts < previous:
            raise FamilyBlockError("family child documents are not ordered")
        consume(wire)
        yield parts
        previous = parts


def _encode(definition, root, children, expected_family_sha256, limits, *, retained):
    if type(definition) is not CustomImportDefinition or type(limits) is not FamilyBlockLimits:
        raise ValueError("family encoding requires a definition and resource bounds")
    _sha256(expected_family_sha256)
    if not isinstance(children, Mapping) or set(children) != set(definition.collections_by_name):
        raise FamilyBlockError("family collections differ")
    output = EncodedFamily(limits)
    try:
        encoder = _Encoder(output)
        encoder.write(_header(definition, expected_family_sha256, retained))
        root_wire, _ = record_parts(definition, None, root, retained=retained)
        encoder.write(root_wire)
        count_by_collection = {}

        def documents(name):
            """Encode and count one complete ordered child collection lazily."""
            encoder.write(["collection", name])
            count = 0
            for parts in _ordered_documents(
                children[name], definition, name, retained, lambda wire: encoder.write(["record", wire])
            ):
                count += 1
                yield parts
            count_by_collection[name] = count
            encoder.write(["end", count])

        documents_by_collection = {name: documents(name) for name in sorted(children)}
        if retained:
            for group in documents_by_collection.values():
                deque(group, maxlen=0)
        else:
            root_values = runner_codec.payload_values(definition.root_fields, root_wire[3], label="family root")
            actual = runner_codec._family_hash_from_documents(definition, root_values, documents_by_collection)
            if not hmac.compare_digest(actual, expected_family_sha256):
                raise FamilyBlockError("family semantic digest differs")
        child_count = sum(count_by_collection.values())
        encoder.write(["complete", child_count])
        encoder.flush()
        output.manifest = FamilyBlockManifest(
            definition.digest,
            expected_family_sha256,
            encoder.digest.digest(),
            retained,
            child_count,
            encoder.fragments,
            encoder.raw_bytes,
            encoder.stored_bytes,
        )
        return output
    except BaseException:
        output.close()
        raise


def encode_family(definition, root, children_by_collection, expected_family_sha256, *, limits):
    """Encode fresh canonical records in existing child-document hash order."""
    return _encode(definition, root, children_by_collection, expected_family_sha256, limits, retained=False)


def encode_retained_family(definition, root, children_by_collection, inherited_family_sha256, *, limits):
    """Encode a caller-proven retained family without redefining its semantic hash.

    The owner must first pin and verify the base, its exact original records and
    scalar/provenance evidence, including its existing key/payload relationship
    checks. Opaque hydration is not semantic-read authority. Raw sparse hash
    checks remain here too. This is not a fallback for failed fresh decoding.
    """
    return _encode(definition, root, children_by_collection, inherited_family_sha256, limits, retained=True)


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


def _verify_records(output, check=None):
    record_file, limits, definition, manifest = output._file, output.limits, output.definition, output.manifest
    record_file.seek(0)
    header = _read_object(record_file, limits)
    if (
        type(header) is not list
        or len(header) != 4
        or type(header[3]) is not bool
        or header != _header(definition, manifest.family_sha256, manifest.retained)
    ):
        raise FamilyBlockError("family header differs from trusted evidence")
    if check is not None:
        check()
    root, _ = record_from_wire(definition, None, _read_object(record_file, limits), retained=manifest.retained)
    count_by_collection = {}

    def documents(name):
        """Verify one complete child collection and its declared count."""
        if _read_object(record_file, limits) != ["collection", name]:
            raise FamilyBlockError("family collection order differs")
        count, previous = 0, None
        while True:
            if check is not None:
                check()
            item = _read_object(record_file, limits)
            if type(item) is not list or len(item) != 2:
                raise FamilyBlockError("family collection record is malformed")
            if item[0] == "end":
                if type(item[1]) is not int or item[1] != count:
                    raise FamilyBlockError("family collection count differs")
                break
            if item[0] != "record":
                raise FamilyBlockError("family collection record is malformed")
            _record, parts = record_from_wire(definition, name, item[1], retained=manifest.retained)
            if previous is not None and parts < previous:
                raise FamilyBlockError("family child documents are not ordered")
            count += 1
            yield parts
            previous = parts
        count_by_collection[name] = count

    documents_by_collection = {name: documents(name) for name in sorted(definition.collections_by_name)}
    if manifest.retained:
        for group in documents_by_collection.values():
            deque(group, maxlen=0)
    else:
        actual = runner_codec._family_hash_from_documents(definition, root.values_by_field, documents_by_collection)
        if not hmac.compare_digest(actual, manifest.family_sha256):
            raise FamilyBlockError("family semantic digest differs")
    if sum(count_by_collection.values()) != manifest.child_count or _read_object(record_file, limits) != [
        "complete",
        manifest.child_count,
    ]:
        raise FamilyBlockError("family final count differs")
    if record_file.read(1):
        raise FamilyBlockError("family contains trailing records")


def verify_family(definition, fragments, manifest, *, limits, check=None):
    """Consume and prove the entire stream before returning any readable values."""
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
    digest = hashlib.sha256(_ENVELOPE_DOMAIN)
    count = raw_bytes = stored_bytes = 0
    try:
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
        _verify_records(output, check)
        if check is not None:
            check()
        return output
    except BaseException:
        output.close()
        raise
