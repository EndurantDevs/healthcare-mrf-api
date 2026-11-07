# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One persisted family integrity commitment, including exact storage directories.

The codec's frame digest is transient. Full readers compare the final envelope
against trusted owner evidence, verify the unchanged semantic/sparse checks and
prove every directory entry before exposing records. Ordinary pinned API reads
may use a bounded checksum-checked fragment without claiming that full proof.
"""

import hashlib
import hmac
import struct
import tempfile
from compression import zstd
from contextlib import ExitStack
from dataclasses import dataclass

import msgpack

from process.custom_import.family_block_codec import (
    _ENVELOPE_DOMAIN,
    _FRAGMENT,
    _LENGTH,
    FamilyBlockError,
    FamilyBlockLimits,
    FamilyBlockManifest,
    FamilyBlockResourceLimit,
    _read_object,
    verify_family,
)
from process.custom_import.family_block_directory import RevisionMembership, storage_envelope_sha256
from process.custom_import.family_block_records import _integer, _sha256

_IDENTITY = struct.Struct("!QQB")


@dataclass(frozen=True)
class StoredFamilyManifest:
    definition_sha256: str
    family_sha256: bytes
    storage_sha256: bytes
    retained: bool
    child_count: int
    fragment_count: int
    raw_bytes: int
    stored_bytes: int
    membership_count: int

    def codec_manifest(self, transient_frame_sha256):
        """Reconstruct the existing transient codec evidence for full verification."""
        return FamilyBlockManifest(
            self.definition_sha256,
            self.family_sha256,
            transient_frame_sha256,
            self.retained,
            self.child_count,
            self.fragment_count,
            self.raw_bytes,
            self.stored_bytes,
        )


def stored_manifest(codec_manifest, fragment_directory, memberships):
    """Build owner evidence from completed encoding; this alone is not verification."""
    digest, count = storage_envelope_sha256(codec_manifest, fragment_directory, memberships)
    return StoredFamilyManifest(
        codec_manifest.definition_sha256,
        codec_manifest.family_sha256,
        digest,
        codec_manifest.retained,
        codec_manifest.child_count,
        codec_manifest.fragment_count,
        codec_manifest.raw_bytes,
        codec_manifest.stored_bytes,
        count,
    )


def _write(source, value):
    data = msgpack.packb(value, use_bin_type=True)
    source.write(_LENGTH.pack(len(data)))
    source.write(data)


def _rows(source):
    source.seek(0)
    while prefix := source.read(8):
        yield msgpack.unpackb(source.read(_LENGTH.unpack(prefix)[0]), raw=False, strict_map_key=True)


def _frames(source):
    source.seek(0)
    while prefix := source.read(8):
        yield source.read(_LENGTH.unpack(prefix)[0])


def _fragment_header(frame, directory, limits):
    if type(frame) is not bytes or len(frame) <= 21 or len(directory) != 6:
        raise FamilyBlockError("storage fragment is malformed")
    ordinal, raw_bytes = _FRAGMENT.unpack(frame[:16])
    if (ordinal, raw_bytes) != tuple(directory[:2]) or not raw_bytes:
        raise FamilyBlockError("storage fragment sequence or length differs")
    # A standard zstd frame's descriptor carries its content-checksum flag.
    if frame[16:20] != b"\x28\xb5\x2f\xfd" or not frame[20] & 4:
        raise FamilyBlockError("storage fragment lacks its intrinsic checksum")
    if raw_bytes > limits.fragment_bytes:
        raise FamilyBlockResourceLimit("storage fragment exceeds the caller's resource budget")
    return raw_bytes


def decode_fragment(frame, directory, *, limits):
    """Bounded sealed-API primitive; this does not verify a family semantic hash."""
    raw_bytes = _fragment_header(frame, directory, limits)
    decoder = zstd.ZstdDecompressor(
        options={zstd.DecompressionParameter.window_log_max: min(31, max(10, (raw_bytes - 1).bit_length()))}
    )
    try:
        raw = decoder.decompress(frame[16:], max_length=raw_bytes + 1)
    except zstd.ZstdError as error:
        raise FamilyBlockError("storage fragment checksum or compression differs") from error
    if len(raw) != raw_bytes or not decoder.eof or decoder.unused_data:
        raise FamilyBlockError("storage fragment extent differs")
    return raw


def _objects(verified, collection_slots, check=None):
    """Internal framed-object spans after the existing full codec checks passed."""
    source, limits = verified._file, verified.limits
    source.seek(0)
    ordinal = -1
    collection = None
    position = 0
    while position < verified.manifest.raw_bytes:
        if check is not None:
            check()
        value = _read_object(source, limits)
        end = source.tell()
        identity = None
        if position and ordinal < 0:
            ordinal = 0
            identity = value[0], 0, ordinal
        elif position and value[0] == "collection":
            collection = collection_slots[value[1]]
        elif position and value[0] == "record":
            ordinal += 1
            identity = value[1][0], collection, ordinal
        yield position, end, identity
        position = end


def iter_fragment_directory(verified, collection_slots, raw_sizes, *, _identity_output=None, check=None):
    """Derive exact logical-record starts and framing continuations with O(1) state.

    Record ordinals are root zero followed by the codec's canonical collection /
    child order. Offsets point at the eight-byte length prefix, including when
    that prefix or the record spans fragments. Non-record framing also contributes
    to continuation_bytes so no fragment is decoded at an invented boundary.
    """
    sizes = iter(raw_sizes)
    ordinal = 0
    start = 0
    size = next(sizes, None)
    if size is None:
        raise FamilyBlockError("storage fragment directory is empty")
    first = offset = None
    starts = continuation = 0
    for object_start, object_end, identity in _objects(verified, collection_slots, check):
        if identity is not None:
            if _identity_output is not None:
                _identity_output.write(_IDENTITY.pack(identity[0], identity[1], 0))
            if first is None:
                first, offset = identity[2], object_start - start
            starts += 1
        while object_end >= start + size:
            if check is not None:
                check()
            yield (ordinal, size, first, starts, offset, continuation)
            start += size
            ordinal += 1
            size = next(sizes, None)
            if size is None:
                if object_end != start:
                    raise FamilyBlockError("storage fragment directory ends inside a record")
                break
            first = offset = None
            starts = 0
            continuation = min(size, object_end - start)
        if size is None:
            break
    if size is not None:
        yield (ordinal, size, first, starts, offset, continuation)
        start += size
        if next(sizes, None) is not None:
            raise FamilyBlockError("storage fragment directory has trailing frames")
    if start != verified.manifest.raw_bytes:
        raise FamilyBlockError("storage fragment directory extent differs")


def _fixed_storage_bytes(manifest):
    return manifest.stored_bytes + manifest.raw_bytes + (manifest.child_count + 1) * _IDENTITY.size


def _check_storage_manifest(manifest, limits):
    if type(manifest) is not StoredFamilyManifest or type(limits) is not FamilyBlockLimits:
        raise ValueError("stored family requires trusted typed evidence and resource bounds")
    _sha256(manifest.storage_sha256)
    for name in ("child_count", "fragment_count", "raw_bytes", "stored_bytes"):
        _integer(getattr(manifest, name), "stored manifest count", zero=name == "child_count")
    _integer(manifest.membership_count, "membership count")
    if _fixed_storage_bytes(manifest) > limits.max_spool_bytes:
        raise FamilyBlockResourceLimit("stored family verification exceeds the caller's combined spool budget")


def _owned_spool(stack, limits):
    return stack.enter_context(
        tempfile.SpooledTemporaryFile(max_size=max(1, limits.memory_bytes // 4), mode="w+b", dir=limits.spool_directory)
    )


def _stage_fragments(fragment_rows, manifest, limits, encoded, directory, check):
    frame_digest = hashlib.sha256(_ENVELOPE_DOMAIN)
    written = count = raw_count = 0
    for metadata, frame in fragment_rows:
        if check is not None:
            check()
        # Full codec decompression checks CRC once; reject absent CRC here.
        _fragment_header(frame, metadata, limits)
        if metadata[0] != count:
            raise FamilyBlockError("storage fragment order differs")
        prefix = _LENGTH.pack(len(frame))
        if written + len(prefix) + len(frame) > manifest.stored_bytes:
            raise FamilyBlockError("storage fragment census exceeds trusted evidence")
        frame_digest.update(prefix)
        frame_digest.update(frame)
        encoded.write(prefix)
        encoded.write(frame)
        _write(directory, metadata)
        count += 1
        raw_count += metadata[1]
        written += len(prefix) + len(frame)
        if (
            written + directory.tell() + manifest.raw_bytes + (manifest.child_count + 1) * _IDENTITY.size
            > limits.max_spool_bytes
        ):
            raise FamilyBlockResourceLimit("stored family exceeds the caller's combined spool budget")
    if (count, raw_count, written) != (manifest.fragment_count, manifest.raw_bytes, manifest.stored_bytes):
        raise FamilyBlockError("storage fragment census differs")
    return frame_digest.digest()


def _stage_memberships(memberships, manifest, limits, directory, member_data, check):
    for membership in memberships:
        if check is not None:
            check()
        if type(membership) is not RevisionMembership:
            raise FamilyBlockError("membership directory type differs")
        _write(member_data, membership.cells())
        if _fixed_storage_bytes(manifest) + directory.tell() + member_data.tell() > limits.max_spool_bytes:
            raise FamilyBlockResourceLimit("stored family exceeds the caller's combined spool budget")


def _verify_decoded_directory(verified, collection_slots, encoded, directory, identity_data, manifest, check):
    expected = iter(_rows(directory))
    for actual in iter_fragment_directory(
        verified,
        collection_slots,
        (_FRAGMENT.unpack(frame[:16])[1] for frame in _frames(encoded)),
        _identity_output=identity_data,
        check=check,
    ):
        if list(actual) != next(expected, None):
            raise FamilyBlockError("fragment directory differs from decoded records")
    if next(expected, None) is not None:
        raise FamilyBlockError("fragment directory contains extra rows")
    if identity_data.tell() != (manifest.child_count + 1) * _IDENTITY.size:
        raise FamilyBlockError("decoded record census differs")


def _verify_membership_identities(member_data, identity_data, manifest, check):
    previous = None
    for cells in _rows(member_data):
        membership = RevisionMembership(*cells)
        if (
            previous is not None
            and membership.collection_slot == previous[0]
            and membership.first_revision_id <= previous[1]
        ):
            raise FamilyBlockError("membership directory ranges overlap")
        for revision_id, ordinal in membership.records():
            if check is not None:
                check()
            if ordinal > manifest.child_count:
                raise FamilyBlockError("membership ordinal is outside the family")
            identity_data.seek(ordinal * _IDENTITY.size)
            actual = _IDENTITY.unpack(identity_data.read(_IDENTITY.size))
            if actual != (revision_id, membership.collection_slot, 0):
                raise FamilyBlockError("membership differs from decoded identity or repeats an ordinal")
            identity_data.seek(ordinal * _IDENTITY.size)
            identity_data.write(_IDENTITY.pack(revision_id, membership.collection_slot, 1))
        previous = membership.collection_slot, membership.last_revision_id


def verify_stored_family(definition, collection_slots, fragment_rows, memberships, manifest, *, limits, check=None):
    """Fully verify the unchanged envelope, semantics, directories and actual IDs.

    Fragment and membership inputs must reach exact EOF; every owned scratch
    closes on failure. Only the independently owned verified family is returned.
    The caller supplies its accounted spool directory and cancellation check.
    """
    _check_storage_manifest(manifest, limits)
    with ExitStack() as stack:
        encoded = _owned_spool(stack, limits)
        directory = _owned_spool(stack, limits)
        member_data = _owned_spool(stack, limits)
        identity_data = _owned_spool(stack, limits)
        frame_digest = _stage_fragments(fragment_rows, manifest, limits, encoded, directory, check)
        _stage_memberships(memberships, manifest, limits, directory, member_data, check)
        transient = manifest.codec_manifest(frame_digest)
        digest, membership_count = storage_envelope_sha256(
            transient, _rows(directory), (RevisionMembership(*cells) for cells in _rows(member_data)), check=check
        )
        if membership_count != manifest.membership_count or not hmac.compare_digest(digest, manifest.storage_sha256):
            raise FamilyBlockError("stored family envelope differs from trusted evidence")
        verified = verify_family(definition, _frames(encoded), transient, limits=limits, check=check)
        try:
            _verify_decoded_directory(verified, collection_slots, encoded, directory, identity_data, manifest, check)
            _verify_membership_identities(member_data, identity_data, manifest, check)
            verified.stored_manifest = manifest
            if check is not None:
                check()
            return verified
        except BaseException:
            verified.close()
            raise
