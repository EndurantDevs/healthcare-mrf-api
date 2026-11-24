# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded actual-ID directories and their separate internal storage commitment.

The existing semantic family and codec-frame hashes remain unchanged. Before
sealing, the owner must compare every directory entry with the fully verified
codec record stream. Hashing an unverified directory does not prove membership.
"""

import hashlib
import struct
from collections.abc import Iterable
from dataclasses import dataclass

import msgpack

from process.custom_import.family_block_records import FamilyBlockError, _integer

_DOMAIN = b"custom-import/family-block-directory/v1\x00"


def _pack_integer(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return result


def _unpack_integer(data, position):
    value = 0
    for shift in range(0, 70, 7):
        if position == len(data):
            break
        item = data[position]
        position += 1
        value |= (item & 127) << shift
        if item < 128:
            if shift and item == 0:
                raise FamilyBlockError("membership integer is not canonical")
            return value, position
    raise FamilyBlockError("membership integer is incomplete or out of range")


@dataclass(frozen=True)
class RevisionMembership:
    collection_slot: int
    first_revision_id: int
    last_revision_id: int
    record_count: int
    first_record_ordinal: int | None
    encoded_membership: bytes | None

    def __post_init__(self):
        _integer(self.collection_slot, "collection slot", zero=True)
        for name in ("first_revision_id", "last_revision_id", "record_count"):
            _integer(getattr(self, name), "membership bound")
        if self.last_revision_id < self.first_revision_id:
            raise FamilyBlockError("membership range is reversed")
        if self.encoded_membership is None:
            _integer(self.first_record_ordinal, "membership ordinal", zero=True)
            _integer(self.first_record_ordinal + self.record_count - 1, "membership ordinal", zero=True)
            if (self.collection_slot == 0) != (self.first_record_ordinal == 0) or (
                self.collection_slot == 0 and self.record_count != 1
            ):
                raise FamilyBlockError("membership collection differs")
        elif (
            type(self.encoded_membership) is not bytes
            or not self.encoded_membership
            or self.first_record_ordinal is not None
        ):
            raise FamilyBlockError("membership encoding differs")

    def cells(self):
        """Return the unchanged typed storage cells in native column order."""
        return (
            self.collection_slot,
            self.first_revision_id,
            self.last_revision_id,
            self.record_count,
            self.first_record_ordinal,
            self.encoded_membership,
        )

    def records(self):
        """Exhaust and validate exact actual-ID/ordinal pairs without inferred gaps."""
        if self.encoded_membership is None:
            if (
                self.last_revision_id - self.first_revision_id != self.record_count - 1
                or self.first_record_ordinal is None
            ):
                raise FamilyBlockError("membership range differs")
            for index in range(self.record_count):
                yield self.first_revision_id + index, self.first_record_ordinal + index
            return
        if self.first_record_ordinal is not None:
            raise FamilyBlockError("membership modes overlap")
        encoded_membership, position, last_id, last_ordinal = self.encoded_membership, 0, 0, 0
        first = None
        for _ in range(self.record_count):
            gap, position = _unpack_integer(encoded_membership, position)
            delta, position = _unpack_integer(encoded_membership, position)
            if gap == 0:
                raise FamilyBlockError("membership IDs repeat")
            last_id += gap
            last_ordinal += (delta >> 1) ^ -(delta & 1)
            _integer(last_id, "membership revision")
            _integer(last_ordinal, "membership ordinal", zero=True)
            if (self.collection_slot == 0) != (last_ordinal == 0):
                raise FamilyBlockError("membership collection differs")
            first = last_id if first is None else first
            yield last_id, last_ordinal
        if position != len(encoded_membership) or first != self.first_revision_id or last_id != self.last_revision_id:
            raise FamilyBlockError("membership range or length differs")


def _membership(collection_slot, records):
    first_id, first_ordinal = records[0]
    consecutive = all(item == (first_id + index, first_ordinal + index) for index, item in enumerate(records))
    data = None
    if not consecutive:
        data = bytearray()
        last_id = last_ordinal = 0
        for revision_id, ordinal in records:
            data.extend(_pack_integer(revision_id - last_id))
            delta = ordinal - last_ordinal
            data.extend(_pack_integer((delta << 1) if delta >= 0 else ((-delta << 1) - 1)))
            last_id, last_ordinal = revision_id, ordinal
        data = bytes(data)
    cells = (collection_slot, first_id, records[-1][0], len(records), first_ordinal if consecutive else None, data)
    return RevisionMembership(*cells)


def iter_revision_memberships(collection_slot, records: Iterable, *, chunk_records):
    """Accept sorted actual (ID, global codec-record ordinal) pairs, never a reservation.

    ``chunk_records`` is the caller's memory/work budget, not a logical input cap.
    Root uses collection zero and record ordinal zero. Child ordinals start at one.
    Arbitrary original ID ordering becomes a bounded packed map, not one SQL row
    per child. The owner supplies the sorted view of the same verified records.
    """
    _integer(chunk_records, "membership chunk budget")
    _integer(collection_slot, "collection slot", zero=True)
    previous = 0
    pending_pairs = []
    for revision_id, ordinal in records:
        _integer(revision_id, "membership revision")
        _integer(ordinal, "membership ordinal", zero=True)
        if revision_id <= previous or (collection_slot == 0) != (ordinal == 0):
            raise FamilyBlockError("membership order or collection differs")
        previous = revision_id
        pending_pairs.append((revision_id, ordinal))
        if len(pending_pairs) == chunk_records:
            yield _membership(collection_slot, pending_pairs)
            pending_pairs = []
    if pending_pairs:
        yield _membership(collection_slot, pending_pairs)


def storage_envelope_sha256(manifest, fragment_directory, memberships, *, check=None):
    """Bind ordered fragment metadata and actual-ID maps to both existing hashes.

    This internal envelope extension does not recompute or rename semantic hashes.
    Fragment tuples are (ordinal, raw_bytes, first_record_ordinal,
    starting_record_count, first_record_offset, continuation_bytes). Memberships
    are in (collection_slot, first_revision_id) order. Full closure must compare
    the metadata with decoded records, independently of this commitment.
    """
    digest = hashlib.sha256(_DOMAIN + b"envelope\x00")

    def feed(value):
        """Retain the original length-prefixed MessagePack envelope framing."""
        data = msgpack.packb(value, use_bin_type=True)
        digest.update(struct.pack("!Q", len(data)))
        digest.update(data)

    feed(
        (
            manifest.definition_sha256,
            manifest.family_sha256,
            manifest.storage_sha256,
            manifest.retained,
            manifest.child_count,
            manifest.fragment_count,
            manifest.raw_bytes,
            manifest.stored_bytes,
        )
    )
    count = 0
    for ordinal, directory_entry in enumerate(fragment_directory):
        if check is not None:
            check()
        if directory_entry[0] != ordinal or len(directory_entry) != 6:
            raise FamilyBlockError("fragment directory order differs")
        feed(("fragment", *directory_entry))
        count += 1
    if count != manifest.fragment_count:
        raise FamilyBlockError("fragment directory count differs")
    previous = None
    count = record_count = 0
    for directory_entry in memberships:
        key = directory_entry.collection_slot, directory_entry.first_revision_id
        if previous is not None and key <= previous:
            raise FamilyBlockError("membership directory order differs")
        if directory_entry.record_count > manifest.child_count + 1 - record_count:
            raise FamilyBlockError("membership directory record count differs")
        # Exhaustion validates every packed integer and endpoint without retaining IDs.
        for _ in directory_entry.records():
            if check is not None:
                check()
            record_count += 1
        feed(("membership", *directory_entry.cells()))
        previous = key
        count += 1
    if record_count != manifest.child_count + 1:
        raise FamilyBlockError("membership directory record count differs")
    feed(("complete", count, record_count))
    return digest.digest(), count
