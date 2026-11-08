# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable bounded GRAPH preparation; committed database receipts remain authority."""

from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass
from tempfile import TemporaryFile

from msgpack import UnpackException, packb, unpackb

from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.source_preparation import MAX_INPUT_BYTES, MAX_ROWS, _has_allocation_failure

_HEADER = struct.Struct("!4sQII32s")
_CHILD_FIELDS = (
    "child_revision_id",
    "collection_slot",
    "canonical_parent_key",
    "parent_key_sha256",
    "canonical_child_key",
    "child_key_sha256",
    "canonical_payload",
    "payload_sha256",
)


def _key(record):
    return record[0], record[1], record[4], record[3]


def _validate_record(record):
    if (
        type(record) is not tuple
        or len(record) != 8
        or any(type(record[index]) is not int or record[index] <= 0 for index in (0, 2, 3, 5))
        or type(record[1]) is not str
        or not 1 <= len(record[1]) <= 63
        or type(record[4]) is not bytes
        or len(record[4]) != 32
        or (record[6] is None) == (record[7] is None)
    ):
        raise CandidateRunnerError("prepared child record differs")
    fallback = record[7]
    if fallback is not None:
        if (
            type(fallback) is not tuple
            or len(fallback) != len(_CHILD_FIELDS)
            or any(type(value) is not int for value in fallback[:2])
            or any(type(fallback[index]) is not str for index in (2, 4, 6))
            or any(type(fallback[index]) is not bytes or len(fallback[index]) != 32 for index in (3, 5, 7))
            or (fallback[1], fallback[0], fallback[5]) != (record[2], record[3], record[4])
        ):
            raise CandidateRunnerError("prepared child fallback differs")


@dataclass(frozen=True)
class PreparedChild:
    child_revision_id: int
    collection_slot: int
    child_key_sha256: bytes
    byte_count: int
    position: tuple[int, int]
    stream: PreparedChildren

    def record(self):
        """Load this validated row without retaining another frame."""
        return self.stream.frame(self.position[0])[0][self.position[1]]


class PreparedChildren:
    """One attempt-local scratch stream, never opened by pathname or resumed."""

    def __init__(self):
        self.file = None
        self.enabled = True
        self.byte_count = 0
        self.row_count = 0
        self.chunk_count = 0
        self.pending = []
        self.pending_bytes = 0
        self.position = 0
        self.last_key = None
        self.cached_frame = None
        self.tips_verified = False

    def close(self):
        """Release all owned scratch and buffered rows on every exit path."""
        self.pending.clear()
        self.cached_frame = None
        if self.file is not None:
            self.file.close()
            self.file = None

    def disable(self):
        """Discard the optimization before falling back to canonical reads."""
        self.enabled = False
        self.close()

    def append(self, root_id, collection, child, byte_count, columns):
        """Buffer one primitive row; planned quota overflow disables this partition."""
        if not self.enabled:
            return
        # A single wide fallback must keep the original streaming path.
        if columns is None and byte_count > MAX_INPUT_BYTES // 2:
            self.disable()
            return
        fallback = None if columns is not None else tuple(getattr(child, name) for name in _CHILD_FIELDS)
        record = (
            root_id,
            collection,
            child.collection_slot,
            child.child_revision_id,
            bytes(child.child_key_sha256),
            byte_count,
            columns,
            fallback,
        )
        size = len(packb(record, use_bin_type=True))
        if size + 16 > MAX_INPUT_BYTES:
            self.disable()
            return
        if self.pending and (len(self.pending) == MAX_ROWS or self.pending_bytes + size + 16 > MAX_INPUT_BYTES):
            self.flush()
        if self.enabled:
            self.pending.append(record)
            self.pending_bytes += size
            self.row_count += 1

    def flush(self):
        """Write one bounded checksummed chunk; actual I/O errors abort."""
        if not self.pending or not self.enabled:
            return
        payload = packb(tuple(self.pending), use_bin_type=True)
        if len(payload) > MAX_INPUT_BYTES or self.byte_count + _HEADER.size + len(payload) > MAX_BATCH_BYTES:
            self.disable()
            return
        if self.file is None:
            self.file = TemporaryFile(mode="w+b")
        count = len(self.pending)
        prefix = struct.pack("!QI", self.chunk_count, count)
        header = _HEADER.pack(b"CGP1", self.chunk_count, len(payload), count, hashlib.sha256(prefix + payload).digest())
        if self.file.write(header) != len(header) or self.file.write(payload) != len(payload):
            raise OSError("prepared child scratch write was incomplete")
        self.byte_count += len(header) + len(payload)
        self.chunk_count += 1
        self.pending.clear()
        self.pending_bytes = 0

    def finish(self):
        """Complete the canonical partition before any root write begins."""
        self.flush()
        if self.file is not None:
            self.file.flush()

    def frame(self, position):
        """Read and validate one bounded frame, retaining at most that frame."""
        if self.cached_frame is not None and self.cached_frame[0] == position:
            return self.cached_frame[1:]
        if self.file is None or not 0 <= position < self.byte_count:
            raise CandidateRunnerError("prepared child frame is missing")
        self.file.seek(position)
        header = self.file.read(_HEADER.size)
        if len(header) != _HEADER.size:
            raise CandidateRunnerError("prepared child frame is truncated")
        magic, ordinal, length, count, digest = _HEADER.unpack(header)
        if (
            magic != b"CGP1"
            or ordinal >= self.chunk_count
            or not 1 <= length <= MAX_INPUT_BYTES
            or not 1 <= count <= MAX_ROWS
        ):
            raise CandidateRunnerError("prepared child frame bound differs")
        encoded_rows = self.file.read(length)
        if (
            len(encoded_rows) != length
            or hashlib.sha256(struct.pack("!QI", ordinal, count) + encoded_rows).digest() != digest
        ):
            raise CandidateRunnerError("prepared child frame integrity differs")
        try:
            child_records = unpackb(encoded_rows, raw=False, use_list=False, max_ext_len=0)
        except (UnpackException, ValueError, TypeError) as error:
            if _has_allocation_failure(error):
                raise
            raise CandidateRunnerError("prepared child frame encoding differs") from error
        if type(child_records) is not tuple or len(child_records) != count:
            raise CandidateRunnerError("prepared child frame prefix differs")
        previous = None
        for child_record in child_records:
            _validate_record(child_record)
            if previous is not None and _key(child_record) <= previous:
                raise CandidateRunnerError("prepared children are not in source order")
            previous = _key(child_record)
        next_position = position + _HEADER.size + length
        self.cached_frame = position, child_records, next_position, ordinal
        return child_records, next_position, ordinal

    def records(self, position=0, *, check_budget=None):
        """Stream ordered rows under the caller's current read deadline."""
        previous = None
        previous_ordinal = None
        while position < self.byte_count:
            if check_budget is not None:
                check_budget()
            records, next_position, ordinal = self.frame(position)
            if previous_ordinal is not None and ordinal != previous_ordinal + 1:
                raise CandidateRunnerError("prepared child frame order differs")
            for index, record in enumerate(records):
                if check_budget is not None:
                    check_budget()
                if previous is not None and _key(record) <= previous:
                    raise CandidateRunnerError("prepared children are not in source order")
                previous = _key(record)
                yield position, index, record
            position, previous_ordinal = next_position, ordinal

    def rows(self, metadata, *, check_budget=None):
        """Match fresh admitted metadata and retain only bounded frame locators."""
        if not metadata:
            return ()
        first_keys = tuple(metadata[0][:-1])
        if self.last_key is not None and first_keys <= self.last_key:
            self.position = 0
        pending = iter(metadata)
        expected = next(pending)
        rows = []
        for position, index, record in self.records(self.position, check_budget=check_budget):
            key = _key(record)
            if key < tuple(expected[:-1]):
                continue
            if key != tuple(expected[:-1]) or record[5] != expected[-1]:
                raise CandidateRunnerError("prepared child identity or byte charge differs")
            child = PreparedChild(record[3], record[2], record[4], record[5], (position, index), self)
            rows.append((record[0], child, True))
            self.position, self.last_key = position, key
            expected = next(pending, None)
            if expected is None:
                return tuple(rows)
        raise CandidateRunnerError("prepared child source prefix is incomplete")

    def verify_tips(self, states, collection_names, *, check_budget=None):
        """Check skipped rows against fresh durable receipts, never scratch authority."""
        resumed_by_root_id = {
            state.current.root_record_id: state.current
            for state in states
            if state.current.selection_kind == "source" and state.current.attached_child_count
        }
        if not resumed_by_root_id:
            return
        counts = dict.fromkeys(resumed_by_root_id, 0)
        checked_roots = set()
        for _position, _index, child_record in self.records(check_budget=check_budget):
            root_id = child_record[0]
            if root_id not in resumed_by_root_id or root_id in checked_roots:
                continue
            counts[root_id] += 1
            tip = resumed_by_root_id[root_id]
            if counts[root_id] == tip.attached_child_count:
                expected = (
                    root_id,
                    collection_names[tip.last_child_collection_slot],
                    tip.last_child_key_sha256,
                    tip.last_input_child_revision_id,
                )
                if _key(child_record) != expected:
                    raise CandidateRunnerError("prepared child prefix differs from durable progress")
                checked_roots.add(root_id)
                if checked_roots == resumed_by_root_id.keys():
                    return
        if checked_roots != resumed_by_root_id.keys():
            raise CandidateRunnerError("prepared child prefix is shorter than durable progress")
