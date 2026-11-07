# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded byte batches for the two route sorts; no file authority enters Rust."""

import heapq
import importlib
import inspect
import os
import stat
import struct
import sys
from collections import deque
from compression import zstd
from contextlib import ExitStack
from dataclasses import dataclass
from functools import lru_cache

from process.custom_import import family_shard_facts as facts

_ORDER = struct.Struct(">Q")
_COMPRESSED = b"\x93\xb0sort-run-zstd/v1"


def _has_pinned_decoder():
    """The source-derived allocation bound applies only to this exact ABI."""
    return (
        sys.implementation.name == "cpython"
        and sys.version_info[:3] == (3, 14, 7)
        and zstd.zstd_version_info == (1, 5, 7)
        and struct.calcsize("Pli") == 20
        and struct.calcsize("P") == struct.calcsize("l") == 8
        and struct.calcsize("i") == 4
    )


def _workspace(window_log):
    if not _has_pinned_decoder():
        return None
    window = 1 << window_log
    # Pinned DCtx bound plus the upstream DStream estimator's three buffers.
    return 1024**2 + window + 3 * min(window, 128 * 1024) + 64 + 16 * 1024


def _is_standard_frame(frame):
    # A fresh dictionary-free modern frame avoids legacy/dictionary contexts.
    return len(frame) >= 5 and frame[:4] == b"\x28\xb5\x2f\xfd" and not frame[4] & 3


@lru_cache(maxsize=1)
def backend():
    """Use only the compiled optional bytes ABI, leaving other sorts unchanged."""
    try:
        module = importlib.import_module("ptg2_address_canon")
    except ImportError:
        return None
    functions = tuple(
        getattr(module, name, None) for name in ("custom_import_route_sort_v1", "custom_import_route_merge_v1")
    )
    return functions if all(inspect.isbuiltin(function) for function in functions) else None


def _check(work):
    check = work._io_check or getattr(work, "current", None)
    if check is not None:
        check()


def key_bytes(order):
    """Account the existing Python key held alongside an immutable packed row."""
    return sys.getsizeof(order) + (sum(key_bytes(cell) for cell in order) if type(order) is tuple else 0)


def _stamp(observed):
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mode,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
        observed.st_uid,
        observed.st_nlink,
    )


def _root(work):
    observed = work.root.lstat()
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o700
        or observed.st_uid != work.identity.st_uid
        or (observed.st_dev, observed.st_ino) != (work.identity.st_dev, work.identity.st_ino)
    ):
        raise ValueError("route work directory custody differs")


class _Output:
    def __init__(self, work):
        self.work, self.path = work, work.path()
        _root(work)
        self.fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self.identity = os.fstat(self.fd)

    def _reserve(self, size):
        _check(self.work)
        _root(self.work)
        observed = self.path.lstat()
        if (
            (observed.st_dev, observed.st_ino) != (self.identity.st_dev, self.identity.st_ino)
            or stat.S_IMODE(observed.st_mode) != 0o600
            or observed.st_uid != self.identity.st_uid
            or observed.st_nlink != 1
        ):
            raise ValueError("route output custody differs")
        self.work.charge(size)
        self.work.paths[self.path] += size

    def _write(self, payload):
        remaining = memoryview(payload)
        while remaining:
            _check(self.work)
            written = os.write(self.fd, remaining)
            if written <= 0:
                raise OSError("route output write made no progress")
            remaining = remaining[written:]

    def write(self, payload):
        """Charge a complete native prefix before any partial write."""
        self._reserve(len(payload))
        self._write(payload)

    def frame(self, payload):
        """Write one bounded stored frame through the existing disk charge."""
        if not 0 < len(payload) <= self.work.limits.max_record_bytes:
            raise facts.SourceFactBudgetExceeded("source fact singleton exceeds the resource budget")
        self._reserve(facts._LENGTH.size + len(payload))
        self._write(facts._LENGTH.pack(len(payload)))
        self._write(payload)

    def close(self, primary=None):
        """Close and remove only this output on failure, preserving its cause."""
        try:
            os.close(self.fd)
        except BaseException as cleanup:
            if primary is None:
                raise
            primary.add_note(f"route output close also failed: {type(cleanup).__name__}")
            primary.__cause__ = cleanup
            return
        if primary is not None:
            try:
                _root(self.work)
                observed = self.path.lstat()
                if (observed.st_dev, observed.st_ino) != (self.identity.st_dev, self.identity.st_ino):
                    raise ValueError("route failed output custody differs")
                self.work.remove(self.path)
            except BaseException as cleanup:
                primary.add_note(f"route output cleanup also failed: {type(cleanup).__name__}")
                primary.__cause__ = cleanup


def _stored_raw(work, raw):
    _check(work)
    if not 0 < len(raw) <= work.limits.max_record_bytes:
        raise facts.SourceFactBudgetExceeded("source fact singleton exceeds the resource budget")
    compressed = zstd.compress(
        raw, options={zstd.CompressionParameter.compression_level: 3, zstd.CompressionParameter.checksum_flag: True}
    )
    encoded = facts._pack((facts._COMPRESSED_SORT, len(raw), compressed))
    _check(work)
    return encoded if len(encoded) < len(raw) else raw


def write_initial(work, batch, mode, sort):
    """Keep the exact admitted raw frame through native sort and compression."""
    _check(work)
    raw_bytes = sum(len(raw) for _order, raw in batch)
    metadata = 1024 + sum(key_bytes(order) + 512 for order, _raw in batch)
    largest = max((len(raw) for _order, raw in batch), default=0)
    if max(2 * raw_bytes, raw_bytes + 2 * largest) + metadata > work.limits.memory_bytes:
        batch.sort(key=lambda row: row[0])
        return facts._write_sort_run(work, ((order, facts._unpack(raw)[1]) for order, raw in batch))
    frames = tuple(raw for _order, raw in batch)
    encoded = sort(mode, frames, work.limits.memory_bytes, work.limits.max_record_bytes)
    _check(work)
    if encoded is None:
        batch.sort(key=lambda row: row[0])
        return facts._write_sort_run(work, ((order, facts._unpack(raw)[1]) for order, raw in batch))
    else:
        if type(encoded) is not bytes or len(encoded) != len(batch) * _ORDER.size:
            raise ValueError("route sort permutation differs")
        indices = tuple(index for (index,) in _ORDER.iter_unpack(encoded))
        if len(set(indices)) != len(batch) or any(index >= len(batch) for index in indices):
            raise ValueError("route sort permutation differs")
        ordered_frames = (batch[index][1] for index in indices)
    output, primary = _Output(work), None
    try:
        for raw in ordered_frames:
            output.frame(_stored_raw(work, raw))
    except BaseException as failure:
        primary = failure
        raise
    finally:
        output.close(primary)
    return output.path


class _Budget:
    def __init__(self, limit, lanes):
        self.limit, self.used, self.peak = limit, lanes * 1024, lanes * 1024

    def can_reserve(self, size):
        """Reserve the full transient envelope only when it fits this batch."""
        if size > self.limit - self.used:
            return False
        self.used += size
        self.peak = max(self.peak, self.used)
        return True

    def release(self, size):
        """Release bytes only after the associated buffers have been dropped."""
        self.used -= size


class _LegacyEnvelope(Exception):
    """A valid older encoding must retain the existing Python decoder."""


def _uint(payload, offset):
    """Read only the writer's canonical positive MessagePack integer header."""
    if offset >= len(payload):
        raise ValueError("route compressed envelope is truncated")
    tag = payload[offset]
    if tag < 128:
        return tag, offset + 1
    width_by_tag = {0xCC: 1, 0xCD: 2, 0xCE: 4, 0xCF: 8}
    width = width_by_tag.get(tag)
    if width is None:
        raise _LegacyEnvelope
    if offset + 1 + width > len(payload):
        raise ValueError("route compressed envelope integer differs")
    value = int.from_bytes(payload[offset + 1 : offset + 1 + width], "big")
    return value, offset + 1 + width


def _compressed(payload):
    raw_size, offset = _uint(payload, len(_COMPRESSED))
    if offset >= len(payload):
        raise ValueError("route compressed envelope is truncated")
    width = {0xC4: 1, 0xC5: 2, 0xC6: 4}.get(payload[offset])
    if width is None:
        raise _LegacyEnvelope
    if offset + 1 + width > len(payload):
        raise ValueError("route compressed envelope bytes differ")
    size = int.from_bytes(payload[offset + 1 : offset + 1 + width], "big")
    offset += 1 + width
    if size != len(payload) - offset:
        raise ValueError("route compressed envelope EOF differs")
    return raw_size, payload[offset:]


@dataclass(frozen=True)
class _Frame:
    start: int
    stored: bytes
    raw: bytes | None
    charge: int


class _Reader:
    def __init__(self, work, path):
        _root(work)
        if path not in work.paths or path.parent != work.root:
            raise ValueError("route input is outside registered work")
        observed = path.lstat()
        if (
            not stat.S_ISREG(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) not in {0o400, 0o600, 0o644}
            or observed.st_nlink != 1
            or observed.st_uid != work.identity.st_uid
            or observed.st_size != work.paths[path]
        ):
            raise ValueError("route input custody differs")
        self.work, self.path, self.identity = work, path, _stamp(observed)
        self.stream = os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb", buffering=0)
        self.pending, self.eof, self.frames = None, False, deque()
        try:
            self.current()
        except BaseException:
            self.stream.close()
            raise

    def current(self):
        """Recheck the held descriptor and registered path before using bytes."""
        _check(self.work)
        _root(self.work)
        if _stamp(self.path.lstat()) != self.identity or _stamp(os.fstat(self.stream.fileno())) != self.identity:
            raise ValueError("route input changed during merge")

    def _prefix(self):
        start, prefix = self.stream.tell(), self.stream.read(facts._LENGTH.size)
        if not prefix:
            self.current()
            self.eof = True
            return None
        if len(prefix) != facts._LENGTH.size:
            raise ValueError("source fact spool prefix is truncated")
        size = facts._LENGTH.unpack(prefix)[0]
        if not 0 < size <= self.work.limits.max_record_bytes:
            raise facts.SourceFactBudgetExceeded("source fact spool frame exceeds its resource budget")
        return start, prefix, size

    def read(self, budget):
        """Admit stored/raw/decoder copies before filling one immutable frame."""
        _check(self.work)
        if self.pending is None:
            self.pending = self._prefix()
            if self.pending is None:
                return "eof"
        start, prefix, size = self.pending
        charge = 5 * (size + facts._LENGTH.size) + 1024
        if not budget.can_reserve(charge):
            return "budget"
        stored_view = self.stream.read(size)
        if len(stored_view) != size:
            raise ValueError("source fact spool record is truncated")
        stored, raw = prefix + stored_view, None
        stored_view = memoryview(stored)[facts._LENGTH.size :]
        if stored_view[: len(_COMPRESSED)] == _COMPRESSED:
            try:
                parts = _compressed(stored_view)
            except _LegacyEnvelope:
                parts = None
            if parts is None:
                self.pending = None
                self.frames.append(_Frame(start, stored, None, charge))
                return "fallback"
            raw_size, compressed = parts
            if not 0 < raw_size <= self.work.limits.max_record_bytes:
                raise ValueError("source fact compressed sort bounds differ")
            window = min(31, max(10, (raw_size - 1).bit_length()))
            workspace = _workspace(window) if raw_size <= 64 * 1024**2 and _is_standard_frame(compressed) else None
            if workspace is None:
                self.pending = None
                self.frames.append(_Frame(start, stored, None, charge))
                return "fallback"
            raw_charge = 2 * (raw_size + 1)
            if budget.can_reserve(raw_charge + workspace):
                charge += raw_charge
                try:
                    raw = facts._decode_sort_frame(self.work, raw_size, compressed)
                    _check(self.work)
                finally:
                    budget.release(workspace)
            else:
                self.stream.seek(start)
                self.pending = None
                budget.release(charge)
                return "budget"
        elif stored_view[0] == 0x92:
            if not budget.can_reserve(2 * size):
                self.stream.seek(start)
                self.pending = None
                budget.release(charge)
                return "budget"
            charge += 2 * size
            raw = bytes(stored_view)
        self.pending = None
        self.frames.append(_Frame(start, stored, raw, charge))
        return "ready" if raw is not None else "fallback"

    def rewind(self, budget):
        """Return this same descriptor to the first unconsumed frame."""
        self.current()
        start = (
            self.frames[0].start if self.frames else self.pending[0] if self.pending is not None else self.stream.tell()
        )
        self.stream.seek(start)
        while self.frames:
            budget.release(self.frames.popleft().charge)
        self.pending, self.eof = None, False

    def rows(self):
        """Drain remaining frames through the unchanged legacy decoder."""
        while prefix := self.stream.read(facts._LENGTH.size):
            _check(self.work)
            if len(prefix) != facts._LENGTH.size:
                raise ValueError("source fact spool prefix is truncated")
            size = facts._LENGTH.unpack(prefix)[0]
            if not 0 < size <= self.work.limits.max_record_bytes:
                raise facts.SourceFactBudgetExceeded("source fact spool frame exceeds its resource budget")
            payload = self.stream.read(size)
            if len(payload) != size:
                raise ValueError("source fact spool record is truncated")
            yield facts._decoded_sort_entry(self.work, facts._unpack(payload))
        self.current()


def _fill(readers, budget):
    for reader in readers:
        if not reader.frames and not reader.eof:
            result = reader.read(budget)
            if result in {"fallback", "budget"}:
                return False
    blocked_lanes = set()
    while len(blocked_lanes) < len(readers):
        for lane, reader in enumerate(readers):
            if lane in blocked_lanes:
                continue
            result = reader.read(budget) if not reader.eof else "eof"
            if result == "fallback":
                return False
            if result in {"budget", "eof"}:
                blocked_lanes.add(lane)
    return all(reader.frames or reader.eof for reader in readers)


def _legacy(work, readers, budget, output):
    for reader in readers:
        reader.rewind(budget)
    rows = [reader.rows() for reader in readers]
    try:
        for value in heapq.merge(*rows, key=lambda value: value[0]):
            output.frame(facts._pack(facts._sort_entry(work, value)))
    finally:
        for values in rows:
            values.close()


def _checked_prefix(prefix, readers):
    chunk, consumed = prefix
    if type(chunk) is not bytes or type(consumed) is not tuple or len(consumed) != len(readers):
        raise ValueError("route merge prefix differs")
    if any(type(count) is not int or not 0 <= count <= len(reader.frames) for reader, count in zip(readers, consumed)):
        raise ValueError("route merge consumption differs")
    expected = sum(
        len(frame.stored) for reader, count in zip(readers, consumed) for frame in list(reader.frames)[:count]
    )
    if len(chunk) != expected or not chunk:
        raise facts.SourceFactBudgetExceeded("route merge made no bounded progress")
    return chunk, consumed


def _consume(readers, consumed, budget):
    for reader, count in zip(readers, consumed):
        for _ in range(count):
            budget.release(reader.frames.popleft().charge)


def write_merged(work, paths, mode, merge):
    """Retain each lane suffix and stop at every unknown refill frontier."""
    if len(paths) * 1024 > work.limits.memory_bytes:
        return facts._write_sort_run(work, facts._merged(work, paths))
    budget, primary = _Budget(work.limits.memory_bytes, len(paths)), None
    output = _Output(work)
    try:
        with ExitStack() as stack:
            readers = []
            for path in paths:
                reader = _Reader(work, path)
                stack.callback(reader.stream.close)
                readers.append(reader)
            while any(not reader.eof or reader.frames for reader in readers):
                if not _fill(readers, budget):
                    _legacy(work, readers, budget, output)
                    break
                if all(reader.eof and not reader.frames for reader in readers):
                    break
                for reader in readers:
                    reader.current()
                windows = tuple(
                    (tuple((frame.raw, frame.stored) for frame in reader.frames), reader.eof) for reader in readers
                )
                prefix = merge(
                    mode, windows, work.limits.memory_bytes, work.limits.max_record_bytes, work.remaining_bytes
                )
                del windows
                _check(work)
                for reader in readers:
                    reader.current()
                if prefix is None:
                    _legacy(work, readers, budget, output)
                    break
                chunk, consumed = _checked_prefix(prefix, readers)
                output.write(chunk)
                _consume(readers, consumed, budget)
                del prefix, chunk, consumed
    except BaseException as failure:
        primary = failure
        raise
    finally:
        output.close(primary)
    return output.path
