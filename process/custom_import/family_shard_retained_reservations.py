# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded owner windows and actual native-ID FIFOs for retained allocation."""

import asyncio
import time
from collections import Counter
from contextlib import ExitStack, closing

from sqlalchemy import text

from process.custom_import.build_source import (
    _WINDOW,
    _close_contexts,
    _enter_context,
    _PageWindow,
    _prepare_statement,
)
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_validate import require_readonly_snapshot
from process.custom_import.family_shard_base import _require_deadline
from process.custom_import.family_shard_work_cost import measure_work_pages
from process.custom_import.read_identity import _is_same_read_transaction, _read_transaction_identity

_KINDS = ("family_revision", "root_revision", "child_revision", "pack")
_ID_BYTES = 96


class RetainedControlWindow:
    """A scheduling bound, never COPY, acceptance or publication authority."""

    def __init__(self, work, limits, observe):
        self.work, self.limits, self.observe = work, limits, observe
        self.saved = None
        self.rows = self.byte_count = 0
        self.refresh_at = 0.0

    def remembered(self, window):
        """Publish an owner window only after its transaction committed."""
        if type(window) is not _PageWindow:
            raise FamilyBlockUnavailable("retained owner window is unavailable")
        _require_deadline(window.deadline)
        self.saved = window
        self.rows = self.byte_count = 0
        now = time.monotonic()
        self.refresh_at = min(now + 1.0, now + (window.deadline - now) / 2)

    async def reserve(self, kinds, *, cursor=()):
        """Obtain actual native IDs within one fresh owner transaction."""
        self.work.current()
        identities = await self.observe(kinds, self.remembered, cursor=cursor)
        if self.saved is None:
            raise FamilyBlockUnavailable("retained allocation omitted its owner window")
        self.work.current()
        return identities

    async def checkpoint(self, *, rows=0, byte_count=0, force=False):
        """Refresh authority at bounded work or elapsed-time windows."""
        self.work.current()
        if (
            force
            or self.saved is None
            or time.monotonic() >= self.refresh_at
            or self.rows + rows > self.limits.maximum_batch_records
            or self.byte_count + byte_count > self.limits.maximum_batch_bytes
        ):
            await self.reserve(())
        self.rows += rows
        self.byte_count += byte_count

    async def cost(self, factory, pages, limits):
        """Measure native costs under the last committed owner deadline."""
        await self.checkpoint()
        saved = self.saved
        async with asyncio.timeout_at(saved.deadline):
            return await self._cost(factory, pages, limits, saved)

    async def _cost(self, factory, pages, limits, saved):
        contexts, primary, measured = [], None, None
        try:
            session = await _enter_context(contexts, factory())
            if session.in_transaction() or session.new or session.dirty or session.deleted:
                raise FamilyBlockUnavailable("retained cost requires a clean owned session")
            await _enter_context(contexts, session.begin())
            session.info[_WINDOW] = saved
            _require_deadline(saved.deadline)
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            transactions = await _read_transaction_identity(session)

            async def prepare(current):
                """Keep the read transaction and saved deadline fixed."""
                self.work.current()
                _require_deadline(saved.deadline)
                if current is not session or not _is_same_read_transaction(
                    transactions, await _read_transaction_identity(session)
                ):
                    raise FamilyBlockUnavailable("retained cost read transaction changed")
                await _prepare_statement(session)
                _require_deadline(saved.deadline)

            await prepare(session)
            await require_readonly_snapshot(session)
            measured = await measure_work_pages(session, pages, limits=limits, prepare=prepare)
            await prepare(session)
            await require_readonly_snapshot(session)
            _require_deadline(saved.deadline)
        except BaseException as error:
            primary = error
        primary = await _close_contexts(contexts, primary)
        if primary is not None:
            raise primary
        self.work.current()
        _require_deadline(saved.deadline)
        return measured


class _Queue:
    def __init__(self, work):
        self.work, self.path = work, work.path()
        self.file = self.path.open("x+b", buffering=0)
        self.offset = self.reserved = self.taken = 0

    @property
    def available(self):
        """Return idle observations without loading their IDs."""
        return self.reserved - self.taken

    def append(self, identity):
        """Spool one actual reservation in per-kind acquisition order."""
        self.file.seek(0, 2)
        self.work.frame(self, (self.reserved, identity), self.path)
        self.reserved += 1

    def write(self, data):
        """Complete unbuffered writes used by the accounted frame writer."""
        view = memoryview(data)
        while view:
            written = self.file.write(view)
            if not written:
                raise OSError("retained reservation spool write made no progress")
            view = view[written:]
        return len(data)

    def pop(self):
        """Consume the next actual ID without recycling failed attempts."""
        self.file.seek(self.offset)
        row = next(self.work.read(self.file), None)
        if row is None or row[0] != self.taken:
            raise FamilyBlockUnavailable("retained reservation FIFO ended early or changed order")
        self.offset = self.file.tell()
        self.taken += 1
        return row[1]


class RetainedReservations:
    """One invocation-lifetime FIFO per kind; roots and children share pack IDs."""

    def __init__(self, work, limits, control):
        self.work, self.limits, self.control = work, limits, control
        self.queues, self.quotas = {}, dict.fromkeys(_KINDS, 0)
        self.capacity = min(limits.maximum_batch_records, limits.maximum_batch_bytes // _ID_BYTES)

    def __enter__(self):
        try:
            for kind in _KINDS:
                self.queues[kind] = _Queue(self.work)
        except BaseException:
            self.__exit__()
            raise
        return self

    def __exit__(self, *_error):
        for queue in self.queues.values():
            queue.file.close()

    def _requests(self, kinds, held_bytes):
        needed = Counter(kinds)
        if set(needed) - set(_KINDS):
            raise ValueError("retained reservation kind differs")
        memory = max(0, (self.work.limits.memory_bytes - held_bytes) // _ID_BYTES)
        capacity = min(self.capacity, memory)
        if len(kinds) > capacity:
            raise FamilyBlockResourceLimit("retained attempt and original values exceed the owner buffer")
        missing_by_kind = {kind: max(0, count - self.queues[kind].available) for kind, count in needed.items()}
        unused = sum(max(0, queue.available - needed[kind]) for kind, queue in self.queues.items())
        spare = min(max(0, self.capacity - unused), capacity - sum(missing_by_kind.values()))
        requests_by_kind = {}
        for kind, count in missing_by_kind.items():
            if count:
                extra = min(spare, max(0, self.quotas[kind] * 2 - count))
                requests_by_kind[kind] = count + extra
                spare -= extra
        return requests_by_kind

    async def take(self, kinds, *, held_bytes=0):
        """Consume exact FIFO demand, refilling only bounded missing vectors."""
        if type(kinds) is not tuple:
            raise ValueError("retained demand requires exact ordered kinds")
        self.work.current()
        requests_by_kind = self._requests(kinds, held_bytes)
        if requests_by_kind:
            requested_kinds = tuple(kind for kind, count in requests_by_kind.items() for _ in range(count))
            cursor = (
                kinds,
                held_bytes,
                tuple(
                    (kind, self.queues[kind].reserved, self.queues[kind].taken, self.quotas[kind]) for kind in _KINDS
                ),
            )
            observed = await self.control.reserve(requested_kinds, cursor=cursor)
            if len(observed) != len(requested_kinds):
                raise FamilyBlockUnavailable("retained reservation count differs")
            for kind, identity in zip(requested_kinds, observed, strict=True):
                if type(identity) is not int or not 0 < identity < 2**63:
                    raise FamilyBlockUnavailable("retained reservation identity differs")
                self.queues[kind].append(identity)
            for kind, count in requests_by_kind.items():
                self.quotas[kind] = count
            del observed
        else:
            await self.control.checkpoint()
        return tuple(self.queues[kind].pop() for kind in kinds)

    async def finish(self, output):
        """Dispose terminal tails, then prove every actual observation exactly once."""
        batch_size = max(1, min(self.capacity, self.work.limits.memory_bytes // _ID_BYTES, 4096))
        for kind, queue in self.queues.items():
            while queue.available:
                self.work.current()
                identities = tuple(queue.pop() for _ in range(min(queue.available, batch_size)))
                output.observed((kind,) * len(identities), identities, False)
                await self.control.checkpoint(rows=len(identities), byte_count=_ID_BYTES * len(identities))
        output.files["allocation"].flush()
        with ExitStack() as stack:
            streams_by_kind = {
                kind: stack.enter_context(closing(self.work.rows(queue.path))) for kind, queue in self.queues.items()
            }
            counts = dict.fromkeys(_KINDS, 0)
            batch_count = 0
            with closing(self.work.rows(output.paths["allocation"])) as dispositions:
                for _attempt, _ordinal, kind, identity, _accepted in dispositions:
                    if kind not in streams_by_kind or next(streams_by_kind[kind], None) != (counts[kind], identity):
                        raise FamilyBlockUnavailable("retained reservation/disposition EOF differs")
                    counts[kind] += 1
                    batch_count += 1
                    if batch_count == batch_size:
                        await self.control.checkpoint(rows=batch_count, byte_count=_ID_BYTES * batch_count)
                        batch_count = 0
            if batch_count:
                await self.control.checkpoint(rows=batch_count, byte_count=_ID_BYTES * batch_count)
            if any(
                next(streams_by_kind[kind], None) is not None or counts[kind] != queue.reserved
                for kind, queue in self.queues.items()
            ):
                raise FamilyBlockUnavailable("retained reservations lack complete dispositions")
        for queue in self.queues.values():
            queue.file.close()
            self.work.remove(queue.path)
