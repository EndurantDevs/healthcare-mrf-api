# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained native vectors under the original live SOURCE checkpoint owner.

Files never prove commit. Reconcile native heads before replay/exposure and keep
every provisional file with the live journal until terminal owner cleanup.
"""

import hashlib
import os
from collections import deque
from contextlib import closing, contextmanager
from dataclasses import replace
from itertools import batched, groupby
from operator import itemgetter
from types import SimpleNamespace
from uuid import uuid4

from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_events as events
from process.custom_import import family_shard_events_journal as event_journal
from process.custom_import import family_shard_metadata_journal as metadata_journal
from process.custom_import import family_shard_source_journal as source_journal
from process.custom_import.build_source import _WINDOW, _page_session
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_records import _integer
from process.custom_import.family_shard_base import _read_deadline, _require_deadline
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_ids import _allocate_record_ids
from process.custom_import.family_shard_plan import PlanWork, _cpu
from process.custom_import.family_shard_source_plan import _plan_digest

_VECTORS = "custom-import/retained-native-vectors/v1"
_COMPLETE = "custom-import/retained-native-complete/v1"
_pack = metadata_journal._pack


class _Prefix:
    def __init__(self, run, snapshot):
        self.run, self.snapshot, self.head = run, snapshot, snapshot.head

    def records(self, *, check=None):
        """Replay only the validated original retained prefix records."""
        from process.custom_import.family_shard_retained_checkpoint import SourcePrefix

        yield from SourcePrefix(self.snapshot).records(check=check)


def _original_prefix(run, snapshot):
    """Bind already completed phase digests; never decode SOURCE values again."""
    context = run.context
    original = context.events
    scope = events._scope(run.request, run.layout, run.source, context.admission)
    if type(original) is not events.AdmissionEventsDescriptor or original.scope != scope:
        raise FamilyBlockUnavailable("retained journal requires original completed admission events")
    stamp = original.owner_created_at
    event_scope = hashlib.sha256(_pack(scope)).digest()
    prior = _Prefix(run, snapshot)
    event_run = SimpleNamespace(
        source=run.source,
        journal=run.journal,
        limits=run.limits,
        work=run.work,
        commitments=(
            ("admission_events", event_scope),
            ("source_allocations", run.source.sha256),
            ("source_plan", run.source.source.sha256),
        ),
    )
    seen = 0
    for retained_row in event_journal._event_records(event_run, prior):
        retained_record = checkpoint.OwnerCheckpointRecord(*retained_row[1:])
        tag, actual_stamp, _identifiers = event_journal._decode(event_run, retained_record)
        if actual_stamp != stamp:
            raise FamilyBlockUnavailable("retained admission original timestamp differs")
        if seen == 0:
            expected = hashlib.sha256(
                _pack(
                    (
                        event_journal._CONTRACT,
                        event_scope,
                        retained_record.ordinal,
                        "complete",
                        (original.sha256, original.counts),
                    )
                )
            ).digest()
            if tag != "complete" or retained_record.input_sha256 != expected:
                raise FamilyBlockUnavailable("retained journal admission completion differs")
        elif tag != ("start" if retained_record.ordinal == 0 else "batch"):
            raise FamilyBlockUnavailable("retained admission prefix lacks exact complete EOF")
        seen += 1
    if seen < 2:
        raise FamilyBlockUnavailable("retained journal requires completed admission prefix")
    # The native head already protects these original vectors. The existing
    # classifier checks their complete control EOF without another value replay.
    deque(metadata_journal._checked_records(run, event_journal._MetadataPrefix(event_run, prior)), maxlen=0)


@contextmanager
def _records(run, snapshot):
    scratch = source_journal.SourceAllocationWork(
        run.journal, run.journal.root / ("retained-scan-" + uuid4().hex), run.journal.control_limits
    )
    scratch._io_check = run.work.current

    def records():
        """Validate the retained suffix while preserving the complete SOURCE prefix."""
        from process.custom_import.family_shard_retained_checkpoint import SourcePrefix

        expected, is_source_prefix = None, False
        for head, record in SourcePrefix(snapshot, keep_vectors=True).records(check=run.work.current):
            if record.kind != "retained_allocation":
                is_source_prefix = True
                continue
            if is_source_prefix or (expected is not None and record.ordinal != expected):
                raise FamilyBlockUnavailable("retained checkpoint ordinal or phase differs")
            expected = record.ordinal - 1
            yield head.revision, record.kind, record.ordinal, record.input_sha256, record.commitments, record.payload
        if expected not in (None, -1):
            raise FamilyBlockUnavailable("retained checkpoint prefix has a gap")

    ordered = _sorted(scratch, records(), lambda row: row[0])
    try:
        yield ordered
    finally:
        try:
            ordered.close()
        finally:
            scratch.close()


def _input(run, kinds, cursor):
    from process.custom_import.family_shard_retained_reservations import _KINDS

    if (
        type(kinds) is not tuple
        or not kinds
        or set(kinds) - set(_KINDS)
        or type(cursor) is not tuple
        or len(cursor) != 3
        or type(cursor[0]) is not tuple
        or set(cursor[0]) - set(_KINDS)
        or type(cursor[1]) is not int
        or cursor[1] < 0
        or type(cursor[2]) is not tuple
        or len(cursor[2]) != len(_KINDS)
    ):
        raise FamilyBlockUnavailable("retained reservation input cursor differs")
    for kind, state in zip(_KINDS, cursor[2], strict=True):
        if (
            type(state) is not tuple
            or len(state) != 4
            or state[0] != kind
            or any(type(value) is not int or value < 0 for value in state[1:])
            or state[2] > state[1]
        ):
            raise FamilyBlockUnavailable("retained reservation FIFO state differs")
    return hashlib.sha256(_pack((_VECTORS, run.scope, run.ordinal, cursor, kinds))).digest()


def _write_vectors(run, kinds, cursor, identities):
    if type(identities) is not tuple or len(identities) != len(kinds):
        raise FamilyBlockUnavailable("retained native vector count differs")
    for identity in identities:
        _integer(identity, "retained journal native identity")
    retained_rows = tuple(
        (kind, retained_values)
        for kind, group in groupby(zip(kinds, identities, strict=True), key=itemgetter(0))
        for retained_values in batched(map(itemgetter(1), group), _vector_chunk(run))
    )
    digest_input = _input(run, kinds, cursor)
    header, footer = (_VECTORS, run.scope, run.ordinal, digest_input, len(identities)), ("end", len(identities))
    run.journal.budget().ensure(sum(len(_pack(retained_row)) + 8 for retained_row in (header, *retained_rows, footer)))
    path = run.journal.root / "vectors" / ("retained-" + uuid4().hex + ".mpk")
    writer = source_journal._VectorFrames(run.journal)
    writer._io_check = run.work.current
    with path.open("xb") as stream:
        for retained_row in (header, *retained_rows, footer):
            writer.frame(stream, retained_row)
        stream.flush()
        os.fchmod(stream.fileno(), 0o400)
        os.fsync(stream.fileno())
    source_journal._sync(path.parent)
    with path.open("rb") as stream:
        digest = _plan_digest(stream, run.work.current)
    return checkpoint.OwnerCheckpointRecord(
        "retained_allocation",
        run.ordinal,
        digest_input,
        metadata_journal._commitments("retained_vectors", digest, run.source),
        metadata_journal._descriptor(run.journal, _VECTORS, path, len(identities), digest),
    )


def _vector_chunk(run):
    return max(1, min(4096, run.work.limits.max_record_bytes // 32, run.work.limits.memory_bytes // 128))


def _read_vectors(run, checkpoint_record, kinds, cursor):
    if (
        checkpoint_record.kind != "retained_allocation"
        or checkpoint_record.ordinal != run.ordinal
        or checkpoint_record.input_sha256 != _input(run, kinds, cursor)
    ):
        raise FamilyBlockUnavailable("retained original demand or FIFO cursor differs")
    retained_vectors = []
    with metadata_journal._blob(run.journal, run.work, checkpoint_record, _VECTORS, "retained_vectors", run.source) as (
        stream,
        count,
    ):
        if count != len(kinds):
            raise FamilyBlockUnavailable("retained vector role count differs")
        with closing(run.work.read(stream)) as retained_rows:
            if next(retained_rows, None) != (_VECTORS, run.scope, run.ordinal, checkpoint_record.input_sha256, count):
                raise FamilyBlockUnavailable("retained vector original header differs")
            while len(retained_vectors) < count:
                retained_row = next(retained_rows, None)
                if (
                    type(retained_row) is not tuple
                    or len(retained_row) != 2
                    or type(retained_row[1]) is not tuple
                    or not 0 < len(retained_row[1]) <= min(_vector_chunk(run), count - len(retained_vectors))
                    or kinds[len(retained_vectors) : len(retained_vectors) + len(retained_row[1])]
                    != (retained_row[0],) * len(retained_row[1])
                ):
                    raise FamilyBlockUnavailable("retained vector original role differs")
                for identifier in retained_row[1]:
                    _integer(identifier, "retained committed native identity")
                retained_vectors.extend(retained_row[1])
            if next(retained_rows, None) != ("end", count) or next(retained_rows, None) is not None:
                raise FamilyBlockUnavailable("retained vector lacks exact EOF")
    return tuple(retained_vectors)


class _Run(metadata_journal._Run):
    def committed_completion(self, snapshot, previous, record, custody):
        """Observe the exact atomic completion/custody pair after a lost reply."""
        if custody is None:
            return self.confirmed(snapshot, previous)
        with closing(snapshot.records(check=self.work.current)) as rows:
            latest, completed, prior = next(rows, None), next(rows, None), next(rows, None)
        revision = 1 if previous is None else previous.revision + 1
        if (
            latest is None
            or completed is None
            or latest[1] != custody
            or completed[1] != record
            or latest[0].revision != revision + 1
            or completed[0].revision != revision
            or (None if prior is None else prior[0]) != previous
        ):
            raise FamilyBlockUnavailable("retained completion lacks its exact atomic custody successor")
        return completed[1]

    async def authority(self, session, build, prefix, *, writing):
        """Require the original unexpired owner authority before retained journal work."""
        from process.custom_import import family_shard_retained_allocate as allocation

        await allocation._guard(session, self.context, prefix)
        if build is None or build.build_id != self.journal.build_id or build.phase != "graph":
            raise FamilyBlockUnavailable("retained journal requires original GRAPH owner")
        descriptor, _native = await checkpoint._native_scope(
            session, self.request, self.journal.build_id, self.source.snapshot_family_id, self.layout, writing=writing
        )
        if checkpoint._head(descriptor) != self.head:
            raise FamilyBlockUnavailable("retained protected head changed before native work")

    async def append(self, prefix, kinds, cursor, descriptor=None):
        """Append original retained allocation custody and require exact native readback."""
        from process.custom_import import family_shard_retained_allocate as allocation

        previous, prepared, custody, retained_record = self.head, None, None, None
        try:
            await allocation._check_base(self.context)
            async with _page_session(self.factory, self.request, self.journal.build_id) as (session, build):
                await self.authority(session, build, prefix, writing=True)
                reserved = source_journal.reserve_checkpoint(self.journal)
                if descriptor is None:
                    retained_values = await _allocate_record_ids(session, kinds)
                    retained_record = await _cpu(self.work, _write_vectors, self, kinds, cursor, retained_values)
                else:
                    retained_record = self.complete_record(descriptor)
                prepared = await self._append_checkpoint(session, retained_record, previous, reserved)
                owner = getattr(self.journal, "_retained_bundle_owner", None)
                if descriptor is not None and owner is not None:
                    from process.custom_import.family_shard_retained_checkpoint import destination_record

                    if owner.context is not self.context or owner.source_journal is not self.journal:
                        raise FamilyBlockUnavailable("retained completion belongs to another live owner")
                    custody = await _cpu(self.work, destination_record, owner, descriptor)
                    reserved = source_journal.reserve_checkpoint(self.journal)
                    prepared = await self._append_checkpoint(session, custody, prepared.head, reserved)
                window = session.info[_WINDOW]
        except Exception as failure:
            try:
                snapshot = await self.snapshot()
            except BaseException as error:
                raise FamilyBlockUnavailable("retained commit is unknown; retain original journal custody") from error
            if snapshot.head == previous:
                raise failure
            retained_record = await _cpu(
                self.work, self.committed_completion, snapshot, previous, retained_record, custody
            )
            await allocation._check_base(self.context)
            self.head = snapshot.head
            return retained_record, None
        await allocation._check_base(self.context)
        snapshot = await self.snapshot()
        if prepared is None or snapshot.head != prepared.head:
            raise FamilyBlockUnavailable("retained head is not freshly committed")
        retained_record = await _cpu(self.work, self.committed_completion, snapshot, previous, retained_record, custody)
        self.head = snapshot.head
        return retained_record, window

    async def observe(self, prefix, kinds, remembered, *, cursor=()):
        """Adopt only the exact previously journalled retained allocation."""
        from process.custom_import import family_shard_retained_allocate as allocation

        if type(kinds) is not tuple:
            raise ValueError("retained native reservation requires exact ordered kinds")
        if len(kinds) > self.limits.maximum_batch_records or 96 * len(kinds) > self.limits.maximum_batch_bytes:
            raise FamilyBlockResourceLimit("retained native allocation exceeds the operational batch budget")
        window = None
        if kinds:
            _input(self, kinds, cursor)
            if self.pending is None:
                retained_record, window = await self.append(prefix, kinds, cursor)
            else:
                retained_record = self.pending
            retained_values = await _cpu(self.work, _read_vectors, self, retained_record, kinds, cursor)
            if self.pending is not None:
                await _cpu(self.work, self.advance)
            self.ordinal += 1
        else:
            retained_values = ()
        if window is None:
            await allocation._check_base(self.context)
            async with _page_session(self.factory, self.request, self.journal.build_id) as (session, build):
                await self.authority(session, build, prefix, writing=False)
                window = session.info[_WINDOW]
            await allocation._check_base(self.context)
        if (await self.snapshot()).head != self.head:
            raise FamilyBlockUnavailable("retained protected head changed before vector exposure")
        remembered(window)
        return retained_values

    def complete_record(self, descriptor):
        """Require the original completed retained record and its full scope."""
        cells = (
            _COMPLETE,
            self.scope,
            descriptor.sha256,
            descriptor.counts,
            descriptor.source_pack_prefix,
            descriptor.destination_pack_tail,
        )
        return checkpoint.OwnerCheckpointRecord(
            "retained_allocation",
            self.ordinal,
            hashlib.sha256(_pack(cells)).digest(),
            metadata_journal._commitments("retained_destination", descriptor.sha256, self.source),
            _pack(cells),
        )

    async def finish(self, descriptor):
        """Finish the retained journal only after its immutable readback agrees."""
        expected = self.complete_record(descriptor)
        if self.pending is None:
            record, _window = await self.append(descriptor.source_pack_prefix, (), (), descriptor)
        else:
            record = self.pending
            await _cpu(self.work, self.advance)
        if record != expected or self.pending is not None:
            raise FamilyBlockUnavailable("retained destination differs from protected complete EOF")
        if (await self.snapshot()).head != self.head:
            raise FamilyBlockUnavailable("retained protected head changed before destination exposure")


def _scope_digest(context, work, scope):
    return hashlib.sha256(
        _pack(
            (
                scope,
                context.events.sha256,
                context.events.counts,
                (
                    work.limits.spool.max_spool_bytes,
                    work.limits.spool.max_record_bytes,
                    work.limits.spool.memory_bytes,
                    work.limits.maximum_batch_records,
                    work.limits.maximum_batch_bytes,
                ),
            )
        )
    ).digest()


async def _start_retained_run(factory, context, work, journal, scope, current):
    from process.custom_import import family_shard_retained_allocate as allocation

    current()
    snapshot = await checkpoint.read_owner_checkpoint(
        factory,
        context.common.request,
        journal.build_id,
        context.common.source.snapshot_family_id,
        context.common.layout,
        root=journal.checkpoints,
        limits=journal.control_limits,
    )
    await _cpu(journal, journal.reconcile_budget)
    scratch_limits = (
        allocation._spool_limits(work.limits.spool)
        if context.final_plan.counts_by_kind["retained"]
        else work.limits.spool
    )
    scratch = metadata_journal.MetadataJournalScratch(
        journal,
        PlanWork(
            journal.root / "outputs" / ("retained-" + uuid4().hex),
            scratch_limits,
            journal.work.register_cleanup,
            current,
        ),
    )
    run = _Run(
        factory,
        context.common,
        journal,
        scratch,
        work.limits,
        snapshot,
    )
    run.context = context
    run.scope = _scope_digest(context, work, scope)
    await _cpu(scratch, _original_prefix, run, snapshot)
    return run, scratch, snapshot


async def journalled_retained_destination(factory, context, *, work, journal):
    """Bind retained production to its original journal and bounded scratch custody."""
    from process.custom_import import family_shard_retained_allocate as allocation

    scope = allocation._scope(context, work)
    if type(journal) is not source_journal.SourceAllocationJournal or journal.build_id != context.build_id:
        raise ValueError("retained allocation requires its original typed SOURCE journal")
    if journal.busy or journal.work.limits != work.limits.spool or context.common.request.continuation is not None:
        raise ValueError("retained journal is busy, has different budgets or lacks original warm custody")
    if type(context.events) is not events.AdmissionEventsDescriptor:
        raise FamilyBlockUnavailable("retained journal requires original completed admission events")
    deadline = _read_deadline(context.common.request)
    journal.busy = True
    journal.stop.clear()
    scratch = None
    try:

        def current():
            """Check the original live owner and retained journal head together."""
            journal.current()
            if work.check is not None:
                work.check()
            _require_deadline(deadline)

        run, scratch, snapshot = await _start_retained_run(factory, context, work, journal, scope, current)
        with _records(run, snapshot) as retained_rows:
            run.rows = retained_rows
            await _cpu(scratch, run.advance)
            descriptor = await allocation._produce_retained_destination(
                factory,
                context,
                work=replace(work, root=scratch.root, check=current),
                observe=run.observe,
                scratch=scratch,
            )
            await run.finish(descriptor)
            return descriptor
    finally:
        journal.busy = False
