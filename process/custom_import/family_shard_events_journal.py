# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Admission timestamps and exact rejection vectors in the original owner journal.

Only a freshly committed native head authorizes replay. Failed attempts remain
with the original live owner; this module neither opens cold jobs nor cleans a
journal on failure. Allocations and checkpoint appends share one transaction.
"""

import hashlib
from contextlib import closing
from dataclasses import replace
from datetime import datetime
from uuid import uuid4

import msgpack

from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_events as events
from process.custom_import import family_shard_metadata as metadata
from process.custom_import import family_shard_metadata_journal as metadata_journal
from process.custom_import import family_shard_source_journal as source_journal
from process.custom_import.build_source import _page_session
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_records import _integer
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_base import _read_deadline, _require_deadline
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_plan import PlanWork, _cpu

_CONTRACT = "custom-import/admission-event-vectors/v1"


def _pack(value):
    return msgpack.packb(value, use_bin_type=True)


def _event_payload(checkpoint_record, limits, maximum_batch_records):
    """Validate the closed event envelope before its SOURCE prefix is exposed."""
    cells = msgpack.unpackb(
        checkpoint_record.payload,
        raw=False,
        use_list=False,
        strict_map_key=True,
        max_array_len=max(16, maximum_batch_records),
        max_map_len=0,
        max_str_len=256,
        max_bin_len=limits.max_record_bytes,
    )
    if (
        checkpoint_record.kind != "admission_allocation"
        or tuple(name for name, _digest in checkpoint_record.commitments)
        != ("admission_events", "source_allocations", "source_plan")
        or type(cells) is not tuple
        or len(cells) != 4
        or cells[0] != _CONTRACT
        or type(cells[1]) is not str
        or cells[1] not in {"start", "batch", "complete"}
        or type(cells[2]) is not str
        or type(cells[3]) is not tuple
        or len(cells[3]) > maximum_batch_records
    ):
        raise FamilyBlockUnavailable("admission journal record scope differs")
    stamp = datetime.fromisoformat(cells[2])
    if stamp.tzinfo is None or stamp.utcoffset() is None or stamp.isoformat() != cells[2]:
        raise FamilyBlockUnavailable("admission journal owner timestamp differs")
    if (cells[1] == "batch") != bool(cells[3]):
        raise FamilyBlockUnavailable("admission journal vector kind differs")
    for identifier in cells[3]:
        _integer(identifier, "journalled admission rejection")
    if len(set(cells[3])) != len(cells[3]):
        raise FamilyBlockUnavailable("admission journal native vector repeats")
    return cells[1:]


def _decode(run, checkpoint_record):
    if checkpoint_record.commitments != run.commitments:
        raise FamilyBlockUnavailable("admission journal record scope differs")
    return _event_payload(checkpoint_record, run.journal.control_limits, run.limits.maximum_batch_records)


class AdmissionSourcePrefix:
    """A typed admission suffix is classified here; original batch proof still follows."""

    def __init__(self, snapshot, maximum_batch_records):
        self.snapshot, self.limits = snapshot, snapshot.limits
        self.maximum_batch_records = maximum_batch_records

    @property
    def head(self):
        """Retain the authenticated whole-chain head while exposing its SOURCE prefix."""
        return self.snapshot.head

    def records(self, *, check=None):
        """Refuse reordered/unknown custody instead of silently skipping later phases."""
        from process.custom_import.family_shard_retained_checkpoint import SourcePrefix

        expected, stamp, commitments, has_prefix = None, None, None, False
        for head, checkpoint_record in SourcePrefix(self.snapshot).records(check=check):
            if checkpoint_record.kind != "admission_allocation":
                if expected not in (None, -1):
                    raise FamilyBlockUnavailable("admission recovery lacks its original start record")
                has_prefix = True
                yield head, checkpoint_record
                continue
            tag, observed, _identifiers = _event_payload(checkpoint_record, self.limits, self.maximum_batch_records)
            if has_prefix or (expected is not None and checkpoint_record.ordinal != expected):
                raise FamilyBlockUnavailable("admission recovery records are duplicate or misplaced")
            if expected is None:
                stamp, commitments = observed, checkpoint_record.commitments
            elif tag == "complete":
                raise FamilyBlockUnavailable("admission recovery complete record is misplaced")
            if observed != stamp or checkpoint_record.commitments != commitments:
                raise FamilyBlockUnavailable("admission recovery original scope or timestamp differs")
            if (checkpoint_record.ordinal == 0) != (tag == "start"):
                raise FamilyBlockUnavailable("admission recovery start ordinal differs")
            expected = checkpoint_record.ordinal - 1
        if expected not in (None, -1) or not has_prefix:
            raise FamilyBlockUnavailable("admission recovery has no complete original SOURCE prefix")


def require_completed_admission(snapshot, maximum_batch_records, check):
    """GRAPH/rejection reopening must have a genuine completed event journal."""
    from process.custom_import.family_shard_retained_checkpoint import SourcePrefix

    with closing(SourcePrefix(snapshot).records(check=check)) as checkpoint_records:
        latest = next(checkpoint_records, None)
    if latest is None or latest[1].kind != "admission_allocation":
        raise FamilyBlockUnavailable("admission recovery lacks its completed event journal")
    tag, _stamp, _identifiers = _event_payload(latest[1], snapshot.limits, maximum_batch_records)
    if tag != "complete":
        raise FamilyBlockUnavailable("admission recovery lacks its completed event journal")


def _event_records(run, snapshot):
    from process.custom_import.family_shard_retained_checkpoint import SourcePrefix

    expected, has_metadata_prefix = None, False
    prefix = SourcePrefix(snapshot, limits=run.journal.control_limits)
    for head, checkpoint_record in prefix.records(check=run.work.current):
        if checkpoint_record.kind != "admission_allocation":
            has_metadata_prefix = True
            continue
        if has_metadata_prefix:
            raise FamilyBlockUnavailable("admission journal records interrupt the metadata prefix")
        _decode(run, checkpoint_record)
        if expected is None:
            expected = checkpoint_record.ordinal
        if checkpoint_record.ordinal != expected:
            raise FamilyBlockUnavailable("admission journal ordinal is duplicate or has a gap")
        expected -= 1
        yield (
            head.revision,
            checkpoint_record.kind,
            checkpoint_record.ordinal,
            checkpoint_record.input_sha256,
            checkpoint_record.commitments,
            checkpoint_record.payload,
        )
    if expected not in (None, -1):
        raise FamilyBlockUnavailable("admission journal lacks its original timestamp record")


class _MetadataPrefix:
    def __init__(self, run, snapshot):
        self.run, self.snapshot, self.head = run, snapshot, snapshot.head

    def records(self, *, check=None):
        """Expose the original metadata prefix without treating event files as proof."""
        from process.custom_import.family_shard_retained_checkpoint import SourcePrefix

        has_metadata_prefix = False
        prefix = SourcePrefix(self.snapshot, limits=self.run.journal.control_limits)
        for head, checkpoint_record in prefix.records(check=check):
            if not has_metadata_prefix and checkpoint_record.kind == "admission_allocation":
                _decode(self.run, checkpoint_record)
                continue
            has_metadata_prefix = True
            yield head, checkpoint_record


def _verify_metadata(run, snapshot):
    """Check every original SOURCE/family vector and typed query EOF, read-only."""
    prefix = _MetadataPrefix(run, snapshot)
    reader = metadata_journal._Run(run.factory, run.source_context, run.journal, run.work, run.limits, prefix)
    with metadata_journal._ordered_records(reader, prefix) as records:
        reader.rows = records
        reader.source_prefix()
        batches = metadata._family_batches(
            metadata._unique_families(run.work, run.source, run.request.definition), run.limits
        )
        with closing(batches):
            for ordinal, batch in enumerate(batches):
                if reader.pending is None:
                    raise FamilyBlockUnavailable("admission journal metadata family prefix is incomplete")
                metadata_journal._read_families(reader, reader.pending, ordinal, batch)
                reader.advance()
        if reader.pending is None:
            raise FamilyBlockUnavailable("admission journal requires completed metadata query keys")
        metadata_journal._read_query(reader, reader.pending)
        reader.advance()
        if reader.pending is not None:
            raise FamilyBlockUnavailable("admission journal metadata has trailing records")


class _Run(metadata_journal._Run):
    def bind_events(self, scope):
        """Bind event commitments to this original SOURCE allocation run."""
        self.scope = hashlib.sha256(_pack(scope)).digest()
        self.commitments = (
            ("admission_events", self.scope),
            ("source_allocations", self.source.sha256),
            ("source_plan", self.source.source.sha256),
        )
        self.stamp = None
        return self

    def input(self, tag, batch):
        """Commit the exact event kind, ordinal and original batch cells."""
        return hashlib.sha256(_pack((_CONTRACT, self.scope, self.ordinal, tag, batch))).digest()

    def checked(self, checkpoint_record, tag, batch):
        """Require original timestamp and exact committed native vector coverage."""
        actual_tag, stamp, identifiers = _decode(self, checkpoint_record)
        if (
            checkpoint_record.ordinal != self.ordinal
            or checkpoint_record.input_sha256 != self.input(tag, batch)
            or actual_tag != tag
            or (self.stamp is not None and stamp != self.stamp)
            or (tag == "batch" and len(identifiers) != len(batch))
        ):
            raise FamilyBlockUnavailable("admission journal original event batch differs")
        return stamp, identifiers

    async def preflight(self, session, build):
        """Check current owner identity and unchanged head inside the write transaction."""
        if build is None or build.build_id != self.journal.build_id or build.phase not in {"source", "admission"}:
            raise FamilyBlockUnavailable("admission allocation requires the original SOURCE or admission phase")
        descriptor, _native = await checkpoint._native_scope(
            session, self.request, self.journal.build_id, self.source.snapshot_family_id, self.layout, writing=True
        )
        if checkpoint._head(descriptor) != self.head:
            raise FamilyBlockUnavailable("admission protected head changed before allocation")

    async def append(self, tag, batch):
        """Append with the native observations, then reconcile the committed head."""
        previous, prepared = self.head, None
        try:
            async with _page_session(self.factory, self.request, self.journal.build_id) as (session, build):
                await self.preflight(session, build)
                reserved = source_journal.reserve_checkpoint(self.journal)
                stamp, identifiers = await events._owner(
                    session, self.request, self.layout, self.source, len(batch) if tag == "batch" else 0
                )
                checkpoint_record = checkpoint.OwnerCheckpointRecord(
                    "admission_allocation",
                    self.ordinal,
                    self.input(tag, batch),
                    self.commitments,
                    _pack((_CONTRACT, tag, stamp.isoformat() if self.stamp is None else self.stamp, identifiers)),
                )
                self.checked(checkpoint_record, tag, batch)
                prepared = await checkpoint.append_owner_checkpoint(
                    session,
                    self.request,
                    self.journal.build_id,
                    self.source.snapshot_family_id,
                    self.layout,
                    storage=self.checkpoint_storage,
                    record=checkpoint_record,
                    expected_head=previous,
                )
                source_journal.finish_checkpoint_charge(self.journal, reserved, prepared)
        except Exception as failure:
            try:
                snapshot = await self.snapshot()
            except BaseException as error:
                raise FamilyBlockUnavailable("admission commit is unknown; retain original custody") from error
            if snapshot.head == previous:
                raise failure
            checkpoint_record = await _cpu(self.work, self.confirmed, snapshot, previous)
            await _cpu(self.work, self.checked, checkpoint_record, tag, batch)
            self.head = snapshot.head
            return checkpoint_record
        snapshot = await self.snapshot()
        if prepared is None or snapshot.head != prepared.head:
            raise FamilyBlockUnavailable("admission head is not freshly committed")
        checkpoint_record = await _cpu(self.work, self.confirmed, snapshot, previous)
        await _cpu(self.work, self.checked, checkpoint_record, tag, batch)
        self.head = snapshot.head
        return checkpoint_record

    async def take(self, tag, batch=()):
        """Replay protected observations or allocate one missing bounded batch."""
        if self.pending is not None:
            checkpoint_record = self.pending
            values = await _cpu(self.work, self.checked, checkpoint_record, tag, batch)
            await _cpu(self.work, self.advance)
        else:
            checkpoint_record = await self.append(tag, batch)
            values = await _cpu(self.work, self.checked, checkpoint_record, tag, batch)
        self.ordinal += 1
        return values


async def _produce(run, admission, scope):
    # IDs are inline in bounded control frames; keep margin for framing and scope.
    control_count = (checkpoint._frame_limit(run.journal.control_limits) - 1024) // 16
    if control_count < 1:
        raise FamilyBlockUnavailable("admission journal control frame budget is too small")
    batch_limits = replace(run.limits, maximum_batch_records=min(run.limits.maximum_batch_records, control_count))
    staged = await _cpu(run.work, events._stage_events, run.work, run.source, admission)
    allocated, pending = await _cpu(run.work, events._split_events, run.work, staged)
    run.work.remove(staged)
    run.stamp, _empty = await run.take("start")
    with closing(events._pending_batches(run.work, pending, batch_limits)) as batches:
        for batch in batches:
            _stamp, identifiers = await run.take("batch", batch)
            await _cpu(run.work, events._append_ids, run.work, allocated, batch, identifiers)
    run.work.remove(pending)
    descriptor = await _cpu(run.work, events._complete, run.work, allocated, admission, scope, run.stamp)
    await run.take("complete", (descriptor.sha256, descriptor.counts))
    if run.pending is not None:
        raise FamilyBlockUnavailable("admission journal has trailing vectors")
    if (await run.snapshot()).head != run.head:
        raise FamilyBlockUnavailable("admission protected head changed before exposure")
    return descriptor


async def journalled_admission_events(factory, request, layout, allocated_source, admission, scope, *, journal, limits):
    """Reconcile the entire original prefix before allocating or returning rows."""
    from process.custom_import.family_shard_common import SourceCommonContext

    if type(journal) is not source_journal.SourceAllocationJournal or type(limits) is not SourceAllocationLimits:
        raise ValueError("admission events require their existing typed SOURCE journal and budgets")
    if journal.busy or journal.work.limits != limits.spool:
        raise ValueError("admission journal is busy or has different data budgets")
    deadline = _read_deadline(request)
    journal.busy = True
    journal.stop.clear()
    try:
        snapshot = await checkpoint.read_owner_checkpoint(
            factory,
            request,
            journal.build_id,
            allocated_source.snapshot_family_id,
            layout,
            root=journal.checkpoints,
            limits=journal.control_limits,
        )
        await _cpu(journal, journal.reconcile_budget)

        def current():
            """Honor original journal custody, cancellation and caller deadline."""
            journal.current()
            _require_deadline(deadline)

        work = metadata_journal.MetadataJournalScratch(
            journal,
            PlanWork(
                journal.root / "outputs" / ("events-" + uuid4().hex),
                limits.spool,
                journal.work.register_cleanup,
                current,
            ),
        )
        run = _Run(
            factory, SourceCommonContext(request, layout, allocated_source), journal, work, limits, snapshot
        ).bind_events(scope)
        await _cpu(work, _verify_metadata, run, snapshot)
        with closing(_sorted(work, _event_records(run, snapshot), lambda row: row[0])) as event_records:
            run.rows = event_records
            await _cpu(work, run.advance)
            return await _produce(run, admission, scope)
    finally:
        journal.busy = False
