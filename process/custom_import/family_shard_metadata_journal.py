# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native family metadata vectors and exact query mappings in the owner journal.

No filesystem file, pending result or counter grants resume authority. The caller
supplies the original authenticated SOURCE and separately pinned base custody.
Failed attempts stay with the journal owner until readers drain and it decides
terminal cleanup. This component does not open a cold job or a continuation.
"""

import hashlib
import os
import threading
from contextlib import asynccontextmanager, closing, contextmanager
from pathlib import Path
from uuid import uuid4

import msgpack
from sqlalchemy import select

from db.models.custom_import import CustomImportCaptureBundle, CustomImportExecution
from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_metadata as metadata
from process.custom_import import family_shard_source_checkpoint as source_checkpoint
from process.custom_import import family_shard_source_journal as source_journal
from process.custom_import.build_source import _drain_cleanup, _page_session
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_store import _execute
from process.custom_import.family_block_validate import check_key_dictionary
from process.custom_import.family_shard_allocate import (
    SourceAllocationLimits,
    _checked_allocation_row,
    _checked_source,
    read_source_allocations,
)
from process.custom_import.family_shard_base import _read_deadline, _require_deadline
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.family_shard_source_plan import _plan_digest

_FAMILY = "custom-import/metadata-family-ids/v1"
_QUERY = "custom-import/metadata-query-keys/v1"


class MetadataJournalScratch(source_journal.SourceAllocationWork):
    """Genuine metadata producer with the existing aggregate journal accounting."""

    def __init__(self, journal, plan):
        self.stop, self.owner_check = threading.Event(), plan.check
        super().__init__(journal, plan.root, plan.limits, check=self.current, register_cleanup=plan.register_cleanup)

    def current(self):
        """Preserve the original independent stop flag and owner/deadline callback."""
        _Scratch.current(self)


def _pack(value):
    return msgpack.packb(value, use_bin_type=True)


def _decode(payload, maximum):
    return msgpack.unpackb(
        payload,
        raw=False,
        use_list=False,
        strict_map_key=True,
        max_array_len=256,
        max_map_len=0,
        max_str_len=4096,
        max_bin_len=maximum,
    )


def _scope(request, source):
    return hashlib.sha256(_pack(metadata.owner_metadata_scope(request, source))).digest()


def _commitments(role, digest, source):
    return ((role, digest), ("source_allocations", source.sha256), ("source_plan", source.source.sha256))


def _input(scope, ordinal, batch):
    return hashlib.sha256(_pack((_FAMILY, scope, ordinal, batch))).digest()


def _descriptor(journal, contract, path, count, digest):
    return _pack(
        (
            contract,
            str(journal.root),
            journal.identity,
            source_journal._directory(path.parent),
            path.name,
            source_journal._stamp(path.lstat()),
            digest,
            count,
        )
    )


@contextmanager
def _blob(journal, work, checkpoint_record, contract, role, source_plan):
    cells = _decode(checkpoint_record.payload, journal.control_limits.max_record_bytes)
    if type(cells) is not tuple or len(cells) != 8:
        raise FamilyBlockUnavailable("metadata journal descriptor shape differs")
    actual_contract, root, identity, directory, name, stamp, digest, count = cells
    for metadata_values, length in ((identity, 4), (directory, 4), (stamp, 7)):
        if (
            type(metadata_values) is not tuple
            or len(metadata_values) != length
            or any(type(metadata_value) is not int or metadata_value < 0 for metadata_value in metadata_values)
        ):
            raise FamilyBlockUnavailable("metadata journal custody tuple differs")
    _integer(count, "metadata journal count", zero=True)
    if (
        actual_contract != contract
        or root != str(journal.root)
        or identity != journal.identity
        or directory != source_journal._directory(journal.root / "vectors")
        or type(name) is not str
        or Path(name).name != name
        or name in ("", ".", "..")
        or type(digest) is not bytes
        or len(digest) != 32
        or checkpoint_record.commitments != _commitments(role, digest, source_plan)
        or stamp[4] > journal.work.limits.max_spool_bytes
    ):
        raise FamilyBlockUnavailable("metadata journal descriptor scope differs")
    journal.current()
    path = journal.root / "vectors" / name
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        if source_journal._stamp(os.fstat(stream.fileno())) != stamp or source_journal._stamp(path.lstat()) != stamp:
            raise FamilyBlockUnavailable("metadata journal file custody differs")
        if _plan_digest(stream, work.current) != digest:
            raise FamilyBlockUnavailable("metadata journal file SHA differs")
        stream.seek(0)
        yield stream, count
        stream.seek(0)
        if (
            _plan_digest(stream, work.current) != digest
            or source_journal._stamp(os.fstat(stream.fileno())) != stamp
            or source_journal._stamp(path.lstat()) != stamp
            or source_journal._directory(path.parent) != directory
        ):
            raise FamilyBlockUnavailable("metadata journal file changed during replay")
    journal.current()


def _family_values(batch, assignments):
    if type(assignments) is not tuple or len(assignments) != len(batch):
        raise FamilyBlockUnavailable("metadata native family vector count differs")
    for entry, row in zip(batch, assignments, strict=True):
        if (
            type(row) is not tuple
            or len(row) != 2
            or row[0] != entry[0]
            or type(row[1]) is not tuple
            or len(row[1]) != 2
        ):
            raise FamilyBlockUnavailable("metadata native family vector key differs")
        for identifier in row[1]:
            _integer(identifier, "metadata native family identity")
    return assignments


def _write_families(run, ordinal, batch, assignments):
    _family_values(batch, assignments)
    digest_input = _input(run.scope, ordinal, batch)
    rows = (
        (_FAMILY, run.scope, ordinal, digest_input, len(batch)),
        *((row[0], row[1], entry[2]) for entry, row in zip(batch, assignments, strict=True)),
        ("end", len(batch)),
    )
    source_journal.reserve_vector(run.journal, sum(len(_pack(row)) + 8 for row in rows))
    path = run.journal.root / "vectors" / ("metadata-family-" + uuid4().hex + ".mpk")
    writer = source_journal._VectorFrames(run.journal)
    writer._io_check = run.work.current
    with path.open("xb") as target:
        for row in rows:
            writer.frame(target, row)
        target.flush()
        os.fchmod(target.fileno(), 0o400)
        os.fsync(target.fileno())
    source_journal._sync(path.parent)
    with path.open("rb") as stream:
        digest = _plan_digest(stream, run.work.current)
    return checkpoint.OwnerCheckpointRecord(
        "family_allocation",
        ordinal,
        digest_input,
        _commitments("family_vectors", digest, run.source),
        _descriptor(run.journal, _FAMILY, path, len(batch), digest),
    )


def _read_families(run, checkpoint_record, ordinal, batch):
    if (
        checkpoint_record.kind != "family_allocation"
        or checkpoint_record.ordinal != ordinal
        or checkpoint_record.input_sha256 != _input(run.scope, ordinal, batch)
    ):
        raise FamilyBlockUnavailable("metadata original family batch differs")
    assignments = []
    with _blob(run.journal, run.work, checkpoint_record, _FAMILY, "family_vectors", run.source) as (stream, count):
        if count != len(batch):
            raise FamilyBlockUnavailable("metadata family vector count differs")
        with closing(run.work.read(stream)) as metadata_rows:
            header = next(metadata_rows, None)
            if (
                type(header) is not tuple
                or len(header) != 5
                or type(header[2]) is not int
                or type(header[4]) is not int
            ):
                raise FamilyBlockUnavailable("metadata family header types differ")
            if header != (_FAMILY, run.scope, ordinal, checkpoint_record.input_sha256, count):
                raise FamilyBlockUnavailable("metadata family header differs")
            for entry in batch:
                metadata_row = next(metadata_rows, None)
                if (
                    type(metadata_row) is not tuple
                    or len(metadata_row) != 3
                    or metadata_row[0] != entry[0]
                    or type(metadata_row[2]) is not int
                    or metadata_row[2] != entry[2]
                ):
                    raise FamilyBlockUnavailable("metadata family SOURCE identity differs")
                assignments.append(metadata_row[:2])
            footer = next(metadata_rows, None)
            if (
                type(footer) is not tuple
                or len(footer) != 2
                or type(footer[1]) is not int
                or footer != ("end", count)
                or next(metadata_rows, None) is not None
            ):
                raise FamilyBlockUnavailable("metadata family vector EOF differs")
    return _family_values(batch, tuple(assignments))


def _custody_phase(checkpoint_record, cells, phase, outputs, common):
    if type(cells) is not tuple or not cells:
        return None
    if cells[0] == source_checkpoint.common_api._CUSTODY:
        if phase is not None or common is not None:
            raise FamilyBlockUnavailable("metadata SOURCE common checkpoint is duplicate or misplaced")
        return "common", source_checkpoint.common_api.source_common_payload(checkpoint_record, cells)
    if cells[0] == source_checkpoint._SOURCE_OUTPUT:
        if phase not in (None, "common", "outputs") or checkpoint_record.ordinal in outputs or len(outputs) >= 4096:
            raise FamilyBlockUnavailable("metadata SOURCE output checkpoint is duplicate or misplaced")
        try:
            outputs[checkpoint_record.ordinal] = source_checkpoint._source_output_payload(checkpoint_record, cells)
        except ValueError as error:
            raise FamilyBlockUnavailable("metadata SOURCE output checkpoint differs") from error
        return "outputs", common
    if cells[0] not in {source_checkpoint._CONTRACT, source_checkpoint._OWNER_READY}:
        return None
    if phase not in (None, "common", "outputs"):
        raise FamilyBlockUnavailable("metadata SOURCE-ready checkpoint is duplicate or misplaced")
    try:
        source_checkpoint._checkpoint_payload(checkpoint_record)
        if outputs or common is not None:
            custody, count = source_checkpoint._owner_ready_payload(cells)
            source_checkpoint._ready_outputs(checkpoint_record, custody, count, outputs, common)
    except ValueError as error:
        raise FamilyBlockUnavailable("metadata SOURCE-ready checkpoint envelope differs") from error
    return "ready", common


def _checked_records(run, snapshot):
    """Classify the ENTIRE reverse chain before sorting exposes any first row."""
    phase, expected, seen_source, seen_family, query = None, None, False, False, False
    source_ready, outputs, common = False, {}, None
    for head, metadata_record in snapshot.records(check=run.work.current):
        match metadata_record.kind:
            case "artifact":
                cells = _decode(metadata_record.payload, run.journal.control_limits.max_record_bytes)
                custody = _custody_phase(metadata_record, cells, phase, outputs, common)
                if custody is not None:
                    phase, common = custody
                    source_ready = source_ready or phase == "ready"
                    # This control envelope is not allocation or reopened-file proof.
                    continue
                if phase not in (None, "ready") or query or metadata_record.ordinal != 0:
                    raise FamilyBlockUnavailable("metadata query checkpoint is duplicate or misplaced")
                if type(cells) is not tuple or len(cells) != 8 or cells[0] != _QUERY:
                    raise FamilyBlockUnavailable("metadata chain contains an unsupported artifact")
                phase, expected, query = "query", None, True
            case "family_allocation" | "source_allocation":
                current = "family" if metadata_record.kind == "family_allocation" else "source"
                if current != phase:
                    if current == "family" and phase not in (None, "query"):
                        raise FamilyBlockUnavailable("metadata family checkpoint follows SOURCE out of order")
                    if current == "source" and (phase == "query" or (phase == "family" and expected != -1)):
                        raise FamilyBlockUnavailable("metadata family checkpoint prefix has a gap")
                    phase, expected = current, metadata_record.ordinal
                if metadata_record.ordinal != expected:
                    raise FamilyBlockUnavailable("metadata checkpoint ordinal is duplicate or has a gap")
                expected -= 1
                seen_source |= current == "source"
                seen_family |= current == "family"
            case _:
                raise FamilyBlockUnavailable("metadata chain contains another allocation phase")
        if dict(metadata_record.commitments).get("source_plan") != run.source.source.sha256:
            raise FamilyBlockUnavailable("metadata checkpoint SOURCE plan differs")
        if (
            metadata_record.kind != "source_allocation"
            and dict(metadata_record.commitments).get("source_allocations") != run.source.sha256
        ):
            raise FamilyBlockUnavailable("metadata checkpoint SOURCE vector differs")
        yield (
            head.revision,
            metadata_record.kind,
            metadata_record.ordinal,
            metadata_record.input_sha256,
            metadata_record.commitments,
            metadata_record.payload,
        )
    if (
        not seen_source
        or phase != "source"
        or expected != -1
        or (query and not seen_family)
        or (source_ready and not query)
        or ((outputs or common is not None) and not source_ready)
    ):
        raise FamilyBlockUnavailable("metadata checkpoint lacks a complete original SOURCE prefix")


@contextmanager
def _ordered_records(run, snapshot):
    scratch = source_journal.SourceAllocationWork(
        run.journal,
        run.journal.root / ("metadata-scan-" + uuid4().hex),
        run.journal.control_limits,
    )
    scratch._io_check = run.work.current
    rows = _sorted(scratch, _checked_records(run, snapshot), lambda row: row[0])
    try:
        yield rows
    finally:
        try:
            rows.close()
        finally:
            scratch.close()


class _Run:
    def __init__(self, factory, common, journal, work, limits, snapshot):
        self.source_context, self.checkpoint_storage = common, snapshot
        self.factory, self.request, self.layout, self.source = factory, common.request, common.layout, common.source
        self.journal, self.work, self.limits = journal, work, limits
        self.head, self.scope, self.ordinal = snapshot.head, _scope(common.request, common.source), 0
        self.rows = self.pending = None

    async def _append_checkpoint(self, session, checkpoint_record, expected_head, reservation):
        prepared = await checkpoint.append_owner_checkpoint(
            session,
            self.request,
            self.journal.build_id,
            self.source.snapshot_family_id,
            self.layout,
            storage=self.checkpoint_storage,
            record=checkpoint_record,
            expected_head=expected_head,
        )
        source_journal.finish_checkpoint_charge(self.journal, reservation, prepared)
        return prepared

    def advance(self):
        """Check the original live owner before further journal work."""
        cells = next(self.rows, None)
        self.pending = None if cells is None else checkpoint.OwnerCheckpointRecord(*cells[1:])

    def source_prefix(self):
        """Recover the exact SOURCE prefix without treating later artifacts as SOURCE."""
        self.advance()  # Exhausts and classifies the complete protected chain.
        ordinal = 0
        with closing(read_source_allocations(self.source, check=self.work.current)) as allocations:
            while self.pending is not None and self.pending.kind == "source_allocation":
                metadata_record = self.pending
                cells = _decode(metadata_record.payload, self.journal.control_limits.max_record_bytes)
                if (
                    type(cells) is not tuple
                    or len(cells) != 10
                    or type(cells[8]) is not int
                    or not 0 <= cells[8] <= self.limits.maximum_batch_records
                ):
                    raise FamilyBlockUnavailable("metadata SOURCE batch descriptor differs")
                count = cells[8]
                if count == 0 and (ordinal != 0 or self.source.source.counts[1] != 0):
                    raise FamilyBlockUnavailable("metadata SOURCE batch is unexpectedly empty")
                observed, metadata_rows, size = [], 0, 0
                for _index in range(count):
                    metadata_row = next(allocations, None)
                    if metadata_row is None:
                        raise FamilyBlockUnavailable("metadata SOURCE journal exceeds the completed allocation file")
                    pack = metadata_row[0]
                    metadata_rows += len(pack.records)
                    size += pack.landing.byte_count + 512 * len(pack.records)
                    if metadata_rows > self.limits.maximum_batch_records or size > self.limits.maximum_batch_bytes:
                        raise FamilyBlockResourceLimit("metadata SOURCE replay exceeds the original batch budget")
                    observed.append(metadata_row)
                packs = tuple(metadata_row[0] for metadata_row in observed)
                actual = source_journal.read_vectors(
                    self.journal,
                    metadata_record,
                    self.source.source,
                    self.source.snapshot_family_id,
                    ordinal,
                    packs,
                    _checked_allocation_row,
                )
                if actual != tuple((metadata_row[0].pack_index, *metadata_row[1:]) for metadata_row in observed):
                    raise FamilyBlockUnavailable("metadata SOURCE native vector differs from original allocations")
                ordinal += 1
                self.advance()
            if ordinal == 0 or next(allocations, None) is not None:
                raise FamilyBlockUnavailable("metadata SOURCE allocation EOF is incomplete")

    async def family_ids(self, batch):
        """Return the exact original family identities retained by this journal."""
        if self.pending is not None:
            if self.pending.kind != "family_allocation":
                raise FamilyBlockUnavailable("metadata family coverage ends before original SOURCE families")
            values = await _cpu(self.work, _read_families, self, self.pending, self.ordinal, batch)
            await _cpu(self.work, self.advance)
        else:
            record = await self.append_family(batch)
            values = await _cpu(self.work, _read_families, self, record, self.ordinal, batch)
        self.ordinal += 1
        return values

    async def snapshot(self):
        """Freeze a metadata prefix descriptor with the original scope and journal head."""
        return await checkpoint.read_owner_checkpoint(
            self.factory,
            self.request,
            self.journal.build_id,
            self.source.snapshot_family_id,
            self.layout,
            root=self.journal.checkpoints,
            limits=self.journal.control_limits,
        )

    async def preflight(self, session, build):
        """Validate existing SOURCE custody before allocating any metadata output."""
        if build is None or build.build_id != self.journal.build_id or build.phase != "source":
            raise FamilyBlockUnavailable("metadata allocation requires the original SOURCE build phase")
        descriptor, _native = await checkpoint._native_scope(
            session,
            self.request,
            self.journal.build_id,
            self.source.snapshot_family_id,
            self.layout,
            writing=True,
        )
        if checkpoint._head(descriptor) != self.head:
            raise FamilyBlockUnavailable("metadata protected head changed before allocation")
        capture = _checked_source(self.request, self.source.source)
        actual = (
            await _execute(
                session,
                select(
                    CustomImportExecution.capture_bundle_id,
                    CustomImportExecution.source_binding_revision_id,
                    CustomImportCaptureBundle.source_binding_revision_id,
                    CustomImportCaptureBundle.manifest_sha256,
                    CustomImportCaptureBundle.source_binding_sha256,
                )
                .join(
                    CustomImportCaptureBundle,
                    CustomImportCaptureBundle.capture_bundle_id == CustomImportExecution.capture_bundle_id,
                )
                .where(CustomImportExecution.execution_id == self.request.execution_id),
            )
        ).one()
        binding = capture["source_binding_sha256"]
        if tuple(actual) != (
            capture["capture_bundle_id"],
            capture["source_binding_revision_id"],
            capture["source_binding_revision_id"],
            bytes.fromhex(capture["manifest_sha256"]),
            None if binding is None else bytes.fromhex(binding),
        ):
            raise FamilyBlockUnavailable("metadata native capture hashes differ")

    def confirmed(self, snapshot, expected):
        """Require exact native journal readback before adopting a family allocation."""
        revision = 1 if expected is None else expected.revision + 1
        if snapshot.head is None or snapshot.head.revision != revision:
            raise FamilyBlockUnavailable("metadata commit lacks an exact next protected head")
        with closing(snapshot.records(check=self.work.current)) as records:
            _head, record = next(records)
            previous = next(records, None)
            if (None if previous is None else previous[0]) != expected:
                raise FamilyBlockUnavailable("metadata commit changed its protected prefix")
        return record

    async def append_family(self, batch):
        """Append one completed family with its immutable allocation and digest custody."""
        previous, prepared = self.head, None
        try:
            async with _page_session(self.factory, self.request, self.journal.build_id) as (session, build):
                await self.preflight(session, build)
                reserved = source_journal.reserve_checkpoint(self.journal)
                metadata_values = await metadata._allocate_family_batch(session, self.request, batch, self.limits)
                metadata_record = await _cpu(self.work, _write_families, self, self.ordinal, batch, metadata_values)
                prepared = await self._append_checkpoint(session, metadata_record, previous, reserved)
        except Exception as failure:
            try:
                snapshot = await self.snapshot()
            except BaseException as error:
                raise FamilyBlockUnavailable("metadata family commit is unknown; retain original custody") from error
            if snapshot.head == previous:
                raise failure
            metadata_record = await _cpu(self.work, self.confirmed, snapshot, previous)
            await _cpu(self.work, _read_families, self, metadata_record, self.ordinal, batch)
            self.head = snapshot.head
            return metadata_record
        snapshot = await self.snapshot()
        if prepared is None or snapshot.head != prepared.head:
            raise FamilyBlockUnavailable("metadata family head is not freshly committed")
        metadata_record = await _cpu(self.work, self.confirmed, snapshot, previous)
        self.head = snapshot.head
        return metadata_record


def _copy_query(run, written):
    if metadata._metadata_stamp(written.path) != written.stamp:
        raise FamilyBlockUnavailable("metadata query producer custody differs")
    source_journal.reserve_vector(run.journal, max(1, written.stamp[4]))
    path = run.journal.root / "vectors" / ("metadata-query-" + uuid4().hex + ".mpk")
    with (
        os.fdopen(os.open(written.path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as metadata_source,
        path.open("xb") as metadata_target,
    ):
        if metadata._file_stamp(os.fstat(metadata_source.fileno())) != written.stamp:
            raise FamilyBlockUnavailable("metadata query producer open identity differs")
        digest = hashlib.sha256()
        while chunk := metadata_source.read(min(65536, run.work.limits.memory_bytes)):
            run.work.current()
            run.journal.budget().charge(len(chunk))
            metadata_target.write(chunk)
            digest.update(chunk)
        metadata_target.flush()
        os.fchmod(metadata_target.fileno(), 0o400)
        os.fsync(metadata_target.fileno())
        if (
            digest.digest() != written.sha256
            or metadata._file_stamp(os.fstat(metadata_source.fileno())) != written.stamp
            or metadata._metadata_stamp(written.path) != written.stamp
        ):
            raise FamilyBlockUnavailable("metadata query producer bytes changed")
    source_journal._sync(path.parent)
    return checkpoint.OwnerCheckpointRecord(
        "artifact",
        0,
        hashlib.sha256(_pack((_QUERY, run.scope))).digest(),
        _commitments("metadata_query_keys", written.sha256, run.source),
        _descriptor(run.journal, _QUERY, path, written.count, written.sha256),
    )


def _read_query(run, record, *, copy=False):
    if (
        record.kind != "artifact"
        or record.ordinal != 0
        or record.input_sha256 != hashlib.sha256(_pack((_QUERY, run.scope))).digest()
    ):
        raise FamilyBlockUnavailable("metadata query checkpoint input differs")
    with _blob(run.journal, run.work, record, _QUERY, "metadata_query_keys", run.source) as (source, count):
        if check_key_dictionary(run.work.read(source)) != count:
            raise FamilyBlockUnavailable("metadata query dictionary typed EOF differs")
        if not copy:
            return None
        source.seek(0)
        run.journal.budget().ensure(max(1, os.fstat(source.fileno()).st_size))
        path = run.work.path()
        with path.open("xb") as target:
            output = metadata._MetadataOutput(target)
            while chunk := source.read(min(65536, run.work.limits.memory_bytes)):
                run.work.current()
                run.work.charge(len(chunk))
                output.write(chunk)
                run.work.paths[path] += len(chunk)
            target.flush()
            os.fsync(target.fileno())
        return metadata._WrittenMetadata(path, count, output.digest.digest(), metadata._metadata_stamp(path))


async def _query(run, retained):
    if run.pending is not None:
        if run.pending.kind != "artifact":
            raise FamilyBlockUnavailable("metadata journal has extra native family vectors")
        iterator = iter(retained)
        close = getattr(iterator, "close", None)
        if close is not None:
            await _cpu(run.work, close)
        metadata_values = await _cpu(run.work, lambda: _read_query(run, run.pending, copy=True))
        await _cpu(run.work, run.advance)
        if run.pending is not None:
            raise FamilyBlockUnavailable("metadata query checkpoint has trailing records")
        return metadata_values
    run.journal.budget().ensure(1)
    written = await _cpu(run.work, metadata._write_query_keys, run.work, run.source, run.request.definition, retained)
    metadata_record = await _cpu(run.work, _copy_query, run, written)
    await _cpu(run.work, _read_query, run, metadata_record)
    previous, prepared = run.head, None
    try:
        async with _page_session(run.factory, run.request, run.journal.build_id) as (session, build):
            await run.preflight(session, build)
            reserved = source_journal.reserve_checkpoint(run.journal)
            prepared = await run._append_checkpoint(session, metadata_record, previous, reserved)
    except Exception as failure:
        try:
            snapshot = await run.snapshot()
        except BaseException as error:
            raise FamilyBlockUnavailable("metadata query commit is unknown; retain original custody") from error
        if snapshot.head == previous:
            raise failure
        actual = await _cpu(run.work, run.confirmed, snapshot, previous)
        await _cpu(run.work, _read_query, run, actual)
        run.head = snapshot.head
        if actual == metadata_record:
            return written
        adopted = await _cpu(run.work, lambda: _read_query(run, actual, copy=True))
        await _cpu(run.work, run.work.remove, written.path)
        return adopted
    snapshot = await run.snapshot()
    if prepared is None or snapshot.head != prepared.head:
        raise FamilyBlockUnavailable("metadata query head is not freshly committed")
    actual = await _cpu(run.work, run.confirmed, snapshot, previous)
    await _cpu(run.work, _read_query, run, actual)
    run.head = snapshot.head
    return written


def _metadata_context(request, layout, source_plan, journal, limits):
    from process.custom_import.family_shard_common import SourceCommonContext

    _checked_source(request, source_plan.source)
    if type(journal) is not source_journal.SourceAllocationJournal or type(limits) is not SourceAllocationLimits:
        raise ValueError("metadata journalling requires its existing typed SOURCE journal")
    if journal.busy or journal.work.limits != limits.spool:
        raise ValueError("metadata journal is busy or has different data budgets")
    common = SourceCommonContext(request, layout, source_plan)
    deadline = _read_deadline(request)
    return common, deadline


@asynccontextmanager
async def journalled_owner_metadata(factory, request, layout, source_plan, *, journal, limits, retained_query_keys=()):
    """Reconcile exact originals; stage/cold-owner activation remains caller-owned."""
    common, deadline = _metadata_context(request, layout, source_plan, journal, limits)
    journal.busy = True
    journal.stop.clear()
    work, run = None, None
    try:
        snapshot = await checkpoint.read_owner_checkpoint(
            factory,
            request,
            journal.build_id,
            source_plan.snapshot_family_id,
            layout,
            root=journal.checkpoints,
            limits=journal.control_limits,
        )
        await _cpu(journal, journal.reconcile_budget)

        def current():
            """Refresh original lease and journal custody before native work."""
            journal.current()
            _require_deadline(deadline)

        work = MetadataJournalScratch(
            journal,
            PlanWork(
                journal.root / "outputs" / ("metadata-" + uuid4().hex),
                limits.spool,
                journal.work.register_cleanup,
                current,
            ),
        )
        run = _Run(factory, common, journal, work, limits, snapshot)
        with _ordered_records(run, snapshot) as metadata_records:
            run.rows = metadata_records
            await _cpu(work, run.source_prefix)
            families = await metadata._write_families(request, source_plan, work, limits, run.family_ids)
            queries = await _query(run, retained_query_keys)
        confirmed = await run.snapshot()
        if confirmed.head != run.head:
            raise FamilyBlockUnavailable("metadata protected head changed before exposure")
        metadata_result = metadata.OwnerSourceMetadata(work, families.path, queries.path)
        object.__setattr__(metadata_result, "_scope", metadata.owner_metadata_scope(request, source_plan))
        object.__setattr__(metadata_result, "_written", (families, queries))
    finally:
        journal.busy = False
    try:
        yield metadata_result
    except BaseException:
        # Original attempts and independent protected copies stay registered.
        raise
    else:
        primary = await _drain_cleanup(_cpu(work, work.close), None)
        if primary is not None:
            raise primary
