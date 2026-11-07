# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Observe bounded native SOURCE allocations before dispatching CPU workers."""

import os
import shutil
import stat
import struct
from contextlib import ExitStack, aclosing, asynccontextmanager, closing
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import msgpack
from sqlalchemy import select

from db.models.custom_import import CustomImportCaptureBundle, CustomImportExecution
from process.custom_import import family_block_store as store
from process.custom_import import family_shard_checkpoint as checkpoint_api
from process.custom_import.build_source import _page_session
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_store import _execute
from process.custom_import.family_shard_checkpoint import (
    OwnerCheckpointRecord,
    append_owner_checkpoint,
    read_owner_checkpoint,
)
from process.custom_import.family_shard_facts import SourceFactLimits, _Work
from process.custom_import.family_shard_identity import intern_root_records
from process.custom_import.family_shard_ids import _allocate_record_ids, allocate_bound_ids, source_sequence_bindings
from process.custom_import.family_shard_source_journal import (
    SourceAllocationJournal,
    SourceAllocationWork,
    finish_checkpoint_charge,
    read_vectors,
    replay_records,
    reserve_checkpoint,
    write_vectors,
)
from process.custom_import.family_shard_source_plan import SourcePlanDescriptor, _plan_digest, read_source_plan

_LENGTH = struct.Struct(">Q")
_NAME = "source-ids.mpk"


async def _cpu(owner, operation, *arguments):
    # Plan readers also consume allocated SOURCE; import after module loading.
    from process.custom_import.family_shard_plan import _cpu as run

    return await run(owner, operation, *arguments)


@dataclass(frozen=True)
class AllocatedSourcePlan:
    source: SourcePlanDescriptor
    root: str
    device: int
    inode: int
    file_device: int
    file_inode: int
    file_bytes: int
    sha256: bytes
    max_frame_bytes: int
    snapshot_family_id: int


@dataclass(frozen=True)
class SourceAllocationLimits:
    spool: SourceFactLimits
    maximum_batch_records: int
    maximum_batch_bytes: int

    def __post_init__(self):
        if type(self.spool) is not SourceFactLimits or any(
            type(bound) is not int or bound <= 0 for bound in (self.maximum_batch_records, self.maximum_batch_bytes)
        ):
            raise ValueError("SOURCE allocation resource budgets differ")


def _checked_source(request, plan):
    if type(plan) is not SourcePlanDescriptor or request.continuation is not None:
        raise FamilyBlockUnavailable("SOURCE allocations require the original complete source plan")
    return _checked_source_provenance(request, plan)


def _checked_source_provenance(request, plan):
    """Compare retained producer bytes; this does not authorize SOURCE allocation."""
    if type(plan) is not SourcePlanDescriptor:
        raise FamilyBlockUnavailable("SOURCE provenance requires its original complete plan")
    capture_by_field = dict(plan.scope[0])
    if (
        plan.scope[1:]
        != (
            request.execution_id,
            request.fence,
            producer_token_sha256(request),
            request.page_row_limit,
            request.page_byte_limit,
        )
        or (
            capture_by_field["dataset_id"],
            capture_by_field["definition_revision_id"],
            capture_by_field["schema_revision_id"],
        )
        != (request.dataset_id, request.definition_revision_id, request.schema_revision_id)
        or capture_by_field["definition_sha256"] != request.definition.digest
        or capture_by_field["schema_sha256"] != request.definition.schema_digest
    ):
        raise FamilyBlockUnavailable("SOURCE allocation plan scope differs")
    return capture_by_field


def _pack_batches(plan, maximum_records, maximum_bytes, *, check=None):
    if any(type(budget) is not int or budget <= 0 for budget in (maximum_records, maximum_bytes)):
        raise ValueError("SOURCE allocation batch budgets must be positive")
    batch, count, size = [], 0, 0
    with closing(read_source_plan(plan, check=check)) as packs:
        for pack in packs:
            charge = pack.landing.byte_count + 512 * len(pack.records)
            if len(pack.records) > maximum_records or charge > maximum_bytes:
                raise FamilyBlockResourceLimit("SOURCE pack exceeds the allocation resource budget")
            if batch and (count + len(pack.records) > maximum_records or size + charge > maximum_bytes):
                yield tuple(batch)
                batch, count, size = [], 0, 0
            batch.append(pack)
            count += len(pack.records)
            size += charge
        if batch:
            yield tuple(batch)
        else:
            yield ()


async def _allocate_batch(
    session, request, packs, maximum_records, maximum_bytes, *, batch_ordinals=None, sequences=None
):
    if not packs:
        return ()
    keys = tuple(
        source_record.typed_parent_key
        for pack in packs
        for source_record in pack.records
        if source_record.typed_parent_key is not None
    )
    roots = iter(
        await intern_root_records(
            session,
            request.definition,
            request.dataset_id,
            keys,
            maximum_rows=maximum_records,
            maximum_bytes=maximum_bytes,
            **({} if batch_ordinals is None else {"batch_ordinals": batch_ordinals}),
        )
    )
    kinds = tuple(
        kind
        for pack in packs
        for kind in (
            "pack",
            *(
                "rejection" if kind is None else "root_revision" if kind == 1 else "child_revision"
                for kind in pack.allocation_kinds
            ),
        )
    )
    identities = iter(
        await (
            _allocate_record_ids(session, kinds) if sequences is None else allocate_bound_ids(session, kinds, sequences)
        )
    )
    allocated_pack_rows = []
    for pack in packs:
        pack_id = next(identities)
        root_ids, revision_ids, rejection_ids = [], [], []
        for source_record, kind in zip(pack.records, pack.allocation_kinds, strict=True):
            root_ids.append(None if source_record.typed_parent_key is None else next(roots))
            identity = next(identities)
            revision_ids.append(None if kind is None else identity)
            rejection_ids.append(identity if kind is None else None)
        allocated_pack_rows.append(
            (pack.pack_index, pack_id, tuple(root_ids), tuple(revision_ids), tuple(rejection_ids))
        )
    if next(roots, None) is not None or next(identities, None) is not None:
        raise FamilyBlockUnavailable("SOURCE allocation result has extra identities")
    return tuple(allocated_pack_rows)


def _plan_capture_binding(capture_by_field):
    binding_sha = capture_by_field["source_binding_sha256"]
    return (
        capture_by_field["capture_bundle_id"],
        capture_by_field["source_binding_revision_id"],
        capture_by_field["source_binding_revision_id"],
        bytes.fromhex(capture_by_field["manifest_sha256"]),
        None if binding_sha is None else bytes.fromhex(binding_sha),
    )


async def _replayed_windows(journal, plan, family_id, replay, windows, window):
    stored = await _cpu(journal, _next, replay)
    ordinal = 0
    while window is not None:
        output, pending = [], []
        for packs in window:
            if stored is not None:
                if stored[0] != ordinal:
                    raise FamilyBlockUnavailable("SOURCE journal prefix is duplicated or has a gap")
                rows = await _cpu(
                    journal,
                    read_vectors,
                    journal,
                    OwnerCheckpointRecord("source_allocation", *stored),
                    plan,
                    family_id,
                    ordinal,
                    packs,
                    _checked_allocation_row,
                )
                output.append(rows)
                stored = await _cpu(journal, _next, replay)
            else:
                pending.append((ordinal, packs))
            ordinal += 1
        yield output, pending
        window = await _cpu(journal, _next, windows)
    if stored is not None:
        raise FamilyBlockUnavailable("SOURCE journal has extra original batches")


async def _allocate_original_output(session_factory, request, family_id, layout, plan, journal, limits, capture_scope):
    journal.current()
    with ExitStack() as stack:
        batches = stack.enter_context(
            closing(
                _pack_batches(
                    plan,
                    limits.maximum_batch_records,
                    limits.maximum_batch_bytes,
                    check=journal.current,
                )
            )
        )
        windows = stack.enter_context(closing(_allocation_windows(batches, limits, check=journal.current)))
        # Refuse an impossible first original cohort before heartbeat or native writes.
        window = await _cpu(journal, _next, windows)
        await _cpu(journal, journal.reconcile_budget)
        checkpoint = await _checkpoint(session_factory, request, family_id, layout, journal)
        head = checkpoint.head
        header = ("source-ids/v1", family_id, plan.sha256, plan.counts)
        journal.budget().ensure(len(msgpack.packb(header, use_bin_type=True)) + 8)
        work = SourceAllocationWork(journal, journal.root / "outputs" / ("attempt-" + uuid4().hex), limits.spool)
        path = work.path()
        spool_output = stack.enter_context(path.open("xb"))
        replay = stack.enter_context(replay_records(journal, checkpoint, plan))
        work.frame(spool_output, header, path)
        original_windows = _replayed_windows(journal, plan, family_id, replay, windows, window)
        async with aclosing(original_windows):
            async for output, pending in original_windows:
                if pending:
                    allocated_rows, head = await _owner_allocations(
                        session_factory,
                        request,
                        family_id,
                        layout,
                        _OwnerAllocationWindow(tuple(pending), capture_scope, head),
                        limits,
                        journal=journal,
                        plan=plan,
                    )
                    output.extend(allocated_rows)
                for allocated_rows in output:
                    await _cpu(journal, _write_rows, work, spool_output, path, allocated_rows)
        spool_output.flush()
        os.fsync(spool_output.fileno())
    if (await _checkpoint(session_factory, request, family_id, layout, journal)).head != head:
        raise FamilyBlockUnavailable("SOURCE journal protected head changed before output completion")
    return await _cpu(journal, _complete_plan, work, path, plan, family_id)


async def allocate_source_plan(
    session_factory,
    request,
    family_id,
    layout,
    plan,
    *,
    journal,
    limits,
):
    """Journal exact native vectors in their allocation transaction before escape.

    Retry reads current protected committed vectors; it rebuilds only the local
    output file. Unknown commit/read failures preserve every original and never
    authorize replacement allocation. Proven rollbacks may leave sequence gaps.
    """
    _checked_source(request, plan)
    if type(limits) is not SourceAllocationLimits or type(journal) is not SourceAllocationJournal:
        raise ValueError("SOURCE allocation spool budgets differ")
    if journal.busy or journal.work.limits != limits.spool:
        raise ValueError("SOURCE allocation journal is busy or has different data budgets")
    capture_scope = _plan_capture_binding(_checked_source(request, plan))
    journal.busy = True
    journal.stop.clear()
    try:
        return await _allocate_original_output(
            session_factory, request, family_id, layout, plan, journal, limits, capture_scope
        )
    finally:
        # Partial files retain the original registered journal owner.
        journal.busy = False


def _next(rows):
    return next(rows, None)


def _write_rows(work, target, path, rows):
    for row in rows:
        work.frame(target, row, path)


async def _checkpoint(factory, request, family_id, layout, journal):
    return await read_owner_checkpoint(
        factory,
        request,
        journal.build_id,
        family_id,
        layout,
        root=journal.checkpoints,
        limits=journal.control_limits,
    )


def _window_bound(limits):
    # Original packs, SQL arrays, actual results and vector encoding overlap.
    return min(limits.maximum_batch_bytes, limits.spool.memory_bytes // 4)


def _allocation_windows(batches, limits, *, check):
    window, charged = [], 0
    for packs in batches:
        check()
        original = sum(pack.landing.byte_count + 512 * len(pack.records) for pack in packs)
        arrays = sum(
            len(record.typed_parent_key[0].encode("utf-8")) + 56
            for pack in packs
            for record in pack.records
            if record.typed_parent_key is not None
        )
        arrays += 64 * sum(1 + len(pack.records) for pack in packs)
        charge = max(original, arrays)
        if charge > _window_bound(limits):
            raise FamilyBlockResourceLimit("SOURCE original allocation batch exceeds the window memory budget")
        if window and charged + charge > _window_bound(limits):
            yield tuple(window)
            window, charged = [], 0
        window.append(packs)
        charged += charge
    if window:
        yield tuple(window)


@asynccontextmanager
async def _source_window_session(factory, request, family_id, layout, journal, transition):
    async with store._block_session(
        factory,
        request,
        family_id,
        layout,
        "loading",
        loading=True,
        transition=transition,
    ) as (session, descriptor, binding):
        scope = await checkpoint_api.bulk_checkpoint_scope(
            session,
            request,
            journal.build_id,
            family_id,
            layout,
            descriptor,
            binding,
        )
        yield session, descriptor, scope


def _confirmed_window(journal, checkpoint, expected_head, plan, family_id, batches):
    """Confirm every original record in exactly one atomic committed suffix."""
    previous_revision = 0 if expected_head is None else expected_head.revision
    if checkpoint.head is None or checkpoint.head.revision != previous_revision + len(batches):
        raise FamilyBlockUnavailable("SOURCE allocation commit has no exact next protected head")
    rows = []
    with closing(checkpoint.records(check=journal.current)) as records:
        for ordinal, packs in reversed(batches):
            _head, record = next(records)
            rows.append(read_vectors(journal, record, plan, family_id, ordinal, packs, _checked_allocation_row))
        previous = next(records, None)
        if (None if previous is None else previous[0]) != expected_head:
            raise FamilyBlockUnavailable("SOURCE allocation commit changed its protected prefix")
    return tuple(reversed(rows))


async def _checked_capture_binding(session, request, expected):
    scope = (
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
            .where(CustomImportExecution.execution_id == request.execution_id),
        )
    ).one()
    if tuple(scope) != expected:
        raise FamilyBlockUnavailable("SOURCE allocation capture binding differs")


def _prepare_window_vectors(journal, plan, family_id, batches, rows, head, scope):
    """Retain each original vector and serial checkpoint before any native head advances."""
    offset, prepared = 0, None
    for ordinal, packs in batches:
        journal.current()
        batch_rows = rows[offset : offset + len(packs)]
        offset += len(packs)
        for pack, row in zip(packs, batch_rows, strict=True):
            _checked_allocation_row(pack, row)
        reserved = reserve_checkpoint(journal)
        record = write_vectors(journal, plan, family_id, ordinal, packs, batch_rows)
        prepared = checkpoint_api.prepare_checkpoint_append(
            journal.checkpoints,
            scope,
            record,
            head,
            head,
            journal.control_limits,
        )
        finish_checkpoint_charge(journal, reserved, prepared)
        head = prepared.head
    if offset != len(rows) or prepared is None:
        raise FamilyBlockUnavailable("SOURCE allocation window result census differs")
    return prepared


async def _window_native_rows(session, request, batches, limits):
    packs = tuple(pack for _ordinal, group in batches for pack in group)
    kinds = tuple(
        dict.fromkeys(
            kind
            for pack in packs
            for kind in (
                "pack",
                *(
                    "rejection" if k is None else "root_revision" if k == 1 else "child_revision"
                    for k in pack.allocation_kinds
                ),
            )
        )
    )
    sequences = await source_sequence_bindings(session, kinds) if packs else None
    root_batches = tuple(
        ordinal
        for ordinal, group in batches
        for pack in group
        for source_record in pack.records
        if source_record.typed_parent_key is not None
    )
    allocated_pack_rows = await _allocate_batch(
        session,
        request,
        packs,
        max(1, _window_bound(limits) // 512),
        _window_bound(limits),
        batch_ordinals=root_batches,
        sequences=sequences,
    )
    return allocated_pack_rows, sequences


async def _current_sequence_bindings(session, sequences):
    return await source_sequence_bindings(session, tuple(kind for kind in sequences if kind != "source_root"))


@dataclass(frozen=True)
class _OwnerAllocationWindow:
    """One exact pending native batch window and its original CAS observations."""

    batches: tuple
    capture_scope: tuple
    expected_head: object


async def _owner_allocations(session_factory, request, family_id, layout, window, limits, *, journal, plan):
    batches, capture_scope, expected_head = window.batches, window.capture_scope, window.expected_head
    prepared, transition = None, store.SourceAllocationTransition(journal.build_id, {})
    try:
        async with _source_window_session(
            session_factory,
            request,
            family_id,
            layout,
            journal,
            transition,
        ) as (session, descriptor, _scope):
            if checkpoint_api._head(descriptor) != expected_head:
                raise FamilyBlockUnavailable("SOURCE protected head changed before native allocation")
            await _checked_capture_binding(session, request, capture_scope)
            allocated_rows, sequences = await _window_native_rows(session, request, batches, limits)
            prepared = await _cpu(
                journal,
                _prepare_window_vectors,
                journal,
                plan,
                family_id,
                batches,
                allocated_rows,
                expected_head,
                _scope,
            )
            if sequences is not None and await _current_sequence_bindings(session, sequences) != sequences:
                raise FamilyBlockUnavailable("SOURCE native sequence bindings changed before commit")
            transition.values.update(
                owner_checkpoint_revision=prepared.head.revision, owner_checkpoint_sha256=prepared.head.sha256
            )
    except Exception as failure:
        try:
            checkpoint = await _checkpoint(session_factory, request, family_id, layout, journal)
        except BaseException as reconcile:
            raise FamilyBlockUnavailable(
                "SOURCE allocation commit is unknown; retained reconciliation is required"
            ) from reconcile
        if checkpoint.head == expected_head:
            raise failure
        allocated_rows = await _cpu(
            journal, _confirmed_window, journal, checkpoint, expected_head, plan, family_id, batches
        )
        return allocated_rows, checkpoint.head
    checkpoint = await _checkpoint(session_factory, request, family_id, layout, journal)
    if prepared is None or checkpoint.head != prepared.head:
        raise FamilyBlockUnavailable("SOURCE allocation prepared head is not freshly committed")
    allocated_rows = await _cpu(
        journal, _confirmed_window, journal, checkpoint, expected_head, plan, family_id, batches
    )
    return allocated_rows, checkpoint.head


def _complete_plan(work, path, plan, family_id):
    ready = work.root / _NAME
    path.rename(ready)
    ready.chmod(0o400)
    with ready.open("rb") as spool_input:
        digest = _plan_digest(spool_input, work._io_check)
    directory = os.open(work.root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    observed = ready.stat()
    return AllocatedSourcePlan(
        plan,
        str(work.root),
        work.identity.st_dev,
        work.identity.st_ino,
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        digest,
        work.limits.max_record_bytes,
        family_id,
    )


def _owned_root(plan):
    if type(plan) is not AllocatedSourcePlan:
        raise ValueError("SOURCE allocation descriptor differs")
    root = Path(plan.root)
    observed = root.lstat()
    if not stat.S_ISDIR(observed.st_mode) or (observed.st_dev, observed.st_ino) != (plan.device, plan.inode):
        raise ValueError("SOURCE allocation root custody differs")
    return root


def _checked_allocation_row(pack, row):
    if (
        type(row) is not tuple
        or len(row) != 5
        or type(row[0]) is not int
        or row[0] != pack.pack_index
        or type(row[1]) is not int
        or not 0 < row[1] < 2**63
        or any(type(cells) is not tuple or len(cells) != len(pack.records) for cells in row[2:])
    ):
        raise ValueError("SOURCE allocation pack census differs")
    for index, (source_record, kind) in enumerate(zip(pack.records, pack.allocation_kinds, strict=True)):
        expected = (source_record.typed_parent_key is not None, kind is not None, kind is None)
        cells = tuple(field_ids[index] for field_ids in row[2:])
        if any(
            (type(allocated_id) is not int or not 0 < allocated_id < 2**63) if present else allocated_id is not None
            for allocated_id, present in zip(cells, expected, strict=True)
        ):
            raise ValueError("SOURCE allocation occurrence identity differs")


def read_source_allocations(plan, *, check=None):
    """Join every observed native ID back to its exact original occurrence."""
    if check is not None and not callable(check):
        raise ValueError("SOURCE allocation replay cancellation check differs")
    root = _owned_root(plan)
    if {path.name for path in root.iterdir()} != {_NAME}:
        raise ValueError("SOURCE allocation file census differs")
    with (
        os.fdopen(os.open(root / _NAME, os.O_RDONLY | os.O_NOFOLLOW), "rb") as allocation_file,
        closing(read_source_plan(plan.source, check=check)) as packs,
    ):
        observed = os.fstat(allocation_file.fileno())
        if not stat.S_ISREG(observed.st_mode) or (
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
            stat.S_IMODE(observed.st_mode),
        ) != (plan.file_device, plan.file_inode, plan.file_bytes, 0o400):
            raise ValueError("SOURCE allocation file custody differs")
        if _plan_digest(allocation_file, check) != plan.sha256:
            raise ValueError("SOURCE allocation digest differs")
        allocation_file.seek(0)
        if _read_frame(allocation_file, plan.max_frame_bytes) != (
            "source-ids/v1",
            plan.snapshot_family_id,
            plan.source.sha256,
            plan.source.counts,
        ):
            raise ValueError("SOURCE allocation header scope differs")
        for pack in packs:
            allocation_row = _read_frame(allocation_file, plan.max_frame_bytes)
            if check is not None:
                check()
            _checked_allocation_row(pack, allocation_row)
            yield pack, allocation_row[1], allocation_row[2], allocation_row[3], allocation_row[4]
        if allocation_file.read(1):
            raise ValueError("SOURCE allocation has trailing identities")
        allocation_file.seek(0)
        if _plan_digest(allocation_file, check) != plan.sha256:
            raise ValueError("SOURCE allocation changed during replay")


def cleanup_source_allocations(plan):
    """Remove only the registered allocation directory with unchanged custody."""
    shutil.rmtree(_owned_root(plan))


def _read_frame(source, maximum_bytes):
    prefix = source.read(8)
    if len(prefix) != 8 or not 0 < (size := _LENGTH.unpack(prefix)[0]) <= maximum_bytes:
        raise ValueError("SOURCE allocation frame differs")
    payload = source.read(size)
    if len(payload) != size:
        raise ValueError("SOURCE allocation frame is truncated")
    return msgpack.unpackb(payload, raw=False, use_list=False, strict_map_key=True)
