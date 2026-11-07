# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained SOURCE-ready files, bound to the existing protected owner journal.

This is same-volume process recovery after SOURCE allocation, not allocation
recovery or permission to resume an owner. Callers retain their original owners
on failure, repin the base separately, and stop/drain consumers before cleanup.
No session, secret, deadline, borrowed reader or verification proof is persisted.
"""

import hashlib
import os
import stat
import struct
import threading
from collections import deque
from contextlib import closing
from dataclasses import astuple, dataclass, replace
from pathlib import Path
from types import SimpleNamespace

import msgpack

from process.custom_import import family_shard_common as common_api
from process.custom_import import family_shard_source_journal as source_journal
from process.custom_import.family_shard_allocate import (
    AllocatedSourcePlan,
    _checked_source,
    _checked_source_provenance,
    read_source_allocations,
)
from process.custom_import.family_shard_capture import PreparedCaptureSource, _load_retained_source
from process.custom_import.family_shard_checkpoint import (
    OwnerCheckpointRecord,
    append_owner_checkpoint,
    read_owner_checkpoint,
)
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_input import PartitionedCapture, manifest_document, read_partition
from process.custom_import.family_shard_metadata import (
    OwnerMetadataSeal,
    owner_metadata_scope,
    reopen_owner_metadata,
    seal_owner_metadata,
)
from process.custom_import.family_shard_plan import _cpu
from process.custom_import.family_shard_route import RoutedShard, read_routed_shard
from process.custom_import.family_shard_source_plan import SourcePlanDescriptor, _scope
from process.custom_import.family_shard_tasks import (
    PreparedSourceTasks,
    SourceOutputCustody,
    SourceTaskFile,
    SourceTaskScope,
    _BorrowedWork,
    _prepared_sha256,
    prepare_source_tasks,
)
from process.custom_import.family_shard_tasks import (
    _scope as task_scope,
)
from process.custom_import.runner_types import CancellationRequested

_CONTRACT = "custom-import/source-ready/v2"
_LEGACY_CONTRACT = "custom-import/source-ready/v1"
_OWNER_READY = "custom-import/owner-source-ready/v1"
_SOURCE_OUTPUT = "custom-import/owner-source-output/v1"
_DESCRIPTOR = "source-ready.mpk"
_LENGTH = struct.Struct("!Q")
_ROLES = ("input", "plan", "allocations", "routed", "metadata", "tasks")
_COUNTS = ("records", "packs", "revisions", "missing_family", "missing_query")


@dataclass(frozen=True)
class SourceReadyArtifacts:
    """Closed custody; only a freshly authenticated journal authorizes reopening."""

    root: str
    prepared: PreparedCaptureSource
    allocated: AllocatedSourcePlan
    shards: tuple
    metadata: OwnerMetadataSeal
    tasks: PreparedSourceTasks
    descriptor_sha256: bytes
    contract: str = _CONTRACT


def _uint(value, *, positive=False):
    if type(value) is not int or not int(positive) <= value < 2**63:
        raise ValueError("SOURCE-ready integer differs")
    return value


def _tuple(value, length=None):
    if type(value) is not tuple or (length is not None and len(value) != length):
        raise ValueError("SOURCE-ready tuple differs")
    return value


def _digest(value):
    if type(value) is not bytes or len(value) != 32:
        raise ValueError("SOURCE-ready digest differs")
    return value


def _hex_digest(value):
    if type(value) is not str or len(value) != 64 or bytes.fromhex(value).hex() != value:
        raise ValueError("SOURCE-ready hexadecimal digest differs")
    return value


def _typed_stamp(value, length):
    for cell in _tuple(value, length):
        _uint(cell)
    return value


def _name(value):
    if type(value) is not str or not value or Path(value).name != value or value in (".", ".."):
        raise ValueError("SOURCE-ready filename differs")
    return value


def _stamp(observed, *, directory=False):
    expected_mode = 0o700 if directory else 0o400
    predicate = stat.S_ISDIR if directory else stat.S_ISREG
    if (
        not predicate(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != expected_mode
        or observed.st_uid != os.getuid()
        or (not directory and observed.st_nlink != 1)
    ):
        raise ValueError("SOURCE-ready file type, owner or mode differs")
    return observed.st_dev, observed.st_ino, observed.st_mode, observed.st_uid, observed.st_size


def _directory(path):
    observed = path.lstat()
    _stamp(observed, directory=True)
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if _stamp(os.fstat(descriptor), directory=True)[:4] != _stamp(observed, directory=True)[:4]:
            raise ValueError("SOURCE-ready opened directory differs")
    finally:
        os.close(descriptor)
    return observed.st_dev, observed.st_ino, observed.st_mode, observed.st_uid


def _sync(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _check(check):
    if check is not None:
        check()


def _hash_file(path, *, check=None):
    before = _stamp(path.lstat())
    digest = hashlib.sha256()
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
        if _stamp(os.fstat(source.fileno())) != before:
            raise ValueError("SOURCE-ready opened file differs")
        while block := source.read(65536):
            _check(check)
            digest.update(block)
        if _stamp(os.fstat(source.fileno())) != before or _stamp(path.lstat()) != before:
            raise ValueError("SOURCE-ready file changed during replay")
    return before, digest.digest()


def _copy_tree(source_root, target_root, limits, check):
    """Copy closed originals; never transfer their cleanup ownership or hardlinks."""
    original = source_root.lstat()
    original_identity = (
        original.st_dev,
        original.st_ino,
        original.st_mode,
        original.st_uid,
        original.st_mtime_ns,
        original.st_ctime_ns,
    )
    if not stat.S_ISDIR(original.st_mode) or original.st_uid != os.getuid():
        raise ValueError("SOURCE-ready original directory differs")
    names = tuple(sorted(entry.name for entry in source_root.iterdir()))
    target_root.mkdir(mode=0o700)
    total = 0
    for name in names:
        _check(check)
        path, copied = source_root / _name(name), target_root / name
        observed = path.lstat()
        if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_nlink != 1:
            raise ValueError("SOURCE-ready original file custody differs")
        identity = (observed.st_dev, observed.st_ino, observed.st_size, observed.st_mtime_ns, observed.st_ctime_ns)
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as src, copied.open("xb") as dst:
            for opened in (os.fstat(src.fileno()), path.lstat()):
                if (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns) != identity:
                    raise ValueError("SOURCE-ready original open identity differs")
            while block := src.read(min(65536, limits.memory_bytes)):
                _check(check)
                total += len(block)
                if total > limits.max_spool_bytes:
                    raise ValueError("SOURCE-ready copy exceeds its spool budget")
                dst.write(block)
            dst.flush()
            os.fchmod(dst.fileno(), 0o400)
            os.fsync(dst.fileno())
            for current in (os.fstat(src.fileno()), path.lstat()):
                if (
                    current.st_dev,
                    current.st_ino,
                    current.st_size,
                    current.st_mtime_ns,
                    current.st_ctime_ns,
                ) != identity:
                    raise ValueError("SOURCE-ready original changed during copying")
    current = source_root.lstat()
    if (
        names != tuple(sorted(entry.name for entry in source_root.iterdir()))
        or (current.st_dev, current.st_ino, current.st_mode, current.st_uid, current.st_mtime_ns, current.st_ctime_ns)
        != original_identity
    ):
        raise ValueError("SOURCE-ready original directory changed during copying")
    _sync(target_root)


def _relocate_file(descriptor, root, name, *, source=None):
    directory, observed = root.lstat(), (root / name).lstat()
    descriptor_fields_by_name = dict(
        root=str(root),
        device=directory.st_dev,
        inode=directory.st_ino,
        file_device=observed.st_dev,
        file_inode=observed.st_ino,
        file_bytes=observed.st_size,
    )
    if source is not None:
        descriptor_fields_by_name["source"] = source
    return replace(descriptor, **descriptor_fields_by_name)


def _plan_cells(plan):
    return (
        plan.device,
        plan.inode,
        plan.file_device,
        plan.file_inode,
        plan.file_bytes,
        plan.sha256,
        plan.partition_sha256,
        plan.scope,
        plan.stream_slots,
        plan.collection_slots,
        plan.source_counts,
        plan.counts,
        plan.max_frame_bytes,
    )


def _allocation_cells(source):
    return (
        source.device,
        source.inode,
        source.file_device,
        source.file_inode,
        source.file_bytes,
        source.sha256,
        source.max_frame_bytes,
        source.snapshot_family_id,
    )


def _task_cells(task):
    shard = task.shard
    return (
        (
            shard.root_device,
            shard.root_inode,
            shard.name,
            shard.file_device,
            shard.file_inode,
            shard.file_bytes,
            shard.sha256,
            shard.records,
            shard.shard_id,
            shard.shard_count,
            shard.partition_sha256,
            shard.definition_sha256,
            shard.max_record_bytes,
        ),
        (task.name, task.device, task.inode, task.byte_count, task.sha256),
    )


def _frame(target, value, limits):
    payload = msgpack.packb(value, use_bin_type=True)
    if len(payload) > min(limits.max_record_bytes, limits.memory_bytes):
        raise ValueError("SOURCE-ready descriptor frame exceeds its resource budget")
    target.write(_LENGTH.pack(len(payload)))
    target.write(payload)
    if target.tell() > limits.max_spool_bytes:
        raise ValueError("SOURCE-ready descriptor exceeds its spool budget")


def _read_frames(source, limits, check):
    while prefix := source.read(8):
        _check(check)
        if len(prefix) != 8 or not 0 < (size := _LENGTH.unpack(prefix)[0]) <= min(
            limits.max_record_bytes, limits.memory_bytes
        ):
            raise ValueError("SOURCE-ready descriptor frame differs")
        payload = source.read(size)
        if len(payload) != size:
            raise ValueError("SOURCE-ready descriptor frame is truncated")
        yield msgpack.unpackb(
            payload,
            raw=False,
            use_list=False,
            strict_map_key=True,
            max_array_len=256,
            max_map_len=0,
            max_str_len=4096,
            max_bin_len=limits.max_record_bytes,
        )


def _write_descriptor(ready, limits, check):
    root, partition = Path(ready.root), ready.prepared.partition
    pending = root / "source-ready.pending"
    files, total = 0, 0
    with pending.open("xb") as descriptor_stream:
        _frame(
            descriptor_stream,
            (
                "header",
                ready.contract,
                str(root),
                _directory(root),
                (partition.sha256, partition.owner, partition.device, partition.inode, partition.storage_sha256),
                _plan_cells(ready.prepared.plan),
                _allocation_cells(ready.allocated),
                (ready.metadata.descriptor_sha256, ready.metadata.counts),
                tuple((name, ready.tasks.counts[name]) for name in _COUNTS),
                _prepared_sha256(ready.tasks),
            ),
            limits,
        )
        for role in _ROLES:
            _frame(descriptor_stream, ("directory", role, _directory(root / role)), limits)
            for path in sorted((root / role).iterdir()):
                stamp, digest = _hash_file(path, check=check)
                files, total = files + 1, total + stamp[-1]
                if total > limits.max_spool_bytes:
                    raise ValueError("SOURCE-ready retained files exceed their spool budget")
                _frame(descriptor_stream, ("file", role, path.name, stamp, digest), limits)
        for task in ready.tasks.files:
            _frame(descriptor_stream, ("task", *_task_cells(task)), limits)
        _frame(descriptor_stream, ("end", files, len(ready.tasks.files), total), limits)
        descriptor_stream.flush()
        os.fchmod(descriptor_stream.fileno(), 0o400)
        os.fsync(descriptor_stream.fileno())
    pending.rename(root / _DESCRIPTOR)
    _sync(root)
    _sync(root.parent)
    return _hash_file(root / _DESCRIPTOR, check=check)[1]


def _validate_ready_eof(ready, request, metadata_work):
    prepared, allocated_source = ready.prepared, ready.allocated
    document = manifest_document(prepared.partition)
    with closing(read_partition(prepared.partition, check=metadata_work.check)) as partition_rows:
        count = sum(1 for _row in partition_rows)
    if count != document["records"] or count != allocated_source.source.counts[0]:
        raise ValueError("SOURCE-ready partition EOF census differs")
    with closing(read_source_allocations(allocated_source, check=metadata_work.check)) as allocations:
        deque(allocations, maxlen=0)
    with reopen_owner_metadata(
        ready.metadata.root,
        expected_scope=owner_metadata_scope(request, allocated_source),
        descriptor_sha256=ready.metadata.descriptor_sha256,
        work=metadata_work,
    ) as metadata:
        counts = (
            ("family", sum(1 for _row in metadata.family_ids())),
            ("query", sum(1 for _row in metadata.query_keys())),
        )
        if counts != ready.metadata.counts:
            raise ValueError("SOURCE-ready owner metadata typed counts differ")
    return count


def _validate(ready, request, registry, family_id, *, limits, worker_limits, metadata_work):
    prepared, allocated_source, tasks = ready.prepared, ready.allocated, ready.tasks
    if type(prepared) is not PreparedCaptureSource or type(prepared.partition) is not PartitionedCapture:
        raise ValueError("SOURCE-ready original capture producer differs")
    prepared.source.headers(request.definition)
    if (
        prepared.partition.identity != prepared.source.identity
        or prepared.plan != allocated_source.source
        or prepared.plan.scope != _scope(prepared.partition, request)
        or prepared.plan.partition_sha256 != prepared.partition.sha256
        or prepared.plan.stream_slots != tuple(sorted(registry.stream_slots.items()))
        or prepared.plan.collection_slots != tuple(sorted(registry.child_collection_slots.items()))
        or allocated_source.snapshot_family_id != family_id
        or tasks.source != allocated_source
        or tasks.definition_canonical != request.definition.canonical
        or tasks.complete_scope is not request.complete_scope
    ):
        raise ValueError("SOURCE-ready capture, allocation or task scope differs")
    _checked_source_provenance(request, allocated_source.source)
    if (
        prepared.plan.max_frame_bytes > limits.max_record_bytes
        or allocated_source.max_frame_bytes > limits.max_record_bytes
        or any(shard.max_record_bytes > limits.max_record_bytes for shard in ready.shards)
    ):
        raise ValueError("SOURCE-ready original frame budgets exceed current resources")
    task_scope(ready.shards, allocated_source, request.definition)
    count = _validate_ready_eof(ready, request, metadata_work)
    revision_count = 0
    for shard, task in zip(ready.shards, tasks.files, strict=True):
        if task.shard != shard:
            raise ValueError("SOURCE-ready task routed identity differs")
        reader = _BorrowedWork(Path(shard.root), (shard.root_device, shard.root_inode), limits)
        with closing(read_routed_shard(shard, reader)) as partition_rows:
            deque(partition_rows, maxlen=0)
        revision_count += len(tasks.load_plan(shard.shard_id, worker_limits).record_ids)
    counts = tasks.counts
    if (
        tuple(counts) != _COUNTS
        or any(type(task_count) is not int or task_count < 0 for task_count in counts.values())
        or (counts["records"], counts["packs"], counts["revisions"])
        != (
            allocated_source.source.counts[0],
            allocated_source.source.counts[1],
            sum(allocated_source.source.counts[2:4]),
        )
        or revision_count != counts["revisions"]
        or max(counts["missing_family"], counts["missing_query"]) > count
    ):
        raise ValueError("SOURCE-ready task typed counts differ")


def _copy_ready_inputs(prepared, allocated, shards, storage, metadata_work, canonical=False):
    root, limits = Path(storage.root).absolute(), storage.limits
    for role, original in (
        ("input", prepared.partition.root),
        ("plan", prepared.plan.root),
        ("allocations", allocated.root),
        ("routed", shards[0].root),
    ):
        if role == "routed" and any(shard.root != original for shard in shards):
            raise ValueError("SOURCE-ready routed cohort roots differ")
        if not canonical or role == "allocations":
            _copy_tree(Path(original), root / role, limits, metadata_work.check)
    if canonical:
        copied_source = _relocate_file(allocated, root / "allocations", "source-ids.mpk", source=prepared.plan)
        return prepared.partition, prepared.plan, copied_source, shards
    partition = replace(
        prepared.partition,
        root=root / "input",
        device=(root / "input").stat().st_dev,
        inode=(root / "input").stat().st_ino,
    )
    plan = _relocate_file(prepared.plan, root / "plan", "source-plan.mpk")
    copied_source = _relocate_file(allocated, root / "allocations", "source-ids.mpk", source=plan)
    routed_root = (root / "routed").stat()
    routed_shards = tuple(
        replace(
            shard,
            root=str(root / "routed"),
            root_device=routed_root.st_dev,
            root_inode=routed_root.st_ino,
            file_device=(root / "routed" / shard.name).stat().st_dev,
            file_inode=(root / "routed" / shard.name).stat().st_ino,
        )
        for shard in shards
    )
    return partition, plan, copied_source, routed_shards


def _prepare_ready_tasks(metadata, request, copied_source, routed_shards, storage, metadata_work):
    root, limits, register_cleanup = Path(storage.root).absolute(), storage.limits, storage.register_cleanup
    sealed = seal_owner_metadata(
        metadata,
        request,
        copied_source,
        root=root / "metadata",
        register_cleanup=register_cleanup,
        work=metadata_work,
    )
    with reopen_owner_metadata(
        sealed.root,
        expected_scope=sealed.scope,
        descriptor_sha256=sealed.descriptor_sha256,
        work=metadata_work,
    ) as copied_metadata:
        tasks = prepare_source_tasks(
            routed_shards,
            copied_source,
            SourceTaskScope(request.definition, request.complete_scope, metadata_work.check),
            copied_metadata.family_ids(),
            copied_metadata.query_keys(),
            root=root / "tasks",
            register_cleanup=register_cleanup,
            limits=limits,
        )
    tasks._work = _BorrowedWork(root / "tasks", (tasks._work.identity.st_dev, tasks._work.identity.st_ino), limits)
    tasks._work._io_check = metadata_work.check
    return sealed, tasks


def _canonical_producers(root, prepared, allocated, shards, context, owner_lock, producer_directory):
    """Reuse only the actual held owner's fixed, already registered producer roots."""
    from process.custom_import.family_shard_owner_lock import OwnerJobLock

    if type(owner_lock) is not OwnerJobLock or context.owner_profile is None:
        raise ValueError("SOURCE-ready canonical producers require their actual owner")
    owner_lock.check()
    if root != owner_lock.root / "source/ready" or _directory(root) != producer_directory:
        raise ValueError("SOURCE-ready canonical parent custody differs")
    if (
        Path(prepared.partition.root) != root / "input"
        or Path(prepared.plan.root) != root / "plan"
        or any(Path(shard.root) != root / "routed" for shard in shards)
        or Path(allocated.root).parent != root.parent / "allocation-journal/outputs"
        or {entry.name for entry in root.iterdir()} != {"input", "plan", "routed"}
    ):
        raise ValueError("SOURCE-ready canonical producer scope differs")


def _ready_root(prepared, allocated, shards, metadata, context, storage, metadata_work, reuse_canonical=False):
    """Check original descriptors and disjoint ownership before any new ready file."""
    if type(storage.limits) is not SourceFactLimits or not callable(storage.register_cleanup):
        raise ValueError("SOURCE-ready requires bounded registered storage")
    if type(prepared) is not PreparedCaptureSource or type(allocated) is not AllocatedSourcePlan:
        raise ValueError("SOURCE-ready requires actual original producer descriptors")
    _checked_source(context.request, allocated.source)
    task_scope(shards, allocated, context.request.definition)
    root = Path(storage.root).absolute()
    originals = tuple(
        Path(path).resolve()
        for path in (
            prepared.partition.root,
            prepared.plan.root,
            allocated.root,
            metadata.work.root,
            *(shard.root for shard in shards),
            metadata_work.root,
        )
    )
    canonical_roots = {root / role for role in ("input", "plan", "routed")} if reuse_canonical else set()
    if any(
        path not in canonical_roots
        and (root.resolve() == path or root.resolve() in path.parents or path in root.resolve().parents)
        for path in originals
    ):
        raise ValueError("SOURCE-ready retained and producer roots overlap")
    return root


def _finish_source_ready(prepared, allocated, shards, metadata, context, storage, worker_limits, metadata_work):
    """Seal metadata and tasks only after every original producer has closed."""
    sealed, tasks = _prepare_ready_tasks(metadata, context.request, allocated, shards, storage, metadata_work)
    ready = SourceReadyArtifacts(str(Path(storage.root).absolute()), prepared, allocated, shards, sealed, tasks, b"")
    _validate(
        ready,
        context.request,
        context.registry,
        allocated.snapshot_family_id,
        limits=storage.limits,
        worker_limits=worker_limits,
        metadata_work=metadata_work,
    )
    return replace(ready, descriptor_sha256=_write_descriptor(ready, storage.limits, metadata_work.check))


def prepare_source_ready(prepared, allocated, shards, metadata, context, storage, *, worker_limits, metadata_work):
    """Copy independently owned originals into a new registered ready root.

    Run off the event loop with the original cancellation/deadline check. Failed
    or unanchored files remain caller-owned and never authorize new allocation.
    """
    root = _ready_root(prepared, allocated, shards, metadata, context, storage, metadata_work)
    if root.exists() or root.is_symlink():
        raise FileExistsError("SOURCE-ready retained root already exists")
    storage.register_cleanup(root)
    root.mkdir(mode=0o700)
    partition, plan, copied_source, routed_shards = _copy_ready_inputs(
        prepared, allocated, shards, storage, metadata_work
    )
    return _finish_source_ready(
        PreparedCaptureSource(prepared.source, partition, plan),
        copied_source,
        routed_shards,
        metadata,
        context,
        storage,
        worker_limits,
        metadata_work,
    )


def prepare_owned_source_ready(owner, storage, *, metadata_work):
    """Borrow the held owner's fixed canonical producers without copying their bytes."""
    prepared, allocated, shards, metadata = owner.prepared, owner.allocated, owner.routed, owner.metadata
    root = _ready_root(prepared, allocated, shards, metadata, owner.context, storage, metadata_work, True)
    _canonical_producers(root, prepared, allocated, shards, owner.context, owner.owner_lock, owner.ready_directory)
    partition, plan, copied_source, routed_shards = _copy_ready_inputs(
        prepared, allocated, shards, storage, metadata_work, True
    )
    return _finish_source_ready(
        PreparedCaptureSource(prepared.source, partition, plan),
        copied_source,
        routed_shards,
        metadata,
        owner.context,
        storage,
        owner.limits.worker,
        metadata_work,
    )


def source_ready_record(ready, custody=None):
    """Prepare the existing descriptor, optionally bound to a complete original owner."""
    if type(ready) is not SourceReadyArtifacts:
        raise ValueError("SOURCE-ready checkpoint candidate differs")
    root = Path(ready.root)
    stamp, digest = _hash_file(root / _DESCRIPTOR)
    if digest != _digest(ready.descriptor_sha256):
        raise ValueError("SOURCE-ready descriptor changed before anchoring")
    payload = (ready.contract, str(root), _directory(root), stamp)
    if custody is not None:
        if type(custody) is not source_journal.SourceOwnerCustody or root != Path(custody.directories[1][0]) / "ready":
            raise ValueError("SOURCE-ready retained root differs from its owner")
        custody.current(custody.directories[0][0], custody.profile)
        for path, _identity in custody.directories:
            _sync(Path(path))
        payload = (_OWNER_READY, payload, astuple(custody), len(ready.tasks.files))
    return OwnerCheckpointRecord(
        "artifact",
        0,
        digest,
        (("descriptor_sha256", digest), ("prepared_sha256", _prepared_sha256(ready.tasks))),
        msgpack.packb(payload, use_bin_type=True),
    )


async def append_source_ready_checkpoint(session, context, ready, *, storage, expected_head):
    """Append in the caller's owner transaction; the result is NEVER commit proof."""
    request, build_id, family_id, layout = context.request, context.build_id, context.family_id, context.layout
    if type(ready) is not SourceReadyArtifacts or ready.allocated.snapshot_family_id != family_id:
        raise ValueError("SOURCE-ready checkpoint candidate differs")
    record = source_ready_record(ready)
    return await append_owner_checkpoint(
        session,
        request,
        build_id,
        family_id,
        layout,
        storage=storage,
        record=record,
        expected_head=expected_head,
    )


def _read_plan(root, cells):
    cells = _tuple(cells, 13)
    for plan_document in (*cells[:5], cells[12]):
        _uint(plan_document, positive=True)
    _digest(cells[5])
    _hex_digest(cells[6])
    for plan_document in cells[7:12]:
        _tuple(plan_document)
    scope = _tuple(cells[7], 6)
    capture_by_field = dict(_tuple(scope[0], 9))
    if tuple(capture_by_field) != tuple(
        sorted(
            (
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "capture_bundle_id",
                "source_binding_revision_id",
                "definition_sha256",
                "schema_sha256",
                "source_binding_sha256",
                "manifest_sha256",
            )
        )
    ):
        raise ValueError("SOURCE-ready capture field names differ")
    for name, plan_document in capture_by_field.items():
        if name.startswith("source_binding_") and plan_document is None:
            continue
        (_uint(plan_document, positive=True) if name.endswith("_id") else _hex_digest(plan_document))
    for plan_document in (scope[1], scope[2], scope[4], scope[5]):
        _uint(plan_document, positive=True)
    _digest(scope[3])
    for slots in cells[8:10]:
        for name, slot in slots:
            if type(name) is not str or not name:
                raise ValueError("SOURCE-ready slot name differs")
            _uint(slot, positive=True)
    for slot, count in cells[10]:
        _uint(slot, positive=True)
        _uint(count)
    for count in _tuple(cells[11], 6):
        _uint(count)
    return SourcePlanDescriptor(str(root / "plan"), *cells)


def _read_allocation(root, plan, cells):
    cells = _tuple(cells, 8)
    for value in (*cells[:5], *cells[6:]):
        _uint(value, positive=True)
    _digest(cells[5])
    return AllocatedSourcePlan(plan, str(root / "allocations"), *cells)


def _read_task(root, cells):
    routed, task = _tuple(cells, 2)
    routed, task = _tuple(routed, 13), _tuple(task, 5)
    for value in (*routed[:2], *routed[3:5], routed[9], routed[12], *task[1:3]):
        _uint(value, positive=True)
    for value in (routed[5], routed[7], routed[8], task[3]):
        _uint(value)
    _name(routed[2])
    _name(task[0])
    _digest(routed[6])
    _digest(task[4])
    _hex_digest(routed[10])
    _hex_digest(routed[11])
    shard = RoutedShard(str(root / "routed"), *routed)
    return SourceTaskFile(shard, *task)


def _checkpoint_payload(checkpoint_record):
    """Validate the closed control envelope; reopening still authenticates every file."""
    if (
        type(checkpoint_record) is not OwnerCheckpointRecord
        or checkpoint_record.kind != "artifact"
        or checkpoint_record.ordinal != 0
        or tuple(name for name, _value in checkpoint_record.commitments) != ("descriptor_sha256", "prepared_sha256")
        or checkpoint_record.input_sha256 != checkpoint_record.commitments[0][1]
    ):
        raise ValueError("SOURCE-ready checkpoint record differs")
    descriptor_payload = _tuple(
        msgpack.unpackb(
            checkpoint_record.payload,
            raw=False,
            use_list=False,
            strict_map_key=True,
            max_array_len=16,
            max_map_len=0,
            max_str_len=4096,
            max_bin_len=0,
        ),
        4,
    )
    if descriptor_payload[0] == _OWNER_READY:
        _owner_ready_payload(descriptor_payload)
        descriptor_payload = _tuple(descriptor_payload[1], 4)
    if (
        descriptor_payload[0] not in {_CONTRACT, _LEGACY_CONTRACT}
        or type(descriptor_payload[1]) is not str
        or not Path(descriptor_payload[1]).is_absolute()
        or "\0" in descriptor_payload[1]
    ):
        raise ValueError("SOURCE-ready checkpoint contract or root differs")
    _typed_stamp(descriptor_payload[2], 4)
    _typed_stamp(descriptor_payload[3], 5)
    return descriptor_payload


def _owner_ready_payload(payload):
    _tuple(payload, 4)
    if payload[0] != _OWNER_READY:
        raise ValueError("SOURCE recovery has no protected complete owner")
    custody = source_journal.SourceOwnerCustody(*_tuple(payload[2], 3))
    retained = _tuple(payload[1], 4)
    _uint(payload[3], positive=True)
    if payload[3] > 4096 or retained[1] != str(Path(custody.directories[1][0]) / "ready"):
        raise ValueError("SOURCE owner task cohort or retained root differs")
    return custody, payload[3]


def source_output_record(ready_record, output):
    """Bind closed worker custody to the exact protected retained task scope."""
    _checkpoint_payload(ready_record)
    if (
        type(output) is not SourceOutputCustody
        or output.scope_sha256 != dict(ready_record.commitments)["prepared_sha256"]
    ):
        raise ValueError("SOURCE output custody belongs to different prepared tasks")
    payload = msgpack.packb((_SOURCE_OUTPUT, ready_record.input_sha256, astuple(output)), use_bin_type=True)
    return OwnerCheckpointRecord(
        "artifact",
        output.artifact[0],
        hashlib.sha256(payload).digest(),
        (("prepared_sha256", output.scope_sha256), ("source_ready", ready_record.input_sha256)),
        payload,
    )


def _source_output_payload(record, cells):
    _tuple(cells, 3)
    if (
        record.kind != "artifact"
        or cells[0] != _SOURCE_OUTPUT
        or tuple(name for name, _value in record.commitments) != ("prepared_sha256", "source_ready")
        or record.input_sha256 != hashlib.sha256(record.payload).digest()
        or dict(record.commitments)["source_ready"] != _digest(cells[1])
    ):
        raise ValueError("SOURCE output checkpoint commitment differs")
    output = SourceOutputCustody(*_tuple(cells[2], 4))
    if output.artifact[0] != record.ordinal or output.scope_sha256 != dict(record.commitments)["prepared_sha256"]:
        raise ValueError("SOURCE output checkpoint scope differs")
    return cells[1], output


def _ready_outputs(ready_record, custody, count, outputs, common):
    prepared = dict(ready_record.commitments)["prepared_sha256"]
    source_root = Path(custody.directories[1][0])
    for shard, (digest, output) in outputs.items():
        if shard >= count or digest != ready_record.input_sha256 or output.scope_sha256 != prepared:
            raise ValueError("SOURCE recovery output belongs to another cohort")
        if Path(output.root).parent != source_root:
            raise ValueError("SOURCE recovery output is outside its owned directory")
    if common is not None:
        if common[0] != ready_record.input_sha256 or set(outputs) != set(range(count)):
            raise ValueError("SOURCE common custody lacks its complete original worker cohort")
        common_api.source_common_scope(common[1], source_root, count)


def source_owner_checkpoint(snapshot, root, profile, check):
    """Read only the closed SOURCE suffix; metadata prefix proof remains mandatory."""
    from process.custom_import.family_shard_events_journal import AdmissionSourcePrefix

    ready, outputs, common = None, {}, None
    prefix = AdmissionSourcePrefix(snapshot, profile[2][1][1])
    for _head, checkpoint_record in prefix.records(check=check):
        cells = None
        if checkpoint_record.kind == "artifact":
            cells = msgpack.unpackb(
                checkpoint_record.payload,
                raw=False,
                use_list=False,
                strict_map_key=True,
                max_array_len=256,
                max_map_len=0,
                max_str_len=4096,
                max_bin_len=snapshot.limits.max_record_bytes,
            )
            _tuple(cells)
        if ready is None and not outputs and common is None and cells and cells[0] == common_api._CUSTODY:
            common = common_api.source_common_payload(checkpoint_record, cells)
            continue
        if ready is None and cells and cells[0] == _SOURCE_OUTPUT:
            if checkpoint_record.ordinal in outputs or len(outputs) >= 4096:
                raise ValueError("SOURCE output checkpoint is duplicated")
            outputs[checkpoint_record.ordinal] = _source_output_payload(checkpoint_record, cells)
            continue
        if ready is None and cells and cells[0] == _OWNER_READY:
            _checkpoint_payload(checkpoint_record)
            custody, count = _owner_ready_payload(cells)
            ready = checkpoint_record
            continue
        if ready is None or checkpoint_record.kind not in {"source_allocation", "family_allocation", "artifact"}:
            raise ValueError("SOURCE recovery lacks its completed owner checkpoint")
        if checkpoint_record.kind == "artifact" and (not cells or cells[0] != "custom-import/metadata-query-keys/v1"):
            raise ValueError("SOURCE recovery contains an unknown or misplaced artifact")
    if ready is None:
        raise ValueError("SOURCE recovery lacks its completed owner checkpoint")
    custody.current(root, profile)
    _ready_outputs(ready, custody, count, outputs, common)
    return ready, custody, tuple(outputs[shard][1] for shard in sorted(outputs)), None if common is None else common[1]


def _read_descriptor_frames(root, descriptor_stream, limits, check, contract=_CONTRACT):
    files, directories, tasks, total = {}, {}, [], 0
    descriptor_rows = _read_frames(descriptor_stream, limits, check)
    header = _tuple(next(descriptor_rows, None), 10)
    if header[:4] != ("header", contract, str(root), _directory(root)):
        raise ValueError("SOURCE-ready descriptor root differs")
    partition_cells = _tuple(header[4], 4 if contract == _LEGACY_CONTRACT else 5)
    _hex_digest(partition_cells[0])
    if type(partition_cells[1]) is not str or not partition_cells[1]:
        raise ValueError("SOURCE-ready partition owner differs")
    for descriptor_value in partition_cells[2:4]:
        _uint(descriptor_value, positive=True)
    if len(partition_cells) == 5 and partition_cells[4] is not None:
        _hex_digest(partition_cells[4])
    plan = _read_plan(root, header[5])
    allocated = _read_allocation(root, plan, header[6])
    footer = None
    for task_cells in descriptor_rows:
        _tuple(task_cells)
        if footer is not None:
            raise ValueError("SOURCE-ready descriptor has trailing frames")
        match task_cells:
            case ("directory", role, directory):
                if role not in _ROLES or role in directories or _typed_stamp(directory, 4) != _directory(root / role):
                    raise ValueError("SOURCE-ready directory custody differs")
                directories[role] = directory
            case ("file", role, name, expected, expected_sha):
                if role not in directories or (role, _name(name)) in files:
                    raise ValueError("SOURCE-ready file census differs")
                if _hash_file(root / role / name, check=check) != (_typed_stamp(expected, 5), _digest(expected_sha)):
                    raise ValueError("SOURCE-ready file custody or bytes differ")
                files[role, name] = expected[-1]
                total += expected[-1]
                if total > limits.max_spool_bytes:
                    raise ValueError("SOURCE-ready file budget differs")
            case ("task", _, _):
                if len(tasks) >= 4096:
                    raise ValueError("SOURCE-ready task cohort exceeds its budget")
                tasks.append(_read_task(root, task_cells[1:]))
            case ("end", _, _, _):
                for descriptor_value in task_cells[1:]:
                    _uint(descriptor_value)
                footer = task_cells
            case _:
                raise ValueError("SOURCE-ready descriptor record differs")
    if footer != ("end", len(files), len(tasks), total) or tuple(directories) != _ROLES:
        raise ValueError("SOURCE-ready descriptor EOF census differs")
    return header, partition_cells, plan, allocated, files, directories, tasks


def _metadata_seal(root, request, allocated, cells):
    metadata_sha, metadata_counts = _tuple(cells, 2)
    for (name, count), expected_name in zip(_tuple(metadata_counts, 2), ("family", "query"), strict=True):
        if name != expected_name:
            raise ValueError("SOURCE-ready metadata count name differs")
        _uint(count)
    return OwnerMetadataSeal(
        str(root / "metadata"),
        _digest(metadata_sha),
        owner_metadata_scope(request, allocated),
        metadata_counts,
    )


def _restore_ready_artifacts(root, capture, request, checkpoint_record, limits, check, parsed, digest):
    header, partition_cells, plan, allocated, files, directories, tasks = parsed
    partition = PartitionedCapture(
        root / "input",
        *partition_cells[:4],
        capture.identity,
        partition_cells[4] if len(partition_cells) == 5 else None,
    )
    counts_by_role = dict(_tuple(header[8], len(_COUNTS)))
    if tuple(counts_by_role) != _COUNTS:
        raise ValueError("SOURCE-ready task count names differ")
    borrowed = _BorrowedWork(root / "tasks", directories["tasks"][:2], limits)
    borrowed._io_check = check
    source_tasks = PreparedSourceTasks(
        borrowed,
        tuple(tasks),
        allocated,
        SourceTaskScope(request.definition, request.complete_scope),
        counts_by_role,
    )
    if (
        _prepared_sha256(source_tasks) != _digest(header[9])
        or dict(checkpoint_record.commitments).get("prepared_sha256") != header[9]
    ):
        raise ValueError("SOURCE-ready prepared task commitment differs")
    metadata = _metadata_seal(root, request, allocated, header[7])
    return SourceReadyArtifacts(
        str(root),
        PreparedCaptureSource(capture, partition, plan),
        allocated,
        tuple(task.shard for task in tasks),
        metadata,
        source_tasks,
        digest,
        header[1],
    )


def _read_descriptor(root, capture, request, checkpoint_record, limits, check):
    if (root / _DESCRIPTOR).lstat().st_size > limits.max_spool_bytes:
        raise ValueError("SOURCE-ready descriptor exceeds its spool budget")
    stamp, digest = _hash_file(root / _DESCRIPTOR, check=check)
    descriptor_payload = _checkpoint_payload(checkpoint_record)
    if (
        descriptor_payload != (descriptor_payload[0], str(root), _directory(root), stamp)
        or dict(checkpoint_record.commitments).get("descriptor_sha256") != digest
        or checkpoint_record.input_sha256 != digest
    ):
        raise ValueError("SOURCE-ready protected descriptor commitment differs")
    with os.fdopen(os.open(root / _DESCRIPTOR, os.O_RDONLY | os.O_NOFOLLOW), "rb") as descriptor_stream:
        parsed = _read_descriptor_frames(root, descriptor_stream, limits, check, descriptor_payload[0])
    files = parsed[4]
    if _hash_file(root / _DESCRIPTOR, check=check) != (stamp, digest):
        raise ValueError("SOURCE-ready descriptor changed during reading")
    if {entry.name for entry in root.iterdir()} != {*_ROLES, _DESCRIPTOR}:
        raise ValueError("SOURCE-ready root file census differs")
    for role in _ROLES:
        if {entry.name for entry in (root / role).iterdir()} != {name for kind, name in files if kind == role}:
            raise ValueError("SOURCE-ready role file census differs")
    return _restore_ready_artifacts(root, capture, request, checkpoint_record, limits, check, parsed, digest)


def _ready_record(checkpoint, check):
    ready_record = None
    for _head, record in checkpoint.records(check=check):
        if record.kind == "artifact" and tuple(name for name, _value in record.commitments) == (
            "descriptor_sha256",
            "prepared_sha256",
        ):
            cells = msgpack.unpackb(record.payload, raw=False, use_list=False, max_array_len=16, max_map_len=0)
            if type(cells) is tuple and cells and cells[0] in {_CONTRACT, _LEGACY_CONTRACT, _OWNER_READY}:
                _checkpoint_payload(record)
                if ready_record is not None:
                    raise ValueError("SOURCE-ready checkpoint contains duplicate descriptors")
                ready_record = record
    if ready_record is None:
        raise ValueError("SOURCE-ready has no protected completed descriptor")
    return ready_record


def _reopen_files(root, capture, context, record, limits, worker_limits, metadata_work):
    request, registry, family_id = context.request, context.registry, context.family_id
    ready = _read_descriptor(root, capture, request, record, limits, metadata_work.check)
    _validate(
        ready, request, registry, family_id, limits=limits, worker_limits=worker_limits, metadata_work=metadata_work
    )
    return ready


async def reopen_source_ready(
    factory, context, *, root, checkpoint_root, limits, worker_limits, metadata_work, checkpoint_limits=None
):
    """Fresh native authority, then bounded file EOF work off the owner event loop.

    No recapture, allocation, completion flag, base pin or verification proof is
    reconstructed. Continuation and worker-lifetime authority stay caller-owned.
    Cancellation stops and drains the reader before returning cleanup control.
    """
    request, build_id, family_id, layout = context.request, context.build_id, context.family_id, context.layout
    checkpoint = await read_owner_checkpoint(
        factory,
        request,
        build_id,
        family_id,
        layout,
        root=checkpoint_root,
        limits=limits if checkpoint_limits is None else checkpoint_limits,
    )
    control = SimpleNamespace(stop=threading.Event())

    def current():
        """Keep cancellation and the original reader check active during reopening."""
        if control.stop.is_set():
            raise CancellationRequested("SOURCE-ready reopening was cancelled")
        _check(metadata_work.check)

    work = replace(metadata_work, check=current)
    ready_record = await _cpu(control, _ready_record, checkpoint, current)
    capture = await _load_retained_source(factory, request, build_id, family_id, layout)
    ready = await _cpu(
        control,
        _reopen_files,
        Path(root).absolute(),
        capture,
        context,
        ready_record,
        limits,
        worker_limits,
        work,
    )
    ready.tasks._work._io_check = metadata_work.check
    return ready
