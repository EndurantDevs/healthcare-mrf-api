# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed retained-only custody under the original protected owner checkpoint.

These records authorize no SQL, lineage or verification result on their own.
Native authority, the drained lifetime lock and strict original file readers are
required before adoption. No values are copied and later GRAPH is unsupported.
"""

import hashlib
import re
from collections import deque
from contextlib import closing
from dataclasses import astuple, fields
from pathlib import Path
from types import SimpleNamespace

import msgpack

from process.custom_import import family_shard_checkpoint as checkpoints
from process.custom_import import family_shard_source_journal as journal_api
from process.custom_import.build_source import _page_session
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.family_shard_plan import _cpu

_CONTRACT = "custom-import/owner-retained-custody/v1"
_STAGES = (
    "plan",
    "input",
    "destination",
    "original",
    "encoded",
    "stored",
    "common-copy",
    "encoded-copy",
    "homes-start",
    "homes",
    "tail",
)
_REQUIRED = (1, 2, 3, 4, 5, 6, 7, 7, 8, 9, 10)
_ROOT = re.compile(r"retained(?:-[0-9a-f]{32})?\Z")
_VECTOR = "custom-import/retained-native-vectors/v1"
_COMPLETE = "custom-import/retained-native-complete/v1"


def _decode(record, limits):
    return msgpack.unpackb(
        record.payload,
        raw=False,
        use_list=False,
        strict_map_key=True,
        max_array_len=4096,
        max_map_len=64,
        max_str_len=limits.max_record_bytes,
        max_bin_len=limits.max_record_bytes,
    )


def _is_bundle(record, limits):
    if record.kind != "artifact" or tuple(name for name, _digest in record.commitments) != (
        "retained_bundle",
        "source_ready",
    ):
        return False
    cells = _decode(record, limits)
    return type(cells) is tuple and bool(cells) and cells[0] == _CONTRACT


def _counts(cells):
    if (
        type(cells) is not tuple
        or len(cells) != 4
        or any(type(value) is not int or value < 0 for value in cells)
        or any(part > whole for part, whole in zip(cells[2:], cells[:2], strict=True))
    ):
        raise FamilyBlockUnavailable("retained custody home counts differ")


def _bundle(checkpoint_record, limits):
    cells = _decode(checkpoint_record, limits)
    if (
        type(cells) is not tuple
        or len(cells) != 7
        or cells[0] != _CONTRACT
        or type(cells[1]) is not bytes
        or len(cells[1]) != 32
        or type(cells[2]) is not str
        or not Path(cells[2]).is_absolute()
        or type(cells[3]) is not tuple
        or len(cells[3]) != 2
        or type(cells[4]) is not tuple
        or len(cells[4]) != 4
        or type(cells[5]) is not tuple
        or len(cells[5]) != 10
        or type(cells[6]) is not int
        or not 0 <= cells[6] < len(_STAGES)
    ):
        raise FamilyBlockUnavailable("retained custody bundle shape differs")
    stage = cells[6]
    if checkpoint_record.ordinal != stage or checkpoint_record.kind != "artifact":
        raise FamilyBlockUnavailable("retained custody stage differs")
    expected = ("retained_bundle", hashlib.sha256(checkpoint_record.payload).digest()), ("source_ready", cells[1])
    if checkpoint_record.commitments != expected or checkpoint_record.input_sha256 != expected[0][1]:
        raise FamilyBlockUnavailable("retained custody bundle commitment differs")
    root, identity = cells[3]
    if (
        type(root) is not str
        or Path(root).parent != Path(cells[2])
        or _ROOT.fullmatch(Path(root).name) is None
        or type(identity) is not tuple
        or len(identity) != 4
        or any(type(identity_cell) is not int or identity_cell < 0 for identity_cell in identity)
        or any(identity_cell is None for identity_cell in cells[5][: _REQUIRED[stage]])
        or any(identity_cell is not None for identity_cell in cells[5][_REQUIRED[stage] :])
    ):
        raise FamilyBlockUnavailable("retained custody root or completed fields differ")
    _counts(cells[4])
    for counts in cells[5][7:9]:
        if counts is not None:
            _counts(counts)
    if cells[5][8] is not None and cells[5][7][:2] != cells[5][8][:2]:
        raise FamilyBlockUnavailable("retained custody home completion differs from its baseline")
    return cells


def _is_complete_vector(record, limits):
    cells = _decode(record, limits)
    if type(cells) is not tuple or not cells or cells[0] not in {_VECTOR, _COMPLETE}:
        raise FamilyBlockUnavailable("retained custody has an unknown native suffix")
    role = "retained_vectors" if cells[0] == _VECTOR else "retained_destination"
    if len(cells) != (8 if cells[0] == _VECTOR else 6) or tuple(name for name, _sha in record.commitments) != (
        role,
        "source_allocations",
        "source_plan",
    ):
        raise FamilyBlockUnavailable("retained custody native vector envelope differs")
    return cells[0] == _COMPLETE


class SourcePrefix:
    """Expose the exact SOURCE/admission prefix while keeping the whole native head."""

    def __init__(self, snapshot, *, keep_vectors=False, limits=None):
        self.snapshot, self.keep_vectors = snapshot, keep_vectors
        self.limits = getattr(snapshot, "limits", limits)
        if self.limits is None:
            raise FamilyBlockUnavailable("retained custody classifier lacks original control bounds")

    @property
    def head(self):
        """Expose the unchanged authenticated head, never an inferred prefix head."""
        return self.snapshot.head

    def records(self, *, check=None):
        """Validate the complete suffix before exposing only the earlier native records."""
        has_prefix = False
        expected_stage = expected_vector = None
        newer = None
        for head, checkpoint_record in self.snapshot.records(check=check):
            is_bundle = _is_bundle(checkpoint_record, self.limits)
            if not is_bundle and checkpoint_record.kind != "retained_allocation":
                has_prefix = True
                yield head, checkpoint_record
                continue
            if has_prefix:
                raise FamilyBlockUnavailable("retained custody interrupts its original SOURCE prefix")
            if is_bundle:
                if expected_stage == 1 and expected_vector is None:
                    raise FamilyBlockUnavailable("retained destination lacks its original completed vector EOF")
                cells = _bundle(checkpoint_record, self.limits)
                if expected_vector is not None and expected_stage is None and cells[6] != 1:
                    raise FamilyBlockUnavailable("retained vectors lack their original input custody")
                if expected_stage is not None and cells[6] != expected_stage:
                    raise FamilyBlockUnavailable("retained custody stages are duplicate or incomplete")
                if newer is not None and (
                    cells[1:3] != newer[1:3]
                    or cells[4] != newer[4]
                    or cells[5][: _REQUIRED[cells[6]]] != newer[5][: _REQUIRED[cells[6]]]
                ):
                    raise FamilyBlockUnavailable("retained custody rebinds completed original artifacts")
                expected_stage, newer = cells[6] - 1, cells
            else:
                is_completion = _is_complete_vector(checkpoint_record, self.limits)
                if (is_completion and expected_vector is not None) or (
                    not is_completion and expected_stage == 1 and expected_vector is None
                ):
                    raise FamilyBlockUnavailable("retained vector completion is missing or repeated")
                if expected_vector is not None and checkpoint_record.ordinal != expected_vector:
                    raise FamilyBlockUnavailable("retained custody native vector ordinals differ")
                if expected_stage not in (None, 1):
                    raise FamilyBlockUnavailable("retained native vectors lie outside destination allocation")
                expected_vector = checkpoint_record.ordinal - 1
                if self.keep_vectors:
                    yield head, checkpoint_record
        if not has_prefix or expected_stage not in (None, -1) or expected_vector not in (None, -1):
            raise FamilyBlockUnavailable("retained custody lacks its original complete prefix")


def latest(snapshot, check):
    """Validate the whole typed suffix and return only its newest protected bundle."""
    deque(SourcePrefix(snapshot).records(check=check), maxlen=0)
    has_retained_vectors = False
    with closing(snapshot.records(check=check)) as records:
        for _head, record in records:
            if _is_bundle(record, snapshot.limits):
                return _bundle(record, snapshot.limits)
            if record.kind != "retained_allocation":
                if has_retained_vectors:
                    raise FamilyBlockUnavailable("cold retained vectors lack their protected original bundle")
                return None
            has_retained_vectors = True
    return None


def _init_cells(value):
    return tuple(getattr(value, field.name) for field in fields(value) if field.init)


def _encoded(output, check):
    from process.custom_import.family_shard_source_checkpoint import _directory, _hash_file

    root = output._work.root
    if output._closed or output.path != root / "output.mpk" or {path.name for path in root.iterdir()} != {"output.mpk"}:
        raise FamilyBlockUnavailable("retained encoded custody is not a closed original file")
    observed = _directory(root), journal_api._stamp(output.path.lstat()), _pack_row(output.receipt)
    cached = getattr(output, "_retained_custody", None)
    if cached is not None:
        if cached[0] != observed:
            raise FamilyBlockUnavailable("retained encoded custody changed after checkpointing")
        return cached[1]
    identity, digest = _hash_file(output.path, check=check)
    if digest != output.sha256:
        raise FamilyBlockUnavailable("retained encoded custody bytes differ")
    cells = str(root), observed[0], identity, digest, observed[2]
    output._retained_custody = observed, cells
    return cells


def _state(owner, destination=None):
    values = [astuple(owner.final_plan)]
    for name in ("retained_input", "destination", "original"):
        value = destination if name == "destination" and destination is not None else getattr(owner, name)
        values.append(None if value is None else _init_cells(value))
    values.append(None if owner.encoded is None else _encoded(owner.encoded, owner.current))
    values.append(None if owner.stored is None else _init_cells(owner.stored))
    values.append(None if owner.common_receipt is None else _pack_row(owner.common_receipt))
    values.append(getattr(owner, "_home_intent", None))
    values.append(None if owner.homes is None else _init_cells(owner.homes))
    values.append(owner.pack_tail)
    return tuple(values)


def _record(owner, stage, destination=None):
    from process.custom_import.family_shard_source_checkpoint import _directory

    index = _STAGES.index(stage)
    values = _state(owner, destination)
    values = values[: _REQUIRED[index]] + (None,) * (10 - _REQUIRED[index])
    source = owner.source
    payload = _pack_row(
        (
            _CONTRACT,
            source.ready_record.input_sha256,
            str(source.owner_lock.root),
            (str(owner.root), _directory(owner.root)),
            _init_cells(source.homes),
            values,
            index,
        )
    )
    digest = hashlib.sha256(payload).digest()
    return checkpoints.OwnerCheckpointRecord(
        "artifact",
        index,
        digest,
        (("retained_bundle", digest), ("source_ready", source.ready_record.input_sha256)),
        payload,
    )


def destination_record(owner, descriptor):
    """Prepare custody for the same transaction as the original completion record."""
    if getattr(owner, "_checkpoint_stage", -1) != _STAGES.index("input"):
        raise FamilyBlockUnavailable("retained completion lacks its original input custody")
    return _record(owner, "destination", descriptor)


def check_roots(snapshot, check):
    """Recheck every retained attempt directory only after acquiring the original lock."""
    from process.custom_import.family_shard_source_checkpoint import _directory

    for _head, record in snapshot.records(check=check):
        if _is_bundle(record, snapshot.limits):
            cells = _bundle(record, snapshot.limits)
            if _directory(Path(cells[3][0])) != cells[3][1]:
                raise FamilyBlockUnavailable("retained custody attempt directory was replaced")


def _before_graph(build):
    if (
        build is None
        or build.phase != "graph"
        or build.plan_stage != "base"
        or any(getattr(build, name) is not None for name in ("generation_id", "plan_complete_at", "graph_frozen_at"))
    ):
        raise FamilyBlockUnavailable("retained custody cannot adopt later GRAPH progress")


async def commit_retained_custody(factory, owner, stage):
    """Append one bounded bundle; unknown replies only observe the exact successor."""
    index = _STAGES.index(stage)
    if getattr(owner.source, "owner_profile", None) is None:
        return
    if getattr(owner, "_checkpoint_stage", -1) >= index:
        return
    owner.current()
    owner.source.owner_lock.check()
    source_owner, journal = owner.source, owner.source_journal
    snapshot = await journal_api._custody_snapshot(factory, source_owner.context, journal)
    prior = latest(snapshot, owner.current)
    custody_record = await _cpu(owner, _record, owner, stage)
    if prior is not None and prior[6] == index and _bundle(custody_record, journal.control_limits) == prior:
        owner._checkpoint_stage = index
        return
    if (prior is None and index != 0) or (prior is not None and prior[6] != index - 1):
        raise FamilyBlockUnavailable("retained custody next stage differs")
    previous, prepared, failure = snapshot.head, None, None
    journal.reconcile_budget()
    try:
        async with _page_session(factory, source_owner.context.request, source_owner.context.build_id) as (
            session,
            build,
        ):
            _before_graph(build)
            reservation = journal_api.reserve_checkpoint(journal)
            prepared = await checkpoints.append_owner_checkpoint(
                session,
                source_owner.context.request,
                source_owner.context.build_id,
                source_owner.context.family_id,
                source_owner.context.layout,
                storage=snapshot,
                record=custody_record,
                expected_head=previous,
            )
            journal_api.finish_checkpoint_charge(journal, reservation, prepared)
    except Exception as error:
        failure = error
    try:
        confirmed = await journal_api._custody_snapshot(factory, source_owner.context, journal)
    except BaseException as error:
        raise FamilyBlockUnavailable("retained custody commit is unknown; retain originals") from error
    if confirmed.head == previous and failure is not None:
        raise failure
    with closing(confirmed.records(check=owner.current)) as checkpoint_records:
        newest, preceding = next(checkpoint_records, None), next(checkpoint_records, None)
    if (
        newest is None
        or newest[1] != custody_record
        or newest[0].revision != (1 if previous is None else previous.revision + 1)
        or (None if preceding is None else preceding[0]) != previous
        or (failure is None and (prepared is None or confirmed.head != prepared.head))
    ):
        raise FamilyBlockUnavailable("retained custody lacks its exact committed successor")
    owner._checkpoint_stage = index


def _closed_descriptor(descriptor, filename, reader, owner_root, check, *, journal=False):
    from collections import deque

    from process.custom_import.family_shard_source_checkpoint import _directory, _hash_file

    root = Path(descriptor.root)
    relative = root.relative_to(owner_root).parts
    if journal:
        is_allowed = len(relative) == 4 and relative[:3] == ("source", "allocation-journal", "outputs")
        is_allowed = is_allowed and re.fullmatch(r"retained-[0-9a-f]{32}", relative[3]) is not None
    else:
        is_allowed = bool(relative) and (
            (len(relative) == 1 and re.fullmatch(r"final-plan(?:-[0-9a-f]{32})?", relative[0]) is not None)
            or (_ROOT.fullmatch(relative[0]) is not None and relative[1:] == ("input",))
        )
    if not is_allowed or {path.name for path in root.iterdir()} != {filename}:
        raise FamilyBlockUnavailable("retained custody file lies outside its original slot")
    identity = _directory(root)
    stamp, digest = _hash_file(root / filename, check=check)
    if identity[:2] != (descriptor.device, descriptor.inode) or (stamp[0], stamp[1], stamp[4], digest) != (
        descriptor.file_device,
        descriptor.file_inode,
        descriptor.file_bytes,
        descriptor.sha256,
    ):
        raise FamilyBlockUnavailable("retained custody descriptor differs from its original file")
    with closing(reader(descriptor, check=check)) as artifact_records:
        deque(artifact_records, maxlen=0)
    if _directory(root) != identity or _hash_file(root / filename, check=check) != (stamp, digest):
        raise FamilyBlockUnavailable("retained custody file changed during complete replay")
    return descriptor


def _reopen_encoded(cells, owner):
    from process.custom_import.family_shard_facts import SourceFactLimits
    from process.custom_import.family_shard_output import FamilyShardOutput
    from process.custom_import.family_shard_source_checkpoint import _directory, _hash_file
    from process.custom_import.family_shard_tasks import _BorrowedWork

    root_name, directory, stamp, digest, receipt = cells
    root = Path(root_name)
    relative = root.relative_to(owner.source.owner_lock.root).parts
    if len(relative) != 2 or _ROOT.fullmatch(relative[0]) is None or relative[1] != "encoded":
        raise FamilyBlockUnavailable("retained encoded file lies outside its original slot")
    if _directory(root) != directory or {path.name for path in root.iterdir()} != {"output.mpk"}:
        raise FamilyBlockUnavailable("retained encoded root custody differs")
    if _hash_file(root / "output.mpk", check=owner.current) != (stamp, digest):
        raise FamilyBlockUnavailable("retained encoded file custody differs")
    limits = owner.source.limits.worker.output
    record_bytes = min(limits.max_spool_bytes, 4 * limits.max_record_bytes + limits.fragment_bytes + 1024)
    work = _BorrowedWork(
        root, directory[:2], SourceFactLimits(limits.max_spool_bytes, record_bytes, max(1, limits.memory_bytes // 8))
    )
    work._io_check = owner.current
    output = FamilyShardOutput(work, root / "output.mpk", _unpack_row(receipt))
    if output.sha256 != digest:
        raise FamilyBlockUnavailable("retained encoded replay digest differs")
    return output


def _hydrate(owner, cells):
    from process.custom_import.family_shard_base_values import RetainedInputDescriptor, read_retained_input
    from process.custom_import.family_shard_retained_homes import RetainedHomeCounts
    from process.custom_import.family_shard_retained_output import (
        RetainedDestinationDescriptor,
        read_retained_destination,
    )
    from process.custom_import.family_shard_retained_storage_validation import RetainedStoredComparison
    from process.custom_import.family_shard_retained_validation import RetainedOriginalComparison

    source_owner, owner_root = owner.source, owner.source.owner_lock.root
    if cells[1] != source_owner.ready_record.input_sha256 or Path(cells[2]) != owner_root:
        raise FamilyBlockUnavailable("retained custody belongs to another SOURCE owner")
    artifact_cells = cells[5]
    if astuple(owner.final_plan) != artifact_cells[0]:
        raise FamilyBlockUnavailable("retained custody original plan changed")
    specifications = (
        ("retained_input", RetainedInputDescriptor, "retained-input.mpk", read_retained_input, False),
        ("destination", RetainedDestinationDescriptor, "retained-destination.mpk", read_retained_destination, True),
    )
    for index, (name, constructor, filename, reader, journal) in enumerate(specifications, 1):
        if artifact_cells[index] is not None:
            setattr(
                owner,
                name,
                _closed_descriptor(
                    constructor(*artifact_cells[index]), filename, reader, owner_root, owner.current, journal=journal
                ),
            )
    if artifact_cells[3] is not None:
        owner.original = RetainedOriginalComparison(*artifact_cells[3])
    if artifact_cells[4] is not None:
        owner.encoded = _reopen_encoded(artifact_cells[4], owner)
    if artifact_cells[5] is not None:
        owner.stored = RetainedStoredComparison(*artifact_cells[5])
    owner.common_receipt = None if artifact_cells[6] is None else _unpack_row(artifact_cells[6])
    owner._home_intent = artifact_cells[7]
    owner.homes = None if artifact_cells[8] is None else RetainedHomeCounts(*artifact_cells[8])
    owner.pack_tail, owner._checkpoint_stage = artifact_cells[9], cells[6]


def _check_native_vectors(snapshot, owner):
    from process.custom_import import family_shard_metadata_journal as metadata
    from process.custom_import.family_shard_retained_allocate import _scope
    from process.custom_import.family_shard_retained_journal import _scope_digest
    from process.custom_import.family_shard_retained_stage import _allocation_work

    original_work = _allocation_work(owner, "destination")
    scope = _scope_digest(owner.context, original_work, _scope(owner.context, original_work))
    for _head, checkpoint_record in SourcePrefix(snapshot, keep_vectors=True).records(check=owner.current):
        if checkpoint_record.kind != "retained_allocation":
            continue
        cells = _decode(checkpoint_record, snapshot.limits)
        if cells[0] == _VECTOR:
            with metadata._blob(
                owner.source_journal, owner, checkpoint_record, _VECTOR, "retained_vectors", owner.source.allocated
            ):
                owner.current()
        elif (
            owner.destination is None
            or cells
            != (
                _COMPLETE,
                scope,
                owner.destination.sha256,
                owner.destination.counts,
                owner.destination.source_pack_prefix,
                owner.destination.destination_pack_tail,
            )
            or checkpoint_record.input_sha256 != hashlib.sha256(checkpoint_record.payload).digest()
            or (
                checkpoint_record.commitments
                != metadata._commitments("retained_destination", owner.destination.sha256, owner.source.allocated)
            )
        ):
            raise FamilyBlockUnavailable("retained complete vector no longer binds its exact original destination")


async def _recheck_native_vectors(factory, owner, cells):
    snapshot = await journal_api._custody_snapshot(factory, owner.source.context, owner.source_journal)
    if latest(snapshot, owner.current) != cells:
        raise FamilyBlockUnavailable("retained custody changed during original vector reopening")
    await _cpu(owner, _check_native_vectors, snapshot, owner)


async def _restore_source_phase(factory, source, cells):
    from process.custom_import import family_shard_events_journal as events
    from process.custom_import.family_shard_source_stage import _native_phase

    source.owner_lock.check()
    snapshot = await journal_api._custody_snapshot(factory, source.context, source.allocation_journal)
    if latest(snapshot, source.current) != cells:
        raise FamilyBlockUnavailable("retained SOURCE custody changed before phase restoration")
    events.require_completed_admission(snapshot, source.limits.allocation.maximum_batch_records, source.current)
    phase = await _native_phase(factory, source)
    if phase != "graph":
        raise FamilyBlockUnavailable("retained reopening requires the fresh native GRAPH phase")
    source.phase = phase


def _retained_file_cost(owner, root, filename, expected, unit):
    from process.custom_import.family_shard_source_checkpoint import _directory, _hash_file, _stamp

    directory = _directory(root)
    if {path.name for path in root.iterdir()} != {filename}:
        raise FamilyBlockUnavailable("retained completed child has unexpected files")
    path = root / filename
    observed = path.lstat()
    stamp = _stamp(observed)
    if (*directory[:2], *stamp[:2], stamp[4]) != expected[:5]:
        raise FamilyBlockUnavailable("retained closed child differs from its original descriptor")
    custody = directory, journal_api._stamp(observed), observed.st_blocks, expected
    previous = owner._closed_costs.get(root)
    if previous is not None and previous != custody:
        raise FamilyBlockUnavailable("retained closed child changed after its allowance was charged")
    if previous is None:
        if _hash_file(path, check=owner.current) != (stamp, expected[5]):
            raise FamilyBlockUnavailable("retained closed child bytes differ")
        owner._closed_costs[root] = custody
    return journal_api._entry_cost(root.lstat(), unit) + journal_api._entry_cost(observed, unit)


def _retained_children_cost(owner, unit):
    expected_names, charged = set(), 0
    descriptor = owner.retained_input
    if descriptor is not None and Path(descriptor.root).parent == owner.root:
        expected_names.add("input")
        charged += _retained_file_cost(
            owner,
            owner.root / "input",
            "retained-input.mpk",
            (
                descriptor.device,
                descriptor.inode,
                descriptor.file_device,
                descriptor.file_inode,
                descriptor.file_bytes,
                descriptor.sha256,
            ),
            unit,
        )
    output = owner.encoded
    if output is not None and output._work.root.parent == owner.root:
        expected_names.add("encoded")
        directory, file = output._work.identity, output._identity
        charged += _retained_file_cost(
            owner,
            owner.root / "encoded",
            "output.mpk",
            (
                directory.st_dev,
                directory.st_ino,
                file.st_dev,
                file.st_ino,
                file.st_size,
                output.sha256,
            ),
            unit,
        )
    if {path.name for path in owner.root.iterdir()} != expected_names:
        raise FamilyBlockUnavailable("retained child cleanup is incomplete; its allowance remains charged")
    return charged


def retained_limits(owner):
    """Bound the next sequential child after charging every closed persistent child.

    Each child must drain before the next is constructed. Encoding already reserves
    both nested replay/output allowances in its one live outer Scratch. Only closed
    input and encoded files persist; an unknown or failed-cleanup subtree refuses.
    """
    import os
    from dataclasses import replace

    from process.custom_import.family_shard_facts import SourceFactBudgetExceeded
    from process.custom_import.family_shard_source_checkpoint import _directory

    owner.current()
    descriptor = os.open(owner.root.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        unit = journal_api._filesystem_unit(descriptor)
    finally:
        os.close(descriptor)
    charged = 2 * unit
    if owner.root.exists() or owner.root.is_symlink():
        directory = _directory(owner.root)
        if owner._root_identity is not None and directory[:2] != owner._root_identity:
            raise FamilyBlockUnavailable("retained current attempt directory changed")
        charged = journal_api._entry_cost(owner.root.lstat(), unit) + _retained_children_cost(owner, unit)
    maximum = owner.work.limits.max_spool_bytes - charged
    if maximum < max(owner.work.limits.max_record_bytes, owner.work.limits.memory_bytes):
        raise SourceFactBudgetExceeded("retained closed children exhaust the original shared allowance")
    return replace(owner.work.limits, max_spool_bytes=maximum)


async def _restore_retained_artifacts(factory, job, owner, cells):
    from process.custom_import.family_shard_base_values import cleanup_retained_input
    from process.custom_import.family_shard_common import _offload
    from process.custom_import.family_shard_retained_allocate import RetainedAllocationContext, _scope
    from process.custom_import.family_shard_retained_output import cleanup_retained_destination
    from process.custom_import.family_shard_retained_stage import _allocation_work
    from process.custom_import.family_shard_retained_storage_validation import _scope as stored_scope

    if owner.retained_input is not None:
        owner.context = RetainedAllocationContext(
            job.source.common_context,
            job.build_id,
            job.source.admission,
            owner.final_plan,
            owner.retained_input,
            job.base,
            job.source.common_output,
            job.source.events,
        )
        _scope(owner.context, _allocation_work(owner, "original", writing=False))
        owner._stack.push_async_callback(_offload, cleanup_retained_input, owner.retained_input)
    if owner.destination is not None:
        owner._stack.push_async_callback(_offload, cleanup_retained_destination, owner.destination)
    if owner.original is not None:
        if owner.original.scope != _scope(owner.context, _allocation_work(owner, "original", writing=False)) or (
            owner.original.destination_sha256 != owner.destination.sha256
            or owner.original.counts != owner.retained_input.counts
        ):
            raise FamilyBlockUnavailable("retained original proof inputs differ after reopening")
    if owner.encoded is not None:
        owner._stack.push_async_callback(_offload, owner.encoded.close)
    if owner.stored is not None:
        stored_scope(
            owner.context, owner.destination, owner.original, owner.encoded, job.source.metadata.query_keys, owner.work
        )
    if owner.context is not None:
        await _recheck_native_vectors(factory, owner, cells)


async def reopen(factory, job, cells):
    """Transfer original retained readers only after native SOURCE and base repinning."""
    from process.custom_import.family_shard_homes import SourceHomeCounts
    from process.custom_import.family_shard_plan import FinalPlanDescriptor, read_family_plan
    from process.custom_import.family_shard_retained_stage import RetainedFamilyStage

    job.source.owner_lock.check()
    await _restore_source_phase(factory, job.source, cells)
    job.source.homes = SourceHomeCounts(*cells[4])
    final_plan = await _cpu(
        job,
        _closed_descriptor,
        FinalPlanDescriptor(*cells[5][0]),
        "family-plan.mpk",
        read_family_plan,
        job.root,
        job.current,
    )
    from process.custom_import.family_shard_owner_job import _work

    if job.base is not None:
        if job.base.scope != final_plan.base_scope:
            raise FamilyBlockUnavailable("retained reopened base differs from its original plan")
        if not job.base.complete:
            async for _header in job.base.headers():
                job.current()
        if not job.base.complete:
            raise FamilyBlockUnavailable("retained reopened base header EOF is incomplete")
    owner = RetainedFamilyStage(job.source, final_plan, job.base, work=_work(job, "retained"))
    job.final_plan, job.retained = final_plan, owner
    owner._ensure_root()
    await _cpu(owner, _hydrate, owner, cells)
    await _restore_retained_artifacts(factory, job, owner, cells)
    await observe(factory, owner)
    return owner


async def reopen_completed(factory, job, cells, snapshot):
    """Restore only a protected completed tail, without allocator/home dispatch."""
    from process.custom_import.family_shard_base_values import cleanup_retained_input
    from process.custom_import.family_shard_common import _offload
    from process.custom_import.family_shard_plan import FinalPlanDescriptor, PlanWork, read_family_plan
    from process.custom_import.family_shard_retained_allocate import RetainedAllocationContext, _scope
    from process.custom_import.family_shard_retained_output import cleanup_retained_destination
    from process.custom_import.family_shard_retained_stage import RetainedFamilyStage, _allocation_work
    from process.custom_import.family_shard_source_checkpoint import _directory

    job.source.owner_lock.check()
    if latest(snapshot, job.current) != cells or cells[6] != len(_STAGES) - 1:
        raise FamilyBlockUnavailable("later OUTPUT requires its exact completed retained tail")
    final_plan = await _cpu(
        job,
        _closed_descriptor,
        FinalPlanDescriptor(*cells[5][0]),
        "family-plan.mpk",
        read_family_plan,
        job.root,
        job.current,
    )
    if final_plan.base_scope != (None if job.base is None else job.base.scope):
        raise FamilyBlockUnavailable("later OUTPUT original base scope differs")
    if job.base is not None and not job.base.complete:
        async for _header in job.base.headers():
            job.current()
        if not job.base.complete:
            raise FamilyBlockUnavailable("later OUTPUT original base headers lack genuine EOF")
    root = Path(cells[3][0])
    if _directory(root) != cells[3][1]:
        raise FamilyBlockUnavailable("completed retained original root changed")
    owner = RetainedFamilyStage(
        job.source,
        final_plan,
        job.base,
        work=PlanWork(root, job.work.limits, job.work.register_cleanup, job.current),
    )
    job.final_plan, job.retained = final_plan, owner
    owner._root_identity = cells[3][1][:2]
    owner._ensure_root()
    await _cpu(owner, _hydrate, owner, cells)
    owner.context = RetainedAllocationContext(
        job.source.common_context,
        job.build_id,
        job.source.admission,
        final_plan,
        owner.retained_input,
        job.base,
        job.source.common_output,
        job.source.events,
    )
    _scope(owner.context, _allocation_work(owner, "original", writing=False))
    owner._stack.push_async_callback(_offload, cleanup_retained_input, owner.retained_input)
    owner._stack.push_async_callback(_offload, cleanup_retained_destination, owner.destination)
    owner._stack.push_async_callback(_offload, _close_completed_encoded, owner)
    owner.tail_started = owner.complete = True
    return owner


def _close_completed_encoded(owner):
    """Transfer exact original encoded-file cleanup only at explicit terminal close."""
    import shutil

    from process.custom_import.family_shard_source_checkpoint import _directory, _hash_file

    output = owner.encoded
    output.close()
    owner.source.owner_lock.check()
    root = output._work.root
    if _directory(root)[:2] != (output._work.identity.st_dev, output._work.identity.st_ino):
        raise FamilyBlockUnavailable("completed retained terminal root changed")
    stamp, digest = _hash_file(output.path, check=owner.source.owner_lock.check)
    if (stamp[0], stamp[1], stamp[4], digest) != (
        output._identity.st_dev,
        output._identity.st_ino,
        output._identity.st_size,
        output.sha256,
    ) or {path.name for path in root.iterdir()} != {"output.mpk"}:
        raise FamilyBlockUnavailable("completed retained terminal file changed")
    shutil.rmtree(root)


async def _native_receipts(session, owner, binding):
    from sqlalchemy import select

    from process.custom_import.family_block_store import _checked_receipt, _execute
    from process.custom_import.family_shard_retained_stage import _common_receipt

    table = binding.tables_by_role["shard_receipt"]
    shards = len(owner.source.workers) + 2, len(owner.source.workers) + 3
    receipt_rows = (await _execute(session, select(table).where(table.c.shard_id.in_(shards)))).mappings().all()
    receipts_by_shard = {receipt_row["shard_id"]: receipt_row for receipt_row in receipt_rows}
    if len(receipts_by_shard) != len(receipt_rows):
        raise FamilyBlockUnavailable("retained native receipt repeats a shard")
    for index, shard in enumerate(shards):
        is_present = shard in receipts_by_shard
        if is_present:
            if owner.stored is None or owner.destination is None or owner.encoded is None:
                raise FamilyBlockUnavailable("retained native COPY precedes its protected original inputs")
            completed_at = receipts_by_shard[shard]["completed_at"]
            if completed_at is None or completed_at.utcoffset() is None:
                raise FamilyBlockUnavailable("retained native COPY receipt lacks its completion")
            expected = await _cpu(owner, _common_receipt, owner) if index == 0 else owner.encoded.receipt
            checked = _checked_receipt(
                owner.context.common.layout,
                {
                    name: receipts_by_shard[shard][name]
                    for name in ("shard_id", "partition_sha256", "result_sha256", "source_record_count", "role_counts")
                },
            )
            if checked != _checked_receipt(owner.context.common.layout, expected):
                raise FamilyBlockUnavailable("retained native COPY receipt differs from original bytes")
            if index == 0:
                owner.common_receipt = expected
        elif getattr(owner, "_checkpoint_stage", -1) >= _STAGES.index("common-copy" if index == 0 else "encoded-copy"):
            raise FamilyBlockUnavailable("retained completed COPY receipt disappeared")
        setattr(owner, "_common_copied" if index == 0 else "_encoded_copied", is_present)
    if owner._encoded_copied and not owner._common_copied:
        raise FamilyBlockUnavailable("retained encoded COPY lacks its original common prefix")


async def _home_batch(session, owner, batch):
    from sqlalchemy import BigInteger, SmallInteger, and_, column, func, select, values

    from db.models.custom_import_storage import CustomImportRevisionHome
    from process.custom_import.family_block_store import _execute

    expected_ids = tuple(pair for pair in batch if pair[0] in (1, 2))
    if not expected_ids:
        return 0, 0
    requested = values(
        column("kind", SmallInteger), column("revision", BigInteger), name="retained_home_expected"
    ).data(expected_ids)
    homes = CustomImportRevisionHome.__table__
    joined = requested.outerjoin(
        homes, and_(homes.c.revision_kind == requested.c.kind, homes.c.revision_ids.op("@>")(requested.c.revision))
    )
    statement = (
        select(
            requested.c.kind,
            requested.c.revision,
            func.count(homes.c.family_id),
            func.count(homes.c.family_id).filter(homes.c.family_id == owner.context.common.source.snapshot_family_id),
        )
        .select_from(joined)
        .group_by(requested.c.kind, requested.c.revision)
    )
    observed, covered_counts = (await _execute(session, statement)).all(), [0, 0]
    if len(observed) != len(expected_ids) or {(home_row[0], home_row[1]) for home_row in observed} != set(expected_ids):
        raise FamilyBlockUnavailable("retained native homes lack exact expected ID coverage")
    for kind, _identity, count, owned in observed:
        if (count, owned) not in {(0, 0), (1, 1)}:
            raise FamilyBlockUnavailable("retained native home belongs to an unrelated producer")
        covered_counts[kind - 1] += owned
    return tuple(covered_counts)


async def _home_page(factory, owner, batch, *, check_packs):
    from process.custom_import import family_shard_retained_allocate as allocation
    from process.custom_import import family_shard_retained_control as control

    async with _page_session(factory, owner.source.context.request, owner.source.context.build_id) as (session, build):
        control._graph_state(owner.context, build)
        _descriptor, binding = await control._open_candidate(session, owner.context)
        if check_packs:
            await control._pack_census(session, owner.context, binding, owner.destination.destination_pack_tail)
        covered_counts = await _home_batch(session, owner, batch)
        await allocation._check_base(owner.context)
        return covered_counts


async def home_census(factory, owner):
    """Check original assigned IDs against native homes, never aggregate deltas alone."""
    from uuid import uuid4

    from process.custom_import import family_shard_retained_allocate as allocation
    from process.custom_import import family_shard_retained_homes as homes
    from process.custom_import.family_shard_plan import PlanWork, _Scratch
    from process.custom_import.family_shard_retained_stage import _allocation_work

    work = _allocation_work(owner, "homes-observe-" + uuid4().hex)
    scratch = _Scratch(PlanWork(work.root, work.limits.spool, work.register_cleanup, work.check))
    try:
        await allocation._check_base(owner.context)
        path, counts, _prefix = await _cpu(
            scratch, homes._stage, owner.context, owner.destination, scratch, work.limits
        )
        covered_counts = [0, 0]
        has_pack_census = False
        with closing(homes._batches(scratch, path, work.limits)) as batches:
            for batch in batches:
                observed = await _home_page(factory, owner, batch, check_packs=not has_pack_census)
                has_pack_census = True
                covered_counts = [first + second for first, second in zip(covered_counts, observed, strict=True)]
        await allocation._check_base(owner.context)
        return (*counts, *covered_counts)
    finally:
        scratch.close()


async def observe(factory, owner):
    """Select only missing retained work using current receipts and exact pack tails."""
    from process.custom_import import family_shard_retained_control as control
    from process.custom_import.family_block_store import _candidate

    owner.current()
    async with _page_session(factory, owner.source.context.request, owner.source.context.build_id) as (session, build):
        _before_graph(build)
        if owner.context is not None:
            control._graph_state(owner.context, build)
        _descriptor, binding = await _candidate(
            session,
            owner.source.context.request,
            owner.source.context.family_id,
            owner.source.context.layout,
            {"loading"},
        )
        await _native_receipts(session, owner, binding)
        native_tails = None
        if owner.destination is not None:
            stream_rows = await control._streams(
                session, owner.context, SimpleNamespace(tail=owner.destination.destination_pack_tail)
            )
            native_tails = tuple(
                (stream_row["stream_slot"], stream_row["next_pack_ordinal"]) for stream_row in stream_rows
            )
            if native_tails not in (owner.destination.source_pack_prefix, owner.destination.destination_pack_tail):
                raise FamilyBlockUnavailable("retained native pack counters are neither original prefix nor tail")
            if owner._common_copied:
                await control._pack_census(session, owner.context, binding, owner.destination.destination_pack_tail)
    if owner.destination is None:
        return
    has_homes = getattr(owner, "_checkpoint_stage", -1) >= _STAGES.index("homes-start")
    if has_homes:
        census = await home_census(factory, owner)
        if census[:2] != owner._home_intent[:2] or any(
            present < before for present, before in zip(census[2:], owner._home_intent[2:], strict=True)
        ):
            raise FamilyBlockUnavailable("retained native home coverage lost its protected baseline")
        if owner.homes is not None and census[:2] != census[2:]:
            raise FamilyBlockUnavailable("retained completed native homes disappeared")
    tail = owner.destination.destination_pack_tail
    if native_tails == tail and (tail != owner.destination.source_pack_prefix or owner.homes is not None):
        if owner.homes is None or not owner._common_copied or not owner._encoded_copied:
            raise FamilyBlockUnavailable("retained native tail precedes its protected completed homes/COPY")
        owner.pack_tail, owner.tail_started, owner.complete = tail, True, True
    elif getattr(owner, "_checkpoint_stage", -1) >= _STAGES.index("tail"):
        raise FamilyBlockUnavailable("retained protected tail is absent from native counters")


async def begin_homes(factory, owner):
    """Protect exact pre-write coverage before the first missing-only homes batch."""
    if getattr(owner, "_home_intent", None) is None:
        owner._home_intent = await home_census(factory, owner)
    await commit_retained_custody(factory, owner, "homes-start")


async def finish_homes(factory, owner):
    """Derive original inserted counts only from protected baseline and exact IDs."""
    from process.custom_import.family_shard_retained_homes import RetainedHomeCounts

    census = await home_census(factory, owner)
    if census[:2] != census[2:] or census[:2] != owner._home_intent[:2]:
        raise FamilyBlockUnavailable("retained homes did not reach exact original ID coverage")
    owner.homes = RetainedHomeCounts(
        *census[:2], *(total - before for total, before in zip(census[:2], owner._home_intent[2:], strict=True))
    )
    await commit_retained_custody(factory, owner, "homes")
