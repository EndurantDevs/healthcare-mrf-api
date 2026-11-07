# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed original OUTPUT custody; native observations alone never rebuild it."""

import hashlib
import os
import re
import shutil
import stat
import sys
from collections import deque
from contextlib import closing, contextmanager
from dataclasses import astuple, replace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import select

from process.custom_import import family_block_store as store
from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_retained_checkpoint as retained
from process.custom_import import family_shard_source_journal as journal
from process.custom_import.build_source import _page_session, _source_cleanup_failure
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded
from process.custom_import.family_shard_output import FamilyShardOutput, _pack_row, _unpack_row
from process.custom_import.family_shard_plan import PlanWork, _cpu
from process.custom_import.family_shard_source_checkpoint import _directory, _hash_file
from process.custom_import.family_shard_tasks import _BorrowedWork

_CONTRACT = "custom-import/owner-output-custody/v1"
_STAGES = ("inputs", "generation", "source-families", "member-winners", "projection", "close-ready")
_REQUIRED = (0, 1, 2, 5, 6, 6)
_REPLAY_SLOT = "source-replay"
_CHILD = re.compile(
    r"(?:source-families|selected|members|members-winners|projection|source-replay(?:-[0-9a-f]{32})?|proof-[0-9a-f]{32}|retained-original-[0-9a-f]{32}|retained-stored-[0-9a-f]{32})\Z"
)


def is_enabled(owner):
    """Only the existing durable owner profile participates in cold custody."""
    return getattr(owner.source, "owner_profile", None) is not None


def _bundle(checkpoint_record):
    cells = _unpack_row(checkpoint_record.payload)
    if type(cells) is not tuple or len(cells) != 9 or cells[0] != _CONTRACT:
        raise FamilyBlockUnavailable("OUTPUT custody envelope differs")
    stage = cells[8]
    if type(stage) is not int or not 0 <= stage < len(_STAGES):
        raise FamilyBlockUnavailable("OUTPUT custody stage differs")
    digest = hashlib.sha256(checkpoint_record.payload).digest()
    if (
        checkpoint_record.kind != "artifact"
        or checkpoint_record.ordinal != stage
        or checkpoint_record.input_sha256 != digest
        or checkpoint_record.commitments != (("output_bundle", digest), ("source_ready", cells[1]))
        or type(cells[1]) is not bytes
        or len(cells[1]) != 32
        or type(cells[2]) is not tuple
        or len(cells[2]) != 7
        or cells[2][6] != len(retained._STAGES) - 1
        or type(cells[3]) is not tuple
        or len(cells[3]) != 3
        or type(cells[4]) is not tuple
        or len(cells[4]) != 7
        or type(cells[5]) is not tuple
        or len(cells[5]) != 6
        or type(cells[6]) is not tuple
        or type(cells[7]) is not tuple
        or any(artifact_cell is None for artifact_cell in cells[5][: _REQUIRED[stage]])
        or any(artifact_cell is not None for artifact_cell in cells[5][_REQUIRED[stage] :])
    ):
        raise FamilyBlockUnavailable("OUTPUT custody commitment or completed fields differ")
    if cells[1] != cells[2][1] or cells[4][3] != cells[2][4] or cells[4][6] != "graph":
        raise FamilyBlockUnavailable("OUTPUT original SOURCE/retained custody join differs")
    counts = cells[4][2]
    if (
        type(counts) is not tuple
        or len(counts) != 2
        or any(type(count) is not int or not 0 <= count < 2**63 for count in counts)
    ):
        raise FamilyBlockUnavailable("OUTPUT original SOURCE count shape differs")
    retained._counts(cells[4][3])
    return cells


def _is_output(record):
    return record.kind == "artifact" and tuple(name for name, _digest in record.commitments) == (
        "output_bundle",
        "source_ready",
    )


class OriginalPrefix:
    """Validate the entire bounded OUTPUT suffix before exposing earlier records."""

    def __init__(self, snapshot):
        self.snapshot, self.head, self.limits = snapshot, snapshot.head, snapshot.limits

    def records(self, *, check=None):
        """Retain the native head while validating every known suffix and original EOF."""
        expected, newest, has_prefix = None, None, False
        for head, record in self.snapshot.records(check=check):
            if not _is_output(record):
                if expected not in (None, -1):
                    raise FamilyBlockUnavailable("OUTPUT custody lacks its original inputs")
                has_prefix = True
                yield head, record
                continue
            if has_prefix:
                raise FamilyBlockUnavailable("OUTPUT custody interrupts its original prefix")
            cells = _bundle(record)
            if expected is not None and cells[8] != expected:
                raise FamilyBlockUnavailable("OUTPUT custody stages repeat or have a gap")
            if newest is not None and (
                cells[1:5] != newest[1:5]
                or cells[6:8] != newest[6:8]
                or cells[5][: _REQUIRED[cells[8]]] != newest[5][: _REQUIRED[cells[8]]]
            ):
                raise FamilyBlockUnavailable("OUTPUT custody replaces a completed original")
            newest, expected = cells, cells[8] - 1
        if not has_prefix or expected not in (None, -1):
            raise FamilyBlockUnavailable("OUTPUT custody lacks complete original EOF")


def latest(snapshot, check):
    """Read only the newest protected OUTPUT bundle after checking its entire chain."""
    deque(OriginalPrefix(snapshot).records(check=check), maxlen=0)
    with closing(snapshot.records(check=check)) as records:
        first = next(records, None)
    return None if first is None or not _is_output(first[1]) else _bundle(first[1])


def _output_cells(output, check):
    root = output._work.root
    if output._closed or output.path != root / "output.mpk" or {path.name for path in root.iterdir()} != {"output.mpk"}:
        raise FamilyBlockUnavailable("OUTPUT producer is not its closed original file")
    stamp, digest = _hash_file(output.path, check=check)
    if digest != output.sha256:
        raise FamilyBlockUnavailable("OUTPUT producer bytes changed")
    return str(root), _directory(root), stamp, digest, _pack_row(output.receipt), astuple(output._work.limits)


def source_cells(source):
    """Preserve the actual admission/event files, counts and original receipts."""
    return (
        astuple(source.admission),
        astuple(source.events),
        astuple(source.counts),
        astuple(source.homes),
        _pack_row(source.common_receipt),
        _pack_row(source.event_receipt),
        source.phase,
    )


def _artifacts(owner, stage):
    from process.custom_import.family_shard_source_values import prepared_source_descriptor

    values = [None] * 6
    if stage >= 1:
        values[0] = astuple(owner.generation), owner._generation_observation
    if stage >= 2:
        values[1] = prepared_source_descriptor(owner.prepared)
    if stage >= 3:
        values[2] = _output_cells(owner.member_winners[0], owner.current)
        values[3] = astuple(owner.member_winners[1])
        values[4] = _pack_row(owner.winner_receipt)
    if stage >= 4:
        values[5] = (
            (False,)
            if owner.projection_output is None
            else (
                True,
                _output_cells(owner.projection_output, owner.current),
                _pack_row(owner.projection_output.proof_inputs),
            )
        )
    return tuple(values)


def _record(owner, stage, tail):
    root = owner.source.owner_lock.root
    cells = (
        _CONTRACT,
        owner.source.ready_record.input_sha256,
        tail,
        (str(owner.root), _directory(owner.root), astuple(owner.limits)),
        source_cells(owner.source),
        _artifacts(owner, stage),
        tuple(owner.source.custody.profile),
        (str(root), owner.source.owner_lock.identity),
        stage,
    )
    payload = _pack_row(cells)
    digest = hashlib.sha256(payload).digest()
    return checkpoint.OwnerCheckpointRecord(
        "artifact",
        stage,
        digest,
        (("output_bundle", digest), ("source_ready", cells[1])),
        payload,
    )


async def _native(session, owner):
    """Observe current native phase, original SOURCE and immutable generation."""
    context = owner.source.context
    descriptor, binding = await store._candidate(
        session,
        context.request,
        context.family_id,
        context.layout,
        {"loading", "closed", "verified", "logged"},
    )
    from db.models.custom_import import CustomImportBuildAttempt

    build = await session.get(CustomImportBuildAttempt, context.build_id, populate_existing=True)
    if (
        build is None
        or build.phase not in {"graph", "output", "verifying"}
        or build.source_frozen_at is None
        or build.candidate_error_count != 0
        or build.source_occurrence_count != owner.source.allocated.source.counts[0]
        or (build.phase == "graph" and descriptor.phase != "loading")
        or (build.phase == "output" and descriptor.phase not in {"loading", "closed"})
        or (build.phase == "verifying" and descriptor.phase not in {"closed", "verified", "logged"})
        or (build.phase == "graph" and (build.generation_id is not None or build.graph_frozen_at is not None))
        or (
            build.phase in {"output", "verifying"}
            and (
                build.generation_id is None
                or build.plan_stage != "complete"
                or build.plan_complete_at is None
                or build.graph_frozen_at is None
                or (build.phase == "output") != (build.output_frozen_at is None)
            )
        )
    ):
        raise FamilyBlockUnavailable("OUTPUT native phase or completed SOURCE differs")
    if owner.generation is not None and build.generation_id != owner.generation.generation_id:
        raise FamilyBlockUnavailable("OUTPUT original generation differs")
    if owner.generation is not None:
        await _observe_generation(session, owner, build)
    return build, descriptor, binding


async def _observe_generation(session, owner, build):
    """Keep every immutable original generation field and timestamp unchanged."""
    from db.models.custom_import import CustomImportGeneration

    generation = await session.get(CustomImportGeneration, build.generation_id, populate_existing=True)
    request = owner.source.context.request
    expected_by_name = dict(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=build.capture_bundle_id,
        base_generation_id=request.expected_base_generation_id,
        base_dataset_id=None if request.expected_base_generation_id is None else request.dataset_id,
        producing_fence=request.fence,
        producing_token_sha256=bytes(build.producing_token_sha256),
    )
    if generation is None or any(getattr(generation, name) != expected for name, expected in expected_by_name.items()):
        raise FamilyBlockUnavailable("OUTPUT generation producer or base differs")
    observed = tuple(
        getattr(generation, name)
        for name in (
            "generation_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "execution_id",
            "capture_bundle_id",
            "base_generation_id",
            "base_dataset_id",
            "source_bundle_sha256",
            "candidate_sha256",
            "root_count",
            "family_count",
            "producing_fence",
            "producing_token_sha256",
            "created_at",
        )
    ) + (build.source_frozen_at, build.plan_complete_at, build.graph_frozen_at)
    original = getattr(owner, "_generation_observation", None)
    if original is not None and observed != original:
        raise FamilyBlockUnavailable("OUTPUT original generation header or timestamp changed")
    owner._generation_observation = observed


async def has_copy_receipt(factory, owner, receipt):
    """Observe an exact receipt; closed candidates may never be missing one."""
    if not is_enabled(owner):
        return False
    context = owner.source.context
    async with _page_session(factory, context.request, context.build_id) as (session, _build):
        _build, descriptor, binding = await _native(session, owner)
        table = binding.tables_by_role["shard_receipt"]
        row = (
            (await store._execute(session, select(table).where(table.c.shard_id == receipt["shard_id"])))
            .mappings()
            .one_or_none()
        )
        if row is None:
            if descriptor.phase != "loading":
                raise FamilyBlockUnavailable("closed OUTPUT is missing an original COPY receipt")
            return False
        expected = store._checked_receipt(context.layout, receipt)
        if (
            {name: row[name] for name in expected} != expected
            or row["completed_at"] is None
            or row["completed_at"].utcoffset() is None
        ):
            raise FamilyBlockUnavailable("OUTPUT native COPY receipt differs")
        return True


async def commit(factory, owner, name):
    """Confirm one exact successor while loading; ambiguous replies retain originals."""
    if not is_enabled(owner):
        return
    index = _STAGES.index(name)
    source_owner, control = owner.source, owner.source.allocation_journal
    snapshot = await journal._custody_snapshot(factory, source_owner.context, control)
    previous = latest(snapshot, owner.current)
    tail = retained.latest(OriginalPrefix(snapshot), owner.current)
    if tail is None or tail[6] != len(retained._STAGES) - 1:
        raise FamilyBlockUnavailable("OUTPUT requires original completed retained custody")
    if index >= 1:
        async with _page_session(factory, source_owner.context.request, source_owner.context.build_id) as (
            session,
            _build,
        ):
            await _native(session, owner)
    custody_record = await _cpu(owner, _record, owner, index, tail)
    if previous is not None and previous[8] >= index:
        if (
            previous[1:5] != _bundle(custody_record)[1:5]
            or previous[5][: _REQUIRED[index]] != _bundle(custody_record)[5][: _REQUIRED[index]]
        ):
            raise FamilyBlockUnavailable("OUTPUT retry replaces completed custody")
        return
    if (previous is None and index != 0) or (previous is not None and previous[8] != index - 1):
        raise FamilyBlockUnavailable("OUTPUT next custody stage differs")
    before, failure = snapshot.head, None
    try:
        async with _page_session(factory, source_owner.context.request, source_owner.context.build_id) as (
            session,
            _build,
        ):
            _, descriptor, _binding = await _native(session, owner)
            if descriptor.phase != "loading":
                raise FamilyBlockUnavailable("OUTPUT custody must precede candidate closure")
            reservation = journal.reserve_checkpoint(control)
            prepared = await checkpoint.append_owner_checkpoint(
                session,
                source_owner.context.request,
                source_owner.context.build_id,
                source_owner.context.family_id,
                source_owner.context.layout,
                storage=snapshot,
                record=custody_record,
                expected_head=before,
            )
            journal.finish_checkpoint_charge(control, reservation, prepared)
    except Exception as error:
        failure = error
    await _confirm_append(factory, owner, before, custody_record, failure)


async def _confirm_append(factory, owner, before, custody_record, failure):
    """Accept only the original immediate native successor after a lost reply."""
    confirmed = await journal._custody_snapshot(factory, owner.source.context, owner.source.allocation_journal)
    if confirmed.head == before and failure is not None:
        raise failure
    with closing(confirmed.records(check=owner.current)) as checkpoint_records:
        newest, preceding = next(checkpoint_records, None), next(checkpoint_records, None)
    if (
        newest is None
        or newest[1] != custody_record
        or newest[0].revision != before.revision + 1
        or preceding is None
        or preceding[0] != before
    ):
        raise FamilyBlockUnavailable("OUTPUT custody commit is unknown; preserve original files")


def _persistent_usage(owner):
    """Census the complete original OUTPUT tree before cleanup or new scratch."""
    owner.current()
    root = owner.root
    if _directory(root)[:2] != owner._root_identity:
        raise FamilyBlockUnavailable("OUTPUT resource directory identity differs")
    unit = max(512, os.statvfs(root).f_frsize or os.statvfs(root).f_bsize)
    budget = journal._JournalBudget(owner.limits.max_spool_bytes, 0)
    with journal._census_directory(None, str(root)) as (directory, observed):
        budget.charge(journal._entry_cost(observed, unit))
        for entry in os.scandir(directory):
            if not _CHILD.fullmatch(entry.name):
                raise FamilyBlockUnavailable("OUTPUT contains an unknown producer or failed attempt")
            journal._census_tree(directory, entry.name, budget, unit, owner.current)
    return budget.used, unit


def remaining(owner):
    """Charge every persistent/failed OUTPUT child before another sequential writer."""
    if not is_enabled(owner):
        return owner.limits
    used, unit = _persistent_usage(owner)
    maximum = owner.limits.max_spool_bytes - used - 2 * unit
    if maximum < max(owner.limits.max_record_bytes, owner.limits.memory_bytes):
        raise SourceFactBudgetExceeded("persistent OUTPUT originals exhaust their admitted allowance")
    return replace(owner.limits, max_spool_bytes=maximum)


@contextmanager
def proof_work(owner):
    """Keep the charged proof parent only while verification/finality own its children."""
    limits = remaining(owner)
    root = owner.root / ("proof-" + uuid4().hex)
    owner.work.register_cleanup(root)
    root.mkdir(mode=0o700)
    identity, primary = _directory(root), None
    try:
        yield PlanWork(root, limits, owner.work.register_cleanup, owner.current)
    except BaseException as error:
        primary = error
    try:
        _remove_proof_parent(owner, root, identity)
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary


def _remove_proof_parent(owner, root, identity):
    """Remove only the original empty parent; keep any failed child as evidence."""
    owner.source.owner_lock.check()
    if _directory(owner.root)[:2] != owner._root_identity or _directory(root) != identity:
        raise FamilyBlockUnavailable("OUTPUT proof directory identity differs")
    root.rmdir()


def remove_root(owner):
    """Remove exact terminal custody only after its live readers have drained."""

    def current():
        """Terminal file cleanup retains custody after processing authority expires."""
        owner.source.owner_lock.check()
        if _directory(owner.root)[:2] != owner._root_identity:
            raise FamilyBlockUnavailable("OUTPUT terminal directory identity differs")

    current()
    # Complete strict census before removing even empty or failed proof directories.
    unit = max(512, os.statvfs(owner.root).f_frsize)
    with journal._census_directory(None, str(owner.root)) as (directory, _observed):
        budget = journal._JournalBudget(owner.limits.max_spool_bytes, 0)
        for entry in os.scandir(directory):
            if not _CHILD.fullmatch(entry.name):
                raise FamilyBlockUnavailable("OUTPUT terminal child is unknown")
            journal._census_tree(directory, entry.name, budget, unit, current)
    shutil.rmtree(owner.root)


def reopen_output(cells, owner, *, projection=None):
    """Borrow one committed member/projection file under the original OUTPUT slot."""
    from process.custom_import.family_shard_facts import SourceFactLimits
    from process.custom_import.family_shard_projection_stage import ProjectionStageOutput

    root_name, directory, stamp, digest, receipt, limits = cells
    if any(actual > ceiling for actual, ceiling in zip(limits, astuple(owner.limits), strict=True)):
        raise FamilyBlockUnavailable("OUTPUT original replay limits exceed its profile")
    root = Path(root_name)
    if root.parent != owner.root or root.name not in {"members", "projection"} or _directory(root) != directory:
        raise FamilyBlockUnavailable("OUTPUT original producer slot differs")
    if {path.name for path in root.iterdir()} != {"output.mpk"} or _hash_file(
        root / "output.mpk", check=owner.current
    ) != (stamp, digest):
        raise FamilyBlockUnavailable("OUTPUT original producer file differs")
    work = _BorrowedWork(root, directory[:2], SourceFactLimits(*limits))
    work._io_check = owner.current
    output = (
        FamilyShardOutput(work, root / "output.mpk", _unpack_row(receipt))
        if projection is None
        else ProjectionStageOutput(work, root / "output.mpk", _unpack_row(receipt), projection)
    )
    if output.sha256 != digest:
        raise FamilyBlockUnavailable("OUTPUT reopened digest differs")
    return output


def check_stage_children(owner, cells):
    """Unconfirmed completed files remain evidence, never replacement work."""
    expected_names = set()
    if cells[8] >= 2:
        expected_names.add("source-families")
    if cells[8] >= 3:
        expected_names.update(("members", "members-winners"))
    if cells[8] >= 4 and cells[5][5][0]:
        expected_names.add("projection")
    producer_names = {"source-families", "members", "members-winners", "projection"}
    actual_producers = set()
    for path in owner.root.iterdir():
        owner.current()
        if not _CHILD.fullmatch(path.name):
            raise FamilyBlockUnavailable("OUTPUT original directory has an unknown child")
        if path.name in producer_names:
            actual_producers.add(path.name)
    if actual_producers != expected_names:
        raise FamilyBlockUnavailable("OUTPUT producer return lacks its exact committed custody")


def _descriptor(descriptor, filename, reader, owner):
    root = Path(descriptor.root)
    relative = root.relative_to(owner.owner_lock.root)
    if not relative.parts or relative.parts[0] != "source" or ".." in relative.parts:
        raise FamilyBlockUnavailable("completed SOURCE file lies outside its original slot")
    parts = relative.parts
    if filename == "admission.mpk":
        valid = parts == ("source", "admission") or (
            len(parts) == 3
            and parts[0] == "source"
            and re.fullmatch(r"resume-[0-9a-f]{32}", parts[1])
            and parts[2] == "admission"
        )
    else:
        valid = (
            len(parts) == 4
            and parts[:3] == ("source", "allocation-journal", "outputs")
            and re.fullmatch(r"events-[0-9a-f]{32}", parts[3])
        )
    if not valid:
        raise FamilyBlockUnavailable("completed SOURCE descriptor role differs")
    parent = owner.owner_lock.root
    for part in relative.parts:
        parent /= part
        _directory(parent)
    if {path.name for path in root.iterdir()} != {filename}:
        raise FamilyBlockUnavailable("completed SOURCE original file census differs")
    stamp, digest = _hash_file(root / filename, check=owner.current)
    if _directory(root)[:2] != (descriptor.device, descriptor.inode) or (stamp[0], stamp[1], stamp[4], digest) != (
        descriptor.file_device,
        descriptor.file_inode,
        descriptor.file_bytes,
        descriptor.sha256,
    ):
        raise FamilyBlockUnavailable("completed SOURCE original descriptor differs")
    with closing(reader(descriptor)) as artifact_rows:
        for _row in artifact_rows:
            owner.current()
    if _hash_file(root / filename, check=owner.current) != (stamp, digest):
        raise FamilyBlockUnavailable("completed SOURCE original changed during EOF")
    return descriptor


def completed_source_files(owner, cells):
    """Exhaust the exact original admission/event files without rebuilding either."""
    from process.custom_import.family_shard_admission import AdmissionOutputDescriptor, read_admission_occurrences
    from process.custom_import.family_shard_events import AdmissionEventsDescriptor, read_allocated_events

    admission = _descriptor(AdmissionOutputDescriptor(*cells[0]), "admission.mpk", read_admission_occurrences, owner)
    events = _descriptor(AdmissionEventsDescriptor(*cells[1]), "events.mpk", read_allocated_events, owner)
    return admission, events


def _replay_census(owner, root):
    """Keep the entire exact disposable-file census inside existing memory/disk bounds."""
    files, memory = {}, sys.getsizeof({})
    with journal._census_directory(None, str(root)) as (directory, observed):
        identity = journal._census_stamp(observed)
        with os.scandir(directory) as entries:
            for entry in entries:
                owner.current()
                item = entry.stat(follow_symlinks=False)
                if (
                    re.fullmatch(r"run-[0-9]{9}[.]mpk", entry.name) is None
                    or not stat.S_ISREG(item.st_mode)
                    or item.st_uid != os.getuid()
                    or item.st_dev != observed.st_dev
                    or item.st_nlink != 1
                    or stat.S_IMODE(item.st_mode) not in {0o400, 0o600, 0o644}
                ):
                    raise FamilyBlockUnavailable("OUTPUT replay contains an unknown disposable file")
                stamp = journal._census_stamp(item)
                previous = sys.getsizeof(files)
                files[entry.name] = stamp
                memory += sys.getsizeof(files) - previous + sys.getsizeof(entry.name) + sys.getsizeof(stamp)
                memory += sum(sys.getsizeof(value) for value in stamp)
                if memory > owner.limits.memory_bytes:
                    raise SourceFactBudgetExceeded("OUTPUT replay census exceeds its original memory allowance")
    return identity, files


def _remove_replay(owner, root, identity, files):
    """Unlink only independently censused scratch under the still-held original lock."""
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if journal._census_stamp(os.fstat(descriptor)) != identity:
            raise FamilyBlockUnavailable("OUTPUT replay directory changed after census")
        for name, stamp in files.items():
            if journal._census_stamp(os.stat(name, dir_fd=descriptor, follow_symlinks=False)) != stamp:
                raise FamilyBlockUnavailable("OUTPUT replay file changed after census")
        for name, stamp in files.items():
            owner.current()
            owner.source.owner_lock.check()
            if _directory(owner.root)[:2] != owner._root_identity or _directory(root)[:2] != identity[:2]:
                raise FamilyBlockUnavailable("OUTPUT replay parent identity differs")
            if journal._census_stamp(os.stat(name, dir_fd=descriptor, follow_symlinks=False)) != stamp:
                raise FamilyBlockUnavailable("OUTPUT replay file changed before removal")
            os.unlink(name, dir_fd=descriptor)
        if os.listdir(descriptor):
            raise FamilyBlockUnavailable("OUTPUT replay gained an unobserved file")
    finally:
        os.close(descriptor)
    owner.current()
    owner.source.owner_lock.check()
    if _directory(owner.root)[:2] != owner._root_identity or _directory(root)[:2] != identity[:2]:
        raise FamilyBlockUnavailable("OUTPUT replay root changed before removal")
    root.rmdir()


def _replay_work(owner):
    """Reset one fixed disposable slot only after the caller rechecks native authority."""
    owner.source.owner_lock.check()
    _persistent_usage(owner)
    root = owner.root / _REPLAY_SLOT
    if os.path.lexists(root):
        identity, files = _replay_census(owner, root)
        _remove_replay(owner, root, identity, files)
    return PlanWork(root, remaining(owner), owner.work.register_cleanup, owner.current)


async def _reopen_prepared(factory, owner, artifact):
    from process.custom_import.family_shard_common import _offload
    from process.custom_import.family_shard_facts import SourceFactLimits
    from process.custom_import.family_shard_source_values import reopen_prepared_source

    if Path(artifact[0]) != owner.root / "source-families" or _directory(Path(artifact[0])) != artifact[1]:
        raise FamilyBlockUnavailable("OUTPUT prepared SOURCE lies outside its original slot")
    policy = PlanWork(Path(artifact[0]), SourceFactLimits(*artifact[2]), owner.work.register_cleanup, owner.current)
    context = owner.source.context
    async with _page_session(factory, context.request, context.build_id) as (session, _build):
        await _native(session, owner)
        replay = await _cpu(owner, _replay_work, owner)
    owner.prepared = await _cpu(
        owner,
        lambda: reopen_prepared_source(
            owner.retained.context.common,
            owner.source.admission,
            owner.retained.final_plan,
            artifact,
            work=policy,
            replay_work=replay,
        ),
    )
    owner._stack.push_async_callback(_offload, owner.prepared.close)


async def _reopen_members(owner, artifacts):
    from process.custom_import.family_shard_winners import (
        WinnerOutputDescriptor,
        read_winner_candidates,
        read_winner_rows,
    )

    members = await _cpu(owner, reopen_output, artifacts[2], owner)
    winners = WinnerOutputDescriptor(*artifacts[3])
    if Path(winners.root) != owner.root / "members-winners":
        raise FamilyBlockUnavailable("OUTPUT winners lie outside their original slot")
    for reader in (read_winner_rows, read_winner_candidates):
        with closing(reader(winners, check=owner.current)) as artifact_rows:
            await _cpu(owner, deque, artifact_rows, 0)
    owner.member_winners, owner.winner_receipt = (members, winners), _unpack_row(artifacts[4])


async def _reopen_projection(owner, artifact):
    from process.custom_import.query_projection_values import configured_fields

    if bool(configured_fields(owner.source.context.request.definition)) != artifact[0]:
        raise FamilyBlockUnavailable("OUTPUT original projection capability differs")
    if artifact[0]:
        owner.projection_output = await _cpu(
            owner, lambda: reopen_output(artifact[1], owner, projection=_unpack_row(artifact[2]))
        )
    elif artifact != (False,):
        raise FamilyBlockUnavailable("OUTPUT unconfigured projection differs")
    owner._projection_ready = True


async def _recheck_retained(owner, cells):
    from process.custom_import.family_shard_retained_allocate import RetainedAllocationWork
    from process.custom_import.family_shard_retained_storage_validation import prove_retained_output
    from process.custom_import.family_shard_retained_validation import verify_retained_destination

    original_work = PlanWork(
        owner.root / ("retained-original-" + uuid4().hex), remaining(owner), owner.work.register_cleanup, owner.current
    )
    allocation_work = RetainedAllocationWork(
        original_work.root,
        replace(owner.source.limits.allocation, spool=original_work.limits),
        original_work.register_cleanup,
        original_work.check,
    )
    original = await verify_retained_destination(
        owner.retained.context, owner.retained.destination, work=allocation_work
    )
    if retained._init_cells(original) != cells[2][5][3]:
        raise FamilyBlockUnavailable("OUTPUT retained original comparison changed")
    stored_work = PlanWork(
        owner.root / ("retained-stored-" + uuid4().hex), remaining(owner), owner.work.register_cleanup, owner.current
    )
    stored = await prove_retained_output(
        owner.retained.context,
        owner.retained.destination,
        original,
        owner.retained.encoded,
        owner.source.metadata.query_keys,
        work=stored_work,
    )
    if retained._init_cells(stored) != cells[2][5][5]:
        raise FamilyBlockUnavailable("OUTPUT retained storage comparison changed")
    owner.retained.original, owner.retained.stored = original, stored


async def reopen_stage(factory, job, cells):
    """Restore only exact committed producers; obtain every comparison afresh."""
    from process.custom_import.family_shard_facts import SourceFactLimits
    from process.custom_import.family_shard_output_stage import ShardOutputStage
    from process.custom_import.materialization import GenerationIdentity

    root, directory, limits = cells[3]
    if Path(root) != job.root / "output" or cells[6] != job._source_custody.profile:
        raise FamilyBlockUnavailable("OUTPUT root or original profile differs")
    owner = ShardOutputStage(
        job.retained, work=PlanWork(Path(root), SourceFactLimits(*limits), job.work.register_cleanup, job.current)
    )
    job.output_stage = owner
    if _directory(owner.root) != directory:
        raise FamilyBlockUnavailable("OUTPUT original directory changed")
    owner._root_identity = directory[:2]
    await _cpu(owner, check_stage_children, owner, cells)
    artifacts = cells[5]
    owner.generation = None if artifacts[0] is None else GenerationIdentity(*artifacts[0][0])
    owner._generation_observation = None if artifacts[0] is None else artifacts[0][1]
    if artifacts[1] is not None:
        await _reopen_prepared(factory, owner, artifacts[1])
    if artifacts[2] is not None:
        await _reopen_members(owner, artifacts)
    if artifacts[5] is not None:
        await _reopen_projection(owner, artifacts[5])
    await _recheck_retained(owner, cells)
    await observe(factory, owner, require_complete=cells[8] == 5)
    return owner


async def observe(factory, owner, *, require_complete=False):
    """Compare native generation, original tails and every existing COPY receipt."""
    from process.custom_import import family_shard_retained_control as control

    source_owner = owner.source
    async with _page_session(factory, source_owner.context.request, source_owner.context.build_id) as (session, _build):
        build, descriptor, binding = await _native(session, owner)
        stream_rows = await control._streams(
            session, owner.retained.context, SimpleNamespace(tail=owner.retained.destination.destination_pack_tail)
        )
        if (
            tuple((stream_row["stream_slot"], stream_row["next_pack_ordinal"]) for stream_row in stream_rows)
            != owner.retained.destination.destination_pack_tail
        ):
            raise FamilyBlockUnavailable("OUTPUT retained native tail differs")
        if descriptor.phase != "loading" and (owner.generation is None or owner.member_winners is None):
            raise FamilyBlockUnavailable("closed OUTPUT lacks original completed producers")
        if build.phase in {"output", "verifying"} and (build.plan_complete_at is None or build.graph_frozen_at is None):
            raise FamilyBlockUnavailable("OUTPUT native graph completion differs")
        if owner.projection_output is not None:
            from process.custom_import.family_shard_projection_stage import _proof_inputs

            if owner.projection_output.proof_inputs != _proof_inputs(
                owner.retained.context.common, source_owner.context.build_id, binding, owner.generation
            ):
                raise FamilyBlockUnavailable("OUTPUT original projection inputs changed")
    receipts = [
        source_owner.common_receipt,
        source_owner.event_receipt,
        owner.retained.common_receipt,
        owner.retained.encoded.receipt,
    ]
    if owner.member_winners is not None:
        receipts.extend((owner.member_winners[0].receipt, owner.winner_receipt))
    if owner.projection_output is not None:
        receipts.append(owner.projection_output.receipt)
    for index, receipt in enumerate(receipts):
        if not await has_copy_receipt(factory, owner, receipt) and (index < 4 or require_complete):
            raise FamilyBlockUnavailable("OUTPUT is missing an already completed original receipt")
