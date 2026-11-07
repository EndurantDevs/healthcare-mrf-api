# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""SOURCE/admission owners for a trusted coordinator, without runtime activation.

Protected SOURCE files can reopen through admission under the original request
and lock. Retained work and continuation remain separate gates. Missing custody
never authorizes recapture, replacement IDs, or reconstruction from native counts.
"""

import asyncio
import shutil
import stat
import threading
from contextlib import AsyncExitStack, closing
from dataclasses import astuple, dataclass
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from process.custom_import import family_block_store as store
from process.custom_import import family_shard_admission as admission_api
from process.custom_import import family_shard_allocate as allocation_api
from process.custom_import import family_shard_capture as capture_api
from process.custom_import import family_shard_common as common_api
from process.custom_import import family_shard_events as events_api
from process.custom_import import family_shard_events_journal as event_journal
from process.custom_import import family_shard_metadata_journal as metadata_journal
from process.custom_import import family_shard_source_checkpoint as source_checkpoint
from process.custom_import import family_shard_source_journal as source_journal
from process.custom_import import family_shard_tasks as tasks_api
from process.custom_import.build_graph import _snapshot
from process.custom_import.build_source import SourceBuildRequest, _drain_cleanup, _page_session
from process.custom_import.family import CandidateRejected
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_records import _integer
from process.custom_import.family_shard_admission_control import complete_shard_admission
from process.custom_import.family_shard_control import _empty_candidate, complete_shard_source
from process.custom_import.family_shard_counts import count_admitted_source_outcomes
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_homes import register_source_homes
from process.custom_import.family_shard_input import cleanup_spool
from process.custom_import.family_shard_metadata import (
    owner_metadata_scope,
    owner_source_metadata,
    reopen_owner_metadata,
)
from process.custom_import.family_shard_outcome_binding import bind_source_outcomes
from process.custom_import.family_shard_owner_lock import OwnerJobLock
from process.custom_import.family_shard_plan import PlanWork, _cpu
from process.custom_import.family_shard_route import prepare_routed_capture, route_capture
from process.custom_import.family_shard_source_journal import SourceAllocationJournal, create_source_allocation_journal
from process.custom_import.family_shard_source_plan import cleanup_source_plan
from process.custom_import.family_shard_worker import SourceWorkerLimits
from process.custom_import.runner_types import CancellationRequested, CandidateRegistry


@dataclass(frozen=True)
class SourceStageContext:
    """Actual request/registry/candidate/build supplied by the initial owner gate."""

    request: SourceBuildRequest
    registry: CandidateRegistry
    layout: FamilyBlockLayout
    family_id: int
    build_id: int
    owner_profile: tuple | None = None

    def __post_init__(self):
        if type(self.request) is not SourceBuildRequest or type(self.registry) is not CandidateRegistry:
            raise ValueError("SOURCE stage requires original typed request and registry")
        if (
            type(self.layout) is not FamilyBlockLayout
            or self.layout.definition.digest != self.request.definition.digest
        ):
            raise ValueError("SOURCE stage layout differs")
        if dict(self.layout.collection_slots) != dict(self.registry.child_collection_slots):
            raise ValueError("SOURCE stage collection registry differs")
        _integer(self.family_id, "SOURCE stage family")
        _integer(self.build_id, "SOURCE stage build")


@dataclass(frozen=True)
class SourceStageLimits:
    """Existing phase budgets under the caller's aggregate node/CPU policy."""

    preparation: capture_api.SourcePreparationLimits
    allocation: allocation_api.SourceAllocationLimits
    facts: SourceFactLimits
    worker: SourceWorkerLimits
    maximum_workers: int = 8
    checkpoint: SourceFactLimits | None = None

    def __post_init__(self):
        if (
            type(self.preparation) is not capture_api.SourcePreparationLimits
            or type(self.allocation) is not allocation_api.SourceAllocationLimits
        ):
            raise ValueError("SOURCE stage preparation/allocation limits differ")
        if type(self.facts) is not SourceFactLimits or type(self.worker) is not SourceWorkerLimits:
            raise ValueError("SOURCE stage fact/worker limits differ")
        if type(self.maximum_workers) is not int or not 1 <= self.maximum_workers <= 8:
            raise ValueError("SOURCE stage worker budget differs")
        if self.checkpoint is None:
            spool = self.allocation.spool
            object.__setattr__(
                self,
                "checkpoint",
                SourceFactLimits(
                    spool.max_spool_bytes,
                    spool.max_record_bytes,
                    min(spool.max_spool_bytes, max(spool.memory_bytes, 256 * 1024)),
                ),
            )
        if type(self.checkpoint) is not SourceFactLimits:
            raise ValueError("SOURCE stage checkpoint control budget differs")


class SourceAdmissionStage:
    """Explicit live custody survives advance errors until terminal cleanup.

    Construct before awaiting advance. The caller retains the original base-key
    factory/base pin and independently renews authority throughout the full job.
    No context exit here implicitly deletes evidence after an import error.
    """

    def __init__(self, context, base_keys, *, root, register_cleanup, limits, current, owner_lock=None):
        if type(context) is not SourceStageContext or type(limits) is not SourceStageLimits:
            raise ValueError("SOURCE stage requires typed context and budgets")
        if not all(callable(function) for function in (base_keys, register_cleanup, current)):
            raise ValueError("SOURCE stage factories and current check differ")
        if owner_lock is not None:
            if type(owner_lock) is not OwnerJobLock:
                raise ValueError("SOURCE stage owner lock differs")
            owner_lock.check()
            if Path(root).absolute().parent != owner_lock.root:
                raise ValueError("SOURCE stage root differs from its owner lock")
        owner_profile = context.owner_profile
        if owner_profile is not None:
            if owner_lock is None or type(owner_profile) is not tuple or len(owner_profile) != 2:
                raise ValueError("SOURCE durable profile requires its actual owner lock")
            source_journal._check_profile(owner_profile)
        self.context, self.base_keys, self.limits = context, base_keys, limits
        self.root, self.register_cleanup, self._current = Path(root).absolute(), register_cleanup, current
        self.owner_lock = owner_lock
        self.owner_profile, self.work_root = owner_profile, self.root
        self.ready = self.ready_record = self.custody = None
        self.ready_directory = None
        self.output_custody = {}
        self.stop, self._stack = threading.Event(), AsyncExitStack()
        self._root_identity, self._busy, self._closed = None, False, False
        self.pending, self.phase = None, None
        self.source_completed = False
        self.prepared = self.allocated = self.metadata = self.routed = self.tasks = None
        self.allocation_journal = None
        self.common_context = self.admission = self.counts = self.common_output = None
        self.common_custody = None
        self.common_receipt = self.events = self.event_receipt = self.homes = None
        self.runs, self.joined, self.workers = [], [], ()

    def current(self):
        """Apply the caller's immutable bounds and this advance's joined stop flag."""
        if self.stop.is_set():
            raise CancellationRequested("SOURCE stage CPU work stopped")
        self._current()
        if self.owner_lock is not None:
            self.owner_lock.check()

    def _ensure_root(self):
        if self._root_identity is None:
            if self.root.exists() or self.root.is_symlink():
                raise FileExistsError("SOURCE stage root already exists; originals must be retained")
            self.register_cleanup(self.root)
            self.root.mkdir(mode=0o700)
            observed = self.root.lstat()
            self._root_identity = observed.st_dev, observed.st_ino
        observed = self.root.lstat()
        if not stat.S_ISDIR(observed.st_mode) or (observed.st_dev, observed.st_ino) != self._root_identity:
            raise FamilyBlockUnavailable("SOURCE stage root custody differs")

    async def close_terminal(self):
        """Close exact owned resources in reverse order after the coordinator's terminal decision."""
        if self._busy:
            raise ValueError("SOURCE stage advance must drain before terminal cleanup")
        if self._closed:
            return
        primary = None
        for run in reversed(self.runs):
            primary = await _drain_cleanup(run.stop(terminate=True), primary)
        if primary is not None:
            raise primary
        primary = await _drain_cleanup(self._stack.aclose(), None)
        if primary is not None:
            raise primary
        if self._root_identity is not None:
            try:
                self._ensure_root()
                if self.owner_profile is None:
                    self.root.rmdir()
                else:
                    self.owner_lock.check()
                    shutil.rmtree(self.root)
            except BaseException as error:
                primary = primary or error
        if primary is not None:
            raise primary
        self._closed = True


async def _retain(owner, name, operation, cleanup=None):
    if getattr(owner, name) is not None:
        return
    owner.pending = name

    async def _record():
        completed = await operation()
        setattr(owner, name, completed)
        if cleanup is not None:
            owner._stack.push_async_callback(common_api._offload, cleanup, completed)
        owner.pending = None

    task = asyncio.create_task(_record())
    try:
        await asyncio.shield(task)
    except BaseException as error:
        task.cancel()

        async def _join():
            return await task

        primary = await _drain_cleanup(_join(), error)
        raise primary


async def _retain_cpu(owner, name, operation, cleanup=None):
    def _record():
        completed = operation()
        setattr(owner, name, completed)
        if cleanup is not None:
            owner._stack.push_async_callback(common_api._offload, cleanup, completed)
        owner.pending = None

    if getattr(owner, name) is None:
        owner.pending = name
        await _cpu(owner, _record)


async def _native_phase(factory, owner):
    owner.current()
    context = owner.context
    observed = await _snapshot(factory, context.request, context.build_id)
    if observed.phase not in {"source", "admission", "graph", "rejected"}:
        raise FamilyBlockUnavailable(
            "SOURCE stage requires initial/admission/graph dispatch; later phases belong to finality"
        )
    async with _page_session(factory, context.request, context.build_id) as (session, build):
        _, binding = await store._candidate(session, context.request, context.family_id, context.layout, {"loading"})
        if build.phase != observed.phase:
            raise FamilyBlockUnavailable("SOURCE native phase changed during dispatch")
        if build.phase != "source" and (
            owner.allocated is None
            or build.source_frozen_at is None
            or build.source_occurrence_count != owner.allocated.source.counts[0]
        ):
            raise FamilyBlockUnavailable("SOURCE replay requires its exact frozen occurrence count")
        if owner.prepared is None:
            await _empty_candidate(session, context.request, binding)
        phase = build.phase
    owner.current()
    return phase


def _work(owner, name):
    root, limits = owner.work_root / name, owner.limits.facts
    if owner.custody is not None:
        limits = source_journal.retained_work_limits(owner.custody, owner.owner_lock, root, name, limits, owner.current)
    return PlanWork(root, limits, owner.register_cleanup, owner.current)


def _reopen_work_root(owner):
    """Refuse exhausted retained attempts before creating another empty resume root."""
    root = owner.root / ("resume-" + uuid4().hex)
    source_journal.retained_work_limits(
        owner.custody, owner.owner_lock, root / "ready-read", "ready-read", owner.limits.facts, owner.current
    )
    owner.register_cleanup(root)
    root.mkdir(mode=0o700)
    owner.work_root = root


def owner_source_profile(owner):
    """Bind the complete owner's original SQL, file and worker budgets."""
    return (*owner.owner_profile, astuple(owner.limits))


async def _prepare_ready(factory, owner):
    context = owner.context
    await _retain_cpu(
        owner,
        "ready",
        lambda: source_checkpoint.prepare_owned_source_ready(
            owner,
            PlanWork(owner.root / "ready", owner.limits.facts, owner.register_cleanup),
            metadata_work=_work(owner, "ready-scratch"),
        ),
    )
    if owner.ready_record is None:
        custody = source_journal.source_owner_custody(
            owner.owner_lock, owner.root, owner.allocation_journal, owner_source_profile(owner)
        )
        ready_record = await _cpu(owner, source_checkpoint.source_ready_record, owner.ready, custody)
        await source_journal.commit_source_custody(factory, context, owner.allocation_journal, ready_record)
        owner.ready_record, owner.custody = ready_record, custody
    owner.prepared, owner.allocated, owner.routed, owner.tasks = (
        owner.ready.prepared,
        owner.ready.allocated,
        owner.ready.shards,
        owner.ready.tasks,
    )
    owner.common_context = common_api.SourceCommonContext(context.request, context.layout, owner.allocated)


def _reopen_metadata(owner, snapshot):
    """Verify original journal vectors before exposing the sealed metadata copy."""
    journal = owner.allocation_journal
    work = metadata_journal.MetadataJournalScratch(
        journal,
        PlanWork(
            journal.root / "outputs" / ("reopen-" + uuid4().hex),
            journal.work.limits,
            owner.register_cleanup,
            owner.current,
        ),
    )
    try:
        event_journal._verify_metadata(
            SimpleNamespace(
                factory=None,
                source_context=common_api.SourceCommonContext(
                    owner.context.request, owner.context.layout, owner.allocated
                ),
                request=owner.context.request,
                layout=owner.context.layout,
                source=owner.allocated,
                journal=journal,
                work=work,
                limits=owner.limits.allocation,
            ),
            event_journal.AdmissionSourcePrefix(snapshot, owner.limits.allocation.maximum_batch_records),
        )
    finally:
        work.close()
    ready = owner.ready
    manager = reopen_owner_metadata(
        ready.metadata.root,
        expected_scope=owner_metadata_scope(owner.context.request, owner.allocated),
        descriptor_sha256=ready.metadata.descriptor_sha256,
        work=_work(owner, "metadata"),
    )
    metadata = manager.__enter__()
    owner._stack.push_async_callback(common_api._offload, manager.__exit__, None, None, None)
    return metadata


def bind_source_custody(owner, custody):
    """Install protected cleanup custody before another fallible resource is opened."""
    if owner.context.request.continuation is not None or owner.owner_profile is None:
        raise FamilyBlockUnavailable("SOURCE reopening requires the original complete owner")
    custody.current(owner.owner_lock.root, owner_source_profile(owner))
    owner.owner_lock.check()
    owner._root_identity = custody.directories[1][1][:2]
    owner.custody = custody
    owner._ensure_root()


async def _reopen_prepared(factory, owner):
    """Open the sealed SOURCE descriptor through its current authenticated header."""
    context = owner.context
    ready = await source_checkpoint.reopen_source_ready(
        factory,
        context,
        root=owner.root / "ready",
        checkpoint_root=owner.root / "allocation-journal" / "checkpoints",
        limits=owner.limits.facts,
        checkpoint_limits=owner.limits.checkpoint,
        worker_limits=owner.limits.worker,
        metadata_work=_work(owner, "ready-read"),
    )
    owner.ready = ready
    owner.prepared, owner.allocated, owner.routed, owner.tasks = (
        ready.prepared,
        ready.allocated,
        ready.shards,
        ready.tasks,
    )


async def reopen_source_admission(factory, owner, snapshot, ready_record, custody, outputs, common):
    """Hydrate original SOURCE/admission custody under the reacquired lifetime lock."""
    context = owner.context
    bind_source_custody(owner, custody)
    phase = (await _snapshot(factory, context.request, context.build_id)).phase
    if phase not in {"source", "admission", "graph", "rejected"}:
        raise FamilyBlockUnavailable("SOURCE reopening cannot adopt later processing phases")
    if phase == "source":
        with closing(snapshot.records(check=owner.current)) as checkpoint_records:
            latest = next(checkpoint_records, None)
        if latest is not None and latest[1].kind == "admission_allocation":
            raise FamilyBlockUnavailable("SOURCE reopening found premature admission progress")
    _reopen_work_root(owner)
    await _reopen_prepared(factory, owner)
    journal, confirmed = await source_journal.reopen_source_allocation_journal(
        factory,
        context,
        custody,
        owner.owner_lock,
        work=PlanWork(
            owner.root / "allocation-journal", owner.limits.allocation.spool, owner.register_cleanup, owner.current
        ),
        control_limits=owner.limits.checkpoint,
        head=snapshot.head,
    )
    owner.allocation_journal = journal
    owner._stack.push_async_callback(common_api._offload, journal.close_terminal)
    owner.ready_record, owner.custody = ready_record, custody
    owner.metadata = await _cpu(owner, _reopen_metadata, owner, confirmed)
    owner.output_custody = {output.artifact[0]: output for output in outputs}
    owner.common_custody = common
    owner.common_context = common_api.SourceCommonContext(context.request, context.layout, owner.allocated)
    if await _native_phase(factory, owner) != phase:
        raise FamilyBlockUnavailable("SOURCE native phase changed during reopening")
    if phase != "source":
        await _reopen_admitted_source(factory, owner, snapshot, phase)


async def _reopen_admitted_source(factory, owner, snapshot, phase):
    """Hydrate frozen SOURCE evidence without launching workers or replaying SOURCE COPY."""
    if owner.common_custody is None:
        raise FamilyBlockUnavailable("frozen SOURCE lacks its protected common output")
    if phase in {"graph", "rejected"}:
        event_journal.require_completed_admission(
            snapshot, owner.limits.allocation.maximum_batch_records, owner.current
        )
    owner.workers = await _cpu(
        owner,
        tasks_api.reopen_source_outputs,
        owner.tasks,
        tuple(owner.output_custody.values()),
        owner.root / ("borrow-" + uuid4().hex),
        owner.limits.worker,
        owner.owner_lock,
    )
    await _admit(owner)
    await _reopen_common(owner)
    await _retain_cpu(
        owner,
        "common_receipt",
        lambda: _receipt(owner, len(owner.workers), _output_batches(owner, owner.common_output)),
    )
    owner.source_completed = True
    if phase in {"graph", "rejected"}:
        await _prepare_events(factory, owner)


async def reopen_completed_source(factory, owner, snapshot, ready_record, custody, outputs, common, completed):
    """Borrow completed original evidence for later phases; never dispatch SOURCE."""
    from process.custom_import import family_shard_checkpoint as checkpoints
    from process.custom_import import family_shard_output_checkpoint as output_checkpoint
    from process.custom_import.build_counts import SourceOutcomeCounts
    from process.custom_import.family_shard_homes import SourceHomeCounts
    from process.custom_import.family_shard_output import _unpack_row

    bind_source_custody(owner, custody)
    context = owner.context
    async with _page_session(factory, context.request, context.build_id) as (session, build):
        descriptor, _scope = await checkpoints._native_scope(
            session, context.request, context.build_id, context.family_id, context.layout, writing=False
        )
        if build.phase not in {"graph", "output", "verifying", "verified"} or build.source_frozen_at is None:
            raise FamilyBlockUnavailable("completed SOURCE requires its original later native build")
        counts = SourceOutcomeCounts(*completed[2])
        if (descriptor.source_accepted_family_count, descriptor.source_rejection_count) != astuple(counts):
            raise FamilyBlockUnavailable("completed SOURCE native outcome counts differ")
    # Completed metadata is borrowed read-only; no writable resume root is needed.
    await _reopen_prepared(factory, owner)
    journal, confirmed = await source_journal.reopen_source_allocation_journal(
        factory,
        context,
        custody,
        owner.owner_lock,
        work=PlanWork(
            owner.root / "allocation-journal", owner.limits.allocation.spool, owner.register_cleanup, owner.current
        ),
        control_limits=owner.limits.checkpoint,
        head=snapshot.head,
    )
    owner.allocation_journal = journal
    owner._stack.push_async_callback(common_api._offload, journal.close_terminal)
    confirmed = output_checkpoint.OriginalPrefix(confirmed)
    owner.ready_record, owner.custody = ready_record, custody
    owner.metadata = await _cpu(owner, _reopen_metadata, owner, confirmed)
    owner.output_custody = {output.artifact[0]: output for output in outputs}
    owner.common_custody = common
    owner.common_context = common_api.SourceCommonContext(context.request, context.layout, owner.allocated)
    owner.workers = await _cpu(
        owner,
        tasks_api.reopen_source_outputs,
        owner.tasks,
        tuple(owner.output_custody.values()),
        owner.root / ("borrow-" + uuid4().hex),
        owner.limits.worker,
        owner.owner_lock,
    )
    owner.admission, owner.events = await _cpu(owner, output_checkpoint.completed_source_files, owner, completed)
    owner.counts, owner.homes = counts, SourceHomeCounts(*completed[3])
    owner.common_receipt, owner.event_receipt = _unpack_row(completed[4]), _unpack_row(completed[5])
    event_journal.require_completed_admission(confirmed, owner.limits.allocation.maximum_batch_records, owner.current)
    await _reopen_common(owner)
    # This is the last completed SOURCE phase, as in a warm OUTPUT owner;
    # fresh writers still check the actual native phase before every operation.
    if completed[6] != "graph" or build.source_occurrence_count != owner.allocated.source.counts[0]:
        raise FamilyBlockUnavailable("completed SOURCE original census or phase differs")
    owner.phase, owner.source_completed = "graph", True


def _close_prepared(prepared):
    try:
        cleanup_source_plan(prepared.plan)
    finally:
        cleanup_spool(prepared.partition)


def _producer_root(owner):
    """Register one canonical parent before any durable SOURCE producer writes."""
    if owner.owner_profile is None:
        return owner.root
    owner.current()
    root = owner.root / "ready"
    if owner.ready_directory is None:
        if root.exists() or root.is_symlink():
            raise FileExistsError("SOURCE-ready producer root already exists")
        owner.register_cleanup(root)
        root.mkdir(mode=0o700)
        owner.ready_directory = source_checkpoint._directory(root)
    if source_checkpoint._directory(root) != owner.ready_directory:
        raise FamilyBlockUnavailable("SOURCE-ready producer parent was replaced")
    return root


async def _prepare_originals(factory, owner):
    context = owner.context
    producer_root = _producer_root(owner) if owner.prepared is None else owner.root
    await _retain(
        owner,
        "prepared",
        partial(
            capture_api.prepare_captured_source,
            factory,
            context.request,
            context.registry,
            context.family_id,
            root=producer_root,
            register_cleanup=owner.register_cleanup,
            limits=owner.limits.preparation,
            build_id=context.build_id,
        ),
        _close_prepared,
    )
    await _retain_cpu(
        owner,
        "allocation_journal",
        lambda: create_source_allocation_journal(
            context.build_id,
            work=PlanWork(
                owner.root / "allocation-journal", owner.limits.allocation.spool, owner.register_cleanup, owner.current
            ),
            control_limits=owner.limits.checkpoint,
        ),
        lambda journal: journal.close_terminal(),
    )
    await _retain(
        owner,
        "allocated",
        partial(
            allocation_api.allocate_source_plan,
            factory,
            context.request,
            context.family_id,
            context.layout,
            owner.prepared.plan,
            journal=owner.allocation_journal,
            limits=owner.limits.allocation,
        ),
        allocation_api.cleanup_source_allocations,
    )
    if owner.common_context is None:
        owner.common_context = common_api.SourceCommonContext(context.request, context.layout, owner.allocated)


async def _enter_metadata(factory, owner):
    context = owner.context
    return await owner._stack.enter_async_context(
        owner_source_metadata(
            factory,
            context.request,
            context.layout,
            owner.allocated,
            journal=owner.allocation_journal,
            limits=owner.limits.allocation,
            retained_query_keys=owner.base_keys(),
        )
    )


async def _prepare_tasks(factory, owner):
    context = owner.context
    await _retain(owner, "metadata", partial(_enter_metadata, factory, owner))

    def _enter_route():
        scope = route_capture(
            owner.prepared.partition,
            context.request.definition,
            root=owner.root / "route",
            register_cleanup=owner.register_cleanup,
            limits=owner.limits.facts,
            check=owner.current,
        )
        routed = scope.__enter__()
        owner._stack.push_async_callback(common_api._offload, scope.__exit__, None, None, None)
        return routed

    if owner.owner_profile is not None:
        await _retain_cpu(
            owner,
            "routed",
            lambda: prepare_routed_capture(
                owner.prepared.partition,
                context.request.definition,
                root=owner.root / "ready/routed",
                register_cleanup=owner.register_cleanup,
                limits=owner.limits.facts,
                check=owner.current,
            ),
        )
    else:
        await _retain_cpu(owner, "routed", _enter_route)
    if owner.owner_profile is not None:
        await _prepare_ready(factory, owner)
        return
    await _retain_cpu(
        owner,
        "tasks",
        lambda: tasks_api.prepare_source_tasks(
            owner.routed,
            owner.allocated,
            tasks_api.SourceTaskScope(context.request.definition, context.request.complete_scope, check=owner.current),
            owner.metadata.family_ids(),
            owner.metadata.query_keys(),
            root=owner.root / "tasks",
            register_cleanup=owner.register_cleanup,
            limits=owner.limits.facts,
        ),
        lambda tasks: tasks.close(),
    )


def _bounded_batches(owner, batches):
    with closing(batches) as source_batches:
        for batch in source_batches:
            owner.current()
            yield batch
    owner.current()


async def _copy(factory, owner, batches, receipt):
    context, limits = owner.context, owner.limits.allocation
    with closing(_bounded_batches(owner, batches)) as checked:
        await store.copy_shard(
            factory,
            context.request,
            context.family_id,
            context.layout,
            checked,
            receipt,
            maximum_rows=limits.maximum_batch_records,
            maximum_bytes=limits.maximum_batch_bytes,
        )
    owner.current()


async def _anchor_output(factory, owner, output):
    if owner.ready_record is None:
        return
    custody = await _cpu(
        owner, tasks_api.source_output_custody, owner.tasks, output, output._work.root.parent, owner.limits.worker
    )
    shard = custody.artifact[0]
    if shard in owner.output_custody:
        if owner.output_custody[shard] != custody:
            raise FamilyBlockUnavailable("SOURCE output changed after protected checkpointing")
        return
    record = source_checkpoint.source_output_record(owner.ready_record, custody)
    await source_journal.commit_source_custody(factory, owner.context, owner.allocation_journal, record)
    owner.output_custody[shard] = custody


def _output_batches(owner, output):
    limits = owner.limits.allocation
    return output.batches(maximum_rows=limits.maximum_batch_records, maximum_bytes=limits.maximum_batch_bytes)


async def _checkpoint(owner, run):
    try:
        joined = await run.checkpoint()
        if joined not in owner.joined:
            owner.joined.append(joined)
        if len(run.completed) == len(owner.tasks.files):
            if tuple(output.receipt["shard_id"] for output in run.completed) != tuple(range(len(owner.tasks.files))):
                raise FamilyBlockUnavailable("SOURCE worker cohort identities differ")
            owner.workers = tuple(run.completed)
    except BaseException:
        owner.pending = "joined"
        raise


async def _anchor_closed_outputs(factory, owner, run):
    """Preserve joined closed survivors under the original fenced custody writer."""
    if run._running or any(process.is_alive() or process.exitcode is None for process in run._processes.values()):
        raise ValueError("source workers must be joined before closed output custody")
    for output in run.completed:
        if output.receipt["shard_id"] not in owner.output_custody:
            owner.current()
            await _anchor_output(factory, owner, output)


async def _run_workers(factory, owner, attempt_root):
    if owner.workers:
        for output in owner.workers:
            await _anchor_output(factory, owner, output)
            await _copy(factory, owner, _output_batches(owner, output), output.receipt)
        return
    attempt_root = Path(attempt_root).absolute()
    if attempt_root.parent != owner.root:
        raise ValueError("SOURCE attempt must be a new direct child of the owned root")
    run, primary = None, None
    try:
        async with tasks_api.run_source_tasks(
            owner.tasks,
            root=attempt_root,
            register_cleanup=owner.register_cleanup,
            worker_limits=owner.limits.worker,
            maximum_workers=owner.limits.maximum_workers,
            preserve_completed=True,
            completed=tuple(owner.output_custody.values()) if owner.ready_record is not None else tuple(owner.joined),
            owner_lock=owner.owner_lock,
        ) as run:
            owner.runs.append(run)
            owner._stack.push_async_callback(run.cleanup)
            async for output in run:
                await _anchor_output(factory, owner, output)
                await _copy(factory, owner, _output_batches(owner, output), output.receipt)
    except BaseException as error:
        primary = error
    if run is not None:
        primary = await _drain_cleanup(_checkpoint(owner, run), primary)
        if isinstance(primary, CandidateRejected):
            primary = await _drain_cleanup(_anchor_closed_outputs(factory, owner, run), primary)
    if primary is not None:
        raise primary
    if not owner.workers:
        raise FamilyBlockUnavailable("SOURCE workers require complete cohort EOF, including empty shards")


async def _admit(owner):
    request = owner.context.request
    if owner.admission is None:
        work = _work(owner, "admission")
        await _retain_cpu(
            owner,
            "admission",
            partial(
                admission_api.build_admission_output,
                owner.allocated,
                request.definition,
                owner.workers,
                work=admission_api.AdmissionWork(work.root, work.limits, work.register_cleanup, work.check),
                complete_scope=request.complete_scope,
            ),
            admission_api.cleanup_admission_output,
        )
    if owner.counts is None:
        await _retain_cpu(
            owner,
            "counts",
            partial(count_admitted_source_outcomes, owner.common_context, owner.admission, work=_work(owner, "counts")),
        )


def _selected_entities(owner):
    with closing(admission_api.read_admission_families(owner.admission, check=owner.current)) as headers:
        for _shard, header in headers:
            if header["selection"] == "source":
                yield header["family_ids"][1]


def _receipt(owner, shard_id, batches):
    partition = bytes.fromhex(owner.allocated.source.partition_sha256)
    proof = store.ShardOutputProof(owner.context.layout, shard_id, partition, 0)
    for batch in _bounded_batches(owner, batches):
        for role, cells in batch.items():
            proof.add(role, cells)
    return proof.receipt()


async def _reopen_common(owner):
    await _retain_cpu(
        owner,
        "common_output",
        lambda: common_api.reopen_source_common(
            owner.common_context, owner.admission, owner.common_custody, owner.limits.allocation, owner.current
        ),
        lambda output: output.close(),
    )


async def _source_common(factory, owner):
    if owner.common_custody is not None:
        await _reopen_common(owner)
    await _retain(
        owner,
        "common_output",
        lambda: common_api.build_source_common(
            factory,
            owner.common_context,
            _selected_entities(owner),
            owner.metadata.query_keys(),
            root=owner.work_root / "common",
            register_cleanup=owner.register_cleanup,
            limits=owner.limits.allocation,
        ),
        lambda output: output.close(),
    )
    await _retain_cpu(
        owner,
        "common_receipt",
        lambda: _receipt(owner, len(owner.workers), _output_batches(owner, owner.common_output)),
    )
    await _anchor_common(factory, owner)
    await _copy(factory, owner, _output_batches(owner, owner.common_output), owner.common_receipt)
    await complete_shard_source(
        factory,
        owner.common_context,
        owner.context.build_id,
        owner.admission,
        owner.common_output,
        owner.common_receipt,
        limits=owner.limits.allocation,
        check=owner.current,
    )
    owner.source_completed = True


async def _anchor_common(factory, owner):
    if owner.ready_record is None:
        return
    custody = await _cpu(
        owner,
        common_api.source_common_custody,
        owner.common_context,
        owner.admission,
        owner.common_output,
        owner.common_receipt,
        owner.limits.allocation,
        owner.current,
    )
    common_api.source_common_scope(custody, owner.root, len(owner.tasks.files))
    if owner.common_custody is not None:
        if owner.common_custody != custody:
            raise FamilyBlockUnavailable("SOURCE common output changed after protected checkpointing")
        return
    record = common_api.source_common_record(owner.ready_record.input_sha256, custody)
    await source_journal.commit_source_custody(factory, owner.context, owner.allocation_journal, record)
    owner.common_custody = custody


def _event_batches(owner):
    limits = owner.limits.allocation
    return events_api.admission_event_batches(
        owner.events, maximum_rows=limits.maximum_batch_records, maximum_bytes=limits.maximum_batch_bytes
    )


async def _prepare_events(factory, owner):
    context = owner.context
    await _retain(
        owner,
        "events",
        partial(
            events_api.allocate_admission_events,
            factory,
            context.request,
            context.layout,
            owner.allocated,
            owner.admission,
            journal=owner.allocation_journal,
            limits=owner.limits.allocation,
        ),
        events_api.cleanup_admission_events,
    )
    await _retain_cpu(owner, "event_receipt", lambda: _receipt(owner, len(owner.workers) + 1, _event_batches(owner)))


async def _finish_admission(factory, owner):
    await _prepare_events(factory, owner)
    await _copy(factory, owner, _event_batches(owner), owner.event_receipt)
    await _record_admission(factory, owner)


async def _record_admission(factory, owner):
    owner.phase = await complete_shard_admission(
        factory,
        owner.common_context,
        owner.context.build_id,
        owner.admission,
        owner.events,
        owner.event_receipt,
        limits=owner.limits.allocation,
        check=owner.current,
    )


def _require_originals(owner, names):
    if any(getattr(owner, name) is None for name in names) or not owner.workers:
        raise FamilyBlockUnavailable(
            "SOURCE native progress requires retained original artifacts; durable reopening is unavailable"
        )


async def _bind_counts(factory, owner):
    await bind_source_outcomes(
        factory,
        owner.common_context,
        owner.context.build_id,
        owner.admission,
        owner.common_output,
        owner.common_receipt,
        owner.counts,
        check=owner.current,
    )


def _reconcile_pending(owner, phase):
    """Permit only fresh journal reconciliation with the original live inputs."""
    if owner.pending is None:
        return
    has_original_journal = (
        type(owner.allocation_journal) is SourceAllocationJournal
        and owner.allocation_journal.build_id == owner.context.build_id
        and owner.context.request.continuation is None
    )
    is_source_retry = owner.pending == "allocated" and owner.prepared is not None
    has_original_source = (
        type(owner.prepared) is capture_api.PreparedCaptureSource
        and type(owner.allocated) is allocation_api.AllocatedSourcePlan
        and owner.allocated.source is owner.prepared.plan
        and owner.allocated.snapshot_family_id == owner.context.family_id
    )
    is_metadata_retry = owner.pending == "metadata" and has_original_source and phase == "source"
    is_event_retry = owner.pending == "events" and has_original_source and phase in {"source", "admission"}
    if not has_original_journal or not ((is_source_retry and phase == "source") or is_metadata_retry or is_event_retry):
        raise FamilyBlockUnavailable(
            "unfinished SOURCE artifact requires retained recovery; automatic rebuilding is refused"
        )
    if is_event_retry:
        _require_originals(
            owner,
            (
                "metadata",
                "routed",
                "tasks",
                "common_context",
                "admission",
                "counts",
                "common_output",
                "common_receipt",
            ),
        )
    # This only re-enters the writer's fresh native reconciliation;
    # the pending flag never proves commit or permits replacement IDs.
    owner.pending = None


async def advance_source_admission(factory, owner, *, attempt_root):
    """Advance genuine live owners; errors retain evidence and original response counts.

    The coordinator owns initial candidate creation, heartbeat, later immutable
    phase dispatch and terminal cleanup. This does not activate a runtime path.
    """
    if type(owner) is not SourceAdmissionStage or owner._closed or owner._busy:
        raise ValueError("SOURCE stage is unavailable or already advancing")
    if owner.context.request.continuation is not None:
        raise FamilyBlockUnavailable(
            "continuation cannot allocate or readmit SOURCE; automatic rebuilding is forbidden"
        )
    owner._busy = True
    owner.stop.clear()
    try:
        phase = await _native_phase(factory, owner)
        _reconcile_pending(owner, phase)
        if phase != "source":
            _require_originals(
                owner,
                (
                    "prepared",
                    "allocated",
                    "metadata",
                    "routed",
                    "tasks",
                    "common_context",
                    "admission",
                    "counts",
                    "common_output",
                    "common_receipt",
                ),
            )
            owner.source_completed = True
        owner._ensure_root()
        if phase == "source":
            await _prepare_originals(factory, owner)
            await _prepare_tasks(factory, owner)
            await _run_workers(factory, owner, attempt_root)
            await _admit(owner)
            await _source_common(factory, owner)
        await _bind_counts(factory, owner)
        if phase in {"source", "admission"}:
            await _finish_admission(factory, owner)
        else:
            _require_originals(owner, ("events", "event_receipt"))
            await _record_admission(factory, owner)
        if owner.phase == "graph" and owner.homes is None:
            owner.homes = await register_source_homes(
                factory,
                owner.common_context,
                owner.context.build_id,
                owner.admission,
                work=_work(owner, "homes"),
                limits=owner.limits.allocation,
            )
        owner.current()
        return owner
    finally:
        owner._busy = False
