# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Trusted owner composition after sealed capture, without runtime activation.

Cold SOURCE/admission reopening requires its protected ready checkpoint, original
request and drained lifetime lock. Retained work still requires live custody.
The caller admits aggregate worker/SQL memory and supplies terminal cleanup.
"""

import re
import shutil
import stat
import threading
from contextlib import AsyncExitStack, nullcontext
from dataclasses import astuple
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select

from db.models.custom_import import CustomImportBuildAttempt
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_output
from process.custom_import import family_block_store as store
from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_output_checkpoint as output_checkpoint
from process.custom_import import family_shard_retained_checkpoint as retained_checkpoint
from process.custom_import import family_shard_source_checkpoint as source_checkpoint
from process.custom_import import family_shard_source_journal as source_journal
from process.custom_import import family_shard_terminal_cleanup as terminal_cleanup
from process.custom_import.build_counts import SourceOutcomeCounts
from process.custom_import.build_graph import _renew_while_reading, _session, _snapshot
from process.custom_import.build_source import SourceBuildRequest, _drain_cleanup, _page_session, _set_timeout
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FAMILY_BLOCK_LAYOUT_CONTRACT, FamilyBlockLayout
from process.custom_import.family_shard_base import _read_deadline, _require_deadline, pinned_base
from process.custom_import.family_shard_base_keys import pinned_base_query_keys
from process.custom_import.family_shard_common import _offload
from process.custom_import.family_shard_completion import complete_closed_shard_build
from process.custom_import.family_shard_control import begin_shard_build
from process.custom_import.family_shard_finality import seal_shard_build
from process.custom_import.family_shard_outcome_binding import terminal_source_outcomes
from process.custom_import.family_shard_output_stage import ShardOutputStage, advance_shard_output
from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock
from process.custom_import.family_shard_plan import PlanWork, _cpu, build_family_plan, cleanup_family_plan
from process.custom_import.family_shard_retained_stage import RetainedFamilyStage, advance_retained_families
from process.custom_import.family_shard_sessions import _setting, import_session_factory
from process.custom_import.family_shard_source_stage import (
    SourceAdmissionStage,
    SourceStageContext,
    SourceStageLimits,
    _retain,
    advance_source_admission,
    bind_source_custody,
    reopen_completed_source,
    reopen_source_admission,
)
from process.custom_import.runner_registry import load_registry
from process.custom_import.runner_types import CancellationRequested
from process.custom_import.snowflake_segmented_runner import _activate


class OwnerShardJob:
    """Keep original stages and base pin alive across all owner-job failures.

    Construct before awaiting advance. A path alone never permits adoption.
    Explicit terminal cleanup closes outputs before SOURCE, keys and pin.
    """

    def __init__(self, request, *, work, source_limits, projection_work_mem_mb=1024, reader_activation=None):
        if type(request) is not SourceBuildRequest or type(work) is not PlanWork:
            raise ValueError("owner job requires its original request and accounted work")
        if type(source_limits) is not SourceStageLimits:
            raise ValueError("owner job requires explicit SOURCE resource limits")
        _setting("work_mem", projection_work_mem_mb)
        self.request, self.work, self.source_limits = request, work, source_limits
        if reader_activation is not None:
            from process.custom_import.family_block_reader import ReaderActivation

            if type(reader_activation) is not ReaderActivation:
                raise ValueError("owner job Reader activation differs")
        self.reader_activation = reader_activation
        self.projection_work_mem_mb = projection_work_mem_mb
        self.root, self._root_identity = Path(work.root).absolute(), None
        self._owner_lock = None
        self._source_custody = None
        self.stop, self._stack, self._keys_stack = threading.Event(), AsyncExitStack(), AsyncExitStack()
        self._deadline, self._busy, self._closed = _read_deadline(request), False, False
        self.registry = self.layout = self.family_id = self.build_id = None
        self.base = self.base_context = self.base_keys = self.source = None
        self.final_plan = self.retained = self.output_stage = self.output = self.publication = None
        self.pending, self.initial_started, self.finality_started, self.complete = None, False, False, False
        self.counts_unavailable = False
        self.durable_counts = None
        self._plan_closed = False
        self._terminal_custody_only = False
        self._terminal_custody = self._terminal_inventory = None

    def current(self):
        """Bound thread work to the original request and caller's stop callback."""
        if self.stop.is_set():
            raise CancellationRequested("owner job CPU work stopped")
        _require_deadline(self._deadline)
        if self.work.check is not None:
            self.work.check()
        if self._owner_lock is not None:
            self._owner_lock.check()

    @property
    def source_counts(self):
        """Keep legacy zero counts until native SOURCE completion is confirmed."""
        if self.counts_unavailable:
            raise FamilyBlockUnavailable("cold owner replay requires original SOURCE outcome counts")
        if self.durable_counts is not None:
            return self.durable_counts
        if self.source is None or not self.source.source_completed:
            return SourceOutcomeCounts(0, 0)
        return self.source.counts

    @property
    def no_change(self):
        """Expose only the ordinary terminal no-change receipt, when present."""
        return None if self.output is None else self.output.no_change

    @property
    def is_rejected(self):
        """Leave native admission rejection finishing to the existing runner."""
        return self.source is not None and self.source.phase == "rejected"

    def _ensure_root(self):
        if self._root_identity is None:
            if self.root.exists() or self.root.is_symlink():
                raise FileExistsError("owner job root exists; original custody is required")
            self.work.register_cleanup(self.root)
            self.root.mkdir(mode=0o700)
            observed = self.root.lstat()
            self._root_identity = observed.st_dev, observed.st_ino
        observed = self.root.lstat()
        if not stat.S_ISDIR(observed.st_mode) or (observed.st_dev, observed.st_ino) != self._root_identity:
            raise FamilyBlockUnavailable("owner job root custody differs")
        if self._owner_lock is None:
            self._owner_lock = acquire_owner_job_lock(self.root, identity=self._root_identity)
        self._owner_lock.check()

    async def close_terminal(self):
        """Release only exact owned resources after all work and readers drain."""
        if self._busy:
            raise ValueError("owner job advance must drain before cleanup")
        if self._closed:
            return
        if self._source_custody is not None and self.source is None and not self._terminal_custody_only:
            raise FamilyBlockUnavailable("partial later owner custody must be preserved for reconciliation")
        if terminal_cleanup.is_interrupted_root_finished(self):
            return
        primary = await _drain_cleanup(terminal_cleanup.prepare(self), None)
        if primary is not None:
            raise primary
        for owner in (self.output_stage, self.retained):
            if owner is not None:
                primary = await _drain_cleanup(owner.close_terminal(), None)
                if primary is not None:
                    raise primary
        if self.final_plan is not None and not self._plan_closed:
            primary = await _drain_cleanup(_offload(cleanup_family_plan, self.final_plan), None)
            if primary is not None:
                raise primary
            self._plan_closed = True
        for operation in (
            None if self.source is None else self.source.close_terminal,
            self._keys_stack.aclose,
            self._stack.aclose,
        ):
            if operation is not None:
                primary = await _drain_cleanup(operation(), None)
                if primary is not None:
                    raise primary
        if self._root_identity is not None:
            if self._owner_lock is None:
                raise FamilyBlockUnavailable("owner job terminal cleanup requires its original lock")
            if self._source_custody is not None:
                self._owner_lock.check()
                for path in self.root.iterdir():
                    if re.fullmatch(
                        r"(?:base-keys|final-plan|retained|output|finality)(?:-[0-9a-f]{32})?", path.name
                    ) or (self._terminal_custody_only and path.name == "source"):
                        source_journal._directory(path)
                        shutil.rmtree(path)
            await _offload(terminal_cleanup.drain, self)
            self._owner_lock.remove_terminal_root()
        terminal_cleanup.finish(self)
        self._closed = True


def _work(job, name):
    slot = "base-keys" if re.fullmatch(r"base-keys(?:-[0-9a-f]{32})?", name) else name
    if job._source_custody is not None and name in {"final-plan", "retained", "finality"}:
        name += "-" + uuid4().hex
    root, limits = job.root / name, job.work.limits
    if job._source_custody is not None and slot in {"base-keys", "final-plan", "retained", "finality"}:
        limits = source_journal.retained_work_limits(
            job._source_custody, job._owner_lock, root, slot, limits, job.current
        )
    return PlanWork(root, limits, job.work.register_cleanup, job.current)


async def _existing_build(factory, request):
    """Discover only an ID; the existing replay/read guards establish authority."""
    async with _session(factory) as session:
        await _set_timeout(session, request.statement_timeout_ms)
        return (
            await session.execute(
                select(CustomImportBuildAttempt.build_id).where(
                    CustomImportBuildAttempt.execution_id == request.execution_id,
                    CustomImportBuildAttempt.producing_fence == request.fence,
                )
            )
        ).scalar_one_or_none()


async def _registry(factory, job):
    if job.registry is None:
        async with _page_session(factory, job.request, job.build_id) as (session, _build):
            registry = await load_registry(session, job.request)
        job.registry = registry
        job.layout = FamilyBlockLayout(job.request.definition, job.registry.child_collection_slots)


async def _verified_block(factory, job, build):
    """Refuse legacy/frozen adoption; actual finality still proves physical custody."""
    await _registry(factory, job)
    async with _page_session(factory, job.request, job.build_id) as (session, _build):
        registered = (
            await store._execute(
                session,
                select(CustomImportSnapshotFamily, CustomImportFamilyBlockLayout)
                .join(
                    CustomImportFamilyBlockLayout,
                    CustomImportFamilyBlockLayout.family_id == CustomImportSnapshotFamily.family_id,
                )
                .where(
                    CustomImportSnapshotFamily.execution_id == job.request.execution_id,
                    CustomImportSnapshotFamily.producing_fence == job.request.fence,
                ),
            )
        ).one_or_none()
        if registered is None:
            raise FamilyBlockUnavailable("verified owner job requires its original block registration")
        family, descriptor = registered
        if (
            (
                family.dataset_id,
                family.definition_revision_id,
                family.schema_revision_id,
                family.capture_bundle_id,
                family.generation_id,
            )
            != (
                job.request.dataset_id,
                job.request.definition_revision_id,
                job.request.schema_revision_id,
                build.capture_bundle_id,
                build.generation_id,
            )
            or bytes(family.producing_token_sha256) != producer_token_sha256(job.request)
            or descriptor.layout_contract != FAMILY_BLOCK_LAYOUT_CONTRACT
            or descriptor.phase != "logged"
            or bytes(descriptor.layout_sha256) != job.layout.digest
            or bytes(descriptor.definition_sha256) != bytes.fromhex(job.request.definition.digest)
            or descriptor.collection_slots != dict(job.layout.collection_slots)
        ):
            raise FamilyBlockUnavailable("verified owner job block registration differs")
        job.family_id = family.family_id


async def _dispatch_existing(factory, job):
    existing = await _existing_build(factory, job.request)
    if existing is None:
        if job.build_id is not None:
            raise FamilyBlockUnavailable("owner job build disappeared")
        return
    if job.build_id is not None and job.build_id != existing:
        raise FamilyBlockUnavailable("owner job native build identity differs")
    if job.source is None and not job.initial_started:
        job.counts_unavailable = True
    job.build_id = existing
    # Terminal replay must precede running-lease checks and writable resolution.
    job.output = await build_output._replay(factory, job.request, job.build_id)
    if job.output is not None:
        return
    job.current()
    build = await _snapshot(factory, job.request, job.build_id)
    if build.phase == "verified":
        await _verified_block(factory, job, build)
        if job._root_identity is None and job.root.exists():
            await _reopen_verified_root(factory, job)
        else:
            job._ensure_root()
        await build_output._freeze_output_snapshot(factory, job.request, job.build_id)
        job.output = await seal_shard_build(factory, job.request, job.build_id, work=_work(job, "finality"))
    elif job.source is None and not job.initial_started:
        if build.phase in {"graph", "output", "verifying"}:
            if await _reopen_output(factory, job):
                return
        if build.phase not in {"source", "admission", "graph", "rejected"} or job.request.continuation is not None:
            raise FamilyBlockUnavailable(
                "native owner progress requires original live artifacts; cold reopening is unavailable"
            )
        await _reopen_source(factory, job)


async def _later_family(factory, job):
    """Resolve one existing protected family without any fresh candidate allocation."""
    async with _page_session(factory, job.request, job.build_id) as (session, _build):
        family_id = (
            await store._execute(
                session,
                select(CustomImportSnapshotFamily.family_id).where(
                    CustomImportSnapshotFamily.execution_id == job.request.execution_id,
                    CustomImportSnapshotFamily.producing_fence == job.request.fence,
                ),
            )
        ).scalar_one_or_none()
        if family_id is None:
            raise FamilyBlockUnavailable("later owner has no original candidate")
        await checkpoint._native_scope(session, job.request, job.build_id, family_id, job.layout, writing=False)
        return family_id


async def _adopt_checkpoint_root(job, snapshot):
    profile = (astuple(job.work.limits), job.projection_work_mem_mb, astuple(job.source_limits))
    ready, custody, outputs, common = await _cpu(
        job,
        source_checkpoint.source_owner_checkpoint,
        output_checkpoint.OriginalPrefix(snapshot),
        job.root,
        profile,
        job.current,
    )
    owner_lock = acquire_owner_job_lock(job.root, identity=custody.directories[0][1][:2])
    if owner_lock.file_identity != custody.lock_identity:
        owner_lock.close()
        raise FamilyBlockUnavailable("later owner lifetime lock changed")
    job._owner_lock, job._root_identity, job._source_custody = owner_lock, owner_lock.identity, custody
    job.work.register_cleanup(job.root)
    return ready, custody, outputs, common


async def _reopen_output(factory, job):
    root = job.root / "source" / "allocation-journal" / "checkpoints"
    if not root.is_dir():
        raise FamilyBlockUnavailable("cold reopening is unavailable without its protected OUTPUT journal")
    await _registry(factory, job)
    job.family_id = await _later_family(factory, job)
    snapshot = await checkpoint.read_owner_checkpoint(
        factory,
        job.request,
        job.build_id,
        job.family_id,
        job.layout,
        root=root,
        limits=job.source_limits.checkpoint,
    )
    cells = await _cpu(job, output_checkpoint.latest, snapshot, job.current)
    if cells is None:
        current = await _snapshot(factory, job.request, job.build_id)
        if current.phase == "graph" and current.plan_stage == "base" and current.plan_complete_at is None:
            return None
        raise FamilyBlockUnavailable("later owner lacks its original protected OUTPUT inputs")
    ready, custody, outputs, common = await _adopt_checkpoint_root(job, snapshot)
    if cells[1] != ready.input_sha256:
        raise FamilyBlockUnavailable("later OUTPUT original SOURCE-ready hash differs")
    job.pending = "output-reopen"
    context = SourceStageContext(
        job.request, job.registry, job.layout, job.family_id, job.build_id, custody.profile[:2]
    )
    job.source = SourceAdmissionStage(
        context,
        lambda: job.base_keys(),
        root=job.root / "source",
        register_cleanup=job.work.register_cleanup,
        limits=job.source_limits,
        current=job.current,
        owner_lock=job._owner_lock,
    )
    bind_source_custody(job.source, custody)
    async with _renew_while_reading(factory, job.request):
        await _open_base(factory, job)
        await reopen_completed_source(factory, job.source, snapshot, ready, custody, outputs, common, cells[4])
        await retained_checkpoint.reopen_completed(factory, job, cells[2], output_checkpoint.OriginalPrefix(snapshot))
        await output_checkpoint.reopen_stage(factory, job, cells)
    job.pending, job.initial_started, job.counts_unavailable = None, True, False
    return job


async def _reopen_verified_root(factory, job):
    """Authenticate terminal cleanup custody without reopening any frozen producer."""
    from db.models.custom_import import CustomImportCaptureBundle
    from process.custom_import.build_source import _prepare_statement
    from process.custom_import.family_block_binding import resolve_family_block_binding

    root = job.root / "source" / "allocation-journal" / "checkpoints"
    async with _page_session(factory, job.request, job.build_id) as (session, build):
        if build.phase != "verified":
            raise FamilyBlockUnavailable("verified owner cleanup build changed")
        await store._owner(session)
        await store._fresh_commit_authority(session, job.request)
        descriptor = await session.get(CustomImportFamilyBlockLayout, job.family_id, populate_existing=True)
        if descriptor is None or descriptor.phase != "logged":
            raise FamilyBlockUnavailable("verified owner requires its exact logged descriptor")
        binding = await resolve_family_block_binding(
            session,
            job.family_id,
            job.request.definition,
            job.layout.collection_slots,
            persistence="logged",
            prepare=_prepare_statement,
        )
        capture = await session.get(CustomImportCaptureBundle, build.capture_bundle_id, populate_existing=True)
        scope = checkpoint._scope_cells(build, capture, job.request, job.layout, binding, job.family_id)
        head = checkpoint._head(descriptor)
        owner_sha, _bytes = await _cpu(
            job, lambda: checkpoint._owner_file(root, scope, job.source_limits.checkpoint, create=False)
        )
        snapshot = checkpoint.CommittedOwnerCheckpoint(str(root), scope, head, owner_sha, job.source_limits.checkpoint)
    cells = await _cpu(job, output_checkpoint.latest, snapshot, job.current)
    if cells is None or cells[8] != 5:
        raise FamilyBlockUnavailable("verified owner lacks its original close-ready custody")
    await _adopt_checkpoint_root(job, snapshot)
    if Path(cells[3][0]) != job.root / "output" or source_checkpoint._directory(job.root / "output") != cells[3][1]:
        raise FamilyBlockUnavailable("verified owner OUTPUT root custody differs")
    counts = SourceOutcomeCounts(*cells[4][2])
    if (descriptor.source_accepted_family_count, descriptor.source_rejection_count) != astuple(counts):
        raise FamilyBlockUnavailable("verified owner original SOURCE counts differ")
    job.durable_counts, job.counts_unavailable = counts, False
    job._terminal_custody_only = True


async def _source_family(factory, job):
    """Discover the existing candidate under current authority without allocating one."""
    async with _page_session(factory, job.request, job.build_id) as (session, build):
        if build is None or build.phase not in {"source", "admission", "graph", "rejected"}:
            raise FamilyBlockUnavailable("cold owner reopening requires original SOURCE/admission custody")
        if build.phase in {"graph", "rejected"} and (
            build.generation_id is not None
            or build.plan_complete_at is not None
            or build.plan_stage != "base"
            or build.graph_frozen_at is not None
        ):
            raise FamilyBlockUnavailable("cold reopening cannot adopt retained GRAPH or generation progress")
        family_id = (
            await store._execute(
                session,
                select(CustomImportSnapshotFamily.family_id).where(
                    CustomImportSnapshotFamily.execution_id == job.request.execution_id,
                    CustomImportSnapshotFamily.producing_fence == job.request.fence,
                ),
            )
        ).scalar_one_or_none()
        if family_id is None:
            raise FamilyBlockUnavailable("cold SOURCE has no original candidate")
        await store._candidate(session, job.request, family_id, job.layout, {"loading"})
        return family_id


async def _reopen_source(factory, job):
    """Adopt only a protected post-ready SOURCE tree after its old workers drain."""
    checkpoint_root = job.root / "source" / "allocation-journal" / "checkpoints"
    if not checkpoint_root.is_dir():
        raise FamilyBlockUnavailable("cold reopening is unavailable without its protected SOURCE-ready journal")
    await _registry(factory, job)
    job.family_id = await _source_family(factory, job)
    snapshot = await checkpoint.read_owner_checkpoint(
        factory,
        job.request,
        job.build_id,
        job.family_id,
        job.layout,
        root=checkpoint_root,
        limits=job.source_limits.checkpoint,
    )
    retained = await _cpu(job, retained_checkpoint.latest, snapshot, job.current)
    profile = (astuple(job.work.limits), job.projection_work_mem_mb, astuple(job.source_limits))
    ready_record, custody, outputs, common = await _cpu(
        job, source_checkpoint.source_owner_checkpoint, snapshot, job.root, profile, job.current
    )
    owner_lock = acquire_owner_job_lock(job.root, identity=custody.directories[0][1][:2])
    if owner_lock.file_identity != custody.lock_identity:
        owner_lock.close()
        raise FamilyBlockUnavailable("cold SOURCE owner lock changed during acquisition")
    job._owner_lock, job._root_identity, job._source_custody = owner_lock, owner_lock.identity, custody
    job.work.register_cleanup(job.root)
    job.pending = "source-reopen"
    context = SourceStageContext(job.request, job.registry, job.layout, job.family_id, job.build_id, profile[:2])
    job.source = SourceAdmissionStage(
        context,
        lambda: job.base_keys(),
        root=job.root / "source",
        register_cleanup=job.work.register_cleanup,
        limits=job.source_limits,
        current=job.current,
        owner_lock=owner_lock,
    )
    bind_source_custody(job.source, custody)
    async with _renew_while_reading(factory, job.request):
        await _open_base(factory, job)
        await reopen_source_admission(factory, job.source, snapshot, ready_record, custody, outputs, common)
        if retained is not None:
            await _cpu(job, retained_checkpoint.check_roots, snapshot, job.current)
            await retained_checkpoint.reopen(factory, job, retained)
    job.pending, job.initial_started, job.counts_unavailable = None, True, False


async def _initial(factory, job):
    await _registry(factory, job)
    if job.family_id is None:
        job.family_id = await store.create_candidate(factory, job.request, job.layout)
    if job.source is None:
        job.initial_started = True
        identity, registry = await begin_shard_build(factory, job.request, job.family_id, job.layout)
        if (job.build_id is not None and identity != job.build_id) or registry != job.registry:
            raise FamilyBlockUnavailable("owner job initial build/registry differs")
        job.build_id = identity


async def _open_base(factory, job):
    """Repin the original base without restaging keys already sealed in metadata."""

    async def _enter_base():
        """Retain the actual context even when its genuine absent base is NULL."""
        manager = pinned_base(factory, job.request, buffer_bytes=job.work.limits.memory_bytes)
        job.base = await job._stack.enter_async_context(manager)
        return manager

    await _retain(job, "base_context", _enter_base)


async def _open_originals(factory, job):
    await _open_base(factory, job)
    if job._source_custody is not None:
        return
    await _retain(
        job,
        "base_keys",
        lambda: job._keys_stack.enter_async_context(
            pinned_base_query_keys(
                job.base, work=_work(job, "base-keys" if job.source is None else "base-keys-" + uuid4().hex)
            )
        ),
    )
    if job.source is None:
        context = SourceStageContext(
            job.request,
            job.registry,
            job.layout,
            job.family_id,
            job.build_id,
            (astuple(job.work.limits), job.projection_work_mem_mb),
        )
        job.source = SourceAdmissionStage(
            context,
            job.base_keys,
            root=job.root / "source",
            register_cleanup=job.work.register_cleanup,
            limits=job.source_limits,
            current=job.current,
            owner_lock=job._owner_lock,
        )


async def _families(factory, job):
    if job.final_plan is None:
        if job.base is not None:
            await job.base.check()
        await _retain(
            job,
            "final_plan",
            lambda: build_family_plan(
                job.request,
                job.source.admission,
                None if job.base is None else job.base.headers(),
                job.source.metadata.family_ids(),
                work=_work(job, "final-plan"),
                base_scope=None if job.base is None else job.base.scope,
            ),
        )
    if job.retained is None:
        job.retained = RetainedFamilyStage(job.source, job.final_plan, job.base, work=_work(job, "retained"))
    if not job.retained.complete:
        await advance_retained_families(factory, job.retained)
    if job.output_stage is None:
        job.output_stage = ShardOutputStage(job.retained, work=_work(job, "output"))
    if job.output_stage.inputs is None:
        await advance_shard_output(
            factory,
            job.output_stage,
            projection_factory=import_session_factory(factory, work_mem_mb=job.projection_work_mem_mb),
            index_factory=factory,
        )


async def _compute(factory, job, attempt_root):
    job.current()
    if Path(attempt_root).absolute().parent != job.root / "source":
        raise ValueError("owner SOURCE attempt must be a direct child of its SOURCE root")
    job._ensure_root()
    if job.pending is not None:
        raise FamilyBlockUnavailable("unfinished owner artifact requires original custody; regeneration is refused")
    async with _renew_while_reading(factory, job.request):
        await _initial(factory, job)
        await _open_originals(factory, job)
        if job.source.phase != "graph" or job.source.homes is None:
            await advance_source_admission(factory, job.source, attempt_root=attempt_root)
        if not job.is_rejected:
            await _families(factory, job)


async def _finish(factory, job):
    if job.counts_unavailable:
        counts = await terminal_source_outcomes(factory, job.request, job.build_id)
        if type(counts) is not SourceOutcomeCounts:
            raise FamilyBlockUnavailable("terminal SOURCE outcome counts are unavailable")
        job.durable_counts = counts
        job.counts_unavailable = False
    # A terminal receipt alone cannot reconstruct API response outcome counts.
    job.source_counts
    if job.is_rejected:
        job.complete = True
        return
    if job.output is None:
        job.finality_started = True
        proof_work = (
            output_checkpoint.proof_work(job.output_stage)
            if type(job.output_stage) is ShardOutputStage and output_checkpoint.is_enabled(job.output_stage)
            else nullcontext(PlanWork(job.root, job.work.limits, job.work.register_cleanup, job.current))
        )
        with proof_work as work:
            output = await complete_closed_shard_build(
                factory,
                job.retained.context,
                job.output_stage.inputs,
                work=work,
                limits=job.source_limits.allocation,
                reader_activation=job.reader_activation,
            )
        job.output = output
    if job.output.no_change is not None:
        job.publication = job.output.no_change
    elif job.reader_activation is None:
        job.publication = await _activate(factory, job.request, job.output)
    else:
        job.publication = await _activate(
            factory, job.request, job.output, reader_activation=job.reader_activation, reader_layout=job.layout
        )
    job.complete = True


async def advance_owner_shard_job(factory, job, *, attempt_root):
    """Advance the one original owner tree; errors preserve every live producer."""
    if type(job) is not OwnerShardJob or job._busy or job._closed or not callable(factory):
        raise ValueError("owner shard job is unavailable or already advancing")
    if job.complete:
        return job
    job._busy = True
    job.stop.clear()
    try:
        if job.output is None:
            await _dispatch_existing(factory, job)
        if job.output is None and not job.finality_started:
            await _compute(factory, job, attempt_root)
        await _finish(factory, job)
        return job
    finally:
        job._busy = False
