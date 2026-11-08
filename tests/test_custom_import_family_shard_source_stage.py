# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Orchestration/custody tests with explicit producer/native transport doubles.

These tests do not claim native storage, worker, or admission acceptance. Existing
leaf suites qualify those boundaries; these doubles inject coordinator failures.
"""

import asyncio
import threading
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import family_shard_owner_lock as locks
from process.custom_import import family_shard_source_stage as stage
from process.custom_import.admission_sql import AdmissionError
from process.custom_import.build_counts import SourceOutcomeCounts
from process.custom_import.family import CandidateRejected
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.runner_types import CancellationRequested, CandidateRegistry, LeaseAuthorityLost
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_family_shard_capture import _limits as _preparation_limits
from tests.test_custom_import_family_shard_worker import _limits as _worker_limits


class _Artifact:
    def __init__(self, root, label, closed):
        self.root, self.label, self.closed_events = Path(root), label, closed
        self.root.mkdir()
        self.closed = False

    def close(self):
        if not self.closed:
            self.root.rmdir()
            self.closed_events.append(self.label)
            self.closed = True

    def family_ids(self):
        return iter(())

    def query_keys(self):
        return iter(())

    def batches(self, **_limits):
        if self.closed:
            raise ValueError("closed synthetic producer")
        yield {}


class _Run:
    def __init__(self, pipeline, root, checkpoints):
        self.pipeline, self.root = pipeline, Path(root)
        self.root.mkdir()
        self.borrowed = {
            output.receipt["shard_id"]: output for checkpoint in checkpoints for output in checkpoint.outputs
        }
        self.outputs_by_shard, self.owned = dict(self.borrowed), []
        self.joined, self.cleaned = False, False
        self._running, self._processes = True, {}

    @property
    def completed(self):
        return tuple(self.outputs_by_shard[key] for key in sorted(self.outputs_by_shard))

    def __aiter__(self):
        return self._outputs()

    async def _outputs(self):
        for output in self.completed:
            yield output
        for shard in range(4):
            if shard in self.borrowed:
                continue
            output = _Artifact(self.root / str(shard), f"worker-{self.root.name}-{shard}", self.pipeline.closed)
            output.receipt = stage.store.ShardOutputProof(
                self.pipeline.owner.context.layout, shard, b"p" * 32, 0
            ).receipt()
            self.outputs_by_shard[shard] = output
            self.owned.append(output)
            yield output

    async def checkpoint(self):
        assert self.joined and not self.cleaned
        if self.pipeline.fail_checkpoint:
            raise OSError("synthetic checkpoint failure")
        return SimpleNamespace(root=str(self.root), outputs=self.completed)

    async def stop(self, *, terminate):
        if self.pipeline.fail_stop:
            raise RuntimeError("synthetic join failure")
        self.joined = True
        self._running = False

    async def cleanup(self):
        assert self.joined
        if not self.cleaned:
            for output in self.owned:
                output.close()
            self.root.rmdir()
            self.pipeline.closed.append(self.root.name)
            self.cleaned = True


class _Pipeline:
    """Finite fake leaf/native boundary; only coordinator behavior is asserted."""

    def __init__(self, owner):
        self.owner, self.phase = owner, "source"
        self.calls, self.closed, self.copied = [], [], []
        self.committed, self.base_reads = {}, 0
        self.fail_copy, self.fail_allocation = False, False
        self.fatal, self.reject, self.commit_error = False, False, False
        self.fail_stop = False
        self.fail_checkpoint = False
        self.counts = SourceOutcomeCounts(0, 0)

    def install(self, monkeypatch):
        patches = (
            (stage, "_native_phase", self.native_phase),
            (stage.capture_api, "prepare_captured_source", self.capture),
            (stage.allocation_api, "allocate_source_plan", self.allocate),
            (stage.allocation_api, "cleanup_source_allocations", lambda artifact: artifact.close()),
            (stage, "cleanup_source_plan", lambda artifact: artifact.close()),
            (stage, "cleanup_spool", lambda artifact: artifact.close()),
            (stage, "owner_source_metadata", self.metadata),
            (stage, "route_capture", self.route),
            (stage.tasks_api, "prepare_source_tasks", self.tasks),
            (stage.tasks_api, "run_source_tasks", self.run),
            (
                stage.common_api,
                "SourceCommonContext",
                lambda *args: SimpleNamespace(request=args[0], layout=args[1], source=args[2]),
            ),
            (stage.admission_api, "build_admission_output", self.admit),
            (stage.admission_api, "cleanup_admission_output", lambda artifact: artifact.close()),
            (stage, "count_admitted_source_outcomes", self.count),
            (stage, "bind_source_outcomes", self.bind_counts),
            (stage.admission_api, "read_admission_families", lambda *_args, **_kwargs: (header for header in ())),
            (stage.common_api, "build_source_common", self.common),
            (stage.store, "copy_shard", self.copy),
            (stage, "complete_shard_source", self.source_complete),
            (stage.events_api, "allocate_admission_events", self.events),
            (stage.events_api, "admission_event_batches", lambda artifact, **limits: artifact.batches(**limits)),
            (stage.events_api, "cleanup_admission_events", lambda artifact: artifact.close()),
            (stage, "complete_shard_admission", self.admission_complete),
            (stage, "register_source_homes", self.homes),
        )
        for module, name, implementation in patches:
            monkeypatch.setattr(module, name, implementation)

    def base_keys(self):
        self.base_reads += 1
        return iter(())

    async def native_phase(self, *_args):
        self.calls.append("native")
        return self.phase

    async def capture(self, *_args, root, **_kwargs):
        self.calls.append("capture")
        return SimpleNamespace(
            plan=_Artifact(root / "plan", "plan", self.closed),
            partition=_Artifact(root / "input", "input", self.closed),
        )

    async def allocate(self, *_args, journal, **_kwargs):
        self.calls.append("allocate")
        assert journal is self.owner.allocation_journal
        root = self.owner.root / "allocation"
        if self.fail_allocation:
            raise ConnectionError("uncertain allocation")
        artifact = _Artifact(root, "allocation", self.closed)
        artifact.source = SimpleNamespace(partition_sha256=(b"p" * 32).hex())
        return artifact

    @asynccontextmanager
    async def metadata(self, *_args, journal, retained_query_keys, **_kwargs):
        self.calls.append("metadata")
        assert journal is self.owner.allocation_journal
        assert list(retained_query_keys) == []
        root = self.owner.root / "metadata"
        artifact = _Artifact(root, "metadata", self.closed)
        try:
            yield artifact
        finally:
            artifact.close()

    @contextmanager
    def route(self, *_args, root, **_kwargs):
        self.calls.append("route")
        artifact = _Artifact(root, "route", self.closed)
        try:
            yield tuple(SimpleNamespace(shard_id=index) for index in range(4))
        finally:
            artifact.close()

    def tasks(self, shards, _allocated, _scope, families, queries, *, root, **_kwargs):
        self.calls.append("tasks")
        assert list(families) == list(queries) == []
        artifact = _Artifact(root, "tasks", self.closed)
        artifact.files = shards
        return artifact

    @asynccontextmanager
    async def run(self, _tasks, *, root, completed, preserve_completed, **_kwargs):
        assert preserve_completed
        assert _kwargs["owner_lock"] is self.owner.owner_lock
        self.calls.append("run")
        run = _Run(self, root, completed)
        try:
            yield run
        finally:
            run.joined = True
            run._running = False

    def admit(self, *_args, work, **_kwargs):
        self.calls.append("admit")
        return _Artifact(work.root, "admission", self.closed)

    def count(self, *_args, **_kwargs):
        self.calls.append("counts")
        return self.counts

    async def common(self, _factory, _context, entities, queries, *, root, **_kwargs):
        self.calls.append("common")
        assert list(entities) == list(queries) == []
        return _Artifact(root, "common", self.closed)

    async def copy(self, _factory, _request, _family, _layout, batches, receipt, **_kwargs):
        shard = receipt["shard_id"]
        self.copied.append(shard)
        if shard in self.committed:
            assert self.committed[shard] == receipt
            return receipt
        assert list(batches) == [{}]
        self.committed[shard] = receipt
        if self.fail_copy and shard == 0:
            self.fail_copy = False
            raise ConnectionError("COPY commit response lost")
        return receipt

    async def source_complete(self, *_args, **_kwargs):
        self.calls.append("source_complete")
        assert self.owner.counts is self.counts and 4 in self.committed
        self.phase = "admission"
        return self.phase

    async def bind_counts(self, *_args, **_kwargs):
        self.calls.append("bind_counts")
        assert self.owner.source_completed and _args[6] is self.counts
        return self.counts

    async def events(self, *_args, journal, **_kwargs):
        self.calls.append("events")
        assert journal is self.owner.allocation_journal
        return _Artifact(self.owner.root / "events", "events", self.closed)

    async def admission_complete(self, *_args, **_kwargs):
        self.calls.append("admission_complete")
        assert self.owner.counts is self.counts and 5 in self.committed
        if self.fatal:
            raise AdmissionError("synthetic fatal admission")
        self.phase = "rejected" if self.reject else "graph"
        if self.commit_error:
            self.commit_error = False
            raise ConnectionError("admission commit response lost")
        return self.phase

    async def homes(self, *_args, **_kwargs):
        self.calls.append("homes")
        assert self.phase == "graph"
        return SimpleNamespace(root_count=0, child_count=0)


def _owner(tmp_path, *, owner_lock=None):
    request = replace(_request(), complete_scope=True)
    registry = CandidateRegistry({"details": 7}, {"root_source": 1, "detail_source": 2}, 1)
    context = stage.SourceStageContext(
        request, registry, FamilyBlockLayout(request.definition, registry.child_collection_slots), 17, 11
    )
    worker = _worker_limits()
    limits = stage.SourceStageLimits(
        _preparation_limits(),
        stage.allocation_api.SourceAllocationLimits(worker.facts, 256, 1024**2),
        worker.facts,
        worker,
        2,
    )
    return stage.SourceAdmissionStage(
        context,
        lambda: iter(()),
        root=tmp_path / "source",
        register_cleanup=lambda _path: None,
        limits=limits,
        current=lambda: None,
        owner_lock=owner_lock,
    )


async def test_continuation_never_dispatches_source_or_admission(tmp_path):
    from process.custom_import.continuation_runtime import BuildContinuationContext

    owner = _owner(tmp_path)
    owner.context = replace(
        owner.context,
        request=replace(owner.context.request, continuation=BuildContinuationContext(9, 11, b"p" * 32)),
    )
    with pytest.raises(FamilyBlockUnavailable, match="allocate or readmit SOURCE"):
        await stage.advance_source_admission(None, owner, attempt_root=owner.root / "attempt")
    assert not owner._busy and not owner.root.exists()


@pytest.fixture
async def pipeline(tmp_path, monkeypatch):
    owner = _owner(tmp_path)
    boundary = _Pipeline(owner)
    owner.base_keys = boundary.base_keys
    boundary.install(monkeypatch)
    try:
        yield owner, boundary
    finally:
        await owner.close_terminal()


async def test_empty_cohort_receipts_and_cleanup(pipeline):
    owner, boundary = pipeline
    assert not owner.root.exists()
    await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
    assert owner.phase == "graph" and len(owner.workers) == 4
    assert boundary.copied == list(range(6))
    assert owner.common_receipt["source_record_count"] == owner.event_receipt["source_record_count"] == 0
    assert owner.common_receipt["partition_sha256"] == b"p" * 32
    assert boundary.base_reads == 1 and not boundary.closed
    assert owner.runs[0].joined and owner.joined
    await owner.close_terminal()
    assert not owner.root.exists()
    assert boundary.closed.index("events") < boundary.closed.index("first") < boundary.closed.index("tasks")
    assert boundary.closed[-5:] == ["route", "metadata", "allocation", "plan", "input"]


async def test_source_forwards_borrowed_owner_lock_and_keeps_it_after_cleanup(tmp_path, monkeypatch):
    root = tmp_path / "job"
    root.mkdir(mode=0o700)
    observed = root.stat()
    with locks.acquire_owner_job_lock(root, identity=(observed.st_dev, observed.st_ino)) as holder:
        owner = _owner(root, owner_lock=holder)
        boundary = _Pipeline(owner)
        boundary.install(monkeypatch)
        try:
            await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
        finally:
            await owner.close_terminal()
        holder.check()
        holder.remove_terminal_root()


async def test_uncertain_copy_reuses_joined_originals(pipeline):
    owner, boundary = pipeline
    boundary.fail_copy = True
    with pytest.raises(ConnectionError, match="COPY commit"):
        await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
    original = owner.allocated
    old_run, checkpoint = owner.runs[0], owner.joined[0]
    assert old_run.joined and owner.root.exists() and not boundary.closed
    await stage.advance_source_admission(None, owner, attempt_root=owner.root / "second")
    assert owner.allocated is original and owner.joined[0] is checkpoint
    assert boundary.copied == [0, 0, 1, 2, 3, 4, 5]
    assert all(boundary.calls.count(name) == 1 for name in ("capture", "allocate", "metadata", "route", "tasks"))
    assert boundary.base_reads == 1
    await owner.runs[-1].cleanup()
    assert old_run.root.exists() and not old_run.cleaned


class _ClosedRefusal:
    def __init__(self, owner, boundary, case):
        self.owner, self.boundary, self.case = owner, boundary, case
        self.events, self.incomplete = [], []
        self.original_current = owner._current
        self.failure = {
            "lease_primary": LeaseAuthorityLost("synthetic lost lease"),
            "cancel_primary": CancellationRequested("synthetic canceled owner"),
        }.get(case, CandidateRejected("source worker audit contains candidate or structural errors"))

    async def outputs(self, run):
        for shard in range(2):
            output = _Artifact(run.root / str(shard), f"closed-{shard}", self.boundary.closed)
            output.receipt = {"shard_id": shard}
            output.status = "refused" if shard == 0 else "source_eof"
            run.outputs_by_shard[shard] = output
            run.owned.append(output)
        if self.case == "partial":
            partial = run.root / "incomplete"
            partial.mkdir()
            path = partial / "partial-audit"
            path.write_bytes(b"unfinished writer is not closed custody")
            self.incomplete.append(path)
        yield run.outputs_by_shard[0]

    async def copy(self, *_args, **_kwargs):
        self.events.append(("copy", 0))
        raise self.failure

    async def checkpoint(self, run):
        assert run.joined
        self.events.append(("checkpoint",))
        raise ValueError("refused source output cannot be reused")

    async def anchor(self, _factory, current, output):
        shard = output.receipt["shard_id"]
        joined = current.runs[-1].joined
        self.events.append(("anchor", shard, joined))
        if shard == 1:
            assert joined
            if self.case == "anchor_failure":
                raise OSError("closed custody commit is unknown")
        current.output_custody[shard] = output

    def current(self):
        self.original_current()
        if self.case == "deadline" and self.owner.runs and self.owner.runs[-1].joined:
            raise TimeoutError("original deadline exceeded")


@pytest.mark.parametrize("case", ["closed", "anchor_failure", "deadline", "lease_primary", "cancel_primary", "partial"])
async def test_refused_closed_custody(pipeline, monkeypatch, case):
    owner, boundary = pipeline
    fault = _ClosedRefusal(owner, boundary, case)
    with monkeypatch.context() as scoped:
        scoped.setattr(_Run, "_outputs", lambda run: fault.outputs(run))
        scoped.setattr(_Run, "checkpoint", lambda run: fault.checkpoint(run))
        scoped.setattr(stage.store, "copy_shard", fault.copy)
        scoped.setattr(stage, "_anchor_output", fault.anchor)
        scoped.setattr(owner, "_current", fault.current)
        with pytest.raises(type(fault.failure)) as caught:
            await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
        assert caught.value is fault.failure and owner.runs[-1].joined
        assert fault.events[:3] == [("anchor", 0, False), ("copy", 0), ("checkpoint",)]
        assert fault.events[3:] == (
            [] if case in {"deadline", "lease_primary", "cancel_primary"} else [("anchor", 1, True)]
        )
        assert set(owner.output_custody) == ({0, 1} if case in {"closed", "partial"} else {0})
        assert all(output.root.exists() for output in owner.runs[-1].completed)
        assert not owner.source_completed and owner.phase is None and boundary.phase == "source"
        assert owner.pending == "joined"
        assert "admit" not in boundary.calls and "source_complete" not in boundary.calls
        if case == "anchor_failure":
            assert isinstance(fault.failure.__cause__, OSError) and "unknown" in str(fault.failure.__cause__)
        elif case == "deadline":
            assert isinstance(fault.failure.__cause__, TimeoutError)
        for path in fault.incomplete:
            assert path.read_bytes() == b"unfinished writer is not closed custody"
            path.unlink()
            path.parent.rmdir()


@pytest.mark.parametrize("condition", ["running", "alive", "unjoined"])
async def test_custody_requires_join(monkeypatch, condition):
    anchor = AsyncMock()
    monkeypatch.setattr(stage, "_anchor_output", anchor)
    process = SimpleNamespace(is_alive=lambda: condition == "alive", exitcode=None if condition == "unjoined" else 0)
    run = SimpleNamespace(_running=condition == "running", _processes={1: process}, completed=())
    with pytest.raises(ValueError, match="joined before closed output custody"):
        await stage._anchor_closed_outputs(None, object(), run)
    anchor.assert_not_awaited()


async def test_join_failure_preserves_terminal_evidence(pipeline):
    owner, boundary = pipeline
    await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
    boundary.fail_stop = True
    try:
        with pytest.raises(RuntimeError, match="join failure"):
            await owner.close_terminal()
        assert owner.root.exists() and owner._closed is False and boundary.closed == []
    finally:
        boundary.fail_stop = False


@pytest.mark.parametrize("fatal", [False, True])
async def test_failure_counts_precede_admission(pipeline, fatal):
    owner, boundary = pipeline
    boundary.reject, boundary.fatal = not fatal, fatal
    if fatal:
        with pytest.raises(AdmissionError, match="synthetic fatal"):
            await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
    else:
        await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
        assert owner.phase == "rejected"
    assert owner.counts is boundary.counts and "homes" not in boundary.calls
    assert owner.source_completed
    assert (
        boundary.calls.index("source_complete")
        < boundary.calls.index("bind_counts")
        < boundary.calls.index("admission_complete")
    )
    assert owner.admission.root.exists() and owner.common_output.root.exists() and not boundary.closed


async def test_graph_retry_skips_earlier_mutations(pipeline):
    owner, boundary = pipeline
    boundary.commit_error = True
    with pytest.raises(ConnectionError, match="admission commit"):
        await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
    copied_shards = tuple(boundary.copied)
    await stage.advance_source_admission(None, owner, attempt_root=owner.root / "unused")
    assert owner.phase == "graph" and tuple(boundary.copied) == copied_shards
    assert boundary.calls.count("source_complete") == 1 and boundary.calls.count("admission_complete") == 2
    assert boundary.calls.count("allocate") == 1


async def test_lost_source_completion_keeps_count_confirmation_false_until_replay(pipeline, monkeypatch):
    owner, boundary = pipeline
    complete = boundary.source_complete

    async def lost(*args, **kwargs):
        await complete(*args, **kwargs)
        raise ConnectionError("SOURCE completion response lost")

    monkeypatch.setattr(stage, "complete_shard_source", lost)
    with pytest.raises(ConnectionError, match="completion response lost"):
        await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
    assert owner.counts is boundary.counts and not owner.source_completed
    original = owner.allocated
    await stage.advance_source_admission(None, owner, attempt_root=owner.root / "unused")
    assert owner.source_completed and owner.allocated is original and boundary.calls.count("allocate") == 1


async def test_common_copy_failure_does_not_confirm_early_counts(pipeline, monkeypatch):
    owner, boundary = pipeline
    copied = boundary.copy

    async def failed(*args, **kwargs):
        result = await copied(*args, **kwargs)
        if args[5]["shard_id"] == 4:
            raise ConnectionError("common COPY response lost")
        return result

    monkeypatch.setattr(stage.store, "copy_shard", failed)
    with pytest.raises(ConnectionError, match="common COPY"):
        await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
    assert owner.counts is boundary.counts and not owner.source_completed
    assert "source_complete" not in boundary.calls


async def test_uncertain_count_binding_retries_before_admission_without_rebuilding(pipeline, monkeypatch):
    owner, boundary = pipeline
    bind = boundary.bind_counts

    async def lost(*args, **kwargs):
        await bind(*args, **kwargs)
        raise ConnectionError("SOURCE count binding response lost")

    monkeypatch.setattr(stage, "bind_source_outcomes", lost)
    with pytest.raises(ConnectionError, match="binding response lost"):
        await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
    original = owner.allocated
    assert owner.source_completed and owner.events is None
    monkeypatch.setattr(stage, "bind_source_outcomes", bind)
    await stage.advance_source_admission(None, owner, attempt_root=owner.root / "unused")
    assert owner.allocated is original and boundary.calls.count("allocate") == 1
    assert boundary.calls.count("bind_counts") == 2 and boundary.calls.count("admission_complete") == 1


@pytest.mark.parametrize("frozen,count", [(None, 2), (object(), 1)])
async def test_native_replay_refuses_unfrozen_or_changed_source_count(tmp_path, monkeypatch, frozen, count):
    owner = _owner(tmp_path)
    owner.prepared = object()
    owner.allocated = SimpleNamespace(source=SimpleNamespace(counts=(2,)))
    build = SimpleNamespace(phase="admission", source_frozen_at=frozen, source_occurrence_count=count)

    async def snapshot(*_args):
        return build

    @asynccontextmanager
    async def authority(*_args):
        yield object(), build

    async def candidate(*_args):
        return object(), object()

    monkeypatch.setattr(stage, "_snapshot", snapshot)
    monkeypatch.setattr(stage, "_page_session", authority)
    monkeypatch.setattr(stage.store, "_candidate", candidate)
    with pytest.raises(FamilyBlockUnavailable, match="frozen occurrence count"):
        await stage._native_phase(None, owner)
    assert not owner.source_completed and not owner.root.exists()


async def test_uncertain_allocation_refuses_regeneration(pipeline):
    owner, boundary = pipeline
    boundary.fail_allocation = True
    with pytest.raises(ConnectionError, match="uncertain allocation"):
        await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
    assert owner.pending == "allocated" and owner.prepared.plan.root.exists()
    with pytest.raises(ConnectionError, match="uncertain allocation"):
        await stage.advance_source_admission(None, owner, attempt_root=owner.root / "second")
    assert boundary.calls.count("capture") == 1 and boundary.calls.count("allocate") == 2
    assert owner.allocation_journal.root.exists() and owner.pending == "allocated"


async def test_live_stage_reconciles_actual_source_journal_before_dispatch(tmp_path, monkeypatch):
    from process.custom_import.family_shard_capture import PreparedCaptureSource
    from tests.test_custom_import_family_shard_allocate import _native_boundary
    from tests.test_custom_import_family_shard_source_plan import _build, _case, _child, _root

    case = await _case(tmp_path, {"root_source": [[_root()] * 4], "detail_source": [[_child()] * 3]})
    plan = _build(tmp_path, case)
    boundary = _native_boundary(monkeypatch, plan)
    boundary.failure = "unknown"
    owner = _owner(tmp_path)
    owner.context = stage.SourceStageContext(
        case[1],
        case[2],
        FamilyBlockLayout(case[1].definition, case[2].child_collection_slots),
        17,
        23,
    )
    owner.limits = replace(
        owner.limits,
        allocation=stage.allocation_api.SourceAllocationLimits(
            replace(owner.limits.allocation.spool, memory_bytes=64 * 1024),
            2,
            1024**2,
        ),
    )
    owner.prepared = PreparedCaptureSource(case[4], case[0], plan)

    async def native(*_args):
        return "source"

    async def after_allocation(*_args):
        raise RuntimeError("bounded stop before metadata")

    monkeypatch.setattr(stage, "_native_phase", native)
    monkeypatch.setattr(stage, "_prepare_tasks", after_allocation)
    try:
        with pytest.raises(FamilyBlockUnavailable, match="commit is unknown"):
            await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
        assert owner.pending == "allocated" and owner.prepared.plan is plan
        assert owner.allocation_journal.root.exists() and len(boundary.allocations) == 1
        with pytest.raises(RuntimeError, match="before metadata"):
            await stage.advance_source_admission(None, owner, attempt_root=owner.root / "second")
        assert owner.pending is None and owner.allocated is not None
        assert [source_row[1] for source_row in stage.allocation_api.read_source_allocations(owner.allocated)] == [
            57,
            74,
            91,
            108,
        ]
        assert len(boundary.allocations) == 1 and boundary.committed["owner_checkpoint_revision"] == 4
        assert Path(plan.root).exists()
    finally:
        await owner.close_terminal()
        stage.cleanup_source_plan(plan)
        stage.cleanup_spool(case[0])
    assert not owner.root.exists()


def _metadata_retry_owner(tmp_path, case, original_source, journal, limits):
    from process.custom_import.family_shard_capture import PreparedCaptureSource

    owner = _owner(tmp_path)
    owner.context = stage.SourceStageContext(
        case[1],
        case[2],
        FamilyBlockLayout(case[1].definition, case[2].child_collection_slots),
        original_source.snapshot_family_id,
        journal.build_id,
    )
    owner.limits = replace(owner.limits, allocation=limits)
    owner.prepared = PreparedCaptureSource(case[4], case[0], original_source.source)
    owner.allocated, owner.allocation_journal = original_source, journal
    return owner


def _install_metadata_retry_faults(monkeypatch, native):
    from process.custom_import import family_shard_metadata_journal as metadata_journal

    family_ids, copy_query = metadata_journal._Run.family_ids, metadata_journal._copy_query
    lost_family, lost_query = [False], [False]

    async def lose_family_result(run, batch):
        result = await family_ids(run, batch)
        if not lost_family[0]:
            lost_family[0] = True
            raise ConnectionError("synthetic first committed family result lost")
        return result

    def lose_query_reply(*args):
        record = copy_query(*args)
        if not lost_query[0]:
            lost_query[0] = True
            native.failure = "unknown"
        return record

    async def snapshot(*_args):
        return SimpleNamespace(phase="source")

    async def candidate(*_args):
        return object(), object()

    async def before_workers(*_args):
        raise RuntimeError("bounded stop before worker dispatch")

    monkeypatch.setattr(metadata_journal._Run, "family_ids", lose_family_result)
    monkeypatch.setattr(metadata_journal, "_copy_query", lose_query_reply)
    monkeypatch.setattr(stage, "_snapshot", snapshot)
    monkeypatch.setattr(stage, "_page_session", stage.allocation_api._page_session)
    monkeypatch.setattr(stage.store, "_candidate", candidate)
    monkeypatch.setattr(stage, "_run_workers", before_workers)


async def test_actual_stage_metadata_warm_retry_reuses_committed_family_and_query_ids(tmp_path, monkeypatch):
    from process.custom_import import family_shard_metadata_journal as metadata_journal
    from tests.test_custom_import_family_shard_metadata_journal import _snapshot as metadata_snapshot
    from tests.test_custom_import_family_shard_metadata_journal import _source

    async with _source(tmp_path, monkeypatch) as (case, original_source, journal, native, limits):
        owner = _metadata_retry_owner(tmp_path, case, original_source, journal, limits)
        _install_metadata_retry_faults(monkeypatch, native)
        try:
            with pytest.raises(ConnectionError, match="committed family result lost"):
                await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
            assert owner.pending == "metadata" and owner.metadata is None
            assert owner.allocated is original_source and len(native.family_calls) == 1
            first_attempts = tuple((journal.root / "outputs").iterdir())
            with pytest.raises(FamilyBlockUnavailable, match="query commit is unknown"):
                await stage.advance_source_admission(None, owner, attempt_root=owner.root / "second")
            assert owner.pending == "metadata" and owner.metadata is None
            assert len(native.family_calls) == 1 and all(path.exists() for path in first_attempts)
            protected = await metadata_snapshot(case, original_source, journal)
            query_record = tuple(protected.records())[0][1]
            assert query_record.kind == "artifact"
            query_name = metadata_journal._decode(query_record.payload, journal.control_limits.max_record_bytes)[4]
            protected_query_bytes = (journal.root / "vectors" / query_name).read_bytes()
            with pytest.raises(RuntimeError, match="before worker dispatch"):
                await stage.advance_source_admission(None, owner, attempt_root=owner.root / "third")
            assert owner.pending is None and owner.metadata is not None and owner.tasks is not None
            assert owner.allocated is original_source and owner.allocation_journal is journal
            assert [ids[0] for _key, ids in owner.metadata.family_ids()] == [2001, 2018]
            assert len(tuple(owner.metadata.query_keys())) == 1 and len(native.family_calls) == 1
            assert owner.metadata.query_path.read_bytes() == protected_query_bytes
            assert (await metadata_snapshot(case, original_source, journal)).head == protected.head
            assert all(path.exists() for path in first_attempts)
            assert len(owner.tasks.files) == len(owner.routed)
        finally:
            await owner.close_terminal()
        assert not owner.root.exists() and journal.root.exists()


def _event_owner(tmp_path, native):
    from process.custom_import.family_shard_capture import PreparedCaptureSource

    owner = _owner(tmp_path)
    owner.context = stage.SourceStageContext(
        native.request, native.capture[2], native.layout, native.source.snapshot_family_id, native.journal.build_id
    )
    owner.limits = replace(owner.limits, allocation=native.limits)
    owner.prepared = PreparedCaptureSource(native.capture[4], native.capture[0], native.source.source)
    owner.allocated, owner.allocation_journal, owner.metadata = native.source, native.journal, native.metadata
    owner.admission, owner.workers = native.admission, native.workers
    # These are retained coordinator inputs; native journal proof stays genuine.
    owner.routed, owner.tasks = object(), object()
    owner.common_context, owner.common_output, owner.common_receipt = object(), object(), object()
    owner.counts, owner.source_completed = SourceOutcomeCounts(0, 0), True
    return owner


async def test_actual_stage_events_warm_retry_reuses_committed_timestamp_vectors_and_copy(tmp_path, monkeypatch):
    from tests import test_custom_import_family_shard_events as event_fixture

    async with event_fixture._case(tmp_path, monkeypatch) as native:
        owner = _event_owner(tmp_path, native)
        _observations, native_ids = event_fixture._native_boundary(monkeypatch, native)
        native.native.phase = "admission"
        event_fixture._lose_reply(monkeypatch, native, "batch", "unknown")
        copied_cells = []

        async def current(*_args):
            return "admission"

        async def bind(*_args):
            assert owner.counts is not None and owner.source_completed

        async def stop_before_copy(_factory, current_owner, batches, receipt):
            assert current_owner is owner and receipt["role_counts"]["rejection"] == len(native_ids)
            copied_cells.extend(copy_row for batch in batches for copy_row in batch["rejection"])
            raise RuntimeError("bounded stop before native event COPY")

        monkeypatch.setattr(stage, "_native_phase", current)
        monkeypatch.setattr(stage, "_bind_counts", bind)
        monkeypatch.setattr(stage, "_copy", stop_before_copy)
        try:
            with pytest.raises(FamilyBlockUnavailable, match="commit is unknown"):
                await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
            assert owner.pending == "events" and owner.events is None and native_ids
            original_ids = tuple(native_ids)
            retained = event_fixture._attempts(native)
            with pytest.raises(RuntimeError, match="stop before native event COPY"):
                await stage.advance_source_admission(None, owner, attempt_root=owner.root / "retry")
            assert owner.pending is None and owner.events is not None and owner.event_receipt is not None
            assert tuple(native_ids) == original_ids and all(path.exists() for path in retained)
            copied_rows = [dict(zip(event_fixture.events._COLUMNS, copy_row, strict=True)) for copy_row in copied_cells]
            assert [copy_row["rejection_id"] for copy_row in copied_rows] == native_ids
            assert all(copy_row["created_at"] == native.owner_stamps[0] for copy_row in copied_rows)
            assert owner.allocation_journal is native.journal and owner.allocated is native.source
        finally:
            await owner.close_terminal()
        assert not owner.root.exists() and native.journal.root.exists()


def _damage_event_custody(owner, native, missing):
    from process.custom_import.continuation_runtime import BuildContinuationContext

    match missing:
        case "journal":
            owner.allocation_journal = None
        case "link":
            owner.prepared = replace(owner.prepared, plan=replace(native.source.source))
        case "family":
            owner.allocated = replace(native.source, snapshot_family_id=native.source.snapshot_family_id + 1)
        case "continuation":
            owner.context = replace(
                owner.context,
                request=replace(
                    native.request, continuation=BuildContinuationContext(9, native.request.fence, b"p" * 32)
                ),
            )
        case _:
            setattr(owner, missing, None)


@pytest.mark.parametrize(
    "missing", ("prepared", "allocated", "journal", "link", "metadata", "admission", "counts", "family", "continuation")
)
async def test_pending_events_cannot_replace_original_custody(tmp_path, monkeypatch, missing):
    from tests import test_custom_import_family_shard_events as event_fixture

    async with event_fixture._case(tmp_path, monkeypatch) as native:
        owner = _event_owner(tmp_path, native)
        _observations, native_ids = event_fixture._native_boundary(monkeypatch, native)
        owner.pending = "events"
        _damage_event_custody(owner, native, missing)

        async def current(*_args):
            return "admission"

        monkeypatch.setattr(stage, "_native_phase", current)
        with pytest.raises(FamilyBlockUnavailable):
            await stage.advance_source_admission(None, owner, attempt_root=owner.root / "forbidden")
        assert owner.pending == "events" and owner.events is None and not native_ids
        assert not owner.root.exists() and not event_fixture._attempts(native)


@pytest.mark.parametrize("missing", ("prepared", "allocated", "journal", "link", "family", "continuation"))
async def test_pending_metadata_cannot_authorize_reconstruction_from_missing_or_changed_originals(
    tmp_path, monkeypatch, missing
):
    from process.custom_import.continuation_runtime import BuildContinuationContext
    from process.custom_import.family_shard_capture import PreparedCaptureSource
    from tests.test_custom_import_family_shard_metadata_journal import _source

    async with _source(tmp_path, monkeypatch) as (case, original_source, journal, native, limits):
        owner = _owner(tmp_path)
        owner.context = stage.SourceStageContext(
            case[1],
            case[2],
            FamilyBlockLayout(case[1].definition, case[2].child_collection_slots),
            original_source.snapshot_family_id,
            journal.build_id,
        )
        owner.prepared = PreparedCaptureSource(case[4], case[0], original_source.source)
        owner.allocated, owner.allocation_journal = original_source, journal
        owner.pending = "metadata"
        match missing:
            case "prepared":
                owner.prepared = None
            case "allocated":
                owner.allocated = None
            case "journal":
                owner.allocation_journal = None
            case "link":
                owner.prepared = replace(owner.prepared, plan=replace(original_source.source))
            case "family":
                owner.allocated = replace(original_source, snapshot_family_id=original_source.snapshot_family_id + 1)
            case _:
                owner.context = replace(
                    owner.context,
                    request=replace(
                        case[1],
                        continuation=BuildContinuationContext(9, case[1].fence, b"p" * 32),
                    ),
                )

        async def current(*_args):
            return "source"

        monkeypatch.setattr(stage, "_native_phase", current)
        with pytest.raises(FamilyBlockUnavailable, match="automatic rebuilding"):
            await stage.advance_source_admission(None, owner, attempt_root=owner.root / "forbidden")
        assert owner.pending == "metadata" and not owner.root.exists()
        assert not native.family_calls


async def test_missing_checkpoint_refuses_worker_regeneration(pipeline):
    owner, boundary = pipeline
    boundary.fail_copy, boundary.fail_checkpoint = True, True
    with pytest.raises(ConnectionError, match="COPY commit") as failure:
        await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
    assert isinstance(failure.value.__cause__, OSError)
    assert owner.pending == "joined" and owner.runs[0].completed and not boundary.closed
    with pytest.raises(FamilyBlockUnavailable, match="automatic rebuilding"):
        await stage.advance_source_admission(None, owner, attempt_root=owner.root / "second")
    assert boundary.calls.count("run") == 1


async def test_native_progress_requires_originals(pipeline):
    owner, boundary = pipeline
    boundary.phase, owner.phase = "graph", "source"
    with pytest.raises(FamilyBlockUnavailable, match="retained original"):
        await stage.advance_source_admission(None, owner, attempt_root=owner.root / "first")
    assert not owner.root.exists() and boundary.calls == ["native"]


async def test_native_dispatch_checks_fresh_guards(tmp_path, monkeypatch):
    owner = _owner(tmp_path)
    guard_events = []

    async def snapshot(*_args):
        guard_events.append("snapshot")
        return SimpleNamespace(phase="source")

    @asynccontextmanager
    async def page(*_args):
        guard_events.append("guard")
        yield object(), SimpleNamespace(phase="source")
        guard_events.append("exit")

    async def candidate(*_args):
        guard_events.append("candidate")
        return None, object()

    async def empty(*_args):
        guard_events.append("empty")

    monkeypatch.setattr(stage, "_snapshot", snapshot)
    monkeypatch.setattr(stage, "_page_session", page)
    monkeypatch.setattr(stage.store, "_candidate", candidate)
    monkeypatch.setattr(stage, "_empty_candidate", empty)
    assert await stage._native_phase(None, owner) == "source"
    assert guard_events == ["snapshot", "guard", "candidate", "empty", "exit"] and not owner.root.exists()


async def test_cancelled_cpu_retains_completed_owner(tmp_path):
    owner = _owner(tmp_path)
    owner._ensure_root()
    entered, release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    close_events = []

    def produce():
        artifact = _Artifact(owner.root / "admission", "admission", close_events)
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return artifact

    task = asyncio.create_task(stage._retain_cpu(owner, "admission", produce, lambda artifact: artifact.close()))
    try:
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert owner.admission.root.exists() and owner.pending is None and close_events == []
    finally:
        release.set()
        await owner.close_terminal()
    assert close_events == ["admission"] and not owner.root.exists()


async def test_completed_async_result_survives_cancel(tmp_path):
    owner = _owner(tmp_path)
    owner._ensure_root()
    completed = asyncio.Event()
    close_events = []

    async def produce():
        artifact = _Artifact(owner.root / "allocation", "allocation", close_events)
        completed.set()
        return artifact

    task = asyncio.create_task(stage._retain(owner, "allocated", produce, lambda artifact: artifact.close()))
    try:
        await completed.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert owner.allocated.root.exists() and owner.pending is None and close_events == []
    finally:
        await owner.close_terminal()
    assert close_events == ["allocation"]


@pytest.mark.parametrize("phase", ["output", "verifying", "verified"])
async def test_later_phase_refuses_source_dispatch(tmp_path, monkeypatch, phase):
    owner = _owner(tmp_path)

    async def snapshot(*_args):
        return SimpleNamespace(phase=phase)

    monkeypatch.setattr(stage, "_snapshot", snapshot)
    with pytest.raises(FamilyBlockUnavailable, match="later phases"):
        await stage.advance_source_admission(None, owner, attempt_root=owner.root / "unused")
    assert not owner.root.exists() and owner._busy is False


def test_selected_entities_consume_complete_headers(tmp_path, monkeypatch):
    owner = _owner(tmp_path)
    events = []

    def headers(*_args, **_kwargs):
        try:
            yield 0, {"selection": "source", "family_ids": (71, 81)}
            yield 1, {"selection": "absent", "family_ids": (72, 82)}
            yield 2, {"selection": "error", "family_ids": (73, 83)}
            events.append("eof")
        finally:
            events.append("closed")

    monkeypatch.setattr(stage.admission_api, "read_admission_families", headers)
    assert list(stage._selected_entities(owner)) == [81]
    assert events == ["eof", "closed"] and not owner.root.exists()


async def test_selected_entities_use_genuine_admission_family_pairs(tmp_path, monkeypatch):
    from tests.test_custom_import_family_shard_admission import _build_audit, _child, _inputs, _root

    records_by_stream = {
        "root_source": [[_root(), _root("1234567893", score=None)]],
        "detail_source": [[_child()]],
    }
    async with _inputs(tmp_path, monkeypatch, records_by_stream) as (case, allocated, outputs):
        admission = _build_audit(tmp_path, case, allocated, outputs, _worker_limits().facts)
        try:
            # Independently read the original worker header dictionaries, before
            # the admission API wraps each preserved header with its shard ID.
            expected_entities = [
                entry[1]["family_ids"][1]
                for output in outputs
                for entry in output.audit_rows()
                if entry[0] == "family" and entry[1]["selection"] == "source"
            ]
            assert len(expected_entities) == 1
            assert {
                header["selection"] for _shard, header in stage.admission_api.read_admission_families(admission)
            } == {"source", "absent"}
            owner = SimpleNamespace(admission=admission, current=lambda: None)
            assert list(stage._selected_entities(owner)) == expected_entities
        finally:
            stage.admission_api.cleanup_admission_output(admission)
