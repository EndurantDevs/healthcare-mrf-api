# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real owner processes, journals, files and workers; native SQL transport is doubled.

These cases establish bounded SOURCE recovery, not PostgreSQL or full-job proof.
"""

import asyncio
import multiprocessing
import os
import traceback
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_block_store as store
from process.custom_import import family_shard_allocate as allocation
from process.custom_import import family_shard_capture as capture
from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_metadata as metadata
from process.custom_import import family_shard_metadata_journal as metadata_journal
from process.custom_import import family_shard_owner_job as owners
from process.custom_import import family_shard_source_checkpoint as ready_api
from process.custom_import import family_shard_source_stage as stage
from process.custom_import import family_shard_tasks as tasks
from process.custom_import.continuation_runtime import BuildContinuationContext, producer_token_sha256
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.family_shard_worker import run_source_shard
from tests.test_custom_import_family_shard_allocate import _native_boundary
from tests.test_custom_import_family_shard_capture import _inputs
from tests.test_custom_import_family_shard_common import _lookup_fixture
from tests.test_custom_import_family_shard_metadata import _owner as _metadata_owner
from tests.test_custom_import_family_shard_source_plan import _child, _root
from tests.test_custom_import_family_shard_source_stage import _owner as _stage_fixture


class _SourceFinished(Exception):
    pass


def _identifiers(source):
    return tuple((pack.pack_index, *identifiers) for pack, *identifiers in allocation.read_source_allocations(source))


def _new_job(root, request):
    limits = _stage_fixture(root.parent).limits
    allocation_limits = limits.allocation
    limits = replace(
        limits,
        allocation=replace(
            allocation_limits,
            spool=replace(allocation_limits.spool, memory_bytes=4 * allocation_limits.maximum_batch_bytes),
        ),
    )
    return owners.OwnerShardJob(request, work=PlanWork(root, limits.facts, lambda _path: None), source_limits=limits)


class _NativeTransport:
    """Double only database discovery/transactions; retain real producer control flow."""

    def __init__(self, monkeypatch, root, request, registry, header):
        self.monkeypatch, self.root = monkeypatch, Path(root)
        self.request, self.registry, self.header = request, registry, header
        self.native, self.existing = None, None
        self.receipts, self.replayed, self.copy_calls = {}, [], []
        self.lose_copy_reply = False
        self.copy_boundary = "worker"
        self.common_builds = 0
        self._prepare, self._allocate = capture.prepare_captured_source, allocation.allocate_source_plan
        self._common = stage.common_api.build_source_common
        self.job = _new_job(root, request)

    async def discover(self, *_arguments):
        return self.existing

    async def registry_read(self, *_arguments):
        return self.registry

    async def create(self, *_arguments):
        return 17

    async def begin(self, *_arguments):
        self.existing = 23
        return 23, self.registry

    async def replay(self, *_arguments):
        return None

    async def snapshot(self, *_arguments):
        return SimpleNamespace(
            phase="source" if self.native is None else self.native.phase,
            plan_stage=getattr(self.native, "plan_stage", "base"),
            plan_complete_at=getattr(self.native, "plan_complete_at", None),
            generation_id=getattr(self.native, "generation_id", None),
            graph_frozen_at=getattr(self.native, "graph_frozen_at", None),
        )

    @asynccontextmanager
    async def page(self, factory, request, build_id):
        assert request == self.request and build_id in (None, 23)
        if self.native is None:
            yield SimpleNamespace(), SimpleNamespace(phase="source")
        else:
            async with allocation._page_session(factory, request, build_id) as pair:
                for field in ("generation_id", "plan_complete_at", "graph_frozen_at"):
                    setattr(pair[1], field, getattr(self.native, field, None))
                pair[1].plan_stage = getattr(self.native, "plan_stage", "base")
                pair[1].source_frozen_at = getattr(self.native, "source_frozen_at", None)
                pair[1].source_occurrence_count = getattr(self.native, "source_occurrence_count", 0)
                yield pair

    @asynccontextmanager
    async def heartbeat(self, *_arguments, **_keywords):
        yield

    @asynccontextmanager
    async def pin(self, *_arguments, **_keywords):
        assert self.request.expected_base_generation_id is None
        yield None

    async def candidate(self, _session, request, family_id, layout, phases):
        assert request == self.request and family_id == 17 and phases == {"loading"}
        assert layout.definition.digest == request.definition.digest
        return None, object()

    async def family_id(self, _session, _statement):
        return SimpleNamespace(scalar_one_or_none=lambda: 17)

    async def empty_candidate(self, *_arguments):
        assert not self.receipts and self.native is None

    async def prepare(self, *arguments, **keywords):
        prepared = await self._prepare(*arguments, **keywords)
        self.native = _native_boundary(self.monkeypatch, prepared.plan)
        return prepared

    async def allocate(self, *arguments, **keywords):
        allocated = await self._allocate(*arguments, **keywords)
        _metadata_owner(self.monkeypatch, allocated, keywords["journal"])
        return allocated

    async def header_read(self, _session, request):
        assert request == self.request
        return self.header

    async def copy(self, _factory, request, family_id, layout, batches, receipt, **_limits):
        assert request == self.request and family_id == 17
        shard = receipt["shard_id"]
        self.copy_calls.append(shard)
        if shard in self.receipts:
            assert self.receipts[shard] == receipt
            self.replayed.append(shard)
            return
        source_owner = self.job.source
        protected = await self.protected()
        _ready, _custody, outputs, common = ready_api.source_owner_checkpoint(
            protected, self.root, stage.owner_source_profile(source_owner), self.job.current
        )
        if shard < len(source_owner.tasks.files):
            assert shard in {output.artifact[0] for output in outputs}
        elif shard == len(source_owner.tasks.files):
            assert common is not None and common.shard_id == shard
        else:
            stage.event_journal.require_completed_admission(
                protected, source_owner.limits.allocation.maximum_batch_records, self.job.current
            )
        proof = store.ShardOutputProof(layout, shard, receipt["partition_sha256"], receipt["source_record_count"])
        for batch in batches:
            for role, copy_values in batch.items():
                proof.add(role, copy_values)
        assert proof.receipt() == receipt
        self.receipts[shard] = receipt
        at_boundary = (
            bool(receipt["source_record_count"])
            if self.copy_boundary == "worker"
            else shard == len(source_owner.tasks.files)
        )
        if self.lose_copy_reply and at_boundary:
            self.lose_copy_reply = False
            raise ConnectionError("synthetic committed COPY reply lost")

    async def common(self, factory, context, *arguments, **keywords):
        self.common_builds += 1
        _lookup_fixture(self.monkeypatch, context)
        lookup = stage.common_api._lookup

        async def clock_lookup(factory, supplied, role=None, requests=()):
            """Model the real transaction clock changing on every producer invocation."""
            result = await lookup(factory, supplied, role, requests)
            return datetime.now(UTC) if role is None else result

        self.monkeypatch.setattr(stage.common_api, "_lookup", clock_lookup)
        return await self._common(factory, context, *arguments, **keywords)

    async def complete_source(self, _factory, context, build_id, admission, output, receipt, **_keywords):
        assert build_id == 23 and context is self.job.source.common_context
        assert receipt == self.receipts[len(self.job.source.tasks.files)]
        assert admission is self.job.source.admission and output is self.job.source.common_output
        self.native.phase = "admission"
        self.native.source_frozen_at = datetime.now(UTC)
        self.native.source_occurrence_count = context.source.source.counts[0]

    async def stop_after_source(self, *_arguments):
        raise _SourceFinished

    async def protected(self):
        return await checkpoint.read_owner_checkpoint(
            None,
            self.request,
            23,
            17,
            self.job.layout,
            root=self.root / "source" / "allocation-journal" / "checkpoints",
            limits=self.job.source_limits.checkpoint,
        )

    def install(self):
        for name, function in (
            ("_existing_build", self.discover),
            ("load_registry", self.registry_read),
            ("begin_shard_build", self.begin),
            ("_snapshot", self.snapshot),
            ("_page_session", self.page),
            ("_renew_while_reading", self.heartbeat),
            ("pinned_base", self.pin),
        ):
            self.monkeypatch.setattr(owners, name, function)
        for module, name, function in (
            (owners.build_output, "_replay", self.replay),
            (store, "create_candidate", self.create),
            (store, "_candidate", self.candidate),
            (store, "_execute", self.family_id),
            (store, "copy_shard", self.copy),
            (stage, "_snapshot", self.snapshot),
            (stage, "_empty_candidate", self.empty_candidate),
            (stage, "_page_session", self.page),
            (capture, "prepare_captured_source", self.prepare),
            (allocation, "allocate_source_plan", self.allocate),
            (capture, "_page_session", self.page),
            (capture, "_read_source_header", self.header_read),
            (stage.common_api, "build_source_common", self.common),
            (stage, "complete_shard_source", self.complete_source),
            (stage, "_bind_counts", self.stop_after_source),
        ):
            self.monkeypatch.setattr(module, name, function)


async def _transport(monkeypatch, root):
    request, registry, header, _parts = await _inputs(
        monkeypatch, {"root_source": [[_root()]], "detail_source": [[_child()]]}
    )
    boundary = _NativeTransport(monkeypatch, root, replace(request, complete_scope=True), registry, header)
    boundary.install()
    return boundary


async def _prepare_interrupted_owner(root, connection, copy_boundary):
    with pytest.MonkeyPatch.context() as monkeypatch:
        boundary = await _transport(monkeypatch, root)
        boundary.lose_copy_reply = True
        boundary.copy_boundary = copy_boundary
        with pytest.raises(ConnectionError, match="committed COPY"):
            await owners.advance_owner_shard_job(lambda: None, boundary.job, attempt_root=root / "source" / "first")
        source = boundary.job.source
        assert all(not process.is_alive() for run in source.runs for process in run._processes.values())
        connection.send(
            {
                "committed": boundary.native.committed,
                "plan": source.allocated.source,
                "receipts": boundary.receipts,
                "identifiers": _identifiers(source.allocated),
                "families": tuple(source.metadata.family_ids()),
                "tasks": tasks._prepared_sha256(source.tasks),
                "descriptor": source.ready.descriptor_sha256,
                "counts": source.allocated.source.counts,
                "recorded": tuple(source.output_custody),
                "common_timestamp": None
                if source.common_output is None
                else source.common_output.proof_inputs["owner_created_at"],
            }
        )
        assert await asyncio.to_thread(connection.recv) == "exit"
        os._exit(77)


def _interrupted_process(root, connection, copy_boundary):
    try:
        asyncio.run(_prepare_interrupted_owner(Path(root), connection, copy_boundary))
    except BaseException:
        connection.send({"error": traceback.format_exc()})
        os._exit(78)


def _forbid(*_arguments, **_keywords):
    pytest.fail("cold SOURCE cannot recapture, create a candidate, or allocate replacement IDs")


def _restore_transport(monkeypatch, boundary, evidence):
    boundary.native = _native_boundary(monkeypatch, evidence["plan"])
    boundary.native.committed = evidence["committed"]
    boundary.native.build_id, boundary.existing = 23, 23
    boundary.receipts = dict(evidence["receipts"])
    for module, names in (
        (capture, ("prepare_captured_source", "_capture_parts")),
        (allocation, ("allocate_source_plan", "_allocate_record_ids", "intern_root_records")),
        (metadata, ("_allocate_record_ids", "intern_global_identities")),
        (store, ("create_candidate",)),
        (owners, ("begin_shard_build",)),
    ):
        for name in names:
            monkeypatch.setattr(module, name, _forbid)


async def _assert_common_order_refused(boundary):
    """The common suffix requires every worker record exactly once and the original ready record."""
    protected = await boundary.protected()
    original_records = list(protected.records())
    source_stage = boundary.job.source
    for change in ("duplicate", "misplaced", "incomplete", "unbound"):
        checkpoint_records = list(original_records)
        if change == "duplicate":
            checkpoint_records.insert(0, checkpoint_records[0])
        elif change == "misplaced":
            checkpoint_records[0], checkpoint_records[1] = checkpoint_records[1], checkpoint_records[0]
        elif change == "incomplete":
            del checkpoint_records[1]
        else:
            head, checkpoint_record = checkpoint_records[0]
            checkpoint_records[0] = head, stage.common_api.source_common_record(b"x" * 32, source_stage.common_custody)
        altered = SimpleNamespace(limits=protected.limits, records=lambda **_options: iter(checkpoint_records))
        with pytest.raises(ValueError):
            ready_api.source_owner_checkpoint(
                altered, boundary.root, stage.owner_source_profile(source_stage), source_stage.current
            )
        run = SimpleNamespace(
            source=source_stage.allocated,
            journal=source_stage.allocation_journal,
            work=SimpleNamespace(current=source_stage.current),
        )
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            list(metadata_journal._checked_records(run, altered))


@pytest.mark.asyncio
@pytest.mark.parametrize("copy_boundary", ("worker", "common"))
async def test_new_owner_after_process_exit_reuses_recorded_shard_and_original_ids(
    tmp_path, monkeypatch, copy_boundary
):
    """An actual dead owner leaves files; a new job replays one COPY and completes SOURCE."""
    root = tmp_path / "job"
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=_interrupted_process, args=(root, child, copy_boundary))
    process.start()
    child.close()
    boundary = None
    try:
        assert await asyncio.to_thread(parent.poll, 40), "owner did not reach committed COPY"
        evidence = parent.recv()
        assert "error" not in evidence, evidence.get("error")
        boundary = await _transport(monkeypatch, root)
        _restore_transport(monkeypatch, boundary, evidence)
        with pytest.raises(BlockingIOError):
            await owners.advance_owner_shard_job(lambda: None, boundary.job, attempt_root=root / "source" / "blocked")
        assert boundary.job.source is None and not (root / "source" / "blocked").exists()
        parent.send("exit")
        await asyncio.to_thread(process.join, 10)
        assert process.exitcode == 77
        boundary.job = _new_job(root, boundary.request)
        assert boundary.job.source is boundary.job.build_id is boundary.job.family_id is None
        with pytest.raises(_SourceFinished):
            await owners.advance_owner_shard_job(lambda: None, boundary.job, attempt_root=root / "source" / "second")
        source_stage = boundary.job.source
        assert source_stage.source_completed and boundary.native.phase == "admission"
        assert _identifiers(source_stage.allocated) == evidence["identifiers"]
        assert tuple(source_stage.metadata.family_ids()) == evidence["families"]
        assert tasks._prepared_sha256(source_stage.tasks) == evidence["tasks"]
        assert source_stage.ready.descriptor_sha256 == evidence["descriptor"]
        assert source_stage.allocated.source.counts == evidence["counts"]
        assert boundary.replayed == sorted(evidence["receipts"])
        assert set(source_stage.runs[0]._missing) == set(range(len(source_stage.tasks.files))) - set(
            evidence["recorded"]
        )
        if copy_boundary == "common":
            assert not source_stage.runs[0]._processes and len(source_stage.tasks.files) in boundary.replayed
            assert boundary.common_builds == 0
            assert source_stage.common_output.proof_inputs["owner_created_at"] == evidence["common_timestamp"]
            await _assert_common_order_refused(boundary)
        assert all(
            output._work.root.parent.name == "first"
            for output in source_stage.workers
            if output.receipt["shard_id"] in boundary.replayed
        )
        assert not boundary.native.allocations and source_stage.counts.accepted_family_count == 1
        await boundary.job.close_terminal()
        assert not root.exists()
    finally:
        if process.is_alive():
            process.terminate()
            await asyncio.to_thread(process.join, 10)
        parent.close()
        if boundary is not None and boundary.job._owner_lock is not None:
            boundary.job._owner_lock.close()


@pytest.fixture
async def ready_boundary(tmp_path, monkeypatch):
    """Stop before pool launch, then produce one genuine closed shard for custody tests."""
    boundary = await _transport(monkeypatch, tmp_path / "job")

    @asynccontextmanager
    async def stop_before_workers(*_arguments, **_keywords):
        raise RuntimeError("synthetic stop before worker launch")
        yield

    monkeypatch.setattr(tasks, "run_source_tasks", stop_before_workers)
    try:
        with pytest.raises(RuntimeError, match="before worker launch"):
            await owners.advance_owner_shard_job(
                lambda: None, boundary.job, attempt_root=boundary.root / "source" / "first"
            )
        source = boundary.job.source
        attempt = source.root / "closed-output"
        attempt.mkdir(mode=0o700)
        shard = next(index for index, task in enumerate(source.tasks.files) if task.shard.records)
        plan = source.tasks.load_plan(shard, source.limits.worker)
        output = await asyncio.to_thread(run_source_shard, plan, attempt / f"shard-{shard:04d}", source.limits.worker)
        yield boundary, output
    finally:
        await boundary.job.close_terminal()
        assert not boundary.root.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("before", "after", "unknown"))
async def test_output_custody_commit_reconciles_before_any_copy(ready_boundary, failure):
    """A lost response never substitutes an uncommitted head or replacement output."""
    boundary, output = ready_boundary
    source = boundary.job.source
    before = (await boundary.protected()).head
    original_ids = _identifiers(source.allocated)
    original_bytes = output.path.read_bytes(), output._output.path.read_bytes()
    boundary.native.failure = failure
    if failure == "before":
        with pytest.raises(RuntimeError, match="lease before commit"):
            await stage._anchor_output(None, source, output)
        assert (await boundary.protected()).head == before and not source.output_custody
    elif failure == "unknown":
        with pytest.raises(FamilyBlockUnavailable, match="commit is unknown"):
            await stage._anchor_output(None, source, output)
        assert (await boundary.protected()).head != before and not source.output_custody
    else:
        await stage._anchor_output(None, source, output)
    boundary.native.failure = None
    await stage._anchor_output(None, source, output)
    committed = (await boundary.protected()).head
    assert committed.revision == before.revision + 1
    await stage._anchor_output(None, source, output)
    assert (await boundary.protected()).head == committed
    assert original_ids == _identifiers(source.allocated) and not boundary.copy_calls
    assert original_bytes == (output.path.read_bytes(), output._output.path.read_bytes())


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("root", "budget", "lock"))
async def test_protected_owner_custody_refuses_replacement(ready_boundary, change):
    """Filesystem observations cannot substitute for the protected root, profile or lock."""
    boundary, _output = ready_boundary
    source = boundary.job.source
    custody, profile = source.custody, stage.owner_source_profile(source)
    if change == "root":
        with pytest.raises(FamilyBlockUnavailable, match="root or resource"):
            custody.current(boundary.root.parent, profile)
    elif change == "budget":
        with pytest.raises(FamilyBlockUnavailable, match="root or resource"):
            custody.current(boundary.root, (profile[0], profile[1] + 1, profile[2]))
    else:
        replaced = replace(custody, lock_identity=(custody.lock_identity[0], custody.lock_identity[1] + 1))
        with pytest.raises(FamilyBlockUnavailable, match="lifetime lock"):
            replaced.current(boundary.root, profile)
    custody.current(boundary.root, profile)
    assert not boundary.copy_calls


@pytest.mark.asyncio
async def test_protected_source_directory_cannot_be_replaced(ready_boundary):
    boundary, _output = ready_boundary
    source = boundary.job.source
    original = source.root.with_name("original-source")
    source.root.rename(original)
    source.root.mkdir(mode=0o700)
    try:
        with pytest.raises(FamilyBlockUnavailable, match="directory custody"):
            source.custody.current(boundary.root, stage.owner_source_profile(source))
    finally:
        source.root.rmdir()
        original.rename(source.root)
    source.custody.current(boundary.root, stage.owner_source_profile(source))
    assert not boundary.copy_calls


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ("output", "verifying", "continuation"))
async def test_reopen_preserves_phase_epoch_guards(ready_boundary, phase):
    """Even a complete original SOURCE checkpoint cannot authorize another phase or epoch."""
    boundary, _output = ready_boundary
    request = boundary.request
    if phase == "continuation":
        request = replace(request, continuation=BuildContinuationContext(71, 23, producer_token_sha256(request)))
    else:
        boundary.native.phase = phase
    reopened = _new_job(boundary.root, request)
    expected = "cold reopening" if phase == "continuation" else "original protected OUTPUT inputs"
    with pytest.raises(FamilyBlockUnavailable, match=expected):
        await owners.advance_owner_shard_job(
            lambda: None, reopened, attempt_root=boundary.root / "source" / "forbidden"
        )
    assert reopened.source is reopened._owner_lock is None
    assert not (boundary.root / "source" / "forbidden").exists() and not boundary.copy_calls
    boundary.job._owner_lock.check()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ("generation_id", "plan_complete_at", "plan_stage", "graph_frozen_at"))
async def test_graph_progress_cannot_adopt_source_only(ready_boundary, field):
    """Current native GRAPH progress cannot be relabelled as an earlier SOURCE retry."""
    boundary, _output = ready_boundary
    boundary.native.phase = "graph"
    setattr(boundary.native, field, "complete" if field == "plan_stage" else 101)
    reopened = _new_job(boundary.root, boundary.request)
    expected = (
        "original protected OUTPUT inputs"
        if field in {"plan_stage", "plan_complete_at"}
        else "retained GRAPH or generation"
    )
    with pytest.raises(FamilyBlockUnavailable, match=expected):
        await owners.advance_owner_shard_job(
            lambda: None, reopened, attempt_root=boundary.root / "source" / "forbidden"
        )
    assert reopened.source is reopened._owner_lock is None
    assert not boundary.copy_calls
    boundary.job._owner_lock.check()


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ("audit", "output"))
async def test_recorded_output_bytes_are_checked_before_cold_copy(ready_boundary, role):
    boundary, output = ready_boundary
    source = boundary.job.source
    await stage._anchor_output(None, source, output)
    path = output.path if role == "audit" else output._output.path
    path.chmod(0o600)
    path.write_bytes(path.read_bytes() + b"changed")
    path.chmod(0o400)
    attempt = source.root / "different-attempt"
    with pytest.raises(ValueError):
        tasks._resume_outputs(
            source.tasks,
            tuple(source.output_custody.values()),
            attempt,
            source.limits.worker,
            owner_lock=source.owner_lock,
        )
    assert not boundary.copy_calls and not attempt.exists()


@pytest.mark.asyncio
async def test_source_terminal_failure_keeps_lock_and_allows_exact_cleanup_retry(ready_boundary, monkeypatch):
    boundary, _output = ready_boundary
    source, attempts = boundary.job.source, []
    original = stage.shutil.rmtree

    def refuse_once(path, *arguments, **keywords):
        if Path(path) == source.root and not attempts:
            attempts.append(path)
            raise PermissionError("synthetic SOURCE cleanup refusal")
        return original(path, *arguments, **keywords)

    monkeypatch.setattr(stage.shutil, "rmtree", refuse_once)
    with pytest.raises(PermissionError, match="SOURCE cleanup"):
        await boundary.job.close_terminal()
    assert not source._closed and source.root.is_dir()
    boundary.job._owner_lock.check()
    await boundary.job.close_terminal()
    assert source._closed and not boundary.root.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("order", ("duplicate_output", "output_before_ready", "missing_ready", "unknown", "empty"))
async def test_custody_parser_refuses_bad_order_and_unknown_records(ready_boundary, order):
    """Classify altered record sequences only; this does not forge a native committed head."""
    boundary, output = ready_boundary
    source = boundary.job.source
    await stage._anchor_output(None, source, output)
    protected = await boundary.protected()
    records = list(protected.records())
    if order == "duplicate_output":
        records.insert(0, records[0])
    elif order == "output_before_ready":
        records[0], records[1] = records[1], records[0]
    elif order == "missing_ready":
        del records[1]
    elif order == "empty":
        records.insert(2, (records[0][0], replace(records[0][1], payload=b"\x90")))
    else:
        records[0] = records[0][0], replace(records[0][1], payload=b"\x91\xb0unknown/artifact")
    altered = SimpleNamespace(limits=protected.limits, records=lambda **_options: iter(records))
    with pytest.raises(ValueError):
        ready_api.source_owner_checkpoint(altered, boundary.root, stage.owner_source_profile(source), source.current)
    run = SimpleNamespace(
        source=source.allocated, journal=source.allocation_journal, work=SimpleNamespace(current=source.current)
    )
    with pytest.raises((ValueError, FamilyBlockUnavailable)):
        list(metadata_journal._checked_records(run, altered))
    assert not boundary.copy_calls


class _AdmissionFinished(Exception):
    pass


class _CommittedAdmission(Exception):
    pass


class _AdmissionTransport(_NativeTransport):
    """Keep actual admission allocation journals/producers; double native transactions only."""

    def __init__(self, *arguments):
        super().__init__(*arguments)
        self.event_ids, self.prior_event_ids = [], []
        self.bound_counts, self.homes_calls = None, 0
        self.original_families, self.interrupted_plan = owners._families, None

    async def event_owner(self, _session, request, layout, allocated, count):
        assert request == self.request and layout is self.job.layout and allocated is self.job.source.allocated
        identifiers = tuple(10001 + len(self.prior_event_ids) + len(self.event_ids) + index for index in range(count))
        self.event_ids.extend(identifiers)
        return datetime.now(UTC), identifiers

    async def bind_counts(self, _factory, source_owner):
        assert source_owner is self.job.source and self.native.phase in {"admission", "graph", "rejected"}
        assert source_owner.common_receipt == self.receipts[len(source_owner.tasks.files)]
        if self.bound_counts is None:
            self.bound_counts = source_owner.counts
        assert self.bound_counts == source_owner.counts

    async def admission(self, _factory, context, build_id, admitted, events, receipt, **options):
        from process.custom_import.family_shard_admission_control import _admission_completion

        assert build_id == 23 and context is self.job.source.common_context
        expected, errors, _counts = _admission_completion(
            context, admitted, events, receipt, options["limits"], options["check"]
        )
        assert expected == self.receipts[receipt["shard_id"]]
        phase = "rejected" if errors else "graph"
        assert self.native.phase in {"admission", phase}
        self.native.phase = phase
        return phase

    async def homes(self, _factory, context, build_id, admitted, **_keywords):
        assert context is self.job.source.common_context and build_id == 23
        assert self.native.phase == "graph" and admitted is self.job.source.admission
        self.homes_calls += 1
        return object()

    async def finish(self, factory, job):
        if self.boundary_name == "plan":
            return await self.original_families(factory, job)
        raise _AdmissionFinished

    async def before_retained(self, *_arguments):
        raise _AdmissionFinished

    def install(self):
        super().install()
        for module, name, function in (
            (stage.event_journal, "_page_session", self.page),
            (stage.events_api, "_owner", self.event_owner),
            (stage, "_bind_counts", self.bind_counts),
            (stage, "complete_shard_admission", self.admission),
            (stage, "register_source_homes", self.homes),
            (owners, "_families", self.finish),
            (owners, "advance_retained_families", self.before_retained),
        ):
            self.monkeypatch.setattr(module, name, function)


async def _admission_transport(monkeypatch, root, boundary_name):
    rows_by_stream = (
        {"root_source": [[]], "detail_source": [[]]}
        if boundary_name == "rejected"
        else {"root_source": [[_root()]], "detail_source": [[_child(amount="1"), _child(amount="2")]]}
    )
    request, registry, header, _parts = await _inputs(monkeypatch, rows_by_stream)
    boundary = _AdmissionTransport(
        monkeypatch, root, replace(request, complete_scope=boundary_name != "rejected"), registry, header
    )
    boundary.boundary_name = boundary_name
    boundary.install()
    return boundary


def _interrupt_admission(monkeypatch, boundary, boundary_name):
    module, name = {
        "source-commit": (stage, "complete_shard_source"),
        "base-failure": (stage, "complete_shard_source"),
        "event-batch": (stage.event_journal._Run, "take"),
        "event-copy": (store, "copy_shard"),
        "graph": (stage, "complete_shard_admission"),
        "rejected": (stage, "complete_shard_admission"),
        "homes": (stage, "register_source_homes"),
        "plan": (owners, "build_family_plan"),
    }[boundary_name]
    original = getattr(module, name)

    async def stop_after_commit(*arguments, **keywords):
        returned = await original(*arguments, **keywords)
        if boundary_name == "event-batch" and arguments[1] != "batch":
            return returned
        if boundary_name == "event-copy" and arguments[5]["shard_id"] != len(boundary.job.source.tasks.files) + 1:
            return returned
        if boundary_name == "plan":
            boundary.interrupted_plan = returned
        raise _CommittedAdmission(boundary_name)

    monkeypatch.setattr(module, name, stop_after_commit)


async def _prepare_admission_exit(root, connection, boundary_name):
    with pytest.MonkeyPatch.context() as monkeypatch:
        boundary = await _admission_transport(monkeypatch, root, boundary_name)
        _interrupt_admission(monkeypatch, boundary, boundary_name)
        with pytest.raises(_CommittedAdmission, match=boundary_name):
            await owners.advance_owner_shard_job(lambda: None, boundary.job, attempt_root=root / "source" / "first")
        source_owner = boundary.job.source
        assert all(not process.is_alive() for run in source_owner.runs for process in run._processes.values())
        connection.send(
            {
                "committed": boundary.native.committed,
                "plan": source_owner.allocated.source,
                "receipts": boundary.receipts,
                "phase": boundary.native.phase,
                "identifiers": _identifiers(source_owner.allocated),
                "families": tuple(source_owner.metadata.family_ids()),
                "descriptor": source_owner.ready.descriptor_sha256,
                "tasks": tasks._prepared_sha256(source_owner.tasks),
                "event_ids": boundary.event_ids,
                "counts": boundary.bound_counts,
                "events": source_owner.events,
                "common": source_owner.common_custody,
                "frozen": boundary.native.source_frozen_at,
                "final_plan": boundary.interrupted_plan,
            }
        )
        assert await asyncio.to_thread(connection.recv) == "exit"
        os._exit(77)


def _admission_process(root, connection, boundary_name):
    try:
        asyncio.run(_prepare_admission_exit(Path(root), connection, boundary_name))
    except BaseException:
        connection.send({"error": traceback.format_exc()})
        os._exit(78)


def _restore_admission(monkeypatch, boundary, evidence):
    _restore_transport(monkeypatch, boundary, evidence)
    boundary.native.phase = evidence["phase"]
    boundary.native.source_frozen_at = evidence["frozen"]
    boundary.native.source_occurrence_count = evidence["plan"].counts[0]
    boundary.prior_event_ids, boundary.bound_counts = evidence["event_ids"], evidence["counts"]
    if evidence["event_ids"]:
        original_owner = boundary.event_owner

        async def no_reallocation(*arguments):
            assert arguments[-1] == 0, "committed admission IDs cannot be allocated again"
            return await original_owner(*arguments)

        monkeypatch.setattr(stage.events_api, "_owner", no_reallocation)


async def _assert_admission_recovered(boundary, evidence, boundary_name):
    source_owner = boundary.job.source
    assert source_owner.source_completed and not source_owner.runs
    assert _identifiers(source_owner.allocated) == evidence["identifiers"]
    assert tuple(source_owner.metadata.family_ids()) == evidence["families"]
    assert source_owner.ready.descriptor_sha256 == evidence["descriptor"]
    assert tasks._prepared_sha256(source_owner.tasks) == evidence["tasks"]
    assert source_owner.common_custody == evidence["common"] and boundary.common_builds == 0
    assert all(shard == len(source_owner.tasks.files) + 1 for shard in boundary.copy_calls)
    if evidence["events"] is not None:
        assert source_owner.events.sha256 == evidence["events"].sha256
        assert source_owner.events.owner_created_at == evidence["events"].owner_created_at
    if evidence["event_ids"]:
        assert not boundary.event_ids
    if evidence["final_plan"] is not None:
        assert boundary.job.final_plan.sha256 == evidence["final_plan"].sha256
        assert boundary.job.final_plan.root != evidence["final_plan"].root
        assert Path(evidence["final_plan"].root).is_dir()
    actual_ids = tuple(event.rejection_id for event in stage.events_api.read_allocated_events(source_owner.events))
    assert set(evidence["event_ids"]) <= set(actual_ids)
    if boundary_name == "rejected":
        assert boundary.job.complete and boundary.job.is_rejected and boundary.homes_calls == 0
    else:
        assert source_owner.phase == "graph" and boundary.homes_calls == 1
    assert not boundary.native.allocations
    await boundary.job.close_terminal()
    assert not boundary.root.exists()


async def _assert_partial_open_cleanup(monkeypatch, boundary):
    """A failed base repin keeps exact SOURCE cleanup custody and the real lock."""

    @asynccontextmanager
    async def unavailable_base(*_arguments, **_keywords):
        raise RuntimeError("synthetic base repin failure")
        yield

    monkeypatch.setattr(owners, "pinned_base", unavailable_base)
    with pytest.raises(RuntimeError, match="base repin"):
        await owners.advance_owner_shard_job(
            lambda: None, boundary.job, attempt_root=boundary.root / "source" / "second"
        )
    source_owner = boundary.job.source
    assert source_owner.prepared is None
    assert source_owner._root_identity == boundary.job._source_custody.directories[1][1][:2]
    boundary.job._owner_lock.check()
    assert not boundary.copy_calls and not boundary.native.allocations
    await boundary.job.close_terminal()
    assert not boundary.root.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "boundary_name",
    ("source-commit", "event-batch", "event-copy", "graph", "rejected", "homes", "plan", "base-failure"),
)
async def test_admission_process_death_preserves_originals(tmp_path, monkeypatch, boundary_name):
    """A fresh owner resumes only admission, including its genuine rejection path."""
    root = tmp_path / "job"
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=_admission_process, args=(root, child, boundary_name))
    process.start()
    child.close()
    boundary = None
    try:
        assert await asyncio.to_thread(parent.poll, 45), "owner did not reach admission commit"
        evidence = parent.recv()
        assert "error" not in evidence, evidence.get("error")
        boundary = await _admission_transport(monkeypatch, root, boundary_name)
        _restore_admission(monkeypatch, boundary, evidence)
        with pytest.raises(BlockingIOError):
            await owners.advance_owner_shard_job(lambda: None, boundary.job, attempt_root=root / "source" / "blocked")
        parent.send("exit")
        await asyncio.to_thread(process.join, 10)
        assert process.exitcode == 77
        boundary.job = _new_job(root, boundary.request)
        if boundary_name == "base-failure":
            await _assert_partial_open_cleanup(monkeypatch, boundary)
            return
        if boundary_name == "rejected":
            await owners.advance_owner_shard_job(lambda: None, boundary.job, attempt_root=root / "source" / "second")
        else:
            with pytest.raises(_AdmissionFinished):
                await owners.advance_owner_shard_job(
                    lambda: None, boundary.job, attempt_root=root / "source" / "second"
                )
        await _assert_admission_recovered(boundary, evidence, boundary_name)
    finally:
        if process.is_alive():
            process.terminate()
            await asyncio.to_thread(process.join, 10)
        parent.close()
        if boundary is not None and boundary.job._owner_lock is not None:
            boundary.job._owner_lock.close()
