# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner ordering, custody and count compatibility with fake leaf transport.

These finite doubles do not establish native storage, finality, API or restart
acceptance. The real stage/leaf suites qualify their own separate boundaries.
"""

from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_owner_job as owner_api
from process.custom_import import family_shard_owner_lock as locks
from process.custom_import.admission_sql import AdmissionError
from process.custom_import.build_counts import SourceOutcomeCounts
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_shard_owner_job import _verified_block as _check_verified_block
from process.custom_import.family_shard_plan import PlanWork, _Scratch
from tests.test_custom_import_family_shard_source_stage import _owner


class _Owned:
    def __init__(self, root, label, closed):
        self.root, self.label, self.closed = Path(root), label, closed
        self.root.mkdir()
        self.done = False

    async def close_terminal(self):
        if not self.done:
            self.root.rmdir()
            self.closed.append(self.label)
            self.done = True


class _Pipeline:
    """Only orchestration is real; producer/native outcomes below are doubled."""

    def __init__(self, job, registry):
        self.job, self.registry = job, registry
        self.calls, self.closed = [], []
        self.existing, self.phase, self.replayed = None, "source", None
        self.failure, self.has_base, self.no_change = None, False, None
        self.counts = SourceOutcomeCounts(7, 3)
        self.result = SimpleNamespace(generation_id=29, seal=object(), no_change=None)
        self.publication = object()
        self.persisted_counts = None
        self.base = SimpleNamespace(scope=(5, 8), check=self.base_check, headers=self.headers)

    def failed(self, name):
        if self.failure == name:
            self.failure = None
            raise ConnectionError("synthetic lost " + name + " reply")

    async def discover(self, *_args):
        self.calls.append("discover")
        return self.existing

    async def replay(self, *_args):
        self.calls.append("replay")
        return self.replayed

    async def snapshot(self, *_args):
        self.calls.append("snapshot")
        return SimpleNamespace(phase=self.phase, generation_id=29)

    @asynccontextmanager
    async def authority(self, *_args):
        yield object(), object()

    async def registry_read(self, *_args):
        self.calls.append("registry")
        return self.registry

    async def create(self, *_args):
        self.calls.append("create")
        self.failed("create")
        return 17

    async def begin(self, *_args):
        self.calls.append("begin")
        self.existing = 11
        self.failed("begin")
        return 11, self.registry

    @asynccontextmanager
    async def heartbeat(self, *_args):
        self.calls.append("heartbeat-start")
        try:
            yield
        finally:
            self.calls.append("heartbeat-stop")

    @asynccontextmanager
    async def pin(self, *_args, **_kwargs):
        self.calls.append("pin")
        try:
            yield self.base if self.has_base else None
        finally:
            self.closed.append("base")

    async def base_check(self):
        self.calls.append("base-check")

    def headers(self):
        self.calls.append("headers")
        return object()

    @asynccontextmanager
    async def keys(self, _base, *, work):
        root = Path(work.root)
        root.mkdir()
        self.calls.append("keys")
        try:
            yield lambda: iter(())
        finally:
            root.rmdir()
            self.closed.append("keys")

    def source_owner(self, context, _keys, *, root, **_kwargs):
        assert _kwargs["owner_lock"] is self.job._owner_lock
        _kwargs["owner_lock"].check()
        source = _Owned(root, "source", self.closed)
        source.context, source.source_completed, source.counts = context, False, None
        source.phase, source.homes = None, None
        source.admission, source.metadata = object(), SimpleNamespace(family_ids=lambda: iter(()))
        return source

    async def source(self, _factory, source, **_kwargs):
        self.calls.append("source")
        source.counts = self.counts
        self.failed("source-copy")
        self.failed("source-complete")
        source.source_completed = True
        if self.failure == "fatal":
            raise AdmissionError("synthetic fatal admission")
        source.phase = "rejected" if self.failure == "rejected" else "graph"
        source.homes = object()

    async def plan(self, request, _admission, headers, metadata, *, work, base_scope):
        assert request is self.job.request and list(metadata) == []
        assert (headers is None) == (base_scope is None) == (not self.has_base)
        self.calls.append("plan")
        output = _Owned(work.root, "plan", self.closed)
        self.failed("plan")
        return output

    def retained_owner(self, source, plan, base, *, work):
        assert source is self.job.source and plan is self.job.final_plan
        assert base is self.job.base
        retained = _Owned(work.root, "retained", self.closed)
        retained.complete, retained.context = False, object()
        return retained

    async def retained(self, _factory, retained):
        self.calls.append("retained")
        retained.complete = True
        self.failed("retained")

    def output_owner(self, retained, *, work):
        assert retained is self.job.retained
        output = _Owned(work.root, "output", self.closed)
        output.inputs = None
        return output

    async def output(self, factory, output, *, projection_factory, index_factory):
        self.calls.append("output")
        assert projection_factory == (factory, 1024) and index_factory is factory
        self.phase = "verifying"
        output.inputs = object()
        self.failed("output")

    async def complete(self, _factory, context, inputs, *, work, **_kwargs):
        assert context is self.job.retained.context and inputs is self.job.output_stage.inputs
        assert self.calls[-1] != "heartbeat-start"
        assert work.root == self.job.root and work.root.is_dir()
        for name in ("verification", "finality"):
            scratch = _Scratch(PlanWork(work.root / name, work.limits, work.register_cleanup, work.check))
            scratch.close()
        self.calls.append("complete")
        self.phase = "verified"
        self.failed("complete")
        self.result.no_change = self.no_change
        self.replayed = self.result
        return self.result

    async def activate(self, _factory, request, output):
        self.calls.append("activate")
        assert request is self.job.request and output is self.result
        self.failed("activate")
        return self.publication

    async def terminal_counts(self, *_args):
        self.calls.append("terminal-counts")
        if self.persisted_counts is None:
            raise FamilyBlockUnavailable("terminal SOURCE outcome counts are missing")
        return self.persisted_counts

    async def verified(self, *_args):
        self.calls.append("registered-block")

    async def freeze(self, *_args):
        self.calls.append("freeze")

    async def seal(self, *_args, **_kwargs):
        self.calls.append("seal")
        return self.result

    def install(self, monkeypatch):
        method_by_name = {
            "_existing_build": self.discover,
            "_page_session": self.authority,
            "load_registry": self.registry_read,
            "begin_shard_build": self.begin,
            "_renew_while_reading": self.heartbeat,
            "_snapshot": self.snapshot,
            "pinned_base": self.pin,
            "pinned_base_query_keys": self.keys,
            "SourceAdmissionStage": self.source_owner,
            "advance_source_admission": self.source,
            "build_family_plan": self.plan,
            "RetainedFamilyStage": self.retained_owner,
            "advance_retained_families": self.retained,
            "ShardOutputStage": self.output_owner,
            "advance_shard_output": self.output,
            "complete_closed_shard_build": self.complete,
            "_activate": self.activate,
            "_verified_block": self.verified,
            "seal_shard_build": self.seal,
            "terminal_source_outcomes": self.terminal_counts,
            "import_session_factory": lambda factory, *, work_mem_mb: (factory, work_mem_mb),
        }
        for name, method in method_by_name.items():
            monkeypatch.setattr(owner_api, name, method)
        monkeypatch.setattr(owner_api.store, "create_candidate", self.create)
        monkeypatch.setattr(owner_api.build_output, "_replay", self.replay)
        monkeypatch.setattr(owner_api.build_output, "_freeze_output_snapshot", self.freeze)
        monkeypatch.setattr(
            owner_api, "cleanup_family_plan", lambda output: (output.root.rmdir(), self.closed.append("plan"))
        )


@pytest.fixture
async def pipeline(tmp_path, monkeypatch):
    template = _owner(tmp_path)
    job = owner_api.OwnerShardJob(
        template.context.request,
        work=PlanWork(tmp_path / "job", template.limits.facts, lambda _p: None),
        source_limits=template.limits,
    )
    boundary = _Pipeline(job, template.context.registry)
    boundary.install(monkeypatch)
    try:
        yield job, boundary
    finally:
        await job.close_terminal()


def _attempt(job):
    return job.root / "source" / "attempt"


@pytest.mark.parametrize("base", [False, True])
async def test_full_pipeline_retains_owners_and_stops_compute_heartbeat(pipeline, base):
    job, boundary = pipeline
    boundary.has_base = base
    assert not job.root.exists() and job.source_counts == SourceOutcomeCounts(0, 0)
    factory = lambda: None
    assert await owner_api.advance_owner_shard_job(factory, job, attempt_root=_attempt(job)) is job
    assert job.complete and job.output is boundary.result and job.publication is boundary.publication
    assert job.source_counts is boundary.counts and job.no_change is None
    assert boundary.calls.index("heartbeat-stop") < boundary.calls.index("complete") < boundary.calls.index("activate")
    assert not boundary.closed
    await job.close_terminal()
    assert not job.root.exists() and boundary.closed == ["output", "retained", "plan", "source", "keys", "base"]


@pytest.mark.parametrize("failure", ["source-copy", "source-complete", "fatal"])
async def test_source_counts_follow_confirmed_source_completion(pipeline, failure):
    job, boundary = pipeline
    boundary.failure = failure
    expected = AdmissionError if failure == "fatal" else ConnectionError
    with pytest.raises(expected):
        await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    assert job.source.counts is boundary.counts
    assert job.source_counts == (boundary.counts if failure == "fatal" else SourceOutcomeCounts(0, 0))
    assert job.retained is job.output is None and not boundary.closed


@pytest.mark.parametrize("failure", ["begin", "retained", "output", "complete", "activate"])
async def test_native_reply_failure_keeps_original_owner_tree(pipeline, failure):
    job, boundary = pipeline
    boundary.failure = failure
    with pytest.raises(ConnectionError, match=failure):
        await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    source, plan, retained, output = job.source, job.final_plan, job.retained, job.output_stage
    assert not boundary.closed and not job.complete
    await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    for original, current in zip(
        (source, plan, retained, output), (job.source, job.final_plan, job.retained, job.output_stage)
    ):
        assert original is None or current is original
    assert job.complete and boundary.calls.count("source") == boundary.calls.count("plan") == 1


async def test_no_change_skips_pointer_activation(pipeline):
    job, boundary = pipeline
    boundary.no_change = object()
    await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    assert job.no_change is job.publication is boundary.no_change
    assert "activate" not in boundary.calls


async def test_rejected_admission_returns_counts_without_generation_or_activation(pipeline):
    job, boundary = pipeline
    boundary.failure = "rejected"
    await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    assert job.is_rejected and job.complete and job.source_counts is boundary.counts
    assert job.final_plan is job.retained is job.output_stage is job.output is job.publication is None
    assert not {"plan", "retained", "output", "complete", "activate"}.intersection(boundary.calls)


async def test_warm_terminal_replay_keeps_exact_source_counts(pipeline):
    job, boundary = pipeline
    boundary.failure = "output"
    with pytest.raises(ConnectionError, match="output"):
        await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    boundary.replayed = boundary.result
    await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    assert job.output is boundary.result and job.source_counts is boundary.counts
    assert job.complete and boundary.calls.count("source") == 1 and "complete" not in boundary.calls


async def test_failed_stage_cleanup_preserves_lower_owners(pipeline, monkeypatch):
    job, boundary = pipeline
    await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    close = job.output_stage.close_terminal

    async def failed():
        raise ConnectionError("synthetic undrained OUTPUT")

    monkeypatch.setattr(job.output_stage, "close_terminal", failed)
    with pytest.raises(ConnectionError, match="undrained OUTPUT"):
        await job.close_terminal()
    assert not boundary.closed and job.source.root.exists() and not job._closed
    monkeypatch.setattr(job.output_stage, "close_terminal", close)


async def test_source_join_failure_keeps_lock_and_borrowed_pins(pipeline, monkeypatch):
    job, boundary = pipeline
    await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    close = job.source.close_terminal

    async def undrained():
        raise ConnectionError("synthetic undrained SOURCE workers")

    monkeypatch.setattr(job.source, "close_terminal", undrained)
    with pytest.raises(ConnectionError, match="undrained SOURCE"):
        await job.close_terminal()
    assert "keys" not in boundary.closed and "base" not in boundary.closed
    job._owner_lock.check()
    with pytest.raises(BlockingIOError):
        locks.acquire_owner_job_lock(job.root, identity=job._root_identity)
    monkeypatch.setattr(job.source, "close_terminal", close)
    await job.close_terminal()
    assert not job.root.exists() and job._owner_lock._descriptor is None


async def test_advance_failure_preserves_exclusive_original_root_lock(pipeline):
    job, boundary = pipeline
    boundary.failure = "source-copy"
    with pytest.raises(ConnectionError):
        await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    job._owner_lock.check()
    with pytest.raises(BlockingIOError):
        locks.acquire_owner_job_lock(job.root, identity=job._root_identity)


async def test_live_owner_lock_checks_private_root_mode(pipeline):
    job, _boundary = pipeline
    job._ensure_root()
    job.root.chmod(0o755)
    try:
        with pytest.raises(ValueError, match="custody"):
            job.current()
    finally:
        job.root.chmod(0o700)


async def test_lock_acquisition_failure_preserves_root_before_native_work(pipeline, monkeypatch):
    job, boundary = pipeline
    acquire = owner_api.acquire_owner_job_lock

    def refused(*_args, **_kwargs):
        raise BlockingIOError("synthetic owner lock contention")

    monkeypatch.setattr(owner_api, "acquire_owner_job_lock", refused)
    with pytest.raises(BlockingIOError, match="contention"):
        await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    assert job.root.is_dir() and job._owner_lock is None and boundary.calls == ["discover"]
    with pytest.raises(FamilyBlockUnavailable, match="original lock"):
        await job.close_terminal()
    monkeypatch.setattr(owner_api, "acquire_owner_job_lock", acquire)
    job._ensure_root()


@pytest.mark.parametrize("phase", ["source", "graph", "output", "verifying"])
async def test_cold_unfinished_progress_is_not_adopted(pipeline, phase):
    job, boundary = pipeline
    boundary.existing, boundary.phase = 11, phase
    with pytest.raises(FamilyBlockUnavailable, match="cold reopening"):
        await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    assert boundary.calls == ["discover", "replay", "snapshot"] and not job.root.exists()


async def test_cold_terminal_output_is_preserved_but_counts_block_activation(pipeline):
    job, boundary = pipeline
    boundary.existing, boundary.replayed = 11, boundary.result
    for _retry in range(2):
        with pytest.raises(FamilyBlockUnavailable, match="outcome counts"):
            await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
        with pytest.raises(FamilyBlockUnavailable, match="outcome counts"):
            _ = job.source_counts
    assert job.output is boundary.result and not job.complete and job.publication is None
    assert boundary.calls == ["discover", "replay", "terminal-counts", "terminal-counts"] and not job.root.exists()


async def test_cold_verified_routes_freeze_seal_before_count_refusal(pipeline):
    job, boundary = pipeline
    boundary.existing, boundary.phase = 11, "verified"
    with pytest.raises(FamilyBlockUnavailable, match="outcome counts"):
        await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    assert boundary.calls == ["discover", "replay", "snapshot", "registered-block", "freeze", "seal", "terminal-counts"]
    assert job.output is boundary.result and not job.complete


async def test_cold_terminal_uses_bound_pair_before_activation(pipeline):
    job, boundary = pipeline
    boundary.existing, boundary.replayed, boundary.persisted_counts = 11, boundary.result, boundary.counts
    await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    assert job.complete and job.source is None and job.source_counts is boundary.counts
    assert boundary.calls == ["discover", "replay", "terminal-counts", "activate"]


async def test_cold_count_retry_uses_same_terminal_output(pipeline):
    job, boundary = pipeline
    boundary.existing, boundary.replayed = 11, boundary.result
    with pytest.raises(FamilyBlockUnavailable, match="outcome counts"):
        await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    boundary.persisted_counts = boundary.counts
    await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    assert job.output is boundary.result and job.source_counts is boundary.counts and job.complete
    assert boundary.calls.count("replay") == boundary.calls.count("activate") == 1


@pytest.mark.parametrize("problem", [None, "missing", "capture", "layout", "phase"])
async def test_verified_native_registration_requires_exact_block_identity(pipeline, monkeypatch, problem):
    job, boundary = pipeline
    job.registry, job.build_id = boundary.registry, 11
    job.layout = FamilyBlockLayout(job.request.definition, job.registry.child_collection_slots)
    family = SimpleNamespace(
        family_id=17,
        dataset_id=job.request.dataset_id,
        definition_revision_id=job.request.definition_revision_id,
        schema_revision_id=job.request.schema_revision_id,
        capture_bundle_id=8,
        generation_id=29,
        producing_token_sha256=owner_api.producer_token_sha256(job.request),
    )
    descriptor = SimpleNamespace(
        layout_contract=owner_api.FAMILY_BLOCK_LAYOUT_CONTRACT,
        phase="logged",
        layout_sha256=job.layout.digest,
        definition_sha256=bytes.fromhex(job.request.definition.digest),
        collection_slots=dict(job.layout.collection_slots),
    )
    if problem == "capture":
        family.capture_bundle_id = 9
    elif problem == "layout":
        descriptor.layout_sha256 = b"x" * 32
    elif problem == "phase":
        descriptor.phase = "closed"

    async def read(*_args):
        return SimpleNamespace(one_or_none=lambda: None if problem == "missing" else (family, descriptor))

    monkeypatch.setattr(owner_api.store, "_execute", read)
    if problem is None:
        await _check_verified_block(lambda: None, job, SimpleNamespace(capture_bundle_id=8, generation_id=29))
        assert job.family_id == 17
    else:
        with pytest.raises(FamilyBlockUnavailable, match="block registration"):
            await _check_verified_block(lambda: None, job, SimpleNamespace(capture_bundle_id=8, generation_id=29))
        assert job.family_id is None
    assert not job.root.exists() and "create" not in boundary.calls


async def test_existing_root_is_never_adopted(pipeline):
    job, boundary = pipeline
    job.root.mkdir()
    try:
        with pytest.raises(FileExistsError):
            await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
        assert boundary.calls == ["discover"]
    finally:
        job.root.rmdir()


async def test_expired_request_and_wrong_attempt_are_refused_before_creation(pipeline):
    job, boundary = pipeline
    with pytest.raises(ValueError, match="direct child"):
        await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=job.root / "wrong")
    job._deadline = 0.0
    with pytest.raises(TimeoutError):
        await owner_api.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    assert not job.root.exists() and "create" not in boundary.calls


def test_projection_memory_and_typed_request_bounds(tmp_path):
    template = _owner(tmp_path)
    work = PlanWork(tmp_path / "job", template.limits.facts, lambda _p: None)
    with pytest.raises(ValueError, match="positive integer"):
        owner_api.OwnerShardJob(
            template.context.request, work=work, source_limits=template.limits, projection_work_mem_mb=True
        )
    with pytest.raises(ValueError, match="original request"):
        owner_api.OwnerShardJob(replace(template.context.request).__dict__, work=work, source_limits=template.limits)
