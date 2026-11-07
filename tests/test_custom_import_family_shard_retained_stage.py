# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained orchestration/custody with explicit leaf/native transport doubles.

Opaque leaf results exercise ordering and retry only; they are not comparison,
native-storage or finality acceptance. Existing retained leaf suites prove data.
"""

import asyncio
import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_retained_stage as stage
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_plan import FinalPlanDescriptor, PlanWork
from tests.test_custom_import_family_shard_source_stage import _Artifact, _owner


class _Boundary:
    """Finite producer/native doubles, never returned as native acceptance."""

    def __init__(self, owner):
        self.owner, self.phase = owner, "graph"
        self.calls, self.closed, self.copies = [], [], []
        self.committed = {}
        self.fail_copy = self.fail_homes = self.fail_tail = self.fail_allocation = False
        self.native_tail, self.native_homes = False, False
        self.original, self.stored = object(), object()
        self.encoding_entered, self.encoding_release = None, None

    def install(self, monkeypatch):
        # Opaque leaf results have no file custody; real shared-budget cases
        # exercise actual closed spools in the retained-stage budget suite.
        monkeypatch.setattr(stage.checkpoint, "retained_limits", lambda owner: owner.work.limits)
        for module, name, method in (
            (stage.source_stage, "_native_phase", self.native_phase),
            (stage, "build_retained_input", self.input),
            (stage, "cleanup_retained_input", lambda artifact: artifact.close()),
            (stage.allocation, "allocate_retained_destination", self.allocate),
            (stage.allocation, "cleanup_retained_destination", lambda artifact: artifact.close()),
            (stage, "verify_retained_destination", self.verify),
            (stage, "build_retained_output", self.encode),
            (stage, "prove_retained_output", self.prove),
            (stage.source_stage.store, "copy_shard", self.copy),
            (stage, "register_retained_homes", self.homes),
            (stage, "complete_retained_pack_counters", self.tail),
        ):
            monkeypatch.setattr(module, name, method)

    async def native_phase(self, *_args):
        self.calls.append("native")
        return self.phase

    async def input(self, base, plan, *, work):
        self.calls.append("input")
        assert base is self.owner.base and plan is self.owner.final_plan
        return _Artifact(work.root, "input", self.closed)

    async def allocate(self, _factory, context, *, work, journal):
        self.calls.append("allocate")
        assert context is self.owner.context and not self.native_tail
        assert journal is self.owner.source.allocation_journal
        if self.fail_allocation:
            raise ConnectionError("uncertain retained allocation")
        return _Artifact(work.root, "destination", self.closed)

    async def verify(self, context, destination, *, work):
        self.calls.append("verify")
        assert context is self.owner.context and destination is self.owner.destination
        return self.original

    def encode(self, context, destination, original, keys, *, work, shard_id):
        self.calls.append("encode")
        assert (context, destination, original) == (self.owner.context, self.owner.destination, self.original)
        assert callable(keys) and list(keys()) == [] and shard_id == 7
        output = _Artifact(work.root, "encoded", self.closed)
        output.receipt = stage.ShardOutputProof(self.owner.source.context.layout, shard_id, b"p" * 32, 0).receipt()
        if self.encoding_entered is not None:
            self.encoding_entered()
            assert self.encoding_release.wait(5)
        return output

    async def prove(self, context, destination, original, output, keys, *, work):
        self.calls.append("prove")
        assert (context, destination, original, output) == (
            self.owner.context,
            self.owner.destination,
            self.original,
            self.owner.encoded,
        )
        assert callable(keys) and list(keys()) == []
        return self.stored

    async def copy(self, _factory, _request, _family, _layout, batches, receipt, **_kwargs):
        assert not self.native_tail
        shard = receipt["shard_id"]
        self.copies.append(shard)
        if shard in self.committed:
            assert self.committed[shard] == receipt
            return receipt
        assert list(batches) == [{}]
        self.committed[shard] = receipt
        if shard == 6 and self.fail_copy:
            self.fail_copy = False
            raise ConnectionError("retained COPY response lost")
        return receipt

    async def homes(self, _factory, context, destination, *, work):
        self.calls.append("homes")
        assert context is self.owner.context and destination is self.owner.destination
        assert not self.native_tail and set(self.committed) == {6, 7}
        self.native_homes = True
        if self.fail_homes:
            self.fail_homes = False
            raise ConnectionError("home registration response lost")
        return SimpleNamespace(root_count=0, child_count=0)

    async def tail(self, _factory, *inputs, work):
        context, destination, original, stored, encoded, receipt = inputs
        self.calls.append("tail")
        assert self.owner.tail_started and self.native_homes and self.owner.homes is not None
        assert (context, destination, original, stored, encoded, receipt) == (
            self.owner.context,
            self.owner.destination,
            self.original,
            self.stored,
            self.owner.encoded,
            self.owner.common_receipt,
        )
        self.native_tail = True
        if self.fail_tail:
            self.fail_tail = False
            raise ConnectionError("tail commit response lost")
        return ((1, 0), (2, 0))


def _ready_source(tmp_path):
    source = _owner(tmp_path)
    source._ensure_root()
    source.phase, source.homes = "graph", object()
    source.workers = tuple(object() for _ in range(4))
    for name in (
        "prepared",
        "routed",
        "tasks",
        "admission",
        "counts",
        "common_output",
        "common_receipt",
        "events",
        "event_receipt",
    ):
        setattr(source, name, object())
    source.allocated = SimpleNamespace(source=SimpleNamespace(partition_sha256=(b"p" * 32).hex()))
    source.metadata = SimpleNamespace(query_keys=lambda: iter(()))
    source.common_context = SimpleNamespace(request=source.context.request)
    return source


@pytest.fixture
async def retained(tmp_path, monkeypatch):
    source = _ready_source(tmp_path)
    from process.custom_import.family_shard_source_journal import create_source_allocation_journal

    source.allocation_journal = create_source_allocation_journal(
        source.context.build_id,
        work=PlanWork(source.root / "journal", source.limits.allocation.spool, lambda _path: None),
        control_limits=source.limits.facts,
    )
    # Inert typed identity token at the mocked producer boundary, never parsed
    # or asserted to be an accepted family plan by these orchestration tests.
    plan_root = tmp_path / "caller-plan"
    plan_root.mkdir()
    plan = FinalPlanDescriptor(str(plan_root), 1, 1, 1, 1, 0, b"f" * 32, b"a" * 32, (), None, (), 1024)
    work = PlanWork(tmp_path / "retained", source.limits.facts, lambda _path: None)
    owner = stage.RetainedFamilyStage(source, plan, None, work=work)
    boundary = _Boundary(owner)
    boundary.install(monkeypatch)
    try:
        yield owner, boundary
    finally:
        await owner.close_terminal()
        assert source.root.exists() and plan_root.exists() and not source._closed
        source.allocation_journal.close_terminal()
        await source.close_terminal()


async def test_continuation_never_reenters_retained_allocation_or_tail(retained):
    from process.custom_import.continuation_runtime import BuildContinuationContext

    owner, boundary = retained
    owner.source.context = replace(
        owner.source.context,
        request=replace(owner.source.context.request, continuation=BuildContinuationContext(9, 11, b"p" * 32)),
    )
    with pytest.raises(FamilyBlockUnavailable, match="retained prefix"):
        await stage.advance_retained_families(None, owner)
    assert not owner._busy and not owner.root.exists() and boundary.calls == []


async def test_empty_base_executes_complete_sequence(retained):
    owner, boundary = retained
    assert not owner.root.exists()
    await stage.advance_retained_families(None, owner)
    assert owner.complete and owner.pack_tail == ((1, 0), (2, 0))
    assert boundary.calls == ["native", "input", "allocate", "verify", "encode", "prove", "homes", "tail"]
    assert boundary.copies == [6, 7] and owner.common_receipt["source_record_count"] == 0
    assert owner.common_receipt["partition_sha256"] == b"p" * 32
    assert not boundary.closed
    await owner.close_terminal()
    assert boundary.closed == ["encoded", "destination", "input"] and not owner.root.exists()


async def test_uncertain_tail_retries_only_control(retained):
    owner, boundary = retained
    boundary.fail_tail = True
    with pytest.raises(ConnectionError, match="tail commit"):
        await stage.advance_retained_families(None, owner)
    originals = (
        owner.retained_input,
        owner.destination,
        owner.original,
        owner.stored,
        owner.encoded,
        owner.common_receipt,
        owner.homes,
    )
    assert owner.tail_started and not owner.complete and boundary.native_tail
    first_calls = tuple(boundary.calls)
    await stage.advance_retained_families(None, owner)
    assert tuple(boundary.calls) == (*first_calls, "native", "tail")
    assert (
        owner.retained_input,
        owner.destination,
        owner.original,
        owner.stored,
        owner.encoded,
        owner.common_receipt,
        owner.homes,
    ) == originals
    assert boundary.copies == [6, 7] and owner.complete


@pytest.mark.parametrize("failure", ["copy", "homes"])
async def test_partial_native_commit_reuses_artifacts(retained, failure):
    owner, boundary = retained
    setattr(boundary, "fail_" + failure, True)
    with pytest.raises(ConnectionError):
        await stage.advance_retained_families(None, owner)
    originals = owner.retained_input, owner.destination, owner.encoded, owner.original, owner.stored
    assert not owner.tail_started and not boundary.closed
    await stage.advance_retained_families(None, owner)
    assert (
        owner.complete
        and (owner.retained_input, owner.destination, owner.encoded, owner.original, owner.stored) == originals
    )
    assert all(boundary.calls.count(name) == 1 for name in ("input", "allocate", "verify", "encode", "prove"))


async def test_uncertain_ids_retry_only_with_original_journal_and_inputs(retained):
    owner, boundary = retained
    boundary.fail_allocation = True
    with pytest.raises(ConnectionError, match="uncertain retained allocation"):
        await stage.advance_retained_families(None, owner)
    assert owner.pending == "destination" and owner.retained_input.root.exists()
    original = owner.retained_input, owner.context, owner.source.allocation_journal
    boundary.fail_allocation = False
    await stage.advance_retained_families(None, owner)
    assert owner.complete and (owner.retained_input, owner.context, owner.source.allocation_journal) == original
    assert boundary.calls.count("input") == 1 and boundary.calls.count("allocate") == 2


async def test_uncertain_ids_refuse_a_replaced_source_journal(retained):
    owner, boundary = retained
    boundary.fail_allocation = True
    with pytest.raises(ConnectionError):
        await stage.advance_retained_families(None, owner)
    original = owner.source.allocation_journal
    owner.source.allocation_journal = None
    try:
        with pytest.raises(FamilyBlockUnavailable, match="journal owner changed"):
            await stage.advance_retained_families(None, owner)
        assert boundary.calls.count("allocate") == 1
    finally:
        owner.source.allocation_journal = original


@pytest.mark.parametrize("phase", ["source", "admission", "output", "rejected"])
async def test_native_phase_precedes_any_creation(retained, phase):
    owner, boundary = retained
    boundary.phase = phase
    with pytest.raises(FamilyBlockUnavailable, match="native GRAPH"):
        await stage.advance_retained_families(None, owner)
    assert boundary.calls == ["native"] and not owner.root.exists()


async def test_cancelled_encoding_retains_exact_output(retained):
    owner, boundary = retained
    entered, release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    boundary.encoding_entered = lambda: loop.call_soon_threadsafe(entered.set)
    boundary.encoding_release = release
    task = asyncio.create_task(stage.advance_retained_families(None, owner))
    try:
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        output = owner.encoded
        assert output.root.exists() and owner.pending is None and not boundary.closed
        await stage.advance_retained_families(None, owner)
        assert owner.encoded is output and boundary.calls.count("encode") == 1 and owner.complete
    finally:
        release.set()
